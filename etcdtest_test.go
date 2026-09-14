package confsource_test

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/go-kratos/kratos/v2/config"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"

	"github.com/decodeex/bifu-confsource"
)

const testService = "content"

// testEnv 一个内嵌 etcd 加上 confsource 需要的全部外围：快照目录、指标注册器、日志缓冲
type testEnv struct {
	t          *testing.T
	dataDir    string
	clientPort int
	peerPort   int
	etcd       *embed.Etcd
	cli        *clientv3.Client
	snapshots  string
	registry   *prometheus.Registry
	logs       *logBuffer
}

func newTestEnv(t *testing.T) *testEnv {
	t.Helper()
	env := &testEnv{
		t:          t,
		dataDir:    t.TempDir(),
		clientPort: freePort(t),
		peerPort:   freePort(t),
		snapshots:  t.TempDir(),
		registry:   prometheus.NewRegistry(),
		logs:       &logBuffer{},
	}
	env.start()
	t.Cleanup(env.stop)
	return env
}

func (e *testEnv) endpoint() string {
	return fmt.Sprintf("http://127.0.0.1:%d", e.clientPort)
}

// options confsource 的基础选项，退避调短让重连测试跑得快
func (e *testEnv) options(extra ...confsource.Option) []confsource.Option {
	opts := []confsource.Option{
		confsource.WithEndpoints(e.endpoint()),
		confsource.WithEnv("dev"),
		confsource.WithService(testService),
		confsource.WithSnapshotDir(e.snapshots),
		confsource.WithRegisterer(e.registry),
		confsource.WithLogger(log.NewStdLogger(e.logs)),
		confsource.WithTimeout(2 * time.Second),
		confsource.WithBackoff(100*time.Millisecond, 500*time.Millisecond),
	}
	return append(opts, extra...)
}

func (e *testEnv) start() {
	e.t.Helper()
	cfg := embed.NewConfig()
	cfg.Name = "confsource-test"
	cfg.Dir = e.dataDir
	cfg.LogLevel = "error"
	cfg.ListenClientUrls = []url.URL{mustURL(e.t, e.endpoint())}
	cfg.AdvertiseClientUrls = cfg.ListenClientUrls
	cfg.ListenPeerUrls = []url.URL{mustURL(e.t, fmt.Sprintf("http://127.0.0.1:%d", e.peerPort))}
	cfg.AdvertisePeerUrls = cfg.ListenPeerUrls
	cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name)

	etcd, err := embed.StartEtcd(cfg)
	require.NoError(e.t, err)
	select {
	case <-etcd.Server.ReadyNotify():
	case <-time.After(20 * time.Second):
		etcd.Close()
		e.t.Fatal("embedded etcd did not become ready")
	}
	e.etcd = etcd

	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{e.endpoint()}, DialTimeout: 2 * time.Second})
	require.NoError(e.t, err)
	e.cli = cli
}

// stop 关闭 etcd 与测试客户端，重复调用安全；embed.Etcd.Close 本身不能调两次
func (e *testEnv) stop() {
	if e.etcd == nil {
		return
	}
	e.cli.Close()
	e.etcd.Close()
	e.etcd = nil
}

// restart 用同一数据目录和端口重新拉起，模拟 etcd 故障恢复
func (e *testEnv) restart() {
	e.t.Helper()
	e.stop()
	e.start()
}

// publish 模拟平台发布：配置与 meta 同一事务写入，返回该次写入的 revision
func (e *testEnv) publish(yaml, publishID string) int64 {
	e.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := e.cli.Txn(ctx).Then(
		clientv3.OpPut("/config/develop/"+testService+".yaml", yaml),
		clientv3.OpPut("/meta/develop/"+testService, fmt.Sprintf(`{"publish_id":%q}`, publishID)),
	).Commit()
	require.NoError(e.t, err)
	return resp.Header.Revision
}

func (e *testEnv) deleteConfig() {
	e.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := e.cli.Delete(ctx, "/config/develop/"+testService+".yaml")
	require.NoError(e.t, err)
}

func (e *testEnv) compact(rev int64) {
	e.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := e.cli.Compact(ctx, rev)
	require.NoError(e.t, err)
}

func (e *testEnv) snapshot() string {
	data, err := os.ReadFile(filepath.Join(e.snapshots, "develop", testService+".yaml"))
	if err != nil {
		return ""
	}
	return string(data)
}

func (e *testEnv) gauge(name string) float64 {
	e.t.Helper()
	families, err := e.registry.Gather()
	require.NoError(e.t, err)
	for _, f := range families {
		if f.GetName() == name && len(f.GetMetric()) > 0 {
			return f.GetMetric()[0].GetGauge().GetValue()
		}
	}
	e.t.Fatalf("metric %s not registered", name)
	return 0
}

// assertCommitted 验收口径：revision、磁盘快照、gauge 三者都指向同一版，且不在降级态
func (e *testEnv) assertCommitted(src *confsource.Source, rev int64, yaml string) {
	e.t.Helper()
	require.Equal(e.t, rev, src.Revision(), "Revision()")
	require.Equal(e.t, yaml, e.snapshot(), "disk snapshot")
	require.Equal(e.t, float64(rev), e.gauge("confsource_loaded_revision"), "loaded_revision gauge")
	require.False(e.t, src.Degraded(), "Degraded()")
	require.Equal(e.t, float64(0), e.gauge("confsource_degraded"), "degraded gauge")
}

func nextWithin(t *testing.T, w config.Watcher, d time.Duration) []*config.KeyValue {
	t.Helper()
	type result struct {
		kvs []*config.KeyValue
		err error
	}
	ch := make(chan result, 1)
	go func() {
		kvs, err := w.Next()
		ch <- result{kvs, err}
	}()
	select {
	case r := <-ch:
		require.NoError(t, r.err)
		return r.kvs
	case <-time.After(d):
		t.Fatalf("no config delivered within %s", d)
		return nil
	}
}

func mustURL(t *testing.T, raw string) url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	require.NoError(t, err)
	return *u
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// logBuffer 并发安全的日志缓冲，用来断言某个 revision 被拒绝过
type logBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *logBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *logBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// overwriteSnapshot 直接改写快照 yaml 而不动 meta，制造两者不配对的场景
func overwriteSnapshot(t *testing.T, env *testEnv, yaml string) {
	t.Helper()
	path := filepath.Join(env.snapshots, "develop", testService+".yaml")
	require.NoError(t, os.WriteFile(path, []byte(yaml), 0o644))
}
