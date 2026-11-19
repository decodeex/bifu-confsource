package confsource_test

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

// clusterEnv 两个成员的内嵌 etcd 集群，用来制造「成员还在但没有 leader」的状态。
// confsource 只连 0 号成员；停掉 1 号成员后 0 号失去多数派，几个选举周期后服务端会取消 WithRequireLeader 的流
type clusterEnv struct {
	testEnv
	members []*clusterMember
}

type clusterMember struct {
	name       string
	dataDir    string
	clientPort int
	peerPort   int
	etcd       *embed.Etcd
}

func newClusterEnv(t testingT, size int) *clusterEnv {
	t.Helper()
	env := &clusterEnv{}
	env.t = t
	env.snapshots = t.TempDir()
	env.registry = prometheus.NewRegistry()
	env.logs = &logBuffer{}
	for i := 0; i < size; i++ {
		env.members = append(env.members, &clusterMember{
			name:       fmt.Sprintf("m%d", i),
			dataDir:    t.TempDir(),
			clientPort: freePort(t),
			peerPort:   freePort(t),
		})
	}
	env.clientPort = env.members[0].clientPort
	env.startAll()
	t.Cleanup(env.stopAll)
	env.dumpLogsOnFailure()
	return env
}

func (c *clusterEnv) initialCluster() string {
	parts := make([]string, 0, len(c.members))
	for _, m := range c.members {
		parts = append(parts, fmt.Sprintf("%s=http://127.0.0.1:%d", m.name, m.peerPort))
	}
	return strings.Join(parts, ",")
}

func (c *clusterEnv) startMember(i int) {
	c.t.Helper()
	m := c.members[i]
	cfg := embed.NewConfig()
	cfg.Name = m.name
	cfg.Dir = m.dataDir
	cfg.LogLevel = "error"
	cfg.ListenClientUrls = []url.URL{mustURL(c.t, fmt.Sprintf("http://127.0.0.1:%d", m.clientPort))}
	cfg.AdvertiseClientUrls = cfg.ListenClientUrls
	cfg.ListenPeerUrls = []url.URL{mustURL(c.t, fmt.Sprintf("http://127.0.0.1:%d", m.peerPort))}
	cfg.AdvertisePeerUrls = cfg.ListenPeerUrls
	cfg.InitialCluster = c.initialCluster()
	etcd, err := embed.StartEtcd(cfg)
	require.NoError(c.t, err)
	m.etcd = etcd
}

func (c *clusterEnv) stopMember(i int) {
	m := c.members[i]
	if m.etcd == nil {
		return
	}
	m.etcd.Close()
	m.etcd = nil
}

// startAll 拉起全部成员并等它们就绪，再建测试客户端（连 0 号成员）
func (c *clusterEnv) startAll() {
	c.t.Helper()
	for i := range c.members {
		c.startMember(i)
	}
	for _, m := range c.members {
		select {
		case <-m.etcd.Server.ReadyNotify():
		case <-time.After(30 * time.Second):
			c.t.Fatal("embedded etcd cluster did not become ready")
		}
	}
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{c.endpoint()}, DialTimeout: 2 * time.Second})
	require.NoError(c.t, err)
	c.cli = cli
}

func (c *clusterEnv) stopAll() {
	if c.cli != nil {
		c.cli.Close()
		c.cli = nil
	}
	for i := range c.members {
		c.stopMember(i)
	}
}

// restartFresh 全部成员换空数据目录重建集群：模拟不带 bump 的恢复，集群 revision 倒退到 1
func (c *clusterEnv) restartFresh() {
	c.t.Helper()
	c.stopAll()
	for _, m := range c.members {
		m.dataDir = c.t.TempDir()
	}
	c.startAll()
}

// waitLeader 等 0 号成员重新看到 leader
func (c *clusterEnv) waitLeader() {
	c.t.Helper()
	require.Eventually(c.t, func() bool { return c.members[0].etcd.Server.Leader() != 0 }, 30*time.Second, 100*time.Millisecond, "member 0 never saw a leader")
}
