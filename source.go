package confsource

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-kratos/kratos/v2/config"
	"github.com/go-kratos/kratos/v2/log"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const configFormat = "yaml"

// Source 基于 etcd 的 Kratos 配置源：精确 key 读取与 Watch、本地快照、降级启动、串行提交。
// 它不解析配置内容，${VAR} 展开与结构校验由调用方通过 Validator 注入。
type Source struct {
	opts      options
	env       PlatformEnv
	configKey string
	metaKey   string
	client    *clientv3.Client
	snap      *snapshotStore
	metrics   *metrics
	log       *log.Helper

	// committed 已通过校验并写入快照的最新 revision；快照与 meta 不配对时为 0
	committed atomic.Int64
	degraded  atomic.Bool

	ctx       context.Context
	cancel    context.CancelFunc
	watchOnce sync.Once
}

// New 创建配置源，endpoints、env、service 缺一不可
func New(opts ...Option) (*Source, error) {
	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}
	if len(o.endpoints) == 0 {
		return nil, errors.New("confsource: endpoints required")
	}
	if o.service == "" {
		return nil, errors.New("confsource: service required")
	}
	env, err := ParsePlatformEnv(o.podEnv)
	if err != nil {
		return nil, err
	}
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   o.endpoints,
		DialTimeout: o.timeout,
		TLS:         o.tls,
		Username:    o.username,
		Password:    o.password,
	})
	if err != nil {
		return nil, fmt.Errorf("confsource: etcd client: %w", err)
	}
	m, err := newMetrics(o.registerer, env, o.service)
	if err != nil {
		client.Close()
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Source{
		opts:      o,
		env:       env,
		configKey: fmt.Sprintf("/config/%s/%s.yaml", env, o.service),
		metaKey:   fmt.Sprintf("/meta/%s/%s", env, o.service),
		client:    client,
		snap:      newSnapshotStore(o.snapshotDir, env, o.service),
		metrics:   m,
		log:       log.NewHelper(log.With(o.logger, "module", "confsource", "env", string(env), "service", o.service)),
		ctx:       ctx,
		cancel:    cancel,
	}, nil
}

// Load 首次加载：优先 etcd，etcd 不可达时用本地快照降级启动，两者都不可用返回 ErrNoConfig。
// etcd 可达但 key 不存在说明配置从未发布，不用快照顶替。
func (s *Source) Load() ([]*config.KeyValue, error) {
	raw, rev, publishID, err := s.fetch(s.ctx)
	if err == nil {
		if err := s.commit(rev, raw, publishID); err != nil {
			return nil, err
		}
		s.setDegraded(false)
		return s.keyValues(raw), nil
	}
	if errors.Is(err, ErrNoConfig) {
		return nil, err
	}

	s.log.Warnf("etcd unavailable, starting from snapshot: %v", err)
	snapRaw, snapRev, snapErr := s.snap.read()
	if snapErr != nil {
		return nil, fmt.Errorf("%w (etcd: %v; snapshot: %v)", ErrNoConfig, err, snapErr)
	}
	if s.opts.validator != nil {
		if verr := s.opts.validator(snapRev, snapRaw); verr != nil {
			return nil, fmt.Errorf("confsource: snapshot rejected: %w", verr)
		}
	}
	s.committed.Store(snapRev)
	s.metrics.loadedRevision.Set(float64(snapRev))
	s.setDegraded(true)
	return s.keyValues(snapRaw), nil
}

// Watch 启动串行事件循环并返回 Kratos Watcher，每个 Source 只能启动一次
func (s *Source) Watch() (config.Watcher, error) {
	var w *watcher
	s.watchOnce.Do(func() { w = newWatcher(s) })
	if w == nil {
		return nil, ErrWatchStarted
	}
	return w, nil
}

// Degraded 是否运行在未与 etcd 对齐的快照上
func (s *Source) Degraded() bool { return s.degraded.Load() }

// Revision 当前内存配置对应的 etcd ModRevision，快照不配对时为 0
func (s *Source) Revision() int64 { return s.committed.Load() }

// Close 停止 Watch 并关闭 etcd 连接
func (s *Source) Close() error {
	s.cancel()
	return s.client.Close()
}

// fetch 在同一事务里读配置与 meta，保证两者来自同一 revision
func (s *Source) fetch(ctx context.Context) (raw []byte, rev int64, publishID string, err error) {
	ctx, cancel := context.WithTimeout(ctx, s.opts.timeout)
	defer cancel()
	resp, err := s.client.Txn(ctx).Then(clientv3.OpGet(s.configKey), clientv3.OpGet(s.metaKey)).Commit()
	if err != nil {
		return nil, 0, "", err
	}
	cfg := resp.Responses[0].GetResponseRange()
	if len(cfg.Kvs) == 0 {
		return nil, 0, "", fmt.Errorf("%w: key %s not found", ErrNoConfig, s.configKey)
	}
	kv := cfg.Kvs[0]
	return kv.Value, kv.ModRevision, publishIDFrom(resp.Responses[1].GetResponseRange()), nil
}

// publishIDAt 取某个 revision 时 meta 里的 publish_id；拿不到返回空，不影响加载
func (s *Source) publishIDAt(rev int64) string {
	ctx, cancel := context.WithTimeout(s.ctx, s.opts.timeout)
	defer cancel()
	resp, err := s.client.Get(ctx, s.metaKey, clientv3.WithRev(rev))
	if err != nil {
		return ""
	}
	return publishIDFrom((*pb.RangeResponse)(resp))
}

func publishIDFrom(r *pb.RangeResponse) string {
	if r == nil || len(r.Kvs) == 0 {
		return ""
	}
	var meta struct {
		PublishID string `json:"publish_id"`
	}
	if json.Unmarshal(r.Kvs[0].Value, &meta) != nil {
		return ""
	}
	return meta.PublishID
}

// commit 校验通过后写快照并推进 revision。校验失败时什么都不动；
// 快照写失败只记日志，etcd 才是主路径，不能因为节点磁盘问题拒绝一份合法配置。
func (s *Source) commit(rev int64, raw []byte, publishID string) error {
	if s.opts.validator != nil {
		if err := s.opts.validator(rev, raw); err != nil {
			return fmt.Errorf("confsource: revision %d rejected: %w", rev, err)
		}
	}
	if err := s.snap.write(raw, snapshotMeta{ModRevision: rev, PublishID: publishID, SavedAt: time.Now()}); err != nil {
		s.log.Errorf("write snapshot failed: %v", err)
	}
	s.committed.Store(rev)
	s.metrics.loadedRevision.Set(float64(rev))
	return nil
}

func (s *Source) keyValues(raw []byte) []*config.KeyValue {
	return []*config.KeyValue{{Key: s.configKey, Value: raw, Format: configFormat}}
}

func (s *Source) setDegraded(v bool) {
	s.degraded.Store(v)
	if v {
		s.metrics.degraded.Set(1)
	} else {
		s.metrics.degraded.Set(0)
	}
}
