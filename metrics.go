package confsource

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
)

var metricLabels = []string{"service"}

// resync 的触发原因，一次完成的 resync 只记一个
const (
	resyncStartup    = "startup"    // 快照降级启动或游标为 0，建 Watch 前先全量拉
	resyncCompacted  = "compacted"  // Watch 起点被压缩
	resyncReconnect  = "reconnect"  // Watch channel 因其它原因关闭（无 leader 等）
	resyncRegression = "regression" // resync 时发现集群 revision 低于游标
)

// metrics 全部带 service 标签。last_sync 在收到第一个有效 etcd 响应前没有序列，
// 「序列缺失」就是「本进程还没同步过 etcd」，不用 0 表示
type metrics struct {
	service        string
	loadedRevision prometheus.Gauge
	degraded       prometheus.Gauge
	contentHash    *prometheus.GaugeVec
	lastSync       *prometheus.GaugeVec
	resync         *prometheus.CounterVec
	regressed      prometheus.Counter
	commitFailures *prometheus.CounterVec
	eventsSkipped  prometheus.Counter
	metaLookup     *prometheus.CounterVec
	snapshotFail   *prometheus.CounterVec
	loadDuration   prometheus.Observer

	// currentHash 当前暴露的内容哈希序列，换值时先删旧序列，任何时刻只有一条
	mu          sync.Mutex
	currentHash string
}

func newMetrics(reg prometheus.Registerer, service string) (*metrics, error) {
	loaded, err := register(reg, prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "confsource_loaded_revision",
		Help: "etcd ModRevision of the config currently committed by confsource",
	}, metricLabels))
	if err != nil {
		return nil, err
	}
	degraded, err := register(reg, prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "confsource_degraded",
		Help: "1 when running on local snapshot without confirming against etcd",
	}, metricLabels))
	if err != nil {
		return nil, err
	}
	contentHash, err := register(reg, prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "confsource_content_hash_info",
		Help: "sha256 of the config content currently committed; always 1, one series per process",
	}, []string{"service", "hash"}))
	if err != nil {
		return nil, err
	}
	lastSync, err := register(reg, prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "confsource_last_sync_timestamp_seconds",
		Help: "unix time of the last successful etcd response (load, watch event, progress notify, resync); absent until the first one",
	}, metricLabels))
	if err != nil {
		return nil, err
	}
	resync, err := register(reg, prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "confsource_resync_total",
		Help: "completed full reloads by trigger reason",
	}, []string{"service", "reason"}))
	if err != nil {
		return nil, err
	}
	regressed, err := register(reg, prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "confsource_revision_regressed_total",
		Help: "resyncs that found the cluster revision below the watch cursor (etcd restored without bump-revision)",
	}, metricLabels))
	if err != nil {
		return nil, err
	}
	commitFailures, err := register(reg, prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "confsource_commit_failures_total",
		Help: "versions rejected before commit, by stage",
	}, []string{"service", "stage"}))
	if err != nil {
		return nil, err
	}
	eventsSkipped, err := register(reg, prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "confsource_events_skipped_total",
		Help: "watch events skipped because their ModRevision is not above the committed revision",
	}, metricLabels))
	if err != nil {
		return nil, err
	}
	metaLookup, err := register(reg, prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "confsource_meta_lookup_failures_total",
		Help: "publish_id lookups at an event's revision that failed, by reason",
	}, []string{"service", "reason"}))
	if err != nil {
		return nil, err
	}
	snapshotFail, err := register(reg, prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "confsource_snapshot_failures_total",
		Help: "local snapshot read or write failures",
	}, []string{"service", "op"}))
	if err != nil {
		return nil, err
	}
	loadDuration, err := register(reg, prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "confsource_load_duration_seconds",
		Help:    "duration of Load, whatever its outcome",
		Buckets: prometheus.DefBuckets,
	}, metricLabels))
	if err != nil {
		return nil, err
	}

	m := &metrics{
		service:        service,
		loadedRevision: loaded.WithLabelValues(service),
		degraded:       degraded.WithLabelValues(service),
		contentHash:    contentHash,
		lastSync:       lastSync,
		resync:         resync,
		regressed:      regressed.WithLabelValues(service),
		commitFailures: commitFailures,
		eventsSkipped:  eventsSkipped.WithLabelValues(service),
		metaLookup:     metaLookup,
		snapshotFail:   snapshotFail,
		loadDuration:   loadDuration.WithLabelValues(service),
	}
	// counter 的全部标签组合先建成 0，告警规则的 increase() 不会因为序列缺失而失效
	for _, reason := range []string{resyncStartup, resyncCompacted, resyncReconnect, resyncRegression} {
		resync.WithLabelValues(service, reason)
	}
	for _, stage := range []string{"validator", "oncommit"} {
		commitFailures.WithLabelValues(service, stage)
	}
	for _, reason := range []string{"compacted", "timeout", "other"} {
		metaLookup.WithLabelValues(service, reason)
	}
	for _, op := range []string{"read", "write"} {
		snapshotFail.WithLabelValues(service, op)
	}
	return m, nil
}

// register 注册指标；同一进程里多个 Source 共用注册器时复用已注册的 collector
func register[T prometheus.Collector](reg prometheus.Registerer, c T) (T, error) {
	if err := reg.Register(c); err != nil {
		var already prometheus.AlreadyRegisteredError
		if errors.As(err, &already) {
			if existing, ok := already.ExistingCollector.(T); ok {
				return existing, nil
			}
		}
		var zero T
		return zero, err
	}
	return c, nil
}

// setContentHash 提交成功后换内容哈希序列：先删旧的再建新的，内容没变就什么都不做
func (m *metrics) setContentHash(hash string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.currentHash == hash {
		return
	}
	if m.currentHash != "" {
		m.contentHash.DeleteLabelValues(m.service, m.currentHash)
	}
	m.contentHash.WithLabelValues(m.service, hash).Set(1)
	m.currentHash = hash
}

// markSync 记录收到了一个有效的 etcd 响应；只看连接是否在同步，与这一版有没有提交成功无关
func (m *metrics) markSync() {
	m.lastSync.WithLabelValues(m.service).Set(float64(time.Now().UnixNano()) / 1e9)
}

func (m *metrics) countResync(reason string) { m.resync.WithLabelValues(m.service, reason).Inc() }

func (m *metrics) countCommitFailure(stage string) {
	m.commitFailures.WithLabelValues(m.service, stage).Inc()
}

func (m *metrics) countMetaLookupFailure(err error) {
	m.metaLookup.WithLabelValues(m.service, metaLookupReason(err)).Inc()
}

func (m *metrics) countSnapshotFailure(op string) {
	m.snapshotFail.WithLabelValues(m.service, op).Inc()
}

// metaLookupReason 把 meta 查询失败归到压缩、超时或其它
func metaLookupReason(err error) string {
	switch {
	case rpctypes.Error(err) == rpctypes.ErrCompacted:
		return "compacted"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	default:
		return "other"
	}
}
