package confsource_test

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stretchr/testify/require"

	"github.com/decodeex/bifu-confsource"
)

func sha(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func svc(extra ...string) map[string]string {
	m := map[string]string{"service": testService}
	for i := 0; i+1 < len(extra); i += 2 {
		m[extra[i]] = extra[i+1]
	}
	return m
}

// content_hash_info 只在提交成功后换值，任何时刻只有一条序列：连续发布、重复内容、校验被拒、回滚到旧内容
func TestMetrics_ContentHashFollowsCommits(t *testing.T) {
	env := newTestEnv(t)
	env.publish(yamlA, "pub-a")
	src, err := confsource.New(env.options(confsource.WithValidator(func(_ int64, raw []byte) error {
		if string(raw) == yamlC {
			return errors.New("yamlC rejected")
		}
		return nil
	}))...)
	require.NoError(t, err)
	defer src.Close()
	_, err = src.Load()
	require.NoError(t, err)
	require.Equal(t, []string{sha(yamlA)}, env.labelValues("confsource_content_hash_info", "hash"))
	v, ok := env.metric("confsource_content_hash_info", svc("hash", sha(yamlA)))
	require.True(t, ok)
	require.Equal(t, float64(1), v)

	w, err := src.Watch()
	require.NoError(t, err)
	sink := sinkOf(w)
	time.Sleep(200 * time.Millisecond)

	revB := env.publish(yamlB, "pub-b")
	sink.next(t, 5*time.Second)
	env.assertCommitted(src, revB, yamlB)
	require.Equal(t, []string{sha(yamlB)}, env.labelValues("confsource_content_hash_info", "hash"), "old hash series must be deleted")

	revB2 := env.publish(yamlB, "pub-b2")
	sink.next(t, 5*time.Second)
	env.assertCommitted(src, revB2, yamlB)
	require.Equal(t, []string{sha(yamlB)}, env.labelValues("confsource_content_hash_info", "hash"), "same content keeps a single series")

	env.publish(yamlC, "pub-c")
	require.Eventually(t, func() bool { return env.counter("confsource_commit_failures_total", svc("stage", "validator")) == 1 }, 5*time.Second, 20*time.Millisecond)
	require.Equal(t, []string{sha(yamlB)}, env.labelValues("confsource_content_hash_info", "hash"), "rejected version must not change the hash")
	require.Equal(t, float64(0), env.counter("confsource_commit_failures_total", svc("stage", "oncommit")))

	revA2 := env.publish(yamlA, "pub-rollback")
	sink.next(t, 5*time.Second)
	env.assertCommitted(src, revA2, yamlA)
	require.Equal(t, []string{sha(yamlA)}, env.labelValues("confsource_content_hash_info", "hash"), "rollback to old content exposes only the old hash")
}

// last_sync 只由有效的 etcd 响应推进：快照启动时没有序列；startup resync、事件、进度通知各推一次；etcd 停了就不动。
// 进度通知有个前提：etcd 只发给 rev ≥ startRev 的 watcher，而 Watch 起点是 cursor+1，所以集群自建 Watch 起没有任何写入时
// 不会有进度通知，last_sync 停在上一次响应；任意 key 写一次之后就持续推进
func TestMetrics_LastSyncOnlyFromEtcdResponses(t *testing.T) {
	env := newTestEnv(t)
	env.progressNotify = 200 * time.Millisecond
	env.restart()
	env.publish(yamlA, "pub-a")
	warm, err := confsource.New(env.options()...)
	require.NoError(t, err)
	_, err = warm.Load()
	require.NoError(t, err)
	require.NoError(t, warm.Close())
	env.registry = prometheus.NewRegistry()
	env.stop()

	src, err := confsource.New(env.options()...)
	require.NoError(t, err)
	defer src.Close()
	_, err = src.Load()
	require.NoError(t, err)
	require.True(t, src.Degraded())
	_, ok := env.metric("confsource_last_sync_timestamp_seconds", svc())
	require.False(t, ok, "snapshot startup is not an etcd sync, the series must be absent")
	w, err := src.Watch()
	require.NoError(t, err)
	sink := sinkOf(w)
	time.Sleep(500 * time.Millisecond)
	_, ok = env.metric("confsource_last_sync_timestamp_seconds", svc())
	require.False(t, ok, "failed resync attempts must not touch last_sync")

	env.start()
	require.Eventually(t, func() bool { return !src.Degraded() }, 10*time.Second, 50*time.Millisecond)
	first, ok := env.metric("confsource_last_sync_timestamp_seconds", svc())
	require.True(t, ok)
	require.InDelta(t, float64(time.Now().Unix()), first, 5)
	require.Equal(t, float64(1), env.counter("confsource_resync_total", svc("reason", "startup")))

	time.Sleep(700 * time.Millisecond)
	idle, _ := env.metric("confsource_last_sync_timestamp_seconds", svc())
	require.Equal(t, first, idle, "no write in the cluster since the watch started: etcd sends no progress notification to a watcher ahead of its revision")

	env.bumpRevision(1)
	require.Eventually(t, func() bool {
		v, _ := env.metric("confsource_last_sync_timestamp_seconds", svc())
		return v > first
	}, 5*time.Second, 50*time.Millisecond, "once the cluster revision reached the watch start, progress notifications advance last_sync without any publish of this key")
	time.Sleep(500 * time.Millisecond)
	afterNotify, _ := env.metric("confsource_last_sync_timestamp_seconds", svc())
	require.Greater(t, afterNotify, first)

	env.publish(yamlB, "pub-b")
	sink.next(t, 5*time.Second)
	afterEvent, _ := env.metric("confsource_last_sync_timestamp_seconds", svc())
	require.GreaterOrEqual(t, afterEvent, afterNotify)

	env.stop()
	time.Sleep(300 * time.Millisecond)
	frozen, _ := env.metric("confsource_last_sync_timestamp_seconds", svc())
	time.Sleep(1 * time.Second)
	still, _ := env.metric("confsource_last_sync_timestamp_seconds", svc())
	require.Equal(t, frozen, still, "no etcd response, no movement")
}

// 首次从 etcd Load：last_sync 立即有值，Load 耗时直方图记一个样本
func TestMetrics_LoadFromEtcdSetsLastSyncAndDuration(t *testing.T) {
	env := newTestEnv(t)
	env.publish(yamlA, "pub-a")
	src, err := confsource.New(env.options()...)
	require.NoError(t, err)
	defer src.Close()
	_, err = src.Load()
	require.NoError(t, err)
	v, ok := env.metric("confsource_last_sync_timestamp_seconds", svc())
	require.True(t, ok)
	require.InDelta(t, float64(time.Now().Unix()), v, 5)
	samples, ok := env.metric("confsource_load_duration_seconds", svc())
	require.True(t, ok)
	require.Equal(t, float64(1), samples)
}

// resync 计数按完成的 resync 记一次、只记一个 reason：压缩触发记 compacted；检测到倒退的那次记 regression 而不是 reconnect，
// 倒退计数器同一次只加一
func TestMetrics_ResyncReasonsAndRegression(t *testing.T) {
	// Load 之后、建 Watch 之前把起点压掉：第一次 Watch 立刻收到 compacted，确定只走压缩这一条路
	t.Run("compacted", func(t *testing.T) {
		env := newTestEnv(t)
		env.publish(yamlA, "pub-a")
		src, err := confsource.New(env.options()...)
		require.NoError(t, err)
		defer src.Close()
		_, err = src.Load()
		require.NoError(t, err)
		head := env.bumpRevision(30)
		env.compact(head)
		w, err := src.Watch()
		require.NoError(t, err)
		require.Eventually(t, func() bool { return env.counter("confsource_resync_total", svc("reason", "compacted")) >= 1 }, 10*time.Second, 50*time.Millisecond)
		time.Sleep(1 * time.Second)
		require.Equal(t, float64(1), env.counter("confsource_resync_total", svc("reason", "compacted")))
		require.Equal(t, float64(0), env.counter("confsource_resync_total", svc("reason", "reconnect")))
		require.Equal(t, float64(0), env.counter("confsource_revision_regressed_total", svc()))
		revB := env.publish(yamlB, "pub-b")
		nextWithin(t, w, 5*time.Second)
		env.assertCommitted(src, revB, yamlB)
	})
	t.Run("regression", func(t *testing.T) {
		env := newClusterEnv(t, 2)
		env.bumpRevision(20)
		revA := env.publish(yamlA, "pub-a")
		src, err := confsource.New(env.options()...)
		require.NoError(t, err)
		defer src.Close()
		_, err = src.Load()
		require.NoError(t, err)
		_, err = src.Watch()
		require.NoError(t, err)
		time.Sleep(300 * time.Millisecond)

		env.restartFresh()
		env.publish(yamlC, "pub-old")
		env.stopMember(1)
		require.Eventually(t, func() bool { return env.logCount("no leader") >= 1 }, 30*time.Second, 100*time.Millisecond)
		env.startMember(1)
		env.waitLeader()
		require.Eventually(t, func() bool { return env.counter("confsource_revision_regressed_total", svc()) >= 1 }, 30*time.Second, 100*time.Millisecond)
		time.Sleep(1 * time.Second)
		require.Equal(t, float64(1), env.counter("confsource_revision_regressed_total", svc()), "one regression is counted once")
		require.Equal(t, float64(1), env.counter("confsource_resync_total", svc("reason", "regression")))
		require.LessOrEqual(t, env.counter("confsource_resync_total", svc("reason", "reconnect")), float64(1))
		require.Equal(t, revA, src.Revision())
	})
}

// 陈旧事件跳过计数与 meta 查询失败计数
func TestMetrics_SkippedEventsAndMetaLookupFailures(t *testing.T) {
	t.Run("skipped", func(t *testing.T) {
		env := newTestEnv(t)
		env.bumpRevision(20)
		env.publish(yamlA, "pub-a")
		warm, err := confsource.New(env.options()...)
		require.NoError(t, err)
		_, err = warm.Load()
		require.NoError(t, err)
		require.NoError(t, warm.Close())
		env.registry = prometheus.NewRegistry()
		env.stop()

		src, err := confsource.New(env.options()...)
		require.NoError(t, err)
		defer src.Close()
		_, err = src.Load()
		require.NoError(t, err)
		w, err := src.Watch()
		require.NoError(t, err)
		sink := sinkOf(w)
		env.dataDir = t.TempDir()
		env.start()
		env.publish(yamlC, "pub-old")
		require.Eventually(t, func() bool { return env.logCount("differs from committed") >= 1 }, 10*time.Second, 50*time.Millisecond)
		env.publish(yamlC, "pub-old-2")
		require.Eventually(t, func() bool { return env.counter("confsource_events_skipped_total", svc()) == 1 }, 10*time.Second, 50*time.Millisecond)
		sink.none(t, 500*time.Millisecond, "stale event must not be delivered")
	})
	t.Run("meta compacted", func(t *testing.T) {
		env := newTestEnv(t)
		env.publish(yamlA, "pub-a")
		src, err := confsource.New(env.options()...)
		require.NoError(t, err)
		defer src.Close()
		_, err = src.Load()
		require.NoError(t, err)
		w, err := src.Watch()
		require.NoError(t, err)
		time.Sleep(200 * time.Millisecond)

		env.publish(yamlB, "pub-b")
		time.Sleep(300 * time.Millisecond)
		env.publish(yamlC, "pub-c")
		head := env.publish(yamlA, "pub-a2")
		time.Sleep(300 * time.Millisecond)
		env.compact(head)
		for i := 0; i < 3; i++ {
			nextWithin(t, w, 5*time.Second)
		}
		// 第一个事件在压缩前就查过 meta；第二个的 revision 低于压缩点查不到；第三个的 revision 等于压缩点仍可读
		require.Equal(t, float64(1), env.counter("confsource_meta_lookup_failures_total", svc("reason", "compacted")))
		require.Equal(t, float64(0), env.counter("confsource_meta_lookup_failures_total", svc("reason", "timeout")))
	})
}

// 快照读写失败计数
func TestMetrics_SnapshotFailures(t *testing.T) {
	t.Run("write", func(t *testing.T) {
		env := newTestEnv(t)
		env.publish(yamlA, "pub-a")
		src, err := confsource.New(env.options()...)
		require.NoError(t, err)
		defer src.Close()
		_, err = src.Load()
		require.NoError(t, err)
		w, err := src.Watch()
		require.NoError(t, err)
		time.Sleep(200 * time.Millisecond)
		env.breakSnapshotDir()
		defer env.restoreSnapshotDir()
		env.publish(yamlB, "pub-b")
		nextWithin(t, w, 5*time.Second)
		require.Equal(t, float64(1), env.counter("confsource_snapshot_failures_total", svc("op", "write")))
		require.Equal(t, float64(0), env.counter("confsource_snapshot_failures_total", svc("op", "read")))
	})
	t.Run("read", func(t *testing.T) {
		env := newTestEnv(t)
		env.publish(yamlA, "pub-a")
		warm, err := confsource.New(env.options()...)
		require.NoError(t, err)
		_, err = warm.Load()
		require.NoError(t, err)
		require.NoError(t, warm.Close())
		overwriteSnapshot(t, env, yamlB)
		env.registry = prometheus.NewRegistry()
		env.stop()
		src, err := confsource.New(env.options()...)
		require.NoError(t, err)
		defer src.Close()
		_, err = src.Load()
		require.ErrorIs(t, err, confsource.ErrNoConfig)
		require.Equal(t, float64(1), env.counter("confsource_snapshot_failures_total", svc("op", "read")))
	})
}

// 同一个注册器被多个 Source 共用：不同服务各自一套序列，同服务的副本不报重复注册；
// 新建 Source 时每个 counter 的全部标签组合都已存在且为 0，告警规则的 increase() 不会因为序列缺失失效
func TestMetrics_SharedRegistryAndPreinitializedSeries(t *testing.T) {
	env := newTestEnv(t)
	env.publish(yamlA, "pub-a")
	a, err := confsource.New(env.options()...)
	require.NoError(t, err)
	defer a.Close()
	b, err := confsource.New(env.options(confsource.WithService("other"))...)
	require.NoError(t, err)
	defer b.Close()
	replica, err := confsource.New(env.options()...)
	require.NoError(t, err)
	defer replica.Close()
	_, err = a.Load()
	require.NoError(t, err)

	for name, labels := range map[string]map[string]string{
		"confsource_resync_total":               svc("reason", "reconnect"),
		"confsource_revision_regressed_total":   svc(),
		"confsource_commit_failures_total":      svc("stage", "oncommit"),
		"confsource_events_skipped_total":       svc(),
		"confsource_meta_lookup_failures_total": svc("reason", "timeout"),
		"confsource_snapshot_failures_total":    svc("op", "read"),
	} {
		v, ok := env.metric(name, labels)
		require.True(t, ok, "%s %v must be pre-initialized", name, labels)
		require.Zero(t, v)
	}
	v, ok := env.metric("confsource_loaded_revision", map[string]string{"service": "other"})
	require.True(t, ok)
	require.Zero(t, v, "the other service's gauge is independent")
	_, ok = env.metric("confsource_content_hash_info", map[string]string{"service": "other"})
	require.False(t, ok, "no commit, no hash series for the other service")
}

// 实际的 /metrics 文本输出：11 个指标全部暴露、名称类型如表，内容哈希只有一条
func TestMetrics_PrometheusExposition(t *testing.T) {
	env := newTestEnv(t)
	env.publish(yamlA, "pub-a")
	src, err := confsource.New(env.options()...)
	require.NoError(t, err)
	defer src.Close()
	_, err = src.Load()
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	promhttp.HandlerFor(env.registry, promhttp.HandlerOpts{}).ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body := rec.Body.String()
	for _, line := range []string{
		"# TYPE confsource_loaded_revision gauge",
		"# TYPE confsource_degraded gauge",
		"# TYPE confsource_content_hash_info gauge",
		"# TYPE confsource_last_sync_timestamp_seconds gauge",
		"# TYPE confsource_resync_total counter",
		"# TYPE confsource_revision_regressed_total counter",
		"# TYPE confsource_commit_failures_total counter",
		"# TYPE confsource_events_skipped_total counter",
		"# TYPE confsource_meta_lookup_failures_total counter",
		"# TYPE confsource_snapshot_failures_total counter",
		"# TYPE confsource_load_duration_seconds histogram",
		fmt.Sprintf(`confsource_content_hash_info{hash="%s",service="%s"} 1`, sha(yamlA), testService),
		`confsource_resync_total{reason="compacted",service="content"} 0`,
		`confsource_resync_total{reason="reconnect",service="content"} 0`,
		`confsource_resync_total{reason="regression",service="content"} 0`,
		`confsource_resync_total{reason="startup",service="content"} 0`,
		`confsource_commit_failures_total{service="content",stage="validator"} 0`,
		`confsource_snapshot_failures_total{op="write",service="content"} 0`,
		`confsource_load_duration_seconds_count{service="content"} 1`,
	} {
		require.Contains(t, body, line)
	}
	require.NotContains(t, body, "timeout: 1", "config content must never appear in metrics")
	require.Equal(t, 1, countOccurrences(body, "confsource_content_hash_info{"))
}

// 采集与提交并发：race 检测下反复 Gather，同时连续发布
func TestMetrics_ConcurrentScrape(t *testing.T) {
	env := newTestEnv(t)
	env.publish(yamlA, "pub-a")
	src, err := confsource.New(env.options()...)
	require.NoError(t, err)
	defer src.Close()
	_, err = src.Load()
	require.NoError(t, err)
	w, err := src.Watch()
	require.NoError(t, err)
	sink := sinkOf(w)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_, err := env.registry.Gather()
				if err != nil {
					t.Error(err)
					return
				}
			}
		}
	}()
	for i := 0; i < 10; i++ {
		y := yamlA
		if i%2 == 1 {
			y = yamlB
		}
		env.publish(y, fmt.Sprintf("pub-%d", i))
		sink.next(t, 5*time.Second)
	}
	close(stop)
	wg.Wait()
	require.Len(t, env.labelValues("confsource_content_hash_info", "hash"), 1)
}

func countOccurrences(s, sub string) int {
	n := 0
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			n++
		}
	}
	return n
}
