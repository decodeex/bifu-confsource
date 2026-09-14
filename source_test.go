package confsource_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/decodeex/bifu-confsource"
)

const (
	yamlA = "server:\n  timeout: 1\n"
	yamlB = "server:\n  timeout: 2\n"
	yamlC = "server:\n  timeout: 3\n"
)

func TestSource_NotPublishedIsNoConfig(t *testing.T) {
	env := newTestEnv(t)
	src, err := confsource.New(env.options()...)
	require.NoError(t, err)
	defer src.Close()

	_, err = src.Load()
	require.ErrorIs(t, err, confsource.ErrNoConfig)
}

func TestSource_EtcdDownWithoutSnapshotIsNoConfig(t *testing.T) {
	env := newTestEnv(t)
	env.stop()

	src, err := confsource.New(env.options()...)
	require.NoError(t, err, "client construction must not depend on etcd being reachable")
	defer src.Close()

	_, err = src.Load()
	require.ErrorIs(t, err, confsource.ErrNoConfig)
}

func TestSource_ValidatorFailureFailsLoad(t *testing.T) {
	env := newTestEnv(t)
	env.publish(yamlA, "pub-a")

	reject := errors.New("bad config")
	src, err := confsource.New(env.options(confsource.WithValidator(func(int64, []byte) error { return reject }))...)
	require.NoError(t, err)
	defer src.Close()

	_, err = src.Load()
	require.ErrorIs(t, err, reject)
	require.Empty(t, env.snapshot())
}

// etcd 不可达时用快照降级启动，恢复后全量对齐并退出降级
func TestSource_SnapshotFallbackAndRecovery(t *testing.T) {
	env := newTestEnv(t)
	revA := env.publish(yamlA, "pub-a")

	warm, err := confsource.New(env.options()...)
	require.NoError(t, err)
	_, err = warm.Load()
	require.NoError(t, err)
	require.NoError(t, warm.Close())
	require.Equal(t, yamlA, env.snapshot())

	env.stop()

	src, err := confsource.New(env.options()...)
	require.NoError(t, err)
	defer src.Close()

	kvs, err := src.Load()
	require.NoError(t, err)
	require.Equal(t, yamlA, string(kvs[0].Value))
	require.True(t, src.Degraded())
	require.Equal(t, revA, src.Revision())
	require.Equal(t, float64(1), env.gauge("confsource_degraded"))

	w, err := src.Watch()
	require.NoError(t, err)
	time.Sleep(300 * time.Millisecond)
	require.True(t, src.Degraded(), "must stay degraded while etcd is down")

	env.restart()
	revB := env.publish(yamlB, "pub-b")

	kvs = nextWithin(t, w, 10*time.Second)
	require.Equal(t, yamlB, string(kvs[0].Value))
	require.Eventually(t, func() bool { return !src.Degraded() }, 5*time.Second, 20*time.Millisecond)
	env.assertCommitted(src, revB, yamlB)
}

// 快照与 meta 不配对时仍可启动，但 revision 视为 0，恢复后强制全量拉取
func TestSource_UnpairedSnapshotStartsWithRevisionZero(t *testing.T) {
	env := newTestEnv(t)
	env.publish(yamlA, "pub-a")

	warm, err := confsource.New(env.options()...)
	require.NoError(t, err)
	_, err = warm.Load()
	require.NoError(t, err)
	require.NoError(t, warm.Close())

	revB := env.publish(yamlB, "pub-b")
	env.stop()
	overwriteSnapshot(t, env, yamlC)

	src, err := confsource.New(env.options()...)
	require.NoError(t, err)
	defer src.Close()

	kvs, err := src.Load()
	require.NoError(t, err)
	require.Equal(t, yamlC, string(kvs[0].Value))
	require.Zero(t, src.Revision())
	require.True(t, src.Degraded())

	w, err := src.Watch()
	require.NoError(t, err)
	env.restart()

	kvs = nextWithin(t, w, 10*time.Second)
	require.Equal(t, yamlB, string(kvs[0].Value), "recovery must fetch the real current config")
	require.Eventually(t, func() bool { return !src.Degraded() }, 5*time.Second, 20*time.Millisecond)
	env.assertCommitted(src, revB, yamlB)
}

// Watch 起点已被 compaction 清掉时，改为全量拉取当前值再从最新 revision 续订
func TestSource_CompactedWatchReloadsCurrent(t *testing.T) {
	env := newTestEnv(t)
	revA := env.publish(yamlA, "pub-a")

	src, err := confsource.New(env.options()...)
	require.NoError(t, err)
	defer src.Close()
	_, err = src.Load()
	require.NoError(t, err)
	require.Equal(t, revA, src.Revision())

	env.publish(yamlB, "pub-b")
	revC := env.publish(yamlC, "pub-c")
	env.compact(revC)

	w, err := src.Watch()
	require.NoError(t, err)
	kvs := nextWithin(t, w, 10*time.Second)
	require.Equal(t, yamlC, string(kvs[0].Value))
	env.assertCommitted(src, revC, yamlC)
	require.Contains(t, env.logs.String(), "compacted")

	revD := env.publish("server:\n  timeout: 4\n", "pub-d")
	kvs = nextWithin(t, w, 10*time.Second)
	require.Equal(t, revD, src.Revision(), "watch must continue from the reloaded revision")
	require.Contains(t, string(kvs[0].Value), "timeout: 4")
}

func TestSource_DeleteEventIsIgnored(t *testing.T) {
	env := newTestEnv(t)
	revA := env.publish(yamlA, "pub-a")

	src, err := confsource.New(env.options()...)
	require.NoError(t, err)
	defer src.Close()
	_, err = src.Load()
	require.NoError(t, err)
	w, err := src.Watch()
	require.NoError(t, err)

	env.deleteConfig()
	require.Eventually(t, func() bool { return len(env.logs.String()) > 0 }, 5*time.Second, 20*time.Millisecond)
	require.Contains(t, env.logs.String(), "deleted in etcd")
	require.Equal(t, revA, src.Revision())
	require.Equal(t, yamlA, env.snapshot())

	revB := env.publish(yamlB, "pub-b")
	kvs := nextWithin(t, w, 10*time.Second)
	require.Equal(t, yamlB, string(kvs[0].Value))
	env.assertCommitted(src, revB, yamlB)
}

func TestSource_WatchOnlyOnce(t *testing.T) {
	env := newTestEnv(t)
	env.publish(yamlA, "pub-a")

	src, err := confsource.New(env.options()...)
	require.NoError(t, err)
	defer src.Close()
	_, err = src.Load()
	require.NoError(t, err)

	_, err = src.Watch()
	require.NoError(t, err)
	_, err = src.Watch()
	require.ErrorIs(t, err, confsource.ErrWatchStarted)
}

func TestSource_RequiresEndpointsEnvService(t *testing.T) {
	_, err := confsource.New(confsource.WithEnv("dev"), confsource.WithService("x"))
	require.ErrorContains(t, err, "endpoints required")
	_, err = confsource.New(confsource.WithEndpoints("http://127.0.0.1:1"), confsource.WithEnv("dev"))
	require.ErrorContains(t, err, "service required")
	_, err = confsource.New(confsource.WithEndpoints("http://127.0.0.1:1"), confsource.WithEnv("qa"), confsource.WithService("x"))
	require.ErrorContains(t, err, `unknown env "qa"`)
}
