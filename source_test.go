package confsource_test

import (
	"errors"
	"fmt"
	"github.com/go-kratos/kratos/v2/config"
	"strings"
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
func TestSource_UnpairedSnapshotIsNoConfig(t *testing.T) {
	env := newTestEnv(t)
	env.publish(yamlA, "pub-a")

	warm, err := confsource.New(env.options()...)
	require.NoError(t, err)
	_, err = warm.Load()
	require.NoError(t, err)
	require.NoError(t, warm.Close())

	env.publish(yamlB, "pub-b")
	env.stop()
	overwriteSnapshot(t, env, yamlC)

	validated := 0
	src, err := confsource.New(env.options(confsource.WithValidator(func(int64, []byte) error {
		validated++
		return nil
	}))...)
	require.NoError(t, err)
	defer src.Close()

	_, err = src.Load()
	require.ErrorIs(t, err, confsource.ErrNoConfig, "a snapshot whose hash does not pair with .meta is no snapshot")
	require.Contains(t, err.Error(), "hash 不配对")
	require.Zero(t, validated, "unverified content never reaches the Validator")
	require.Zero(t, src.Revision())
	require.False(t, src.Degraded(), "nothing was loaded, so nothing is degraded")
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

func TestSource_RequiresEndpointsAndService(t *testing.T) {
	_, err := confsource.New(confsource.WithService("x"))
	require.ErrorContains(t, err, "endpoints required")
	_, err = confsource.New(confsource.WithEndpoints("http://127.0.0.1:1"))
	require.ErrorContains(t, err, "service required")
}

// OnCommit 返回 error：快照、gauge、revision 都停在上一版，后续合法版本照常
func TestSource_OnCommitErrorKeepsPreviousEverywhere(t *testing.T) {
	env := newTestEnv(t)
	revA := env.publish(yamlA, "pub-a")

	src, err := confsource.New(env.options(confsource.WithOnCommit(func(_ int64, raw []byte) error {
		if string(raw) == yamlB {
			return errors.New("injected apply failure before store")
		}
		return nil
	}))...)
	require.NoError(t, err)
	defer src.Close()
	_, err = src.Load()
	require.NoError(t, err)
	w, err := src.Watch()
	require.NoError(t, err)
	env.assertCommitted(src, revA, yamlA)

	revB := env.publish(yamlB, "pub-b")
	require.Eventually(t, func() bool {
		return strings.Contains(env.logs.String(), fmt.Sprintf("revision %d apply failed", revB))
	}, 5*time.Second, 20*time.Millisecond)
	env.assertCommitted(src, revA, yamlA)

	revC := env.publish(yamlC, "pub-c")
	kvs := nextWithin(t, w, 10*time.Second)
	require.Equal(t, yamlC, string(kvs[0].Value))
	env.assertCommitted(src, revC, yamlC)
}

// OnCommit panic 按返回 error 处理，事件循环不中断
func TestSource_OnCommitPanicIsTreatedAsFailure(t *testing.T) {
	env := newTestEnv(t)
	revA := env.publish(yamlA, "pub-a")

	src, err := confsource.New(env.options(confsource.WithOnCommit(func(_ int64, raw []byte) error {
		if string(raw) == yamlB {
			panic("injected panic before store")
		}
		return nil
	}))...)
	require.NoError(t, err)
	defer src.Close()
	_, err = src.Load()
	require.NoError(t, err)
	w, err := src.Watch()
	require.NoError(t, err)

	revB := env.publish(yamlB, "pub-b")
	require.Eventually(t, func() bool {
		return strings.Contains(env.logs.String(), fmt.Sprintf("revision %d apply failed: on-commit panic", revB))
	}, 5*time.Second, 20*time.Millisecond)
	env.assertCommitted(src, revA, yamlA)

	revC := env.publish(yamlC, "pub-c")
	kvs := nextWithin(t, w, 10*time.Second)
	require.Equal(t, yamlC, string(kvs[0].Value))
	env.assertCommitted(src, revC, yamlC)
}

// 首次 Load 时 OnCommit 失败：Load 返回错误，不落快照，服务不得启动
func TestSource_OnCommitFailureFailsLoad(t *testing.T) {
	env := newTestEnv(t)
	env.publish(yamlA, "pub-a")

	src, err := confsource.New(env.options(confsource.WithOnCommit(func(int64, []byte) error {
		return errors.New("cannot apply")
	}))...)
	require.NoError(t, err)
	defer src.Close()

	_, err = src.Load()
	require.ErrorContains(t, err, "apply failed")
	require.Empty(t, env.snapshot())
	require.Zero(t, src.Revision())
}

// 同节点另一副本已把快照写到新版本，本实例提交同一版本时跳过写盘，但 OnCommit 仍要执行、revision 仍要推进
func TestSource_OnCommitRunsEvenWhenSnapshotAlreadyAhead(t *testing.T) {
	env := newTestEnv(t)
	revA := env.publish(yamlA, "pub-a")

	var committedRevs []int64
	src, err := confsource.New(env.options(confsource.WithOnCommit(func(rev int64, _ []byte) error {
		committedRevs = append(committedRevs, rev)
		return nil
	}))...)
	require.NoError(t, err)
	defer src.Close()
	_, err = src.Load()
	require.NoError(t, err)
	require.Equal(t, []int64{revA}, committedRevs)

	revB := env.publish(yamlB, "pub-b")
	replica, err := confsource.New(env.options()...)
	require.NoError(t, err)
	_, err = replica.Load()
	require.NoError(t, err)
	require.NoError(t, replica.Close())
	require.Equal(t, yamlB, env.snapshot(), "the other replica wrote the snapshot first")

	w, err := src.Watch()
	require.NoError(t, err)
	kvs := nextWithin(t, w, 10*time.Second)
	require.Equal(t, yamlB, string(kvs[0].Value))
	require.Equal(t, []int64{revA, revB}, committedRevs, "OnCommit must run even though the disk already had B")
	env.assertCommitted(src, revB, yamlB)
}

// 集群 revision 越过 Watch 起点被压缩、配置本身没有新版本：resync 一次就把游标抬到集群 revision，不在 ErrCompacted 与 resync 之间空转
func TestSource_CompactionBeyondCursorDoesNotSpin(t *testing.T) {
	env := newTestEnv(t)
	revA := env.publish(yamlA, "pub-a")
	src, err := confsource.New(env.options()...)
	require.NoError(t, err)
	defer src.Close()
	_, err = src.Load()
	require.NoError(t, err)
	w, err := src.Watch()
	require.NoError(t, err)
	time.Sleep(200 * time.Millisecond)

	head := env.bumpRevision(30)
	env.compact(head)
	// 已同步的 watcher 不受 compaction 影响，重启 etcd 让 Watch 重新注册，起点才会撞上压缩点
	env.restart()
	time.Sleep(3 * time.Second)

	require.Equal(t, revA, src.Revision(), "nothing new to commit")
	require.False(t, src.Degraded())
	require.LessOrEqual(t, env.logCount("compacted"), 2, "one resync should settle the cursor; more means spinning:\n%s", env.logs.String())

	revB := env.publish(yamlB, "pub-b")
	kvs := nextWithin(t, w, 10*time.Second)
	require.Equal(t, yamlB, string(kvs[0].Value))
	env.assertCommitted(src, revB, yamlB)
}

// 校验被拒的版本也推进监听游标：压缩越过它之后不会把同一个坏版本反复送回来，下一个合法版本照常生效
func TestSource_RejectedVersionStillAdvancesCursor(t *testing.T) {
	env := newTestEnv(t)
	revA := env.publish(yamlA, "pub-a")
	src, err := confsource.New(env.options(confsource.WithValidator(func(_ int64, raw []byte) error {
		if string(raw) == yamlB {
			return errors.New("yamlB rejected")
		}
		return nil
	}))...)
	require.NoError(t, err)
	defer src.Close()
	_, err = src.Load()
	require.NoError(t, err)
	w, err := src.Watch()
	require.NoError(t, err)
	time.Sleep(200 * time.Millisecond)

	revB := env.publish(yamlB, "pub-b")
	require.Eventually(t, func() bool { return env.logCount("staying on revision") >= 1 }, 5*time.Second, 20*time.Millisecond)
	require.Equal(t, revA, src.Revision(), "rejected version must not be committed")
	env.compact(revB)
	env.restart()
	time.Sleep(3 * time.Second)
	require.Equal(t, 1, env.logCount("staying on revision"), "the rejected version must not be re-delivered after compaction:\n%s", env.logs.String())
	require.LessOrEqual(t, env.logCount("compacted"), 2)

	revC := env.publish(yamlC, "pub-c")
	kvs := nextWithin(t, w, 10*time.Second)
	require.Equal(t, yamlC, string(kvs[0].Value))
	env.assertCommitted(src, revC, yamlC)
}

// etcd 被不带 bump 恢复到更早的状态（revision 倒退、内容不同）：内存与快照保持已提交的版本，倒退后的内容既不采用也不交付，循环不空转。
// 这条路径客户端自己发现不了（重新注册的 Watch 起点在未来，channel 不关），由平台对账第 6 行告警，恢复规程要求带 bump 恢复
func TestSource_FreshClusterBelowCommittedIsNotAdopted(t *testing.T) {
	env := newTestEnv(t)
	env.bumpRevision(20)
	revA := env.publish(yamlA, "pub-a")
	src, err := confsource.New(env.options()...)
	require.NoError(t, err)
	defer src.Close()
	_, err = src.Load()
	require.NoError(t, err)
	w, err := src.Watch()
	require.NoError(t, err)
	time.Sleep(200 * time.Millisecond)

	env.restartFresh()
	revOld := env.publish(yamlC, "pub-old")
	require.Less(t, revOld, revA, "the fresh cluster is behind the committed revision")
	select {
	case <-time.After(2 * time.Second):
	case kvs := <-deliveries(w):
		t.Fatalf("a version with ModRevision below the committed one must not be delivered: %q", kvs[0].Value)
	}
	require.Equal(t, revA, src.Revision(), "committed revision never goes backwards")
	require.Equal(t, yamlA, env.snapshot(), "snapshot keeps the committed content")
	require.False(t, src.Degraded())
	require.Zero(t, env.logCount("compacted"))
	require.Zero(t, env.logCount("resync failed"))
}

// 快照降级启动后 etcd 上是更老且不同的内容：不清 degraded、不采用；只有 etcd 给出更高的 ModRevision（带 bump 恢复后平台对齐）才提交并恢复
func TestSource_DegradedNotClearedByOlderDifferentContent(t *testing.T) {
	env := newTestEnv(t)
	env.bumpRevision(20)
	revA := env.publish(yamlA, "pub-a")
	warm, err := confsource.New(env.options()...)
	require.NoError(t, err)
	_, err = warm.Load()
	require.NoError(t, err)
	require.NoError(t, warm.Close())
	env.stop()

	src, err := confsource.New(env.options()...)
	require.NoError(t, err)
	defer src.Close()
	kvs, err := src.Load()
	require.NoError(t, err)
	require.Equal(t, yamlA, string(kvs[0].Value))
	require.True(t, src.Degraded())
	w, err := src.Watch()
	require.NoError(t, err)

	env.dataDir = t.TempDir()
	env.start()
	env.publish(yamlC, "pub-older")
	require.Eventually(t, func() bool { return env.logCount("differs from committed") >= 1 }, 10*time.Second, 50*time.Millisecond, "resync must notice the mismatch:\n%s", env.logs.String())
	time.Sleep(1 * time.Second)
	require.True(t, src.Degraded(), "different content at a lower revision is not a confirmation")
	require.Equal(t, revA, src.Revision())
	require.Equal(t, yamlA, env.snapshot())
	require.LessOrEqual(t, env.logCount("differs from committed"), 2, "mismatch is reported once per resync, not in a loop")

	env.bumpRevision(40)
	revNew := env.publish(yamlB, "pub-new")
	require.Greater(t, revNew, revA)
	got := nextWithin(t, w, 10*time.Second)
	require.Equal(t, yamlB, string(got[0].Value))
	require.Eventually(t, func() bool { return !src.Degraded() }, 5*time.Second, 20*time.Millisecond)
	env.assertCommitted(src, revNew, yamlB)
}

// 快照降级启动、etcd 回来后内容没变：resync 核对哈希一致即清 degraded，不重复提交
func TestSource_DegradedClearsWhenEtcdUnchanged(t *testing.T) {
	env := newTestEnv(t)
	revA := env.publish(yamlA, "pub-a")
	warm, err := confsource.New(env.options()...)
	require.NoError(t, err)
	_, err = warm.Load()
	require.NoError(t, err)
	require.NoError(t, warm.Close())
	env.stop()

	src, err := confsource.New(env.options()...)
	require.NoError(t, err)
	defer src.Close()
	_, err = src.Load()
	require.NoError(t, err)
	require.True(t, src.Degraded())
	w, err := src.Watch()
	require.NoError(t, err)

	env.restart()
	require.Eventually(t, func() bool { return !src.Degraded() }, 10*time.Second, 50*time.Millisecond)
	require.Equal(t, revA, src.Revision())
	select {
	case <-time.After(500 * time.Millisecond):
	case kvs := <-deliveries(w):
		t.Fatalf("unchanged content must not be re-delivered: %q", kvs[0].Value)
	}
}

// deliveries 把 Watcher 的下一次交付转成 channel，方便在 select 里等「不应该来的」事件
func deliveries(w config.Watcher) <-chan []*config.KeyValue {
	ch := make(chan []*config.KeyValue, 1)
	go func() {
		if kvs, err := w.Next(); err == nil {
			ch <- kvs
		}
	}()
	return ch
}
