package confsource

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSnapshotNeverGoesBackwards(t *testing.T) {
	s := newSnapshotStore(t.TempDir(), "svc")

	require.NoError(t, s.write([]byte("v5"), snapshotMeta{ModRevision: 5, PublishID: "p5"}))
	require.NoError(t, s.write([]byte("v3"), snapshotMeta{ModRevision: 3, PublishID: "p3"}))

	raw, rev, err := s.read()
	require.NoError(t, err)
	require.Equal(t, "v5", string(raw))
	require.EqualValues(t, 5, rev)

	meta, ok := s.readMeta()
	require.True(t, ok)
	require.Equal(t, "p5", meta.PublishID)
	require.Equal(t, contentHash([]byte("v5")), meta.ContentHash)
}

// 完整性校验不过一律视为无快照：内容不返回、错误带固定原因，四种损坏形态在日志里能区分
func TestSnapshotRejectsCorruptSnapshots(t *testing.T) {
	cases := []struct {
		name   string
		damage func(s *snapshotStore)
		reason string
	}{
		{"tampered yaml", func(s *snapshotStore) { require.NoError(t, os.WriteFile(s.yamlPath, []byte("tampered"), 0o644)) }, "hash 不配对"},
		{"truncated yaml", func(s *snapshotStore) { require.NoError(t, os.WriteFile(s.yamlPath, []byte("v"), 0o644)) }, "hash 不配对"},
		{"meta missing", func(s *snapshotStore) { require.NoError(t, os.Remove(s.metaPath)) }, "meta 缺失"},
		{"meta corrupt", func(s *snapshotStore) { require.NoError(t, os.WriteFile(s.metaPath, []byte("{not json"), 0o644)) }, "meta 损坏"},
		{"yaml missing", func(s *snapshotStore) { require.NoError(t, os.Remove(s.yamlPath)) }, "yaml 缺失"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := newSnapshotStore(t.TempDir(), "svc")
			require.NoError(t, s.write([]byte("v5"), snapshotMeta{ModRevision: 5}))
			c.damage(s)

			raw, rev, err := s.read()
			require.ErrorIs(t, err, ErrSnapshotMissing)
			require.Contains(t, err.Error(), c.reason)
			require.Nil(t, raw, "unverified content must not be handed out")
			require.Zero(t, rev)
		})
	}
}

// 同节点另一副本正在写快照时读：写方在 flock 下连做两次 rename，读方持共享锁，永远读不到「yaml 新、meta 旧」的半成品
func TestSnapshotReadNeverSeesTornWrite(t *testing.T) {
	s := newSnapshotStore(t.TempDir(), "svc")
	require.NoError(t, s.write([]byte("v1"), snapshotMeta{ModRevision: 1}))

	done := make(chan struct{})
	go func() {
		defer close(done)
		for rev := int64(2); rev <= 400; rev++ {
			body := []byte(fmt.Sprintf("v%d-%s", rev, strings.Repeat("x", int(rev%7))))
			_ = s.write(body, snapshotMeta{ModRevision: rev})
		}
	}()
	for i := 0; i < 2000; i++ {
		raw, rev, err := s.read()
		require.NoError(t, err, "a reader racing the writer must never see a torn snapshot")
		require.True(t, strings.HasPrefix(string(raw), fmt.Sprintf("v%d-", rev)) || (rev == 1 && string(raw) == "v1"), "content %q does not belong to revision %d", raw, rev)
	}
	<-done
}

func TestSnapshotMissing(t *testing.T) {
	s := newSnapshotStore(t.TempDir(), "svc")
	_, _, err := s.read()
	require.ErrorIs(t, err, ErrSnapshotMissing)
}

func TestSnapshotLeavesNoTempFiles(t *testing.T) {
	dir := t.TempDir()
	s := newSnapshotStore(dir, "svc")
	require.NoError(t, s.write([]byte("v1"), snapshotMeta{ModRevision: 1}))

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	require.ElementsMatch(t, []string{"svc.yaml", "svc.yaml.meta", "svc.lock"}, names)
}
