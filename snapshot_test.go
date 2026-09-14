package confsource

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSnapshotNeverGoesBackwards(t *testing.T) {
	s := newSnapshotStore(t.TempDir(), EnvDevelop, "svc")

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

func TestSnapshotUnpairedMetaReadsAsRevisionZero(t *testing.T) {
	s := newSnapshotStore(t.TempDir(), EnvDevelop, "svc")
	require.NoError(t, s.write([]byte("v5"), snapshotMeta{ModRevision: 5}))
	require.NoError(t, os.WriteFile(s.yamlPath, []byte("tampered"), 0o644))

	raw, rev, err := s.read()
	require.NoError(t, err)
	require.Equal(t, "tampered", string(raw))
	require.Zero(t, rev)
}

func TestSnapshotMissing(t *testing.T) {
	s := newSnapshotStore(t.TempDir(), EnvDevelop, "svc")
	_, _, err := s.read()
	require.ErrorIs(t, err, ErrSnapshotMissing)
}

func TestSnapshotLeavesNoTempFiles(t *testing.T) {
	dir := t.TempDir()
	s := newSnapshotStore(dir, EnvDevelop, "svc")
	require.NoError(t, s.write([]byte("v1"), snapshotMeta{ModRevision: 1}))

	entries, err := os.ReadDir(dir + "/develop")
	require.NoError(t, err)
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	require.ElementsMatch(t, []string{"svc.yaml", "svc.yaml.meta", "svc.lock"}, names)
}
