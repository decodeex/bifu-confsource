package confsource

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

// snapshotMeta 快照伴随文件，用于校验 yaml 与 meta 是否配对并恢复 revision
type snapshotMeta struct {
	ModRevision int64     `json:"mod_revision"`
	PublishID   string    `json:"publish_id"`
	ContentHash string    `json:"content_hash"`
	SavedAt     time.Time `json:"saved_at"`
}

// snapshotStore 节点级本地快照，同节点上同一服务的多个副本共享同一路径
type snapshotStore struct {
	yamlPath string
	metaPath string
	lockPath string
}

// 一个节点只属于一个环境的集群，路径不需要环境段
func newSnapshotStore(dir string, service string) *snapshotStore {
	base := filepath.Join(dir, service)
	return &snapshotStore{
		yamlPath: base + ".yaml",
		metaPath: base + ".yaml.meta",
		lockPath: base + ".lock",
	}
}

// read 读快照。meta 缺失、损坏或与 yaml 不配对时仍返回内容，但 revision 视为 0，
// 调用方据此在重连后强制全量拉取而不是按旧 revision 续订。
func (s *snapshotStore) read() (raw []byte, rev int64, err error) {
	raw, err = os.ReadFile(s.yamlPath)
	if err != nil {
		return nil, 0, ErrSnapshotMissing
	}
	meta, ok := s.readMeta()
	if !ok || meta.ContentHash != contentHash(raw) {
		return raw, 0, nil
	}
	return raw, meta.ModRevision, nil
}

// write 在文件锁下写快照；revision 不高于磁盘现有版本时跳过，保证快照永不回退
func (s *snapshotStore) write(raw []byte, meta snapshotMeta) error {
	if err := os.MkdirAll(filepath.Dir(s.yamlPath), 0o755); err != nil {
		return err
	}
	lock, err := os.OpenFile(s.lockPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		return err
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)

	if cur, ok := s.readMeta(); ok && meta.ModRevision <= cur.ModRevision {
		return nil
	}
	meta.ContentHash = contentHash(raw)
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	if err := writeAtomic(s.yamlPath, raw); err != nil {
		return err
	}
	return writeAtomic(s.metaPath, metaBytes)
}

func (s *snapshotStore) readMeta() (snapshotMeta, bool) {
	var meta snapshotMeta
	data, err := os.ReadFile(s.metaPath)
	if err != nil || json.Unmarshal(data, &meta) != nil {
		return snapshotMeta{}, false
	}
	return meta, true
}

// writeAtomic 先写带 pid 与随机串的临时文件并 fsync，再 rename 覆盖，读方永远只见完整文件
func writeAtomic(path string, data []byte) error {
	tmp := fmt.Sprintf("%s.tmp.%d.%d", path, os.Getpid(), rand.Uint32())
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err = f.Write(data); err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

func contentHash(raw []byte) string {
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}
