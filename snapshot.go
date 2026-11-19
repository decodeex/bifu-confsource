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

// read 读快照。只有 yaml 可读、.meta 可解析且 content_hash 与 yaml 现算哈希相等时才算有快照；
// 任一不成立都按无快照返回 ErrSnapshotMissing 并带上原因，内容不交给调用方。
// 读在共享锁下进行：写方持排他锁连做两次 rename，读方不会看到「yaml 新、.meta 旧」的半成品
func (s *snapshotStore) read() (raw []byte, rev int64, err error) {
	unlock, err := s.lock(syscall.LOCK_SH)
	if err != nil {
		return nil, 0, fmt.Errorf("%w: 加锁失败: %v", ErrSnapshotMissing, err)
	}
	defer unlock()

	raw, err = os.ReadFile(s.yamlPath)
	if err != nil {
		return nil, 0, fmt.Errorf("%w: yaml 缺失", ErrSnapshotMissing)
	}
	data, err := os.ReadFile(s.metaPath)
	if err != nil {
		return nil, 0, fmt.Errorf("%w: meta 缺失", ErrSnapshotMissing)
	}
	var meta snapshotMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, 0, fmt.Errorf("%w: meta 损坏", ErrSnapshotMissing)
	}
	if meta.ContentHash != contentHash(raw) {
		return nil, 0, fmt.Errorf("%w: hash 不配对", ErrSnapshotMissing)
	}
	return raw, meta.ModRevision, nil
}

// lock 对快照目录的锁文件加 flock，返回解锁函数；目录不存在时先建
func (s *snapshotStore) lock(how int) (func(), error) {
	if err := os.MkdirAll(filepath.Dir(s.lockPath), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(s.lockPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), how); err != nil {
		f.Close()
		return nil, err
	}
	return func() {
		syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		f.Close()
	}, nil
}

// write 在文件锁下写快照；revision 不高于磁盘现有版本时跳过，保证快照永不回退
func (s *snapshotStore) write(raw []byte, meta snapshotMeta) error {
	unlock, err := s.lock(syscall.LOCK_EX)
	if err != nil {
		return err
	}
	defer unlock()

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
