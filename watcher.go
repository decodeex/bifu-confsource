package confsource

import (
	"context"
	"errors"
	"time"

	"github.com/go-kratos/kratos/v2/config"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// watcher 串行事件循环。事件按 revision 顺序逐个校验、落快照、交付，
// 前一版被 Kratos 取走之前不处理下一版，因此 Kratos 看到的顺序与 etcd 一致。
type watcher struct {
	s      *Source
	ctx    context.Context
	cancel context.CancelFunc
	events chan []*config.KeyValue
}

func newWatcher(s *Source) *watcher {
	ctx, cancel := context.WithCancel(s.ctx)
	w := &watcher{s: s, ctx: ctx, cancel: cancel, events: make(chan []*config.KeyValue)}
	go w.run()
	return w
}

// Next 阻塞直到有一版通过校验的配置可交付
func (w *watcher) Next() ([]*config.KeyValue, error) {
	select {
	case kvs := <-w.events:
		return kvs, nil
	case <-w.ctx.Done():
		return nil, w.ctx.Err()
	}
}

func (w *watcher) Stop() error {
	w.cancel()
	return nil
}

func (w *watcher) run() {
	wait := newBackoff(w.s.opts.backoffMin, w.s.opts.backoffMax)
	needResync := w.s.Degraded() || w.s.Revision() == 0
	for w.ctx.Err() == nil {
		if needResync {
			if err := w.resync(); err != nil {
				w.s.log.Warnf("resync failed, retry in %s: %v", wait.current(), err)
				wait.sleep(w.ctx)
				continue
			}
			needResync = false
			wait.reset()
		}

		compacted, err := w.watch(wait)
		if w.ctx.Err() != nil {
			return
		}
		if compacted {
			w.s.log.Warnf("watch revision compacted, reloading full config")
			needResync = true
			continue
		}
		w.s.log.Warnf("watch interrupted, reconnect in %s: %v", wait.current(), err)
		wait.sleep(w.ctx)
	}
}

// resync 全量拉取并交付，用于降级恢复与 compaction 后重建 Watch 起点。
// 只有拉到、校验过、写完快照且 Kratos 取走后才算恢复。
func (w *watcher) resync() error {
	raw, rev, publishID, err := w.s.fetch(w.ctx)
	if err != nil {
		return err
	}
	if rev > w.s.Revision() || w.s.Revision() == 0 {
		if err := w.s.commit(rev, raw, publishID); err != nil {
			return err
		}
		if !w.deliver(raw) {
			return w.ctx.Err()
		}
	}
	w.s.setDegraded(false)
	return nil
}

// watch 从已提交 revision 的下一版开始监听；返回 compacted=true 表示起点已被压缩，需要全量重拉
func (w *watcher) watch(wait *backoff) (compacted bool, err error) {
	ctx, cancel := context.WithCancel(w.ctx)
	defer cancel()
	for resp := range w.s.client.Watch(ctx, w.s.configKey, clientv3.WithRev(w.s.Revision()+1)) {
		if resp.CompactRevision != 0 || errors.Is(resp.Err(), rpctypes.ErrCompacted) {
			return true, resp.Err()
		}
		if err := resp.Err(); err != nil {
			return false, err
		}
		wait.reset()
		for _, ev := range resp.Events {
			switch {
			case ev.Type == clientv3.EventTypeDelete:
				w.s.log.Errorf("config key deleted in etcd, ignoring and keeping revision %d", w.s.Revision())
			case ev.Kv.ModRevision <= w.s.Revision():
				w.s.log.Debugf("skip stale revision %d, committed %d", ev.Kv.ModRevision, w.s.Revision())
			default:
				if !w.apply(ev.Kv.ModRevision, ev.Kv.Value) {
					return false, w.ctx.Err()
				}
			}
		}
	}
	return false, errors.New("watch channel closed")
}

// apply 校验并交付一个 Put 事件；校验失败只告警，内存与快照停在上一版，继续处理后续事件
func (w *watcher) apply(rev int64, raw []byte) bool {
	if err := w.s.commit(rev, raw, w.s.publishIDAt(rev)); err != nil {
		w.s.log.Errorf("%v; staying on revision %d", err, w.s.Revision())
		return true
	}
	return w.deliver(raw)
}

// deliver 把配置交给 Kratos，阻塞到被取走；返回 false 表示已停止
func (w *watcher) deliver(raw []byte) bool {
	select {
	case w.events <- w.s.keyValues(raw):
		return true
	case <-w.ctx.Done():
		return false
	}
}

// backoff 指数退避，每次 sleep 后翻倍直到上限
type backoff struct {
	min, max, cur time.Duration
}

func newBackoff(minWait, maxWait time.Duration) *backoff {
	return &backoff{min: minWait, max: maxWait, cur: minWait}
}

func (b *backoff) current() time.Duration { return b.cur }

func (b *backoff) reset() { b.cur = b.min }

func (b *backoff) sleep(ctx context.Context) {
	t := time.NewTimer(b.cur)
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
	}
	if b.cur = b.cur * 2; b.cur > b.max {
		b.cur = b.max
	}
}
