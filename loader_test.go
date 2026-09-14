package confsource_test

import (
	"encoding/json"
	"fmt"
	"sync/atomic"

	"github.com/go-kratos/kratos/v2/config"

	"github.com/decodeex/bifu-confsource"
	"github.com/decodeex/bifu-confsource/envexpand"
)

// loader 模拟 bifufx-api/utils/configuration 接入 etcd 源后的形态：
// 严格 decoder、整份替换合并、按顶层 key 注册观察者，回调里重新 Scan 并原子替换内存。
// 这就是原型要验证的「加载器改动」，通过后照搬进 bifufx-api。
type loader[T any] struct {
	source  *confsource.Source
	c       config.Config
	value   atomic.Value
	applied chan *T
}

func newLoader[T any](opts ...confsource.Option) (*loader[T], error) {
	l := &loader[T]{applied: make(chan *T, 64)}
	src, err := confsource.New(append(opts, confsource.WithValidator(validate[T]))...)
	if err != nil {
		return nil, err
	}
	c := config.New(
		config.WithSource(src),
		config.WithDecoder(strictDecoder),
		config.WithMergeFunc(replaceMerge),
	)
	if err := c.Load(); err != nil {
		src.Close()
		return nil, err
	}
	cfg := new(T)
	if err := c.Scan(cfg); err != nil {
		src.Close()
		return nil, err
	}
	l.source, l.c = src, c
	l.value.Store(cfg)

	var top map[string]any
	if err := c.Scan(&top); err != nil {
		src.Close()
		return nil, err
	}
	for key := range top {
		if err := c.Watch(key, func(string, config.Value) { l.rescan() }); err != nil {
			src.Close()
			return nil, err
		}
	}
	return l, nil
}

func (l *loader[T]) GetConfig() *T { return l.value.Load().(*T) }

func (l *loader[T]) Close() error {
	l.c.Close()
	return l.source.Close()
}

func (l *loader[T]) rescan() {
	cfg := new(T)
	if err := l.c.Scan(cfg); err != nil {
		return
	}
	l.value.Store(cfg)
	select {
	case l.applied <- cfg:
	default:
	}
}

// validate 提交前校验：严格展开加试解析到 T，和 Kratos Scan 走同一条 yaml → json → struct 路径
func validate[T any](_ int64, raw []byte) error {
	m, err := decode(raw)
	if err != nil {
		return err
	}
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, new(T))
}

func decode(raw []byte) (map[string]any, error) {
	var m map[string]any
	if err := envexpand.UnmarshalYAMLEnv(raw, &m); err != nil {
		return nil, err
	}
	return m, nil
}

func strictDecoder(kv *config.KeyValue, v map[string]any) error {
	m, err := decode(kv.Value)
	if err != nil {
		return err
	}
	for k, val := range m {
		v[k] = val
	}
	return nil
}

// replaceMerge 单来源单文件，整份替换而不是深合并，yaml 里删掉的 key 才会在运行期消失
func replaceMerge(dst, src any) error {
	target, ok := dst.(*map[string]any)
	if !ok {
		return fmt.Errorf("unexpected merge target %T", dst)
	}
	next, ok := src.(map[string]any)
	if !ok {
		return fmt.Errorf("unexpected merge source %T", src)
	}
	*target = next
	return nil
}
