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
// 严格 decoder、整份替换合并、OnCommit 回调里 Scan 并原子替换内存。
// 运行时更新不依赖 Kratos 观察者，Kratos 只保留给直接用 config.Value 的业务代码。
// 这就是原型要验证的「加载器改动」，通过后照搬进 bifufx-api。
type loader[T any] struct {
	source  *confsource.Source
	c       config.Config
	value   atomic.Value
	applied chan *T
}

func newLoader[T any](opts ...confsource.Option) (*loader[T], error) {
	return newLoaderWithDecoder[T](strictDecoder, opts...)
}

// newLoaderWithDecoder 允许测试换掉 Kratos 侧的 decoder，用来制造「Kratos 合并失败但 OnCommit 成功」
func newLoaderWithDecoder[T any](decoder config.Decoder, opts ...confsource.Option) (*loader[T], error) {
	l := &loader[T]{applied: make(chan *T, 64)}
	src, err := confsource.New(append(opts,
		confsource.WithValidator(validate[T]),
		confsource.WithOnCommit(l.onCommit),
	)...)
	if err != nil {
		return nil, err
	}
	c := config.New(
		config.WithSource(src),
		config.WithDecoder(decoder),
		config.WithMergeFunc(replaceMerge),
	)
	if err := c.Load(); err != nil {
		src.Close()
		return nil, err
	}
	l.source, l.c = src, c
	return l, nil
}

func (l *loader[T]) GetConfig() *T {
	v := l.value.Load()
	if v == nil {
		return nil
	}
	return v.(*T)
}

func (l *loader[T]) Close() error {
	l.c.Close()
	return l.source.Close()
}

// onCommit 所有可能失败的步骤都在 Store 之前，Store 是最后一步，之后只做不会失败的通知
func (l *loader[T]) onCommit(_ int64, raw []byte) error {
	cfg, err := decodeStrict[T](raw)
	if err != nil {
		return err
	}
	l.value.Store(cfg)
	select {
	case l.applied <- cfg:
	default:
	}
	return nil
}

// validate 提交前校验：严格展开加试解析到 T，和 OnCommit 走同一条解析路径
func validate[T any](_ int64, raw []byte) error {
	_, err := decodeStrict[T](raw)
	return err
}

// decodeStrict yaml → 严格展开 → json → T，与 Kratos Scan 的类型转换路径一致
func decodeStrict[T any](raw []byte) (*T, error) {
	m, err := decode(raw)
	if err != nil {
		return nil, err
	}
	data, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	cfg := new(T)
	if err := json.Unmarshal(data, cfg); err != nil {
		return nil, err
	}
	return cfg, nil
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

// replaceMerge 单来源单文件，整份替换而不是深合并，yaml 里删掉的 key 才会在 Kratos 视图里消失
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
