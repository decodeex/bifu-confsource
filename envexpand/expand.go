// Package envexpand 严格模式的 ${VAR} 展开：任何被引用而未设置的变量都报错，
// 不再像 os.ExpandEnv 那样静默展开成空字符串。
package envexpand

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// MissingError 一次列出全部未设置的变量名，便于按清单补齐
type MissingError struct {
	Names []string
}

func (e *MissingError) Error() string {
	return fmt.Sprintf("envexpand: missing variables: %s", strings.Join(e.Names, ", "))
}

// Lookup 变量查找函数，签名与 os.LookupEnv 一致
type Lookup func(name string) (string, bool)

// Expand 对纯文本做 ${VAR} 与 $VAR 展开。注意它不理解 YAML，注释里的 $ 也会被当成变量；
// 配置文件请用 UnmarshalYAML。
func Expand(raw []byte, lookup Lookup) ([]byte, error) {
	e := expander{lookup: lookup}
	out := e.expand(string(raw))
	if err := e.err(); err != nil {
		return nil, err
	}
	return []byte(out), nil
}

// UnmarshalYAML 先解析 YAML，再只对标量值展开 ${VAR} 与 $VAR，注释和 key 不受影响。
// 未加引号的标量展开后重新推断类型，"${PORT}" 展开成 8080 仍然是整数，与先替换再解析的老行为一致。
func UnmarshalYAML(raw []byte, lookup Lookup, out any) error {
	var doc yaml.Node
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return err
	}
	e := expander{lookup: lookup}
	e.expandNode(&doc)
	if err := e.err(); err != nil {
		return err
	}
	if doc.Kind == 0 {
		return nil
	}
	return doc.Decode(out)
}

// UnmarshalYAMLEnv 用进程环境变量展开
func UnmarshalYAMLEnv(raw []byte, out any) error {
	return UnmarshalYAML(raw, os.LookupEnv, out)
}

// ExpandEnv 用进程环境变量做纯文本展开
func ExpandEnv(raw []byte) ([]byte, error) {
	return Expand(raw, os.LookupEnv)
}

type expander struct {
	lookup  Lookup
	missing map[string]struct{}
}

func (e *expander) expand(s string) string {
	return os.Expand(s, func(name string) string {
		v, ok := e.lookup(name)
		if !ok {
			if e.missing == nil {
				e.missing = map[string]struct{}{}
			}
			e.missing[name] = struct{}{}
		}
		return v
	})
}

// expandNode 递归处理 YAML 节点树，映射只展开值不展开 key，别名指向的锚点只会被展开一次
func (e *expander) expandNode(n *yaml.Node) {
	switch n.Kind {
	case yaml.DocumentNode, yaml.SequenceNode:
		for _, c := range n.Content {
			e.expandNode(c)
		}
	case yaml.MappingNode:
		for i := 1; i < len(n.Content); i += 2 {
			e.expandNode(n.Content[i])
		}
	case yaml.ScalarNode:
		e.expandScalar(n)
	}
}

// 带引号或块标量已被作者明确指定为字符串，展开后保持字符串；纯标量则清掉标签让解码时重新推断类型
func (e *expander) expandScalar(n *yaml.Node) {
	if n.Tag != "!!str" || !strings.Contains(n.Value, "$") {
		return
	}
	n.Value = e.expand(n.Value)
	if n.Style&(yaml.DoubleQuotedStyle|yaml.SingleQuotedStyle|yaml.LiteralStyle|yaml.FoldedStyle) == 0 {
		n.Tag = ""
	}
}

func (e *expander) err() error {
	if len(e.missing) == 0 {
		return nil
	}
	names := make([]string, 0, len(e.missing))
	for name := range e.missing {
		names = append(names, name)
	}
	sort.Strings(names)
	return &MissingError{Names: names}
}
