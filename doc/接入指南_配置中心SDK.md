# bifu-confsource 接入指南

bifu-confsource 是统一配置平台的 Go 客户端 SDK。服务接入后，配置改为从配置平台读取：平台上发布一版，运行中的进程不用重启，内存里的配置就会换成新版；运行中 etcd 断开不影响服务，继续用内存里的配置。启动时 etcd 必须可用，当前版本的本机快照还起不到启动兜底作用（见[第 10 节](#10-当前版本的已知限制)）。

本文适用于自己读取 yaml 配置并解析成结构体的 Go 服务，Kratos 和非 Kratos 都可以。bifufx-api 仓库里的服务由 bifufx-api 统一接入，不需要看本文。

SDK 版本：`main` 分支 `0f285b7`，go.mod 里写作 `v0.0.0-20261029061230-0f285b75bbff`。

---

## 1 先了解这几件事

```
配置平台（每个环境一个）            etcd（每个环境一套）              你的服务（每个 Pod）
  编辑 → 提交 → 审批 → 发布  ──写入──▶  /config/{服务名}.yaml  ──监听──▶  SDK 校验 → 加载器解析成结构体
                                         /meta/{服务名}                    → 原子替换内存 → 写本机快照
```

- **一个服务一份 yaml**，存在 etcd 的 `/config/{服务名}.yaml`。每次发布都是整份替换：yaml 里删掉的字段，运行时会变回零值，不会留着旧值。
- **一个环境一套**：dev、staging、生产各有一个平台实例和一套 etcd。服务连哪套由部署时注入的地址决定，key 里不带环境。
- **密钥不进平台**：配置里只写 `${VAR}`，真值由 SRE 注入 Pod 环境变量，服务启动和每次更新时再展开。
- **坏配置不会生效**：变量缺失、类型解析不了的版本整份拒掉，内存和快照都停在上一版。
- **热更新只更新到 `GetConfig()`**：SDK 负责把新版换进内存，业务代码要在用的时候调用 `GetConfig()` 才能读到新值，见 [5.6](#56-业务代码怎么读到新配置)。

---

## 2 接入步骤总览

| 步骤 | 谁做 | 做什么 |
|---|---|---|
| 1 | 服务负责人 + 平台管理员 | 每个环境的平台上建服务、写配置、发布第一版（[第 3 节](#3-平台侧建服务和首次发布)） |
| 2 | 服务开发 | 按第 4 节整理配置，按第 5 节接 SDK 并打新镜像 |
| 3 | SRE | 注入环境变量、开 etcd 只读账号、挂快照目录、配置指标抓取（[第 6 节](#6-部署给-sre)） |
| 4 | 服务开发 + SRE | 先在 dev 验证，再上 staging 和生产（[第 7 节](#7-上线与验证)） |

**第 1 步必须先做**。某个环境的平台上还没发布过这个服务的配置时，服务在这个环境打开 etcd 模式会直接启动失败。

---

## 3 平台侧：建服务和首次发布

每个环境的平台都要各做一遍。

| 环境 | 平台地址 |
|---|---|
| dev | https://config-center.dev.decodebackoffice.com/ |
| staging | https://config-center-staging.decodebackoffice.com/ |
| 生产 | https://config-center.decodebackoffice.com/ |

平台只能经 Teleport 访问，用 Teleport 身份登录，没有单独的密码。平台不会自动建号：第一次使用前，要找平台管理员按你的 Teleport 用户名建好账号，并授予对应服务的权限，否则登录时会提示账号未开通。

1. **新增服务**：在「服务」页新增，需要平台管理员操作。
   - 服务名只能用小写字母、数字和 `-`，以字母或数字开头，长度 2–64，**建好后不能改**。
   - 服务名就是部署里的 `CONFIG_SERVICE`，也决定 etcd key `/config/{服务名}.yaml`。服务原来在 bifufx-configmaps 里有配置文件的，服务名要和文件名一致。
   - 负责人就是审批人，至少要设一个，否则没人能审批这个服务的发布。
2. **写配置**：进服务的「编辑」Tab 新建草稿，粘贴整份 yaml，写法见[第 4 节](#4-配置文件怎么写)。保存时平台会检查有没有明文密钥，不合规的保存不了。
3. **发布**：
   - dev：「提交并发布」，不用审批，直接写入 etcd。
   - staging、生产：「提交审批」，负责人在 Lark 审批，通过后自动发布。提交后内容就冻结了，要改得重新派生草稿。
4. **确认**：「当前配置」Tab 能看到这一版的 **etcd revision**，上线验证时要和服务暴露的指标对比。

以后改配置也走同样的流程。要回滚，就在「历史版本」里选一个旧版本生成回滚草稿，再走一遍发布。

---

## 4 配置文件怎么写

### 4.1 两种占位符

| 写法 | 用途 | 谁来替换、什么时候 | 例 |
|---|---|---|---|
| `{{ VAR }}` | 同一环境里多个服务共用、各环境取值不同的**非密钥**值，比如数据库主机 | 平台在**发布时**按本环境变量表替换成字面值，再写入 etcd | `addr: {{ DB_RW_HOST }}:3306` |
| `${VAR}` | **密钥**，以及 Pod 自己的环境变量 | 服务里的加载器在**启动和每次更新时**用 Pod 环境变量替换 | `password: ${MYSQL_PASSWORD}` |

`{{ VAR }}` 的变量在平台「变量」页维护。变量表不能存密钥，变量名像密钥的写不进去。

### 4.2 密钥的写法

保存草稿时，平台对字段名里带 `secret`、`password`、`passwd`、`token`、`api_key`、`access_key`、`private_key`、`app_secret`、`credential`、`sign_key`、`salt` 的字段（不区分大小写）做检查：

- 值是数字、布尔、空值：放行，比如 `token_ttl_sec: 900`。
- 值**整个就是**一个 `${VAR}`：放行，这是密钥唯一合法的写法。
- 其他字符串，包括明文、`{{ VAR }}`、拼接写法（如 `pre_${X}`）：**拒绝保存**。

字段名不像密钥的字段不受上面的限制，可以拼接，比如 MySQL DSN：`source: "${DB_USER}:${DB_PASSWORD}@tcp({{ DB_RW_HOST }}:3306)/app"`。

字段名不像密钥、但值看起来像密钥的（长串高熵、`AKIA…`、`-----BEGIN` 等），不会拒绝保存，会在审批时标出来提醒审批人。

新增一个密钥变量时，按平台报错提示的流程找 SRE 把真值放进密钥管理，并注入 Pod 环境变量。

### 4.3 `${VAR}` 的展开规则

- 只展开**值**，key 和注释里的 `$` 不处理。
- 不加引号的值展开后会重新判断类型：`db: ${REDIS_DB}` 展开成 `1` 还是整数。加了引号的保持字符串。
- `$VAR`（不带花括号）**也会被当成变量**。值里不要出现字面的 `$`，确实需要的话放进环境变量，再用 `${VAR}` 引用。
- **默认严格模式**：引用的变量只要有一个没在 Pod 环境里设置，这一版就整份拒掉，报错里会列出全部缺失的变量名。变量设成空字符串算已设置。可选的变量也要在部署里声明，可以留空。

### 4.4 结构体映射

加载器的解析路径是 yaml → json → 结构体，所以：

- 普通结构体用 `json` tag 对应 yaml 的 key。yaml 里有、结构体里没有的字段会被忽略。
- 结构体是 protobuf 消息（Kratos 常见的 `conf.Bootstrap`）时走 protojson，`5s` 这种 Duration 能直接解析。
- 普通结构体里的 `time.Duration` 接不了 `5s` 这种写法，需要自定义类型并实现 `UnmarshalJSON`。

### 4.5 从文件配置迁移要注意

- 原来从目录读多个 yaml 再合并的，要合成一份。
- 原来用 `os.ExpandEnv` 先整段替换再解析的：缺的变量会静默变成空串，注释里的 `$X` 也会被替换。换到 etcd 模式后，缺变量会直接报错。迁移前先把每个 `${VAR}` 对一遍部署里有没有。

---

## 5 代码接入

### 5.1 引依赖

```bash
go get github.com/decodeex/bifu-confsource@0f285b7
go mod tidy
```

- 需要 Go 1.24.1 及以上。
- 这是私有模块，`GOPRIVATE` 和拉取凭据的配置方式，和你们现在拉其他 `github.com/decodeex/*` 私有模块一样。CI 和 Docker 构建同样要能拉到。
- 会带进 `go.etcd.io/etcd/client/v3 v3.5.24`、`github.com/go-kratos/kratos/v2 v2.9.2`、`github.com/prometheus/client_golang v1.20.5` 等依赖。非 Kratos 服务也会引入 kratos 模块，因为 SDK 实现的是 Kratos 的配置源接口，业务代码不用管它。`go mod tidy` 之后用 `git diff go.mod` 看一下有没有原有依赖被抬了版本。
- go.mod 和 go.sum 一起提交。

### 5.2 选哪种写法

- **写法 A（默认）**：加载器直接调用 SDK，不经过 Kratos 的 `config.Config`。非 Kratos 服务用它，Kratos 服务如果只通过 `GetConfig()` 读配置也用它。
- **写法 B**：业务代码里有直接用 Kratos `config.Config` 的地方（`c.Value(...)`、`c.Watch(...)`）才用它，让那部分也跟着更新。

两种写法对外都是同一个 `GetConfig()`。下面的代码（含 5.3 的入口）已经在内嵌 etcd 上编译运行过，覆盖首次加载、连续热更新、缺变量的版本被拒、删字段变零值。

### 5.3 入口：按环境变量切换来源

文件模式保留服务原来的写法，加一个分支就行。没设 `CONFIG_SOURCE` 的环境行为完全不变，回退也靠它。

```go
// Configuration 服务配置，GetConfig 每次返回当前生效的一份
type Configuration[T any] interface {
	GetConfig() *T
	Close() error
}

// NewConfiguration 部署设了 CONFIG_SOURCE=etcd 时从配置平台读并跟随发布更新，否则沿用原来的文件读取
func NewConfiguration[T any](flagConf string) (Configuration[T], error) {
	if os.Getenv(envConfigSource) == sourceEtcd {
		return newEtcdConfiguration[T]()
	}
	return newFileConfiguration[T](flagConf)
}
```

- `newFileConfiguration` 换成服务原有的文件读取函数。本地开发不设 `CONFIG_SOURCE`，照旧读本地文件。
- 拿到的 `Configuration` 在进程退出前调用 `Close()`（比如 `defer cf.Close()`），停止监听并关闭 etcd 连接。

### 5.4 写法 A：直接用 SDK

新建 `etcd.go`，放在服务现有的配置加载包里：

```go
package configuration

import (
	"encoding/json"
	"fmt"
	"os"
	"sync/atomic"

	confsource "github.com/decodeex/bifu-confsource"
	"github.com/decodeex/bifu-confsource/envexpand"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// 部署注入的环境变量
const (
	envConfigSource  = "CONFIG_SOURCE"
	sourceEtcd       = "etcd"
	envEtcdEndpoints = "CONFIG_ETCD_ENDPOINTS"
	envEtcdUsername  = "CONFIG_ETCD_USERNAME"
	envEtcdPassword  = "CONFIG_ETCD_PASSWORD"
	envService       = "CONFIG_SERVICE"
	envSnapshotDir   = "CONFIG_SNAPSHOT_DIR"
	envStrictEnv     = "CONFIG_STRICT_ENV"
)

// etcdConfiguration 从配置平台读配置：SDK 负责拉取、监听、快照和降级，
// 这里负责把每一版原文展开 ${VAR}、解析成 T 并原子替换
type etcdConfiguration[T any] struct {
	source *confsource.Source
	value  atomic.Pointer[T]
	lookup envexpand.Lookup
}

// 返回接口类型：出错时调用方拿到的是真正的 nil，而不是装着 nil 指针的接口
func newEtcdConfiguration[T any]() (Configuration[T], error) {
	service := os.Getenv(envService)
	if service == "" {
		return nil, fmt.Errorf("configuration: %s is required when %s=%s", envService, envConfigSource, sourceEtcd)
	}
	m := &etcdConfiguration[T]{lookup: envLookup(os.Getenv(envStrictEnv) != "false")}
	opts := []confsource.Option{
		confsource.WithEndpoints(os.Getenv(envEtcdEndpoints)),
		confsource.WithService(service),
		confsource.WithValidator(m.validate),
		confsource.WithOnCommit(m.onCommit),
	}
	if username := os.Getenv(envEtcdUsername); username != "" {
		opts = append(opts, confsource.WithAuth(username, os.Getenv(envEtcdPassword)))
	}
	if dir := os.Getenv(envSnapshotDir); dir != "" {
		opts = append(opts, confsource.WithSnapshotDir(dir))
	}
	source, err := confsource.New(opts...)
	if err != nil {
		return nil, err
	}
	if _, err := source.Load(); err != nil {
		source.Close()
		return nil, err
	}
	watcher, err := source.Watch()
	if err != nil {
		source.Close()
		return nil, err
	}
	// SDK 交出一版后要等它被取走才处理下一版，这里一直取到 Close 为止
	go func() {
		for {
			if _, err := watcher.Next(); err != nil {
				return
			}
		}
	}()
	m.source = source
	return m, nil
}

func (m *etcdConfiguration[T]) GetConfig() *T { return m.value.Load() }

func (m *etcdConfiguration[T]) Close() error { return m.source.Close() }

// validate 和 onCommit 走同一条解析路径，解析不过的版本整份拒掉，内存和快照停在上一版
func (m *etcdConfiguration[T]) validate(_ int64, raw []byte) error {
	_, err := m.decode(raw)
	return err
}

// onCommit 可能失败的步骤都放在 Store 之前，Store 之后不能再返回错误
func (m *etcdConfiguration[T]) onCommit(_ int64, raw []byte) error {
	cfg, err := m.decode(raw)
	if err != nil {
		return err
	}
	m.value.Store(cfg)
	return nil
}

// parse 先解析 yaml，再只对值展开 ${VAR}，注释和 key 里的 $ 不受影响
func (m *etcdConfiguration[T]) parse(raw []byte) (map[string]any, error) {
	var doc map[string]any
	if err := envexpand.UnmarshalYAML(raw, m.lookup, &doc); err != nil {
		return nil, err
	}
	return doc, nil
}

// decode yaml → json → T。T 是 protobuf 消息时走 protojson，"5s" 这类 Duration 才解得进去
func (m *etcdConfiguration[T]) decode(raw []byte) (*T, error) {
	doc, err := m.parse(raw)
	if err != nil {
		return nil, err
	}
	data, err := json.Marshal(doc)
	if err != nil {
		return nil, err
	}
	cfg := new(T)
	if msg, ok := any(cfg).(proto.Message); ok {
		err = protojson.UnmarshalOptions{DiscardUnknown: true}.Unmarshal(data, msg)
	} else {
		err = json.Unmarshal(data, cfg)
	}
	if err != nil {
		return nil, err
	}
	return cfg, nil
}

// envLookup 严格模式下引用了没设置的变量就报错；宽松模式当空串处理
func envLookup(strict bool) envexpand.Lookup {
	if strict {
		return os.LookupEnv
	}
	return func(name string) (string, bool) { return os.Getenv(name), true }
}
```

服务原来的文件模式如果是先文本替换再解析，`decode` 里的 json → 结构体这一步最好和它共用同一个函数，两种模式解析出来的结果才会一致。

### 5.5 写法 B：经过 Kratos config

在写法 A 的基础上改四处。

结构体加一个字段：

```go
type etcdConfiguration[T any] struct {
	source       *confsource.Source
	kratosConfig config.Config // import "github.com/go-kratos/kratos/v2/config"
	value        atomic.Pointer[T]
	lookup       envexpand.Lookup
}
```

`newEtcdConfiguration` 里，从 `if _, err := source.Load(); err != nil {` 开始到函数结尾，整段换成：

```go
	// Kratos 的 Load 会调用 SDK 的 Load 和 Watch，并在后台一直取新版本，不用自己起协程
	c := config.New(
		config.WithSource(source),
		config.WithDecoder(m.kratosDecoder),
		config.WithMergeFunc(replaceMerge),
	)
	if err := c.Load(); err != nil {
		source.Close()
		return nil, err
	}
	m.source, m.kratosConfig = source, c
	return m, nil
```

`Close` 改成先关 Kratos：

```go
func (m *etcdConfiguration[T]) Close() error {
	if err := m.kratosConfig.Close(); err != nil {
		return err
	}
	return m.source.Close()
}
```

再加两个函数：

```go
// kratosDecoder 让 Kratos 视图里的 ${VAR} 也按同样规则展开
func (m *etcdConfiguration[T]) kratosDecoder(kv *config.KeyValue, v map[string]any) error {
	doc, err := m.parse(kv.Value)
	if err != nil {
		return err
	}
	for k, val := range doc {
		v[k] = val
	}
	return nil
}

// replaceMerge 整份替换而不是 Kratos 默认的深合并，yaml 里删掉的 key 才会真的消失
func replaceMerge(dst, src any) error {
	target, ok := dst.(*map[string]any)
	if !ok {
		return fmt.Errorf("configuration: unexpected merge target %T", dst)
	}
	next, ok := src.(map[string]any)
	if !ok {
		return fmt.Errorf("configuration: unexpected merge source %T", src)
	}
	*target = next
	return nil
}
```

写法 B **不要**再自己调用 `source.Load()` 和 `source.Watch()`。Kratos 已经调用过了，SDK 的 `Watch` 只能启动一次。

### 5.6 业务代码怎么读到新配置

- **用的时候再调 `GetConfig()`**。启动时取一次存进变量（`bc := cf.GetConfig()`）然后一直用，平台发布后业务代码永远读不到新值。要热更新的地方，持有 `Configuration[T]` 本身，用的时候再取。
- 一段逻辑里需要几个字段前后一致的，开头取一次 `cfg := cf.GetConfig()`，后面都用这个 `cfg`。
- **不要改 `GetConfig()` 返回的结构体**。所有协程共享这一份，只读。
- **启动时用来建连接的配置改了，仍然要重启**，比如数据库 DSN、Redis 地址、Kafka broker、监听端口、gRPC 客户端地址：这些对象在启动时就建好了，SDK 不会帮你重建。
- 想在配置变化后做点什么（比如重建某个客户端），可以在 `onCommit` 的 `Store` 之后发一个不会失败、不阻塞的通知（如 `select { case ch <- cfg: default: }`），具体的活放到别的协程里做。启动加载时也会调用一次 `onCommit`。
- 想拦截业务上不合法的值（比如超时为 0），在 `decode` 末尾加校验，返回错误就会拒掉这一版。

### 5.7 必须遵守的规则

1. **`onCommit` 里可能失败的步骤都放在 `Store` 之前**。SDK 撤销不了已经发生的 `Store`，Store 之后再返回错误，就会出现内存已经换了、SDK 却认为这一版失败的情况。Store 之后只允许做不会失败、不阻塞的事，比如 5.6 里的非阻塞通知。
2. **写法 A 必须一直有协程在取 `Next()`**。没人取的话，第一次发布还能进内存，从第二次起全部收不到。
3. **`validate` 和 `onCommit` 用同一个 `decode`**。校验通过、加载却失败的话，这一版会被拒，告警也不好查。
4. **`NewConfiguration` 返回错误就退出进程**，不要吞掉错误或者用默认值兜底启动。宁可启动失败让 K8s 重试，也不能带着空配置跑。
5. **一个进程里同一个服务名只建一个配置源**。

---

## 6 部署（给 SRE）

### 6.1 环境变量

| 变量 | 必填 | 说明 |
|---|---|---|
| `CONFIG_SOURCE` | 是 | 填 `etcd` 打开配置平台模式。不设或填其他值就走原来的文件配置，回退时去掉它即可 |
| `CONFIG_ETCD_ENDPOINTS` | 是 | 本环境 etcd 地址，多个用逗号分隔，如 `http://etcd-0:2379,http://etcd-1:2379` |
| `CONFIG_SERVICE` | 是 | 服务名，和平台上建的服务名一致 |
| `CONFIG_ETCD_USERNAME` / `CONFIG_ETCD_PASSWORD` | 是 | etcd 只读账号，密码从 Secret 注入 |
| `CONFIG_SNAPSHOT_DIR` | 否 | 本机快照目录，默认 `/var/lib/confsource` |
| `CONFIG_STRICT_ENV` | 否 | 默认严格，缺变量就拒掉这一版。填 `false` 时缺的变量当空串，只建议迁移过渡期临时用 |
| 配置里 `${VAR}` 引用的全部变量 | 是 | 密钥从 Secret 注入。可选的变量也要声明，可以为空 |

### 6.2 etcd 账号

- 服务用**只读**账号，对 `/config/` 和 `/meta/` 两个前缀都要有 READ 权限。SDK 在一个事务里同时读配置和 meta，少了 `/meta/` 的权限，整个读取都会被拒。
- etcd 只开了账号密码认证，各环境都没有启用 TLS，所以参考代码也只接了账号密码。

### 6.3 快照目录

- 建议挂 hostPath `/var/lib/confsource`（`DirectoryOrCreate`），读写，容器用户要有写权限。
- 文件是 `{服务名}.yaml`、`{服务名}.yaml.meta` 和 `{服务名}.lock`。同一节点上同一服务的多个副本共享这几个文件，靠文件锁互斥，这是设计内的用法。
- 快照里只有 `${VAR}` 引用，没有密钥真值。
- 当前版本启动阶段用不上快照（见[第 10 节](#10-当前版本的已知限制)第 1 条），要等 SDK 修复后才真正起兜底作用，建议现在就挂上。不挂卷也能运行，目录写不进去时只会打错误日志，不影响服务运行。

### 6.4 指标

- SDK 默认把指标注册到 `prometheus.DefaultRegisterer`，服务用 `promhttp.Handler()` 暴露 `/metrics` 就能带出来。服务用的是自定义 Registry 的话，加 `confsource.WithRegisterer(reg)`。
- Prometheus 要能抓到这个服务的 `/metrics`。

| 指标 | 含义 |
|---|---|
| `confsource_loaded_revision{service}` | 当前内存里那版配置的 etcd revision，正常情况下等于平台「当前配置」显示的 etcd revision |
| `confsource_degraded{service}` | 1 表示正用本机快照运行，还没和 etcd 核对上 |

建议告警：`confsource_degraded == 1` 持续 5 分钟；同一服务各副本的 `confsource_loaded_revision` 长时间不一致。

当前版本没有「版本被拒」的指标，坏版本被拒只体现在日志里，所以还要配日志告警：级别为 ERROR、带 `module=confsource` 的日志（关键字 `rejected`、`apply failed`、`config key deleted`）。每次发布后，也可以对比各副本的 `confsource_loaded_revision` 和平台「当前配置」的 etcd revision，不一致说明有副本没拿到这一版。

### 6.5 日志

SDK 默认用 Kratos 的全局 logger 打到标准输出，日志带 `module=confsource service=…`。想接进服务自己的日志体系，就实现 Kratos 的 `log.Logger` 接口（只有一个 `Log(level, keyvals...)` 方法），通过 `confsource.WithLogger(l)` 传进去。

---

## 7 上线与验证

按 dev → staging → 生产的顺序，每个环境做一遍：

1. 平台上这个环境已经发布过第一版（第 3 节）。
2. 部署新镜像，带上第 6 节的环境变量和快照卷。
3. 看启动：日志里没有 `confsource` 报错，进程正常起来。
4. 看 `/metrics`：`confsource_loaded_revision` 等于平台「当前配置」的 etcd revision，`confsource_degraded` 为 0。
5. 在平台上改一个无关紧要的值并发布。不重启 Pod，`confsource_loaded_revision` 跟着变成新的 revision，业务按 5.6 的方式读到新值。

**回退**：

- 原来有文件配置的服务：去掉 `CONFIG_SOURCE` 后滚动重启，回到文件配置，前提是原来的文件配置挂载还在。切到平台之后改过的配置只存在平台上，回退前要先把平台「当前配置」同步回文件配置，否则回退等于配置倒回切换前。
- 一开始就只在平台上的新服务没有文件配置可退，配置出问题就在平台「历史版本」里回滚。

---

## 8 运行时行为

| 情况 | 结果 |
|---|---|
| 启动时 etcd 正常 | 从 etcd 加载，写本机快照，`degraded=0` |
| 启动时 etcd 连不上 | 约 3 秒后启动失败，本机有快照也用不上，见第 10 节第 1 条 |
| 启动时连上了 etcd 但读取失败（比如账号缺读权限），本机有快照 | 用快照启动，`degraded=1`；后台按 1 秒到 30 秒退避重试，读到后对齐到 etcd 最新版 |
| 启动时连上了 etcd 但读取失败，本机没快照 | 启动失败 |
| 平台上没发布过这个服务 | 启动失败（`key ... not found`），这种情况也不会用快照顶替 |
| 启动时最新一版缺变量或解析不了 | 启动失败，报错里写明原因 |
| 运行中发布了合法的新版 | 通常一秒内换进内存，写快照，revision 前进 |
| 运行中发布了坏版本 | 这一版被拒，打错误日志，内存、快照、revision 都停在上一版，后面的合法版本照常生效 |
| 运行中 etcd 断开 | 内存配置不变，服务照常运行，后台自动重连 |
| etcd 里的 key 被删 | 忽略，打错误日志，保持当前配置 |

**坏版本被拒，只是保护正在跑的 Pod，不等于回滚了**。etcd 里存的仍是那版坏配置，新起的 Pod（扩容、重启、发版）会因为它启动失败。看到 `rejected` 日志就要马上在平台上修正或回滚。当前版本只有日志能反映版本被拒，日志告警的配法见 [6.4](#64-指标)。

所以**新增 `${VAR}` 的顺序是固定的**：先让 SRE 把变量注入 Pod 并滚动重启，再在平台上发布引用它的配置。顺序反过来，正在跑的 Pod 会全部拒掉新版，新起的 Pod 则起不来。

---

## 9 常见报错

| 报错 | 原因 | 处理 |
|---|---|---|
| `configuration: CONFIG_SERVICE is required when CONFIG_SOURCE=etcd` | 没设服务名 | 补 `CONFIG_SERVICE` |
| `confsource: endpoints required` | 没设 etcd 地址 | 补 `CONFIG_ETCD_ENDPOINTS` |
| `confsource: etcd client: context deadline exceeded` | 创建连接时连不上 etcd：地址不对、网络不通或 etcd 不可用 | 检查地址和网络策略；etcd 恢复后，K8s 重启 Pod 时就能起来 |
| `confsource: etcd client: etcdserver: authentication failed, invalid user ID or password` | 账号或密码不对 | 核对账号密码 |
| `confsource: no config available from etcd or snapshot: key /config/xxx.yaml not found` | 这个环境的平台上没发布过，或者服务名写错、连错了环境的 etcd | 去平台发布第一版，核对 `CONFIG_SERVICE` 和地址 |
| `confsource: no config available from etcd or snapshot (etcd: …; snapshot: confsource: snapshot missing)` | etcd 读不到，本机也没有快照。括号里 `etcd:` 后面是真正的原因，可能是不通，也可能是 `permission denied` | 按括号里的原因处理；`permission denied` 就是只读账号缺 `/config/` 或 `/meta/` 的读权限 |
| `confsource: revision N rejected: envexpand: missing variables: A, B` | 配置引用的变量没注入 Pod | 补环境变量；运行中出现的话，同时去平台修正或回滚 |
| `confsource: revision N rejected: json: cannot unmarshal …` 或 `proto: …` | 值的类型和结构体对不上 | 改配置里的值，或者改结构体 |
| `confsource: snapshot rejected: …` | etcd 不可用，本机快照里那版也通不过校验，比如变量后来被删了 | 恢复 etcd 连接，或补齐变量 |
| 日志 `etcd unavailable, starting from snapshot` | 用快照降级启动了 | 查 etcd 连通性和账号权限。读权限不够时，有快照也会走这条 |
| 日志 `resync failed, retry in …` 持续出现 | 降级运行中，重连 etcd 后重新拉取一直失败，会按退避一直重试，`confsource_degraded` 保持 1 | 按日志里的原因查连通性和账号权限。读权限不够、本机又有快照时，服务会一直停在这个状态 |
| 日志 `write snapshot failed` | 快照目录写不进去 | 查挂载和目录权限，不影响运行 |
| 日志 `watch interrupted, reconnect in …` / `watch revision compacted, reloading full config` | 监听断开后自动重连 / 重新全量拉取 | 偶发属于正常；持续出现就查 etcd |

---

## 10 当前版本的已知限制

1. **启动阶段的快照兜底不生效**。用账号密码连 etcd 时，etcd 客户端在创建连接时会同步认证，启动那一刻 etcd 连不上就直接失败（`confsource: etcd client: context deadline exceeded`），走不到读快照那一步，已在内嵌 etcd 上实测确认。也就是说 etcd 不可用期间，新起的 Pod（扩容、重启、发版）都起不来。已经在运行的服务不受影响，etcd 断开后照常用内存配置。
2. **没有连接心跳**。监听连接在空闲时静默断开（连接看起来还在，但数据已经不通），SDK 发现不了，之后的发布可能一直收不到，直到 Pod 重启。也没有「最近一次同步时间」指标，只能在下一次发布后发现 `confsource_loaded_revision` 没跟上平台。遇到时重启 Pod。
3. **快照校验不严格**。快照的 `.meta` 缺失或和内容对不上时，仍会用快照内容启动，revision 记为 0，连上 etcd 后会重新拉取全量。
4. **指标只有两个**，看不到内容哈希和最近一次同步的时间。

第 2–4 条在 SDK 后续版本里已经修复，main 更新后再统一升级，升级只需改 go.mod 和 go.sum。
