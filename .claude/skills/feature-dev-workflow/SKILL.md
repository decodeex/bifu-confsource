---
name: feature-dev-workflow
description: bifu-confsource（统一配置平台的 Kratos config.Source 客户端封装，独立 Go 模块，被 bifufx-api 全部服务引用）的开发工作流。覆盖「从系分 §4.3 / §6.2 与方案文档出发 → 判定改动是否触及公共 API、etcd 协议、快照格式、指标 → 等拍板 → 实现 + 内嵌 etcd 测试 + race → 同步 bifufx-api 加载器与伪版本」。当用户要「改 confsource 的 Load / Watch / 快照 / 降级 / 指标 / 连接参数」或「把 confsource 新版本接进 bifufx-api」时使用。平台侧用 config-center-service 的同名 skill。
---

# Feature Dev Workflow · 配置客户端封装（bifu-confsource）

这是一个**库**，不是服务：每个接入服务的每个 Pod 里都跑着它，它的一个 bug 会同时出现在 36 个服务上。所以本仓的纪律比平台更严：公共 API 稳定、行为可测、每个改动有内嵌 etcd 用例、race 必过。

> 工作区根 `CLAUDE.md` 的注释、拆文件、改代码安全、Git 与远程仓库限制对本仓生效。本文只写本仓特有的部分。

## 本仓画像

**一句话**：实现 Kratos `config.Source`，按服务名从本环境 etcd 读 `/config/{service}.yaml` 与 `/meta/{service}`，Watch 推送、本地 hostPath 快照降级、校验与提交回调、两个指标。**不依赖平台，不依赖 K8s，不解析业务 yaml**。

**模块**：`github.com/decodeex/bifu-confsource`，Go 1.24，依赖只有 Kratos v2.9、etcd client v3.5、prometheus client、yaml；测试额外用 etcd server（内嵌）。

**文件与职责**：

| 文件 | 职责 |
|---|---|
| `source.go` | `New(opts...)`、`Load`（Txn 同读 config+meta → commit；etcd 不可达转快照并置 degraded）、`Watch`（只允许一次）、`Close`、`Revision`、`Degraded`；`fetch`、`commit`（Validator → OnCommit → 写快照 → 推进 revision 与指标）、`runOnCommit`（回调 panic 转错误） |
| `watcher.go` | 事件循环：resync / watch / 退避；陈旧事件跳过、Delete 忽略、ErrCompacted 全量 resync；交付给 Kratos 的 channel |
| `snapshot.go` | `/var/lib/confsource/{service}.yaml` + `.yaml.meta`（mod_revision、publish_id、saved_at、content_hash）；flock、单调检查、临时文件 fsync + rename；读时校验 hash |
| `options.go` | `Option` 函数与默认值（timeout 3 s、退避 1 s～30 s、快照目录） |
| `metrics.go` | `confsource_loaded_revision`、`confsource_degraded`（带 `service` 标签） |
| `errors.go` | `ErrNoConfig`、`ErrSnapshotMissing`、`ErrWatchStarted` |
| `envexpand/` | `${VAR}` 严格展开，只展开标量，供接入方的加载器用 |
| `etcdtest_test.go` | 内嵌 etcd（随机端口，可 compact / 断连 / 重启），所有集成用例的底座 |
| `prototype_test.go`、`loader_test.go` | **镜像 bifufx-api `utils/configuration/etcd.go` 的加载器**（Validator / OnCommit / protojson 解码路径），验证「平台发布 → 进程内存替换」的两个验收场景 |

**公共 API**（改签名就是破坏性变更）：`New`、全部 `With*` Option、`Source.Load / Watch / Close / Revision / Degraded`、`Validator` / `OnCommit` 类型、三个错误值、`envexpand.Lookup / UnmarshalYAML`、两个指标名。

**协议契约**（与平台 config-center-service 共同拥有，改一边必改另一边）：key 规则 `/config/{service}.yaml`、`/meta/{service}`；meta JSON 字段 `publish_id / version / publisher / published_at / source_commit / content_hash`；`content_hash` 算法 `sha256:<hex>`；平台每次发布在同一个 Txn 里写两个 key。

**消费者契约**（与 bifufx-api 共同拥有）：环境变量 `CONFIG_SOURCE / CONFIG_ETCD_ENDPOINTS / CONFIG_ETCD_USERNAME / CONFIG_ETCD_PASSWORD / CONFIG_SERVICE / CONFIG_SNAPSHOT_DIR / CONFIG_STRICT_ENV` 由 bifufx-api 的 `utils/configuration/etcd.go` 读取并传进 Option；commit 顺序保证「Store 是回调最后一步」。

## 开发依据

- 系分 `mydoc/系分_统一配置平台_系统分析与设计文档.md`（v0.15）§4.3 客户端流程、§6.2 etcd 运维与恢复规程、§7.2 真值边界；压测方案 §1.1 的「实现事实表」是本仓行为的对外说明，**改了行为要回去改那张表**；修复方案 §3～§5 是当前客户端改动的设计依据。只引用章节，不抄进代码。
- 设计上的硬规则（来自系分，不要在代码里推翻）：不用空配置启动；`${VAR}` 缺失拒掉整版（严格模式默认）；校验失败内存、快照、指标都停在上一版；Delete 事件忽略；快照写失败只记日志；etcd 是主路径，快照只在 etcd 不可达时用。

## 核心铁律

1. **动代码等「开始」**；开始后按里程碑一路做完。
2. **公共 API 与协议不随手改**：要改先列 bifufx-api `utils/configuration/etcd.go` 与平台 `internal/data/etcd.go` 的受影响点，得到同意再动；新能力优先用新增 Option 并给默认值，让 bifufx-api 不改代码也能升级。
3. **每个行为改动配一个内嵌 etcd 用例**，用例名写场景（压缩后重连、恢复后 revision 倒退、静默断连 …），并且 `-race` 必过。
4. **不引入新依赖**；不加 K8s、HTTP、平台 API 之类的任何耦合；不把业务 yaml 的结构带进来。
5. **只改必要的**：不重构、不改既有命名；日志前缀保持 `module=confsource`，指标前缀保持 `confsource_`。
6. **Git**：只改文件，不 add / commit / push，不切分支，不碰远程；本仓 remote URL 内嵌了 token，**任何输出里不要回显 `git remote -v` 的原文**。
7. **影响范围说明**：每步写清改了什么、bifufx-api 要不要跟着改、压测方案 §1.1 哪几行要更新。

## 命令（本机核实过）

```bash
cd /root/bifu/decodeex-admin/bifu-confsource
go build ./... && go vet ./...
go test -race ./...            # 含内嵌 etcd 集成用例，随机端口，不需要外部 etcd、不需要网络
go test -race -run 'TestXxx' -v ./...
```

没有 Makefile，就这三条。测试会在临时目录起 etcd，机器慢时单个用例几秒属正常。

## 测试要求

| 类型 | 覆盖 | 要求 |
|---|---|---|
| 单测 | 快照读写与损坏识别、`envexpand`、退避 | 纯内存或临时目录 |
| 集成（内嵌 etcd） | Load / Watch / 校验拒绝 / 断连重连 / compaction / 降级与恢复 / 指标值 | 每个改动的场景至少一个用例；用 `etcdtest_test.go` 的能力造故障，不手工起外部 etcd |
| 镜像用例 | `prototype_test.go` / `loader_test.go` | bifufx-api 加载器改了解码或回调顺序，这里同步改，反之亦然 |
| 回归 | `go test -race ./...` 全量 | 任何改动都跑全量，20+ 用例全绿、race 干净才算过 |

验收口径：build / vet / 全量 race 测试绿 + 新场景用例存在且先红后绿 + bifufx-api 用本地路径替换后 `go build ./...` 通过（见下）+ 压测方案 §1.1 已同步。

## 发布到 bifufx-api 的流程

bifufx-api 用伪版本钉死本仓某个 commit（`go.mod` 里 `github.com/decodeex/bifu-confsource v0.0.0-<时间戳>-<12位hash>`）。升级步骤：

1. 本仓改完、测试绿，**由用户 commit 并 push**（AI 不做）。
2. 本地验证不经网络：在 scratchpad 放一份 gitconfig，用 `insteadOf` 把 `https://github.com/decodeex/bifu-confsource` 指到本地克隆路径，然后 `GIT_CONFIG_GLOBAL=<那份文件> GOFLAGS=-mod=mod go get github.com/decodeex/bifu-confsource@<commit>` 解析出伪版本与 go.sum。
3. 改 bifufx-api `go.mod` 的 require 行（放在直接依赖块，不带 `// indirect`），**不要 `go mod tidy`**（它会动 `google/subcommands` 等无关项），然后 `go build ./... && go test ./utils/configuration/...`。
4. 告诉用户：bifufx-api 这次只改 `go.mod` / `go.sum`，以及 `.github/gitconfig` 的 SSH 改写依赖 CI 身份对本仓有读权限。

## 分步开发的固定节奏

每个里程碑：**写用例（红）→ 实现（绿）→ 全量 race → 影响范围说明 → 报告**。报告包含：改动文件、公共 API 是否变化、协议是否变化、bifufx-api 需要的配合、压测方案 §1.1 的更新点、commit message。

收尾：汇总里程碑、给 bifufx-api 的升级说明、给平台的协议变更说明（如有）、给 SRE 的部署变量或运维规程变化（如有）。

## 与另外两个仓的边界

- **平台**：共同拥有 etcd key 与 meta 协议；平台不 import 本仓。
- **bifufx-api**：共同拥有环境变量与回调顺序契约；它的 `utils/configuration/etcd.go` 是本仓的唯一真实消费者，`prototype_test.go` 是它的镜像。
- **前端**：无关。

## 长任务抗压缩

状态落在方案文档的里程碑勾选与每步报告；压缩后先读方案文档再读对应用例续做。
