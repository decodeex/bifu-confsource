---
name: feature-dev-workflow
description: bifu-confsource（统一配置平台的 Kratos config.Source 客户端封装，独立 Go 模块，被 bifufx-api 全部服务引用）的开发工作流。覆盖「从系分客户端章节与方案文档出发 → 判定改动是否触及公共 API、etcd 协议、快照格式、指标 → 等拍板 → 实现 + 内嵌 etcd 测试 + race → 同步 bifufx-api 加载器与伪版本」。当用户要「改 confsource 的 Load / Watch / 快照 / 降级 / 指标 / 连接参数」或「把 confsource 新版本接进 bifufx-api」时使用。平台侧用 config-center-service 的同名 skill。
---

# Feature Dev Workflow · 配置客户端封装（bifu-confsource）

这是一个**库**，不是服务：每个接入服务的每个 Pod 里都跑着它，它的一个 bug 会同时出现在所有接入服务上。所以本仓的纪律比平台更严：公共 API 稳定、行为可测、每个改动有内嵌 etcd 用例、race 必过。

> 工作区根 `CLAUDE.md` 的通用规范（注释、拆文件、修改安全与兼容性、Git 操作与远程仓库限制）对本仓生效，本文不重复，只写本仓特有的部分。

## 本仓画像

**一句话**：实现 Kratos `config.Source`，按服务名从本环境 etcd 读 `/config/{service}.yaml` 与 `/meta/{service}`，Watch 推送、本地快照降级、校验与提交回调、Prometheus 指标。**不依赖平台，不依赖 K8s，不解析业务 yaml**。

**模块**：`github.com/decodeex/bifu-confsource`（版本以 `go.mod` 为准），依赖只有 Kratos、etcd client、prometheus client、yaml；测试额外用内嵌 etcd server。

**文件与职责**：

| 文件 | 职责 |
|---|---|
| `source.go` | `New(opts...)`、`Load`（Txn 同读 config+meta → commit；etcd 不可达转快照并置 degraded）、`Watch`（只允许一次）、`Close`、`Revision`、`Degraded`；`fetch`、`commit`（Validator → OnCommit → 写快照 → 推进 revision 与指标）、`runOnCommit`（回调 panic 转错误） |
| `watcher.go` | 串行事件循环：resync / watch / 退避；陈旧事件跳过、Delete 忽略、compaction 全量 resync；交付给 Kratos 的 channel |
| `snapshot.go` | `{snapshotDir}/{service}.yaml` + `.yaml.meta`（mod_revision、publish_id、saved_at、content_hash）；flock、单调检查、临时文件 fsync + rename；读时校验 hash |
| `options.go` | `Option` 函数与默认值（超时、退避区间、快照目录、registerer、logger） |
| `metrics.go` | `confsource_` 前缀指标，`service` 标签；同进程多个 Source 共用注册器时复用已注册 collector |
| `errors.go` | `ErrNoConfig`、`ErrSnapshotMissing`、`ErrWatchStarted` |
| `envexpand/` | `${VAR}` 严格展开，只展开标量，供接入方的加载器用 |
| `etcdtest_test.go` | 内嵌 etcd（随机端口，可 compact / 断连 / 重启），所有集成用例的底座 |
| `prototype_test.go`、`loader_test.go` | **镜像 bifufx-api `utils/configuration/etcd.go` 的加载器**（Validator / OnCommit / protojson 解码路径），验证「平台发布 → 进程内存替换」的验收场景 |

**公共 API**（改签名就是破坏性变更）：`New`、全部 `With*` Option、`Source.Load / Watch / Close / Revision / Degraded`、`Validator` / `OnCommit` 类型、`errors.go` 里的错误值、`envexpand` 导出函数、已存在的指标名与标签。

**协议契约**（与平台 config-center-service 共同拥有，改一边必改另一边）：key 规则 `/config/{service}.yaml`、`/meta/{service}`；meta JSON 字段 `publish_id / version / publisher / published_at / source_commit / content_hash`；`content_hash` 算法 `sha256:<hex>`；平台每次发布在同一个 Txn 里写两个 key。

**消费者契约**（与 bifufx-api 共同拥有）：环境变量 `CONFIG_SOURCE / CONFIG_ETCD_ENDPOINTS / CONFIG_ETCD_USERNAME / CONFIG_ETCD_PASSWORD / CONFIG_SERVICE / CONFIG_SNAPSHOT_DIR / CONFIG_STRICT_ENV` 由 bifufx-api 的 `utils/configuration/etcd.go` 读取并传进 Option；commit 顺序保证「Store 是回调最后一步」。

## 开发依据

- 系分 `mydoc/系分_统一配置平台_系统分析与设计文档.md`（以文件里的最新版本为准）的客户端流程、etcd 运维与恢复规程、真值边界章节；该需求的方案文档（`mydoc/修复方案_*` / `mydoc/开发方案_*`）；压测方案里的「客户端实现事实表」是本仓行为的对外说明，**改了行为要回去改那张表**。只引用章节，不抄进代码。
- 代码与系分不一致时先问用户以哪边为准；要改设计，先改系分再改代码。

## 核心铁律

1. **动代码等「开始」**；开始后按里程碑一路做完，只在下文「停止条件」出现时停下来问。
2. **公共 API 与协议不随手改**：要改先列 bifufx-api `utils/configuration/etcd.go` 与平台 `internal/data/etcd.go` 的受影响点，得到同意再动；新能力优先用新增 Option 并给默认值，让 bifufx-api 不改代码也能升级。
3. **每个行为改动配一个内嵌 etcd 用例**，用例名写场景（压缩后重连、恢复后 revision 倒退、静默断连 …），先红后绿，并且 `-race` 必过。
4. **回归保护**：已有用例是行为契约，`prototype_test.go` / `loader_test.go` 还是 bifufx-api 加载器的镜像。**不允许删除用例、放宽断言、加 Skip、改期望值或缩小测试范围来让测试通过**；确需改旧用例必须在方案里写明并经用户确认，改动说明里逐条列出。
5. **不引入新依赖**；不加 K8s、HTTP、平台 API 之类的任何耦合；不把业务 yaml 的结构带进来；日志前缀保持 `module=confsource`，指标前缀保持 `confsource_`。
6. **Git 在根规范之外的补充**：分支不对就提示用户切，不自己切；每步给 commit message，不带 AI 署名尾行；本仓 remote URL 内嵌了 token，**任何输出里不要回显 `git remote -v` 的原文**。
7. **共享环境不自己碰**（与平台 skill 同一规则）：dev / staging / main 的 etcd、k8s、Pod 一律不自己操作，只给命令由用户执行，只读也先问；本仓测试只用内嵌 etcd，不连任何外部 etcd。
8. **密钥不落盘**：测试、脚本、文档里不出现真实 etcd 地址、账号、配置内容；`bifufx-configmaps` 仓库含明文生产密钥，不引用其内容。

## 客户端链路的核心约束

改 `source.go` / `watcher.go` / `snapshot.go` 之前先把下面几条对照代码读一遍，这些是系分定下的硬规则，不要在代码里推翻：

- **不以空配置启动**：etcd 与快照都拿不到就返回 `ErrNoConfig`，由接入方决定进程退出；`${VAR}` 缺失拒掉整版（严格模式默认）。
- **同一 Txn 读两个 key**：config 与 meta 必须来自同一 revision；`publish_id` 拿不到只影响快照 meta，不影响加载。
- **commit 顺序固定**：Validator → OnCommit（加载器 Store 是最后一步）→ 写快照 → 推进 revision 与指标。校验或回调失败时内存、快照、revision、指标一起停在上一版；回调 panic 按错误处理，事件循环不停。快照写失败只记日志，etcd 是主路径。
- **事件循环串行**：按 revision 顺序逐个校验、落快照、交付，前一版被 Kratos 取走之前不处理下一版；Delete 事件忽略并告警；`ModRevision` 不大于已提交的跳过。
- **Watch 游标严格遵循 clientv3 的推进规则**：恢复点以 clientv3 给出的 revision 为准（WatchResponse header / 最后一个事件），不用自己推算的值；revision 倒退只能通过全量 resync 发现并由 ModRevision 单调规则处理，**不采用内容、不猜测**；compaction（`ErrCompacted` / `CompactRevision != 0`）一律全量 resync 后重建 Watch 起点；断连、compaction、resync 失败都走退避，不空转。
- **降级只在 etcd 不可达时发生**：Load 阶段 etcd 失败才读快照并置 `degraded=1`；快照必须通过 hash 校验与 Validator；恢复条件是「拉到、校验过、写完快照且 Kratos 取走」，不是连上就算恢复。
- **快照单调**：只写 ModRevision 更高的版本，写入走临时文件 fsync + rename，并发用 flock。
- **指标只反映已提交状态**：`loaded_revision` 与 `degraded` 在 commit 成功后更新，失败不动；新增指标沿用 `service` 标签，带可变标签值的要有清理逻辑。

## 命令（本机核实过）

```bash
cd <本仓绝对路径>
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
| 回归 | `go test -race ./...` 全量 | 任何改动都跑全量，全绿、race 干净才算过 |

验收口径：build / vet / 全量 race 测试绿 + 新场景用例存在且先红后绿 + bifufx-api 用本地路径替换后 `go build ./...` 通过（见下）+ 压测方案的客户端事实表已同步。

## 发布到 bifufx-api 的流程

bifufx-api 用伪版本钉死本仓某个 commit（`go.mod` 里 `github.com/decodeex/bifu-confsource v0.0.0-<时间戳>-<12位hash>`）。升级步骤：

1. 本仓改完、测试绿，**由用户 commit 并 push**（AI 不做）。
2. 本地验证不经网络：在 scratchpad 放一份 gitconfig，用 `[url "<本仓本地路径>"] insteadOf = https://github.com/decodeex/bifu-confsource` 把模块地址指到本地克隆，然后 `GIT_CONFIG_GLOBAL=<那份文件> GOFLAGS=-mod=mod go get github.com/decodeex/bifu-confsource@<commit>` 解析出伪版本与 go.sum。
3. 改 bifufx-api `go.mod` 的 require 行（放在直接依赖块，不带 `// indirect`），**不要 `go mod tidy`**（它会动无关依赖），然后 `go build ./... && go test ./utils/configuration/...`。
4. 告诉用户：bifufx-api 这次只改 `go.mod` / `go.sum`，以及 CI 拉本仓依赖 `.github/gitconfig` 的 SSH 改写与 CI 身份对本仓的读权限。

## 分步开发的固定节奏

每个里程碑：**写用例（红）→ 实现（绿）→ 全量 race → 影响范围说明 → 报告**。报告包含：改动文件、公共 API 是否变化、协议是否变化、bifufx-api 需要的配合、压测方案事实表的更新点、commit message。做完直接进下一个里程碑。用户中途改需求：先更新方案文档的里程碑，再改代码。

**停止条件**只针对超出已批准方案范围的改动；方案里已列出的改动，以及它必然带动的关联修改（对应用例、镜像用例、Option 默认值、事实表）不算扩大范围，不重复确认。遇到下列情况停下，说明情况、给推荐方案，等用户确认后再继续：
- 架构层面的改动：改公共 API 签名、改 etcd key / meta 协议、改快照文件格式、改 commit 顺序或事件循环的串行模型、引入新依赖。
- 破坏性改动：删除或改写已有用例、改已有指标名或标签、改默认值导致 bifufx-api 不改代码就行为变化。
- 需求范围变化：实现中发现方案没覆盖的故障场景、要改方案里没列且不是关联适配的文件、或者按方案做会破坏原有功能。
- bifufx-api 加载器必须跟着改才能用上本次改动。
- 方案与系分、或代码与系分冲突，无法判断以哪边为准。

收尾：汇总里程碑、给 bifufx-api 的升级说明、给平台的协议变更说明（如有）、给 SRE 的部署变量或运维规程变化（如有）。

## 与另外两个仓的边界

- **平台**：共同拥有 etcd key 与 meta 协议；平台不 import 本仓。
- **bifufx-api**：共同拥有环境变量与回调顺序契约；它的 `utils/configuration/etcd.go` 是本仓的唯一真实消费者，`prototype_test.go` 是它的镜像。
- **前端**：无关。

## 长任务抗压缩

状态落在方案文档的里程碑勾选与每步报告；压缩后先读方案文档再读对应用例续做。
