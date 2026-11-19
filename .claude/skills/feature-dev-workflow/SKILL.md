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
2. **公共 API 与协议不随手改**：要改先列 bifufx-api `utils/configuration/etcd.go` 与平台 `internal/data/etcd.go` 的受影响点，得到同意再动；具体边界见下文「向后兼容约束」。
3. **每个行为改动配一个内嵌 etcd 用例**，用例名写场景（压缩后重连、恢复后 revision 倒退、静默断连 …），先红后绿，并且 `-race` 必过。
4. **回归保护**：已有用例是行为契约，`prototype_test.go` / `loader_test.go` 还是 bifufx-api 加载器的镜像。**不允许删除用例、放宽断言、加 Skip、改期望值或缩小测试范围来让测试通过**；确需改旧用例必须在方案里写明并经用户确认，改动说明里逐条列出。
5. **不引入新依赖**；不加 K8s、HTTP、平台 API 之类的任何耦合；不把业务 yaml 的结构带进来；日志前缀保持 `module=confsource`，指标前缀保持 `confsource_`。
6. **Git 在根规范之外的补充**：分支不对就提示用户切，不自己切；每步给 commit message，不带 AI 署名尾行；本仓 remote URL 内嵌了 token，**任何输出里不要回显 `git remote -v` 的原文**。
7. **共享环境不自己碰**（与平台 skill 同一规则）：dev / staging / main 的 etcd、k8s、Pod 一律不自己操作，只给命令由用户执行，只读也先问；本仓测试只用内嵌 etcd，不连任何外部 etcd。
8. **密钥不落盘、不进日志**：测试、脚本、文档里不出现真实 etcd 地址、账号、配置内容；`bifufx-configmaps` 仓库含明文生产密钥，不引用其内容。日志规则见下文「日志安全」。

## 客户端链路的核心约束

改 `source.go` / `watcher.go` / `snapshot.go` 之前先把下面几条对照代码读一遍，这些是系分定下的硬规则，不要在代码里推翻：

**三个 revision 各司其职，不能混用**

| 名字 | 含义 | 谁推进 | 存在哪 |
|---|---|---|---|
| Watch 游标 | 下一次 Watch 从哪个 revision 开始订阅 | 严格按 clientv3 的规则：`WatchResponse.Header.Revision` 或最后一个事件的 `ModRevision`，加一；**不用 Committed Revision 推算** | 事件循环的局部状态 |
| Committed Revision | 内存里正在生效的那版配置的 etcd `ModRevision`，`Source.Revision()` 与 `confsource_loaded_revision` 返回的就是它 | 只在 Validator 与 OnCommit 都成功后推进 | `Source.committed`（原子） |
| Snapshot Revision | 磁盘快照 meta 里的 `mod_revision` | 只在 commit 成功后尽力写入，写失败只记日志 | `{snapshotDir}/{service}.yaml.meta` |

规则：
- Watch 游标只跟 etcd 事件流走。**某一版校验失败时，游标照常推进、Committed Revision 不动**，这样坏版本不会被反复重投，下一版到来时正常处理；全量 resync 之后游标重新以 resync 读到的 header revision 为准。
- Committed Revision 只升不降；revision 倒退（灾备恢复后 etcd 的 revision 比内存小）只能通过全量 resync 发现，按 ModRevision 单调规则处理，**不采用内容、不猜测**。
- Snapshot Revision 永远不高于 Committed Revision，可以落后（写失败）；快照 meta 缺失、损坏或与内容 hash 不配对时内容仍可用但 revision 视为 0，之后必须全量 resync，不能按它续订。
- compaction（`ErrCompacted` / `CompactRevision != 0`）一律全量 resync 后重建 Watch 起点；断连、compaction、resync 失败都走退避，不空转。

**degraded 的语义只有一条**：内存里的配置来自本地快照、尚未与 etcd 核对过。它与「etcd 连接是否健康」「快照是否写成功」无关：
- 置 1 只发生在 Load 阶段 etcd 不可达、改用快照启动时；快照内容仍要过 Validator 与 OnCommit，过不了就是 `ErrNoConfig`，不以空配置启动。
- 置 0 的条件是一次成功的全量 resync：从 etcd 读到配置，且内存版本不落后于 etcd（需要时先 commit 并交付给 Kratos）。只是连上了不算。
- Watch 运行中断连**不**置 degraded，内存里仍是最后一版已提交的配置；连接健康用专门的指标表达，不复用 degraded。
- 三件事分开看：etcd 连接健康（Watch 流在不在）、业务内存生效（Committed Revision）、快照持久化（尽力而为）。写规则或指标时说清楚指的是哪一件。

**commit 顺序固定**：Validator → OnCommit（加载器 Store 是最后一步）→ 写快照 → 推进 Committed Revision 与指标。校验或回调失败时内存、快照、revision、指标一起停在上一版；回调 panic 按错误处理，事件循环不停。

**同一 Txn 读两个 key**：config 与 meta 必须来自同一 revision；`publish_id` 拿不到只影响快照 meta，不影响加载。`${VAR}` 缺失拒掉整版（严格模式默认）；Delete 事件忽略并告警；`ModRevision` 不大于 Committed Revision 的事件跳过。

**并发安全与资源生命周期**
- 一个 `Source` 只有一条事件 goroutine，由 `Watch()` 启动且只允许一次（`ErrWatchStarted`）；所有 goroutine 与 etcd 调用都派生自 `Source.ctx`，`Close()` 取消它并关闭 client，之后不得再有后台活动或往 channel 写。
- 事件按 revision 顺序串行处理：Validator、OnCommit、快照写入都只在事件 goroutine 里调用，**不并发调用回调**；不加缓冲 channel、不丢事件、不乱序。
- 慢消费者：交付给 Kratos 是阻塞的，前一版被取走之前不处理下一版；这是设计，不要为了「不阻塞」改成丢弃或覆盖。阻塞必须能被 ctx 取消打断，`Close()` 不能被慢消费者卡住。
- 跨 goroutine 读写的状态（Committed Revision、degraded）只走原子操作；新增共享状态同样用原子或锁，并在 race 下验证。
- 快照文件同节点多副本共享，写入必须在 flock 下且原子（临时文件 fsync + rename）。
- 每个改动都要回答：出错路径有没有泄漏 goroutine、ctx、etcd watch 流；测试里用 `Close()` 后断言循环退出。

## 向后兼容约束

本仓一个版本会被所有接入服务引用，升级必须让 bifufx-api **不改代码也能编译并保持原行为**。以下任一项变化都是破坏性变更，必须先经用户确认并在报告里标明：

- **公共 API**：`New`、`Source` 的方法、`Validator` / `OnCommit` 签名、`envexpand` 导出函数只增不改不删。
- **Option**：新能力用新增 `With*` 加默认值；已有 Option 的语义与默认值不变；默认值要改必须拍板，并写明对所有接入服务的影响。
- **默认行为**：Load / Watch / 降级 / 退避 / 快照位置的默认表现不变。
- **etcd 协议**：key 规则、meta 字段只能新增可选字段；读 meta 时未知字段忽略、缺失字段按空处理。
- **快照格式**：新版本必须能读旧版本写的快照；读不懂按「meta 不配对」处理（内容可用、revision 为 0），不能崩、不能拒绝启动。
- **错误类型**：`errors.go` 的哨兵值不改名不删除；对外返回的错误用 `%w` 包装，调用方的 `errors.Is` 必须继续成立。
- **指标**：已有指标名、类型、标签集不变；只能新增指标。
- **环境变量**：名字由 bifufx-api 的加载器读取，是共同契约；新增变量必须有默认值，不设也能启动。

## 日志安全

日志前缀保持 `module=confsource`，只记 service、revision、publish_id、错误类型与错误文本。**禁止输出**：配置原文或片段、展开后的变量值、`${VAR}` 对应的实际值、etcd 用户名密码、带凭据的 endpoint、TLS 材料。Validator / OnCommit 返回的错误只能带变量名或字段路径，不能带值；本仓对回调错误一律 `%v` 转发，不自己拼内容；Debug 级别也不例外。

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

验收口径：build / vet / 全量 race 测试绿 + 新场景用例存在且先红后绿 + bifufx-api 本地验证通过（见下）+ 压测方案的客户端事实表已同步。

## 接入 bifufx-api：本地验证与正式升级分开

**本地验证**（不需要 commit / push，不访问远程）：在 bifufx-api 用 `go.work`（`go work use <bifufx-api> <本仓本地路径>`）或在它的 `go.mod` 临时加 `replace github.com/decodeex/bifu-confsource => <本仓本地路径>`，然后 `go build ./... && go test ./utils/configuration/...`。`go.work`、`go.work.sum` 与临时 `replace` **只用于本机验证，不得进入提交**；验证完删掉并确认 `git status` 干净。

**正式升级**（伪版本钉死某个 commit）：
1. 本仓改完、测试绿，**由用户 commit 并 push**（AI 不做）。
2. 解析伪版本不经网络：在 scratchpad 放一份 gitconfig，用 `[url "<本仓本地路径>"] insteadOf = https://github.com/decodeex/bifu-confsource` 把模块地址指到本地克隆，然后 `GIT_CONFIG_GLOBAL=<那份文件> GOFLAGS=-mod=mod go get github.com/decodeex/bifu-confsource@<commit>` 得到 `v0.0.0-<时间戳>-<12位hash>` 与 go.sum。
3. 改 bifufx-api `go.mod` 的 require 行（放在直接依赖块，不带 `// indirect`），**不要 `go mod tidy`**（它会动无关依赖），然后 `go build ./... && go test ./utils/configuration/...`。
4. 告诉用户：bifufx-api 这次只改 `go.mod` / `go.sum`；CI 拉本仓依赖 `.github/gitconfig` 的 SSH 改写与 CI 身份对本仓的读权限。

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
