# Feature Dev Workflow（bifu-confsource 配置客户端封装）· 使用导读

> 规则原文见 [SKILL.md](SKILL.md)，AI 照它执行。本文给人看。配置平台三个仓各有一份同名 skill：config-center-service（平台后端）、config-center-frontend（页面）、**本仓（服务侧客户端）**。

## 它管什么

跑在每个接入服务 Pod 里的那个库：Load / Watch / 快照降级 / 校验与提交回调 / 指标 / 连接参数。一个 bug 会同时出现在全部服务上，所以这里的要求是：公共 API 不随手改、每个行为改动配内嵌 etcd 用例、race 必过、改完同步 bifufx-api 的伪版本。

## 怎么跑

```text
① 你给需求（系分客户端章节、方案文档里程碑、或口头）
② AI 分析：是否触及公共 API / etcd 协议 / 快照格式 / 指标，列受影响的 bifufx-api 与平台位置 → 停在「要不要开始」
③ 你说「开始」
④ AI 按里程碑做：写用例（红）→ 实现（绿）→ 全量 race → 影响范围说明 → 报告
⑤ 收尾：给 bifufx-api 的升级说明、给平台的协议变更说明（如有）、压测方案客户端事实表的更新
```

## 你要出手的点

1. **公共 API 或协议要改时拍板**：AI 会先列受影响点，不自己改签名。
2. **Git 与发布**：AI 只改文件；commit、push 由你做；推上去之后 AI 用本地路径替换解析伪版本并改 bifufx-api 的 `go.mod`，不 `go mod tidy`。
3. **bifufx-api 的配合改动**：加载器 `utils/configuration/etcd.go` 要不要改，AI 会在报告里明说。
4. **CI**：bifufx-api 拉本仓靠 `.github/gitconfig` 的 SSH 改写，CI 身份对本仓的读权限由你与 SRE 确认。

## 什么时候别用它

- 平台侧改动（后端 skill）、页面（前端 skill）。
- bifufx-api 自己的业务代码。
