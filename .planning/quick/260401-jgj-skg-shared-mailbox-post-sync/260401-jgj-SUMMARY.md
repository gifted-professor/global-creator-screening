# Quick Task 260401-jgj Summary

## Task

在仓库根目录补一份 shared-mailbox post-sync 全链路说明文档，并把 `2026-04-01` 这次 `SKG` 的完整复盘一起落地，让当前共享邮箱正式主线、`SKG` 特殊口径、scrape / `Missing` / 自动补抓 / 阻断导出语义、客户填写口径、以及 live writeback 的真实卡点都能被直接查到。

## What Changed

### 1. 新增根目录 runbook，并扩充到当前真实运行口径

新增并持续扩充了 `SHARED_MAILBOX_POST_SYNC_CHAIN.md`，把当前正式主线拆成：

- 共享邮箱同步
- post-sync 主入口
- 上游品牌邮件匹配和 keep-list
- `mail-only update` / `full-screening` 分流
- downstream scrape / prescreen / visual / positioning
- final export / Feishu writeback
- 客户侧输入哪些字段当前真的会被 runtime 吃到
- `2026-04-01` 这次 `SKG` live writeback 的补充结论

### 2. 新增一份单独的 `SKG` 详细复盘

新增了 `SKG_2026-04-01_SHARED_MAILBOX_POST_SYNC_RETRO.md`，把这次实际发生过的事情收成可回看的时间线，包括：

- 为什么 `3403` 封共享邮箱总邮件不能直接理解成博主数
- `712` 封品牌命中邮件如何收敛成 `460 keep / 455 downstream`
- 第一次 run 为什么会出现 `100` 条 final export 的误导性结果
- scrape 差集 / `Missing` / retry 修复后，为什么能做到 `455 -> 455`
- visual 主路 / fallback 的真实表现
- live writeback 为什么第一次看起来像挂住
- 为什么最终要走 “existing-analysis -> 差集 payload -> 分块续传”
- 最终 `455` 条 payload key 全部落表的核对结果

### 3. 把最近已经生效的 shared-mailbox 口径写实了

文档明确记录了这些当前真实行为：

- `SKG-1 / SKG-2` 会收口成一个逻辑任务 `SKG`
- 品牌匹配默认纳入邮件 `from`
- shared-mailbox 路线按邮件内容逐行解析 `达人对接人`
- shared-mailbox 上传飞书时抑制 `标签(ai)`

### 4. 把 scrape / Missing / 补抓语义讲清楚了

文档单独解释了：

- Apify batch 失败不等于整轮直接结束
- `requested - returned` 差集会显式变成 `Missing`
- `Missing` 会自动补抓
- 若 `Missing` 仍未清零，最终导出应被阻断

这部分是为了避免后面再次把“上游 keep 很大、final export 很小”误判成链路已经跑完。

### 5. 补了完整执行 SOP、踩坑清单、客户输入口径、和 live writeback 处理方式

文档现在不只是解释链路结构，也直接给了：

- 从 `inspect-task-upload` 到 live writeback 的推荐执行顺序
- 每一步应该看哪些 summary / artifact
- 已经踩过的关键坑和对应规避方式

这样后续既能照着命令跑，也能提前知道：

- 哪些地方最容易出现“看起来有产物，但语义其实不对”
- 客户侧哪些字段这版真生效，哪些只是会解析
- 如果飞书 live writeback 很慢，应该怎么安全补完，而不是整批反复硬冲

## Outcome

现在仓库根目录已经同时有：

- 一份面向运行和排障的主 runbook
- 一份专门记录 `2026-04-01 SKG` 实跑事实、修复、验证、live 写回过程的详细复盘

后续判断一轮任务是否真正跑完、客户该填什么、以及飞书写回为什么慢时，不需要再只靠聊天记录回忆状态语义。
