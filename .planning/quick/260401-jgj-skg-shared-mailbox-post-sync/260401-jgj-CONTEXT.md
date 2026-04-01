# Quick Task 260401-jgj Context

## Task

在仓库根目录补一份共享邮箱 `shared-mailbox post-sync` 全链路说明文档，把 `SKG` 这轮实际验证过的运行语义一起写清楚，避免之后继续靠口头回忆判断“抓了多少、为什么继续、哪里会阻断”。

## Scope

- 只写文档，不改 runtime 行为
- 文档落在仓库根目录，方便非开发同事直接打开
- 覆盖共享邮箱同步、任务分堆、品牌邮件匹配、keep-list、scrape、visual、positioning、飞书写回和失败语义
- 把 `SKG-1/2 -> SKG`、`include_from`、`Lilith/Rhea`、`suppress_ai_labels`、`Missing 自动补抓` 这些最近确认过的行为一起写进去

## Why

最近 shared-mailbox 主线发生了几次关键口径变化：

- `SKG-1/2` 已收口成单逻辑任务
- 品牌匹配默认纳入 `from`
- shared-mailbox 写回飞书时不上传 `标签(ai)`
- scrape 批次失败后不该再偷偷产出残缺 final review

如果没有一份根目录 runbook，后续很容易再次混淆“keep 数、review 数、Missing、最终导出数”各自代表什么。
