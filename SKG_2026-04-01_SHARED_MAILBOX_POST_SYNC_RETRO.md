# SKG 2026-04-01 Shared-Mailbox Post-Sync 复盘

更新时间：`2026-04-01`

这份文档只记录 `2026-04-01` 这次 `SKG` shared-mailbox post-sync 实跑过程中确认过的事实、踩过的坑、修掉的点、以及最终 live writeback 的真实卡点。

如果只想看正式链路说明，先看 `SHARED_MAILBOX_POST_SYNC_CHAIN.md`。  
如果要回放“这次到底发生了什么”，看这份。

## 一句话结论

这次 `SKG` 最终已经完整落表到飞书。

完整结果是：

- shared-mailbox 命中 `712` 封 `SKG` 品牌邮件
- 上游 keep 收敛到 `460` 个 TikTok 创作者
- 其中 `5` 个因为邮件里没解析出负责人，在 shared-mailbox 顶层写回前被挡掉
- 真正进入 downstream full-screening 的是 `455`
- 修完 scrape 差集 / `Missing` / retry 语义后，`455 -> 455` 全部拿到 profile review
- 最终总表 `455` 行：
  - `ai是否通过 = 是`：`123`
  - `ai是否通过 = 否`：`329`
  - `ai是否通过 = 转人工`：`3`
- live writeback 最终也已完成：
  - 先发现表里已部分写入 `183`
  - 再安全续传剩余 `272`
  - 最终飞书里 `455` 个 payload key 全部存在

## 这次之前已经确认并生效的 shared-mailbox 口径

这次运行不是在旧逻辑上直接硬跑，而是在下面这些口径已经生效之后进行的：

- `SKG-1 / SKG-2` 若共享同一套 task assets，会收口成一个逻辑任务 `SKG`
- 品牌邮件匹配默认纳入 `from`
- `达人对接人` 不再整批复用 task owner，而是逐行按邮件内容匹配员工英文名 / 邮箱别名
- shared-mailbox 路线本地保留 `标签(ai)`，但写回飞书时 suppress `标签（ai）`
- scrape 阶段遇到 batch 失败时，不再直接让整轮提前结束；`requested - returned` 差集会显式补成 `Missing`，并自动补抓

## 为什么一开始会有“3403 封邮件怎么只剩一百来个博主”的错觉

`3403` 是共享邮箱本地库里的总邮件数，不是 `SKG` 的博主数。

这次真正跟 `SKG` 有关的层级是：

1. 共享邮箱总库：`3403` 封邮件
2. `SKG` 品牌关键词命中：`712` 封
3. 命中 `发信名单` 邮箱并去重后：`460` 个 keep
4. 负责人解析失败拦掉 `5` 个后，进入 downstream 的是 `455`
5. 修完后最终总表也是 `455`

所以正确理解是：

- 不是“3403 封邮件最后只剩 455 个博主”
- 而是“3403 总库 -> 712 品牌相关邮件 -> 460 keep -> 455 full-screening”

## 为什么 `from` 匹配必须开

这次确认过一个关键 case：

- 邮件正文是 `Lilith` 发出合作询问
- 回复方的真实发件人是 `chanel@arsagendi.com`
- 发信名单里确实有 `chanel@arsagendi.com`

旧逻辑只看：

- `to`
- `cc`
- `bcc`
- `reply_to`

这样会漏掉“达人从自己的邮箱回邮”的场景。

这次确认后，shared-mailbox 主线已经默认 `brand_match_include_from = true`，所以像 `chanel@arsagendi.com -> Lilith` 这种 case 会被正确命中。

## 第一次完整 `SKG` 重跑为什么看起来“有产物，但没跑完”

第一次 shared-mailbox 全链路 run root 是：

- `temp/shared_mailbox_post_sync_20260401_124546`

这轮关键现象是：

- 上游 keep 已经扩到 `460`
- 但最终总表只有 `100`
- 顶层状态不是完全成功，而是 partial/with_failures 语义

真正的问题不在视觉，也不在飞书上传，而是在 **TikTok scrape 阶段**。

当时确认到的事实是：

- 请求抓取 `455` 个 TikTok identifiers
- Apify 轮询过程中出现 `502 Bad Gateway`
- 下游代码当时会拿着已经回来的 partial result 继续跑 visual / positioning / export
- 所以会出现“上游 keep 很大、final export 只有 100”这种很误导的结果面

这次最关键的认知变化是：

- 那一轮不是“跑完了只通过 100 个”
- 而是“只抓回了足够生成 `100` 条 review 的数据，但代码继续往下走了”

## 这次修掉的核心坑：requested 不等于 returned 时不能装作没事

这次修的是 scrape 语义，不是调一个数字。

旧问题：

- 某个 Apify batch 挂掉后，runner 可能过早结束
- 没回来的 identifiers 没有被显式暴露
- 最终只拿部分 review 继续往下产 visual / final review
- 用户看到的是缩水结果，却不知道哪些账号压根没抓到

修复后的语义：

1. batch 失败时，不再直接让整轮提前结束
2. 主抓取阶段跑完后，统一做 `requested - returned` 差集
3. 差集里的 identifiers 显式补成 `Missing`
4. 对 `Missing` 自动补抓
5. 如果补抓后还有 `Missing`，那这轮就不应该被当成完整收敛

这次 `SKG` 重跑已经证明这个修复生效：

- 一度出现 `Missing`
- 最后补齐到 `requested_identifier_count = 455`
- `profile_review_count = 455`
- `Missing = 0`

## 修复后那轮真正成功的 downstream run

修复后完整 downstream run root 是：

- `temp/skg_downstream_retryfix_20260401_1`

这轮关键结果：

### Scrape / prescreen

- `requested_identifier_count = 455`
- `profile_review_count = 455`
- `Pass = 236`
- `Reject = 219`
- `Missing = 0`

这里的 `219` 不是丢了，也不是没跑到，而是在数据 / prescreen 层就被筛掉了。

### Visual / positioning

- 进入 visual 的是 `236`
- visual 结果拆开是：
  - `Pass = 123`
  - `Reject = 110`
  - `Error = 3`
- positioning 完成 `123`

### Final review

最终总表在：

- `temp/skg_downstream_retryfix_20260401_1/exports/all_platforms_final_review.xlsx`

这张表是完整覆盖 `455` 行，不是只保留 visual pass。

最终拆分：

- `ai是否通过 = 是`：`123`
- `ai是否通过 = 否`：`329`
- `ai是否通过 = 转人工`：`3`

也就是说：

- prescreen reject / visual reject / visual error 都会在 final review 里保留下来
- “视觉只过了 123 个” 不等于 “最终只产出 123 行”

## 这次视觉 provider 的真实表现

这轮 visual 数据在：

- `temp/skg_downstream_retryfix_20260401_1/data/tiktok/tiktok_visual_results.json`

真实 provider 分布是：

- `openai / gpt-5.4`：`211`
- `reelx / qwen-vl-max`：`22`
- 无 provider 的异常项：`3`

所以结论不是“全都是 openai”，而是：

- 主路默认确实是 `openai`
- fallback 这次确实救回了 `22` 条
- 但 fallback 不是完全无异常

### 这轮 visual error 的真实原因

这轮 `3` 条异常不是逻辑没走到，而是 fallback provider 接口问题：

- `apollo_und_cosmo`
- `comyahmed`
- `rehaexperte`

错误一致，都是：

- `reelx: HTTP 413 请求体过大；上游返回 HTML 错误页（413 Request Entity Too Large）`

这说明当前更准确的描述是：

- `openai` 偶发会有 `500 / EOF`
- `reelx` fallback 能接住一部分
- 但在大请求体场景，`reelx` 还会报 `413`

## shared-mailbox 顶层还有一个独立拦截：负责人解析失败

shared-mailbox 顶层在下游之前，还会做一层“能不能安全写回飞书负责人”的判断。

这次上游 keep 是 `460`，但真正进入 downstream 的是 `455`，差掉的 `5` 个不是 scrape 问题，而是写回前就被挡掉了。

这 `5` 个是：

- `sparklylife`
- `micasa_mipaz_official`
- `poopa.loves.loopa`
- `lindseyanjel1`
- `poonam.sidhu`

失败原因一致：

- `stage = pre_upload_validation`
- `reason = 邮件内容未命中任何负责人英文名或邮箱别名，已跳过写回`

所以这 `5` 个属于：

- 上游已经命中邮件
- 但当前邮件内容无法稳定判断应该挂给哪个员工
- 因此 shared-mailbox 主线选择先挡住，不让它模糊写回

## 这次 live writeback 实际踩到的坑

downstream 跑完之后，真正的 live writeback 还踩了第二类坑：**飞书写入吞吐很慢**。

### 第一次 full live upload 的现象

第一次我直接拿整份 payload 做 live upload，调用的是：

- `upload_final_review_payload_to_bitable(...)`

输入是：

- payload：`temp/skg_downstream_retryfix_20260401_1/exports/all_platforms_final_review_payload.json`
- `dry_run = False`
- `suppress_ai_labels = True`

现象是：

- 进程长时间没有产出最终 result json
- 但并不是 schema/权限直接报错
- 我中途把那个长时间挂着的进程 kill 掉了

### 为什么后来确认它不是“完全没写进去”

我后面做了两步验证：

1. 先做 `limit = 1` 的 live probe  
   结果是成功的，而且第一条被判断为 `skipped_existing = 1`
2. 再对目标飞书表做 existing-analysis  
   发现完整 payload 的 `455` 个 key 里，已经有 `183` 个存在

这说明：

- 第一次 full live upload 不是完全没动
- 它其实已经部分写进飞书
- 只是本地迟迟没有等到最终收口结果

### 这次真正暴露出来的 live writeback 卡点

不是：

- 字段缺失
- 飞书权限不够
- `达人对接人` 字段缺失
- 目标表已有重复主键

这些都不是。

这次确认到的真实卡点是：

- **飞书 live 写入吞吐偏慢**
- 全量一次性直传会让本地看起来像“卡住”
- 但实际上它是在慢慢写

### 为什么不能只看目标表里 `任务名 == SKG`

这次还确认了一个容易误判的点：

- 目标表里单纯按 `任务名 == SKG` 去数记录，并不能可靠判断 payload 写入完成度

更可靠的判断方式是：

- 用 uploader 的主键去做 existing-analysis
- 主键语义是：`达人对接人 + 达人ID + 平台`

因为这次表里记录数量已经增加，但 `任务名` 字段并不是最可靠的追踪锚点。

## 这次 live writeback 最终是怎么安全补完的

做法不是重新整批硬跑，而是：

1. 先读目标表现状
2. 用 uploader key 对原始 payload 做差集
3. 只保留“飞书里还不存在”的那部分 rows
4. 把缺口 payload 分成小块 live 写回

这次实际差集结果是：

- 原始 payload：`455`
- 已存在 key：`183`
- 缺口 rows：`272`

缺口 payload 在：

- `temp/skg_live_resume_20260401/remaining_payload.json`

然后把这 `272` 条拆成 `6` 块：

- `50`
- `50`
- `50`
- `50`
- `50`
- `22`

每块都直接 live 写回飞书。

块级结果在：

- `temp/skg_live_resume_20260401/chunks/resume_summary.json`

最终结果是：

- `chunk_count = 6`
- `created_total = 272`
- `updated_total = 0`
- `failed_total = 0`
- `all_ok = true`

也就是说，这次安全续传是完整成功的。

## 最终飞书落表核对结果

最后我对整份 payload 又做了一次 existing-analysis 回查。

结果是：

- `payload_rows = 455`
- `existing_keys_in_table = 455`
- `missing_payload_keys_after_resume = 0`
- `duplicate_existing_groups = 0`
- `owner_scope_missing_record_count = 0`

所以截至这次复盘时，结论已经可以写死：

- `SKG` 这轮最终结果已经完整写入飞书
- 没有残留 payload key 缺口
- 没有因为这次写入引入新的重复主键问题

## 这次之后仍然还在的已知问题

这次不能理解成“从此完全没有遗留问题”，更准确地说是：

- 主链路这次已经跑通
- 但还有几类问题还没被彻底产品化或彻底消灭

### 1. visual fallback 还有 `413` 类异常

这次 visual 最终虽然跑完了，但仍然有 `3` 条异常：

- `apollo_und_cosmo`
- `comyahmed`
- `rehaexperte`

它们都不是主链没走到，而是 fallback provider `reelx` 返回：

- `413 Request Entity Too Large`

所以当前状态是：

- visual 主路 + fallback 已经明显可用
- 但 fallback 还不是 100% 稳

### 2. 负责人路由覆盖还不算完备

这次 `460 keep` 里有 `5` 条最后没有进入 downstream，不是因为邮件没命中，而是因为邮件内容里没解析出负责人。

所以当前状态是：

- 多负责人品牌已经能逐行路由
- 但员工英文名 / 邮箱别名表如果覆盖不够，仍然会挡掉一小批记录

### 3. live writeback 的 chunked resume 这次是手工运营策略，不是主线自动能力

这次最终补完飞书写回，是靠：

- 先 existing-analysis
- 再做 payload 差集
- 再分块续传

它现在已经被证明是正确 SOP，但还不是 shared-mailbox 主线自带的默认自动恢复机制。

### 4. 客户侧主表字段支持面还是“部分 runtime 生效”

这次已经确认：

- `适用平台`
- `审核目标`
- 数据阈值
- visual 特征 / 排除项

这批字段会真正影响当前 runtime。

但像：

- `参考账号`
- `反例账号`
- `判定关系`

这批字段现在还不能当成完全生效的自动规则输入。

## 这次之后的后续待修清单

按这次实际踩坑排序，后面最该继续收的是：

1. visual fallback 的 `413` 问题，避免大请求体样本继续落成异常行。
2. 员工英文名 / 邮箱别名覆盖，让 shared-mailbox 顶层少挡掉负责人无法解析的记录。
3. 把 live writeback 的分块续传流程产品化，避免大 payload 还要人工补传。
4. 继续把客户主表里“已解析但未真生效”的字段支持面补齐，减少填写预期和 runtime 行为之间的落差。

## 这次对客户填写口径确认过的东西

这次还额外确认了一件运营侧很容易误解的事：

- 客户那张“标准化筛号需求主表”并不是所有字段当前都真正在 runtime 里生效

### 当前版本真正值得客户认真填的

- `适用平台`
- `审核目标`
- 数据阈值
- `查看封面数量`
- `至少命中几类特征`
- `需要的特征清单`
- 排除项阈值 / 开关
- 人工判断项

### 当前版本会被解析，但不是强 runtime 决策输入的

- `参考账号`
- `反例账号`
- `备注`
- `品牌 / 产品使用场景`
- `判定关系`

更关键的是：

- 对 shared-mailbox 这条线，真正决定回邮能不能命中的，不是这张需求主表
- 而是 **发信名单里的邮箱字段**

所以客户侧最容易踩的坑，仍然是：

- 发信名单里没有 `Email`
- 一个达人多个邮箱只填了一个
- 任务里挂的不是最新发信名单

## 这次之后应该记住的运行原则

### 1. 不要只看 final export

一轮是不是完整跑完，先看：

- `requested_identifier_count`
- `profile_review_count`
- `Missing`

### 2. shared-mailbox 命中要默认看 `from`

否则真实回邮会漏。

### 3. `SKG-1 / SKG-2` 这种共用资产任务不能重复跑

要收口成一个逻辑任务。

### 4. `达人对接人` 不能整批沿用 task owner

要逐行按邮件内容解析。

### 5. shared-mailbox 飞书写回不要一次性全量硬冲

如果 payload 很大，更稳的办法是：

- 先做 existing-analysis
- 再只传缺口
- 必要时分块续传

### 6. 这条线当前本地保留 AI 标签，但飞书不写 AI 标签

也就是：

- 本地 workbook / payload 可以继续看 `标签(ai)`
- 飞书里当前不要期待 `标签（ai）` 已经被写上去

## 本次复盘涉及的关键产物

shared-mailbox 首次完整 run：

- `temp/shared_mailbox_post_sync_20260401_124546`

修复后完整 downstream run：

- `temp/skg_downstream_retryfix_20260401_1`

live writeback 缺口续传：

- `temp/skg_live_resume_20260401`

正式链路说明：

- `SHARED_MAILBOX_POST_SYNC_CHAIN.md`
