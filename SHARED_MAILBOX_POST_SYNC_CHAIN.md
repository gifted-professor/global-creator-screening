# Shared Mailbox Post-Sync 链路说明

更新时间：`2026-04-01`

详细复盘另见：

- `SKG_2026-04-01_SHARED_MAILBOX_POST_SYNC_RETRO.md`

这份文档只解释当前仓库里已经落地的共享邮箱正式主线，重点覆盖：

- 共享邮箱同步
- shared-mailbox post-sync 主入口
- `SKG` 这类按任务名运行的真实语义
- `keep-list -> Apify -> visual -> positioning -> export -> upload`
- 抓取失败、补抓、`Missing`、阻断导出的行为

## 一句话概述

当前正式生产主线是：

1. 先把共享邮箱同步到本地 `email_sync.db`
2. 再运行 `scripts/run_shared_mailbox_post_sync_pipeline.py`
3. 从飞书 `task-upload` 拉有效任务并自动分堆
4. 用共享邮箱里命中的品牌邮件去匹配任务的 `发信名单`
5. 新达人走完整筛号链路，老达人只做邮件增量更新
6. 最后把结果按任务写回目标飞书表

## 正式入口

共享邮箱同步：

```bash
python3 scripts/run_shared_mailbox_sync.py \
  --env-file .env \
  --sent-since 2026-03-26
```

shared-mailbox post-sync：

```bash
python3 scripts/run_shared_mailbox_post_sync_pipeline.py \
  --shared-mail-db-path data/shared_mailbox/email_sync.db \
  --env-file .env \
  --task-name "SKG" \
  --upload-dry-run
```

推荐顺序永远是：

1. 先同步共享邮箱
2. 再跑 `--upload-dry-run`
3. 检查 `summary.json` 和 payload
4. 最后才去掉 `--upload-dry-run`

## 完整执行 SOP

如果现在要按任务名跑一轮完整 shared-mailbox 主线，最推荐按下面顺序走。

### Step 0：先确认任务资产是齐的

```bash
python3 -m feishu_screening_bridge inspect-task-upload \
  --env-file .env \
  --download-templates \
  --json
```

重点确认：

- `taskName`
- `linkedBitableUrl`
- template 附件
- `发信名单`
- 员工信息 / 负责人映射

### Step 1：先同步共享邮箱

```bash
python3 scripts/run_shared_mailbox_sync.py \
  --env-file .env \
  --sent-since 2026-03-26
```

如果只是试跑，可以先加：

```bash
--limit 20
```

### Step 2：先 dry-run 跑 shared-mailbox 主线

```bash
python3 scripts/run_shared_mailbox_post_sync_pipeline.py \
  --shared-mail-db-path data/shared_mailbox/email_sync.db \
  --env-file .env \
  --task-name "SKG" \
  --upload-dry-run
```

### Step 3：看上游是不是对了

重点先看：

- 顶层 `summary.json`
- 每个任务自己的 `summary.json`
- upstream `summary.json`

先确认这几个数字：

- `matched_mail_count`
- `mail_only_update_count`
- `full_screening_count`
- `final_keep_row_count`

### Step 4：再看 downstream 有没有真正跑全

重点看 downstream `summary.json` 和 `tiktok_profile_reviews.json`。

最重要的是这三个数：

- `requested_identifier_count`
- `profile_review_count`
- `missing_profile_count`

正确理解方式是：

- `requested_identifier_count`
  这轮本来想抓多少账号
- `profile_review_count`
  实际已经生成了多少 review
- `missing_profile_count`
  请求了但最终没回来的账号还有多少

### Step 5：确认 visual / positioning / final review 产物出来了

如果 scrape 收敛了，再继续看：

- `tiktok_visual_results.json`
- `tiktok_final_review.xlsx`
- `tiktok_positioning_card_review.xlsx`
- `all_platforms_final_review.xlsx`
- `all_platforms_final_review_payload.json`

### Step 6：dry-run 没问题，再 live writeback

确认上面都正常后，再去掉：

```bash
--upload-dry-run
```

## 当前版本实际吃到的输入字段

这一节只讲**当前版本真实生效**的字段，不讲理想状态。

要区分 3 层：

1. 筛号需求主表
2. 发信名单
3. 任务上传 / 回信管理表 / 负责人映射

### 1）筛号需求主表：当前真正会影响 runtime 的字段

这批字段现在确实会进入自动链路：

- `适用平台`
  会决定当前任务实际跑哪些平台。
- `审核目标`
  会进入 visual / positioning 的 prompt 上下文。
- `平均播放量阈值`
  会进入数据审核规则。
- `中位数播放量阈值`
  会进入数据审核规则。
- `粉丝数阈值（可选）`
  会进入数据审核规则。
- `最近活跃要求（天，可选）`
  会进入数据审核规则。
- `查看封面数量`
  会影响 visual 最多看多少张封面。
- `至少命中几类特征`
  会影响 visual 正向命中特征门槛。
- `需要的特征清单`
  其中属于当前 runtime 支持的特征，会进入 visual feature group。
- `排除项审核` 里的已支持阈值 / 开关
  会进入 visual exclusion 或 runtime exclusion 规则。
- `人工判断项 / 合规提醒`
  会进入 prompt 和人工提醒语义。

### 2）筛号需求主表：当前会被解析，但不是强 runtime 决策输入的字段

这批字段现在**不是“没用”**，但不能把它们理解成已经稳定驱动自动判定：

- `项目名称（品牌名）`
  更偏上下文 / 编译信息。
- `品牌 / 产品使用场景`
  更偏上下文，不是硬 gate。
- `参考账号`
  当前会解析，但没有直接进入自动筛号决策。
- `反例账号`
  当前会解析，但没有直接进入自动筛号决策。
- `备注`
  当前更偏备注信息。
- `判定关系`
  当前 parser 会收，但现版本不能把它当成稳定 runtime 逻辑总开关。

### 3）筛号需求主表：当前只有部分平台或部分能力支持的字段

- `地区要求`
  当前不是全平台稳定自动执行；现阶段更偏部分平台支持。
- `语言要求`
  同样不是全平台稳定自动执行。
- 某些特别细的视觉特征
  只有 runtime 已支持的特征会真正进自动 visual；其余更像保存在 spec / prompt 里。

所以如果只问“这版客户主表里哪些值得认真填”，优先级应该是：

- `适用平台`
- `审核目标`
- 数据阈值字段
- visual 正向特征
- visual 排除项
- 人工判断项

而不是把 `参考账号 / 反例账号` 当成当前版本的核心驱动字段。

## 当前版本对发信名单的真实要求

shared-mailbox 这条主线能不能把回信邮件准确匹配回达人，关键不在需求主表，而在发信名单。

### 1）当前版本最低限度必须有的列

按当前链路，发信名单至少要能稳定提供：

- `Platform`
- `@username`
- `URL`
- `Email`

对 shared-mailbox 邮件命中来说，`Email` 尤其关键。

因为当前主匹配逻辑是：

- 先品牌词命中邮件
- 再用发信名单邮箱去命中邮件地址槽位

如果没有邮箱，很多真实回邮场景根本匹配不上。

### 2）当前版本强烈建议补齐的列

- `Region`
- `Language`
- `nickname`
- `Followers`

这些字段不是 shared-mailbox 邮件命中的第一锚点，但会影响：

- 下游 upload metadata
- 平台预览
- 部分 prescreen / 诊断可见性

### 3）发信名单最容易踩的坑

- 只填昵称，不填 `@username`
- 只填达人名，不填 `Email`
- 多个邮箱账号只保留一个，漏掉代理 / MCN 邮箱
- 发出去的是新名单，任务里挂的还是旧名单

## 当前版本链路外但必须齐的配置

除了主表和发信名单，下面这些现在也是硬依赖：

- `taskName`
  shared-mailbox post-sync 需要靠它分堆和筛选任务。
- `linkedBitableUrl`
  决定最终往哪张表写回。
- `模板附件`
  决定当前 rulespec / visual prompt contract。
- `发信名单附件`
  决定邮件匹配和下游 canonical upload。
- `AI回信管理` 子表
  目标表必须存在这个名字的子表。
- 员工信息里的英文名 / 邮箱
  当前负责人路由要靠这个识别 `Lilith / Rhea` 这类邮件归属。

## 这版最值得客户认真填的内容

如果只想收一个“本版本最小必填口径”，可以直接按下面给客户：

### 需求主表里最关键的

- `适用平台`
- `审核目标`
- `平均播放量阈值`
- `中位数播放量阈值`
- `粉丝数阈值`
- `最近活跃要求`
- `查看封面数量`
- `至少命中几类特征`
- `需要的特征清单`
- `排除项阈值 / 开关`
- `人工判断项`

### 发信名单里最关键的

- `Platform`
- `@username`
- `URL`
- `Email`

### 任务上传里最关键的

- `taskName`
- 模板附件
- 发信名单附件
- 回信管理表链接

### 员工信息里最关键的

- 中文名
- 英文名
- 邮箱
- 常见签名别名（如果有）

## 避坑总览

这条链路当前最容易踩的坑，不是“命令不会跑”，而是“跑出了一个看起来像结果的东西，但语义其实不对”。

下面这几条是已经踩过、并且之后还最容易复发的坑。

### 坑 1：只看 final export 行数，以为整轮跑完了

错误理解：

- 上游 keep 有很多行
- final review 也有产物
- 所以整轮应该已经完成

真实情况：

- final export 只是最后一个结果面
- 它不能单独证明 scrape 是否完整

正确做法：

- 先看 `requested_identifier_count`
- 再看 `profile_review_count`
- 再看 `missing_profile_count`

如果这三层没对齐，不能只靠 final review 判断整轮成功。

### 坑 2：Apify 失败后，没抓到的账号会“消失”

这是之前已经踩过的真实坑。

旧问题是：

- 某个 batch 失败后，链路可能拿着 partial result 继续往下跑
- 没回来的账号既没被补抓，也没被显式暴露
- 最后只剩一份缩水的 final review

现在正确语义是：

- batch 失败后，不能直接把账号当不存在
- `requested - returned` 差集必须显式变成 `Missing`
- `Missing` 必须自动补抓
- 如果补抓后还有 `Missing`，final export 应阻断

### 坑 3：把 `SKG-1 / SKG-2` 当两条独立逻辑任务

这会导致：

- 同一套任务资产被重复跑
- 同一批邮件被重复匹配
- 同一批达人被重复筛号

当前正确口径是：

- 如果两个 task row 共享同一套 task assets
- 那它们应该收口成一个逻辑任务 `SKG`

### 坑 4：只看收件人地址，不看回邮发件人

这个坑会漏掉真实合作回复。

典型例子就是：

- 邮件是 `chanel@arsagendi.com -> Lilith`
- 发信名单里有 `chanel@arsagendi.com`
- 但如果只看 `to/cc/bcc/reply_to`，这封就会漏掉

当前 shared-mailbox 主线已经修成：

- 默认把 `from` 也纳入品牌邮件地址命中

### 坑 5：把 `达人对接人` 当成 task owner 的固定字段

这在多负责人品牌里会写错人。

`SKG` 的真实情况是：

- 同品牌下可能同时存在 `Lilith` 和 `Rhea`
- 最终写回飞书时不能整批沿用一个 task owner

当前正确口径是：

- 按邮件内容逐行解析员工英文名 / 邮箱别名
- 再逐行写回 `达人对接人`

### 坑 6：以为 shared-mailbox 没有 AI 标签

更准确的说法是：

- 本地 payload / workbook 仍然保留 `标签(ai)`
- 只是当前 shared-mailbox 写回飞书时显式跳过 `标签（ai）`

所以：

- 本地诊断仍然存在
- 飞书写回当前不带这个字段

### 坑 7：把 `/operator` 当成唯一正式入口

当前更稳的生产口径仍然是 CLI。

`/operator` 现在更适合：

- bounded run
- 观察 summary
- 下载产物

而不是替代 shared-mailbox CLI 主线。

## 阶段 1：共享邮箱同步

入口文件是 `scripts/run_shared_mailbox_sync.py`。

它的职责很单纯：

- 登录共享邮箱 `partnerships@amagency.biz`
- 从 `其他文件夹/邮件备份` 拉邮件
- 把邮件、附件索引、线程索引写到本地 `email_sync.db`

这一步不做任务分堆，不做筛号，只负责把共享邮箱沉淀成 repo-local 数据库。

默认主产物：

- `data/shared_mailbox/email_sync.db`
- `data/shared_mailbox/raw/`

## 阶段 2：post-sync 主入口

入口文件是 `scripts/run_shared_mailbox_post_sync_pipeline.py`。

这一步开始，链路不再碰 IMAP，而是直接消费本地共享邮箱库。

它会先做这几件事：

1. 读取本地 `email_sync.db`
2. 从飞书 `task-upload` 拉当前有效任务
3. 按项目名或任务名自动分堆
4. 为每个任务准备上游 `keep-list` 和下游 screening run

当前共享邮箱主线有 4 个已经确认的业务语义：

### 2.1 `SKG-1 / SKG-2` 会被收口成一个逻辑任务 `SKG`

如果多行 `task-upload` 实际指向：

- 同一个 `linkedBitableUrl`
- 同一个 template
- 同一个 `发信名单`

那 shared-mailbox 主线会把它们收口成一个逻辑任务跑一次，而不是重复跑两轮。

### 2.2 品牌关键词匹配默认会纳入邮件 `from`

当前 shared-mailbox 主线默认 `brand_match_include_from = true`。

这意味着品牌邮件匹配时，不只看：

- `to`
- `cc`
- `bcc`
- `reply_to`

也会看：

- `from`

所以像 `chanel@arsagendi.com -> Lilith` 这种“达人从发信名单邮箱回邮”的场景，不会再漏。

### 2.3 `达人对接人` 不是整批复用 task owner，而是逐行解析

对于像 `SKG` 这种同品牌但由多个同事跟进的任务，最终写回飞书时：

- 不是整批统一写同一个负责人
- 而是从邮件内容里匹配员工英文名 / 邮箱别名
- 再逐行写回 `Lilith` 或 `Rhea`

### 2.4 shared-mailbox 写回飞书时会抑制 `标签(ai)`

当前 shared-mailbox 主线会保留本地 `标签(ai)` 诊断，但不会把 `标签(ai)` 写回飞书。

也就是：

- 本地 workbook / payload 里还能看到 `标签(ai)`
- 真正写回飞书时会跳过 `标签（ai）`

## 阶段 3：上游品牌邮件匹配和 keep-list

这一段本质上是共享邮箱版的 `brand-keyword-fast-path`。

核心步骤：

1. 先用品牌关键词从共享邮箱里捞出相关邮件
2. 再拿任务的 `发信名单` 去做邮箱命中
3. 再对命中结果去重、拆 shared email、做归并
4. 最后生成本轮 canonical `keep-list`

这里的关键点不是“按博主名模糊找”，而是：

- 先品牌词命中邮件
- 再用 `发信名单` 里的邮箱字段去命中邮件地址
- 博主信息主要用于 profile 级去重和 shared-email 归并

当前地址命中来源包括：

- `to`
- `cc`
- `bcc`
- `reply_to`
- `from`

这一步常见产物包括：

- `*_brand_keyword_match.xlsx`
- `*_brand_keyword_match_deduped.xlsx`
- `*_shared_email_resolution_final_keep.xlsx`
- upstream `summary.json`

这一步结束后，任务会被拆成两类：

- `mail-only update`
- `full-screening`

## 阶段 4：mail-only update 和 full-screening 分流

shared-mailbox 主线不会把所有命中的达人都重新筛一遍，而是先看飞书目标表里有没有现成记录。

判定逻辑是：

- 飞书里不存在该达人当前主键：创建新记录，走完整筛号
- 飞书里存在记录，但 `ai是否通过` 为空：补跑完整筛号并更新记录
- 飞书里存在记录，且 `ai是否通过` 非空：只更新邮件字段和最新 `.eml`

也就是说：

- 已经筛过的达人，不会因为收到新邮件就重新跑完整下游
- 只有真正需要新建或补筛的达人，才会进入 `keep-list -> screening`

## 阶段 5：downstream screening

下游入口是 `scripts/run_keep_list_screening_pipeline.py`。

它消费的是上游已经沉淀好的 `keep workbook`，然后按平台跑标准筛号链路。

在 `SKG` 这轮里，当前 keep 里的平台都是 `TikTok`，所以只会跑 TikTok。

### 5.1 scrape

第一步是 Apify 抓取。

流程是：

1. 从 `keep workbook` 提取平台 identifier
2. 按 batch 发给 Apify
3. 把回来的 raw items 合并到本地 `tiktok_data.json`
4. 过滤成 `profile_reviews`

当前这一段最重要的修复是：

- 如果某个 batch 直接失败，不会再导致整轮过早退出
- runner 会先继续其他批次
- 等主抓取阶段结束后，再统一计算“请求了但没回”的差集

### 5.2 `requested - returned` 差集会显式变成 `Missing`

现在链路不会再默默吞掉没回来的账号。

对于本轮请求了、但最终没进入 `profile_reviews` 的 identifier：

- 会被补成 `status = Missing`
- 会进入 `missing_profiles`
- 会在 summary 里显式可见

### 5.3 Missing 会自动补抓

当前 runner 会对 `Missing` 账号自动再补抓一轮。

也就是说：

- 主抓取失败过的 batch，不会直接让账号消失
- 差集会先被显式化
- 再走自动补抓

`2026-04-01` 这轮 `SKG` 重跑已经验证到了这一点：

- 开始时只回了部分 review
- 中途显式出现 `Missing`
- 最终补到 `requested = 455`、`profile_review_count = 455`、`Missing = 0`

### 5.4 还有 Missing 时，final export 必须阻断

如果自动补抓以后仍然有 `Missing`，这轮不会再被允许安静地产出 final review。

当前正确语义应该理解成：

- `Missing = 0` 才允许继续走 visual / final export
- 如果还有 `Missing`，这轮应该被视为未完整收敛

## 阶段 6：prescreen / visual / positioning

当 scrape 收敛后，downstream 会继续跑：

1. `prescreen`
2. `visual review`
3. `positioning_card_analysis`

规则是：

- `profile_reviews` 会先给出 `Pass / Reject / Missing`
- 只有 `Prescreen = Pass` 的账号进入 visual
- 只有 `Prescreen = Pass` 且 `Visual = Pass` 的账号进入 positioning

定位卡分析当前是附加分析阶段：

- 会产出独立 artifact
- 默认不改变原有 visual gate
- 第一版是 non-blocking

常见产物：

- `tiktok_profile_reviews.json`
- `tiktok_visual_results.json`
- `tiktok_final_review.xlsx`
- `tiktok_positioning_card_review.xlsx`

## 阶段 7：汇总导出和飞书写回

当平台级 final review 生成后，shared-mailbox wrapper 会继续做：

1. 聚合 all-platform final review
2. 生成 upload payload
3. 跑 Feishu dry-run 或 live writeback

常见总产物：

- `exports/all_platforms_final_review.xlsx`
- `exports/all_platforms_final_review_payload.json`
- `exports/feishu_upload_local_archive/feishu_bitable_upload_result.json`
- `exports/feishu_upload_local_archive/feishu_bitable_upload_result.xlsx`

目标表优先级当前是：

1. `AI回信管理`
2. `达人管理`

shared-mailbox 路线上传时还有两个明确约束：

- `达人对接人` 必须能从邮件内容解析到员工
- `标签(ai)` 当前不上传飞书

## 当前推荐查看的 summary / artifact

如果只想快速判断一轮 run 有没有对，先看这几个：

1. 顶层 `summary.json`
   看整轮状态、任务列表、`task_results`
2. 每个任务自己的 `summary.json`
   看 `matched_mail_count`、`mail_only_update_count`、`full_screening_count`
3. upstream `summary.json`
   看 `message_hit_count`、`final_keep_row_count`
4. downstream `summary.json`
   看 `requested_identifier_count`、`profile_review_count`、`missing_profile_count`
5. `failed_or_skipped_records.json`
   看哪些记录因为负责人解析或上传前校验被挡掉

## 2026-04-01 `SKG` 重跑验证结论

这轮是 shared-mailbox `include_from` 修正后、并补上 scrape 差集显式化 / 自动补抓语义之后的一次完整重跑。

重跑目录：

- `temp/skg_downstream_retryfix_20260401_1`

这轮最重要的结论有 4 个：

### 1）抓取阶段已经证明不会再偷偷丢号

这轮实际结果是：

- `requested_identifier_count = 455`
- `profile_review_count = 455`
- `Missing = 0`

这说明至少在这轮 `SKG` 上，之前那种“请求了很多，但只 silently 产出部分 review”的坑没有再出现。

### 2）prescreen / visual / positioning 都完整收尾了

这轮平台级结果是：

- `prescreen Pass = 236`
- `prescreen Reject = 219`
- `visual Pass = 123`
- `visual Reject = 110`
- `visual Error = 3`
- `positioning Completed = 123`

也就是说：

- 不是所有 `455` 都进入视觉
- 而是先经过 prescreen
- 只有 prescreen pass 的 `236` 个才继续走 visual
- 其中 visual pass 的 `123` 个才继续走 positioning

### 3）最终总表是完整覆盖 `455` 行，不是只保留 visual pass

这轮最终总表：

- `all_platforms_final_review.xlsx` 行数是 `455`
- 其中：
  - `ai是否通过 = 是`：`123`
  - `ai是否通过 = 否`：`329`
  - `ai是否通过 = 转人工`：`3`

这件事很重要，因为它说明：

- 最终总表的行数不等于 visual pass 数
- prescreen reject / visual reject / visual error 也会保留在最终结果里
- 所以“视觉只过了 123 个”不等于“最终只产出了 123 行”

### 4）当前视觉主路是 `openai`，但 fallback 不是摆设

这轮视觉里：

- `openai / gpt-5.4` 成功处理：`211`
- `reelx / qwen-vl-max` fallback 成功处理：`22`
- 异常：`3`

这说明当前视觉现状是：

- 主路默认仍然是 `openai`
- fallback 已经证明可以接住一部分主路失败样本
- 但 fallback 还不能算 100% 无异常

## 当前视觉 provider 的真实状态

这一节只记录当前看到的真实表现，不做理想化描述。

### 主路

当前首选 provider 是：

- `openai`
- model: `gpt-5.4`

这轮绝大多数视觉结果确实走的是这条主路。

### fallback

这轮 fallback 实际成功接住了 `22` 条样本，说明它现在是有实际价值的，不是死配置。

当前这轮成功的 fallback 组合是：

- `reelx / qwen-vl-max`

### 这轮异常的主要来源

这轮视觉异常主要有两类：

1. 主路 `openai` 偶发 `500 / EOF`
   这批样本会被路由到 fallback。
2. fallback `reelx` 的 `413 Request Entity Too Large`
   这轮有 `3` 条样本卡在这里：
   - `apollo_und_cosmo`
   - `comyahmed`
   - `rehaexperte`

所以当前更准确的结论不是“fallback 已经完全稳了”，而是：

- fallback 已经能实际救回一批主路失败样本
- 但在大请求体场景下，`reelx` 仍然可能报 `413`

### 现在应该怎么理解 visual error

当前 visual error 不应该被理解成“整轮失败”。

更准确地说：

- 少量 visual error 会在 final review 里保留为异常行
- 这类记录当前可能进入 `转人工`
- 不会再像早期那样直接让整轮 silently 缩水

## 2026-04-01 `SKG` live writeback 补充结论

这次真正把 `SKG` 写入飞书时，又额外暴露出一个和 downstream 不同的坑：

- **飞书 live 写入吞吐很慢**

### 先发生了什么

第一次我是直接拿整份：

- `temp/skg_downstream_retryfix_20260401_1/exports/all_platforms_final_review_payload.json`

做 full live writeback。

现象是：

- 本地长时间没有等到最终 result json
- 但后面回查目标飞书表，发现其实已经写进去一部分

这说明：

- 它不是 schema / 权限直接报错
- 而是大批量 live 写入时，本地看起来像“挂住”，但飞书端其实在慢慢落表

### 这次怎么安全补完

这次最终没有再整批硬冲，而是：

1. 先对目标飞书表做 existing-analysis
2. 用 `达人对接人 + 达人ID + 平台` 主键去和原 payload 做差集
3. 只保留飞书里还不存在的 rows
4. 把缺口 payload 分成 `50 / 50 / 50 / 50 / 50 / 22` 六块 live 写回

缺口 payload 和 chunk 结果在：

- `temp/skg_live_resume_20260401/remaining_payload.json`
- `temp/skg_live_resume_20260401/chunks/resume_summary.json`

最终这次续传结果是：

- `created_total = 272`
- `failed_total = 0`
- 回查后 `existing_keys_in_table = 455`
- `missing_payload_keys_after_resume = 0`

所以这次最终结论是：

- `SKG` 这轮已经完整写入飞书
- 当前真正的 live 卡点是**吞吐慢**
- 如果后面再遇到“大 payload 写回”，更稳的 SOP 是：
  - 先 existing-analysis
  - 再只传缺口
  - 必要时分块续传

## 当前仍未完全解决的问题

这次不能说“链路已经完全没有问题”，更准确地说是：

- 主链路这次已经跑通
- 但还有几类已知遗留风险

### 1. visual fallback 还不是 100% 稳

这次 visual 里仍然有 `3` 条因为 fallback provider `reelx` 的 `413 Request Entity Too Large` 失败。

也就是说：

- 主路 `openai` + fallback 组合已经明显比之前稳
- 但大请求体场景下，fallback 还会丢异常行

### 2. 负责人解析仍然会挡掉一小批记录

这次 `460 keep` 里有 `5` 条因为邮件内容里没命中任何负责人英文名或邮箱别名，被 shared-mailbox 顶层拦掉。

也就是说：

- 这条线现在不是“所有命中邮件都一定能写回”
- 员工英文名 / 别名覆盖不够时，仍会有记录被挡住

### 3. live writeback 的 chunked resume 现在是已验证 SOP，但还不是主线自动能力

这次最终是靠：

- existing-analysis
- 差集 payload
- 分块续传

把 live writeback 补完的。

也就是说：

- 这条路现在已经证明可行
- 但 shared-mailbox 主线还没有把“大 payload 自动切块续传”固化成默认 runtime 行为

### 4. 客户主表里仍有一批字段只是“会解析”，不是“真生效”

当前像：

- `参考账号`
- `反例账号`
- `判定关系`

这些字段不能被当成已经稳定驱动 runtime 的硬规则。

所以：

- 客户填写口径已经更清楚了
- 但字段支持面还不是“表里写了什么，链路就全自动吃什么”

## 后续待修清单

如果后面继续收这条链路，当前最值得优先做的是：

1. 把 visual fallback 的 `413 Request Entity Too Large` 收掉，至少要让大请求体场景不再直接变成异常行。
2. 把负责人英文名 / 邮箱别名覆盖补全，减少 shared-mailbox 顶层因为负责人解析失败而挡掉的记录。
3. 把 live writeback 的 existing-analysis + 差集续传 + 分块上传固化成默认自动恢复能力，而不是继续靠人工 SOP。
4. 继续收窄“客户主表已解析但未生效”的字段差距，至少明确哪些字段下一版会真正进入 runtime。

## 最小排障顺序

如果一轮 run 看起来“不太对”，按这个顺序排查最快：

1. 看顶层 `summary.json`
   先确认任务有没有真的收尾
2. 看 task-level `summary.json`
   先分清是 `mail-only update` 少，还是 `full-screening` 少
3. 看 upstream `summary.json`
   确认邮件命中和 keep-list 有没有明显缩掉
4. 看 downstream `summary.json`
   确认是卡在 scrape、visual 还是 positioning
5. 看 `tiktok_profile_reviews.json`
   直接判断 `Pass / Reject / Missing` 数量
6. 看 `failed_or_skipped_records.json`
   查是不是负责人解析或上传前校验挡住了写回

## `SKG` 这条线当前的具体业务口径

截至 `2026-04-01`，`SKG` 相关业务口径已经是：

- `SKG-1 / SKG-2` 不再视为两条独立逻辑任务
- 只要 task assets 相同，就收口成一个逻辑任务 `SKG`
- 负责人不是固定 task owner，而是逐行按邮件内容识别 `Lilith / Rhea`
- 邮件匹配默认包含 `from`，所以像 `chanel@arsagendi.com` 这类回邮也会进入匹配

## 现在最推荐的执行 SOP

```bash
python3 -m feishu_screening_bridge inspect-task-upload \
  --env-file .env \
  --download-templates \
  --json
```

```bash
python3 scripts/run_shared_mailbox_sync.py \
  --env-file .env \
  --sent-since 2026-03-26
```

```bash
python3 scripts/run_shared_mailbox_post_sync_pipeline.py \
  --shared-mail-db-path data/shared_mailbox/email_sync.db \
  --env-file .env \
  --task-name "SKG" \
  --upload-dry-run
```

确认没问题后，再去掉：

```bash
--upload-dry-run
```

## 和 `/operator` 的关系

`/operator` 现在是 local-only 控制台，不是共享邮箱生产调度的唯一正式入口。

当前更稳的口径仍然是：

- 正式运行优先走 CLI
- `/operator` 主要用于 bounded run 和观察 summary

## 需要记住的状态语义

- `scrape_running`
  还在 Apify 抓取阶段
- `visual_running`
  抓取已经完成，正在跑视觉
- `missing_profiles_blocked`
  还有 `Missing` 没补齐，最终导出应被挡住
- `completed`
  该任务链路完整收尾

如果以后再看到“上游 keep 很大，但 final export 只有一小部分”，第一优先检查的不是 final review，而是：

1. `requested_identifier_count`
2. `profile_review_count`
3. `missing_profile_count`
4. `scrape_job.status`

只看 final export 行数，很容易误以为链路跑完了。
