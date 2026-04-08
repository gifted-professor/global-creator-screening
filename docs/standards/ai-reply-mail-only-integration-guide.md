# AI回信管理 Mail-Only 接入指南

## 目的

这份文档给需要接入或代跑这条链路的同事使用。

目标是让操作者只看这一份文档，就能理解：

- 这条链路解决什么问题
- 应该跑哪一个入口
- 会往飞书写哪些字段
- 哪些样本会自动入库，哪些会转人工
- 历史数据要怎么补写
- 当前已经验证过的稳定做法和已知边界

这份文档描述的是当前仓库里的默认新路径，不是旧的 sending-list 主路径。

## 一句话说明

给一个 `task-name`，脚本会：

1. 从 Feishu task upload 读取任务信息和目标 `linkedBitableUrl`
2. 实时同步共享邮箱 `partnerships@amagency.biz / 其他文件夹/达人回信`
3. 在本地邮件库里按任务名做 parsed-field 全量召回
4. 先跑规则段，再跑 LLM 尾部
5. 生成 mail-only payload
6. 把结果写回对应任务的 `AI回信管理`
7. 把低置信尾部自动写成 `转人工`

## 默认入口

默认入口脚本：

- `scripts/run_task_to_ai_reply_mail_only_pipeline.py`

最常用命令：

```bash
python3 scripts/run_task_to_ai_reply_mail_only_pipeline.py \
  --task-name <TASK_NAME> \
  --env-file .env
```

只看结果，不真正写回飞书：

```bash
python3 scripts/run_task_to_ai_reply_mail_only_pipeline.py \
  --task-name <TASK_NAME> \
  --env-file .env \
  --upload-dry-run
```

指定日期回放：

```bash
python3 scripts/run_task_to_ai_reply_mail_only_pipeline.py \
  --task-name <TASK_NAME> \
  --env-file .env \
  --local-date 2026-04-07
```

## 当前默认逻辑

### 召回

- 不再默认依赖发信名单
- 默认对指定日期窗口里的 parsed fields 全量召回
- 召回字段：
  - `subject`
  - `snippet`
  - `body_text`
  - `body_html`

这里的“全量”意思是：

- 不再走旧的 `100` 封 sample 口径
- 当天命中多少封，就处理多少封

### 规则段

当前保留的规则段是：

- `regex pass1`
- `regex pass2`

默认不启用：

- sending-list email exact match

如果确实需要回旧逻辑，才显式加：

```bash
--use-sending-list-match
```

### LLM

- 规则段没有直接打穿的尾部，进入 LLM
- `high` 置信直接入主结果
- `medium / low / blank` 进入 `manual tail`

### 人工池

`manual tail` 默认继续写同一张 `AI回信管理`，规则是：

- `达人ID = <任务名><月>/<日>转人工<n>`
- `平台 = 转人工`

例如：

- `SKG4/7转人工1`
- `Duet4/8转人工3`
- `MINISO4/8转人工6`

## 飞书写入字段

这条链路当前默认写入这些字段：

- `达人ID`
- `平台`
- `主页链接`
- `当前网红报价`
- `达人最后一次回复邮件时间`
- `full body`
- `eml`

其中：

- `full body` 是当前标准邮件正文字段
- `eml` 是原始无损邮件附件

## 字段生成规则

### 平台

平台只按 `full body` 判定，优先级固定：

1. 出现 `tiktok` -> `TikTok`
2. 否则出现 `instagram` -> `Instagram`
3. 否则出现 `youtube` -> `YouTube`
4. 都没有 -> 留空

补充：

- `转人工` 行的平台固定就是 `转人工`

### 主页链接

主页链接按 `达人ID + 平台` 直接生成：

1. `TikTok -> https://www.tiktok.com/@<达人ID>`
2. `Instagram -> https://www.instagram.com/<达人ID>`
3. `YouTube -> https://www.youtube.com/@<达人ID>`
4. 平台为空或 `转人工` -> 留空

补充：

- 如果 `达人ID` 自带 `@`，会先去掉前导 `@`
- 飞书里的 `主页链接` 是 URL 字段，上传时会自动转成 URL 对象

### 报价

- 有明确报价就写入 `当前网红报价`
- 没抽到就允许为空
- 空报价不阻塞上传

### 回复时间

- 写入的是 `达人最后一次回复邮件时间`
- 当前按 `Asia/Shanghai` 口径处理日期窗口

### full body

- 优先使用已经解析好的正文
- 必要时可回落到 raw `.eml`

### eml

- 每条记录会带本地 raw `.eml` 路径
- 如果目标飞书表存在附件字段 `eml`，就一起上传
- 如果目标表没有附件列，上传不会报错，只会跳过附件

## 主键和更新语义

mail-only 这条链路默认使用：

- `达人ID + 平台`

作为同一条飞书记录的匹配键。

上传语义是：

- 如果飞书里不存在同 `达人ID + 平台`：创建
- 如果飞书里已存在同 `达人ID + 平台`：只更新邮件相关字段

这意味着：

- 正常高置信行会稳定 update
- `转人工` 行也会稳定 update
- 平台为空的行没有稳定主键，回填历史时不建议直接继续 create

## 标准产物

每次 run 都会在下面生成一套产物：

- `summary.json`
- `shared_mailbox_sync_summary.json`
- `*_parsed_field_funnel.xlsx`
- `*_parsed_field_funnel_keep.xlsx`
- `*_parsed_field_funnel_manual_tail.xlsx`
- `*_parsed_field_funnel_llm_review.jsonl`
- `*_mail_only_upload_payload.json`
- `feishu_upload_local_archive/mail_only_upload_result.json`
- `feishu_upload_local_archive/manual_pool_upload_result.json`

标准输出根目录：

- `temp/task_to_ai_reply_mail_only/<timestamp>_<task_name>`

## 历史回填怎么做

如果只是补：

- `主页链接`
- `full body`
- `eml`

不要重跑整条链路，不要重打 LLM。

正确做法是：

1. 复用已有 `keep workbook`
2. 重新生成 mail-only payload
3. 直接回写飞书

对应脚本：

- `scripts/build_mail_only_payload_from_funnel_keep.py`
- `scripts/build_mail_only_payload_from_manual_tail.py`

经验规则：

- 历史回填优先 `update`
- 平台为空的旧记录不要盲目新建
- 大批量回填建议先去重，再分 chunk 上传

## 已验证通过的真实任务

这条路径已经在真实任务上跑通过：

- `SKG`
- `Duet`
- `MINISO`

而且已经验证过：

- 最新邮件正常实时入库
- `主页链接` 自动生成
- `full body` 正常写入
- `.eml` 附件正常上传
- `manual tail` 自动入 `转人工`
- 历史记录可以不重打 LLM 直接补写

## 推荐给同事的操作口径

### 日常跑最新

直接跑：

```bash
python3 scripts/run_task_to_ai_reply_mail_only_pipeline.py \
  --task-name <TASK_NAME> \
  --env-file .env
```

### 先验收再写飞书

先跑 dry-run：

```bash
python3 scripts/run_task_to_ai_reply_mail_only_pipeline.py \
  --task-name <TASK_NAME> \
  --env-file .env \
  --upload-dry-run
```

看两个文件：

- `summary.json`
- `feishu_upload_local_archive/*.json`

### 历史补写字段

不要重跑主链路，直接做回填。

适合历史补写的字段：

- `主页链接`
- `full body`
- `eml`

## 已知边界

### 1. 飞书附件上传比普通字段慢

这是当前最常见的“看起来像卡住”的原因。

现象：

- 终端长时间没有新输出
- 但实际是在正常传附件

### 2. 飞书附件会碰频率限制

报错形式通常像：

- `99991400`
- `request trigger frequency limit`

处理建议：

- 提高写入间隔
- 分 chunk 上传
- 不要一口气把超大批量附件全打上去

### 3. 空平台行没有稳定主键

如果一条记录：

- `达人ID` 有值
- 但 `平台` 为空

那历史回填时要非常小心，因为它容易变成重复新建。

### 4. manual tail 不是 bug

`manual tail` 的意思不是失败，而是：

- 当前证据不足以高置信确认 `达人ID`
- 这类样本应该进 `转人工`

## 当前我建议的优化

如果要继续把这条链路交给更多同事跑，我建议做这三件事：

1. 给 mail-only uploader 增加显式 CLI 参数：
   - `--write-min-interval-seconds`
   - `--request-max-retries`
   - `--retry-backoff-base-seconds`
   这样大批量 `.eml` 回填时不用再走临时脚本。

2. 加一个官方回填入口：
   - 输入 `keep workbook` 或 `manual tail workbook`
   - 直接补 `主页链接 / full body / eml`
   - 默认跳过空平台风险行

3. 加一个跨 run 的 LLM 结果缓存：
   - 用 `thread_key + task_name + local_date` 复用既有 review
   - 避免历史回放时重新打 LLM

## 关联文档

真实 `MINISO 2026-04-07` 首次打通记录见：

- `docs/standards/miniso-ai-reply-mail-only-path-2026-04-07.md`

总 runbook 见：

- `docs/standards/creator-screening-e2e-runbook.md`
