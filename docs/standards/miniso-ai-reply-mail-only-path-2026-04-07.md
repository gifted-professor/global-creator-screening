# MINISO AI回信管理直写路径

日期：`2026-04-07`

这份文档记录一条已经在本仓库实跑通过的链路：

- 共享邮箱来源：`partnerships@amagency.biz / 其他文件夹/达人回信`
- 任务：`MINISO`
- 目标表：`AI回信管理`
- 本轮写入字段：
  - `达人ID`
  - `平台`
  - `主页链接`
  - `当前网红报价`
  - `达人最后一次回复邮件时间`
  - `full body`

这份文档现在记录两层路径：

- 第一层：先不跑剩余 `300` 封 LLM，只把规则段已经稳定解析出的 `147` 封先写进飞书
- 第二层：把剩余 `300` 封也走完 LLM，最终按 mail-only 口径写入 `440` 封

两层都遵守同一个写入语义：

- 已存在的同 `达人ID + 平台` 记录，只更新邮件字段

`2026-04-07` 晚间更新：

- mail-only / `AI回信管理` 这条链路后续默认不再依赖发信名单
- `task upload` 只保留任务信息和 `linkedBitableUrl`
- 规则段默认只保留 `regex pass1 / regex pass2`
- 其余尾部直接进 LLM
- `manual tail` 默认继续写入同一张 `AI回信管理`
  - `达人ID = <任务名><月>/<日>转人工<n>`
  - `平台 = 转人工`
- 如果确实要回到旧逻辑，才显式开启 sending-list match

## 0. 当前默认执行口径

在继续设计 LLM 前，`MINISO` 先默认按这条路径执行：

1. 用当天 `450` 封解析字段命中邮件做召回池。
2. 只跑规则段，先拿到可直接落表的 `147` 封。
3. 先把这 `147` 封按 mail-only 字段写进 `AI回信管理`。
4. 如果只想先落稳定结果，先写 `147` 封规则段结果。
5. 如果要跑完整 mail-only 路径，再把剩余 `300` 封送进 LLM，最后写 `440` 封。

## 前置条件

这条路径默认依赖下面这些前置产物已经准备好：

- 共享邮箱已同步到本地：
  - `data/shared_mailbox/email_sync.db`
- 本轮共享邮箱来源：
  - `partnerships@amagency.biz / 其他文件夹/达人回信`
- 目标飞书表来自 `task-upload` 的 `linkedBitableUrl`，并已确认实际写入表名是：
  - `AI回信管理`

如果要复刻白天那条老口径，才额外需要：

- `MINISO` 发信名单附件

当前这条路径只写邮件相关字段，不写：

- `ai是否通过`

## 1. 召回口径

先按上海时区 `2026-04-07` 统计 `MINISO` 命中邮件：

- 解析字段命中：`450`
  - 命中字段：`subject / body_text / body_html / snippet`
- raw `.eml` 原文直接命中：`370`

这条直写路径使用的是更宽的召回池：`450` 封解析字段命中邮件。

## 2. 规则段筛选结果

在这 `450` 封邮件上，只跑 `mail_thread_funnel` 规则段，不进 LLM：

- `pass0 = 74`
- `pass1 = 7`
- `pass2 = 66`
- `filtered_auto_reply = 3`
- `llm_candidate = 300`

所以当前规则段可直接保留的邮件一共是：

- `147 = 74 + 7 + 66`

## 3. 先导出 147 封邮件字段表

先生成一份本地 workbook，方便检查字段完整性：

- 输出文件：
  - `temp/miniso_today_147_with_mail_fields.xlsx`

该文件包含至少这些列：

- `达人ID`
- `平台`
- `主页链接`
- `当前网红报价`
- `full body`
- `达人最后一次回复邮件时间`
- `latest_external_from`
- `subject`
- `resolution_stage_final`
- `thread_key`
- `raw_path`

本轮实测字段完整性：

- `达人ID`：`147/147`
- `平台`：允许少量空值
- `当前网红报价`：`109/147`
- `full body`：`147/147`
- `达人最后一次回复邮件时间`：`147/147`

说明：

- `当前网红报价` 为空不阻塞上传
- 这类记录仍然可以先把 `达人ID / 平台 / 回复时间 / full body` 写入飞书

## 4. 生成最小上传 payload

再把这 `147` 行转成 uploader 可直接消费的 payload：

- 输出文件：
  - `temp/miniso_today_147_upload_payload.json`

payload 只保留本轮需要的业务字段：

- `达人ID`
- `平台`
- `主页链接`
- `当前网红报价`
- `达人最后一次回复邮件时间`
- `full body`

并统一带：

- `__feishu_update_mode = create_or_mail_only_update`

语义是：

- 飞书里不存在同 `达人ID + 平台` 记录：创建
- 飞书里已存在同 `达人ID + 平台` 记录：只更新邮件字段

## 5. 目标飞书表

本轮确认的 `MINISO` 任务记录：

- `recordId = recveXGV2i3BS0`
- `linkedBitableUrl = https://bcnorxdfy50v.feishu.cn/base/G0ifbflgtafPY1sn3WHcMFvMnAb?table=tblkEszvaJujmjEa&view=vewWIN9jul`

同一个 base 下实查到的表有：

- `AI回信管理`
- `AI回信管理 4.7`
- `AI 回信管理4.3`
- `提报表`

本轮确认实际写入目标就是：

- 表名：`AI回信管理`
- `table_id = tblkEszvaJujmjEa`
- `view_id = vewWIN9jul`

## 6. 实际上传命令

先 dry-run：

```bash
python3 -m feishu_screening_bridge upload-final-review-payload \
  --env-file .env \
  --payload-json temp/miniso_today_147_upload_payload.json \
  --linked-bitable-url "https://bcnorxdfy50v.feishu.cn/base/G0ifbflgtafPY1sn3WHcMFvMnAb?table=tblkEszvaJujmjEa&view=vewWIN9jul" \
  --dry-run \
  --json
```

再正式写入：

```bash
python3 -m feishu_screening_bridge upload-final-review-payload \
  --env-file .env \
  --payload-json temp/miniso_today_147_upload_payload.json \
  --linked-bitable-url "https://bcnorxdfy50v.feishu.cn/base/G0ifbflgtafPY1sn3WHcMFvMnAb?table=tblkEszvaJujmjEa&view=vewWIN9jul"
```

## 7. 本轮正式结果

正式写入完成后，结果是：

- `selected_row_count = 147`
- `duplicate_payload_group_count = 7`
- `deduplicated_row_count = 7`
- `processed_row_count = 140`
- `created_count = 127`
- `updated_count = 13`
- `skipped_existing_count = 0`
- `failed_count = 0`

说明：

- payload 内部有 `7` 组同 `达人ID + 平台` 的重复记录
- uploader 会自动保留最后一条
- 所以最终真实处理的是 `140` 条

## 8. 结果归档

本轮关键产物：

- 字段检查 workbook：
  - `temp/miniso_today_147_with_mail_fields.xlsx`
- 实际上传 payload：
  - `temp/miniso_today_147_upload_payload.json`
- 飞书上传结果 JSON：
  - `temp/feishu_upload_local_archive/feishu_bitable_upload_result.json`
- 飞书上传结果 XLSX：
  - `temp/feishu_upload_local_archive/feishu_bitable_upload_result.xlsx`

## 9. LLM 全量尾部结果

按同一批 `450` 封解析字段命中邮件继续往下跑：

- `147` 封规则段直接保留
- `300` 封进入 LLM
- `3` 封自动回复继续过滤

LLM 结果：

- `llm_high = 293`
- `llm_medium = 3`
- `llm_low_or_blank = 4`
- `manual_tail = 7`
- 最终 keep：`440`

对应产物：

- 全量 parsed-field funnel review：
  - `temp/miniso_today_parsed_field_funnel.xlsx`
- 全量 parsed-field funnel keep：
  - `temp/miniso_today_parsed_field_funnel_keep.xlsx`
- LLM manual tail：
  - `temp/miniso_today_parsed_field_funnel_manual_tail.xlsx`
- LLM review jsonl：
  - `temp/miniso_today_parsed_field_funnel_llm_review.jsonl`

## 10. full body 平台判定规则

当前 mail-only 上传表不再混用旧的 sending-list 平台补全。

平台只按 `full body` 判定，优先级固定为：

1. 只要 `full body` 里出现 `tiktok`，平台就是 `TikTok`
2. 否则如果出现 `instagram`，平台就是 `Instagram`
3. 否则如果出现 `youtube`，平台就是 `YouTube`
4. 三者都没有，平台留空

本轮 `440` 封的实际平台分布：

- `TikTok = 436`
- `Instagram = 1`
- `YouTube = 0`
- 空平台 = `3`

## 10.1 主页链接生成规则

主页链接不再依赖发信名单，直接按 `达人ID + 平台` 生成：

1. `TikTok -> https://www.tiktok.com/@<达人ID>`
2. `Instagram -> https://www.instagram.com/<达人ID>`
3. `YouTube -> https://www.youtube.com/@<达人ID>`
4. 平台为空或 `转人工`，主页链接留空

补充约束：

- 如果 `达人ID` 自带前缀 `@`，生成链接前会先去掉前导 `@`
- 飞书里的 `主页链接` 字段是 URL 字段，上传时会由 uploader 自动转成 URL 对象

## 11. 440 封 mail-only 写回结果

把 `440` 封 keep 结果重新整理成邮件字段表和 payload 后，再写回 `AI回信管理`：

- 输入 keep workbook：
  - `temp/miniso_today_parsed_field_funnel_keep.xlsx`
- 生成邮件字段 workbook：
  - `temp/miniso_today_440_with_mail_fields.xlsx`
- 生成 upload payload：
  - `temp/miniso_today_440_upload_payload.json`

最终 live 结果：

- `selected_row_count = 440`
- `processed_row_count = 422`
- `created_count = 347`
- `updated_count = 75`
- `failed_count = 0`
- `duplicate_payload_group_count = 17`

说明：

- payload 内部有 `17` 组同 `达人ID + 平台` 的重复项
- uploader 实际去重后处理 `422` 条
- 由于空平台有 `3` 条，按 `达人ID + 平台` 口径统计的飞书索引净增会比 `created_count` 少 `3`

## 12. 单入口脚本

现在仓库里已经有一条单入口脚本，可以把这条链路收成：

- 给 `task-name`
- 实时同步共享邮箱
- 对当天命中的解析字段邮件做全量匹配
- 规则段 + LLM
- 按 mail-only 口径直接写回 `AI回信管理`

脚本：

- `scripts/run_task_to_ai_reply_mail_only_pipeline.py`

典型命令：

```bash
python3 scripts/run_task_to_ai_reply_mail_only_pipeline.py \
  --task-name MINISO \
  --env-file .env
```

如果只想先看结果、不真正写回飞书：

```bash
python3 scripts/run_task_to_ai_reply_mail_only_pipeline.py \
  --task-name MINISO \
  --env-file .env \
  --upload-dry-run
```

当前默认语义要注意：

- 它会先做共享邮箱增量同步，再在本地库里对当天窗口做全量 parsed-field 匹配
- 这里的“全量”指的是：不再像旧口径那样只取 `100` 封 sample，而是把该日期窗口里命中的邮件全吃进去
- 如果后续要把窗口从“当天”改成“任务开始日到今天”，再在这个入口上继续扩就可以

## 13. 下一步

1. 用 `450` 封解析字段命中邮件作为当天召回池。
2. 先跑规则段，拿到 `147` 封可直接写入的结果。
3. 再把剩余 `300` 封送进 LLM。
4. 生成 `440` 封 mail-only workbook / payload。
5. 最后写入 `AI回信管理`。
