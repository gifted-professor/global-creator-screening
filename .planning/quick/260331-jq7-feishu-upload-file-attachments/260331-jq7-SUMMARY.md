# Quick Task 260331-jq7 Summary

## Task

提前把最终上传飞书这条链路补成“可带文件”的形态：

- 行级保留最后一封邮件原文 `.eml` 的本地文件候选
- 总表保留共享 workbook 文件候选
- 飞书上传器支持把本地文件上传到目标表的附件字段

这次先把链路和 payload 约定接好；具体下一次再按真实客户表落位继续用。

## What Changed

### 1. `all_platforms_final_review_payload.json` 现在会携带文件级元数据

`backend/final_export_merge.py` 现在会在不污染客户可见 Excel 列的前提下，把这些内部字段写进 payload：

- `__last_mail_raw_path`
  保留 keep workbook 里对应行的最后一封邮件原文路径
- `__feishu_attachment_local_paths`
  行级可上传附件候选，当前优先放最后一封邮件原文 `.eml`
- `__feishu_shared_attachment_local_paths`
  顶层共享文件候选，当前固定保留本次总表 `all_platforms_final_review.xlsx`

这样下次真正上传时，不需要再反推“附件从哪里来”。

### 2. 飞书上传器现在能识别附件字段并上传本地文件

`feishu_screening_bridge/bitable_upload.py` 现在会：

- 自动识别目标表里的附件字段（当前优先匹配单个 type `17` 字段或常见附件列名）
- 忽略 payload 里的内部元数据键，不把它们当普通文本字段乱写
- 如果某行带了 `__feishu_attachment_local_paths`，就把这些本地文件上传到飞书，再把得到的 `file_token` 写入附件列

当前实现优先处理行级附件，也就是最后一封邮件原文；总表 workbook 先作为共享候选保留在 payload 顶层，留待下一次按真实落位决定怎么传。

### 3. Feishu client 补了 repo-local 本地文件上传 helper

`feishu_screening_bridge/feishu_api.py` 新增了本地文件上传 helper，当前封装的是：

- multipart `upload_all`
- 本地文件路径 -> 飞书 `file_token`

上传器后面如果要继续接：

- 总表 workbook
- 其他证据附件
- 本地归档文件

都可以直接复用这条 helper，不需要再临时拼 multipart。

## Boundaries

- 这次默认只把“最后一封邮件原文 `.eml`”接成行级附件上传候选
- 总表 workbook 已经进 payload 顶层共享候选，但还没有默认复制到每一条 bitable 记录里，避免把同一份大文件重复塞进所有客户记录
- 没有做真实客户表 live upload；本次验证以单元测试和 payload contract 为主

## Outcome

现在这条链已经从“只有最后一封邮件内容文本”升级成：

- 文本内容仍然保留
- 原始 `.eml` 也能作为文件候选跟着 payload 走
- 总表 workbook 也有了共享文件候选入口

后面下一次真的要继续传飞书时，只需要决定“总表 workbook 最终挂在哪个飞书落点”，而不用再改一轮底层 payload / uploader。
