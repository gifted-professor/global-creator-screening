# Quick Task 260330-gqs Summary

## Outcome

任务驱动抓信默认账号已经切到统一邮箱 `partnerships@amagency.biz`。现在：

1. `sync-task-upload-mail` 会先读 `TASK_UPLOAD_MAIL_ACCOUNT` / `TASK_UPLOAD_MAIL_AUTH_CODE`。
2. 如果没有 task-upload 专用键，会自动回退到 `.env` 里的 `EMAIL_ACCOUNT` / `EMAIL_AUTH_CODE`。
3. 只有在这两套默认凭据都没有时，才继续走员工表里的 `employeeEmail` / `imapCode`。

## Fix

- `feishu_screening_bridge/task_upload_sync.py`
  - `sync_task_upload_mailboxes()` 新增统一默认账号优先级。
  - 当默认账号存在时，即使任务没有匹配到员工表，也允许继续抓信。
  - 返回结果新增 `mailCredentialSource` 和 `mailLoginEmail`，便于确认实际登录账号。
- `feishu_screening_bridge/__main__.py`
  - `sync-task-upload-mail` 入口现在会自动把默认抓取账号传给 mail sync。
- `scripts/run_task_upload_to_keep_list_pipeline.py`
  - keep-list summary 新增 `default_account_email`、凭据来源和 `credential_mode`。
  - `mail_sync` step 现在会把 `credential_source` / `login_email` 留进 summary。
- `.env.example`
  - 增加 `TASK_UPLOAD_MAIL_ACCOUNT` / `TASK_UPLOAD_MAIL_AUTH_CODE` 注释说明。

## Verification

- 单测：
  - `python3 -m unittest tests.test_feishu_screening_bridge tests.test_task_upload_to_keep_list_pipeline -v`
  - 结果：`26 tests`, `OK (skipped=3)`
- 真实受限验证：
  - `python3 -m feishu_screening_bridge sync-task-upload-mail --env-file .env --task-name MINISO --limit 1 --json`
  - 结果里已经明确显示：
    - `defaultCredentialMode = "default_account"`
    - `defaultAccountEmail = "partnerships@amagency.biz"`
    - `mailCredentialSource = "default_account"`
    - `mailLoginEmail = "partnerships@amagency.biz"`

## Remaining Risk

- 这次真实验证失败点已经不再是账号来源，而是统一邮箱里没有 `MINISO` 对应文件夹，只看到 `Deleted Messages / Drafts / INBOX / Junk / Sent Messages`。
- 如果后面要让统一邮箱真正承担所有任务抓取，还需要把任务文件夹同步到这个邮箱，或者补 `folder_overrides`。
