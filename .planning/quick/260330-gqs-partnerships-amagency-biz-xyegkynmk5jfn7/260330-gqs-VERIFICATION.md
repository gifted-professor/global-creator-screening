status: passed
checked_at: 2026-03-30
mode: quick-full

# Verification

## Goal

确认任务驱动抓信入口默认改为统一邮箱 `partnerships@amagency.biz / xYeGKyNmK5jFN7Y2`，并且 keep-list summary 能看见这一来源。

## Checks

- `sync_task_upload_mailboxes()` 是否优先使用默认账号：通过
- 没有员工匹配时，默认账号是否仍可执行抓信：通过
- keep-list runner 是否把默认账号和来源写入 `resolved_inputs.mail_sync`：通过
- 真实入口是否已显示 `default_account` 而不是员工表账号：通过

## Evidence

- 新增单测 `test_sync_task_upload_mailboxes_can_use_default_credentials_without_employee_match`
- 新增单测 `test_runner_passes_default_mail_credentials_to_mail_sync`
- `python3 -m unittest tests.test_feishu_screening_bridge tests.test_task_upload_to_keep_list_pipeline -v` => `26 tests`, `OK (skipped=3)`
- 真实命令 `python3 -m feishu_screening_bridge sync-task-upload-mail --env-file .env --task-name MINISO --limit 1 --json` 返回：
  - `defaultCredentialMode = "default_account"`
  - `defaultAccountEmail = "partnerships@amagency.biz"`
  - `items[0].mailCredentialSource = "default_account"`
  - `items[0].mailLoginEmail = "partnerships@amagency.biz"`

## Gaps

- 真实命令最终没有抓到邮件，因为统一邮箱中不存在 `MINISO` 对应文件夹；这说明默认账号切换生效，但邮箱侧文件夹还没准备好。
