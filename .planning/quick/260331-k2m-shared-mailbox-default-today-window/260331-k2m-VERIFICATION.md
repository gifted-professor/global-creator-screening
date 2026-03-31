status: passed

# Quick Task 260331-k2m Verification

## Verified

- `python3 -m unittest tests.test_main_cli tests.test_feishu_screening_bridge tests.test_task_upload_to_keep_list_pipeline -v` 通过，`53 tests, OK (skipped=3)`
- `python3 -m py_compile feishu_screening_bridge/task_upload_sync.py scripts/run_task_upload_to_keep_list_pipeline.py email_sync/date_windows.py feishu_screening_bridge/__main__.py` 通过
- live probe：
  - `python3 -m feishu_screening_bridge sync-task-upload-mail --env-file .env --task-name MINISO --limit 1 --json`
  - 返回里已确认：
    - `defaultCredentialMode = default_account_preferred_with_employee_fallback`
    - `mailCredentialSource = default_account`
    - `mailLoginEmail = partnerships@amagency.biz`
    - `sentSince = 2026-03-31`
    - `resolvedFolder = 其他文件夹/邮件备份`
    - `mailSyncStrategy = shared_backup_folder`

## Notes

- live probe 里 `mailFetchedCount = 0`，并伴随 `其他文件夹/邮件备份: command: UID => socket error: EOF`
- 这说明默认路由切换成功，但共享邮箱备份目录的企业邮 IMAP 稳定性问题仍然存在
