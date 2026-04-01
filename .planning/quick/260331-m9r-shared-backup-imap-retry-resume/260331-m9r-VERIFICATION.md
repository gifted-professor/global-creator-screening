status: passed

# Quick Task 260331-m9r Verification

## Verified

- `python3 -m unittest tests.test_sync_workers tests.test_feishu_screening_bridge tests.test_task_upload_to_keep_list_pipeline -v` 通过，`31 tests, OK (skipped=3)`
- `python3 -m py_compile email_sync/imap_sync.py tests/test_sync_workers.py` 通过

### Targeted regression

新增 `tests.test_sync_workers.SyncWorkersTests.test_shared_backup_retry_reconnects_and_resumes_from_last_checkpoint`

已验证：

- 共享备份目录在 `UID=2` 第一次抛 `socket error: EOF`
- runner 会自动重连
- 第二次搜索从 `last_seen_uid=1` 继续
- 最终 `1,2,3` 三封都成功落库

### Live probe

执行：

```bash
python3 -m feishu_screening_bridge sync-task-upload-mail --env-file .env --task-name MINISO --limit 1 --json
```

真实结果：

- `defaultCredentialMode = default_account_preferred_with_employee_fallback`
- `mailCredentialSource = default_account`
- `mailLoginEmail = partnerships@amagency.biz`
- `sentSince = 2026-03-31`
- `resolvedFolder = 其他文件夹/邮件备份`
- `mailSyncStrategy = shared_backup_folder`
- `mailFetchedCount = 1`
- `mailSyncDurationSeconds ≈ 2.794`

## Notes

- 上一轮同样的 live probe 曾出现 `UID => socket error: EOF` 且 `mailFetchedCount = 0`
- 本次 live probe 已经成功抓到 `1` 封，说明 retry + reconnect + resume 修复对当前共享备份目录主线生效
