# Quick Task 260331-k2m Summary

## Task

把默认抓信主线切成：

- 默认邮箱优先走共享邮箱 `partnerships@amagency.biz`
- 默认抓信窗口从“最近三个月”改成“只从今天开始”
- 共享邮箱没有项目专属目录时，优先抓 `其他文件夹/邮件备份`

## What Changed

### 1. task-upload mail sync 现在默认共享邮箱优先

`feishu_screening_bridge/task_upload_sync.py` 当前凭据优先级已经改成：

1. `default_account_email/default_auth_code`
2. 员工表 `employeeEmail/imapCode`

对应 summary / result 口径也一起调整成：

```text
default_account_preferred_with_employee_fallback
```

这样任务驱动抓信以后默认会先走共享邮箱，而不是员工邮箱。

### 2. 共享邮箱下默认优先抓 `其他文件夹/邮件备份`

之前共享邮箱如果找不到任务专属文件夹，会退回：

```text
all_selectable_fallback
```

现在补成了更贴近共享邮箱主线的策略：

- 如果当前凭据来源是 `default_account`
- 且找不到任务专属文件夹
- 且共享邮箱里存在 `其他文件夹/邮件备份`

则优先用：

```text
shared_backup_folder
```

这样不会默认把所有可选目录都扫一遍。

### 3. 默认抓信窗口改成今天

`email_sync/date_windows.py` 的默认 sent-since 已切成 `today`。

所以以后不显式传 `--sent-since` 时：

- `python3 -m email_sync sync`
- `python3 -m feishu_screening_bridge sync-task-upload-mail`

都会默认只抓今天的邮件。

### 4. 本机 `.env` 已切到共享邮箱

这台机器的本地 `.env` 已经改成：

- `EMAIL_ACCOUNT=partnerships@amagency.biz`
- `EMAIL_AUTH_CODE=xYeGKyNmK5jFN7Y2`
- `TASK_UPLOAD_MAIL_ACCOUNT=partnerships@amagency.biz`
- `TASK_UPLOAD_MAIL_AUTH_CODE=xYeGKyNmK5jFN7Y2`
- `MAIL_FOLDERS=其他文件夹/邮件备份`

这部分是本地敏感配置，不进 git，但已经在当前机器生效。

## Live Probe

我用当前 `.env` 直接跑了：

```bash
python3 -m feishu_screening_bridge sync-task-upload-mail --env-file .env --task-name MINISO --limit 1 --json
```

真实结果已经明确显示：

- `defaultCredentialMode = default_account_preferred_with_employee_fallback`
- `mailCredentialSource = default_account`
- `mailLoginEmail = partnerships@amagency.biz`
- `sentSince = 2026-03-31`
- `resolvedFolder = 其他文件夹/邮件备份`
- `mailSyncStrategy = shared_backup_folder`

## Boundary

这次 live probe 同时也再次证明：

- 共享邮箱路线已经切过去了
- 但企业邮 IMAP 在 `其他文件夹/邮件备份` 上仍然偶发 `UID => socket error: EOF`

所以现在的状态不是“默认还没切”，而是：

- 默认主线已切成共享邮箱 + 今天窗口
- 共享备份目录本身仍有 IMAP 稳定性风险
