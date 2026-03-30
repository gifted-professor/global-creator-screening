# Quick Task 260330-gqs: 默认所有抓取邮箱都改为 partnerships@amagency.biz / xYeGKyNmK5jFN7Y2，统一邮件抓取默认账号来源 - Context

**Gathered:** 2026-03-30
**Status:** Ready for planning

<domain>
## Task Boundary

把任务驱动抓信链路的默认登录凭据统一切到固定邮箱 `partnerships@amagency.biz` 和授权码 `xYeGKyNmK5jFN7Y2`。

要求：

1. `sync-task-upload-mail` 和单入口 keep-list 主链默认优先使用统一抓取邮箱，而不是员工表里的邮箱 / `imap 码`。
2. 仍保留显式覆盖和原有员工映射信息，避免把别的配置入口做没。
3. summary / 测试要能看出当前 mail sync 实际走的是哪种凭据来源。

</domain>

<decisions>
## Implementation Decisions

### 默认凭据来源
- 默认任务抓信优先读 `TASK_UPLOAD_MAIL_ACCOUNT` / `TASK_UPLOAD_MAIL_AUTH_CODE`，未设置时回退到现有 `EMAIL_ACCOUNT` / `EMAIL_AUTH_CODE`。

### 回退行为
- 如果没有配置统一默认凭据，则继续沿用员工信息表里的 `employeeEmail` + `imapCode`。

### 员工映射依赖
- 一旦统一默认凭据可用，即使任务没有匹配到员工表，也允许继续抓信；任务名与邮箱文件夹解析逻辑保持不变。

### 可观察性
- keep-list summary 要新增默认抓取账号来源字段，便于确认是否真的切到了统一邮箱。

### Claude's Discretion
- 这次不扩成新的多套账号调度系统，只把默认凭据优先级和观测面收口清楚。

</decisions>

<specifics>
## Specific Ideas

- 当前 `.env` 已经包含：
  - `EMAIL_ACCOUNT=partnerships@amagency.biz`
  - `EMAIL_AUTH_CODE=xYeGKyNmK5jFN7Y2`
- 真正需要改的是任务驱动 mail sync 仍然优先使用员工表凭据这一层，而不是 standalone `email_sync` 的 env 读取。

</specifics>

<canonical_refs>
## Canonical References

- `feishu_screening_bridge/task_upload_sync.py`
- `feishu_screening_bridge/__main__.py`
- `scripts/run_task_upload_to_keep_list_pipeline.py`
- `tests/test_feishu_screening_bridge.py`
- `tests/test_task_upload_to_keep_list_pipeline.py`

</canonical_refs>
