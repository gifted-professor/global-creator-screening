---
status: completed
mode: quick-full
description: 默认所有抓取邮箱都改为 partnerships@amagency.biz / xYeGKyNmK5jFN7Y2，统一邮件抓取默认账号来源。
must_haves:
  truths:
    - 任务驱动 mail sync 默认优先使用统一抓取邮箱和授权码
    - 未配置统一默认凭据时仍可回退到员工表邮箱和 imap 码
    - keep-list summary 必须能显示默认抓取邮箱来源
  artifacts:
    - feishu_screening_bridge/task_upload_sync.py
    - feishu_screening_bridge/__main__.py
    - scripts/run_task_upload_to_keep_list_pipeline.py
    - tests/test_feishu_screening_bridge.py
    - tests/test_task_upload_to_keep_list_pipeline.py
  key_links:
    - feishu_screening_bridge/task_upload_sync.py
    - feishu_screening_bridge/__main__.py
    - scripts/run_task_upload_to_keep_list_pipeline.py
---

# Quick Task 260330-gqs Plan

## Task 1

- files: `feishu_screening_bridge/task_upload_sync.py`, `feishu_screening_bridge/__main__.py`
- action: 给任务驱动抓信链路增加统一默认账号优先级，CLI/env 默认先读 task-upload 专用键，再回退到 `EMAIL_ACCOUNT` / `EMAIL_AUTH_CODE`。
- verify: 默认凭据存在时，mail sync 使用统一账号；缺失时仍保留员工映射回退。
- done: 已完成

## Task 2

- files: `scripts/run_task_upload_to_keep_list_pipeline.py`, `.env.example`
- action: keep-list runner 解析并透出默认抓取账号来源，把 mail sync summary 收口到可观察 contract，并补充 env 模板注释。
- verify: summary 明确显示默认抓取账号和来源键位，不再需要靠猜测确认。
- done: 已完成

## Task 3

- files: `tests/test_feishu_screening_bridge.py`, `tests/test_task_upload_to_keep_list_pipeline.py`
- action: 增加默认凭据接管 mail sync、无员工匹配仍可抓取、以及 runner 传参与 summary 透出的回归测试。
- verify: `python3 -m unittest tests.test_feishu_screening_bridge tests.test_task_upload_to_keep_list_pipeline -v`
- done: 已完成
