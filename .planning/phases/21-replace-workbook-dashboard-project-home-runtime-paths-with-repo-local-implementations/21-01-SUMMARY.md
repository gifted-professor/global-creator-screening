---
phase: 21-replace-workbook-dashboard-project-home-runtime-paths-with-repo-local-implementations
plan: 01
requirements-completed:
  - DEP-01
  - DEP-02
completed: 2026-03-29
---

# Phase 21-01 Summary

## Goal

把 `import-from-feishu` / `sync-task-upload-view` 的默认 workbook import 与 dashboard 可见面从 legacy external `email` project 切到当前仓库自己的 repo-local runtime。

## Executed Work

- 新增 `feishu_screening_bridge/repo_local_runtime.py`
  - 负责生成 repo-local `summary.json`
  - 负责生成 `project_state.json`
  - 负责把模板 workbook 编译成当前仓库拥有的 parse artifacts
  - 负责写一个简单的本地 `dashboard.html` 作为 operator visibility surface
- 更新 `feishu_screening_bridge/bridge.py`
  - `import_screening_workbook_from_feishu(...)` 在未显式提供 legacy root 时，默认改走 repo-local runtime
  - 仍保留显式 legacy compatibility mode
- 更新 `feishu_screening_bridge/task_upload_sync.py`
  - `sync_task_upload_view_to_email_project(...)` 在未显式提供 legacy root 时，默认改走 repo-local runtime
  - 每个任务写各自的 repo-local `summary/project_state`
  - 聚合输出一个 repo-local sync summary 和本地 dashboard
- 更新 `feishu_screening_bridge/__main__.py`
  - CLI handler 不再把“没给 legacy root”视为默认失败
  - 默认分支直接调用 repo-local import/sync path
  - 非 JSON 输出改成展示 repo-local `summary` 与 `dashboard` 路径

## Validation

- `python3 -m unittest tests.test_main_cli tests.test_feishu_screening_bridge -v`

## Notes

- 这一步完成后，`DEP-01` / `DEP-02` 不再依赖 external full `email` project 才能跑默认 workbook / dashboard path
- external `--email-project-root` / `EMAIL_PROJECT_ROOT` 仍然保留，但只作为显式 compatibility mode
- dashboard 在 repo-local 默认路径下不再要求 external `exports/index.html`；当前交付面是 repo-local `summary.json` + `project_state.json` + 简单本地 `dashboard.html`

## Next

- 继续 Phase `21-02`
- 把默认 project-home / workbench ownership 也切到 repo-local project-state artifact
