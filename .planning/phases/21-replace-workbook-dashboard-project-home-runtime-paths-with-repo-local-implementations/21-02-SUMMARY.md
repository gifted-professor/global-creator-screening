---
phase: 21-replace-workbook-dashboard-project-home-runtime-paths-with-repo-local-implementations
plan: 02
requirements-completed:
  - DEP-03
completed: 2026-03-29
---

# Phase 21-02 Summary

## Goal

把默认 project-home / workbench ownership 从 external read-model rebuild stack 收口到当前仓库自己的 machine-readable project-state artifact。

## Executed Work

- 让 repo-local import/sync 默认分支完全绕开 external:
  - `ensure_project`
  - `import_requirements`
  - `rebuild_project_home_read_model`
  - `rebuild_project_workbench_read_model`
  - `export_dashboard`
- 把 repo-local project ownership 统一落成：
  - `summary.json`
  - `project_state.json`
  - 本地 `dashboard.html`
- 保留 `feishu_screening_bridge/email_project.py` 作为显式 compatibility boundary
  - 只有 operator 明确传 `--email-project-root` / `EMAIL_PROJECT_ROOT` 时才进入 legacy branch
- 更新测试，证明：
  - 默认分支不需要 external root
  - 默认分支会返回 repo-local project-state artifact 路径
  - legacy integration coverage 仍然只在 `CHUHAI_LEGACY_EMAIL_PROJECT_ROOT` 显式存在时运行

## Validation

- `python3 -m unittest tests.test_main_cli tests.test_feishu_screening_bridge -v`

## Notes

- 当前 repo-local project-home replacement 没有追求 byte-for-byte 复刻旧 external read-model stack
- 这一轮交付的正式 contract 是：
  - repo-local `summary.json`
  - repo-local `project_state.json`
  - repo-local `dashboard.html`
- 这已经满足当前 milestone 的 decoupling 目标，也更适合后续自动化或单入口 orchestration API 消费

## Next

- Phase 21 完成
- 下一步进入 Phase 22，对 decoupled runtime 做 bounded regression 和 fallback/runbook 收口
