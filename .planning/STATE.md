---
gsd_state_version: 1.0
milestone: v1.2.0
milestone_name: End-to-End Single-Entry Pipeline Verification
status: active
stopped_at: Phase 16 planned; next workflow step is discuss or plan Phase 16
last_updated: "2026-03-28T02:30:00.000Z"
last_activity: 2026-03-28 — started milestone v1.2.0 to unify the validated upstream and downstream chains into one repo-local end-to-end runner
progress:
  total_phases: 3
  completed_phases: 0
  total_plans: 6
  completed_plans: 0
  percent: 0
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-03-28)

**Core value:** 在不打断现有本地工作流的前提下，把飞书内容获取、筛选导入和相关配置集中到一个可持续维护的仓库里。
**Current focus:** 定义并计划 Phase 16，把 task-upload 起点到 keep-list 生成编排成单入口 orchestration

## Current Position

Phase: 16 of 18 (Build single-entry task-to-keep-list orchestrator)
Plan: 0 of 2 in current phase
Status: Phase 16 planned and ready for discussion/planning
Last activity: 2026-03-28 — started `v1.2.0` with single-entry E2E as the next mainline objective

Progress: [░░░░░░░░░░] 0%

## Performance Metrics

**Velocity:**

- Total plans completed: 6
- Average duration: n/a
- Total execution time: n/a

**By Phase:**

| Phase | Plans | Total | Avg/Plan |
|-------|-------|-------|----------|
| 1 | 1 | n/a | n/a |
| 2 | 2 | n/a | n/a |
| 3 | 1 | n/a | n/a |
| 4 | 2 | n/a | n/a |
| 5 | 2 | n/a | n/a |
| 6 | 1 | n/a | n/a |
| 7 | 1 | n/a | n/a |
| 8 | 1 | n/a | n/a |
| 9 | 1 | n/a | n/a |
| 14 | 2 | n/a | n/a |
| 15 | 2 | n/a | n/a |

**Recent Trend:**

- Last 5 plans: n/a
- Trend: Stable

## Accumulated Context

### Decisions

Decisions are logged in PROJECT.md Key Decisions table.
Recent decisions affecting current work:

- [Phase 1]: 先补最小 `.planning` 再迁移代码
- [Phase 2]: 迁移以整包复制旧仓库的飞书桥接实现为主，避免遗漏
- [Phase 2]: 暂不切换默认 `EMAIL_PROJECT_ROOT`，保持现有可工作路径
- [Quick Task]: 先做只读 inspect，确认任务上传、员工邮箱与 `imap 码` 映射，再决定是否接邮件抓取
- [Phase 4]: 当前仓库只迁入 `email_sync` 的任务驱动核心能力，不直接替代外部全量 `email` 项目
- [Phase 4]: `feishu_screening_bridge` 加入隔离导入，避免本仓库 `email_sync` 与外部 `email/email_sync` 冲突
- [Phase 5]: `模板解析` 是独立模板编译闭环，应该作为新 phase 迁入，而不是继续塞进邮件抓取 phase
- [Phase 5]: 当前阶段只锁定“下载模板 -> 模板解析”，不承诺后续筛号/导入链路
- [Phase 5]: `inspect-task-upload --parse-templates` 成为当前最小任务闭环入口
- [Phase 6]: 任务邮件抓取不再额外复制旧项目代码，而是直接桥接到本仓库已迁入的 `email_sync`
- [Phase 6]: 任务邮箱文件夹优先按 `任务名`、其次按 `其他文件夹/<任务名>` 自动解析，允许覆盖映射
- [Phase 7]: 报价抽取继续复用本地 `messages` / `message_index` / `threads`，不再依赖 sibling 项目
- [Phase 7]: 真实飞书达人表导出的 xlsx 当前只拿到 `record_id`，因此验证改用本地真实达人库文件 `【测试】达人库.xlsx`
- [Phase 8]: `筛号` 后端按整包迁入当前仓库，优先保证当前本地链路可运行
- [Phase 8]: 视觉复核默认并发采用 benchmark 结论，默认值设为 `6`
- [Phase 9]: 先把模板 rulespec 和达人匹配名单写入筛号当前输入状态，再考虑继续打通报价或自动跑 job
- [Phase 10]: 每个任务对应的红人库来源正式切到任务上传里的飞书 `发信名单`，筛号输入准备改为优先走 task-driven 下载
- [Phase 12]: duplicate review 的主产物链固定为“高置信 workbook -> 按我们去重 -> 去重 -> llm_candidates -> llm_review -> reviewed -> keep”
- [Phase 12]: duplicate review 的 provider 切换只改 `.env` 的 OpenAI-compatible 配置，不改业务代码
- [Phase 12]: 当前 `.env.local` 的 998Code provider 在真实 MINISO review 中不稳定，live validation 改用 `.env` 里的 legacy DeepSeek 配置完成

### Roadmap Evolution

- Phase 5 added: Integrate workbook template parser into current repo
- Phase 6 added: Integrate task-driven mail sync bridge
- Phase 7 added: Integrate creator enrichment into current repo
- Phase 8 added: Integrate screening backend into current repo
- Phase 9 added: Bridge upstream outputs into screening backend state
- Phase 10 added: Switch creator source to Feishu 发信名单
- Phase 11 added: Resolve duplicate creator matches with group-level LLM review
- Phase 12 added: Align production duplicate-review pipeline with keep-list outputs
- Phase 13 added: Wire keep-list outputs into screening pipeline
- Phase 14 added: Stabilize visual provider config and preflight
- Phase 15 added: Validate non-error bounded visual review flow
- Phase 16 added: Build single-entry task-to-keep-list orchestrator
- Phase 17 added: Close repo-local runtime dependency gaps
- Phase 18 added: Validate real bounded end-to-end pipeline

### Pending Todos

- 讨论并规划 Phase 16，锁定单入口 orchestration 的输入、输出和 resume contract
- 决定报价结果接入是不是 `v1.2.0` 的 committed scope，还是继续延到后续 milestone
- 决定 legacy 外部 `email` 依赖是本 milestone 必须消除，还是先做显式诊断与边界说明
- 决定是否在 `openai` 之外继续补其他 provider 的 live proof run

### Blockers/Concerns

- `feishu_screening_bridge` 的 workbook / dashboard / project-home 旧流程仍依赖外部全量 `email` 项目
- 报价结果还没有正式灌入 `筛号` 后端的数据入口
- 当前只对 `openai` 留下了真实 non-error proof run；其他 provider 还没有 live 可用性证明
- 当前 998Code provider 对长跑真实 review 不稳定，可能在 `chat_completions` 侧返回 520 或长时间挂起
- 当前 Phase 15 的成功验证是 bounded 的 `instagram 1`，不是全量批次稳定性证明
- `v1.1.0` 归档前没有单独补 milestone audit 文件，这被接受为流程债
- 当前仍缺少“从任务上传起点到最终导出”的单入口 orchestration，operator 还需要手工拼接两段已验证链路

### Quick Tasks Completed

- 2026-03-27: Migrate `上传飞书` bridge code, env references, tests, and codebase docs
- 2026-03-27: Add `inspect-task-upload` to read task upload rows, match employee email / `imap 码`, and download templates
- 2026-03-27: Verify MINISO task end-to-end through Feishu lookup, IMAP login, and one-message limit sync
- 2026-03-27: Migrate core `email_sync` package and selected tests into current repo
- 2026-03-27: Isolate external `email` project imports so full test discovery stays green after migration
- 2026-03-27: Review sibling `模板解析` module and add Phase 5 for parser integration
- 2026-03-27: Create Phase 5 context and execution plans for download-then-parse workbook flow
- 2026-03-27: Execute Phase 5 and verify `inspect-task-upload --parse-templates` on real Feishu task data
- 2026-03-27: Execute Phase 6 and verify `sync-task-upload-mail --task-name MINISO --limit 1` against real Feishu + IMAP data
- 2026-03-27: Execute Phase 7 and verify `【测试】达人库.xlsx` against the local MINISO mail DB, producing match and quote outputs
- 2026-03-27: Execute Phase 8 and migrate `筛号` backend/config/scripts/temp assets into current repo
- 2026-03-27: Install screening backend dependencies in `backend/.venv`, pass runtime validation, and align visual review default concurrency to 6
- 2026-03-27: Add `scripts/prepare_screening_inputs.py` to bridge template rulespec and creator workbook outputs into screening backend state
- 2026-03-27: Prepare real MINISO screening inputs, writing 454 matched creators and the parsed template rulespec into current repo runtime files
- 2026-03-27: Expose task-upload `发信名单` metadata and add task-driven asset download helpers in `feishu_screening_bridge`
- 2026-03-27: Prepare real MINISO screening inputs directly from Feishu task-upload template + `发信名单`, writing 15293 TikTok and 8254 Instagram creators into current repo runtime files
- 2026-03-27: Add duplicate-group preparation and sample-first review runner for Phase 11, then validate a 3-group real MINISO sample
- 2026-03-27: Complete group-level duplicate review with real LLM calls on 3 MINISO groups and emit audited row/group outputs
- 2026-03-27: Default `email_sync sync` and `sync-task-upload-mail` to a recent 3-calendar-month window unless `--sent-since` explicitly overrides it
- 2026-03-27: Make `email_sync enrich-creators` accept task-driven Feishu `发信名单` inputs so future matching no longer depends on `【测试】达人库.xlsx`
- 2026-03-27: Plan Phase 12 to align duplicate review with production keep-list outputs and `.env`-driven OpenAI-compatible provider switching
- 2026-03-27: Complete Phase 12 12-01 by generating production `按我们去重 / 去重 / llm_candidates` artifacts and validating them on real MINISO data
- 2026-03-27: Complete Phase 12 12-02 by generating real `llm_review / reviewed / keep` outputs; live validation succeeded with the legacy DeepSeek provider after the current 998Code endpoint proved unstable
- 2026-03-28: Complete Phase 13 13-01 by accepting reviewed keep workbooks as staging input, adding `scripts/run_keep_list_screening_pipeline.py`, and validating real MINISO staged counts (`TikTok 122 / Instagram 146 / YouTube 15`)
- 2026-03-28: Complete Phase 13 13-02 by running a bounded real MINISO keep-list downstream validation (`instagram` 1 identifier), reaching scrape, prescreen, visual-review invocation, and export download under `temp/keep_list_bounded_live_validation_escalated`
- 2026-03-28: Complete Phase 14 14-01 by adding backend vision provider snapshot/preflight diagnostics, exposing them through `/api/health`, and returning structured preflight errors on visual-review start
- 2026-03-28: Complete Phase 14 14-02 by wiring backend `vision_preflight` into keep-list/smoke summaries, updating README operator docs, and leaving a fresh diagnostic artifact under `temp/keep_list_visual_diagnostic_phase14`
- 2026-03-28: Complete Phase 15 15-01 by adding explicit visual provider selection, a lightweight live provider probe endpoint, and runner support for `--vision-provider` / `--probe-vision-provider-only`
- 2026-03-28: Complete Phase 15 15-02 by proving a real bounded MINISO Instagram run with explicit `openai` completes scrape, prescreen, visual review, and export without `auth_not_found`, leaving artifacts under `temp/phase15_bounded_openai_live`

## Session Continuity

Last session: 2026-03-28 02:30
Stopped at: `v1.2.0` started; next workflow step is discuss/plan Phase 16
Resume file: None
