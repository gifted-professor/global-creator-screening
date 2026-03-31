---
gsd_state_version: 1.0
milestone: v1.3.0
milestone_name: External Email Dependency Decoupling
status: ready_for_milestone_audit
stopped_at: Phase 25 completed on 2026-03-30; next workflow step is `$gsd-audit-milestone`
last_updated: "2026-03-30T15:07:01+08:00"
progress:
  total_phases: 6
  completed_phases: 6
  total_plans: 13
  completed_plans: 13
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-03-30)

**Core value:** 在不打断现有本地工作流的前提下，把飞书内容获取、筛选导入和相关配置集中到一个可持续维护的仓库里。
**Current focus:** Milestone closeout after Phase 25 local operator UI delivery

## Current Position

Phase: 25 (Build local thin operator UI for task-driven screening runs) — COMPLETED
Plan: 2 of 2 completed

## Performance Metrics

**Velocity:**

- Total plans completed: 33
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
| 16 | 2 | n/a | n/a |
| 16.1 | 2 | n/a | n/a |
| 17 | 2 | n/a | n/a |
| 18 | 2 | n/a | n/a |
| 19 | 3 | n/a | n/a |
| 20 | 2 | n/a | n/a |
| 21 | 2 | n/a | n/a |
| 22 | 2 | n/a | n/a |
| 23 | 3 | n/a | n/a |
| 24 | 2 | n/a | n/a |
| 25 | 2 | n/a | n/a |

**Recent Trend:**

- Last 5 plans: n/a
- Trend: Stable

## Accumulated Context

### Decisions

Decisions are logged in PROJECT.md Key Decisions table.
Recent decisions affecting current work:

- [Milestone v1.3.0]: committed scope 先锁定 `DEP-01`（external dependency decoupling）；`QTE-01` 与 `REL-01` 继续 deferred，避免范围耦合
- [Phase 20 Review Intake]: 2026-03-29 的补充建议里，`self-contained repo / 清理 hard-coded external paths / repo-local-first remediation` 被收进当前 decoupling 主线；`backend/app.py` 模块化、shared settings、`pipeline_runtime.py`、SQLite WAL/FTS、upload/job hardening、LLM config consolidation、`pyproject.toml`/lint/typecheck、以及 workbook handle cleanup 先记为 deferred technical debt
- [Phase 20 Verification]: 在当前 checkout 上，`python3 -m unittest tests.test_main_cli tests.test_feishu_screening_bridge -v` 于 2026-03-29 通过；所以外部路径问题当前更像 portability / self-containment debt，而不是这组 targeted tests 的即时红灯
- [Phase 20]: legacy bridge commands 不再隐式默认外部 `email` 项目根目录；compatibility mode 现在必须显式提供 `--email-project-root` / `EMAIL_PROJECT_ROOT`
- [Phase 20]: legacy integration coverage 不再偷偷依赖某台机器上的 sibling checkout；现在只有显式设置 `CHUHAI_LEGACY_EMAIL_PROJECT_ROOT` 时才会跑相关 integration tests
- [Phase 21]: `import-from-feishu` / `sync-task-upload-view` 默认执行路径应切到 repo-local artifact ownership；external `email` root 只保留为显式 compatibility mode
- [Phase 21]: workbook / dashboard / project-home 的 repo-local replacement 不要求 byte-for-byte 重建旧 external read-model stack；machine-readable manifest / summary + current-repo runtime state 是可接受交付面
- [Phase 21]: "输入任务名即运行" 的单入口 orchestration API 和薄 UI 确认有价值，但在当前 workflow 中继续 deferred，等 decoupling 和 Phase 22 bounded validation 完成后再开新里程碑
- [Phase 21 Plan]: `21-01` 先把 workbook import / task-upload sync 的默认 ownership 切到 repo-local，并以 summary/manifest 取代 external dashboard 作为默认可见面
- [Phase 21 Plan]: `21-02` 再移除默认 external project-home/workbench rebuild 依赖，用 repo-local project-state artifact 完成 `DEP-03`
- [Phase 21]: `import-from-feishu` / `sync-task-upload-view` 默认分支现在会直接生成 repo-local `summary.json`、`project_state.json` 和本地 `dashboard.html`
- [Phase 21]: 默认 workbook/dashboard/project-home path 已不再要求 external full `email` project 存在；external root 只保留为显式 compatibility mode
- [Phase 22]: 本轮只验证 decoupled 后 canonical bounded mainline 和 operator fallback contract，不插入新的 task-name orchestration API、UI、或更大样本 proof 范围
- [Phase 22]: fallback/recovery 需要统一成三层说法：repo-local single-entry mainline resume、repo-local bridge outputs、以及显式 legacy compatibility mode
- [Phase 22 Plan]: `22-01` 先重跑 repo-local bridge + final-wrapper 的 targeted regression，并在 `temp/phase22_decoupled_bounded_validation` 留下一轮 fresh bounded proof 和 `.planning/.../22-BOUNDED-REGRESSION.md`
- [Phase 22 Plan]: `22-02` 再把 README / CLI / planning docs 收口成统一 runbook，明确 single-entry resume、repo-local bridge outputs、以及 explicit legacy compatibility mode 三条 operator 路径
- [Phase 22]: decoupled 后的 targeted regression suite 通过，`task upload -> final export` bounded mainline 没有因为 repo-local runtime replacement 而回退
- [Phase 22]: fresh bounded wrapper 顶层失败点是外部 `openai` vision probe 503；上游仍然成功达到 keep-list，说明问题在 provider/channel availability，不在 decoupling contract
- [Phase 22]: summary-driven recovery 已实证成立；operator 可以从 `resume_points.keep_list.recommended_command` 接过 keep-list boundary，并在改用健康 provider 后继续完成 downstream
- [Phase 22]: operator fallback/runbook 现在固定成三层说法：`repo-local single-entry mainline resume`、`repo-local bridge outputs`、`explicit legacy compatibility mode`
- [Phase 25 Intake]: 用户在 2026-03-30 明确把本地薄 operator UI 从 deferred follow-up 提前拉进当前 roadmap，作为已验证主链之上的 local-only control plane
- [Phase 25 Intake]: 第一版 UI 由现有 Flask backend 直接服务，并调用 canonical `scripts/run_task_upload_to_final_export_pipeline.py`；不重写 orchestration 逻辑
- [Phase 25 Intake]: 统一邮箱 `partnerships@amagency.biz` 的 `邮件备份(30316)` 目录当前仍未通过 IMAP 暴露，所以 UI v1 默认继续走已验证的老邮箱 route / 健康视觉 provider
- [Phase 25 25-01]: `/operator`、`/api/operator/tasks`、`/api/operator/runs`、`/api/operator/runs/<id>`、`/api/operator/file` 已落地；第一版 control plane 与 targeted route tests 已通过
- [Phase 23 Plan]: runtime visual contract 先拆成三段：持久化 `active_visual_prompts.json`、把 `visual_feature_group` / 视觉排除项接入 runtime cover/prompt contract、再用 smoke artifact + README 把 prompt precedence 和 fallback 顺序固定下来
- [Phase 23]: runtime 现在已消费模板编译出的 active visual prompts、`visual_feature_group` 和支持的视觉排除项；brand/provider fallback 顺序与 runtime diagnostics 已通过回归覆盖固定下来
- [Phase 24]: positioning-card analysis 现在作为 visual-pass 后的 repo-local stage 接入 downstream runner 和 final wrapper，可导出 machine-readable artifact，且默认 non-blocking
- [Reference Intake]: 外部 `筛号/docs/2026-03-29-qwen-prompt-benchmark.md` 可作为后续视觉优化基线；优先保留 `gpt-5.4` 原 prompt，给 `qwen-vl-max` 单独挂 `v2`，并按 `gpt-5.4 -> qwen-vl-max` 路由，而不是继续强行用单 prompt 逼近 GPT
- [Reference Intake]: 外部 `apify` 项目的 handoff 文档模式可直接复用到当前仓库；开发接手应先看 `README + .planning`，再从 `scripts/run_task_upload_to_final_export_pipeline.py`、`scripts/run_task_upload_to_keep_list_pipeline.py`、`scripts/run_keep_list_screening_pipeline.py`、`backend/app.py` 4 个入口下钻
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
- [Phase 15]: 当前显式 `openai` provider 已 real run 成功；此前 `auth_not_found` 更像旧 provider 路径/请求面不稳定，而不是“现在 key 没填好”
- [Phase 16.1]: 用户验证过的 brand-keyword fast path 必须被 productize 成 repo-local 主线能力，而不是继续留在终端 sidecar
- [Phase 16.1]: 不把 `MINISO`、工作簿列名或输出路径写死进 fast-path；要抽成泛化 contract
- [Phase 16.1]: LLM 只处理 deterministic unique/shared split 和内容规则之后剩下的极小 unresolved tail
- [Phase 16.1]: 单入口 runner 必须显式暴露 `matching_strategy`，而不是静默用 fast-path 替换 legacy enrichment
- [Phase 16]: `mail_sync` 永远由当前 run 增量重跑；只有当上游输入未变化且没有抓到新邮件时，下游 matching / review stages 才允许复用
- [Phase 16]: `keep-list` 是整个 milestone 的 canonical upstream boundary；Phase 17/18 必须建立在这个 handoff contract 上，而不是重新定义边界
- [Phase 17]: repo-local runtime summary 必须显式暴露 resolved input/source/preflight，避免 operator 再靠猜路径和 env 来源排障
- [Phase 17]: legacy `feishu_screening_bridge` 允许继续保留外部 full `email` 依赖，但必须在命令入口显式诊断并给出 repo-local 主线 remediation，不能 silent fallback
- [Phase 18]: 最小可行落地应是 thin final wrapper 或等价 continuation path，复用 upstream/downstream runners，同时把 `keep-list` 保留为内部 canonical resume boundary
- [Phase 18]: 单入口真实 proof 的上游 unresolved-tail review 继续显式走 `.env` 里的 legacy `LLM_*`，下游视觉则保持 `.env.local` + `--vision-provider openai`，避免把两层 provider 语义混在一起
- [Phase 18]: 真实 proof 暴露出来的 staging fallback 和 top-level status aggregation 问题必须当场修掉，不能接受“真实 run 失败但 summary 还写 completed”
- [Phase 19]: Apify reliability contract 先收口到 `backend/app.py` + runner summary surface，而不是急着拆成独立模块；目标是统一语义和恢复能力，不是为了拆文件而拆文件
- [Phase 19]: `scrape_failed` 必须只代表零输出失败；一旦已经有部分 scrape 结果，就要以 partial salvage status 和 `partial_result` 对 operator 可见
- [Phase 19]: final wrapper 必须把 `completed_with_partial_scrape` 当作可交付状态，并保留 `delivery_status` / `platform_statuses`；顶层不能再把部分可交付 run 扁平成 opaque `failed`

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
- Phase 16.1 inserted: Integrate brand-keyword fast-path matching and shared-email resolution
- Phase 17 added: Close repo-local runtime dependency gaps
- Phase 18 added: Validate real bounded end-to-end pipeline
- Phase 19 added: Integrate reusable Apify client abstraction into repo-local total pipeline
- Phase 20 added: Baseline legacy dependency surfaces and lock repo-local replacement contract
- Phase 21 added: Replace workbook/dashboard/project-home runtime paths with repo-local implementations
- Phase 22 added: Validate decoupled runtime stability and operator fallback contract
- Phase 23 added: Wire template-compiled visual prompts into runtime and define visual feature-group contract
- Phase 24 added: Add post-visual-review positioning-card analysis step for approved creators
- Phase 25 added: Build local thin operator UI for task-driven screening runs

### Pending Todos

- Milestone audit / closeout: 执行 `v1.3.0` closeout，确认 Phases 20-25 与 deferred follow-ups 的边界
- Milestone closeout: `v1.3.0` 的 audit / closeout 顺延到 Phase 25 完成之后，再一起确认 Phases 20-25 与 deferred follow-ups 的边界
- Deferred debt intake: `backend/app.py` 模块化 + app factory、shared settings loader、`pipeline_runtime.py` 抽取、SQLite WAL/FTS、upload/job hardening、LLM config consolidation、`pyproject.toml`/lint/typecheck、以及 workbook handle cleanup

### Blockers/Concerns

- 当前仓库仍要兼容 lite `active_rulespec.json`；后续优化不能假设所有入口都会先产出 full `rules`
- 外部 qwen benchmark 仍在 sibling docs；后续 prompt 调优应继续走 override / benchmark route，而不是把 sibling benchmark 文件迁入变成阻塞项
- UI v1 不能假设统一邮箱 `partnerships@amagency.biz` 已经通过 IMAP 暴露 `邮件备份(30316)` 或项目专属目录；默认执行路径要继续沿用已验证的老邮箱 route

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
- 2026-03-28: Complete Phase 16 16-01 by adding `scripts/run_task_upload_to_keep_list_pipeline.py`, a machine-readable single-entry upstream summary contract, reuse-aware artifact handoff, and README docs for the new runner
- 2026-03-28: Complete Phase 16 16-02 by hardening runner resume semantics so downstream reuse only happens when upstream inputs are unchanged and `keep-list` remains the canonical handoff boundary
- 2026-03-28: Insert urgent Phase 16.1 so the validated brand-keyword fast path and shared-email resolution become part of the repo-local mainline instead of a terminal-only sidecar
- 2026-03-28: Complete Phase 16.1 16.1-01 by adding `email_sync/brand_keyword_match.py`, CLI surfaces for `match-brand-keyword` / `split-shared-email`, automatic dedupe + unique/shared-email outputs, and regression coverage
- 2026-03-28: Complete Phase 16.1 16.1-02 by adding `resolve-shared-email` / `llm-final-review`, integrating fast-path strategy into the single-entry runner, and documenting the repo-local operator path
- 2026-03-28: Complete Phase 17 17-01 by surfacing resolved input/source/preflight data across upstream/downstream runner summaries and staging summaries
- 2026-03-28: Complete Phase 17 17-02 by adding structured early diagnostics for legacy external `email` dependencies, missing keep/template artifacts, and downstream runtime import failures
- 2026-03-28: Complete Phase 18 18-01 by adding `scripts/run_task_upload_to_final_export_pipeline.py`, fixing keep-row staging fallback and top-level downstream status aggregation bugs found by the real proof, and landing a real bounded `MINISO` artifact set under `temp/phase18_real_bounded_e2e_final2`
- 2026-03-28: Fix `Country/Region` propagation + Instagram US-region fallback detection so keep-list staging prefers upload-region when present and bio-based prescreen catches common US signals like `NYC`, `South Florida`, `Sedona, Arizona`, and `LA CA`
- 2026-03-28: Complete Phase 18 18-02 by documenting the exact bounded rerun command, keep-list resume path, proof artifact locations, and updated requirement/roadmap/state outcomes
- 2026-03-28: Add an independent `mimo` visual provider branch with raw `chat/completions` HTTP wiring, `api-key` auth, default `mimo-v2-omni` model / `max_completion_tokens=1024`, plus probe/docs regression coverage
- 2026-03-28: Switch default visual provider back to explicit `openai` / `gpt-5.4`, verify a live 2-row visual run on reused Instagram artifacts, and restore full-suite test isolation so `unittest discover` + runtime validation pass with MiMo kept as backup
- 2026-03-29: Complete Phase 19 19-01 by hardening Apify lifecycle salvage semantics, persisting live downstream platform stages, and reserving `scrape_failed` for true no-output failures
- 2026-03-29: Complete Phase 19 19-02 by adding multi-candidate upstream LLM fallback, selected provider/model observability, and absorbed-failure reporting to keep-list summary
- 2026-03-29: Complete Phase 19 19-03 by tightening visual preferred-pool retry behavior, normalizing model diagnostics, preserving `completed_with_partial_scrape` in the final wrapper, and aligning README/operator docs
- 2026-03-30: Quick Task `260330-bsm` — add one automatic Missing re-scrape pass before final export, keep blocking remaining Missing from downstream export, and record artifacts under `.planning/quick/260330-bsm-missing-missing-300-250-missing-final-ex/`
- 2026-03-30: Quick Task `260330-gqs` — default task-upload mail sync to `partnerships@amagency.biz`, surface credential source in keep-list summary, and verify with targeted tests plus one bounded live `MINISO` mail-sync probe under `.planning/quick/260330-gqs-partnerships-amagency-biz-xyegkynmk5jfn7/`
- 2026-03-31: Remove remaining `.planning` absolute workstation paths so the repo can move under a new parent directory without stale local path references
- 2026-03-29: Clarify in README/PROJECT that the Phase 18 bounded proof only proves the repo-local single-entry mainline, not full-batch or multi-platform stability, full legacy-entry decoupling, or non-`openai` provider readiness
- 2026-03-29: Intake external qwen prompt benchmark into local context: future visual tuning should prefer dual-prompt routing (`gpt-5.4` original + `qwen-vl-max` v2) with fixed benchmark harness instead of more blind prompt chasing
- 2026-03-29: Add docs-first developer handoff guidance modeled on the external `apify` project: docs explain the full chain, while 4 code entrypoints are enough to start safe changes
- 2026-03-29: Complete Phase 20 20-01 by inventorying every remaining workbook / dashboard / project-home external dependency surface and splitting current decoupling scope from deferred engineering debt
- 2026-03-29: Complete Phase 20 20-02 by making legacy diagnostics repo-local-first, removing private absolute-path defaults from docs/sample/tests, and gating legacy integration coverage behind explicit `CHUHAI_LEGACY_EMAIL_PROJECT_ROOT`
- 2026-03-30: Complete Phase 23 by wiring active visual prompts, `visual_feature_group`, cover-limit/runtime diagnostics, and rulespec fallback prompts into the visual-review runtime with regression coverage
- 2026-03-30: Complete Phase 24 by adding post-visual-review `positioning_card_analysis`, backend positioning artifacts, downstream/top-level summary visibility, operator docs, and non-blocking stage semantics

## Session Continuity

Last session: 2026-03-30 15:07
Stopped at: Phase 25 completed on 2026-03-30; next workflow step is `$gsd-audit-milestone`
Resume file: None
