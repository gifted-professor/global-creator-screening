---
gsd_state_version: 1.0
milestone: none
milestone_name: between-milestones
status: milestone_complete
stopped_at: `v1.2.0` archived on 2026-03-29; next workflow step is `$gsd-new-milestone`
last_updated: "2026-03-29T08:00:00+08:00"
progress:
  total_phases: 5
  completed_phases: 5
  total_plans: 11
  completed_plans: 11
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-03-29)

**Core value:** 在不打断现有本地工作流的前提下，把飞书内容获取、筛选导入和相关配置集中到一个可持续维护的仓库里。
**Current focus:** between milestones; decide next committed scope from `DEP-01`, `QTE-01`, and `REL-01`

## Current Position

No active phase.
Last shipped milestone: `v1.2.0 End-to-End Single-Entry Pipeline Verification` — archived 2026-03-29 with audit status `tech_debt`.

## Performance Metrics

**Velocity:**

- Total plans completed: 11
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

### Pending Todos

- 决定何时对多平台或更大样本补下一轮 live stability proof，而不只停留在 bounded validation
- 决定报价结果接入是不是下一里程碑的 committed scope，还是继续延后
- 决定是否在 `openai` 之外继续补其他 provider 的 live proof run
- 在下一次 `$workflow` 里决定是否直接启动 `$gsd-new-milestone`

### Blockers/Concerns

- `feishu_screening_bridge` 的 workbook / dashboard / project-home 旧流程仍依赖外部全量 `email` 项目，但现在已经有显式 early diagnostics 和 remediation
- 报价结果还没有正式灌入 `筛号` 后端的数据入口
- 当前只对 `openai` 留下了真实 non-error proof run；其他 provider 还没有 live 可用性证明
- 当前 998Code / Apify 侧的瞬时网络抖动已经有 repo-local retry / salvage / fallback contract，但还没有多平台或大样本 live proof
- 当前真实 proof 仍然主要是 bounded 的 `instagram 1` 或局部 live rerun；它证明的是主链 contract 与恢复语义，不是全量批次稳定性证明
- `v1.1.0` 归档前没有单独补 milestone audit 文件，这被接受为流程债
- 当前 16 + 16.1 + 17 + 18 + 19 已经解决了“单入口 bounded E2E proof + reliability contract”的主要编排、fast-path 融合、resume contract、runtime resolution、early diagnostics 和 partial-delivery semantics；后续主线更偏向 milestone closeout 和更大样本验证

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

## Session Continuity

Last session: 2026-03-29 00:00
Stopped at: `v1.2.0` archived on 2026-03-29; next workflow step is `$gsd-new-milestone`
Resume file: None
