# Milestones

## Next Candidate: v1.4.0 Shared Mailbox Post-Sync Routing and Feishu Incremental Update

**Status:** queued after current `v1.3.0` gap-closure / re-audit work

**Planned first phase:** `Phase 30`

**Intent:**

- 以共享邮箱本地 `email_sync.db` 为统一输入
- 按 `task-upload` 当前有效任务分堆
- 同项目内按 `达人ID + 平台` 做增量判定
- 已筛号达人只更新邮件字段和最新 `.eml`
- 新达人继续走完整筛号链路并写回各自飞书表

**Reference:**

- `.planning/v1.4.0-PHASE-30-INTAKE.md`

---

## v1.2.0 End-to-End Single-Entry Pipeline Verification (Shipped: 2026-03-29)

**Phases completed:** 5 roadmap phases through Phase 19

**Key accomplishments:**

- Shipped a repo-local single-entry path from task upload through keep-list to final export
- Productized the brand-keyword fast path and shared-email resolution as a first-class upstream strategy
- Hardened runtime contracts around resolved inputs, legacy dependency diagnostics, and keep-list resume boundaries
- Proved a real bounded `MINISO` end-to-end run and documented the exact rerun command plus artifact locations
- Added reliability semantics for upstream LLM failover, downstream partial scrape salvage, and visual model diagnostics

**Known gaps accepted at archive time:**

- `DEP-01`: legacy workbook / dashboard / project-home flows still depend on the external full `email` project
- `QTE-01`: quote results are still not wired into screening runtime or final export
- `REL-01`: live evidence is still bounded-first and mostly `instagram/openai`-centric rather than broad multi-platform stability proof

---

## v1.1.0 Visual Provider Reliability and Downstream Hardening (Shipped: 2026-03-28)

**Phases completed:** 2 roadmap phases through Phase 15

**Key accomplishments:**

- Added backend-owned vision provider snapshot, preflight, and structured early-failure diagnostics
- Surfaced `vision_preflight`, provider source, and visual gate details directly in keep-list and smoke summaries
- Added deterministic `--vision-provider` selection and a lightweight live provider probe for bounded runs
- Proved a real `MINISO` bounded `instagram` run with `openai` can complete scrape, prescreen, visual review, and export
- Left behind reproducible real-run artifacts under `temp/phase15_probe_only_openai` and `temp/phase15_bounded_openai_live`

**Known gaps accepted at archive time:**

- No dedicated milestone audit file was run before archival
- The real proof currently covers `openai` and bounded `instagram 1`, not full-batch stability or the other providers
- Some legacy workbook / dashboard / project-home flows still depend on the external full `email` project
- Quote results are still not formally wired into the screening runtime or final export path

---

## v1.0.0 Consolidated Local Creator Screening Pipeline (Shipped: 2026-03-28)

**Phases completed:** 13 roadmap phases through Phase 13

**Key accomplishments:**

- Consolidated the Feishu bridge, task-driven mail sync, workbook parsing, and screening backend into one repo-local workflow
- Verified the real `MINISO` chain from task upload lookup through mailbox sync, enrichment, and screening-input preparation
- Switched creator-source preparation and later matching to the Feishu task-upload `发信名单` path instead of the old local workbook-only flow
- Built the production duplicate-review chain from `高置信` to final reviewed keep workbook
- Wired the reviewed keep workbook back into the screening mainline and proved bounded downstream execution
- Left behind real bounded MINISO artifacts covering scrape, prescreen, visual-review invocation, and export download

**Known gaps accepted at archive time:**

- No dedicated milestone audit file was run before archival
- The active visual provider can still return `openai: HTTP 500 auth_not_found: no auth available`
- Some legacy workbook / dashboard / project-home flows still depend on the external full `email` project

---
