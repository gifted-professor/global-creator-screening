# Phase 14: Stabilize visual provider config and preflight - Context

**Gathered:** 2026-03-28
**Status:** Ready for planning
**Source:** User direction after `v1.0.0` archive to go straight into planning, with the next mainline focused on fixing visual provider auth and making bounded visual validation reliable

<domain>
## Phase Boundary

This phase starts from the already working keep-list downstream runner and the current visual-review failure mode, where the chain reaches `visual-review` but can still end in `openai: HTTP 500 auth_not_found: no auth available`. The scope is to make vision-provider configuration diagnosable before or at the start of the visual-review step, and to expose enough structured diagnostics in backend and runner summaries that operators can tell which provider/model/base URL/auth surface is actually in play. This phase does not redesign prescreen rules, does not change duplicate-review decisions, and does not yet promise a successful non-error live visual run; that success proof belongs to Phase 15.

</domain>

<decisions>
## Implementation Decisions

### Existing Runtime Contract To Preserve
- The keep-list downstream entrypoint remains:
  - `scripts/run_keep_list_screening_pipeline.py`
- Visual-review execution remains owned by:
  - `backend.app.perform_visual_review(...)`
  - `POST /api/jobs/visual-review`
- Screening export semantics remain unchanged:
  - prescreen review workbook
  - image review workbook
  - test-info outputs
  - final review workbook
- This phase should improve config resolution, preflight, and diagnostics without rewriting the rest of the screening pipeline.

### Current Failure Shape Is Already Narrow
- The current bounded MINISO validation proves the mainline already reaches:
  - staging
  - scrape
  - prescreen
  - visual-review invocation
  - export download
- The current blocker is no longer “视觉没走到”，而是“视觉走到了，但 provider auth 失败”.
- The current observed failure string is:
  - `openai: HTTP 500 auth_not_found: no auth available`

### Provider Resolution Reality
- `backend/app.py` is the owner of visual provider config.
- Backend visual providers are declared in `VISION_PROVIDER_CONFIGS` with three supported provider slots:
  - `openai`
  - `quan2go`
  - `lemonapi`
- Provider availability is currently determined only by whether the corresponding API key env var is non-empty.
- `backend.app.load_dotenv_local()` only auto-loads:
  - `.env.local`
- The keep-list runner `--env-file` currently affects `prepare_screening_inputs.py`, but it does not directly reconfigure backend visual providers.
- This mismatch is likely a major source of operator confusion and must be made explicit in diagnostics and docs.

### Diagnostics Contract For This Phase
- The backend should expose a structured “vision provider snapshot / preflight” surface that tells operators:
  - which provider candidates exist
  - which env key each provider depends on
  - whether the API key is present
  - which model/base URL/api style will be used
  - whether the provider is considered runnable
- Missing or obviously incomplete config should be reported before the operator waits for a full visual-review job to finish.
- Runner summaries should persist provider diagnostics alongside:
  - `vision_providers`
  - `visual_job`
  - `artifact_status`
  - `exports`

### Scope Split Across Plans
- `14-01` should focus on backend-side provider resolution, preflight, and failure semantics.
- `14-02` should focus on runner-side summary fields, operator docs, and a bounded validation artifact that proves the new diagnostics land where operators actually look.
- The non-error live run is intentionally deferred to Phase 15 so this phase can land a stable diagnostic contract first.

### Claude's Discretion
- Exact names of the new provider snapshot/preflight helpers
- Whether diagnostics surface through `/api/health`, a dedicated helper, the visual-review start path, or all of the above
- Exact summary JSON shape, as long as operators can reliably answer “which provider config did this run actually use?” and “why did visual review fail?”

</decisions>

<specifics>
## Specific Ideas

- `backend.app.get_available_vision_providers()` currently filters only on API key presence and returns normalized provider names.
- `backend.app.health_check()` currently reports only:
  - `vision: configured|unconfigured`
  - `vision_providers: [...]`
- `scripts/run_keep_list_screening_pipeline.py` currently copies only provider names into run summary and has no explicit preflight section.
- The current bounded live artifact that captured the auth failure is:
  - `temp/keep_list_bounded_live_validation_escalated/summary.json`
- The current exported workbook already shows the runtime symptom clearly:
  - `visual_status = Error`
  - `final_status = Error`
- `.env.example` already documents all vision-related keys, but the repo docs do not yet explain the actual precedence between `.env.local`, shell env, and runner flags for backend visual-review.

</specifics>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Phase Continuity
- `.planning/PROJECT.md` — new milestone goals and current project constraints
- `.planning/REQUIREMENTS.md` — Phase 14 requirement mapping (`VIS-01`, `VIS-02`, `SCR-03`)
- `.planning/ROADMAP.md` — Phase 14 goal, success criteria, and dependency on Phase 13
- `.planning/STATE.md` — current blockers and the explicit follow-up after `v1.0.0`
- `.planning/MILESTONES.md` — what shipped in `v1.0.0` and which known gaps were accepted
- `.planning/phases/13-wire-keep-list-outputs-into-screening-pipeline/13-02-SUMMARY.md` — proof that the downstream chain already reaches visual-review invocation

### Existing Runtime Surface
- `backend/app.py` — provider config, health endpoint, visual-review execution, job APIs
- `scripts/run_keep_list_screening_pipeline.py` — current keep-list downstream runner and run summary
- `scripts/run_screening_smoke.py` — parallel runner pattern already used for downstream summaries
- `README.md` — current operator docs for keep-list and visual-review runtime
- `.env.example` — documented env surface for vision providers

### Real Validation Artifacts
- `temp/keep_list_bounded_live_validation_escalated/summary.json` — current bounded run showing visual auth failure
- `temp/keep_list_bounded_live_validation_escalated/exports/instagram/instagram_final_review.xlsx` — current exported workbook showing `visual_status = Error`

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `backend.app.get_available_vision_providers()` already centralizes provider enumeration and is the natural place to grow richer diagnostics.
- `backend.app.health_check()` already exposes backend readiness and can carry more detailed vision preflight output.
- `backend.app.start_visual_review_job()` already rejects obvious “no provider configured” cases before creating a job.
- `scripts.run_keep_list_screening_pipeline.run_keep_list_screening_pipeline(...)` already writes per-platform summaries after scrape and visual-review.

### Established Gaps
- Backend vision config currently has no structured snapshot explaining which env key/base URL/model/api style was actually selected.
- Provider readiness is binary today: “API key present” versus “not present”; there is no intermediate diagnostic for incomplete or misleading config.
- The keep-list runner summary currently records `vision_providers` but not enough provider metadata to explain an auth failure.
- The current backend auto-load behavior only reads `.env.local`, which is easy to miss because other flows in this repo also talk about `.env`.

### Recommended Integration Shape
- Add backend-owned provider snapshot/preflight helpers first.
- Reuse those helpers from both `/api/health` and the keep-list runner summary path.
- Treat “config diagnosis” as a first-class artifact in this phase, not just a side effect of a failed live run.

</code_context>

<deferred>
## Deferred Ideas

- Retrying with a different provider automatically after a hard auth error
- Reordering provider precedence or changing default provider strategy
- The actual non-error live visual validation run, which belongs to Phase 15

</deferred>

---

*Phase: 14-stabilize-visual-provider-config-and-preflight*
*Context gathered: 2026-03-28*
