# Creator Screening End-to-End Runbook

## Purpose

This document is the current operator runbook for the repo’s full creator-screening chain.

It is meant to answer four practical questions:

1. which script is the canonical entry for each stage
2. what artifacts each stage reads and writes
3. which implementation details matter when a run fails or needs a partial rerun
4. which pitfalls have already been discovered in real runs and should not be re-learned the hard way

This runbook reflects the current behavior on the `main` line after the April 2 integration fixes.

## Scope

This document covers the chain from:

- Feishu task upload inspection
- task asset download
- shared mailbox / task mailbox email fetch
- sending list normalization and creator-mail matching
- keep-list generation
- downstream scrape
- downstream visual review
- final export merge
- final Feishu upload
- optional shared-mailbox post-sync rewrite

It does not document every helper module in the repo. It focuses on the operational path that people actually run.

## Canonical Entry Points

| stage | canonical script | when to use it |
| --- | --- | --- |
| full chain | `scripts/run_task_upload_to_final_export_pipeline.py` | run task upload all the way to final export and Feishu upload |
| upstream only | `scripts/run_task_upload_to_keep_list_pipeline.py` | stop at keep-list generation and inspect email matching quality |
| downstream only | `scripts/run_keep_list_screening_pipeline.py` | rerun scrape / visual / export from an existing keep workbook |
| shared mailbox sync only | `scripts/run_shared_mailbox_sync.py` | only refresh the shared mailbox cache / `email_sync.db` |
| post-sync rewrite only | `scripts/run_shared_mailbox_post_sync_pipeline.py` | re-filter an existing final payload using real reply threads, then rewrite Feishu |

## High-Level Data Flow

1. `run_task_upload_to_final_export_pipeline.py`
   reads Feishu task upload, expands group tasks if needed, and orchestrates one upstream child run plus one downstream child run per real task.
2. `run_task_upload_to_keep_list_pipeline.py`
   downloads the sending list and template workbook, syncs the mailbox, and writes a keep workbook.
3. `run_keep_list_screening_pipeline.py`
   stages the keep workbook into backend runtime data, runs scrape, visual review, positioning analysis, final export merge, and final Feishu upload.
4. `run_shared_mailbox_post_sync_pipeline.py`
   is the repair / rewrite path after the fact. It uses a synced mailbox DB to remove rows where the creator never replied, fix the last-reply fields, and correct owner routing before uploading again.

The main artifacts that matter across stages are:

- `summary.json`
- `workflow_handoff.json`
- `task_spec.json`
- keep workbook
- `all_platforms_final_review.xlsx`
- `all_platforms_final_review_payload.json`

The canonical internal boundary of the full chain is the keep workbook.

That is why the repo supports a very practical split:

- upstream: task upload -> keep workbook
- downstream: keep workbook -> scrape / visual / upload

## Required Configuration

### Feishu

Required for any task-upload-driven run:

- `TASK_UPLOAD_URL`
- `EMPLOYEE_INFO_URL`
- `FEISHU_APP_ID`
- `FEISHU_APP_SECRET`

Current note:

- `run_shared_mailbox_post_sync_pipeline.py` rewrite mode also supports `FEISHU_SOURCE_URL` as a fallback when the dedicated `TASK_UPLOAD_URL` / `EMPLOYEE_INFO_URL` are absent.

### Mailbox

If the upstream run is responsible for fetching mail itself, it needs:

- `IMAP_HOST`
- `IMAP_PORT`
- mailbox account credentials resolved by the task upload row or local overrides

If a shared mailbox DB is already available, the upstream run can reuse it instead of fetching again:

- `existing_mail_db_path`
- `existing_mail_raw_dir`
- `existing_mail_data_dir`

### Scrape

Apify-backed scrape stages need at least one funded token:

- `APIFY_TOKEN`

Optional pool:

- `APIFY_BACKUP_TOKENS`
- `APIFY_API_TOKEN`
- `APIFY_FREE_TOKENS`

The downstream scrape stage will fail as `missing_profiles_blocked` when the token pool cannot actually return the requested profiles, even if the env vars are present.

### Visual

The local OpenAI-compatible visual path in this repo currently assumes:

- `OPENAI_BASE_URL`
- `OPENAI_API_KEY`
- `OPENAI_VISION_MODEL`
- `VISION_PROVIDER_PREFERENCE=openai`

Current production caveat:

- `OPENAI_BASE_URL` must include `/v1` when using local `cliproxyapi` style proxies.
- The current OpenAI visual implementation uses the Responses API, not Chat Completions.
- For local proxy setups, the actual request is `POST <OPENAI_BASE_URL>/responses`.

Example:

```env
OPENAI_BASE_URL=http://127.0.0.1:8317/v1
OPENAI_API_KEY=...
OPENAI_VISION_MODEL=gpt-5.4
VISION_PROVIDER_PREFERENCE=openai
OPENAI_MAX_INFLIGHT_REQUESTS=8
```

## Full-Chain Command

The main full-chain entry is:

```bash
python3 scripts/run_task_upload_to_final_export_pipeline.py --task-name MINISO
```

Useful flags:

```bash
python3 scripts/run_task_upload_to_final_export_pipeline.py \
  --task-name Duet1 \
  --env-file .env \
  --platform tiktok \
  --vision-provider openai
```

What the full runner does:

- preflight checks env, Feishu config, and output-root materialization
- materializes a run root and `task_spec.json`
- runs upstream through `scripts/run_task_upload_to_keep_list_pipeline.py`
- runs downstream through `scripts/run_keep_list_screening_pipeline.py`
- writes top-level `summary.json` and `workflow_handoff.json`

Code map:

- `scripts/run_task_upload_to_final_export_pipeline.py`
- task-group expansion: `_resolve_task_group_members()`
- single-task execution: `_run_single_task_upload_to_final_export_pipeline()`

## Step 1: Task Upload Inspection And Task Group Expansion

This is the first place where operators often get confused.

The input `task_name` is not always a one-to-one mapping to a single Feishu task row.

Current behavior:

- the full runner can expand group aliases to multiple real tasks
- example: `duet` may resolve to `Duet1` and `Duet2`
- the full runner currently fans these out serially, not in parallel

Why serial fan-out was kept:

- child run attribution is easier to inspect
- canonical `summary.json` and `workflow_handoff.json` stay one-child-one-run
- failure attribution is clearer

Code map:

- `scripts/run_task_upload_to_final_export_pipeline.py`
- `feishu_screening_bridge/task_upload_sync.py`

Operational pitfall:

- do not assume the display alias is the real task name
- if Feishu only has `Duet1 / Duet2`, a direct `duet` run only works because the final runner now expands it

## Step 2: Download Task Assets

The upstream runner downloads:

- the sending list workbook
- the screening template workbook
- any task-scoped metadata needed later

The upstream summary stores these under `task_assets`.

Typical artifacts:

- `sending_list_workbook`
- `template_workbook`

Code map:

- `scripts/run_task_upload_to_keep_list_pipeline.py`
- `feishu_screening_bridge/task_upload_sync.py`

## Step 3: Mail Fetch / Mail Sync

The upstream runner has two modes:

1. task-owned mailbox sync
2. reuse an existing shared-mailbox DB

When it fetches mail itself, the core call is:

- `sync_task_upload_mailboxes(...)`

When a shared mailbox DB is passed in, upstream only records the reference and skips real IMAP fetch work.

Code map:

- `scripts/run_task_upload_to_keep_list_pipeline.py`
- `feishu_screening_bridge/task_upload_sync.py`
- standalone wrapper: `scripts/run_shared_mailbox_sync.py`

Standalone shared-mailbox sync example:

```bash
python3 scripts/run_shared_mailbox_sync.py \
  --env-file .env \
  --account-email partnerships@amagency.biz \
  --account-auth-code '***' \
  --folder '其他文件夹/邮件备份' \
  --data-dir data/shared_mailbox \
  --sent-since 2026-04-01
```

Operational pitfall:

- a mailbox DB can have `messages` but still be operationally unusable if relation / thread indexes are missing
- for reply-based rewrite logic, `message_index` and valid `thread_key` resolution matter more than raw message count

## Step 4: Sending List Normalization And Matching

This stage is where the repo decides which creators are actually matched to usable email evidence.

There are currently two major matching flows:

### A. Legacy Enrichment

Used through:

- `email_sync.creator_enrichment.enrich_creator_workbook()`

This path:

- parses the sending list workbook
- normalizes creator identifiers
- extracts creator emails
- rebuilds / uses the relation index
- matches creators to threads
- emits evidence fields such as:
  - `creator_emails`
  - `matched_contact_email`
  - `evidence_thread_key`
  - `last_mail_*`
  - `latest_quote_*`

### B. Brand Keyword Fast Path

Used when `matching_strategy=brand-keyword-fast-path`.

This path goes through:

- brand keyword match
- shared-email resolution
- final review for shared-email groups

It is more appropriate when the shared mailbox is the main source of truth and there are many manager / shared-email threads.

Code map:

- `scripts/run_task_upload_to_keep_list_pipeline.py`
- `email_sync/creator_enrichment.py`
- `email_sync/brand_keyword_match.py`
- `email_sync/shared_email_resolution.py`
- `email_sync/llm_review.py`

## Step 5: Sending List Input Contract

This repo now explicitly supports a modern four-column sending-list contract:

- `博主用户名`
- `主页链接`
- `地区`
- `邮箱`

The parser also keeps legacy multi-link formats alive.

What changed:

- workbook parsing is intentionally low-level now
- it checks `ws.max_row`
- it checks `ws.max_column`
- it reads `ws.cell(r, c).value`
- it inspects hidden columns and merged cells

That change was necessary because real sending lists contained:

- hidden columns
- merged cells
- header offsets
- workbooks where the effective contract existed but higher-level row iteration returned no valid rows

Code map:

- `email_sync/creator_enrichment.py`

Current parser behavior:

- if the four-column contract is detected but no valid creator rows are produced, it fails early with workbook diagnostics
- it no longer waits until downstream to discover that the keep workbook is empty

Operational pitfall:

- if a sending list downloads successfully but enrichment shows `rows=0`, the issue is usually workbook layout or contract recognition, not Feishu download failure

## Step 6: Keep Workbook Generation

The upstream run ends at the keep workbook.

This is the canonical handoff artifact into downstream.

Typical upstream stop points:

- `task_assets`
- `mail_sync`
- `brand_match`
- `shared_resolution`
- `final_review`

Or in legacy enrichment mode:

- `task_assets`
- `mail_sync`
- `enrichment`
- `llm_candidates`
- `llm_review`

Key idea:

- the keep workbook is the stable resume boundary
- once it exists, scrape / visual / export can be rerun without re-downloading Feishu assets or re-fetching mail

## Step 7: Downstream Staging

The downstream runner:

- stages the keep workbook into backend runtime data
- writes its own `task_spec.json`
- writes `summary.json` and `workflow_handoff.json`
- resolves requested platforms

Code map:

- `scripts/run_keep_list_screening_pipeline.py`

Key runtime concept:

- platforms are processed sequentially
- within a platform, creator-level visual work is concurrent

## Step 8: Scrape

The scrape stage starts per platform using `/api/jobs/scrape`.

Important current behavior:

- TikTok scrape payload now defaults to `excludePinnedPosts=true`
- that default can be reversed with `--include-pinned-posts`
- Instagram and YouTube use platform-specific payload shapes

Code map:

- `scripts/run_keep_list_screening_pipeline.py`
- `backend/app.py`

Operational behavior to remember:

- a downstream rerun from an existing keep workbook re-scrapes the staged identifiers for that workbook
- it does not restart from Feishu task upload
- it does not rebuild the keep workbook
- for TikTok / Instagram, this still means a fresh Apify scrape run

Real-world pitfall:

- `missing_profiles_blocked` often means the token pool or actor result could not return all requested profiles
- it does not automatically mean the input workbook is wrong

Current mitigation already in code:

- missing-profile retries are split into smaller batches to avoid one oversized retry batch exhausting the entire Apify budget

## Step 9: Visual Review

The visual stage starts through:

- `POST /api/jobs/visual-review`

The local OpenAI-compatible path is configured in:

- `backend/app.py`

Current provider facts:

- OpenAI visual uses `api_style=responses`
- local `cliproxyapi` style proxies must expose `/v1/responses`
- local OpenAI-compatible provider has an in-flight gate
- default local OpenAI max in-flight is `8`

Important concurrency semantics:

- platforms are sequential
- creators inside one platform are concurrent
- one creator’s recent covers are bundled into a single request

This means the system is not “one image one call.”

It is closer to:

- one creator -> one visual request
- many creators in parallel inside a platform

## Step 10: Template Rules And Manual Review Reminders

The template workbook is not only used for structure.

Its visual contract is now compiled into downstream runtime behavior.

Current implementation:

- `workbook_template_parser/workbook_visual_reuse_compiler.py` compiles the template into a visual rulespec
- `backend/screening.py` carries runtime contract fields such as:
  - `manual_review_items`
  - `compliance_notes`
- `backend/app.py` renders those reminders into the visual prompt

This specifically includes:

- `F.人工判断项/合规提醒`

Example:

- “当封面出现奶瓶等情况，判断达人为哺乳期妈妈时需要人工复核”

Important current safeguard:

- if the rulespec only contains reminders and no real goal / positive features / exclusions, the repo now falls back to the generic visual prompt
- this avoids a reminder-only rulespec accidentally replacing the actual screening criteria

## Step 11: Positioning And Final Export

After scrape and visual review:

- positioning analysis runs
- per-platform final review workbooks are written
- all platforms are merged into:
  - `all_platforms_final_review.xlsx`
  - `all_platforms_final_review_payload.json`

Code map:

- `backend/app.py`
- `backend/final_export_merge.py`

The merged payload now also carries internal fields needed for safer rewrite behavior:

- `creator_emails`
- `matched_contact_email`
- `matched_contact_name`
- `__brand_message_raw_path`
- `__last_mail_raw_path`

Those internal fields are not just for debugging.

They are used later by the shared-mailbox rewrite path to distinguish:

- the real creator reply
- manager / agency replies
- internal teammate reply-all noise

## Step 12: Final Feishu Upload

The final upload goes through:

- `feishu_screening_bridge/bitable_upload.py`

Current write semantics:

- duplicate key scope is `达人对接人 + 达人ID + 平台`
- default final-export payload update mode is now `create_or_mail_only_update`

That means:

- if the row does not exist in Feishu, create it
- if the row already exists, only update:
  - `当前网红报价`
  - `达人最后一次回复邮件时间`
  - `达人回复的最后一封邮件内容`

This avoids re-creating duplicate creator rows while still allowing the latest mail context to refresh.

Important guardrails:

- if the target Feishu table lacks `达人对接人`, upload is blocked
- if the target table contains existing unscoped records without `达人对接人`, upload is blocked
- duplicate groups in payload or existing records are surfaced as failures instead of being silently merged

## Step 13: Shared-Mailbox Post-Sync Rewrite

This is the repair path when the final upload already happened but the team wants to re-apply true reply logic from mailbox threads.

Canonical entry:

```bash
python3 scripts/run_shared_mailbox_post_sync_pipeline.py \
  --shared-mail-db-path "<shared mailbox db>" \
  --existing-final-payload-json "<all_platforms_final_review_payload.json>" \
  --env-file .env \
  --output-root "<new output dir>"
```

What it does:

1. loads the existing final payload
2. uses the mailbox DB to find the real thread and latest valid creator reply
3. removes rows where the creator never replied
4. corrects `达人最后一次回复邮件时间 / 达人回复的最后一封邮件内容`
5. re-resolves the owner fields using Feishu task upload + employee info
6. writes a filtered workbook and filtered payload
7. uploads the rewritten payload back to Feishu

Code map:

- `scripts/run_shared_mailbox_post_sync_pipeline.py`

Current safeguards added in this round:

- rewrite does not accept any random `inbound` as a creator reply
- if the payload carries `matched_contact_email` or `creator_emails`, that explicit creator identity is used first
- legacy payloads only fall back to inferred recipient matching when the creator target can be inferred unambiguously
- `Cc` recipients are included during creator-target inference
- ambiguous multi-recipient legacy threads are marked `creator_identity_unresolved` instead of being falsely kept or falsely removed
- rewrite mode now also supports `FEISHU_SOURCE_URL` fallback for owner inspection

## Recommended Commands

### Run the full chain

```bash
python3 scripts/run_task_upload_to_final_export_pipeline.py --task-name MINISO
```

### Stop at keep-list only

```bash
python3 scripts/run_task_upload_to_keep_list_pipeline.py \
  --task-name MINISO \
  --stop-after llm-review
```

### Rerun downstream from an existing keep workbook

```bash
python3 scripts/run_keep_list_screening_pipeline.py \
  --keep-workbook "<keep workbook>" \
  --template-workbook "<template workbook>" \
  --env-file .env
```

### Probe the visual provider only

```bash
python3 scripts/run_keep_list_screening_pipeline.py \
  --keep-workbook "<keep workbook>" \
  --template-workbook "<template workbook>" \
  --env-file .env \
  --probe-vision-provider-only
```

### Rewrite an existing final payload after shared-mailbox sync

```bash
python3 scripts/run_shared_mailbox_post_sync_pipeline.py \
  --shared-mail-db-path "<email_sync.db>" \
  --existing-final-payload-json "<all_platforms_final_review_payload.json>" \
  --env-file .env \
  --output-root "<rewrite output>"
```

## What To Inspect First When Something Breaks

For canonical runners, inspect in this order:

1. `summary.json`
2. `workflow_handoff.json`
3. the step-specific artifact paths recorded inside `summary.json`

Use the step names recorded by the runner instead of guessing from logs.

For example:

- full chain failure:
  inspect top-level `summary.json`, then child upstream/downstream summaries
- upstream failure:
  inspect `task_assets`, `mail_sync`, `enrichment`, `brand_match`, `shared_resolution`, `final_review`
- downstream failure:
  inspect `vision_preflight`, per-platform `scrape_job`, `visual_job`, `visual_retry`, `positioning_card_analysis`

## Pitfalls Already Seen In Real Runs

### 1. Task alias is not always a real Feishu task name

Example:

- `duet` is an operator alias
- Feishu may actually contain `Duet1` and `Duet2`

Current fix:

- final runner expands aliases into real task rows

### 2. Sending list workbook can be “downloaded fine” and still be unreadable for matching

Real causes already seen:

- hidden columns
- merged cells
- four-column contract not recognized by high-level readers

Current fix:

- low-level workbook inspection in `creator_enrichment.py`

### 3. `OPENAI_BASE_URL` must include `/v1` for the local proxy path

Wrong:

```env
OPENAI_BASE_URL=http://127.0.0.1:8317
```

Right:

```env
OPENAI_BASE_URL=http://127.0.0.1:8317/v1
```

### 4. The local OpenAI visual chain uses Responses API

If the proxy only supports Chat Completions, visual probe can still fail even when the key is present.

### 5. Apify “missing profiles” can be a budget problem, not an input problem

If a run says `missing_profiles_blocked`, inspect token budget and actor returns before blaming the keep workbook.

### 6. Downstream reruns are not the same as full reruns

A downstream rerun:

- does not restart Feishu task upload
- does not restart mailbox matching
- does re-scrape the identifiers in the keep workbook

### 7. Final Feishu upload is owner-scoped

The row key is not only `达人ID + 平台`.

It is:

- `达人对接人 + 达人ID + 平台`

This is intentional and required for multi-owner task splits.

### 8. “Last reply” must be the creator’s reply, not just the latest inbound

Internal reply-all, manager reply, or auto-responder noise should not keep a row alive.

Current fix:

- rewrite reply matching is now creator-address-aware and ambiguity-aware

## Implementation File Map

| responsibility | file |
| --- | --- |
| full-chain orchestration | `scripts/run_task_upload_to_final_export_pipeline.py` |
| upstream orchestration | `scripts/run_task_upload_to_keep_list_pipeline.py` |
| downstream orchestration | `scripts/run_keep_list_screening_pipeline.py` |
| shared mailbox sync | `scripts/run_shared_mailbox_sync.py` |
| shared mailbox post-sync rewrite | `scripts/run_shared_mailbox_post_sync_pipeline.py` |
| Feishu task inspection / mailbox sync | `feishu_screening_bridge/task_upload_sync.py` |
| creator workbook parsing / enrichment | `email_sync/creator_enrichment.py` |
| visual provider routing / scrape / job endpoints | `backend/app.py` |
| visual runtime contract | `backend/screening.py` |
| template visual rulespec compiler | `workbook_template_parser/workbook_visual_reuse_compiler.py` |
| final export merge | `backend/final_export_merge.py` |
| Feishu upload semantics | `feishu_screening_bridge/bitable_upload.py` |

## Execution-Machine Handoff Standard

When code is developed locally but production-like task runs happen on an execution machine, do not hand off the entire repo working tree blindly.

Use the following transfer contract instead.

| category | source | note |
| --- | --- | --- |
| code | `git pull origin main` | do not manually copy repo directories |
| config | `.env` + `.env.local` + shared-mailbox runner credentials | sensitive files, send separately |
| task inputs | sending list + template workbook + keep workbook | the keep workbook is the direct downstream resume boundary |
| caches | `creator_cache.db` + `email_sync.db` + `raw/` + `last_summary.json` | mailbox cache artifacts should come from the same sync run; do not omit `raw/` |
| results | `all_platforms_final_review_payload.json` + `.xlsx` + `feishu_bitable_upload_result.json` | used for inspection, verification, and emergency re-upload |

Practical interpretation:

- code should move through git, not through copied source folders
- task-specific reruns should be driven from the keep workbook, not from re-downloading Feishu assets unless necessary
- mailbox reuse is safest when `email_sync.db`, `raw/`, and `last_summary.json` are transferred together
- `creator_cache.db` is the preferred long-lived scrape / visual reuse layer
- historical `MINISO` / `DUET` run directories do not need to be copied wholesale once the keep workbook, final payload, upload result, and cache DB are preserved

For `MINISO` / `DUET` style handoff, the minimal high-value bundle is usually:

- keep workbook
- template workbook
- `creator_cache.db`
- `email_sync.db`
- `raw/`
- `last_summary.json`
- final payload JSON
- final workbook
- Feishu upload result JSON

Avoid handing off:

- the whole repo directory as a zip
- unrelated historical run roots
- secrets mixed into normal artifact bundles
## Operator Checklist

Before a run:

- confirm the real task name or task-group alias
- confirm Feishu task upload and employee info URLs are present
- confirm mailbox credentials or shared mailbox DB path
- confirm Apify token pool has budget
- confirm visual provider path is reachable
- confirm `OPENAI_BASE_URL` includes `/v1` if using local proxy

After a run:

- inspect `summary.json`
- inspect `workflow_handoff.json`
- inspect final exports or removed rows archive
- confirm whether the run stopped at keep-list, blocked at scrape, failed at visual, or finished with mail-only updates

## Decision Notes From This Round

These behavior changes are now intentional and should be preserved unless there is a deliberate product decision to reverse them:

- task-group aliases fan out serially
- four-column sending-list contract is first-class
- TikTok scrape excludes pinned posts by default
- final upload defaults to `create_or_mail_only_update`
- post-sync rewrite removes outbound-only rows
- `达人对接人` is re-resolved from task upload + employee info during rewrite
- `F.人工判断项/合规提醒` enters the visual prompt
- reminder-only rulespecs fall back to the generic prompt instead of replacing it
