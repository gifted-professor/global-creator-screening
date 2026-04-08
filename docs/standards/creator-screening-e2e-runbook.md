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

Current note:

- the official upstream matching logic is now the mail funnel described in this document
- operationally, that funnel is: brand filter -> sending-list match -> regex match -> LLM tail
- the keep workbook remains the canonical downstream handoff artifact
- for the dedicated mail-only `AI回信管理` rewrite path introduced on `2026-04-07`, the default is now:
  - brand filter -> regex match -> LLM tail
  - no sending-list match unless explicitly re-enabled
  - high-confidence keep rows write back directly
  - manual-tail rows also write back by default as:
    - `达人ID = <任务名><月>/<日>转人工<n>`
    - `平台 = 转人工`

## Canonical Entry Points

| stage | canonical script | when to use it |
| --- | --- | --- |
| full chain | `scripts/run_task_upload_to_final_export_pipeline.py` | run task upload all the way to final export and Feishu upload |
| upstream only | `scripts/run_task_upload_to_keep_list_pipeline.py` | stop at keep-list generation and inspect email matching quality |
| downstream only | `scripts/run_keep_list_screening_pipeline.py` | rerun scrape / visual / export from an existing keep workbook |
| shared mailbox sync only | `scripts/run_shared_mailbox_sync.py` | only refresh the shared mailbox cache / `email_sync.db` |
| post-sync rewrite only | `scripts/run_shared_mailbox_post_sync_pipeline.py` | re-filter an existing final payload using real reply threads, then rewrite Feishu |

## Execution Machine Quick Guide

If you are operating on the execution machine, read this section first.

### What The Official Upstream Chain Is

The official upstream mail-screening logic is now:

1. brand / task-name filter
2. sending-list match
3. regex match
4. LLM tail match
5. keep workbook

Do not interpret the upstream as:

- “scan all local mail first and then see what happens”
- or “sending-list and regex are optional side paths”

They are now the formal funnel.

### What You Should Normally Run

If you want the full chain:

```bash
python3 scripts/run_task_upload_to_final_export_pipeline.py --task-name <TASK_NAME>
```

If you only want to inspect upstream mail matching quality:

```bash
python3 scripts/run_task_upload_to_keep_list_pipeline.py \
  --task-name <TASK_NAME> \
  --stop-after keep-list
```

If you already have a keep workbook and only want scrape / visual / final export:

```bash
python3 scripts/run_keep_list_screening_pipeline.py \
  --keep-workbook "<keep workbook>" \
  --template-workbook "<template workbook>" \
  --env-file .env
```

### How Mail Time Window Works Now

The execution machine should no longer assume upstream will scan the whole local mailbox history by default.

Current `sent_since` priority is:

1. explicit CLI `--sent-since`
2. Feishu task-upload field:
   - `任务开始时间`
   - `开始时间`
   - `任务开始日期`
   - `开始日期`
3. fallback default today-only window

Practical meaning:

- if you do not pass `--sent-since`, the runner should use the task’s start time from Feishu
- if you need repair / backfill / historical rerun behavior, pass `--sent-since` explicitly

### What To Expect From Upstream Output

The keep workbook is still the normal downstream boundary artifact.

But conceptually, that keep workbook now comes from this funnel:

- brand-correct thread
- sending-list exact evidence if available
- regex extraction from `latest_external_full_body`
- LLM only on the unresolved tail

### What Goes To Apify

Only these rows should auto-enter downstream scrape:

- sending-list resolved
- regex resolved
- `llm` resolved with `high` confidence

These should not auto-enter downstream scrape:

- `llm medium`
- `llm low`
- `uncertain`
- auto-replies / OOO / ticket acknowledgements

### Field Conventions You Should Assume

Current final field conventions are:

- mail content field: `full body`
- views field: `Median Views (K)`
- reply times should be interpreted in `Asia/Shanghai`

### Common Misunderstandings To Avoid

- The formal upstream chain is not “legacy enrichment first.”
- `brand-keyword-fast-path` is the intended default.
- A normal keep workbook is the canonical downstream boundary.
- Do not assume missing CLI `--sent-since` means “scan all local mail.”
- Do not auto-trust `llm` rows unless they are high confidence.

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

### Mail Window Rule

The upstream runner no longer treats mailbox search as an unbounded local-history scan.

Current `sent_since` precedence is:

1. explicit CLI `--sent-since`
2. Feishu task-upload row `任务开始时间 / 开始时间 / 任务开始日期 / 开始日期`
3. fallback default today-only behavior

This means:

- if the operator passes `--sent-since`, that always wins
- if the operator does not pass it, the runner now tries to use the task’s start date from the Feishu task-upload row
- only when the task row does not contain a usable start-time field does the old default window apply

Operational intent:

- do not sweep the entire local mailbox history for a fresh task by default
- use the task’s own start time as the natural mailbox lower bound
- keep manual override available for repair or backfill runs

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

## Step 4: Upstream Mail Funnel

This stage is where the repo decides which creators are actually matched to usable email evidence.

Current official upstream chain is:

1. brand keyword / task-name match
2. sending-list match
3. regex match
4. LLM tail match
5. keep workbook

This is now the intended formal logic for shared-mailbox brands such as `SKG`, `MINISO`, and `DUET`.

### Official Ordering

#### 1. Brand keyword / task-name filter

Only keep emails or threads that belong to the current task / brand.

This is the first and mandatory filter.

Operational rule:

- do not match creator identity before the brand / task-name filter
- do not allow cross-brand mailbox noise to survive past this stage

#### 2. Sending-list match

After brand filtering, try the strongest sending-list evidence first.

Current strong evidence types are:

- exact sender-email hit
- exact creator handle hit when the sending list already carries that handle
- explicit creator profile link when available

The point of this stage is precision:

- if a creator is already cleanly explainable by the task’s own sending list, resolve here
- do not waste regex or LLM on rows that are already exact sending-list hits

#### 3. Regex match

For emails that are brand-correct but not resolved by the sending list, use thread/body extraction rules.

Primary source text:

- `latest_external_full_body`

Typical patterns:

- `Hi creator_id`
- `Hi @ creator_id`
- `Hi *creator_id*`
- `Hello creator_id`
- `Hallo creator_id`
- quoted forms such as `> Hi creator_id`
- explicit social labels such as `TikTok: creator_id`
- profile URLs in signature blocks

This stage is especially important when:

- replies come from managers or agencies
- the reply email address no longer matches the original sending-list mailbox
- the creator handle survives in the quoted outreach thread instead of the sender email

#### 4. LLM tail match

Only unresolved tail rows should go to the model.

The model’s job is:

- read the full thread context
- infer the most likely creator handle
- return evidence
- return confidence

Operational rule:

- the model is a tail resolver, not the first matching layer

### Production Default

On current `main`, the production default for task-upload-driven keep-list runs is still:

- `matching_strategy=brand-keyword-fast-path`

But operationally, the intended upstream identification logic should now be understood as:

1. task assets
2. mail sync
3. brand filtering
4. sending-list matching
5. regex matching
6. LLM tail matching
7. keep workbook

Code map:

- `scripts/run_task_upload_to_keep_list_pipeline.py`
- `email_sync/brand_keyword_match.py`
- `email_sync/shared_email_resolution.py`
- `email_sync/llm_review.py`
- `scripts/prepare_screening_inputs.py`

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

### Keep Workbook vs Mail-Thread Fields

The canonical downstream resume boundary is still the keep workbook.

Typical examples:

- `*_shared_email_resolution_final_keep.xlsx`
- `*_llm_reviewed_keep.xlsx`

At the same time, the repo now also uses mail-thread-style fields to explain how a creator ID was resolved.

Typical fields include:

- `final_id_final` or `final_id`
- `latest_external_full_body`
- `latest_external_clean_body`
- `resolution_stage_final`
- `resolution_confidence_final`

Operational interpretation:

- the keep workbook remains the canonical handoff artifact
- but upstream mail matching should now be thought of as a funnel that produces a creator-ID decision plus evidence fields
- execution machines should reason about keep generation using the funnel order above, not as a flat one-shot workbook enrichment step

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

Current field behavior:

- the mail-content field written downstream now uses `full body`
- it prefers full thread-aware mail body fields over short snippets
- if needed it can fall back to raw `.eml` parsing

Current metric behavior:

- the final views field is now `Median Views (K)`
- median is preferred over average when both are available
- legacy average fields only survive as compatibility fallback

## Step 12: Final Feishu Upload

The final upload goes through:

- `feishu_screening_bridge/bitable_upload.py`

Current write semantics:

- duplicate key scope is `达人对接人 + 达人ID + 平台`
- default final-export payload update mode is now `create_or_mail_only_update`

That means:

- if the row does not exist in Feishu, create it
- if the row already exists, only update:
  - `主页链接`
  - `当前网红报价`
  - `达人最后一次回复邮件时间`
  - `full body`

This avoids re-creating duplicate creator rows while still allowing the latest mail context to refresh.

Mail-only homepage URL generation now follows the same canonical rule:

- `TikTok -> https://www.tiktok.com/@<达人ID>`
- `Instagram -> https://www.instagram.com/<达人ID>`
- `YouTube -> https://www.youtube.com/@<达人ID>`
- empty platform or `转人工` -> blank homepage URL

Mail-only upload can also attach the raw `.eml` file for each matched reply row:

- when the payload includes a resolvable local `raw_path`, the uploader forwards it through `__feishu_attachment_local_paths`
- if the target Feishu table has an attachment field (for example `eml`), the raw mail is uploaded alongside the row
- if the target table has no attachment field, the upload continues without error and simply skips the `.eml` attachment

Important guardrails:

- if the target Feishu table lacks `达人对接人`, upload is blocked
- if the target table contains existing unscoped records without `达人对接人`, upload is blocked
- duplicate groups in payload or existing records are surfaced as failures instead of being silently merged

Current compatibility note:

- the code still accepts older payload aliases that map to the old Chinese mail-content field name
- but the current target Feishu schema should use `full body`
- the current target views field should use `Median Views (K)`

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

## MINISO Direct Mail Path

For the concrete `2026-04-07 / MINISO / AI回信管理` direct-write path that was run end-to-end in this repo, see:

- `docs/standards/miniso-ai-reply-mail-only-path-2026-04-07.md`

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

## Mail-Thread to Final-ID Funnel

This is the formal mailbox-thread identification contract for shared-mailbox brands such as `SKG`, `MINISO`, and `DUET`.

The purpose of this stage is only to produce a creator identity decision from mailbox evidence.

It does not run scrape, visual review, or final Feishu upload.

Important clarification:

- this funnel should now be treated as the official upstream matching logic
- the keep workbook is still the downstream boundary artifact
- but conceptually the keep workbook is the result of this funnel, not a separate competing chain

### Stage Goal

For each brand-filtered reply thread, produce:

- `final_id`
- `resolution_stage`
- `evidence`
- `latest_external_clean_body`
- `latest_external_full_body`
- `raw_path`

### Input Scope

The funnel should operate on:

- a bounded sync window such as `2026-04-01` through `2026-04-03`
- brand-keyword-filtered threads
- thread-level records, not single isolated emails

Use the latest external message as the anchor message, but keep the full quoted thread text available for evidence extraction.

### Body Fields

Use two body representations:

- `latest_external_clean_body`
  the newest external reply body only, trimmed as much as safely possible
- `latest_external_full_body`
  the newest external reply plus quoted history that can expose the original outreach greeting such as `Hi @ creator_id`

`latest_external_full_body` is the primary text for regex extraction.

`latest_external_clean_body` is the operator-facing inspection field.

### Teammate Filter

Before regex or LLM creator extraction, filter internal teammate names from candidate handles.

Current temporary teammate blocklist:

- `William`
- `Eden`
- `Rhea`
- `Elin`
- `Yvette`
- `Astrid`
- `Lilith`
- `Ruby`

Common spelling variants should also be filtered when obvious, for example `Lillith`.

### Resolution Funnel

Run the following stages in order.

#### `pass0_sending_list`

Before regex extraction, try the strongest possible sending-list join on the already brand-filtered thread set.

This stage should prioritize the sending list that belongs to the current task or brand.

Behavior rules:

- run only after the brand keyword / task-name filter
- respect the same `sent_since` window used for mailbox sync
- use exact sender-email equality first
- allow exact creator-handle or creator-link hits when the sending list already provides them
- do not mix brands that happen to share the same mailbox database
- if exactly one creator row is implied by the sending list evidence, resolve the thread here

This stage is the highest-confidence creator identity layer after the brand filter.

#### `regex_pass1`

Greeting-based extraction from `latest_external_full_body`.

Match patterns such as:

- `Hi creator_id`
- `Hi @creator_id`
- `Hi @ creator_id`
- `Hello creator_id`
- `Hallo creator_id`
- quoted forms such as `> Hi creator_id`

If the stage yields exactly one non-teammate candidate handle, resolve the thread here.

#### `regex_pass2`

If pass 1 does not uniquely resolve, look for stronger explicit creator signals in the same thread text:

- `TikTok: creator_id`
- `Instagram: creator_id`
- social profile URLs
- explicit signature handles
- obvious `@handle` forms that are not teammate names

If the stage yields exactly one strong candidate handle, resolve the thread here.

#### `llm`

Only unresolved tail threads should go to the model.

The LLM should read the full thread context and output:

- the most likely creator handle
- a short evidence snippet
- a confidence-aware justification

The model should not be used on the whole keyword-hit population by default.

### Operational Shape

The intended funnel is:

1. sync mailbox window
2. filter by brand keyword
3. collapse to reply threads
4. run `pass0_sending_list`
5. run `regex_pass1`
6. run `regex_pass2`
7. send only the unresolved tail to the LLM
8. emit a fixed `final_id` table for downstream use

### Apify Gate

When the upstream mail funnel hands off into downstream screening, only the following rows should be allowed into Apify-ready staging:

- sending-list exact hits
- regex-resolved hits
- `llm` hits with `high` confidence only

The following rows should not auto-enter Apify:

- `llm` medium / low / unknown confidence
- unresolved rows
- obvious auto-replies / OOO / ticket acknowledgements

This behavior is now implemented in the current mail-funnel preparation path.

### Current SKG Reference Snapshot

Using `latest_external_full_body` plus teammate filtering, the first full-body regex experiment produced:

- `regex_pass1 = 252`
- `regex_pass2 = 6`
- `llm = 37`
- `unresolved = 0`

This confirms that most thread identification should be solved before the model layer.

When `pass0_sending_list` is inserted ahead of regex on the same `SKG` thread set, the reference experiment produced:

- `pass0_sending_list = 66`
- `regex_pass1 = 162`
- `regex_pass2 = 4`
- `llm = 33`

Important interpretation:

- `pass0_sending_list` only reduced the final LLM tail by `4` rows on that sample
- but those rows were upgraded from model inference to exact sending-list evidence
- so this stage is worth keeping for precision, even though most of its hits overlap with later regex stages

### LLM Concurrency

Thread-level creator-ID extraction has been stable at `12` concurrent workers in local benchmarking.

Use `12` as the default concurrency target for the unresolved LLM tail unless the provider or environment suggests otherwise.

## Platform Fallback Design

After the mailbox funnel emits `final_id`, downstream platform probing should follow this order:

1. TikTok
2. Instagram
3. YouTube
4. manual review

Behavior contract:

- first build `TikTok`, `Instagram`, and `YouTube` profile URLs from `final_id`
- try TikTok first
- if TikTok scrape succeeds and yields usable review input, stop there
- only if TikTok fails, try Instagram
- only if Instagram also fails, try YouTube
- if all three fail, mark the row as `转人工`

Recommended downstream fields:

- `resolved_platform`
- `resolved_profile_url`
- `platform_attempt_order`
- `fallback_reason`

This platform fallback happens after the mailbox identification funnel is complete.

It should not be mixed into the mail-thread resolution stages above.
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
- task-upload-driven mailbox runs now default `sent_since` from the Feishu task start time when no explicit CLI override is provided
- `full body` is the canonical final mail-content field for export and Feishu upload
- `Median Views (K)` is the canonical final views field for export and Feishu upload
- the formal keep workbook and the experimental mail-thread funnel workbook are different contracts and must not be treated as interchangeable
