## Stage Placement

`positioning_card_analysis` runs only after visual review and only for creators whose saved visual result is already `Pass`.

It does not replace prescreen or visual review, and it does not redefine who may enter visual review.

## Structured Output Contract

Persisted positioning-card records are machine-readable JSON objects keyed by creator identifier. Each record now includes these fields:

- `platform`
- `username`
- `profile_url`
- `positioning_labels`
- `fit_recommendation`
- `fit_summary`
- `evidence_signals`
- `provider`
- `model`
- `configured_model`
- `requested_model`
- `response_model`
- `effective_model`
- `prompt_source`
- `prompt_selection`
- `reviewed_at`
- `visual_status`
- `visual_reason`
- `visual_reviewed_at`
- `visual_contract_source`
- `usage`
- `cover_count`
- `candidate_cover_count`
- `skipped_cover_count`

Artifacts exposed by the backend:

- `<platform>_positioning_card_results.json`
- download endpoint: `/api/download/<platform>/positioning-card-json`
- download endpoint: `/api/download/<platform>/positioning-card-review`

## Summary Keys

Runner summary keys:

- `platforms.<platform>.positioning_card_analysis`
- `platforms.<platform>.artifact_status.saved_positioning_card_artifacts_available`
- `platforms.<platform>.artifact_status.positioning_card_results_path`
- `platforms.<platform>.artifact_status.positioning_card_result_count`
- `platforms.<platform>.exports.positioning_card_review`
- `platforms.<platform>.exports.positioning_card_json`

Wrapper summary keys:

- `steps.downstream.positioning_card_analysis`
- `steps.downstream.positioning_artifacts`
- `artifacts.positioning_artifacts`

CLI skip control:

- `scripts/run_keep_list_screening_pipeline.py --skip-positioning-card-analysis`
- `scripts/run_task_upload_to_final_export_pipeline.py --skip-positioning-card-analysis`

## Non-Blocking Behavior

First-version positioning-card analysis is additive by default.

If the stage is skipped, has zero eligible creators, or records a non-blocking stage failure, the downstream runner still continues to artifact collection and final export. The stage outcome is written into summary payloads instead of promoting the whole run to `failed`.

## Verification Commands

Targeted unittest coverage:

```bash
PYTHONPYCACHEPREFIX=/tmp/pycache backend/.venv/bin/python -m unittest \
  tests.test_visual_provider_diagnostics \
  tests.test_keep_list_screening_pipeline \
  tests.test_task_upload_to_final_export_pipeline -v
```

Bounded repo-local summary write, no live provider required:

```bash
backend/.venv/bin/python scripts/run_keep_list_screening_pipeline.py \
  --keep-workbook "exports/测试达人库_MINISO_匹配结果_高置信_按我们去重_llm_reviewed_keep.xlsx" \
  --template-workbook "downloads/task_upload_attachments/recveXGV2i3BS0/需求上传（excel 格式）/miniso-星战红人筛号需求模板(1).xlsx" \
  --output-root "temp/phase24_positioning_card_analysis" \
  --summary-json "temp/phase24_positioning_card_analysis/summary.json" \
  --platform instagram \
  --skip-scrape
```
