## Outcome

Phase 24-02 is complete.

The new positioning-card stage is now operator-visible from both the downstream runner summary and the top-level `task upload -> final export` wrapper summary. Contract and verification notes were also written into repo-local docs.

## Changes

- Extended `scripts/run_screening_smoke.py` export collection to download positioning-card JSON/XLSX artifacts when available
- Extended `scripts/run_task_upload_to_final_export_pipeline.py` to surface:
  - `steps.downstream.positioning_card_analysis`
  - `steps.downstream.positioning_artifacts`
  - `artifacts.positioning_artifacts`
- Kept wrapper delivery semantics non-blocking for skipped or stage-failed positioning analysis
- Documented the contract in `24-POSITIONING-CARD-CONTRACT.md`
- Updated `README.md` with stage placement, skip flag, and evidence locations
- Added wrapper regression coverage in `tests/test_task_upload_to_final_export_pipeline.py`

## Verification

```bash
PYTHONPYCACHEPREFIX=/tmp/pycache backend/.venv/bin/python -m unittest \
  tests.test_visual_provider_diagnostics \
  tests.test_keep_list_screening_pipeline \
  tests.test_task_upload_to_final_export_pipeline -v
```
