## Outcome

Phase 24-01 is complete.

The backend now owns a repo-local `positioning_card_analysis` contract that runs only on creators who already passed visual review. Positioning results are persisted per platform, exposed through artifact status, and available through dedicated JSON/XLSX download endpoints.

## Changes

- Added positioning-card prompt construction, parsing, normalized record schema, and persisted artifact helpers in `backend/app.py` and `backend/screening.py`
- Added `/api/jobs/positioning-card-analysis`
- Added backend artifact-status fields for positioning-card paths and counts
- Added downstream runner stage `positioning_card_analysis` with explicit `completed` / `skipped` / `failed` states
- Added non-blocking runner behavior and skip flag `--skip-positioning-card-analysis`
- Added regression coverage in `tests/test_visual_provider_diagnostics.py` and `tests/test_keep_list_screening_pipeline.py`

## Verification

```bash
PYTHONPYCACHEPREFIX=/tmp/pycache backend/.venv/bin/python -m unittest \
  tests.test_visual_provider_diagnostics \
  tests.test_keep_list_screening_pipeline -v
```
