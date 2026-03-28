# Phase 15-02 Summary

## Goal

基于 15-01 的显式 provider 选择和 live probe，对真实 keep-list 跑出一轮 non-error 的 bounded visual review，并把 operator path 文档化。

## Executed Work

- 使用 `scripts/run_keep_list_screening_pipeline.py` 对真实 MINISO keep-list 执行一轮 bounded live validation：
  - platform: `instagram`
  - max identifiers: `1`
  - provider: `openai`
- 真实 run 成功覆盖以下主链：
  - `staging`
  - `scrape`
  - `prescreen`
  - `visual-review`
  - `export`
- 更新 `README.md`
  - 补充 `--vision-provider`
  - 补充 `--probe-vision-provider-only`
  - 补充 probe-only 命令
  - 补充真实 bounded visual validation 命令和产物路径

## Validation

- Real bounded validation:
  - `backend/.venv/bin/python scripts/run_keep_list_screening_pipeline.py --keep-workbook "exports/测试达人库_MINISO_匹配结果_高置信_按我们去重_llm_reviewed_keep.xlsx" --template-workbook "downloads/task_upload_attachments/recveXGV2i3BS0/需求上传（excel 格式）/miniso-星战红人筛号需求模板(1).xlsx" --output-root "temp/phase15_bounded_openai_live" --summary-json "temp/phase15_bounded_openai_live/summary.json" --platform instagram --max-identifiers-per-platform 1 --vision-provider openai`
- Summary/doc verification:
  - `rg -n "vision-provider|probe-vision-provider-only|phase15_bounded_openai_live|phase15_probe_only_openai" README.md`

## Real Output

- Final summary:
  - `temp/phase15_bounded_openai_live/summary.json`
- Visual results:
  - `temp/phase15_bounded_openai_live/data/instagram/instagram_visual_results.json`
- Final export:
  - `temp/phase15_bounded_openai_live/exports/instagram/instagram_final_review.xlsx`

## Notes

- 这轮真实结果已经满足 Phase 15 的核心目标：
  - `vision_probe.success = true`
  - `scrape` 成功返回 `1/1`
  - `prescreen_pass_count = 1`
  - `visual_job.result.success = true`
  - visual summary 为：
    - `pass = 1`
    - `reject = 0`
    - `error = 0`
- 账号 `_sophiesilva_` 的真实视觉结果为：
  - `decision = Pass`
  - `provider = openai`
- 这证明当前 active `openai` 视觉链在真实 bounded run 下已经不再返回：
  - `auth_not_found`
  - `visual_status = Error`

## Next

- 当前 milestone 的 phase 已全部完成，下一步应执行 milestone closeout
- 如果业务上还要继续增强，可以再开 follow-up：
  - 扩大到更多账号或更多平台
  - 补 `quan2go` / `lemonapi` 的 live proof
