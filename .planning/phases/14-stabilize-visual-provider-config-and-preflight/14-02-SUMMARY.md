# Phase 14-02 Summary

## Goal

把 backend 的视觉 provider 诊断落到 keep-list / smoke summary 和 operator 文档里，并留下一份新的 bounded diagnostic artifact。

## Executed Work

- 更新 `scripts/run_keep_list_screening_pipeline.py`
  - 顶层 summary 新增 `vision_preflight`
  - 每个平台 summary 新增 `vision_preflight`
  - 每个平台 summary 新增 `visual_gate`
  - 当 visual 因 provider 预检不可运行而跳过时，summary 会直接保留：
    - `error_code`
    - `reason`
    - `vision_preflight`
- 更新 `scripts/run_screening_smoke.py`
  - 顶层和平台级 summary 对齐 backend `vision_preflight`
  - `visual_gate` 也会记录 preflight 状态与 runnable provider 列表
- 更新 `README.md`
  - 明确 backend 视觉配置来源是进程环境 + `.env.local`
  - 明确 keep-list runner 的 `--env-file` 不会直接改 backend visual provider
  - 补了 bounded visual diagnostic 命令
  - 标出需要看的 summary 字段
- 新增测试：
  - `tests/test_keep_list_screening_pipeline.py`
- 留下一份新的真实 bounded diagnostic artifact：
  - `temp/keep_list_visual_diagnostic_phase14/summary.json`
  - `temp/keep_list_visual_diagnostic_phase14/staging_summary.json`

## Validation

- `python3 -m unittest tests.test_keep_list_screening_pipeline -v`
- `python3 -m unittest tests.test_main_cli -v`
- `rg -n "OPENAI_VISION_MODEL|VISION_QUAN2GO|VISION_LEMONAPI|env.local|run_keep_list_screening_pipeline|visual" README.md`
- Real bounded diagnostic run:
  - `backend/.venv/bin/python scripts/run_keep_list_screening_pipeline.py --keep-workbook "exports/测试达人库_MINISO_匹配结果_高置信_按我们去重_llm_reviewed_keep.xlsx" --template-workbook "downloads/task_upload_attachments/recveXGV2i3BS0/需求上传（excel 格式）/miniso-星战红人筛号需求模板(1).xlsx" --output-root "temp/keep_list_visual_diagnostic_phase14" --summary-json "temp/keep_list_visual_diagnostic_phase14/summary.json" --platform instagram --max-identifiers-per-platform 1 --skip-scrape`

## Real Output

- Diagnostic summary:
  - `temp/keep_list_visual_diagnostic_phase14/summary.json`
- Diagnostic staging summary:
  - `temp/keep_list_visual_diagnostic_phase14/staging_summary.json`

## Notes

- 这次 bounded artifact 故意只做到 `staging_only`，目的是证明新 summary 已经能把 backend 视觉诊断完整暴露出来，而不是重复打一遍真实 scrape。
- 新 artifact 已经能直接回答：
  - backend 看到哪些 provider
  - runnable provider 是谁
  - `api_key / base_url / model` 来自 `.env.local` 还是默认值
  - visual 为什么会执行、跳过，或者预期失败
- 当前本地真实诊断结果显示：
  - runnable provider 只有 `openai`
  - `OPENAI_API_KEY / OPENAI_BASE_URL / OPENAI_VISION_MODEL` 都来自 `.env.local`
  - `quan2go` 和 `lemonapi` 因缺少 key 被标记为 `missing_config`

## Next

- 进入 Phase 15，修正当前 active provider 的真实鉴权可用性
- 用真实 keep-list 跑一轮 non-error 的 bounded visual review，而不是只做诊断 artifact
