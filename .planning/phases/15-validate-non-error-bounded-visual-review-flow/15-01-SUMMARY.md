# Phase 15-01 Summary

## Goal

为真实 bounded visual run 增加 deterministic provider selection 和轻量 live probe，避免再被静态 provider 顺序或模糊鉴权失败拖住。

## Executed Work

- 更新 `backend/app.py`
  - 新增显式 provider 请求解析：
    - `resolve_vision_provider_request(...)`
  - 扩展 `build_vision_preflight(...)`
    - 支持传入指定 provider
    - 回填 `requested_provider`、`requested_provider_source`、`requested_provider_declared`、`requested_provider_runnable`
  - 新增 provider probe 能力：
    - `build_vision_provider_probe_request(...)`
    - `probe_vision_provider(...)`
  - 新增 API：
    - `POST /api/vision/providers/probe`
  - 更新 visual-review 启动和执行路径
    - `POST /api/jobs/visual-review` 现在支持 payload 指定 `provider`
    - 真实执行时会按请求 provider 做 preflight 和 provider 选择
- 更新 `scripts/run_keep_list_screening_pipeline.py`
  - 新增 `--vision-provider`
  - 新增 `--probe-vision-provider-only`
  - summary 新增：
    - `requested_vision_provider`
    - `probe_vision_provider_only`
    - `vision_probe`
  - 如果 probe 失败，runner 会以 `vision_probe_failed` 早退出
  - 如果只做 probe，会以 `vision_probe_only` 结束并写 summary
- 更新测试：
  - `tests/test_visual_provider_diagnostics.py`
  - `tests/test_keep_list_screening_pipeline.py`
  - `tests/test_main_cli.py`

## Validation

- `python3 -m unittest tests.test_keep_list_screening_pipeline tests.test_main_cli -v`
- `backend/.venv/bin/python -m unittest tests.test_visual_provider_diagnostics -v`
- Real live probe:
  - `backend/.venv/bin/python scripts/run_keep_list_screening_pipeline.py --keep-workbook "exports/测试达人库_MINISO_匹配结果_高置信_按我们去重_llm_reviewed_keep.xlsx" --template-workbook "downloads/task_upload_attachments/recveXGV2i3BS0/需求上传（excel 格式）/miniso-星战红人筛号需求模板(1).xlsx" --output-root "temp/phase15_probe_only_openai" --summary-json "temp/phase15_probe_only_openai/summary.json" --platform instagram --vision-provider openai --probe-vision-provider-only`

## Real Output

- Probe artifact:
  - `temp/phase15_probe_only_openai/summary.json`

## Notes

- 这一步已经证明当前 `openai` 视觉配置不是“backend 没读到 key”。
- 真实 live probe 返回：
  - `provider = openai`
  - `success = true`
  - `base_url = https://9985678.xyz/v1`
  - `model = gpt-5.4`
- 所以当前问题不应再笼统表述为“apikey 缺失”；更准确的说法是：
  - 之前的 `auth_not_found` 更像是旧 run 的 provider 路径不稳定或特定请求路径问题
  - 至少在当前 Phase 15 这轮配置下，轻量 probe 已能成功

## Next

- 用显式 `--vision-provider openai` 跑真实 bounded visual validation
- 把成功命令、产物路径和 operator 口径写回 README 与 planning 收尾文档
