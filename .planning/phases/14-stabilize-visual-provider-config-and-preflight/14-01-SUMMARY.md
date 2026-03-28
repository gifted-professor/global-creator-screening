# Phase 14-01 Summary

## Goal

为 backend 增加视觉 provider snapshot / preflight 契约，并把早失败诊断接进 health 与 visual-review start 路径。

## Executed Work

- 在 `backend/app.py` 增加 backend-owned 视觉 provider 诊断契约：
  - `build_vision_provider_snapshot(...)`
  - `build_vision_provider_snapshots()`
  - `build_vision_preflight()`
  - `build_vision_preflight_error_payload()`
- 扩展 provider 配置元数据，显式记录：
  - `env_key`
  - `base_url_env_key`
  - `default_base_url`
  - `model_env_key`
  - `api_style`
- 让视觉 provider 诊断能说明：
  - API key 是否存在
  - API key 来自 `.env.local` 还是进程环境
  - backend 将使用的 `base_url / model / api_style`
  - provider 是否 runnable
  - provider 为什么不可运行
- 保留现有 visual-review 执行主链，但把可运行 provider 的判断从“只有 key 就算可用”收紧为：
  - key 存在
  - `base_url` 可解析
  - `api_style` 受支持
  - `model` 可解析
- 更新 `/api/health`，现在会返回：
  - `checks.vision`
  - `checks.vision_providers`
  - `checks.vision_preflight`
- 更新 `POST /api/jobs/visual-review` 和 `perform_visual_review(...)` 的早失败语义：
  - 无 provider 时不再只给模糊报错
  - 返回结构化 `vision_preflight`
- 新增测试：
  - `tests/test_visual_provider_diagnostics.py`
- 更新 runtime validation：
  - `scripts/test_runtime_validation.py`

## Validation

- `backend/.venv/bin/python -m unittest tests.test_visual_provider_diagnostics -v`
- `backend/.venv/bin/python scripts/test_runtime_validation.py`

## Notes

- 这一步只解决“backend 到底认出了什么视觉配置、什么时候应该早失败”，不承诺 provider 真实鉴权一定成功。
- runtime validation 现在会打印结构化 `vision_preflight`，可直接看到：
  - `.env.local` 是否存在
  - 哪些 key 是 backend 启动时从 `.env.local` 注入的
  - 当前 runnable provider 列表
- 当前本地真实环境下，diagnostic contract 已能明确指出：
  - `openai` 来源于 `.env.local`
  - `quan2go` / `lemonapi` 当前缺少 key

## Next

- 把同一份 backend `vision_preflight` 直接写进 keep-list runner 和 smoke summary
- 更新 README，让 operator 明确知道 backend 视觉配置不吃 runner `--env-file`
