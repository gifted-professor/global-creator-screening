# Quick Task 260331-gn2 Summary

## Task

验证 `CLIProxyAPI` 本地 `gpt-5.4` 文字与图片链路，判断是否可作为首选 GPT provider 接入现有视觉主链。

## What I Did

- 真实调用 `http://127.0.0.1:8317/v1/chat/completions`
- 真实调用 `http://127.0.0.1:8317/v1/responses`
- 检查当前仓库 `openai` vision provider 的 `base_url` / `api_style` / request body 生成逻辑

## Results

### 1. Text path passed

请求：

```bash
curl http://127.0.0.1:8317/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer cliproxyapi-local" \
  -d '{"model":"gpt-5.4","messages":[{"role":"user","content":"Reply with OK only."}]}'
```

结果：

- HTTP 成功返回
- `model = gpt-5.4`
- assistant content = `OK`

### 2. Image path passed

请求：

```bash
curl http://127.0.0.1:8317/v1/responses \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer cliproxyapi-local" \
  -d '{"model":"gpt-5.4","input":[{"role":"user","content":[{"type":"input_text","text":"Describe this image in one short sentence."},{"type":"input_image","image_url":"https://upload.wikimedia.org/wikipedia/commons/3/3a/Cat03.jpg"}]}]}'
```

结果：

- HTTP 成功返回
- `model = gpt-5.4`
- 图片理解结果正常返回：橘猫近景描述

### 3. Existing repo provider path is wire-compatible

在 [backend/app.py](/Users/a1234/Desktop/Coding/chuhaihai/backend/app.py) 中：

- `openai` provider 默认 `api_style = responses`
- probe 请求会打到 `{base_url}/responses`
- body 结构就是：
  - `model`
  - `input: [{role: "user", content: [{type: "input_text", text: ...}]}]`

这与 `CLIProxyAPI` 本地接口的成功样例是兼容的。

## Recommended Config

如果后面要把它接成首选 GPT provider，建议先从下面这组环境变量开始：

```env
OPENAI_BASE_URL=http://127.0.0.1:8317/v1
OPENAI_API_KEY=cliproxyapi-local
OPENAI_VISION_MODEL=gpt-5.4
```

## Limitations

- 这次没有直接通过仓库内 `backend.app` 运行完整 provider probe，因为当前仓库路径下缺少 `backend/.venv/bin/python`，而系统 `python3` 又缺 `pandas`，无法原样 import `backend.app`。
- 但原始 HTTP 验证已通过，且代码路径核对显示现有 `openai` provider 请求面与该接口兼容，因此当前结论是“纯配置接入大概率可行”。

## Conclusion

`CLIProxyAPI -> gpt-5.4` 这条本地 HTTP 链路已跑通，且与当前仓库 `openai` 视觉 provider 的 `responses` 接口形状兼容。后续如果要落成首选 GPT provider，优先尝试纯 env 接入，而不是先写新适配器。
