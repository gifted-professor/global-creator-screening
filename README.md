# global-creator-screening

这个仓库现在已经把几个原来分散的 sibling 工具逐步收进来了：

- `feishu_screening_bridge/`：飞书任务、模板、bitable 导出和任务驱动邮件桥接
- `email_sync/`：IMAP 抓信、正文解析、线程索引、达人库匹配和报价抽取
- `workbook_template_parser/`：需求模板解析
- `backend/`：`筛号` 后端主链路

## 当前主链路

1. 飞书任务上传 -> 员工邮箱 / IMAP 码
2. 下载需求模板 -> 模板解析
3. 按任务抓取邮箱文件夹邮件
4. 建线程、匹配达人库、抽报价
5. 把模板 rulespec + 达人匹配名单写入筛号当前输入状态
6. Apify 抓取 -> 预筛 -> 视觉复核 -> 导出

## 关键目录

- `backend/app.py`：筛号 Flask API 入口
- `backend/screening.py`：三平台预筛和封面候选逻辑
- `backend/rules.py`：SOP -> RuleSpec 编译
- `config/`：当前筛号规则配置和编译结果
- `scripts/prepare_screening_inputs.py`：把模板解析结果和达人匹配名单写入筛号当前输入状态
- `scripts/`：样本链路、输入准备和 runtime validation
- `data/`：邮件、筛号和本地运行产物
- `temp/`：样本、benchmark 和临时导出

## 本地运行

筛号后端依赖在 [backend/requirements.txt](/Users/a1234/Desktop/Coding/网红/chuhai/chuhaihai/backend/requirements.txt)：

```bash
python3 -m venv backend/.venv
backend/.venv/bin/python -m pip install -r backend/requirements.txt
backend/.venv/bin/python backend/app.py
```

当前仓库已经同时支持：

```bash
python3 -m feishu_screening_bridge --help
python3 -m email_sync --help
```

邮件抓取如果不显式传 `--sent-since`，默认只抓最近 `3` 个自然月内的邮件；例如在 `2026-03-27` 运行时，默认等价于 `--sent-since 2025-12-27`。如果要改窗口，显式传 `--sent-since YYYY-MM-DD` 即可覆盖默认值。

以后达人匹配默认应直接使用任务上传里的飞书 `发信名单`，而不是本地测试达人库 workbook。可以直接按任务名下载 `发信名单` 并做匹配：

```bash
python3 -m email_sync enrich-creators \
  --env-file .env \
  --task-name "MINISO" \
  --db-path "data/task_upload_mail_sync/MINISO/email_sync.db" \
  --output-prefix "data/task_upload_mail_sync/MINISO/exports/发信名单_MINISO_匹配结果"
```

当前 duplicate review 有两条链：

- 旧的 sample-first sidecar：`prepare-duplicate-review` / `review-duplicate-groups`
- 新的生产 keep-list 链：`prepare-llm-review-candidates` / `run-llm-review`

生产链先把高置信 workbook 整理成“按我们去重 / 去重 / llm_candidates”三份产物：

```bash
python3 -m email_sync prepare-llm-review-candidates \
  --input "data/task_upload_mail_sync/MINISO/exports/测试达人库_MINISO_匹配结果_高置信.xlsx" \
  --db-path "data/task_upload_mail_sync/MINISO/email_sync.db" \
  --output-prefix "exports/测试达人库_MINISO_匹配结果_高置信_按我们去重"
```

然后直接基于 `*_llm_candidates.jsonl` 跑正式 LLM 审核并回填 reviewed / keep：

```bash
python3 -m email_sync run-llm-review \
  --env-file .env \
  --input-prefix "exports/测试达人库_MINISO_匹配结果_高置信_按我们去重"
```

这条生产 review 命令会输出：

- `*_llm_review.jsonl`
- `*_llm_reviewed.xlsx`
- `*_llm_reviewed_keep.xlsx`

如果你要切 provider，例如换成千问，不改代码，只改 `.env` 里的这些键：

- `OPENAI_API_KEY`
- `OPENAI_BASE_URL`
- `OPENAI_MODEL`
- `OPENAI_WIRE_API`
- `OPENAI_PROVIDER_NAME`
- `OPENAI_REASONING_EFFORT`

其中 `OPENAI_WIRE_API` 目前支持：

- `chat_completions`
- `responses`

下面这条仍然保留，作为旧的 sample-first 复核入口：

```bash
PYTHONPYCACHEPREFIX=/tmp/pycache python3 -m email_sync prepare-duplicate-review \
  --input "data/task_upload_mail_sync/MINISO/exports/测试达人库_MINISO_匹配结果_高置信.xlsx" \
  --db-path "data/task_upload_mail_sync/MINISO/email_sync.db" \
  --output-prefix "temp/miniso_duplicate_review_sample" \
  --sample-limit 3
```

对少量重复组做 group-level LLM 归属裁决：

```bash
PYTHONPYCACHEPREFIX=/tmp/pycache python3 -m email_sync review-duplicate-groups \
  --env-file .env \
  --input "data/task_upload_mail_sync/MINISO/exports/测试达人库_MINISO_匹配结果_高置信.xlsx" \
  --db-path "data/task_upload_mail_sync/MINISO/email_sync.db" \
  --output-prefix "temp/miniso_duplicate_review_live" \
  --sample-limit 3
```

这条 review 命令会自动叠加读取 `.env.local`，并输出：

- `*_audit.json`：按邮件组保留 decision、reason、raw_text
- `*_annotated.csv`
- `*_annotated.xlsx`
- `*_review_summary.json`

真实 MINISO 的 3 组 sample 结果已经落在：

- `temp/miniso_duplicate_review_live_audit.json`
- `temp/miniso_duplicate_review_live_annotated.csv`
- `temp/miniso_duplicate_review_live_annotated.xlsx`
- `temp/miniso_duplicate_review_live_review_summary.json`

把模板规则和达人名单写入筛号输入状态：

```bash
backend/.venv/bin/python scripts/prepare_screening_inputs.py \
  --creator-workbook "data/task_upload_mail_sync/MINISO/exports/测试达人库_MINISO_匹配结果_高置信.xlsx" \
  --template-workbook "downloads/task_upload_attachments/recveXGV2i3BS0/需求上传（excel 格式）/miniso-星战红人筛号需求模板(1).xlsx" \
  --summary-json "temp/miniso_screening_input_prep_summary.json"
```

按任务名直接从飞书任务上传里的 `发信名单` 和模板准备筛号输入：

```bash
backend/.venv/bin/python scripts/prepare_screening_inputs.py \
  --task-name "MINISO" \
  --task-upload-url "$TASK_UPLOAD_URL" \
  --summary-json "temp/miniso_task_driven_prep_summary.json"
```

如果上游已经完成 production duplicate review，也可以直接从 `keep` 名单进入当前 `筛号` 主链。推荐入口是：

```bash
backend/.venv/bin/python scripts/run_keep_list_screening_pipeline.py \
  --keep-workbook "exports/测试达人库_MINISO_匹配结果_高置信_按我们去重_llm_reviewed_keep.xlsx" \
  --template-workbook "downloads/task_upload_attachments/recveXGV2i3BS0/需求上传（excel 格式）/miniso-星战红人筛号需求模板(1).xlsx" \
  --summary-json "temp/keep_list_pipeline_summary.json" \
  --platform instagram \
  --max-identifiers-per-platform 20 \
  --skip-visual
```

这条 keep-list runner 会先做两件事：

- 把 `*_llm_reviewed_keep.xlsx` 写成 `data/<platform>/<platform>_upload_metadata.json`
- 把模板编译并写入 `config/active_rulespec.json`

然后再按边界控制参数决定是否继续往下跑：

- `--platform`：只跑指定平台，可重复传入
- `--max-identifiers-per-platform`：每个平台最多跑多少个账号，适合 bounded validation
- `--skip-scrape`：只做 staging，不触发 Apify / prescreen / export
- `--skip-visual`：跑 scrape 和导出，但跳过视觉复核
- `--vision-provider`：显式指定视觉 provider，例如 `openai`
- `--probe-vision-provider-only`：只做视觉 provider 轻量探活，不继续跑 scrape / visual

summary 会明确记录：

- keep workbook 路径
- 实际 staged 数量和分平台计数
- 是否执行 scrape
- 是否执行 visual review，或为什么跳过
- 每个平台导出产物路径

这条入口和 `scripts/run_screening_smoke.py` 的区别是：

- `run_keep_list_screening_pipeline.py`：面向 reviewed keep-list，作为正式下游入口
- `run_screening_smoke.py`：面向 sample workbook 的 smoke / benchmark / runtime validation

## 视觉 provider 诊断

当前 backend 的视觉配置来源要分开看：

- `scripts/run_keep_list_screening_pipeline.py --env-file ...`
  这个参数只会传给 `prepare_screening_inputs.py` 之类的 staging 逻辑，不会直接改 backend 进程里的视觉 provider 配置。
- `backend/app.py`
  视觉 provider 在 backend 导入时会自动读取 `./.env.local`，并且遵循“已有进程环境优先，其次才是 `.env.local`”。

也就是说，视觉复核真正吃的是：

- 当前 backend 进程环境
- 仓库根目录的 `.env.local`

而不是 keep-list runner 传入的 `--env-file`。

当前支持的视觉 provider 相关环境变量包括：

- `OPENAI_API_KEY`
- `OPENAI_BASE_URL`
- `OPENAI_VISION_MODEL`
- `VISION_MODEL`
- `VISION_QUAN2GO_API_KEY`
- `VISION_QUAN2GO_BASE_URL`
- `VISION_QUAN2GO_MODEL`
- `VISION_LEMONAPI_API_KEY`
- `VISION_LEMONAPI_BASE_URL`
- `VISION_LEMONAPI_MODEL`

开始真实视觉 run 前，推荐先做一次 bounded 诊断：

```bash
backend/.venv/bin/python scripts/run_keep_list_screening_pipeline.py \
  --keep-workbook "exports/测试达人库_MINISO_匹配结果_高置信_按我们去重_llm_reviewed_keep.xlsx" \
  --template-workbook "downloads/task_upload_attachments/recveXGV2i3BS0/需求上传（excel 格式）/miniso-星战红人筛号需求模板(1).xlsx" \
  --output-root "temp/keep_list_visual_diagnostic" \
  --platform instagram \
  --max-identifiers-per-platform 1 \
  --skip-scrape
```

如果要先确认指定 provider 的 auth surface 是真的可用，再做完整 bounded run，先跑 probe：

```bash
backend/.venv/bin/python scripts/run_keep_list_screening_pipeline.py \
  --keep-workbook "exports/测试达人库_MINISO_匹配结果_高置信_按我们去重_llm_reviewed_keep.xlsx" \
  --template-workbook "downloads/task_upload_attachments/recveXGV2i3BS0/需求上传（excel 格式）/miniso-星战红人筛号需求模板(1).xlsx" \
  --output-root "temp/phase15_probe_only_openai" \
  --summary-json "temp/phase15_probe_only_openai/summary.json" \
  --platform instagram \
  --vision-provider openai \
  --probe-vision-provider-only
```

这条命令会提前调用 backend 的 `/api/vision/providers/probe`，并把结果写入 summary 顶层的 `vision_probe`。如果 probe 不通过，runner 会直接以 `vision_probe_failed` 退出，不再浪费时间进入 scrape / visual。

当前仓库已经用 `openai` 完成了一次真实 bounded visual validation。可复跑命令是：

```bash
backend/.venv/bin/python scripts/run_keep_list_screening_pipeline.py \
  --keep-workbook "exports/测试达人库_MINISO_匹配结果_高置信_按我们去重_llm_reviewed_keep.xlsx" \
  --template-workbook "downloads/task_upload_attachments/recveXGV2i3BS0/需求上传（excel 格式）/miniso-星战红人筛号需求模板(1).xlsx" \
  --output-root "temp/phase15_bounded_openai_live" \
  --summary-json "temp/phase15_bounded_openai_live/summary.json" \
  --platform instagram \
  --max-identifiers-per-platform 1 \
  --vision-provider openai
```

这次真实验证的结果是：

- `vision_probe.success = true`
- `scrape` 成功返回 `1/1`
- `prescreen_pass_count = 1`
- `visual_job.result.success = true`
- 视觉结果为真实业务判断，不再是 `auth_not_found` / `Error`

对应产物在：

- `temp/phase15_probe_only_openai/summary.json`
- `temp/phase15_bounded_openai_live/summary.json`
- `temp/phase15_bounded_openai_live/data/instagram/instagram_visual_results.json`
- `temp/phase15_bounded_openai_live/exports/instagram/instagram_final_review.xlsx`

如果要先看 backend 自己认出的视觉状态，也可以直接查：

```bash
curl -sS http://127.0.0.1:5001/api/health
```

重点看这些字段：

- `checks.vision_preflight.status`
- `checks.vision_preflight.runnable_provider_names`
- `checks.vision_preflight.providers[*].api_key_source`
- `checks.vision_preflight.providers[*].base_url`
- `checks.vision_preflight.providers[*].model`

keep-list runner 和 smoke summary 里也会同步写入 backend 视角的诊断结果：

- 顶层 `vision_preflight`
- `platforms.<platform>.vision_preflight`
- `platforms.<platform>.visual_gate`

这样可以直接判断：

- backend 当时认出了哪些 provider
- provider 是来自 `.env.local` 还是进程环境
- visual review 为什么会执行、跳过，或者预期会失败
