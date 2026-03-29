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

## 开发接手最短路径

如果新开发要快速接手这个仓库，建议按下面顺序看：

1. 先看这份 `README.md`
   重点是：
   - `当前主链路`
   - 上游两条正式路径：`legacy-enrichment` / `brand-keyword-fast-path`
   - 单入口 `task upload -> final export`
   - `视觉 provider 诊断`
2. 再看 planning 文档
   - `.planning/PROJECT.md`：当前系统边界、已验证能力、里程碑决策
   - `.planning/ROADMAP.md`：当前 milestone 的 phase 和后续实现顺序

如果只是理解整体链路，这两层文档已经够了。

如果要开始改代码，再打开这 4 个入口文件就够了：

- `scripts/run_task_upload_to_final_export_pipeline.py`
- `scripts/run_task_upload_to_keep_list_pipeline.py`
- `scripts/run_keep_list_screening_pipeline.py`
- `backend/app.py`

通常做法应该是：先靠文档理解系统怎么跑，再从这 4 个入口顺着调用链往下看，而不是一上来全仓库搜索。

## 本地运行

筛号后端依赖在 `backend/requirements.txt`：

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

当前上游匹配已经有两条正式 repo-local 路径：

- `legacy-enrichment`
  走 `enrich-creators -> prepare-llm-review-candidates -> run-llm-review`
- `brand-keyword-fast-path`
  走 `match-brand-keyword -> split/resolve shared-email -> llm-final-review`

如果你要直接跑品牌关键词快路径，可以按下面四步：

```bash
python3 -m email_sync match-brand-keyword \
  --db-path "data/task_upload_mail_sync/MINISO/email_sync.db" \
  --keyword "MINISO" \
  --input "陈俊仁的总表.xlsx" \
  --output-prefix "exports/MINISO_brand_keyword_match"
```

这一步会输出：

- `exports/MINISO_brand_keyword_match.xlsx`
- `exports/MINISO_brand_keyword_match_deduped.xlsx`
- `exports/MINISO_brand_keyword_match_unique_email.xlsx`
- `exports/MINISO_brand_keyword_match_shared_email.xlsx`

如果你手里已经只有 deduped workbook，也可以单独重跑一次 unique/shared-email split：

```bash
python3 -m email_sync split-shared-email \
  --input "exports/MINISO_brand_keyword_match_deduped.xlsx" \
  --output-prefix "exports/MINISO_shared_email_split"
```

然后对 shared-email groups 做内容规则判定，先自动解决能定人的部分：

```bash
python3 -m email_sync resolve-shared-email \
  --db-path "data/task_upload_mail_sync/MINISO/email_sync.db" \
  --input "exports/MINISO_brand_keyword_match_shared_email.xlsx" \
  --output-prefix "exports/MINISO_shared_email_resolution"
```

这一步会输出：

- `exports/MINISO_shared_email_resolution_resolved.xlsx`
- `exports/MINISO_shared_email_resolution_unresolved.xlsx`
- `exports/MINISO_shared_email_resolution_llm_candidates.jsonl`

最后只把 unresolved tail 交给 LLM，并把自动保留部分合并成最终 keep workbook：

```bash
python3 -m email_sync llm-final-review \
  --env-file .env \
  --input-prefix "exports/MINISO_shared_email_resolution" \
  --auto-keep-workbook "exports/MINISO_brand_keyword_match_unique_email.xlsx" \
  --auto-keep-workbook "exports/MINISO_shared_email_resolution_resolved.xlsx"
```

这一步会输出：

- `exports/MINISO_shared_email_resolution_llm_review.jsonl`
- `exports/MINISO_shared_email_resolution_llm_resolved.xlsx`
- `exports/MINISO_shared_email_resolution_manual_tail.xlsx`
- `exports/MINISO_shared_email_resolution_final_keep.xlsx`

其中：

- `*_manual_tail.xlsx` 是 LLM 仍无法稳定裁决、需要人工看的尾部
- `*_final_keep.xlsx` 是可直接给下游筛号 runner 的 keep workbook

legacy enrichment 的 duplicate review 仍然保留两条链：

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

如果你不想再手工串 `任务上传 -> mail sync -> enrichment -> duplicate review -> keep-list` 这些命令，当前仓库已经提供了一个单入口上游 runner：

```bash
backend/.venv/bin/python scripts/run_task_upload_to_keep_list_pipeline.py \
  --task-name "MINISO" \
  --env-file .env \
  --output-root "temp/miniso_task_to_keep_list" \
  --summary-json "temp/miniso_task_to_keep_list/summary.json" \
  --stop-after keep-list
```

这条命令会按固定顺序串起：

- 下载任务上传里的模板和 `发信名单`
- 按任务抓取邮箱文件夹邮件
- 根据 `--matching-strategy` 选择一条上游路径：
  - `legacy-enrichment`：生成 enrichment 匹配结果和高置信 workbook，再继续 `llm_candidates -> llm_review`
  - `brand-keyword-fast-path`：执行 `brand_match -> shared_resolution -> final_review`
- 产出最终 keep workbook

如果你要让单入口 runner 直接走 integrated fast path，用这条：

```bash
backend/.venv/bin/python scripts/run_task_upload_to_keep_list_pipeline.py \
  --task-name "MINISO" \
  --env-file .env \
  --output-root "temp/miniso_task_to_keep_list_fast_path" \
  --summary-json "temp/miniso_task_to_keep_list_fast_path/summary.json" \
  --matching-strategy brand-keyword-fast-path \
  --brand-keyword "MINISO" \
  --brand-match-include-from \
  --stop-after keep-list
```

summary 会明确记录：

- 实际使用的是哪条 `matching_strategy`
- `contract.canonical_boundary = keep-list`
- `resume_context`
  区分这次是否接受了旧 summary、为什么允许或禁止下游复用
- 每个步骤的状态
- 每个步骤的 `execution_mode`
  取值是 `produced` / `reused` / `rerun`
- 每个步骤对应的 artifact 路径
- keep-list 的 canonical 路径
- fast-path 下的 `brand_match` / `shared_resolution` / `final_review` 统计
- `final_review.selected_provider` / `selected_model` / `provider_attempts` / `absorbed_failures`
- `manual_tail_xlsx` 和最终 `keep_workbook`
- 下一步下游筛号的 handoff 命令

其中当前 runner 的 resume 语义是：

- `task_assets` 可以在旧 artifact 还在时复用
- `mail_sync` 永远按当前 run 重新执行增量同步，不直接复用旧 step
- 只有当 `task_assets` 没变、`mail_sync` 也没有抓到新邮件时，下游匹配 / review steps 才允许复用旧 artifact
- 无论上游怎么复用，给下游 `scripts/run_keep_list_screening_pipeline.py` 的 canonical boundary 都还是 `resume_points.keep_list`

如果你只是想先做到某个边界，也可以用：

- `--stop-after task-assets`
- `--stop-after mail-sync`
- `--stop-after enrichment`
- `--stop-after llm-candidates`
- `--stop-after brand-match`
- `--stop-after shared-resolution`
- `--stop-after keep-list`

如果当前 `output-root` 下已经存在同名 artifact，runner 默认会尽量复用已有上游产物；如果你要强制重跑，不复用历史 artifact，传：

```bash
--no-reuse-existing
```

如果你要从 `task upload` 起点直接跑到最终导出，当前正式单入口是：

`scripts/run_task_upload_to_final_export_pipeline.py`

真实 `MINISO` bounded proof 用的是下面这条命令。这里用一个很薄的 Python wrapper 从 `.env` 读取上游 unresolved-tail review 需要的 legacy `LLM_*`，避免把 secret 直接写进 shell history；下游视觉仍然走 `.env.local` 里的 `OPENAI_*`，并通过 `--vision-provider openai` 显式选中：

```bash
python3 - <<'PY'
import subprocess
from pathlib import Path

env_values = {}
for raw_line in Path('.env').read_text(encoding='utf-8').splitlines():
    line = raw_line.strip()
    if not line or line.startswith('#') or '=' not in line:
        continue
    key, value = line.split('=', 1)
    key = key.strip()
    if key not in {'LLM_API_BASE', 'LLM_API_KEY', 'LLM_MODEL'}:
        continue
    env_values[key] = value.strip().strip('"').strip("'")

cmd = [
    'backend/.venv/bin/python',
    'scripts/run_task_upload_to_final_export_pipeline.py',
    '--task-name', 'MINISO',
    '--env-file', '.env',
    '--output-root', 'temp/phase18_real_bounded_e2e_final2',
    '--summary-json', 'temp/phase18_real_bounded_e2e_final2/summary.json',
    '--matching-strategy', 'brand-keyword-fast-path',
    '--brand-keyword', 'MINISO',
    '--brand-match-include-from',
    '--platform', 'instagram',
    '--max-identifiers-per-platform', '1',
    '--vision-provider', 'openai',
    '--no-reuse-existing',
    '--base-url', env_values.get('LLM_API_BASE', ''),
    '--api-key', env_values.get('LLM_API_KEY', ''),
    '--model', env_values.get('LLM_MODEL', ''),
    '--wire-api', 'chat_completions',
]
raise SystemExit(subprocess.run(cmd).returncode)
PY
```

这条命令会：

- 从任务上传起点开始跑
- 上游显式走 `brand-keyword-fast-path`
- 在内部保留 `keep-list` 作为 canonical resume boundary
- 下游只跑 `instagram`
- 每个平台只跑 `1` 个账号
- 视觉 provider 显式指定为 `openai`

Phase 19 之后，这条单入口命令还默认带上了新的可靠性 contract：

- 上游 `llm-final-review` 会先在当前 candidate 内重试，再按 `OPENAI_SECONDARY_*` / `OPENAI_TERTIARY_*` failover；成功后 summary 会记录 `selected_provider`、`selected_model`、`provider_attempts`、`absorbed_failures`
- 下游 `summary.json` 会在平台执行过程中持续落盘 `current_stage`、`last_updated_at`、`scrape_job.apify_run_id`、`scrape_job.apify_dataset_id`
- 如果 Apify poll 抖动发生在已经拿到部分结果之后，平台不会再被一刀切成 `scrape_failed`；而是保留 `scrape_partial` 或 `scrape_poll_failed_with_partial`，并在 `scrape_job.partial_result` 里留下可恢复上下文
- 顶层 final wrapper 会把 `completed_with_partial_scrape` 视为可交付状态，并通过 `delivery_status` 与 `steps.downstream.platform_statuses` 保留各平台真实状态
- 视觉复核 trace 现在会显式区分 `configured_model`、`requested_model`、`response_model`、`effective_model`；preferred pool 遇到 retryable fault 时会继续走健康候选并允许回到优先池，而不是过早落成终态 `Error`

按当前 contract，手工介入应该只发生在候选池和 resume 路径都真正耗尽之后，而不是默认操作方式。

这轮真实 bounded proof 的顶层结果已经落在：

- `temp/phase18_real_bounded_e2e_final2/summary.json`
- `temp/phase18_real_bounded_e2e_final2/upstream/summary.json`
- `temp/phase18_real_bounded_e2e_final2/downstream/summary.json`

关键结果是：

- 顶层 `status = completed`
- 上游 `final_keep_row_count = 325`
- 上游 `shared_resolution.llm_candidate_group_count = 26`
- 下游 `vision_probe.success = true`
- `instagram scrape_job.status = completed`
- `prescreen_pass_count = 0`
- `visual_job.status = skipped`
  因为这轮 bounded proof 选中的账号在 prescreen 就被 Reject，所以没有进入视觉复核，但最终导出链已经完整完成
- 最终导出：
  `temp/phase18_real_bounded_e2e_final2/downstream/exports/instagram/instagram_final_review.xlsx`

如果后续某轮 run 出现“部分平台已可交付、但 scrape 侧还有 salvage 信息”的情况，顶层 `summary.json` 可能会是：

- `status = completed_with_partial_scrape`
- `delivery_status = completed_with_partial_scrape`
- `steps.downstream.platform_statuses = {"instagram": "completed_with_partial_scrape", ...}`

这表示至少一个平台已经留下可交付导出，operator 下一步应优先看：

- `steps.downstream.platform_statuses`
- `downstream/summary.json` 里的 `current_stage` / `scrape_job.partial_result`
- `resume_points.keep_list.recommended_command`

如果这条单入口命令在上游已经产出了 keep workbook，但下游因为网络抖动之类的问题需要单独重跑，不要重新拼路径；直接用 summary 里给出的 `resume_points.keep_list.recommended_command`。这次真实 proof 对应的可直接复跑命令是：

```bash
backend/.venv/bin/python scripts/run_keep_list_screening_pipeline.py \
  --keep-workbook "temp/phase18_real_bounded_e2e_final2/upstream/exports/MINISO_shared_email_resolution_final_keep.xlsx" \
  --env-file ".env" \
  --template-workbook "temp/phase18_real_bounded_e2e_final2/upstream/downloads/recveXGV2i3BS0/需求上传（excel 格式）/miniso-星战红人筛号需求模板(1).xlsx" \
  --platform instagram \
  --max-identifiers-per-platform 1 \
  --vision-provider openai \
  --poll-interval 5.0
```

要注意，这个 proof 证明的是“repo-local 单入口主线已经可跑通”，但它有三层明确限定：

- 这是 bounded validation，不等价于任意任务、任意批量、任意平台都已经完成稳定性证明
- 这是单入口 mainline runner 的 proof，不等价于 legacy workbook / dashboard / project-home 入口已经全部脱离 external full `email` 依赖
- 这轮 proof 主要证明了当前 `openai` 路径和现有 orchestration contract 可用，不等价于其他 provider 或更大样本也已完成 live 可用性证明

Phase 15 已经单独证明过 `openai` 视觉链本身可以真实进入 visual review；Phase 18 证明的是单入口 `task upload -> final export` 在 repo-local 主线里已经真实跑通，但不应把这轮 bounded proof 误读成“所有入口、所有 provider、所有规模都已 fully proven”。

如果后面要继续做视觉 prompt / fallback 优化，可以直接参考外部 benchmark：

- 参考文档：外部 sibling `筛号/docs/2026-03-29-qwen-prompt-benchmark.md`
- 当前最实用的结论不是“继续靠 prompt 追平 GPT”，而是保留 `gpt-5.4` 原始 prompt，给 `qwen-vl-max` 单独使用最佳 `v2` prompt
- 推荐路由顺序：`gpt-5.4 -> qwen-vl-max`
- 推荐方法：固定 benchmark 样本、分 provider prompt 文件、允许 `SKIP_OPENAI=1` 只迭代 fallback，并把结果持续落盘做横向对比

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
- 平台执行中的 `current_stage`、`last_updated_at`
- `scrape_job.apify_run_id`、`scrape_job.apify_dataset_id`
- `scrape_job.partial_result`，以及 `scrape_failed` / `scrape_partial` / `scrape_poll_failed_with_partial` 的区别
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
- `VISION_PROVIDER_PREFERENCE`
- `VISION_MODEL`
- `VISION_MIMO_API_KEY`
- `VISION_MIMO_BASE_URL`
- `VISION_MIMO_MODEL`
- `VISION_MIMO_MAX_COMPLETION_TOKENS`
- `VISION_QIANDAO_API_KEY`
- `VISION_QIANDAO_BASE_URL`
- `VISION_QIANDAO_MODEL`
- `VISION_QIANDAO_FALLBACK_MODEL`
- `VISION_QIANDAO_MAX_TOKENS`
- `VISION_QIANDAO_TEMPERATURE`
- `VISION_QUAN2GO_API_KEY`
- `VISION_QUAN2GO_BASE_URL`
- `VISION_QUAN2GO_MODEL`
- `VISION_LEMONAPI_API_KEY`
- `VISION_LEMONAPI_BASE_URL`
- `VISION_LEMONAPI_MODEL`

当前建议把默认视觉 provider 显式锁到 `openai`，例如在 `.env.local` 里设置：

- `VISION_PROVIDER_PREFERENCE=openai`

其中 `mimo` 走独立的 `chat/completions` 分支，作为备选 provider，默认配置是：

- `VISION_MIMO_BASE_URL=https://api.xiaomimimo.com/v1`
- `VISION_MIMO_MODEL=mimo-v2-omni`
- 鉴权头使用 `api-key: ...`
- 默认 `max_completion_tokens=2048`

`qiandao` 也走 `chat/completions` 分支，适合接 OpenAI-compatible 的 Gemini 聚合通道，默认配置是：

- `VISION_QIANDAO_BASE_URL=https://api2.qiandao.mom/v1`
- `VISION_QIANDAO_MODEL=gemini-2.5-pro-preview-p`
- `VISION_QIANDAO_FALLBACK_MODEL=gemini-3-flash-preview-S`
- `VISION_QIANDAO_MAX_TOKENS=900`
- `VISION_QIANDAO_TEMPERATURE=0.2`
- 鉴权头使用 `Authorization: Bearer ...`
- 图片会先由服务端下载，再转成 `data:image/...;base64,...` 后送到模型
- 当直连或 tiered 路由里包含 `gemini-2.5-pro-preview-p` 时，backend 会把视觉复核默认并发收紧到 `2`，并把显式 `max_workers` 硬限制在 `3`
- 也就是说，`gemini-2.5-pro-preview-p` 不建议默认并发 `4+`

如果你要启用分层视觉路由，而不是固定单 provider，可以额外配置：

- `VISION_VISUAL_REVIEW_ROUTING_STRATEGY=tiered`
- `VISION_VISUAL_REVIEW_PRIMARY_PROVIDER=qiandao`
- `VISION_VISUAL_REVIEW_PRIMARY_MODEL=gemini-3-flash-preview-S`
- `VISION_VISUAL_REVIEW_PRIMARY_TIMEOUT_SECONDS=20`
- `VISION_VISUAL_REVIEW_BACKUP_PROVIDER=qiandao`
- `VISION_VISUAL_REVIEW_BACKUP_MODEL=gemini-2.5-pro-preview-p`
- `VISION_VISUAL_REVIEW_BACKUP_TIMEOUT_SECONDS=25`
- `VISION_VISUAL_REVIEW_JUDGE_PROVIDER=openai`
- `VISION_VISUAL_REVIEW_JUDGE_MODEL=gpt-5.4`
- `VISION_VISUAL_REVIEW_JUDGE_TIMEOUT_SECONDS=30`

这条 tiered 路由当前只在显式开启时生效，默认仍然是单 provider 直连。启用后会按：

- primary 跑量
- backup 兜底结构化输出和边界样本
- judge 处理高价值或疑难复判

如果你要启用“批次开始前先赛马，再按优先级选主通道”的策略，可以配置：

- `VISION_VISUAL_REVIEW_ROUTING_STRATEGY=probe_ranked`
- `VISION_VISUAL_REVIEW_PROBE_RANKED_PREFERRED_PROVIDER=openai`
- `VISION_VISUAL_REVIEW_PROBE_RANKED_PREFERRED_MODEL=gpt-5.4`
- `VISION_VISUAL_REVIEW_PROBE_RANKED_PREFERRED_TIMEOUT_SECONDS=30`
- `VISION_VISUAL_REVIEW_PROBE_RANKED_PREFERRED_PARALLEL_PROVIDER=quan2go`
- `VISION_VISUAL_REVIEW_PROBE_RANKED_PREFERRED_PARALLEL_MODEL=gpt-5.4`
- `VISION_VISUAL_REVIEW_PROBE_RANKED_PREFERRED_PARALLEL_TIMEOUT_SECONDS=30`
- `VISION_VISUAL_REVIEW_PROBE_RANKED_SECONDARY_PROVIDER=qiandao`
- `VISION_VISUAL_REVIEW_PROBE_RANKED_SECONDARY_MODEL=gemini-2.5-pro-preview-p`
- `VISION_VISUAL_REVIEW_PROBE_RANKED_SECONDARY_TIMEOUT_SECONDS=25`
- `VISION_VISUAL_REVIEW_PROBE_RANKED_TERTIARY_PROVIDER=qiandao`
- `VISION_VISUAL_REVIEW_PROBE_RANKED_TERTIARY_MODEL=gemini-3-flash-preview-S`
- `VISION_VISUAL_REVIEW_PROBE_RANKED_TERTIARY_TIMEOUT_SECONDS=20`
- `VISION_VISUAL_REVIEW_PROBE_RANKED_DISABLE_AFTER_FAILURES=2`

这条 `probe_ranked` 路由会在每个 visual batch 开始前并发做最小图片 probe，然后按固定优先级构建当前批次执行池：

- `gpt-5.4 / openai`
- `gpt-5.4 / quan2go`
- `gemini-2.5-pro-preview-p`
- `gemini-3-flash-preview-S`

如果两条 `gpt-5.4` 通道都 probe 成功，backend 会把账号分摊到这两个通道并发执行，而不是整批只吃一条。单个账号失败时，会按：

- 另一条 `gpt-5.4`
- `gemini-2.5-pro-preview-p`
- `gemini-3-flash-preview-S`

顺序降级，不会每个账号都重新赛马。某条通道连续失败达到阈值后，会在当前批次里临时摘掉。

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
  --vision-provider mimo \
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
