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

先把 enrichment 结果里的重复 `last_mail` 组整理成 sample-first 复核输入：

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
