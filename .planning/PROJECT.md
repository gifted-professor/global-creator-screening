# chuhaihai

## What This Is

`chuhaihai` 是一个把创作者筛选相关本地工具收拢到同一仓库的整合工程。到 `v1.2.0` 为止，仓库已经不仅完成了飞书桥接、任务驱动邮件抓取、模板解析、达人匹配 / duplicate review 和 `筛号` 后端整合，还把它们收成了一个从 task upload 到 final export 的 repo-local 单入口闭环，并补齐了可靠性与诊断 contract。

## Core Value

在不打断现有本地工作流的前提下，把飞书内容获取、筛选导入和相关配置集中到一个可持续维护的仓库里。

## Requirements

### Validated

- [x] 当前仓库已经包含可直接运行的 `feishu_screening_bridge`
- [x] 当前仓库已经包含任务驱动邮件抓取所需的核心 `email_sync` 能力
- [x] 任务上传 -> 员工信息 -> 邮箱 / `imap 码` -> `email_sync` 的 `MINISO` 链路已实测通过
- [x] 当前仓库已经包含 `sync-task-upload-mail`，可以直接按任务名抓取对应邮箱文件夹邮件
- [x] 当前仓库已经包含可直接运行的 workbook 模板解析模块
- [x] 飞书任务模板现在可以在下载后立即解析，并已用真实 Feishu 数据实测通过
- [x] 当前仓库已经包含 `creator_enrichment` 和 `enrich-creators`，可以在本地邮件库上抽最后一封邮件和报价
- [x] `【测试】达人库.xlsx` 已经和本地 `MINISO` 邮件库实跑匹配完成
- [x] 当前仓库已经包含 `筛号` 后端核心链路、规则配置和运行脚本
- [x] 当前仓库本地 `backend/.venv` 已完成依赖安装，并通过 runtime validation
- [x] 视觉复核默认并发已按 2026-03-27 benchmark 调整为 `6`
- [x] 当前仓库已经包含 `scripts/prepare_screening_inputs.py`，可把模板解析产物和达人匹配名单直接写入筛号后端当前输入状态
- [x] `MINISO` 模板 rulespec 和高置信达人名单已实写到当前仓库 `config/active_rulespec.json` 与 `data/*/*_upload_metadata.json`
- [x] 当前仓库已经包含 backend-owned 的视觉 provider snapshot / preflight / structured early failure 诊断能力，并写进 health 与 runner summary
- [x] 当前仓库已经用显式 `openai` provider 成功跑通一轮真实 bounded `MINISO instagram` visual review，从 scrape 一直到 final export
- [x] 用户已在另一条已验证流程中证明品牌关键词快路径可以把 `MINISO` 三个月邮件中的绝大多数候选自动绑定，只剩极小人工尾部

### Active

- [ ] 移除 legacy workbook / dashboard / project-home 对外部全量 `email` 项目的剩余依赖
- [ ] 把报价结果正式接入 `筛号` 运行态或最终导出链
- [ ] 为多平台或更大批量补充稳定性证明，而不只停留在 bounded validation
- [ ] 规划下一里程碑应该优先做 dependency removal、quote integration，还是更大样本的 live proof

### Out of Scope

- 彻底重写 `email_sync` 或 `筛号` 业务逻辑 — 当前目标是迁移与整合，不是重构旧系统
- 云端部署或生产化打包 — 目前是本地工作流整合
- 在没有明确业务需要前重做视觉复核算法本身 — 当前优先级是把现有链路编排成单入口、可复跑的闭环

## Context

当前仓库起步时只有 `.env`、`.env.example`、`.gitignore` 和 GSD 工具目录，没有应用代码。`v1.0.0` 完成了从 `上传飞书`、`抓取邮件`、`模板解析` 和 `筛号` sibling 目录向当前仓库的主线整合。现在本仓库已经能完成两条真实业务链：

- `任务上传 -> 员工信息 -> 模板下载/解析 -> 按任务抓取邮箱文件夹邮件 -> 达人匹配 -> duplicate review -> keep-list`
- `keep-list -> 筛号运行态 -> Apify 抓取 -> 预筛 -> 视觉复核调用 -> 导出`

当前仓库已经分别验证过两段真实业务链：

- `任务上传 -> 员工信息 -> 模板下载/解析 -> 按任务抓取邮箱文件夹邮件 -> 达人匹配 -> duplicate review -> keep-list`
- `keep-list -> 筛号运行态 -> Apify 抓取 -> 预筛 -> 视觉复核 -> 导出`

`v1.1.0` 已证明此前的 `auth_not_found` 不是“现在 apikey 没填好”，而是旧 provider 路径不稳定；当前显式 `openai` 路径已经 real run 成功。与此同时，用户还提供了一条在 `MINISO` 三个月真实邮件上验证过的更快上游路径：按品牌关键词筛信、邮箱精确匹配总表、按 IGlink 去重、拆唯一/共享邮箱、共享邮箱先看邮件内容再决定，只有极少数尾部交给 LLM / 人工。`16.1` 已经把这条 fast-path 泛化并串进单入口上游 runner；`18-01` 则进一步把它和 keep-list 下游串成一个 thin final wrapper，并在 `temp/phase18_real_bounded_e2e_final2` 留下了一轮真实 bounded `task upload -> final export` proof。

## Current State

- `v1.0.0` 已交付 repo-local 的创作者筛选主线，核心模块不再散落在多个 sibling 工程里
- `v1.1.0` 已交付视觉 provider 诊断、显式 provider 选择、live probe，以及一轮真实 non-error bounded visual validation
- `MINISO` 已具备真实 duplicate review 产物链和 keep workbook，下游也已证明能从 scrape 跑到 final export
- Phase 16 已交付 repo-local 的 `task upload -> keep-list` 单入口 runner 和 machine-readable handoff summary
- 当前 runner summary 已能显式区分每一步是 `produced`、`reused` 还是 `rerun`
- 当前 runner 已明确收紧复用语义：`task_assets` 可复用，`mail_sync` 永远按当前 run 增量重跑，只有在上游输入未变化且没有新邮件时，下游 matching / review steps 才能复用
- 当前 upstream/downstream runner summary 已能显式暴露 resolved input/source/preflight，operator 可以直接看到 env file、任务附件/workbook、task DB 和 output dirs 的实际归属
- `16.1` 已交付 repo-local 的 `match-brand-keyword` / `split-shared-email` / `resolve-shared-email` / `llm-final-review`，并把 fast-path 正式接进单入口上游 runner
- 单入口上游 runner 现在显式支持 `legacy-enrichment` 与 `brand-keyword-fast-path` 两条策略；fast-path 会输出 `manual_tail.xlsx` 与最终 `final_keep.xlsx`
- legacy `feishu_screening_bridge` 命令现在会在入口显式诊断外部 full `email` 依赖，缺失时直接返回 remediation，而不是中途模糊失败
- `scripts/run_task_upload_to_final_export_pipeline.py` 已交付为最终单入口 surface，并保留 `keep-list` 作为内部 canonical resume boundary
- `v1.2.0` 已正式归档，当前仓库处于“等待下一里程碑定义”的 between-milestones 状态
- Phase 19 已把 upstream shared-email final review 升级为 primary / secondary / tertiary candidate 可重试、可 failover 的 transport contract，summary 会显式保留 `selected_provider`、`selected_model`、`provider_attempts`、`absorbed_failures`
- Phase 19 已把 downstream scrape 状态收紧成 live、stageful、partial-result-aware contract；`scrape_failed` 只代表零输出失败，partial salvage 会落成 `scrape_partial` 或 `scrape_poll_failed_with_partial`
- Phase 19 已让 final wrapper 保留 `delivery_status` 与 `platform_statuses`，所以 `completed_with_partial_scrape` 这类可交付状态不会再被顶层误判为纯失败
- Phase 19 已把 visual review trace 标准化为 `configured_model` / `requested_model` / `response_model` / `effective_model`，并让 preferred pool 在 retryable fault 后继续尝试健康候选
- 真实 bounded `MINISO` proof 已完成：
  - 顶层 artifact root: `temp/phase18_real_bounded_e2e_final2`
  - top-level `status = completed`
  - upstream `final_keep_row_count = 325`
  - downstream `instagram` bounded run 成功产出 `instagram_final_review.xlsx`
- 当前没有 active milestone；下一步应通过 `$gsd-new-milestone` 决定是先处理 `DEP-01` / `QTE-01`，还是补 `REL-01` 的更大样本 live proof

## Next Milestone Candidates

`v1.2.0` 已归档完成。下一里程碑更合理的候选方向有三类：

- 先处理 `DEP-01`，把 legacy workbook / dashboard / project-home 的剩余 external dependency 拆掉
- 先处理 `QTE-01`，把报价结果接进 screening runtime / final export，让链路从“筛选”走向“可执行交付”
- 先处理 `REL-01`，补更大样本或多平台 live proof，把当前 bounded-first 证据扩成更强的稳定性证明

建议在新 milestone 里只选其中一个作为 committed 主轴，避免把 dependency removal、data integration 和 broad live proof 混成一个模糊里程碑。

## Constraints

- **Compatibility**: 迁移后仍需兼容现有 sibling 项目结构 — 旧的 `抓取邮件/email_sync` 目录短期内仍是依赖方
- **Scope**: 只搬运已经验证过的飞书桥接和邮件处理能力 — 暂不顺手扩展无关新功能
- **Security**: `.env.example` 不应继续携带真实密钥 — 示例文件只保留结构和安全默认值
- **Workflow**: 里程碑收尾与新里程碑启动要分开提交 — 归档 tag 应对应真实 shipped 状态，而不是混入后续规划

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| 先建立最小 `.planning` 骨架，再执行迁移 | 用户通过 `$workflow` 发起任务，仓库此前未初始化 | ✓ Good |
| 优先迁移 `feishu_screening_bridge` 整包和测试 | 旧仓库相关能力集中在该包内，拆分迁移会增加遗漏风险 | ✓ Good |
| 暂不修改默认 `EMAIL_PROJECT_ROOT` | 旧外部 email 目录仍是当前可工作的默认配置，sibling `抓取邮件` 目录下暂未发现 `.env` | ✓ Good |
| 当前仓库只迁入 `email_sync` 的任务驱动核心能力 | 用户当前优先目标是按任务拿邮箱、`imap 码` 后直接抓邮件，而不是搬完整 project-home 系统 | ✓ Good |
| 桥接层通过隔离导入加载外部 `email` 项目 | 当前仓库已存在最小 `email_sync`，必须避免与外部全量 `email_sync` 包冲突 | ✓ Good |
| Phase 5 先只做“下载模板 -> 模板解析” | 用户明确表示后续链路暂不锁定，当前先把最小可用闭环打通 | ✓ Good |
| 任务邮件抓取桥接直接复用当前仓库本地 `email_sync` | 代码已经迁入，下一步缺的不是再次复制，而是把任务映射结果接到现有同步入口 | ✓ Good |
| 报价抽取业务层整包迁入当前仓库 | 邮件解析与线程索引已经在本地，缺的是基于 thread 的业务判断，不该继续依赖 sibling 目录 | ✓ Good |
| `筛号` 后端按整包迁移到当前仓库 | 用户要的是完整可用链路，不是分散读取若干零件 | ✓ Good |
| 视觉复核默认并发设为 `6` | 2026-03-27 冷缓存 8 位达人 benchmark 下，`6` 在速度与稳定性间最优 | ✓ Good |
| 上游产物先写入筛号当前输入状态 | 当前最缺的是把已拿到的模板规则和达人名单真正喂给后端，而不是继续只停留在文件输出 | ✓ Good |
| `auth_not_found` 不再被当成“当前 apikey 配错”问题 | Phase 15 已用显式 `openai` 路径完成真实 bounded run，说明当前 key/base_url 读入与 live run 已可用 | ✓ Good |
| 用户验证过的 brand-keyword fast path 必须进入 repo-local 主线，而不是继续留在终端 sidecar | 它显著提升上游匹配速度与准确率，并改变当前 milestone 的 upstream contract | ✓ Good |
| 单入口 runner 必须显式暴露 `--matching-strategy` | 用户要的是“融合进总链路”，但不能静默替换 legacy route；operator 必须知道现在到底走的是哪条路径 | ✓ Good |
| shared-email 先 deterministic 判定，再把 unresolved tail 交给 LLM | 这样才能保持 fast-path 的速度与可解释性，不把可规则化的问题过早扔给模型 | ✓ Good |
| 下游 matching / review artifact 只能在上游输入未变化时复用 | `mail_sync` 是增量阶段，抓到新邮件后继续复用旧匹配结果会破坏 contract | ✓ Good |
| `keep-list` 继续作为 canonical upstream boundary | 这能让 Phase 17 的 dependency diagnostics 和 Phase 18 的真实 proof run 都建立在同一个稳定 handoff 上 | ✓ Good |
| 最终 E2E surface 采用 thin wrapper，而不是改写 upstream/downstream 业务逻辑 | 这样可以直接复用已验证 runner，同时给 operator 一个真正的单入口命令 | ✓ Good |
| Phase 19 的 Apify reliability contract 继续保留在 `backend/app.py` 和 runner summary surface，而不是急着拆成新模块 | 现有 guard、job 状态和 partial salvage 已经深度耦合在 backend runtime；先收口 reusable contract，再考虑物理拆分更稳妥 | ✓ Good |
| `scrape_failed` 只保留给“没有任何可用 scrape 输出”的情况 | 这样 operator 才能严格区分 true failure 和 partial salvage，不会把可恢复 run 误当成全损 | ✓ Good |
| final wrapper 必须把 `completed_with_partial_scrape` 当作可交付状态 | 顶层 operator surface 不能再把已有导出的 partial delivery run 扁平成 opaque `failed` | ✓ Good |

---
*Last updated: 2026-03-29 after v1.2.0 milestone archive*
