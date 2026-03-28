# chuhaihai

## What This Is

`chuhaihai` 是一个把创作者筛选相关本地工具收拢到同一仓库的整合工程。`v1.0.0` 已把飞书桥接、任务驱动邮件抓取、模板解析、达人匹配与报价抽取、duplicate review，以及 `筛号` 后端主链统一到当前仓库，并保留对现有本地工作流的兼容。

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

### Active

- [ ] 保留与外部全量 `email` 项目的兼容，避免一次性重写现有导入链路
- [ ] 把报价结果也正式接入 `筛号` 或其他后续链路
- [ ] 用当前仓库已写入的 `MINISO` 输入状态继续跑下游抓取 / 预筛 / 视觉复核
- [ ] 让当前仓库成为统一的 `.env` 承载位置，减少跨项目手工同步

### Out of Scope

- 彻底重写 `email_sync` 或 `筛号` 业务逻辑 — 当前目标是迁移与整合，不是重构旧系统
- 云端部署或生产化打包 — 目前是本地工作流整合

## Context

当前仓库起步时只有 `.env`、`.env.example`、`.gitignore` 和 GSD 工具目录，没有应用代码。`v1.0.0` 完成了从 `上传飞书`、`抓取邮件`、`模板解析` 和 `筛号` sibling 目录向当前仓库的主线整合。现在本仓库已经能完成两条真实业务链：

- `任务上传 -> 员工信息 -> 模板下载/解析 -> 按任务抓取邮箱文件夹邮件 -> 达人匹配 -> duplicate review -> keep-list`
- `keep-list -> 筛号运行态 -> Apify 抓取 -> 预筛 -> 视觉复核调用 -> 导出`

真实 `MINISO` 验证已经证明 keep-list 主线能下探到导出，但当前视觉 provider 仍可能因为鉴权问题返回 `auth_not_found`。桥接层里依赖 workbook / dashboard / project-home 的旧流程，暂时仍默认指向外部全量 `email` 项目。

## Current State

- `v1.0.0` 已交付一个 repo-local 的创作者筛选主线，不再需要跨多个 sibling 工程手工拼核心步骤
- `MINISO` 已完成真实 duplicate review 产物链，并生成最终 keep workbook
- keep-list 已经正式接入当前仓库 `筛号` 主链，并做过 bounded live downstream validation
- 当前剩余主要问题不是链路缺失，而是视觉 provider 鉴权与少数历史兼容点

## Next Milestone Goals

- 修复视觉 provider 鉴权，让 bounded visual review 不再返回 `auth_not_found`
- 决定是否把报价结果正式接入 `筛号` 运行态或最终导出链
- 继续减少对外部全量 `email` 项目的 workbook / dashboard / project-home 依赖
- 视业务需要补做 milestone audit、cross-phase verification 或后续生产化整理

## Current Milestone: v1.1.0 Visual Provider Reliability and Downstream Hardening

**Goal:** 收稳当前 keep-list -> visual-review -> export 主线，优先修掉视觉 provider 鉴权和排障体验。

**Target features:**
- 让视觉 provider 的鉴权解析路径可诊断、可预检
- 让 bounded visual review 不再因 `auth_not_found` 失败
- 把视觉配置与验证命令写进 repo 文档和 summary 产物

## Constraints

- **Compatibility**: 迁移后仍需兼容现有 sibling 项目结构 — 旧的 `抓取邮件/email_sync` 目录短期内仍是依赖方
- **Scope**: 只搬运已经验证过的飞书桥接能力 — 暂不顺手扩展新功能
- **Security**: `.env.example` 不应继续携带真实密钥 — 示例文件只保留结构和安全默认值

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

---
*Last updated: 2026-03-28 after v1.1.0 milestone initialization*
