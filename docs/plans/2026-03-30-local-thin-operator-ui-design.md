# 2026-03-30 Local Thin Operator UI Design

## Goal

在当前 repo-local、task-driven screening pipeline 之上加一层本地可视化控制面板，让 operator 不用手敲长命令，也能完成：

1. 读取飞书任务列表
2. 选择任务并发起 run
3. 观察当前 stage / 错误 / summary
4. 打开最终导出 Excel

## Options Considered

### Option A: Backend-served local HTML page

- 复用现有 `backend/app.py`
- 用 Flask 直接提供 `/operator` 页面和 `api/operator/*` 接口
- 后端直接启动 canonical runner 子进程并维护轻量 run registry
- 页面轮询 run 状态与 summary/artifact

**Pros**

- 复用现有 backend runtime、job contract 和模板服务
- 不需要额外 dev server 或前端打包链
- 最接近“本地工具面板”这个目标

**Cons**

- `backend/app.py` 会继续变大
- UI 交互复杂度要控制在 MVP 范围内

### Option B: Static HTML + filesystem polling

- 独立 HTML 只读取磁盘上的 `summary.json`
- 用 shell/手工命令启动 run

**Pros**

- 实现很快

**Cons**

- 不能真正从页面发起 run
- 用户体验仍然依赖终端，不符合 operator UI 的核心价值

### Option C: Separate frontend dev server

- 新建前端工程，通过 HTTP 调现有 backend

**Pros**

- 视觉空间更大

**Cons**

- 增加前端工程、依赖和运行复杂度
- 对现在的 local-only MVP 明显过重

## Recommendation

选择 **Option A: backend-served local HTML page**。

原因：这版需求的核心不是“做一个前端项目”，而是“给现有主链补一层可视化操作入口”。既然 `backend/app.py` 已经有 API、job status、artifact download 和一个现成模板页，那最稳的做法就是继续沿这个 runtime 直接长出 `/operator`。

## MVP Scope

1. 提供任务列表接口，从 task-upload 读取最近可跑任务
2. 提供 run 启动接口，调用 canonical `scripts/run_task_upload_to_final_export_pipeline.py`
3. 提供 run 轮询接口，暴露 stage、status、summary path、output root、process metadata
4. 页面展示当前 run 状态、错误、summary 路径和最终导出 Excel 链接
5. 默认参数走已验证组合：
   - local-only
   - canonical runner
   - 已验证 provider
   - 已验证老邮箱 route

## Non-goals

- 云端部署
- 多用户权限 / 登录
- 飞书结果回写
- 重写 orchestration engine
- 修复统一邮箱 `邮件备份(30316)` 的 IMAP 可见性

## Assumptions

- 单操作者、本地使用
- operator 已配置好 `.env` / `.env.local`
- 第一版优先服务已验证路径，不追求所有 provider / mailbox 策略都可配

## Candidate File Surface

- `backend/app.py`
- `backend/templates/operator_console.html`
- `scripts/run_task_upload_to_final_export_pipeline.py`
- `feishu_screening_bridge/task_upload_sync.py`
- `tests/test_visual_provider_diagnostics.py`
- `tests/test_task_upload_to_final_export_pipeline.py`

## Canonical References

- `.planning/PROJECT.md`
- `.planning/REQUIREMENTS.md`
- `.planning/ROADMAP.md`
- `.planning/STATE.md`
- `temp/260330_old_mail_live/run/summary.json`
- `temp/260330_old_mail_live/run/downstream/summary.json`
