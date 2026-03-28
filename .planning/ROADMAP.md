# Roadmap: chuhaihai

## Milestones

- ✅ **v1.0.0 Consolidated Local Creator Screening Pipeline** — Phases 1-13 shipped 2026-03-28. Archive: `.planning/milestones/v1.0.0-ROADMAP.md`
- 🚧 **v1.1.0 Visual Provider Reliability and Downstream Hardening** — Phases 14-15 planned

## Overview

`v1.1.0` 的目标不是再扩主链，而是把当前已打通的 keep-list -> visual-review -> export 路径收稳。重点是修复视觉 provider 鉴权、补预检与可诊断性，并留下一轮 non-error 的真实 bounded validation。

## Phases

- [ ] **Phase 14: Stabilize visual provider config and preflight** - 修复视觉 provider 鉴权解析、预检与错误可定位性
- [ ] **Phase 15: Validate non-error bounded visual review flow** - 用真实 keep-list 跑通一轮 non-error 的 bounded visual review 并收口文档

## Progress

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 14. Stabilize visual provider config and preflight | 0/0 | Not Started | |
| 15. Validate non-error bounded visual review flow | 0/0 | Not Started | |

### Phase 14: Stabilize visual provider config and preflight

**Goal**: 修复当前视觉 provider 的鉴权解析路径，让 bounded run 在真正进视觉前就能暴露配置问题，并把 provider 诊断信息写进 summary。
**Depends on**: Phase 13
**Requirements**: [VIS-01, VIS-02, SCR-03]
**Success Criteria** (what must be TRUE):
  1. 当前仓库能稳定解析视觉复核使用的 provider / model / base URL / auth 来源，不要求改业务代码
  2. 缺失或错误鉴权会在 preflight 或早期执行阶段给出明确错误，不再只表现为尾部 `auth_not_found`
  3. keep-list downstream summary 会记录视觉 provider 相关诊断字段，便于人工定位
**Plans**: 0 plans

### Phase 15: Validate non-error bounded visual review flow

**Goal**: 基于修复后的 provider 配置，对真实 keep-list 执行一轮 non-error 的 bounded visual review，并把 operator path 文档化。
**Depends on**: Phase 14
**Requirements**: [VIS-03, DOC-01]
**Success Criteria** (what must be TRUE):
  1. 真实 keep-list bounded run 不再返回 `auth_not_found`
  2. visual-review 结果在 summary 和导出文件里表现为真实业务结果，而不是统一 `Error`
  3. README 与 planning 收尾文档给出可复跑的验证命令、前置配置和产物路径
**Plans**: 0 plans
