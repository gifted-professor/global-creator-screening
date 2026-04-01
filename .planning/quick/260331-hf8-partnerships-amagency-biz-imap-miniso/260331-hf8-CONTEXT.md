# Quick Task 260331-hf8: 验证共享邮箱 partnerships@amagency.biz 的 IMAP 可用性并定位无法抓取 MINISO 邮件的具体原因 - Context

**Gathered:** 2026-03-31
**Status:** Ready for execution

## Task Boundary

- 直接验证共享邮箱 `partnerships@amagency.biz / xYeGKyNmK5jFN7Y2` 是否还能通过 IMAP 工作
- 确认 `MINISO` 邮件到底是不存在，还是只是目录结构和我们之前的假设不一致
- 给出当前共享邮箱路线的真实阻塞点或可用路径

## Decisions

- 先绕开 task-upload 的“员工邮箱优先”逻辑，直接对共享邮箱做底层 IMAP 诊断
- 同时验证 `imap.qq.com` 和 `imap.exmail.qq.com`，避免把主机问题误判成账号问题
- 不只看 `LOGIN`，还要看 `LIST / SELECT / STATUS` 和真实抓信抽样结果
- 以 repo-local `email_sync` 抓信抽样结果作为最终判断依据，而不是只看手写 IMAP 命令
