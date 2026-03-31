# Quick Task 260331-hf8 Summary

## Task

验证共享邮箱 `partnerships@amagency.biz` 的 IMAP 可用性，并查清为什么之前看起来抓不到 `MINISO` 邮件。

## What I Found

### 1. 问题不在账号本身，而在主机和目录假设

原始 IMAP 诊断结果分成两层：

- `imap.qq.com`
  - 登录失败，返回标准 QQ 邮箱 `Login fail...` 错误
- `imap.exmail.qq.com`
  - 登录成功
  - `CAPABILITY`、`NAMESPACE`、`STATUS`、`SELECT INBOX` 都能正常工作

所以这组共享邮箱凭据不是完全失效，而是应该走企业邮主机：

```text
imap.exmail.qq.com:993
```

### 2. 共享邮箱里没有项目专属 `MINISO` 文件夹，但有一个超大的备份池

把企业邮 IMAP 返回的目录做 UTF-7 解码后，当前共享邮箱能看到：

- `INBOX`
- `Sent Messages`
- `Drafts`
- `Deleted Messages`
- `Junk`
- 多个 `其他文件夹/<员工名>_Inbox|Sent`
- `其他文件夹/邮件备份`

关键点是：

- IMAP 暴露的是 `其他文件夹/邮件备份`
- 不是网页里那个带数量后缀的 `邮件备份(30316)`

当前 `STATUS` 结果里：

- `INBOX`：`163` 封
- `其他文件夹/邮件备份`：`31195+` 封

也就是说，这个共享邮箱的“最全”邮件并不在项目专属目录里，而是在统一备份目录里。

### 3. `MINISO` 邮件在共享邮箱里是存在的，而且能被真实抓下来

我直接用 repo-local `email_sync` 从：

```text
其他文件夹/邮件备份
```

做了一次最近三个月的真实抽样抓信：

- 抽样窗口：`since 2025-12-27`
- 抽样上限：`400`
- 实抓结果：`400/400` 成功
- 本地库路径：
  [email_sync.db](/Users/a1234/Desktop/Coding/chuhaihai/temp/260331_partnerships_miniso_recent_probe/email_sync.db)

抽样后的本地搜索结果：

- `400` 封样本里
- `43` 封正文/摘要/标题里命中 `miniso`
- 其中 `42` 封标题直接含 `MINISO`

真实样本主题包括：

- `Re: Paid Collaboration Opportunity with MINISO`
- `Re: Paid Collab with MINISO – In-Store Check-In Campaign`
- `Re: New Paid Collaboration Opportunity: MINISO Physical Store Check-in Video`

这说明共享邮箱并不是抓不到 `MINISO` 邮件，而是：

- 这些邮件现在混在 `其他文件夹/邮件备份` 里
- 不是按 `MINISO` 目录独立暴露

## Conclusion

当前共享邮箱路线的真实情况是：

1. 账号可用，但必须走 `imap.exmail.qq.com`
2. 邮件存在，但主要沉在 `其他文件夹/邮件备份`
3. 之前“抓不到”的根因不是 `MINISO` 邮件不存在，而是我们之前按“项目文件夹 + 错主机”去理解它

所以如果后面要用这个共享邮箱跑 `MINISO`，当前最靠谱的做法不是继续找 `MINISO` 文件夹，而是：

- 直接把共享邮箱的抓取目录切到 `其他文件夹/邮件备份`
- 再在本地用任务名/品牌关键词做二次过滤

## Current Recommendation

当前主线建议：

- 稳定生产仍优先员工邮箱专属目录
- 如果要启用共享邮箱最全备份路线，应显式使用：
  - `IMAP_HOST=imap.exmail.qq.com`
  - `MAIL_FOLDERS=其他文件夹/邮件备份`

这样共享邮箱路线就有真实可抓的入口，不需要继续假设存在 `MINISO` 专属文件夹。
