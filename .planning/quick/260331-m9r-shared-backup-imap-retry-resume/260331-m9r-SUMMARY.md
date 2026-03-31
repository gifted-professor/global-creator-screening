# Quick Task 260331-m9r Summary

## Task

修复共享邮箱 `其他文件夹/邮件备份` 在企业邮 IMAP 下偶发：

```text
command: UID => socket error: EOF
```

导致 `MINISO` 今天窗口抓信直接中断的问题。

目标是让共享备份目录遇到 `EOF` 时：

- 自动重连
- 从当前已成功的 UID 继续
- 不从头整段重扫

## What Changed

### 1. `imap_sync` 现在识别 retryable IMAP 错误

`email_sync/imap_sync.py` 现在会把以下错误视为可重试：

- `socket error: EOF`
- `connection aborted`
- `server closed connection`
- `timed out`
- `system busy`

### 2. 共享备份目录支持文件夹级重连重试

对于 `其他文件夹/邮件备份` 这类共享备份目录，当前会启用额外的文件夹级 retry。

策略是：

- 普通目录：保持单次执行
- 共享备份目录：最多 `3` 次尝试

如果在 `SELECT / SEARCH / FETCH` 过程中出现 retryable IMAP 错误，就：

1. 关闭当前连接
2. 重新登录
3. 从上一次 checkpoint 继续

### 3. 抓取过程中增加 checkpoint

之前 `last_seen_uid` 只在整个文件夹收尾时更新，所以中途掉线会导致下一次从老位置重扫。

现在：

- 单线程抓取每成功一封就 checkpoint
- 并发批量抓取每成功一批就 checkpoint

这样共享备份目录一旦中途断线，重连后能直接从最近成功的 UID 继续。

### 4. 并发批量抓取遇到整批 retryable 错误会升级成重连

如果某个 batch 没有任何成功项，而且错误都是 retryable IMAP 错误，就不再把整批 UID 当永久失败，而是直接触发文件夹级重连。

这能避免共享邮箱大目录里“整批挂掉后全记成坏 UID”的情况。

## Outcome

这次修完后，共享邮箱主线已经不是“碰到 EOF 就直接掉”。

至少在 bounded live probe 上，`MINISO` 现在可以在：

- 共享邮箱
- 今天窗口
- `其他文件夹/邮件备份`

这条路上稳定收口。
