status: passed

# Quick Task 260331-hf8 Verification

## Verified

- `partnerships@amagency.biz` 在 `imap.exmail.qq.com` 上可以成功登录
- 企业邮 IMAP 真实暴露出了 `其他文件夹/邮件备份`
- `其他文件夹/邮件备份` 当前有 `31195+` 封邮件
- 通过 repo-local `email_sync` 对该目录做最近三个月 `400` 封抽样抓取时，`400/400` 成功
- 抽样本地库中已确认存在 `MINISO` 邮件样本

## Notes

- `imap.qq.com` 对这组共享邮箱凭据仍然登录失败，因此不能作为这条共享邮箱主机
- 网页里显示的 `邮件备份(30316)` 在 IMAP 下实际暴露名为 `其他文件夹/邮件备份`
- 这次没有修改业务代码；结论来自原始 IMAP 诊断和 repo-local 抽样抓信
