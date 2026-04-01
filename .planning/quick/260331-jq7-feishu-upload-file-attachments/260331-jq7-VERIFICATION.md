status: passed

# Quick Task 260331-jq7 Verification

## Verified

- `python3 -m py_compile backend/final_export_merge.py feishu_screening_bridge/bitable_upload.py feishu_screening_bridge/feishu_api.py` 通过
- `python3 -m unittest tests.test_final_export_merge tests.test_bitable_upload -v` 通过，`6 tests, OK`
- 新增测试已覆盖：
  - payload 会保留 `__last_mail_raw_path`
  - payload 会保留 `__feishu_attachment_local_paths`
  - payload 顶层会保留 `__feishu_shared_attachment_local_paths`
  - 飞书 uploader 会把本地 `.eml` 文件转成附件字段写入请求 payload

## Notes

- 当前工作区下没有 `backend/.venv/bin/python`，所以这次定向回归用的是系统 `python3`
- 这次没有对真实客户飞书表做 live attachment upload；重点是把 repo-local payload contract 和 uploader 通道补齐
