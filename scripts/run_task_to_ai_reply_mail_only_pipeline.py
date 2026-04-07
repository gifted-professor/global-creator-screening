from __future__ import annotations

import argparse
import json
import sys
from argparse import Namespace
from datetime import datetime
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from feishu_screening_bridge.feishu_api import DEFAULT_FEISHU_BASE_URL, FeishuOpenClient  # type: ignore
from feishu_screening_bridge.local_env import load_local_env  # type: ignore
from feishu_screening_bridge.task_upload_sync import download_task_upload_screening_assets  # type: ignore
from scripts.build_mail_only_payload_from_funnel_keep import run as build_mail_only_payload_run  # type: ignore
from scripts.build_mail_only_payload_from_manual_tail import run as build_manual_pool_payload_run  # type: ignore
from scripts.run_parsed_field_mail_funnel import run as run_parsed_field_mail_funnel  # type: ignore
from scripts.run_shared_mailbox_sync import run_shared_mailbox_sync  # type: ignore


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Given a task name, run shared mailbox sync -> full parsed-field match -> LLM -> Feishu AI reply writeback.",
    )
    parser.add_argument("--task-name", required=True, help="Task name, for example MINISO.")
    parser.add_argument("--env-file", default=".env", help="Env file path.")
    parser.add_argument("--local-date", default="", help="Local date in YYYY-MM-DD. Defaults to today in local timezone.")
    parser.add_argument("--timezone", default="Asia/Shanghai", help="Local timezone for the recall window.")
    parser.add_argument(
        "--task-upload-url",
        default="",
        help="Override task upload URL; otherwise read TASK_UPLOAD_URL from env.",
    )
    parser.add_argument(
        "--download-dir",
        default="downloads/task_upload_attachments",
        help="Task attachment download dir. Only used when --use-sending-list-match is enabled.",
    )
    parser.add_argument(
        "--shared-folder",
        default="其他文件夹/达人回信",
        help="Shared mailbox folder to sync.",
    )
    parser.add_argument("--shared-workers", type=int, default=5, help="Shared mailbox sync workers.")
    parser.add_argument(
        "--shared-sent-since",
        default="",
        help="Incremental sync lower bound. If empty, reuse wrapper default behavior.",
    )
    parser.add_argument("--reset-shared-sync-state", action="store_true", help="Reset shared mailbox IMAP state before syncing.")
    parser.add_argument("--llm-max-workers", type=int, default=12, help="Max concurrent LLM requests.")
    parser.add_argument("--llm-limit", type=int, default=0, help="Only review first N LLM candidates; 0 means all.")
    parser.add_argument(
        "--use-sending-list-match",
        action="store_true",
        help="Opt back into the legacy sending-list email match step.",
    )
    parser.add_argument("--upload-dry-run", action="store_true", help="Do not perform live Feishu writeback.")
    parser.add_argument(
        "--skip-manual-pool-upload",
        action="store_true",
        help="Do not upload manual-tail rows into the 转人工 pool.",
    )
    parser.add_argument(
        "--output-root",
        default="",
        help="Output root; defaults to temp/task_to_ai_reply_mail_only/<timestamp>_<task_name>",
    )
    return parser.parse_args()


def _safe_name(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return "task"
    cleaned = []
    for ch in text:
        if ch.isalnum() or ch in {"-", "_"}:
            cleaned.append(ch)
        else:
            cleaned.append("_")
    return "".join(cleaned).strip("_") or "task"


def _json_write(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _run_command(cmd: list[str]) -> dict[str, Any]:
    import subprocess

    completed = subprocess.run(
        cmd,
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
    )
    return {
        "command": cmd,
        "returncode": completed.returncode,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
    }


def _run_upload(
    *,
    env_file: str,
    payload_json: Path,
    linked_bitable_url: str,
    dry_run: bool,
    result_json_path: Path,
    result_xlsx_path: Path,
) -> dict[str, Any]:
    upload_cmd = [
        sys.executable,
        "-m",
        "feishu_screening_bridge",
        "upload-final-review-payload",
        "--env-file",
        env_file,
        "--payload-json",
        str(payload_json),
        "--linked-bitable-url",
        linked_bitable_url,
        "--result-json",
        str(result_json_path),
        "--result-xlsx",
        str(result_xlsx_path),
        "--json",
    ]
    if dry_run:
        upload_cmd.append("--dry-run")
    upload_result = _run_command(upload_cmd)
    if upload_result["returncode"] != 0:
        raise RuntimeError(upload_result["stderr"] or upload_result["stdout"] or "upload-final-review-payload failed")
    try:
        return json.loads(upload_result["stdout"])
    except json.JSONDecodeError as exc:  # noqa: BLE001
        raise RuntimeError(f"无法解析 upload-final-review-payload 输出: {exc}") from exc


def run(args: argparse.Namespace) -> dict[str, Any]:
    env_values = load_local_env(args.env_file)
    task_upload_url = str(args.task_upload_url or env_values.get("TASK_UPLOAD_URL") or env_values.get("FEISHU_SOURCE_URL") or "").strip()
    if not task_upload_url:
        raise ValueError("缺少 TASK_UPLOAD_URL；请在 .env 配置或显式传 --task-upload-url。")
    app_id = str(env_values.get("FEISHU_APP_ID") or "").strip()
    app_secret = str(env_values.get("FEISHU_APP_SECRET") or "").strip()
    if not app_id or not app_secret:
        raise ValueError("缺少 FEISHU_APP_ID / FEISHU_APP_SECRET。")

    local_date = str(args.local_date or "").strip() or datetime.now().astimezone().date().isoformat()
    run_root = (
        Path(args.output_root).expanduser().resolve()
        if str(args.output_root or "").strip()
        else (REPO_ROOT / "temp" / "task_to_ai_reply_mail_only" / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{_safe_name(args.task_name)}").resolve()
    )
    run_root.mkdir(parents=True, exist_ok=True)

    summary: dict[str, Any] = {
        "task_name": args.task_name,
        "local_date": local_date,
        "timezone": args.timezone,
        "sending_list_matching_enabled": bool(args.use_sending_list_match),
        "run_root": str(run_root),
        "steps": {},
        "artifacts": {},
    }

    client = FeishuOpenClient(
        app_id=app_id,
        app_secret=app_secret,
        base_url=str(env_values.get("FEISHU_OPEN_BASE_URL") or DEFAULT_FEISHU_BASE_URL),
    )

    assets = download_task_upload_screening_assets(
        client=client,
        task_upload_url=task_upload_url,
        task_name=args.task_name,
        download_dir=Path(args.download_dir).expanduser().resolve(),
        download_template=False,
        download_sending_list=bool(args.use_sending_list_match),
    )
    summary["steps"]["task_assets"] = assets
    sending_list_path_raw = str(assets.get("sendingListDownloadedPath") or "").strip()
    sending_list_path = Path(sending_list_path_raw).expanduser().resolve() if sending_list_path_raw else None
    linked_bitable_url = str(assets.get("linkedBitableUrl") or "").strip()
    summary["artifacts"]["sending_list_path"] = str(sending_list_path) if sending_list_path is not None else ""
    summary["artifacts"]["linked_bitable_url"] = linked_bitable_url

    sync_args = Namespace(
        env_file=args.env_file,
        account_email="",
        account_auth_code="",
        folder=args.shared_folder,
        data_dir="",
        db_path="",
        raw_dir="",
        summary_json=str((run_root / "shared_mailbox_sync_summary.json").resolve()),
        sent_since=args.shared_sent_since,
        limit=0,
        reset_state=bool(args.reset_shared_sync_state),
        workers=int(args.shared_workers),
    )
    sync_result = run_shared_mailbox_sync(sync_args)
    summary["steps"]["shared_mail_sync"] = sync_result
    db_path = Path(str(sync_result["db_path"])).expanduser().resolve()
    summary["artifacts"]["shared_mail_db_path"] = str(db_path)

    funnel_args = Namespace(
        env_file=args.env_file,
        db_path=str(db_path),
        input_workbook=str(sending_list_path) if sending_list_path is not None else "",
        keyword=args.task_name,
        local_date=local_date,
        timezone=args.timezone,
        output_prefix=str((run_root / f"{_safe_name(args.task_name)}_parsed_field_funnel").resolve()),
        base_url="",
        api_key="",
        model="",
        wire_api="",
        llm_max_workers=int(args.llm_max_workers),
        llm_limit=int(args.llm_limit),
    )
    funnel_result = run_parsed_field_mail_funnel(funnel_args)
    summary["steps"]["parsed_field_funnel"] = funnel_result
    keep_workbook = Path(str(funnel_result["keep_xlsx_path"])).expanduser().resolve()
    manual_tail_workbook = Path(str(funnel_result["manual_tail_xlsx_path"])).expanduser().resolve()
    summary["artifacts"]["funnel_keep_xlsx"] = str(keep_workbook)
    summary["artifacts"]["funnel_manual_tail_xlsx"] = str(manual_tail_workbook)
    summary["artifacts"]["funnel_summary_json"] = str((run_root / f"{_safe_name(args.task_name)}_parsed_field_funnel_summary.json").resolve())

    task_owner_payload_json = run_root / f"{_safe_name(args.task_name)}_task_owner_payload.json"
    _json_write(
        task_owner_payload_json,
        {
            "task_owner": {
                "responsible_name": "",
                "employee_name": "",
                "employee_id": "",
                "employee_record_id": "",
                "employee_email": "",
                "owner_name": "",
                "linked_bitable_url": linked_bitable_url,
                "task_name": args.task_name,
            }
        },
    )

    build_args = Namespace(
        keep_workbook=str(keep_workbook),
        task_owner_payload_json=str(task_owner_payload_json),
        output_prefix=str((run_root / f"{_safe_name(args.task_name)}_mail_only").resolve()),
    )
    build_result = build_mail_only_payload_run(build_args)
    summary["steps"]["build_mail_only_payload"] = build_result
    payload_json = Path(str(build_result["payload_path"])).expanduser().resolve()
    summary["artifacts"]["mail_only_payload_json"] = str(payload_json)
    summary["artifacts"]["mail_only_workbook_xlsx"] = str(build_result["workbook_path"])
    parsed_upload_result = _run_upload(
        env_file=args.env_file,
        payload_json=payload_json,
        linked_bitable_url=linked_bitable_url,
        dry_run=bool(args.upload_dry_run),
        result_json_path=(run_root / "feishu_upload_local_archive" / "mail_only_upload_result.json").resolve(),
        result_xlsx_path=(run_root / "feishu_upload_local_archive" / "mail_only_upload_result.xlsx").resolve(),
    )
    summary["steps"]["feishu_upload"] = parsed_upload_result
    summary["artifacts"]["upload_result_json"] = parsed_upload_result.get("result_json_path", "")
    summary["artifacts"]["upload_result_xlsx"] = parsed_upload_result.get("result_xlsx_path", "")

    manual_row_count = int(funnel_result.get("manual_row_count") or 0)
    manual_pool_enabled = manual_row_count > 0 and not bool(args.skip_manual_pool_upload)
    summary["manual_pool_enabled"] = manual_pool_enabled
    summary["artifacts"]["manual_pool_payload_json"] = ""
    summary["artifacts"]["manual_pool_workbook_xlsx"] = ""
    summary["artifacts"]["manual_pool_upload_result_json"] = ""
    summary["artifacts"]["manual_pool_upload_result_xlsx"] = ""

    if manual_pool_enabled:
        manual_build_args = Namespace(
            manual_tail_workbook=str(manual_tail_workbook),
            task_owner_payload_json=str(task_owner_payload_json),
            task_name=args.task_name,
            local_date=local_date,
            output_prefix=str((run_root / f"{_safe_name(args.task_name)}_manual_pool").resolve()),
        )
        manual_build_result = build_manual_pool_payload_run(manual_build_args)
        summary["steps"]["build_manual_pool_payload"] = manual_build_result
        manual_payload_json = Path(str(manual_build_result["payload_path"])).expanduser().resolve()
        summary["artifacts"]["manual_pool_payload_json"] = str(manual_payload_json)
        summary["artifacts"]["manual_pool_workbook_xlsx"] = str(manual_build_result["workbook_path"])

        manual_upload_result = _run_upload(
            env_file=args.env_file,
            payload_json=manual_payload_json,
            linked_bitable_url=linked_bitable_url,
            dry_run=bool(args.upload_dry_run),
            result_json_path=(run_root / "feishu_upload_local_archive" / "manual_pool_upload_result.json").resolve(),
            result_xlsx_path=(run_root / "feishu_upload_local_archive" / "manual_pool_upload_result.xlsx").resolve(),
        )
        summary["steps"]["manual_pool_upload"] = manual_upload_result
        summary["artifacts"]["manual_pool_upload_result_json"] = manual_upload_result.get("result_json_path", "")
        summary["artifacts"]["manual_pool_upload_result_xlsx"] = manual_upload_result.get("result_xlsx_path", "")

    summary_path = run_root / "summary.json"
    _json_write(summary_path, summary)
    summary["summary_json_path"] = str(summary_path)
    return summary


def main() -> int:
    args = _parse_args()
    result = run(args)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
