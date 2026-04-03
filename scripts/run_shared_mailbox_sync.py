from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import fcntl


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from email_sync.config import Settings, _load_dotenv, _get_value  # type: ignore
from email_sync.db import Database
from email_sync.date_windows import resolve_sync_sent_since
from email_sync.imap_sync import sync_mailboxes


DEFAULT_FOLDER = "其他文件夹/邮件备份"
DEFAULT_DATA_DIR = REPO_ROOT / "data" / "shared_mailbox"
DEFAULT_LOCK_NAME = ".sync.lock"


def iso_now() -> str:
    return datetime.now().astimezone().isoformat()


def _preferred_env_value(env_values: dict[str, str], *keys: str, default: str = "") -> str:
    for key in keys:
        value = str(_get_value(key, env_values, "") or "").strip()
        if value:
            return value
    return default


def _build_settings(args: argparse.Namespace) -> Settings:
    env_values = _load_dotenv(Path(args.env_file))
    base = Settings.from_environment(args.env_file, require_credentials=False)

    account_email = (
        str(args.account_email or "").strip()
        or _preferred_env_value(env_values, "SHARED_EMAIL_ACCOUNT", "EMAIL_ACCOUNT")
    )
    auth_code = (
        str(args.account_auth_code or "").strip()
        or _preferred_env_value(env_values, "SHARED_EMAIL_AUTH_CODE", "EMAIL_AUTH_CODE")
    )
    if not account_email:
        raise ValueError("缺少共享邮箱账号：请传 --account-email 或在 .env 里配置 SHARED_EMAIL_ACCOUNT/EMAIL_ACCOUNT")
    if not auth_code:
        raise ValueError("缺少共享邮箱 IMAP 授权码：请传 --account-auth-code 或在 .env 里配置 SHARED_EMAIL_AUTH_CODE/EMAIL_AUTH_CODE")

    data_dir = Path(args.data_dir).expanduser().resolve() if args.data_dir else DEFAULT_DATA_DIR.resolve()
    db_path = Path(args.db_path).expanduser().resolve() if args.db_path else (data_dir / "email_sync.db")
    raw_dir = Path(args.raw_dir).expanduser().resolve() if args.raw_dir else (data_dir / "raw")

    return Settings(
        account_email=account_email,
        auth_code=auth_code,
        imap_host=base.imap_host,
        imap_port=base.imap_port,
        data_dir=data_dir,
        db_path=db_path,
        raw_dir=raw_dir,
        mail_folders=[str(args.folder or DEFAULT_FOLDER).strip()],
        readonly=base.readonly,
    )


def _message_count(db_path: Path) -> int:
    if not db_path.exists():
        return 0
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        tables = {row[0] for row in cur.execute("select name from sqlite_master where type='table'")}
        if "messages" not in tables:
            return 0
        value = cur.execute("select count(*) from messages").fetchone()
        return int(value[0] if value else 0)
    finally:
        conn.close()


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _parse_iso_datetime(value: str | None) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone() if parsed.tzinfo is not None else parsed


def resolve_shared_mailbox_sent_since(
    raw_value: str | None,
    *,
    today: date | None = None,
    last_sync_completed_at: str | None = None,
) -> tuple[date, str]:
    explicit = str(raw_value or "").strip()
    if explicit:
        return resolve_sync_sent_since(explicit, today=today), "cli"

    last_completed = _parse_iso_datetime(last_sync_completed_at)
    if last_completed is not None:
        return last_completed.date() - timedelta(days=1), "last_successful_sync_backfill"

    return resolve_sync_sent_since(None, today=today), "default_today_only"


@contextmanager
def _single_instance_lock(lock_path: Path):
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = lock_path.open("w", encoding="utf-8")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        handle.write(f"pid={os.getpid()}\n")
        handle.write(f"started_at={iso_now()}\n")
        handle.flush()
        yield
    finally:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        except Exception:
            pass
        handle.close()


def run_shared_mailbox_sync(args: argparse.Namespace) -> dict[str, Any]:
    settings = _build_settings(args)
    settings.ensure_directories()

    summary_path = (
        Path(args.summary_json).expanduser().resolve()
        if args.summary_json
        else (settings.data_dir / "summary.json").resolve()
    )
    lock_path = (settings.data_dir / DEFAULT_LOCK_NAME).resolve()

    before_count = _message_count(settings.db_path)
    summary: dict[str, Any] = {
        "started_at": iso_now(),
        "finished_at": "",
        "status": "running",
        "env_file": str(Path(args.env_file).expanduser().resolve()),
        "account_email": settings.account_email,
        "folder": settings.mail_folders[0],
        "imap_host": settings.imap_host,
        "imap_port": settings.imap_port,
        "data_dir": str(settings.data_dir),
        "db_path": str(settings.db_path),
        "raw_dir": str(settings.raw_dir),
        "summary_json": str(summary_path),
        "lock_path": str(lock_path),
        "sent_since": str(args.sent_since or "").strip(),
        "limit": int(args.limit) if args.limit else 0,
        "reset_state": bool(args.reset_state),
        "workers": int(args.workers),
        "message_count_before": before_count,
        "message_count_after": before_count,
        "fetched_count": 0,
        "results": [],
        "error": "",
    }
    _write_json(summary_path, summary)

    try:
        with _single_instance_lock(lock_path):
            db = Database(settings.db_path)
            try:
                db.init_schema()
                state = db.get_sync_state(settings.account_email, settings.mail_folders[0])
                resolved_sent_since, sent_since_source = resolve_shared_mailbox_sent_since(
                    args.sent_since,
                    last_sync_completed_at=str(state["last_sync_completed_at"] or "") if state else "",
                )
                summary["resolved_sent_since"] = resolved_sent_since.isoformat()
                summary["sent_since_source"] = sent_since_source
                _write_json(summary_path, summary)
                results = sync_mailboxes(
                    settings,
                    db,
                    requested_folders=settings.mail_folders,
                    limit=args.limit or None,
                    reset_state=bool(args.reset_state),
                    workers=max(1, int(args.workers)),
                    sent_since=resolved_sent_since,
                )
            finally:
                db.close()
    except BlockingIOError as exc:
        summary["finished_at"] = iso_now()
        summary["status"] = "skipped_locked"
        summary["error"] = f"另一个 shared mailbox sync 正在运行: {exc}"
        _write_json(summary_path, summary)
        return summary
    except Exception as exc:  # noqa: BLE001
        summary["finished_at"] = iso_now()
        summary["status"] = "failed"
        summary["error"] = str(exc)
        _write_json(summary_path, summary)
        raise

    after_count = _message_count(settings.db_path)
    summary["finished_at"] = iso_now()
    summary["status"] = "completed"
    summary["message_count_after"] = after_count
    summary["fetched_count"] = max(0, after_count - before_count)
    summary["results"] = [
        {
            "folder_name": result.folder_name,
            "fetched": result.fetched,
            "skipped_state_advance": result.skipped_state_advance,
            "last_seen_uid": result.last_seen_uid,
            "uidvalidity": result.uidvalidity,
            "message_count_on_server": result.message_count_on_server,
        }
        for result in results
    ]
    _write_json(summary_path, summary)
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run shared mailbox incremental sync with a stable wrapper.")
    parser.add_argument("--env-file", default=".env", help="env 文件路径，默认 ./.env")
    parser.add_argument("--account-email", default="", help="共享邮箱账号；默认优先读 SHARED_EMAIL_ACCOUNT，再回退 EMAIL_ACCOUNT")
    parser.add_argument("--account-auth-code", default="", help="共享邮箱 IMAP 授权码；默认优先读 SHARED_EMAIL_AUTH_CODE，再回退 EMAIL_AUTH_CODE")
    parser.add_argument("--folder", default=DEFAULT_FOLDER, help=f"共享文件夹，默认 {DEFAULT_FOLDER}")
    parser.add_argument("--data-dir", default="", help="共享邮箱数据目录；默认 data/shared_mailbox")
    parser.add_argument("--db-path", default="", help="SQLite 路径；默认 <data-dir>/email_sync.db")
    parser.add_argument("--raw-dir", default="", help="raw 邮件目录；默认 <data-dir>/raw")
    parser.add_argument("--summary-json", default="", help="summary 输出路径；默认 <data-dir>/summary.json")
    parser.add_argument(
        "--sent-since",
        default="",
        help="只抓这个日期及之后的邮件，格式 YYYY-MM-DD；不传时优先按上次成功同步时间自动回补 1 天。",
    )
    parser.add_argument("--limit", type=int, default=0, help="只抓最新 N 封用于测试，不推进游标")
    parser.add_argument("--reset-state", action="store_true", help="忽略本地游标，重新扫描")
    parser.add_argument("--workers", type=int, default=1, help="并发抓取 worker 数，默认 1")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    summary = run_shared_mailbox_sync(args)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if summary.get("status") == "completed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
