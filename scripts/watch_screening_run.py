from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any


def _parse_iso_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def load_progress_snapshot(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("progress payload must be a JSON object")
    return payload


def resolve_progress_path(*, run_dir: Path | None, progress_json: Path | None) -> Path:
    if progress_json is not None:
        return progress_json.expanduser().resolve()
    if run_dir is None:
        raise ValueError("must provide --run-dir or --progress-json")
    return (run_dir.expanduser().resolve() / "progress.json").resolve()


def format_progress_snapshot(
    payload: dict[str, Any],
    *,
    now: datetime | None = None,
    stalled_after_seconds: float = 120.0,
) -> str:
    if not payload:
        return "progress.json is empty"

    now_dt = now or datetime.now().astimezone()
    heartbeat_dt = _parse_iso_datetime(payload.get("last_heartbeat_at"))
    heartbeat_age = None
    if heartbeat_dt is not None:
        heartbeat_age = max(0, int((now_dt - heartbeat_dt.astimezone(now_dt.tzinfo)).total_seconds()))

    status = str(payload.get("status") or "").strip() or "unknown"
    stage = str(payload.get("stage") or "").strip() or "unknown"
    platform = str(payload.get("platform") or "").strip() or "-"
    phase = str(payload.get("phase") or "").strip() or "-"
    current_batch = int(payload.get("current_batch") or 0)
    batch_count = int(payload.get("batch_count") or 0)
    processed = int(payload.get("processed") or 0)
    total = int(payload.get("total") or 0)
    last_item = str(payload.get("last_item") or "").strip()
    last_log_line = str(payload.get("last_log_line") or "").strip()
    terminal = bool(str(payload.get("finished_at") or "").strip()) or status in {
        "completed",
        "failed",
        "vision_probe_failed",
        "vision_probe_only",
        "staged_only",
        "dry_run_only",
        "scrape_failed",
        "completed_with_quality_warnings",
    }
    stalled = bool(
        heartbeat_age is not None
        and not terminal
        and heartbeat_age > max(1.0, float(stalled_after_seconds))
    )
    state = "stalled" if stalled else ("completed" if terminal else "running")

    parts = [
        f"status={status}",
        f"state={state}",
        f"stage={stage}",
        f"platform={platform}",
        f"phase={phase}",
    ]
    if batch_count > 0:
        parts.append(f"batch={current_batch}/{batch_count}")
    if total > 0:
        parts.append(f"processed={processed}/{total}")
    elif processed > 0:
        parts.append(f"processed={processed}")
    if heartbeat_age is not None:
        parts.append(f"heartbeat_age={heartbeat_age}s")
    if last_item:
        parts.append(f"last_item={last_item}")
    if last_log_line:
        parts.append(f"log={last_log_line}")
    return " | ".join(parts)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Watch keep-list screening progress.json")
    parser.add_argument("--run-dir", type=Path, default=None, help="Run directory containing progress.json")
    parser.add_argument("--progress-json", type=Path, default=None, help="Explicit progress.json path")
    parser.add_argument("--interval", type=float, default=15.0, help="Polling interval in seconds")
    parser.add_argument("--stalled-after", type=float, default=120.0, help="Mark run stalled after N seconds")
    parser.add_argument("--once", action="store_true", help="Print one snapshot and exit")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    try:
        progress_path = resolve_progress_path(run_dir=args.run_dir, progress_json=args.progress_json)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    while True:
        if not progress_path.exists():
            print(f"progress file not found: {progress_path}")
            return 1 if args.once else 0
        try:
            snapshot = load_progress_snapshot(progress_path)
        except Exception as exc:  # noqa: BLE001
            print(f"failed to read progress file: {exc}", file=sys.stderr)
            return 1

        print(format_progress_snapshot(snapshot, stalled_after_seconds=args.stalled_after), flush=True)
        if args.once:
            return 0
        if str(snapshot.get("finished_at") or "").strip():
            return 0
        time.sleep(max(1.0, float(args.interval)))


if __name__ == "__main__":
    raise SystemExit(main())
