from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path
import sys
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from feishu_screening_bridge.bitable_export import resolve_bitable_view_from_url
from feishu_screening_bridge.bitable_upload import _canonicalize_target_url
from feishu_screening_bridge.feishu_api import DEFAULT_FEISHU_BASE_URL, FeishuOpenClient
from feishu_screening_bridge.local_env import get_preferred_value, load_local_env

DEFAULT_WARN_THRESHOLD = 200


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _safe_name(value: str) -> str:
    text = _clean_text(value)
    if not text:
        return "record_ids"
    cleaned: list[str] = []
    for ch in text:
        if ch.isalnum() or ch in {"-", "_"}:
            cleaned.append(ch)
        else:
            cleaned.append("_")
    return "".join(cleaned).strip("_") or "record_ids"


def _dedupe_record_ids(record_ids: list[str]) -> list[str]:
    seen: set[str] = set()
    unique_ids: list[str] = []
    for record_id in record_ids:
        normalized = _clean_text(record_id)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        unique_ids.append(normalized)
    return unique_ids


def _load_record_ids(path: Path) -> list[str]:
    suffix = path.suffix.casefold()
    if suffix == ".txt":
        return _dedupe_record_ids(path.read_text(encoding="utf-8").splitlines())
    if suffix == ".json":
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, list):
            return _dedupe_record_ids([_clean_text(item) for item in payload])
        if isinstance(payload, dict):
            for key in ("record_ids", "recordIds", "ids", "items"):
                value = payload.get(key)
                if isinstance(value, list):
                    return _dedupe_record_ids([_clean_text(item) for item in value])
        raise ValueError("JSON 清单必须是 record_id 数组，或包含 record_ids/recordIds/ids/items 数组字段。")
    raise ValueError(f"不支持的清单格式: {path.suffix or '<none>'}；目前仅支持 .txt / .json")


def _write_result(output_root: Path, result: dict[str, Any]) -> Path:
    output_root.mkdir(parents=True, exist_ok=True)
    result_path = output_root / "record_id_delete_result.json"
    result_path.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return result_path


def _matches_expected(actual: str, expected: str) -> bool:
    normalized_expected = _clean_text(expected)
    if not normalized_expected:
        return True
    return _clean_text(actual).casefold() == normalized_expected.casefold()


def delete_bitable_records_from_list(
    *,
    client: FeishuOpenClient,
    linked_bitable_url: str,
    record_id_list_path: Path,
    output_root: Path,
    execute: bool,
    warn_threshold: int = DEFAULT_WARN_THRESHOLD,
    expected_app_token: str = "",
    expected_table_id: str = "",
    expected_table_name: str = "",
) -> dict[str, Any]:
    timestamp = datetime.now().isoformat(timespec="seconds")
    record_ids = _load_record_ids(record_id_list_path)
    resolved_view = resolve_bitable_view_from_url(client, _canonicalize_target_url(client, linked_bitable_url))
    validation_errors: list[str] = []
    if not _matches_expected(getattr(resolved_view, "app_token", ""), expected_app_token):
        validation_errors.append(
            f"目标 app_token 不匹配：expected={_clean_text(expected_app_token)} actual={_clean_text(getattr(resolved_view, 'app_token', ''))}"
        )
    if not _matches_expected(getattr(resolved_view, "table_id", ""), expected_table_id):
        validation_errors.append(
            f"目标 table_id 不匹配：expected={_clean_text(expected_table_id)} actual={_clean_text(getattr(resolved_view, 'table_id', ''))}"
        )
    if not _matches_expected(getattr(resolved_view, "table_name", ""), expected_table_name):
        validation_errors.append(
            f"目标 table_name 不匹配：expected={_clean_text(expected_table_name)} actual={_clean_text(getattr(resolved_view, 'table_name', ''))}"
        )

    warnings: list[str] = []
    if len(record_ids) > max(0, int(warn_threshold)):
        warnings.append(f"待删除记录数 {len(record_ids)} 超过警戒阈值 {int(warn_threshold)}，请先人工复核。")

    base_result = {
        "ok": not validation_errors,
        "execute": bool(execute),
        "dry_run": not bool(execute),
        "timestamp": timestamp,
        "input_record_id_list_path": str(record_id_list_path),
        "input_record_count": len(record_ids),
        "record_id_preview": record_ids[:20],
        "warning_threshold": max(0, int(warn_threshold)),
        "warning_large_batch": bool(warnings),
        "warnings": warnings,
        "target_url": _clean_text(getattr(resolved_view, "source_url", linked_bitable_url)),
        "target_app_token": _clean_text(getattr(resolved_view, "app_token", "")),
        "target_table_id": _clean_text(getattr(resolved_view, "table_id", "")),
        "target_table_name": _clean_text(getattr(resolved_view, "table_name", "")),
        "target_view_id": _clean_text(getattr(resolved_view, "view_id", "")),
        "target_view_name": _clean_text(getattr(resolved_view, "view_name", "")),
        "expected_app_token": _clean_text(expected_app_token),
        "expected_table_id": _clean_text(expected_table_id),
        "expected_table_name": _clean_text(expected_table_name),
        "validation_errors": validation_errors,
    }

    if not record_ids:
        result = {
            **base_result,
            "ok": True,
            "no_op": True,
            "message": "record_id 清单为空，未执行任何删除。",
            "planned_delete_count": 0,
            "deleted_count": 0,
            "failed_count": 0,
            "failed_record_ids": [],
        }
        result_path = _write_result(output_root, result)
        result["result_json_path"] = str(result_path)
        result_path.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return result

    if validation_errors:
        result = {
            **base_result,
            "ok": False,
            "guard_blocked": True,
            "message": "目标 base/table 校验未通过，已阻止删除。",
            "planned_delete_count": len(record_ids),
            "deleted_count": 0,
            "failed_count": 0,
            "failed_record_ids": [],
        }
        result_path = _write_result(output_root, result)
        result["result_json_path"] = str(result_path)
        result_path.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return result

    deleted_record_ids: list[str] = []
    failed_records: list[dict[str, str]] = []
    if execute:
        for record_id in record_ids:
            try:
                client.delete_api_json(
                    f"/bitable/v1/apps/{resolved_view.app_token}/tables/{resolved_view.table_id}/records/{record_id}"
                )
                deleted_record_ids.append(record_id)
            except Exception as exc:
                failed_records.append(
                    {
                        "record_id": record_id,
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )

    result = {
        **base_result,
        "no_op": False,
        "message": "dry-run 模式未执行删除。" if not execute else "删除已执行。",
        "planned_delete_count": len(record_ids),
        "deleted_count": len(deleted_record_ids),
        "deleted_record_ids": deleted_record_ids,
        "failed_count": len(failed_records),
        "failed_record_ids": [str(item.get("record_id")) for item in failed_records],
        "failed_records": failed_records,
    }
    result_path = _write_result(output_root, result)
    result["result_json_path"] = str(result_path)
    result_path.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return result


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="按固化 record_id 清单删除飞书多维表记录；默认 dry-run。")
    parser.add_argument("--env-file", default=".env", help="本地 env 文件路径，默认当前目录 ./.env")
    parser.add_argument("--url", "--linked-bitable-url", dest="url", required=True, help="目标飞书多维表 URL")
    parser.add_argument("--record-id-list", required=True, help="record_id 清单文件路径，仅支持 .txt / .json")
    parser.add_argument("--output-root", default="", help="结果输出目录，默认 ./temp/record_id_delete_<timestamp>_<list>")
    parser.add_argument("--execute", action="store_true", help="显式执行删除；默认只 dry-run")
    parser.add_argument(
        "--warn-threshold",
        type=int,
        default=DEFAULT_WARN_THRESHOLD,
        help=f"待删条数超过该阈值时打印警告，默认 {DEFAULT_WARN_THRESHOLD}",
    )
    parser.add_argument("--expected-app-token", default="", help="可选：期望的目标 base app_token，不匹配时阻止执行")
    parser.add_argument("--expected-table-id", default="", help="可选：期望的目标 table_id，不匹配时阻止执行")
    parser.add_argument("--expected-table-name", default="", help="可选：期望的目标 table_name，不匹配时阻止执行")
    parser.add_argument("--json", action="store_true", help="输出完整 JSON 结果")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    env_values = load_local_env(args.env_file)
    app_id = get_preferred_value("", env_values, "FEISHU_APP_ID")
    app_secret = get_preferred_value("", env_values, "FEISHU_APP_SECRET")
    if not app_id or not app_secret:
        raise ValueError("缺少 FEISHU_APP_ID / FEISHU_APP_SECRET。")

    client = FeishuOpenClient(
        app_id=app_id,
        app_secret=app_secret,
        base_url=get_preferred_value("", env_values, "FEISHU_OPEN_BASE_URL", DEFAULT_FEISHU_BASE_URL),
    )
    record_id_list_path = Path(args.record_id_list).expanduser().resolve()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_root = (
        Path(args.output_root).expanduser().resolve()
        if _clean_text(args.output_root)
        else Path("./temp") / f"record_id_delete_{timestamp}_{_safe_name(record_id_list_path.stem)}"
    )
    result = delete_bitable_records_from_list(
        client=client,
        linked_bitable_url=args.url,
        record_id_list_path=record_id_list_path,
        output_root=output_root,
        execute=bool(args.execute),
        warn_threshold=max(0, int(args.warn_threshold)),
        expected_app_token=args.expected_app_token,
        expected_table_id=args.expected_table_id,
        expected_table_name=args.expected_table_name,
    )
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        warning_prefix = "WARNING: " if result.get("warning_large_batch") else ""
        print(
            f"{warning_prefix}record-id delete {'executed' if args.execute else 'planned'}: "
            f"planned={result['planned_delete_count']} "
            f"deleted={result['deleted_count']} "
            f"failed={result['failed_count']} "
            f"target={result['target_table_name']}({result['target_table_id']}) "
            f"result={result['result_json_path']}"
        )
        if result.get("record_id_preview"):
            print("preview:", ", ".join(str(item) for item in list(result["record_id_preview"])[:10]))
        for warning in list(result.get("warnings") or []):
            print("warning:", warning)
        if result.get("validation_errors"):
            for error in list(result.get("validation_errors") or []):
                print("validation_error:", error)
    return 0 if result.get("ok", False) else 1


if __name__ == "__main__":
    raise SystemExit(main())
