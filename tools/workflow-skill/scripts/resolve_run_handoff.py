#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


WORKFLOW_HANDOFF_FILENAME = "workflow_handoff.json"
SUMMARY_FILENAME = "summary.json"
TASK_SPEC_FILENAME = "task_spec.json"
WORKFLOW_HANDOFF_VERSION = "harness.workflow-handoff.v1-draft"
SUPPORTED_SCOPES = {
    "task-upload-to-final-export",
    "task-upload-to-keep-list",
    "keep-list-screening",
}
TOP_LEVEL_REQUIRED_FIELDS = (
    "workflow_handoff_version",
    "source_contract_version",
    "source_failure_schema_version",
    "scope",
    "canonical_boundary",
    "run_id",
    "run_root",
    "status",
    "summary_json",
    "task_spec_json",
    "task_spec_available",
    "current_stage",
    "next_report_triggers",
    "verdict",
    "recommended_action",
    "retryable",
    "requires_manual_intervention",
    "resume",
    "intent_summary",
    "pointers",
)
VERDICT_REQUIRED_FIELDS = (
    "outcome",
    "status",
    "failure_layer",
    "category",
    "resolution_mode",
    "retryable",
    "requires_manual_intervention",
    "recommended_action",
    "conclusion",
)
FAILURE_DECISION_REQUIRED_FIELDS = (
    "category",
    "resolution_mode",
    "retryable",
    "requires_manual_intervention",
    "recommended_action",
    "conclusion",
)
FAILURE_REQUIRED_FIELDS = (
    "error_code",
    "message",
    "failure_layer",
    "stage",
)
RESUME_REQUIRED_FIELDS = (
    "available",
    "canonical_resume_point",
    "resume_point_keys",
)
INTENT_SUMMARY_REQUIRED_FIELDS = (
    "intent",
    "controls",
)
POINTERS_REQUIRED_FIELDS = (
    "run_root",
    "summary_json",
    "task_spec_json",
)


class WorkflowResolutionError(Exception):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


def _normalize_path(raw_path: str | Path) -> Path:
    return Path(raw_path).expanduser().resolve()


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise WorkflowResolutionError(
            "path_missing",
            f"路径不存在：{path}",
        ) from exc
    except json.JSONDecodeError as exc:
        raise WorkflowResolutionError(
            "invalid_json",
            f"文件不是合法 JSON：{path}",
        ) from exc
    if not isinstance(payload, dict):
        raise WorkflowResolutionError(
            "invalid_payload",
            f"JSON 顶层必须是对象：{path}",
        )
    return payload


def _infer_handoff_path(target_path: Path) -> tuple[Path, str]:
    if target_path.is_dir():
        return target_path / WORKFLOW_HANDOFF_FILENAME, "run_root"

    if not target_path.exists():
        raise WorkflowResolutionError(
            "path_missing",
            f"目标路径不存在：{target_path}",
        )

    filename = target_path.name
    if filename == WORKFLOW_HANDOFF_FILENAME:
        return target_path, "workflow_handoff_json"
    if filename == SUMMARY_FILENAME:
        return target_path.with_name(WORKFLOW_HANDOFF_FILENAME), "summary_json"
    if filename == TASK_SPEC_FILENAME:
        return target_path.with_name(WORKFLOW_HANDOFF_FILENAME), "task_spec_json"

    raise WorkflowResolutionError(
        "unsupported_input",
        "只支持显式传入 run_root、workflow_handoff.json、summary.json 或 task_spec.json。",
    )


def _resolve_optional_path(raw_value: Any, *, fallback_dir: Path, filename: str) -> Path:
    normalized = str(raw_value or "").strip()
    if normalized:
        path = Path(normalized).expanduser()
        if not path.is_absolute():
            path = (fallback_dir / path).resolve()
        return path
    return (fallback_dir / filename).resolve()


def _raise_invalid_handoff(reason: str) -> None:
    raise WorkflowResolutionError(
        "invalid_workflow_handoff",
        f"workflow_handoff.json 不是有效的 v1 handoff：{reason}",
    )


def _ensure_required_fields(payload: dict[str, Any], required_fields: tuple[str, ...], *, path: str) -> None:
    missing = [field for field in required_fields if field not in payload]
    if missing:
        qualified = ", ".join(f"{path}.{field}" for field in missing)
        _raise_invalid_handoff(f"缺少必需字段 {qualified}")


def _ensure_field_type(payload: dict[str, Any], field: str, expected_type: type[Any], *, path: str) -> None:
    value = payload[field]
    if isinstance(value, expected_type):
        return

    type_name = "boolean" if expected_type is bool else "array" if expected_type is list else "object" if expected_type is dict else "string"
    _raise_invalid_handoff(f"{path}.{field} 必须是 {type_name}")


def _ensure_string_list(value: Any, *, path: str) -> None:
    if not isinstance(value, list):
        _raise_invalid_handoff(f"{path} 必须是 array")
    for index, item in enumerate(value):
        if not isinstance(item, str):
            _raise_invalid_handoff(f"{path}[{index}] 必须是 string")


def _validate_object(
    payload: Any,
    *,
    path: str,
    required_fields: tuple[str, ...],
    field_types: dict[str, type[Any]],
    string_list_fields: tuple[str, ...] = (),
) -> dict[str, Any]:
    if not isinstance(payload, dict):
        _raise_invalid_handoff(f"{path} 必须是 object")

    _ensure_required_fields(payload, required_fields, path=path)
    for field, expected_type in field_types.items():
        _ensure_field_type(payload, field, expected_type, path=path)
    for field in string_list_fields:
        _ensure_string_list(payload[field], path=f"{path}.{field}")
    return payload


def _validate_workflow_handoff(handoff: dict[str, Any]) -> None:
    workflow_handoff_version = str(handoff.get("workflow_handoff_version") or "").strip()
    if workflow_handoff_version != WORKFLOW_HANDOFF_VERSION:
        raise WorkflowResolutionError(
            "unsupported_workflow_handoff_version",
            "当前 $workflow v1 只支持 "
            f"workflow_handoff_version={WORKFLOW_HANDOFF_VERSION}，收到：{workflow_handoff_version or '<empty>'}",
        )

    _ensure_required_fields(handoff, TOP_LEVEL_REQUIRED_FIELDS, path="workflow_handoff")
    top_level_field_types = {
        "source_contract_version": str,
        "source_failure_schema_version": str,
        "scope": str,
        "canonical_boundary": str,
        "run_id": str,
        "run_root": str,
        "status": str,
        "summary_json": str,
        "task_spec_json": str,
        "task_spec_available": bool,
        "current_stage": str,
        "next_report_triggers": list,
        "verdict": dict,
        "recommended_action": str,
        "retryable": bool,
        "requires_manual_intervention": bool,
        "resume": dict,
        "intent_summary": dict,
        "pointers": dict,
    }
    for field, expected_type in top_level_field_types.items():
        _ensure_field_type(handoff, field, expected_type, path="workflow_handoff")
    _ensure_string_list(handoff["next_report_triggers"], path="workflow_handoff.next_report_triggers")

    _validate_object(
        handoff["verdict"],
        path="workflow_handoff.verdict",
        required_fields=VERDICT_REQUIRED_FIELDS,
        field_types={
            "outcome": str,
            "status": str,
            "failure_layer": str,
            "category": str,
            "resolution_mode": str,
            "retryable": bool,
            "requires_manual_intervention": bool,
            "recommended_action": str,
            "conclusion": str,
        },
    )

    if "failure_decision" in handoff:
        _validate_object(
            handoff["failure_decision"],
            path="workflow_handoff.failure_decision",
            required_fields=FAILURE_DECISION_REQUIRED_FIELDS,
            field_types={
                "category": str,
                "resolution_mode": str,
                "retryable": bool,
                "requires_manual_intervention": bool,
                "recommended_action": str,
                "conclusion": str,
            },
        )

    if "failure" in handoff:
        _validate_object(
            handoff["failure"],
            path="workflow_handoff.failure",
            required_fields=FAILURE_REQUIRED_FIELDS,
            field_types={
                "error_code": str,
                "message": str,
                "failure_layer": str,
                "stage": str,
            },
        )

    _validate_object(
        handoff["resume"],
        path="workflow_handoff.resume",
        required_fields=RESUME_REQUIRED_FIELDS,
        field_types={
            "available": bool,
            "canonical_resume_point": str,
            "resume_point_keys": list,
        },
        string_list_fields=("resume_point_keys",),
    )
    _validate_object(
        handoff["intent_summary"],
        path="workflow_handoff.intent_summary",
        required_fields=INTENT_SUMMARY_REQUIRED_FIELDS,
        field_types={
            "intent": dict,
            "controls": dict,
        },
    )
    _validate_object(
        handoff["pointers"],
        path="workflow_handoff.pointers",
        required_fields=POINTERS_REQUIRED_FIELDS,
        field_types={
            "run_root": str,
            "summary_json": str,
            "task_spec_json": str,
        },
    )


def resolve_run_handoff(
    target: str | Path,
    *,
    load_task_spec: bool = True,
    load_summary: bool = False,
) -> dict[str, Any]:
    target_path = _normalize_path(target)
    handoff_path, input_kind = _infer_handoff_path(target_path)
    if not handoff_path.exists():
        raise WorkflowResolutionError(
            "workflow_handoff_missing",
            f"没有找到 {WORKFLOW_HANDOFF_FILENAME}：{handoff_path}",
        )

    handoff = _read_json(handoff_path)
    _validate_workflow_handoff(handoff)
    scope = str(handoff.get("scope") or "").strip()
    if scope not in SUPPORTED_SCOPES:
        raise WorkflowResolutionError(
            "unsupported_scope",
            f"当前 $workflow v1 只支持 canonical scopes，收到：{scope or '<empty>'}",
        )

    fallback_dir = handoff_path.parent
    summary_path = _resolve_optional_path(
        handoff.get("summary_json") or (handoff.get("pointers") or {}).get("summary_json"),
        fallback_dir=fallback_dir,
        filename=SUMMARY_FILENAME,
    )
    task_spec_path = _resolve_optional_path(
        handoff.get("task_spec_json") or (handoff.get("pointers") or {}).get("task_spec_json"),
        fallback_dir=fallback_dir,
        filename=TASK_SPEC_FILENAME,
    )
    run_root = _resolve_optional_path(
        handoff.get("run_root") or (handoff.get("pointers") or {}).get("run_root"),
        fallback_dir=fallback_dir,
        filename="",
    )

    task_spec_available = bool(handoff.get("task_spec_available"))
    task_spec_payload: dict[str, Any] | None = None
    task_spec_missing = False
    if load_task_spec and task_spec_available:
        if task_spec_path.exists():
            task_spec_payload = _read_json(task_spec_path)
        else:
            task_spec_missing = True

    summary_payload: dict[str, Any] | None = None
    summary_missing = False
    if load_summary:
        if summary_path.exists():
            summary_payload = _read_json(summary_path)
        else:
            summary_missing = True

    return {
        "supported": True,
        "input_path": str(target_path),
        "input_kind": input_kind,
        "scope": scope,
        "canonical_boundary": str(handoff.get("canonical_boundary") or "").strip(),
        "workflow_handoff_json": str(handoff_path),
        "run_root": str(run_root),
        "summary_json": str(summary_path),
        "summary_exists": summary_path.exists(),
        "summary_loaded": summary_payload is not None,
        "summary_missing": summary_missing,
        "task_spec_json": str(task_spec_path),
        "task_spec_available": task_spec_available,
        "task_spec_exists": task_spec_path.exists(),
        "task_spec_loaded": task_spec_payload is not None,
        "task_spec_missing": task_spec_missing,
        "handoff": handoff,
        "task_spec": task_spec_payload,
        "summary": summary_payload,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Resolve a canonical run target into workflow_handoff/task_spec/summary pointers.",
    )
    parser.add_argument("target", help="run_root or one of workflow_handoff.json/summary.json/task_spec.json")
    parser.add_argument(
        "--include-summary",
        action="store_true",
        help="Also load summary.json when available.",
    )
    args = parser.parse_args()

    try:
        payload = resolve_run_handoff(
            args.target,
            load_task_spec=True,
            load_summary=args.include_summary,
        )
    except WorkflowResolutionError as exc:
        error_payload = {
            "supported": False,
            "error_code": exc.code,
            "message": exc.message,
        }
        print(json.dumps(error_payload, ensure_ascii=False, indent=2))
        return 1

    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
