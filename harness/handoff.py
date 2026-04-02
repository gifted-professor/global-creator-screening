from __future__ import annotations

import json
from pathlib import Path
from typing import Any


WORKFLOW_HANDOFF_VERSION = "harness.workflow-handoff.v1-draft"
RUNNING_HANDOFF_TRIGGERS = [
    "stage_changed",
    "structured_failure",
    "run_terminal",
]

INTENT_KEYS = {
    "task_name",
    "task_upload_url",
    "employee_info_url",
    "keep_workbook",
    "template_workbook",
    "requested_platforms",
    "matching_strategy",
    "brand_keyword",
    "stop_after",
}
CONTROL_KEYS = {
    "reuse_existing",
    "requested_platforms",
    "vision_provider",
    "max_identifiers_per_platform",
    "probe_vision_provider_only",
    "skip_scrape",
    "skip_visual",
    "skip_positioning_card_analysis",
}
FINAL_CHILD_POINTER_KEYS = (
    "upstream_workflow_handoff_json",
    "upstream_summary_json",
    "upstream_task_spec_json",
    "downstream_workflow_handoff_json",
    "downstream_summary_json",
    "downstream_task_spec_json",
)


def _json_clone(payload: Any) -> Any:
    return json.loads(json.dumps(payload, ensure_ascii=False))


def _select_fields(payload: dict[str, Any], allowed_keys: set[str]) -> dict[str, Any]:
    normalized = dict(payload or {})
    return {key: _json_clone(value) for key, value in normalized.items() if key in allowed_keys}


def _compact_failure(summary: dict[str, Any]) -> dict[str, Any] | None:
    failure = summary.get("failure")
    if not isinstance(failure, dict):
        return None
    return {
        "error_code": str(failure.get("error_code") or summary.get("error_code") or ""),
        "message": str(failure.get("message") or summary.get("error") or ""),
        "failure_layer": str(failure.get("failure_layer") or summary.get("failure_layer") or ""),
        "stage": str(failure.get("stage") or ""),
    }


def _build_pointers(
    *,
    summary: dict[str, Any],
    task_spec: dict[str, Any],
) -> dict[str, Any]:
    pointers: dict[str, Any] = {
        "run_root": str(summary.get("run_root") or ""),
        "summary_json": str(summary.get("summary_json") or ""),
        "task_spec_json": str(summary.get("task_spec_json") or ""),
    }
    task_spec_paths = dict(task_spec.get("paths") or {})
    for key in FINAL_CHILD_POINTER_KEYS:
        value = task_spec_paths.get(key)
        if value:
            pointers[key] = str(value)
    return pointers


def _build_resume(summary: dict[str, Any]) -> dict[str, Any]:
    resume_points = dict(summary.get("resume_points") or {})
    canonical_resume_point = str((summary.get("contract") or {}).get("canonical_resume_point") or "").strip()
    resume_point_keys = sorted(str(key).strip() for key in resume_points.keys() if str(key).strip())
    if not canonical_resume_point and len(resume_point_keys) == 1:
        canonical_resume_point = resume_point_keys[0]
    return {
        "available": bool(resume_point_keys),
        "canonical_resume_point": canonical_resume_point,
        "resume_point_keys": resume_point_keys,
    }


def _derive_current_stage(summary: dict[str, Any]) -> str:
    failure = summary.get("failure")
    if isinstance(failure, dict):
        stage = str(failure.get("stage") or "").strip()
        if stage:
            return stage

    platforms = summary.get("platforms")
    if isinstance(platforms, dict):
        for platform_name, platform_summary in platforms.items():
            if not isinstance(platform_summary, dict):
                continue
            current_stage = str(platform_summary.get("current_stage") or "").strip()
            if current_stage:
                return f"{platform_name}:{current_stage}"

    steps = summary.get("steps")
    contract = dict(summary.get("contract") or {})
    if isinstance(steps, dict):
        for step_name, step_summary in steps.items():
            if not isinstance(step_summary, dict):
                continue
            step_status = str(step_summary.get("status") or "").strip()
            if step_status in {"running", "launching"}:
                return step_name

        step_order = list(contract.get("step_order") or [])
        for step_name in step_order:
            step_summary = steps.get(step_name)
            if not isinstance(step_summary, dict):
                return str(step_name)
            step_status = str(step_summary.get("status") or "").strip()
            if not step_status or step_status in {"running", "launching"}:
                return str(step_name)

        if str(summary.get("status") or "").strip() == "running":
            if "upstream" in steps and "downstream" not in steps:
                return "downstream_pending"
            if "upstream" not in steps:
                return "upstream_pending"

    if summary.get("staging"):
        return "staging"

    setup = summary.get("setup")
    if isinstance(setup, dict) and not bool(setup.get("completed")) and not bool(setup.get("skipped")):
        return "setup"

    preflight = summary.get("preflight")
    if isinstance(preflight, dict) and not bool(preflight.get("ready", True)):
        return "preflight"

    return str(summary.get("status") or "unknown").strip() or "unknown"


def _build_next_report_triggers(summary: dict[str, Any]) -> list[str]:
    verdict = dict(summary.get("verdict") or {})
    if str(verdict.get("outcome") or "") == "running":
        return list(RUNNING_HANDOFF_TRIGGERS)
    return []


def build_workflow_handoff(
    *,
    summary: dict[str, Any],
    task_spec: dict[str, Any],
    task_spec_available: bool,
) -> dict[str, Any]:
    normalized_summary = dict(summary or {})
    normalized_task_spec = dict(task_spec or {})
    verdict = _json_clone(normalized_summary.get("verdict") or {})
    failure_decision = normalized_summary.get("failure_decision")
    workflow_handoff = {
        "workflow_handoff_version": WORKFLOW_HANDOFF_VERSION,
        "source_contract_version": str(normalized_summary.get("contract_version") or ""),
        "source_failure_schema_version": str(normalized_summary.get("failure_schema_version") or ""),
        "scope": str(
            normalized_task_spec.get("scope")
            or normalized_summary.get("contract", {}).get("scope")
            or ""
        ),
        "canonical_boundary": str(
            normalized_task_spec.get("canonical_boundary")
            or normalized_summary.get("contract", {}).get("canonical_boundary")
            or normalized_summary.get("contract", {}).get("canonical_internal_boundary")
            or ""
        ),
        "run_id": str(normalized_summary.get("run_id") or normalized_task_spec.get("run", {}).get("run_id") or ""),
        "run_root": str(normalized_summary.get("run_root") or normalized_task_spec.get("run", {}).get("run_root") or ""),
        "status": str(normalized_summary.get("status") or ""),
        "summary_json": str(normalized_summary.get("summary_json") or normalized_task_spec.get("run", {}).get("summary_json") or ""),
        "task_spec_json": str(normalized_summary.get("task_spec_json") or normalized_task_spec.get("run", {}).get("task_spec_json") or ""),
        "task_spec_available": bool(task_spec_available),
        "current_stage": _derive_current_stage(normalized_summary),
        "next_report_triggers": _build_next_report_triggers(normalized_summary),
        "verdict": verdict,
        "recommended_action": str(verdict.get("recommended_action") or ""),
        "retryable": bool(verdict.get("retryable")),
        "requires_manual_intervention": bool(verdict.get("requires_manual_intervention")),
        "resume": _build_resume(normalized_summary),
        "intent_summary": {
            "intent": _select_fields(dict(normalized_task_spec.get("intent") or {}), INTENT_KEYS),
            "controls": _select_fields(dict(normalized_task_spec.get("controls") or {}), CONTROL_KEYS),
        },
        "pointers": _build_pointers(summary=normalized_summary, task_spec=normalized_task_spec),
    }
    compact_failure = _compact_failure(normalized_summary)
    if compact_failure is not None:
        workflow_handoff["failure"] = compact_failure
    if isinstance(failure_decision, dict):
        workflow_handoff["failure_decision"] = _json_clone(failure_decision)
    return workflow_handoff


def write_workflow_handoff(
    path: Path,
    *,
    summary: dict[str, Any],
    task_spec: dict[str, Any],
    task_spec_available: bool,
) -> dict[str, Any]:
    payload = build_workflow_handoff(
        summary=summary,
        task_spec=task_spec,
        task_spec_available=task_spec_available,
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return payload
