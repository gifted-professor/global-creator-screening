from __future__ import annotations

from typing import Any


RUN_CONTRACT_VERSION = "harness.run-summary.v1-draft"
FAILURE_SCHEMA_VERSION = "harness.failure.v1-draft"

SUCCESSFUL_TERMINAL_STATUSES = {
    "completed",
    "completed_with_quality_warnings",
    "completed_with_partial_scrape",
    "completed_with_platform_failures",
    "dry_run_only",
    "staged_only",
    "vision_probe_only",
}
BLOCKED_TERMINAL_STATUSES = {
    "missing_profiles_blocked",
}
RUNNING_STATUSES = {
    "running",
    "launching",
}


def build_run_verdict(
    summary: dict[str, Any] | None = None,
    *,
    status: str | None = None,
    failure: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = summary or {}
    normalized_status = str(status if status is not None else payload.get("status") or "").strip()
    normalized_failure = failure if failure is not None else payload.get("failure")
    if isinstance(normalized_failure, dict):
        outcome = "blocked" if normalized_status in BLOCKED_TERMINAL_STATUSES else "failed"
        return {
            "outcome": outcome,
            "status": normalized_status,
            "failure_layer": str(normalized_failure.get("failure_layer") or ""),
            "category": str(normalized_failure.get("category") or ""),
            "resolution_mode": str(normalized_failure.get("resolution_mode") or ""),
            "retryable": bool(normalized_failure.get("retryable")),
            "requires_manual_intervention": bool(normalized_failure.get("requires_manual_intervention")),
            "recommended_action": str(normalized_failure.get("recommended_action") or ""),
            "conclusion": str(normalized_failure.get("conclusion") or ""),
        }
    if normalized_status == "completed_with_quality_warnings":
        return {
            "outcome": "completed",
            "status": normalized_status,
            "failure_layer": "",
            "category": "",
            "resolution_mode": "",
            "retryable": False,
            "requires_manual_intervention": False,
            "recommended_action": "inspect_summary",
            "conclusion": "本次 run 已完成，但存在质量告警，请先检查 summary 中的质量报告再消费产物。",
        }
    if normalized_status == "completed_with_platform_failures":
        return {
            "outcome": "completed",
            "status": normalized_status,
            "failure_layer": "",
            "category": "",
            "resolution_mode": "",
            "retryable": False,
            "requires_manual_intervention": False,
            "recommended_action": "inspect_summary",
            "conclusion": "本次 run 已完成，但至少一个平台在执行中失败，请先检查 summary 里的平台失败详情再消费产物。",
        }
    if normalized_status == "dry_run_only":
        return {
            "outcome": "completed",
            "status": normalized_status,
            "failure_layer": "",
            "category": "",
            "resolution_mode": "",
            "retryable": False,
            "requires_manual_intervention": False,
            "recommended_action": "resume_run",
            "conclusion": "本次 run 仅执行 dry-run 预估，已生成增量与平台计划摘要，但未真正执行 scrape/visual/upload。",
        }
    if normalized_status in SUCCESSFUL_TERMINAL_STATUSES:
        return {
            "outcome": "completed",
            "status": normalized_status,
            "failure_layer": "",
            "category": "",
            "resolution_mode": "",
            "retryable": False,
            "requires_manual_intervention": False,
            "recommended_action": "consume_outputs",
            "conclusion": "本次 run 已完成，可以直接查看 summary 和产物。",
        }
    if normalized_status.startswith("stopped_after_"):
        return {
            "outcome": "stopped",
            "status": normalized_status,
            "failure_layer": "",
            "category": "",
            "resolution_mode": "",
            "retryable": False,
            "requires_manual_intervention": False,
            "recommended_action": "resume_run",
            "conclusion": "本次 run 按 stop_after 主动停止，可基于 resume point 继续。",
        }
    if normalized_status in RUNNING_STATUSES:
        return {
            "outcome": "running",
            "status": normalized_status,
            "failure_layer": "",
            "category": "",
            "resolution_mode": "",
            "retryable": False,
            "requires_manual_intervention": False,
            "recommended_action": "wait_for_completion",
            "conclusion": "本次 run 正在执行，可继续观察 summary 或等待完成。",
        }
    return {
        "outcome": "unknown",
        "status": normalized_status,
        "failure_layer": "",
        "category": "",
        "resolution_mode": "",
        "retryable": False,
        "requires_manual_intervention": False,
        "recommended_action": "inspect_summary",
        "conclusion": "当前 run 状态需要结合 summary 进一步判断。",
    }


def attach_run_contract(summary: dict[str, Any]) -> dict[str, Any]:
    summary["contract_version"] = RUN_CONTRACT_VERSION
    summary["failure_schema_version"] = FAILURE_SCHEMA_VERSION
    summary["verdict"] = build_run_verdict(summary)
    return summary
