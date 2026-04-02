from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class FailureTaxonomyRule:
    category: str
    resolution_mode: str
    retryable: bool
    requires_manual_intervention: bool
    recommended_action: str
    conclusion: str


FILESYSTEM_RULE = FailureTaxonomyRule(
    category="filesystem",
    resolution_mode="manual_fix",
    retryable=False,
    requires_manual_intervention=True,
    recommended_action="repair_output_path",
    conclusion="需要人工修复输出路径、权限或磁盘状态后重试。",
)

DEPENDENCY_RULE = FailureTaxonomyRule(
    category="dependency",
    resolution_mode="manual_fix",
    retryable=False,
    requires_manual_intervention=True,
    recommended_action="repair_environment",
    conclusion="需要人工补齐本地依赖或运行环境后重试。",
)

CONFIGURATION_RULE = FailureTaxonomyRule(
    category="configuration",
    resolution_mode="manual_fix",
    retryable=False,
    requires_manual_intervention=True,
    recommended_action="fix_configuration",
    conclusion="需要人工补齐或修正配置后重试。",
)

INPUT_RULE = FailureTaxonomyRule(
    category="input",
    resolution_mode="manual_fix",
    retryable=False,
    requires_manual_intervention=True,
    recommended_action="repair_inputs",
    conclusion="需要人工修复输入、模板或上游产物后重试。",
)

ORCHESTRATION_RULE = FailureTaxonomyRule(
    category="orchestration",
    resolution_mode="manual_investigation",
    retryable=False,
    requires_manual_intervention=True,
    recommended_action="inspect_child_run",
    conclusion="需要先检查子 run summary 和运行态，再决定是否重试。",
)

RUNTIME_RULE = FailureTaxonomyRule(
    category="runtime",
    resolution_mode="manual_investigation",
    retryable=False,
    requires_manual_intervention=True,
    recommended_action="inspect_runtime",
    conclusion="需要先检查运行时错误和相关 step 产物，再决定是否重试。",
)

AUTO_RETRY_RULE = FailureTaxonomyRule(
    category="external_runtime",
    resolution_mode="auto_retry",
    retryable=True,
    requires_manual_intervention=False,
    recommended_action="retry_run",
    conclusion="更像临时运行失败，可以先直接重试当前 run；若连续失败再转人工排查。",
)


EXACT_RULES: dict[str, FailureTaxonomyRule] = {
    "TASK_NAME_MISSING": CONFIGURATION_RULE,
    "MATCHING_STRATEGY_INVALID": CONFIGURATION_RULE,
    "STOP_AFTER_INVALID": CONFIGURATION_RULE,
    "TASK_UPLOAD_URL_MISSING": CONFIGURATION_RULE,
    "EMPLOYEE_INFO_URL_MISSING": CONFIGURATION_RULE,
    "FEISHU_APP_ID_MISSING": CONFIGURATION_RULE,
    "FEISHU_APP_SECRET_MISSING": CONFIGURATION_RULE,
    "MAIL_SYNC_DEFAULT_CREDENTIALS_INCOMPLETE": CONFIGURATION_RULE,
    "VISION_PROVIDER_PREFLIGHT_FAILED": CONFIGURATION_RULE,
    "MISSING_VISION_CONFIG": CONFIGURATION_RULE,
    "KEEP_WORKBOOK_MISSING": INPUT_RULE,
    "TEMPLATE_WORKBOOK_MISSING": INPUT_RULE,
    "KEEP_LIST_ARTIFACT_MISSING": INPUT_RULE,
    "MAIL_SYNC_RESULT_MISSING": INPUT_RULE,
    "MISSING_PROFILES_BLOCKED": INPUT_RULE,
    "VISION_CHANNEL_RACE_FAILED": AUTO_RETRY_RULE,
    "TASK_UPLOAD_TO_KEEP_LIST_FAILED": ORCHESTRATION_RULE,
    "KEEP_LIST_TO_FINAL_EXPORT_FAILED": ORCHESTRATION_RULE,
    "TASK_GROUP_CHILD_FAILED": ORCHESTRATION_RULE,
    "MAIL_SYNC_FAILED": RUNTIME_RULE,
    "SCREENING_STAGING_FAILED": RUNTIME_RULE,
    "KEEP_LIST_SCREENING_RUNTIME_FAILED": RUNTIME_RULE,
    "TASK_UPLOAD_KEEP_LIST_PIPELINE_FAILED": RUNTIME_RULE,
    "SCRAPE_FAILED": AUTO_RETRY_RULE,
}


def _fallback_rule(error_code: str, *, stage: str, failure_layer: str) -> FailureTaxonomyRule:
    normalized_code = str(error_code or "").strip().upper()
    normalized_stage = str(stage or "").strip().lower()
    normalized_layer = str(failure_layer or "").strip().lower()
    if normalized_code.startswith("DOWNSTREAM_"):
        if normalized_code.endswith("SCRAPE_FAILED") or normalized_code.endswith("VISION_PROBE_FAILED"):
            return AUTO_RETRY_RULE
        if normalized_code.endswith("MISSING_PROFILES_BLOCKED"):
            return INPUT_RULE
        return ORCHESTRATION_RULE
    if normalized_code.endswith("_UNAVAILABLE") or normalized_code.endswith("_WRITE_FAILED"):
        return FILESYSTEM_RULE
    if normalized_code.endswith("_RUNTIME_IMPORT_FAILED"):
        return DEPENDENCY_RULE
    if normalized_code.endswith("_MISSING"):
        if normalized_code in {
            "KEEP_WORKBOOK_MISSING",
            "TEMPLATE_WORKBOOK_MISSING",
            "KEEP_LIST_ARTIFACT_MISSING",
            "MAIL_SYNC_RESULT_MISSING",
        }:
            return INPUT_RULE
        return CONFIGURATION_RULE
    if normalized_layer == "setup":
        return FILESYSTEM_RULE
    if normalized_layer == "preflight":
        return CONFIGURATION_RULE
    if normalized_stage in {"vision_probe", "platform_scrape"}:
        return AUTO_RETRY_RULE
    return RUNTIME_RULE


def classify_failure_taxonomy(
    *,
    error_code: str,
    stage: str = "",
    failure_layer: str = "runtime",
) -> FailureTaxonomyRule:
    normalized_code = str(error_code or "").strip().upper()
    if normalized_code in EXACT_RULES:
        return EXACT_RULES[normalized_code]
    return _fallback_rule(normalized_code, stage=stage, failure_layer=failure_layer)


def build_failure_payload(
    *,
    stage: str,
    error_code: str,
    message: str,
    remediation: str,
    failure_layer: str = "runtime",
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    taxonomy = classify_failure_taxonomy(
        error_code=error_code,
        stage=stage,
        failure_layer=failure_layer,
    )
    return {
        "stage": stage,
        "failure_layer": failure_layer,
        "error_code": error_code,
        "message": message,
        "remediation": remediation,
        "details": details or {},
        "category": taxonomy.category,
        "resolution_mode": taxonomy.resolution_mode,
        "retryable": taxonomy.retryable,
        "requires_manual_intervention": taxonomy.requires_manual_intervention,
        "recommended_action": taxonomy.recommended_action,
        "conclusion": taxonomy.conclusion,
    }


def build_failure_decision(failure: dict[str, Any]) -> dict[str, Any]:
    return {
        "category": str(failure.get("category") or ""),
        "resolution_mode": str(failure.get("resolution_mode") or ""),
        "retryable": bool(failure.get("retryable")),
        "requires_manual_intervention": bool(failure.get("requires_manual_intervention")),
        "recommended_action": str(failure.get("recommended_action") or ""),
        "conclusion": str(failure.get("conclusion") or ""),
    }


def attach_failure_to_summary(
    summary: dict[str, Any],
    failure: dict[str, Any],
    *,
    expose_top_level: bool = True,
) -> dict[str, Any]:
    summary["failure"] = failure
    summary["failure_decision"] = build_failure_decision(failure)
    if expose_top_level:
        summary["failure_layer"] = str(failure.get("failure_layer") or "")
        summary["error"] = str(failure.get("message") or "")
        summary["error_code"] = str(failure.get("error_code") or "")
    return summary
