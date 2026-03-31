from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


DEFAULT_KEEP_WORKBOOK = (
    REPO_ROOT / "exports" / "测试达人库_MINISO_匹配结果_高置信_按我们去重_llm_reviewed_keep.xlsx"
)
DEFAULT_TEMPLATE_WORKBOOK = (
    REPO_ROOT
    / "downloads"
    / "task_upload_attachments"
    / "recveXGV2i3BS0"
    / "需求上传（excel 格式）"
    / "miniso-星战红人筛号需求模板(1).xlsx"
)
DEFAULT_PLATFORM_ORDER = ("tiktok", "instagram", "youtube")


def _load_runtime_dependencies():
    import backend.app as backend_app
    from backend.final_export_merge import build_all_platforms_final_review_artifacts, collect_final_exports
    from scripts.prepare_screening_inputs import prepare_screening_inputs
    from scripts.run_screening_smoke import (
        count_passed_profiles,
        export_platform_artifacts,
        poll_job,
        require_success,
        reset_backend_runtime_state,
    )

    return {
        "backend_app": backend_app,
        "build_all_platforms_final_review_artifacts": build_all_platforms_final_review_artifacts,
        "collect_final_exports": collect_final_exports,
        "prepare_screening_inputs": prepare_screening_inputs,
        "count_passed_profiles": count_passed_profiles,
        "export_platform_artifacts": export_platform_artifacts,
        "poll_job": poll_job,
        "require_success": require_success,
        "reset_backend_runtime_state": reset_backend_runtime_state,
    }


def default_output_root() -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return REPO_ROOT / "temp" / f"keep_list_screening_{timestamp}"


def iso_now() -> str:
    return datetime.now().astimezone().isoformat()


def _write_summary(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _path_summary(path: Path | None, *, source: str, kind: str) -> dict[str, Any]:
    if path is None:
        return {
            "kind": kind,
            "path": "",
            "exists": False,
            "source": source,
        }
    expanded = path.expanduser()
    return {
        "kind": kind,
        "path": str(expanded.resolve()),
        "exists": expanded.exists(),
        "source": source,
    }


def _build_failure_payload(
    *,
    stage: str,
    error_code: str,
    message: str,
    remediation: str,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "stage": stage,
        "error_code": error_code,
        "message": message,
        "remediation": remediation,
        "details": details or {},
    }


def normalize_platforms(values: list[str] | None) -> list[str]:
    if not values:
        return list(DEFAULT_PLATFORM_ORDER)
    supported_platforms = set(DEFAULT_PLATFORM_ORDER)
    normalized: list[str] = []
    seen: set[str] = set()
    for value in values:
        platform = str(value or "").strip().lower()
        if platform not in supported_platforms:
            raise ValueError(f"不支持的平台: {value}")
        if platform in seen:
            continue
        seen.add(platform)
        normalized.append(platform)
    return normalized


def _build_platform_scrape_identifier(backend_app, platform: str, raw_identifier: str, metadata: dict[str, Any] | None) -> str:
    normalized_metadata = dict(metadata or {})
    fallback_identifier = str(raw_identifier or "").strip()
    if platform in {"tiktok", "youtube"}:
        for candidate in (normalized_metadata.get("url"), normalized_metadata.get("profile_url")):
            value = str(candidate or "").strip()
            if value:
                return value
        screening_module = getattr(backend_app, "screening", None)
        if fallback_identifier and "://" not in fallback_identifier and hasattr(screening_module, "build_canonical_profile_url"):
            canonical = str(screening_module.build_canonical_profile_url(platform, fallback_identifier) or "").strip()
            if canonical:
                return canonical
    if platform == "instagram":
        for candidate in (normalized_metadata.get("handle"), fallback_identifier, normalized_metadata.get("url")):
            value = str(candidate or "").strip()
            if value:
                return value
    return fallback_identifier


def select_platform_identifiers(platform: str, max_identifiers_per_platform: int) -> list[str]:
    runtime = _load_runtime_dependencies()
    backend_app = runtime["backend_app"]
    metadata_lookup = backend_app.load_upload_metadata(platform)
    identifiers = []
    for identifier, metadata in metadata_lookup.items():
        selected = _build_platform_scrape_identifier(backend_app, platform, str(identifier or "").strip(), metadata)
        if selected:
            identifiers.append(selected)
    if max_identifiers_per_platform > 0:
        return identifiers[:max_identifiers_per_platform]
    return identifiers


def build_scrape_payload(platform: str, identifiers: list[str]) -> dict[str, Any]:
    values = [str(item).strip() for item in identifiers if str(item).strip()]
    if platform == "tiktok":
        return {"profiles": values}
    if platform == "instagram":
        return {"usernames": values}
    if platform == "youtube":
        return {"urls": values}
    raise ValueError(f"不支持的平台: {platform}")


def build_visual_payload(platform: str, identifiers: list[str]) -> dict[str, Any]:
    values = [str(item).strip() for item in identifiers if str(item).strip()]
    if platform in {"tiktok", "instagram", "youtube"}:
        return {"identifiers": values}
    raise ValueError(f"不支持的平台: {platform}")


def summarize_platform_statuses(platforms: dict[str, dict[str, Any]]) -> str:
    statuses = [str((payload or {}).get("status") or "").strip() for payload in (platforms or {}).values()]
    if any(status == "failed" for status in statuses):
        return "failed"
    if any(status == "scrape_failed" for status in statuses):
        return "scrape_failed"
    if any(status == "missing_profiles_blocked" for status in statuses):
        return "missing_profiles_blocked"
    if any(status == "completed_with_partial_scrape" for status in statuses):
        return "completed_with_partial_scrape"
    if statuses and all(status in {"staged_only", "skipped"} for status in statuses):
        return "staged_only"
    return "completed"


def _extract_scrape_partial_result(scrape_job: dict[str, Any] | None) -> dict[str, Any]:
    payload = dict(scrape_job or {})
    result = payload.get("result")
    if isinstance(result, dict):
        partial_result = result.get("partial_result")
        if isinstance(partial_result, dict):
            return dict(partial_result)
    partial_result = payload.get("partial_result")
    if isinstance(partial_result, dict):
        return dict(partial_result)
    return {}


def _extract_scrape_profile_reviews(scrape_job: dict[str, Any] | None) -> list[dict[str, Any]]:
    payload = dict(scrape_job or {})
    result = payload.get("result")
    if isinstance(result, dict):
        profile_reviews = result.get("profile_reviews")
        if isinstance(profile_reviews, list):
            return [dict(item) for item in profile_reviews if isinstance(item, dict)]
    partial_result = _extract_scrape_partial_result(payload)
    profile_reviews = partial_result.get("profile_reviews")
    if isinstance(profile_reviews, list):
        return [dict(item) for item in profile_reviews if isinstance(item, dict)]
    return []


def _scrape_has_partial_result(scrape_job: dict[str, Any] | None) -> bool:
    partial_result = _extract_scrape_partial_result(scrape_job)
    if not partial_result:
        return False
    if partial_result.get("profile_reviews"):
        return True
    if partial_result.get("successful_identifiers"):
        return True
    return bool(int(partial_result.get("raw_count") or 0))


def _resolve_scrape_pass_count(scrape_job: dict[str, Any], count_passed_profiles) -> int:
    profile_reviews = _extract_scrape_profile_reviews(scrape_job)
    if profile_reviews:
        return len(
            [
                item
                for item in profile_reviews
                if str((item or {}).get("status") or "").strip() == "Pass"
            ]
        )
    return int(count_passed_profiles(scrape_job) or 0)


def _extract_missing_profile_reviews(scrape_job: dict[str, Any] | None) -> list[dict[str, Any]]:
    return [
        item
        for item in _extract_scrape_profile_reviews(scrape_job)
        if str((item or {}).get("status") or "").strip() == "Missing"
    ]


def _build_missing_profile_summary(profile_reviews: list[dict[str, Any]]) -> list[dict[str, Any]]:
    summaries: list[dict[str, Any]] = []
    for item in profile_reviews:
        upload_metadata = dict(item.get("upload_metadata") or {})
        identifier = str(item.get("username") or upload_metadata.get("handle") or "").strip()
        summaries.append({
            "identifier": identifier,
            "profile_url": str(item.get("profile_url") or upload_metadata.get("url") or "").strip(),
            "reason": str(item.get("reason") or "").strip(),
        })
    return summaries


def _extract_scrape_failure_stage(scrape_job: dict[str, Any] | None) -> str:
    payload = dict(scrape_job or {})
    result = payload.get("result")
    if isinstance(result, dict):
        stage = str(result.get("failure_stage") or "").strip()
        if stage:
            return stage
    return str(payload.get("stage") or "").strip()


def _extract_scrape_apify_metadata(scrape_job: dict[str, Any] | None) -> dict[str, Any]:
    payload = dict(scrape_job or {})
    result = payload.get("result")
    if isinstance(result, dict):
        apify = result.get("apify")
        if isinstance(apify, dict):
            return dict(apify)
    metadata = payload.get("metadata")
    if isinstance(metadata, dict):
        return dict(metadata)
    return {}


def _persist_platform_summary(
    *,
    summary: dict[str, Any],
    run_summary_path: Path,
    backend_app,
    platform: str,
    platform_summary: dict[str, Any],
    current_stage: str | None = None,
    status: str | None = None,
) -> None:
    if current_stage is not None:
        platform_summary["current_stage"] = current_stage
    if status is not None:
        platform_summary["status"] = status
    platform_summary["last_updated_at"] = backend_app.iso_now()
    summary["platforms"][platform] = platform_summary
    _write_summary(run_summary_path, summary)


def _build_positioning_stage_payload(status: str, reason: str = "", **extra: Any) -> dict[str, Any]:
    payload = {
        "status": str(status or "").strip(),
        "reason": str(reason or "").strip(),
    }
    payload.update({key: value for key, value in extra.items() if value not in (None, "")})
    return payload


def _extract_visual_partial_result(visual_job: dict[str, Any] | None) -> dict[str, Any]:
    payload = dict(visual_job or {})
    result = payload.get("result")
    if isinstance(result, dict):
        partial_result = result.get("partial_result")
        if isinstance(partial_result, dict):
            return dict(partial_result)
        if result.get("visual_results") or result.get("summary"):
            return dict(result)
    partial_result = payload.get("partial_result")
    if isinstance(partial_result, dict):
        return dict(partial_result)
    return {}


def _extract_visual_results_map(platform: str, visual_job: dict[str, Any] | None, backend_app) -> dict[str, dict[str, Any]]:
    partial_result = _extract_visual_partial_result(visual_job)
    visual_results = partial_result.get("visual_results")
    if isinstance(visual_results, dict) and visual_results:
        return {
            str(key or (value or {}).get("username") or "").strip(): dict(value)
            for key, value in visual_results.items()
            if isinstance(value, dict) and str(key or (value or {}).get("username") or "").strip()
        }
    loader = getattr(backend_app, "load_visual_results", None)
    if callable(loader):
        loaded = loader(platform)
        if isinstance(loaded, dict):
            return {
                str(key or (value or {}).get("username") or "").strip(): dict(value)
                for key, value in loaded.items()
                if isinstance(value, dict) and str(key or (value or {}).get("username") or "").strip()
            }
    return {}


def _extract_failed_visual_identifiers(platform: str, visual_job: dict[str, Any] | None, backend_app) -> list[str]:
    visual_results = _extract_visual_results_map(platform, visual_job, backend_app)
    failed_identifiers: list[str] = []
    seen: set[str] = set()
    for key, item in visual_results.items():
        identifier = str(key or item.get("username") or "").strip()
        if not identifier or identifier in seen:
            continue
        if item.get("success") is False:
            failed_identifiers.append(identifier)
            seen.add(identifier)
    return failed_identifiers


def _run_visual_postcheck_retries(
    *,
    client,
    backend_app,
    platform: str,
    platform_summary: dict[str, Any],
    vision_provider: str,
    poll_job,
    require_success,
    poll_interval: float,
    max_rounds: int,
) -> dict[str, Any]:
    retry_summary: dict[str, Any] = {
        "enabled": True,
        "max_rounds": max(0, int(max_rounds)),
        "rounds": [],
        "initial_error_count": 0,
        "final_error_count": 0,
        "status": "not_needed",
        "reason": "",
    }
    visual_job = dict(platform_summary.get("visual_job") or {})
    if not visual_job:
        retry_summary["status"] = "skipped"
        retry_summary["reason"] = "visual job was not started"
        return retry_summary

    failed_identifiers = _extract_failed_visual_identifiers(platform, visual_job, backend_app)
    retry_summary["initial_error_count"] = len(failed_identifiers)
    retry_summary["final_error_count"] = len(failed_identifiers)
    if not failed_identifiers:
        return retry_summary
    if retry_summary["max_rounds"] <= 0:
        retry_summary["status"] = "exhausted"
        retry_summary["reason"] = "visual postcheck retry disabled by max rounds"
        return retry_summary

    remaining = list(failed_identifiers)
    for round_index in range(1, retry_summary["max_rounds"] + 1):
        visual_payload_body = build_visual_payload(platform, remaining)
        if vision_provider:
            visual_payload_body["provider"] = str(vision_provider).strip().lower()
        retry_payload = require_success(
            client.post("/api/jobs/visual-review", json={"platform": platform, "payload": visual_payload_body}),
            f"{platform} visual retry round {round_index} start",
        )
        retry_job = poll_job(
            client,
            retry_payload["job"]["id"],
            f"{platform} visual retry round {round_index} poll",
            max(1.0, float(poll_interval)),
        )
        unresolved = _extract_failed_visual_identifiers(platform, retry_job, backend_app)
        retry_summary["rounds"].append({
            "round": round_index,
            "requested_identifier_count": len(remaining),
            "requested_identifier_preview": remaining[:10],
            "job": retry_job,
            "resolved_count": max(0, len(remaining) - len(unresolved)),
            "remaining_error_count": len(unresolved),
        })
        remaining = unresolved
        if not remaining:
            retry_summary["status"] = "completed"
            retry_summary["final_error_count"] = 0
            retry_summary["reason"] = "all failed visual rows were recovered by postcheck rerun"
            return retry_summary

    retry_summary["status"] = "exhausted"
    retry_summary["final_error_count"] = len(remaining)
    retry_summary["remaining_identifier_preview"] = remaining[:10]
    retry_summary["reason"] = "visual postcheck rerun exhausted max rounds with unresolved failures"
    return retry_summary


def run_keep_list_screening_pipeline(
    *,
    keep_workbook: Path,
    template_workbook: Path | None = None,
    task_name: str = "",
    task_upload_url: str = "",
    env_file: str | Path = ".env",
    output_root: Path | None = None,
    summary_json: Path | None = None,
    platform_filters: list[str] | None = None,
    vision_provider: str = "",
    max_identifiers_per_platform: int = 0,
    poll_interval: float = 5.0,
    probe_vision_provider_only: bool = False,
    skip_scrape: bool = False,
    skip_visual: bool = False,
    skip_positioning_card_analysis: bool = False,
    visual_postcheck_max_rounds: int = 3,
    task_owner_name: str = "",
    task_owner_employee_id: str = "",
    task_owner_employee_record_id: str = "",
    task_owner_employee_email: str = "",
    task_owner_owner_name: str = "",
    linked_bitable_url: str = "",
) -> dict[str, Any]:
    resolved_output_root = (output_root or default_output_root()).expanduser().resolve()
    resolved_output_root.mkdir(parents=True, exist_ok=True)

    run_summary_path = (summary_json.expanduser().resolve() if summary_json else resolved_output_root / "summary.json")
    staging_summary_path = resolved_output_root / "staging_summary.json"
    screening_data_dir = resolved_output_root / "data"
    config_dir = resolved_output_root / "config"
    temp_dir = resolved_output_root / "temp"
    exports_dir = resolved_output_root / "exports"
    requested_platforms = normalize_platforms(platform_filters)
    env_path = Path(env_file).expanduser()
    resolved_keep_workbook = keep_workbook.expanduser()
    resolved_template_workbook = template_workbook.expanduser() if template_workbook else None

    summary: dict[str, Any] = {
        "started_at": iso_now(),
        "keep_workbook": str(resolved_keep_workbook.resolve()),
        "template_workbook": str(resolved_template_workbook.resolve()) if resolved_template_workbook else "",
        "task_name": str(task_name or "").strip(),
        "task_upload_url": str(task_upload_url or "").strip(),
        "env_file": str(env_file),
        "output_root": str(resolved_output_root),
        "summary_json": str(run_summary_path),
        "staging_summary_json": str(staging_summary_path),
        "resolved_inputs": {
            "env_file": {
                "path": str(env_path.resolve()),
                "exists": env_path.exists(),
                "source": "cli_or_default",
            },
            "keep_workbook": _path_summary(resolved_keep_workbook, source="cli_or_default", kind="file"),
            "template_workbook": _path_summary(
                resolved_template_workbook,
                source=("cli_or_default" if resolved_template_workbook else "task_upload_or_none"),
                kind="file",
            ),
            "output_dirs": {
                "output_root": _path_summary(resolved_output_root, source="cli_or_default", kind="dir"),
                "screening_data_dir": _path_summary(screening_data_dir, source="output_root", kind="dir"),
                "config_dir": _path_summary(config_dir, source="output_root", kind="dir"),
                "temp_dir": _path_summary(temp_dir, source="output_root", kind="dir"),
                "exports_dir": _path_summary(exports_dir, source="output_root", kind="dir"),
            },
        },
        "preflight": {
            "keep_workbook_exists": resolved_keep_workbook.exists(),
            "template_input_mode": "template_workbook" if resolved_template_workbook else ("task_upload" if str(task_name or "").strip() else "none"),
            "template_workbook_exists": resolved_template_workbook.exists() if resolved_template_workbook else False,
            "requested_platforms": requested_platforms,
            "skip_scrape": bool(skip_scrape),
            "skip_visual": bool(skip_visual),
            "skip_positioning_card_analysis": bool(skip_positioning_card_analysis),
            "visual_postcheck_max_rounds": max(0, int(visual_postcheck_max_rounds)),
            "probe_vision_provider_only": bool(probe_vision_provider_only),
            "ready": False,
            "errors": [],
        },
        "requested_platforms": requested_platforms,
        "requested_vision_provider": str(vision_provider or "").strip().lower(),
        "max_identifiers_per_platform": int(max_identifiers_per_platform),
        "skip_scrape": bool(skip_scrape),
        "skip_visual": bool(skip_visual),
        "skip_positioning_card_analysis": bool(skip_positioning_card_analysis),
        "visual_postcheck_max_rounds": max(0, int(visual_postcheck_max_rounds)),
        "probe_vision_provider_only": bool(probe_vision_provider_only),
        "vision_providers": [],
        "vision_preflight": {},
        "staging": {},
        "platforms": {},
        "artifacts": {
            "keep_workbook": str(resolved_keep_workbook.resolve()),
            "template_workbook": str(resolved_template_workbook.resolve()) if resolved_template_workbook else "",
            "all_platforms_final_review": "",
            "all_platforms_upload_payload_json": "",
            "final_exports": {},
        },
    }

    preflight_errors: list[dict[str, Any]] = []
    if not resolved_keep_workbook.exists():
        preflight_errors.append(
            _build_failure_payload(
                stage="preflight",
                error_code="KEEP_WORKBOOK_MISSING",
                message=f"keep workbook 不存在: {resolved_keep_workbook.resolve()}",
                remediation="先确认上游 keep-list 已生成，或通过 `--keep-workbook` 指向真实存在的 `*_keep.xlsx` 文件。",
                details={"path": str(resolved_keep_workbook.resolve())},
            )
        )
    if resolved_template_workbook is not None and not resolved_template_workbook.exists():
        preflight_errors.append(
            _build_failure_payload(
                stage="preflight",
                error_code="TEMPLATE_WORKBOOK_MISSING",
                message=f"template workbook 不存在: {resolved_template_workbook.resolve()}",
                remediation="通过 `--template-workbook` 指向真实模板文件，或改为传 `--task-name` 让 staging 走任务上传模板下载。",
                details={"path": str(resolved_template_workbook.resolve())},
            )
        )
    if preflight_errors:
        summary["status"] = "failed"
        summary["finished_at"] = iso_now()
        summary["error"] = preflight_errors[0]["message"]
        summary["error_code"] = preflight_errors[0]["error_code"]
        summary["failure"] = preflight_errors[0]
        summary["preflight"]["errors"] = preflight_errors
        _write_summary(run_summary_path, summary)
        return summary

    try:
        runtime = _load_runtime_dependencies()
    except Exception as exc:  # noqa: BLE001
        failure = _build_failure_payload(
            stage="runtime_import",
            error_code="SCREENING_RUNTIME_IMPORT_FAILED",
            message=f"筛号 downstream runtime 加载失败: {exc}",
            remediation="先补齐 `backend` 与筛号相关本地依赖，再重试 keep-list downstream run。",
            details={"exception_type": exc.__class__.__name__},
        )
        summary["status"] = "failed"
        summary["finished_at"] = iso_now()
        summary["error"] = failure["message"]
        summary["error_code"] = failure["error_code"]
        summary["failure"] = failure
        summary["preflight"]["errors"] = [failure]
        _write_summary(run_summary_path, summary)
        return summary

    backend_app = runtime["backend_app"]
    if "build_all_platforms_final_review_artifacts" in runtime and "collect_final_exports" in runtime:
        build_all_platforms_final_review_artifacts = runtime["build_all_platforms_final_review_artifacts"]
        collect_final_exports = runtime["collect_final_exports"]
    else:
        from backend.final_export_merge import build_all_platforms_final_review_artifacts, collect_final_exports

    prepare_screening_inputs = runtime["prepare_screening_inputs"]
    count_passed_profiles = runtime["count_passed_profiles"]
    export_platform_artifacts = runtime["export_platform_artifacts"]
    poll_job = runtime["poll_job"]
    require_success = runtime["require_success"]
    reset_backend_runtime_state = runtime["reset_backend_runtime_state"]

    try:
        reset_backend_runtime_state()
        staging_summary = prepare_screening_inputs(
            creator_workbook=resolved_keep_workbook.resolve(),
            template_workbook=resolved_template_workbook.resolve() if resolved_template_workbook else None,
            task_name=str(task_name or "").strip(),
            task_upload_url=str(task_upload_url or "").strip(),
            env_file=env_file,
            screening_data_dir=screening_data_dir,
            config_dir=config_dir,
            temp_dir=temp_dir,
            summary_json=staging_summary_path,
        )
    except Exception as exc:  # noqa: BLE001
        failure = _build_failure_payload(
            stage="staging",
            error_code="SCREENING_STAGING_FAILED",
            message=str(exc) or exc.__class__.__name__,
            remediation="检查 keep workbook、模板输入、任务上传相关 env，以及 staging summary 的 resolved_inputs 后重试。",
            details={"exception_type": exc.__class__.__name__},
        )
        summary["status"] = "failed"
        summary["finished_at"] = backend_app.iso_now()
        summary["error"] = failure["message"]
        summary["error_code"] = failure["error_code"]
        summary["failure"] = failure
        summary["preflight"]["errors"] = [failure]
        _write_summary(run_summary_path, summary)
        return summary

    summary["started_at"] = backend_app.iso_now()
    summary["staging"] = staging_summary
    summary["vision_providers"] = backend_app.get_available_vision_provider_names()
    summary["vision_preflight"] = backend_app.build_vision_preflight(vision_provider)
    summary["preflight"]["ready"] = True
    summary["preflight"]["errors"] = []
    _write_summary(run_summary_path, summary)

    try:
        client = backend_app.app.test_client()
        resolve_routing_strategy = getattr(backend_app, "resolve_visual_review_routing_strategy", None)
        active_routing_strategy = ""
        if callable(resolve_routing_strategy):
            active_routing_strategy = str(resolve_routing_strategy({}) or "").strip().lower()
        if not skip_visual or probe_vision_provider_only:
            if (
                not str(vision_provider or "").strip()
                and active_routing_strategy == "probe_ranked"
                and hasattr(backend_app, "run_probe_ranked_visual_provider_race")
            ):
                race_payload = backend_app.run_probe_ranked_visual_provider_race(
                    platform=requested_platforms[0] if requested_platforms else "instagram"
                )
                probe_payload = {
                    "success": bool(race_payload.get("success")),
                    "provider": race_payload.get("selected_provider") or "",
                    "probe": {
                        "success": bool(race_payload.get("success")),
                        "provider": race_payload.get("selected_provider") or "",
                        "model": race_payload.get("selected_model") or "",
                        "checked_at": race_payload.get("checked_at") or "",
                    },
                    "channel_race": race_payload,
                    "vision_preflight": summary["vision_preflight"],
                }
                probe_status_code = 200 if probe_payload.get("success") else 400
                if not probe_payload.get("success"):
                    probe_payload["error_code"] = "VISION_CHANNEL_RACE_FAILED"
                    probe_payload["error"] = "视觉通道赛马失败：当前优先链路都不可用。"
            else:
                probe_response = client.post("/api/vision/providers/probe", json={"provider": vision_provider or ""})
                probe_payload = probe_response.get_json(silent=True) or {
                    "success": False,
                    "error": f"unexpected HTTP {probe_response.status_code}",
                }
                probe_status_code = probe_response.status_code
            summary["vision_probe"] = probe_payload
            summary["vision_preflight"] = probe_payload.get("vision_preflight") or summary["vision_preflight"]
            if probe_status_code >= 400 or probe_payload.get("success") is False:
                summary["status"] = "vision_probe_failed"
                summary["finished_at"] = backend_app.iso_now()
                _write_summary(run_summary_path, summary)
                return summary
        if probe_vision_provider_only:
            summary["status"] = "vision_probe_only"
            summary["finished_at"] = backend_app.iso_now()
            _write_summary(run_summary_path, summary)
            return summary

        for platform in requested_platforms:
            requested_identifiers = select_platform_identifiers(platform, max(0, int(max_identifiers_per_platform)))
            platform_summary: dict[str, Any] = {
                "staged_identifier_count": len(backend_app.load_upload_metadata(platform)),
                "requested_identifier_count": len(requested_identifiers),
                "requested_identifier_preview": requested_identifiers[:10],
                "requested_vision_provider": str(vision_provider or "").strip().lower(),
                "vision_preflight": backend_app.build_vision_preflight(vision_provider),
                "status": "running",
                "current_stage": "platform_preparing",
            }
            _persist_platform_summary(
                summary=summary,
                run_summary_path=run_summary_path,
                backend_app=backend_app,
                platform=platform,
                platform_summary=platform_summary,
            )

            if not requested_identifiers:
                platform_summary["status"] = "skipped"
                platform_summary["reason"] = "no staged identifiers for platform"
                _persist_platform_summary(
                    summary=summary,
                    run_summary_path=run_summary_path,
                    backend_app=backend_app,
                    platform=platform,
                    platform_summary=platform_summary,
                    current_stage="platform_skipped",
                )
                continue

            if skip_scrape:
                platform_summary["status"] = "staged_only"
                platform_summary["scrape_job"] = {"status": "skipped", "reason": "skip_scrape flag set"}
                platform_summary["visual_gate"] = {
                    "executed": False,
                    "reason": "scrape skipped before visual review",
                    "preflight_status": platform_summary["vision_preflight"]["status"],
                    "runnable_provider_names": platform_summary["vision_preflight"]["runnable_provider_names"],
                    "selected_provider": platform_summary["vision_preflight"].get("preferred_provider") or "",
                }
                platform_summary["positioning_card_analysis"] = _build_positioning_stage_payload(
                    "skipped",
                    "scrape skipped before positioning analysis",
                )
                _persist_platform_summary(
                    summary=summary,
                    run_summary_path=run_summary_path,
                    backend_app=backend_app,
                    platform=platform,
                    platform_summary=platform_summary,
                    current_stage="scrape_skipped",
                )
                continue

            _persist_platform_summary(
                summary=summary,
                run_summary_path=run_summary_path,
                backend_app=backend_app,
                platform=platform,
                platform_summary=platform_summary,
                current_stage="scrape_starting",
            )
            scrape_payload_body = build_scrape_payload(platform, requested_identifiers)
            scrape_payload = require_success(
                client.post("/api/jobs/scrape", json={"platform": platform, "payload": scrape_payload_body}),
                f"{platform} scrape start",
            )
            platform_summary["scrape_job"] = dict(scrape_payload.get("job") or {})
            _persist_platform_summary(
                summary=summary,
                run_summary_path=run_summary_path,
                backend_app=backend_app,
                platform=platform,
                platform_summary=platform_summary,
                current_stage="scrape_running",
            )
            scrape_job = poll_job(client, scrape_payload["job"]["id"], f"{platform} scrape poll", max(1.0, float(poll_interval)))
            platform_summary["scrape_job"] = scrape_job
            scrape_apify = _extract_scrape_apify_metadata(scrape_job)
            if scrape_apify:
                platform_summary["scrape_job"]["failure_stage"] = _extract_scrape_failure_stage(scrape_job)
                platform_summary["scrape_job"]["apify_run_id"] = scrape_apify.get("apify_run_id") or ""
                platform_summary["scrape_job"]["apify_dataset_id"] = scrape_apify.get("apify_dataset_id") or ""
                platform_summary["scrape_job"]["reused_guard"] = bool(scrape_apify.get("reused_guard"))
                platform_summary["scrape_job"]["guard_key"] = scrape_apify.get("guard_key") or ""
            scrape_partial_result = _extract_scrape_partial_result(scrape_job)
            if scrape_partial_result:
                platform_summary["scrape_job"]["partial_result"] = scrape_partial_result
            _persist_platform_summary(
                summary=summary,
                run_summary_path=run_summary_path,
                backend_app=backend_app,
                platform=platform,
                platform_summary=platform_summary,
                current_stage="scrape_completed" if scrape_job["status"] == "completed" else "scrape_poll_finished",
            )
            scrape_was_salvaged = False
            if scrape_job["status"] != "completed":
                if _scrape_has_partial_result(scrape_job):
                    scrape_was_salvaged = True
                    platform_summary["scrape_job"]["salvaged"] = True
                    _persist_platform_summary(
                        summary=summary,
                        run_summary_path=run_summary_path,
                        backend_app=backend_app,
                        platform=platform,
                        platform_summary=platform_summary,
                        current_stage="scrape_partial_ready",
                    )
                else:
                    platform_summary["status"] = "scrape_failed"
                    _persist_platform_summary(
                        summary=summary,
                        run_summary_path=run_summary_path,
                        backend_app=backend_app,
                        platform=platform,
                        platform_summary=platform_summary,
                        current_stage="scrape_failed",
                    )
                    continue

            scrape_profile_reviews = _extract_scrape_profile_reviews(scrape_job)
            pass_count = _resolve_scrape_pass_count(scrape_job, count_passed_profiles)
            missing_reviews = _extract_missing_profile_reviews(scrape_job)
            missing_profiles = _build_missing_profile_summary(missing_reviews)
            platform_summary["profile_review_count"] = len(scrape_profile_reviews)
            platform_summary["prescreen_pass_count"] = pass_count
            platform_summary["missing_profile_count"] = len(missing_profiles)
            if missing_profiles:
                platform_summary["missing_profiles"] = missing_profiles
            platform_summary["visual_gate"] = {
                "executed": False,
                "skip_visual_flag": bool(skip_visual),
                "preflight_status": platform_summary["vision_preflight"]["status"],
                "runnable_provider_names": platform_summary["vision_preflight"]["runnable_provider_names"],
                "configured_provider_names": platform_summary["vision_preflight"]["configured_provider_names"],
                "selected_provider": platform_summary["vision_preflight"].get("preferred_provider") or "",
            }
            if missing_profiles:
                platform_summary["visual_gate"]["blocked"] = True
                platform_summary["visual_gate"]["reason"] = "prescreen contains Missing targets"
                platform_summary["visual_job"] = {
                    "status": "skipped",
                    "reason": "名单账号未在本次抓取结果中返回，已阻断视觉复核和最终导出",
                }
                platform_summary["positioning_card_analysis"] = _build_positioning_stage_payload(
                    "skipped",
                    "missing profiles blocked downstream stages",
                )
                platform_summary["artifact_status"] = require_success(
                    client.get(f"/api/artifacts/{platform}/status"),
                    f"{platform} artifact status",
                )
                platform_summary["exports"] = {}
                platform_summary["status"] = "missing_profiles_blocked"
                _persist_platform_summary(
                    summary=summary,
                    run_summary_path=run_summary_path,
                    backend_app=backend_app,
                    platform=platform,
                    platform_summary=platform_summary,
                    current_stage="missing_profiles_blocked",
                )
                continue
            if skip_visual:
                platform_summary["visual_job"] = {"status": "skipped", "reason": "skip_visual flag set"}
            elif pass_count <= 0:
                platform_summary["visual_job"] = {"status": "skipped", "reason": "no Prescreen=Pass targets"}
            elif backend_app.get_available_vision_provider_names(vision_provider):
                visual_payload_body = build_visual_payload(platform, requested_identifiers)
                if vision_provider:
                    visual_payload_body["provider"] = str(vision_provider).strip().lower()
                _persist_platform_summary(
                    summary=summary,
                    run_summary_path=run_summary_path,
                    backend_app=backend_app,
                    platform=platform,
                    platform_summary=platform_summary,
                    current_stage="visual_starting",
                )
                visual_payload = require_success(
                    client.post("/api/jobs/visual-review", json={"platform": platform, "payload": visual_payload_body}),
                    f"{platform} visual start",
                )
                platform_summary["visual_job"] = dict(visual_payload.get("job") or {})
                _persist_platform_summary(
                    summary=summary,
                    run_summary_path=run_summary_path,
                    backend_app=backend_app,
                    platform=platform,
                    platform_summary=platform_summary,
                    current_stage="visual_running",
                )
                platform_summary["visual_job"] = poll_job(
                    client,
                    visual_payload["job"]["id"],
                    f"{platform} visual poll",
                    max(1.0, float(poll_interval)),
                )
                platform_summary["visual_gate"]["executed"] = True
            else:
                platform_summary["visual_job"] = {
                    "status": "skipped",
                    "reason": platform_summary["vision_preflight"]["message"],
                    "error_code": platform_summary["vision_preflight"]["error_code"],
                    "vision_preflight": platform_summary["vision_preflight"],
                }

            if not skip_visual and pass_count > 0:
                platform_summary["visual_retry"] = _run_visual_postcheck_retries(
                    client=client,
                    backend_app=backend_app,
                    platform=platform,
                    platform_summary=platform_summary,
                    vision_provider=vision_provider,
                    poll_job=poll_job,
                    require_success=require_success,
                    poll_interval=poll_interval,
                    max_rounds=visual_postcheck_max_rounds,
                )
            else:
                platform_summary["visual_retry"] = {
                    "enabled": True,
                    "max_rounds": max(0, int(visual_postcheck_max_rounds)),
                    "rounds": [],
                    "initial_error_count": 0,
                    "final_error_count": 0,
                    "status": "skipped",
                    "reason": "visual review was skipped or no prescreen-pass targets",
                }

            if skip_positioning_card_analysis:
                platform_summary["positioning_card_analysis"] = _build_positioning_stage_payload(
                    "skipped",
                    "skip_positioning_card_analysis flag set",
                )
            elif skip_visual:
                platform_summary["positioning_card_analysis"] = _build_positioning_stage_payload(
                    "skipped",
                    "visual review skipped",
                )
            else:
                resolve_targets = getattr(backend_app, "resolve_positioning_card_analysis_targets", None)
                eligible_targets = []
                if callable(resolve_targets):
                    eligible_targets = list(resolve_targets(platform, build_visual_payload(platform, requested_identifiers)))
                if not eligible_targets:
                    platform_summary["positioning_card_analysis"] = _build_positioning_stage_payload(
                        "skipped",
                        "no Visual=Pass targets",
                    )
                else:
                    positioning_payload_body = build_visual_payload(platform, requested_identifiers)
                    if vision_provider:
                        positioning_payload_body["provider"] = str(vision_provider).strip().lower()
                    try:
                        _persist_platform_summary(
                            summary=summary,
                            run_summary_path=run_summary_path,
                            backend_app=backend_app,
                            platform=platform,
                            platform_summary=platform_summary,
                            current_stage="positioning_card_analysis_starting",
                        )
                        positioning_payload = require_success(
                            client.post(
                                "/api/jobs/positioning-card-analysis",
                                json={"platform": platform, "payload": positioning_payload_body},
                            ),
                            f"{platform} positioning card start",
                        )
                        platform_summary["positioning_card_analysis"] = dict(positioning_payload.get("job") or {})
                        _persist_platform_summary(
                            summary=summary,
                            run_summary_path=run_summary_path,
                            backend_app=backend_app,
                            platform=platform,
                            platform_summary=platform_summary,
                            current_stage="positioning_card_analysis_running",
                        )
                        platform_summary["positioning_card_analysis"] = poll_job(
                            client,
                            positioning_payload["job"]["id"],
                            f"{platform} positioning card poll",
                            max(1.0, float(poll_interval)),
                        )
                    except Exception as exc:  # noqa: BLE001
                        platform_summary["positioning_card_analysis"] = _build_positioning_stage_payload(
                            "failed",
                            str(exc) or exc.__class__.__name__,
                            error_code="POSITIONING_CARD_ANALYSIS_FAILED",
                            non_blocking=True,
                        )

            _persist_platform_summary(
                summary=summary,
                run_summary_path=run_summary_path,
                backend_app=backend_app,
                platform=platform,
                platform_summary=platform_summary,
                current_stage="exporting_artifacts",
            )
            platform_summary["artifact_status"] = require_success(
                client.get(f"/api/artifacts/{platform}/status"),
                f"{platform} artifact status",
            )
            platform_summary["exports"] = export_platform_artifacts(client, platform, exports_dir / platform)
            platform_summary["status"] = "completed_with_partial_scrape" if scrape_was_salvaged else "completed"
            _persist_platform_summary(
                summary=summary,
                run_summary_path=run_summary_path,
                backend_app=backend_app,
                platform=platform,
                platform_summary=platform_summary,
                current_stage="completed",
            )
    except Exception as exc:  # noqa: BLE001
        failure = _build_failure_payload(
            stage="downstream",
            error_code="KEEP_LIST_SCREENING_RUNTIME_FAILED",
            message=str(exc) or exc.__class__.__name__,
            remediation="检查 staging 产物、vision preflight、backend runtime 和对应平台 job 日志后重试。",
            details={"exception_type": exc.__class__.__name__},
        )
        summary["status"] = "failed"
        summary["finished_at"] = backend_app.iso_now()
        summary["error"] = failure["message"]
        summary["error_code"] = failure["error_code"]
        summary["failure"] = failure
        _write_summary(run_summary_path, summary)
        return summary

    combined_exports = collect_final_exports(summary.get("platforms"))
    combined_artifacts = build_all_platforms_final_review_artifacts(
        output_path=exports_dir / "all_platforms_final_review.xlsx",
        payload_json_path=exports_dir / "all_platforms_final_review_payload.json",
        final_exports=combined_exports,
        keep_workbook=resolved_keep_workbook,
        task_owner={
            "responsible_name": str(task_owner_name or "").strip(),
            "employee_name": str(task_owner_name or "").strip(),
            "employee_id": str(task_owner_employee_id or "").strip(),
            "employee_record_id": str(task_owner_employee_record_id or "").strip(),
            "employee_email": str(task_owner_employee_email or "").strip(),
            "owner_name": str(task_owner_owner_name or "").strip(),
            "linked_bitable_url": str(linked_bitable_url or "").strip(),
            "task_name": str(task_name or "").strip(),
        },
    )
    summary["artifacts"]["final_exports"] = combined_exports
    summary["artifacts"]["all_platforms_final_review"] = combined_artifacts["all_platforms_final_review"]
    summary["artifacts"]["all_platforms_upload_payload_json"] = combined_artifacts["all_platforms_upload_payload_json"]
    summary["artifacts"]["all_platforms_upload_local_archive_dir"] = combined_artifacts["all_platforms_upload_local_archive_dir"]
    summary["artifacts"]["all_platforms_upload_skipped_archive_json"] = combined_artifacts["all_platforms_upload_skipped_archive_json"]
    summary["artifacts"]["all_platforms_upload_skipped_archive_xlsx"] = combined_artifacts["all_platforms_upload_skipped_archive_xlsx"]
    summary["artifacts"]["all_platforms_upload_row_count"] = combined_artifacts["row_count"]
    summary["artifacts"]["all_platforms_upload_source_row_count"] = combined_artifacts["source_row_count"]
    summary["artifacts"]["all_platforms_upload_skipped_row_count"] = combined_artifacts["skipped_row_count"]
    summary["status"] = summarize_platform_statuses(summary["platforms"])
    summary["finished_at"] = backend_app.iso_now()
    _write_summary(run_summary_path, summary)
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Stage and optionally run the screening pipeline from a reviewed keep-list workbook."
    )
    parser.add_argument("--env-file", default=".env", help="本地 env 文件路径，默认 ./.env。")
    parser.add_argument("--keep-workbook", default=str(DEFAULT_KEEP_WORKBOOK), help="`*_llm_reviewed_keep.xlsx` 路径。")
    parser.add_argument("--template-workbook", default=str(DEFAULT_TEMPLATE_WORKBOOK), help="需求模板 xlsx。")
    parser.add_argument("--task-name", default="", help="任务名；如需直接复用任务上传模板解析链可传。")
    parser.add_argument("--task-upload-url", default="", help="飞书任务上传 wiki/base 链接。")
    parser.add_argument("--output-root", default="", help="输出目录；默认写到 temp/keep_list_screening_<timestamp>。")
    parser.add_argument("--summary-json", default="", help="最终 run summary.json 输出路径。")
    parser.add_argument("--platform", action="append", help="只跑指定平台，可重复传入：tiktok / instagram / youtube。")
    parser.add_argument("--vision-provider", default="", help="指定视觉 provider，例如 openai / reelx。")
    parser.add_argument("--max-identifiers-per-platform", type=int, default=0, help="每个平台最多跑多少个账号；0 表示不截断。")
    parser.add_argument("--poll-interval", type=float, default=5.0, help="轮询 job 状态的秒数。")
    parser.add_argument("--probe-vision-provider-only", action="store_true", help="只做视觉 provider live probe，不继续 scrape/visual/export。")
    parser.add_argument("--skip-scrape", action="store_true", help="只做 staging，不触发 scrape/visual/export。")
    parser.add_argument("--skip-visual", action="store_true", help="跑 scrape 和导出，但跳过视觉复核。")
    parser.add_argument("--visual-postcheck-max-rounds", type=int, default=3, help="视觉完成后自动补跑失败账号的最大轮数；默认 3。")
    parser.add_argument("--skip-positioning-card-analysis", action="store_true", help="跳过 visual-pass 后的定位卡分析。")
    parser.add_argument("--task-owner-name", default="", help="任务负责人展示名，用于总表 `达人对接人`。")
    parser.add_argument("--task-owner-employee-id", default="", help="任务负责人飞书 employeeId，用于总表 payload。")
    parser.add_argument("--task-owner-employee-record-id", default="", help="任务负责人员工表 record_id。")
    parser.add_argument("--task-owner-employee-email", default="", help="任务负责人邮箱。")
    parser.add_argument("--task-owner-owner-name", default="", help="任务上传 ownerName 原始值。")
    parser.add_argument("--linked-bitable-url", default="", help="任务关联达人管理表链接。")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    summary = run_keep_list_screening_pipeline(
        keep_workbook=Path(args.keep_workbook),
        template_workbook=Path(args.template_workbook) if args.template_workbook else None,
        task_name=args.task_name or "",
        task_upload_url=args.task_upload_url or "",
        env_file=args.env_file,
        output_root=Path(args.output_root) if args.output_root else None,
        summary_json=Path(args.summary_json) if args.summary_json else None,
        platform_filters=args.platform,
        vision_provider=args.vision_provider or "",
        max_identifiers_per_platform=max(0, int(args.max_identifiers_per_platform)),
        poll_interval=max(1.0, float(args.poll_interval)),
        probe_vision_provider_only=bool(args.probe_vision_provider_only),
        skip_scrape=bool(args.skip_scrape),
        skip_visual=bool(args.skip_visual),
        visual_postcheck_max_rounds=max(0, int(args.visual_postcheck_max_rounds)),
        skip_positioning_card_analysis=bool(args.skip_positioning_card_analysis),
        task_owner_name=args.task_owner_name or "",
        task_owner_employee_id=args.task_owner_employee_id or "",
        task_owner_employee_record_id=args.task_owner_employee_record_id or "",
        task_owner_employee_email=args.task_owner_employee_email or "",
        task_owner_owner_name=args.task_owner_owner_name or "",
        linked_bitable_url=args.linked_bitable_url or "",
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if summary.get("status") != "failed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
