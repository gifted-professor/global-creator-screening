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

from backend.final_export_merge import extract_task_owner_context
from harness.contract import SUCCESSFUL_TERMINAL_STATUSES, attach_run_contract
from harness.config import (
    RequiredConfigSpec,
    build_required_config_errors,
    normalize_platform_filters,
    resolve_final_runner_config,
)
from harness.failures import attach_failure_to_summary, build_failure_payload as build_harness_failure_payload
from harness.handoff import write_workflow_handoff
from harness.paths import resolve_final_runner_paths
from harness.preflight import (
    build_preflight_error,
    build_preflight_payload,
    inspect_directory_materialization_target,
)
from harness.setup import materialize_setup
from harness.spec import build_final_runner_task_spec, write_task_spec


MATCHING_STRATEGIES = ("legacy-enrichment", "brand-keyword-fast-path")
DEFAULT_MATCHING_STRATEGY = "brand-keyword-fast-path"
SUCCESSFUL_DOWNSTREAM_STATUSES = SUCCESSFUL_TERMINAL_STATUSES


def _load_runtime_dependencies():
    from feishu_screening_bridge.feishu_api import DEFAULT_FEISHU_BASE_URL, FeishuOpenClient
    from feishu_screening_bridge.task_upload_sync import resolve_task_upload_entries
    from scripts.run_keep_list_screening_pipeline import run_keep_list_screening_pipeline
    from scripts.run_task_upload_to_keep_list_pipeline import run_task_upload_to_keep_list_pipeline

    return {
        "DEFAULT_FEISHU_BASE_URL": DEFAULT_FEISHU_BASE_URL,
        "FeishuOpenClient": FeishuOpenClient,
        "resolve_task_upload_entries": resolve_task_upload_entries,
        "run_task_upload_to_keep_list_pipeline": run_task_upload_to_keep_list_pipeline,
        "run_keep_list_screening_pipeline": run_keep_list_screening_pipeline,
    }


def default_output_root() -> Path:
    return resolve_final_runner_paths(task_name="task").run_root


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
    failure_layer: str = "runtime",
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return build_harness_failure_payload(
        stage=stage,
        error_code=error_code,
        message=message,
        remediation=remediation,
        failure_layer=failure_layer,
        details=details,
    )


def _build_keep_list_resume_command(
    *,
    keep_workbook: str,
    template_workbook: str,
    task_name: str,
    task_upload_url: str,
    env_file: str,
    requested_platforms: list[str],
    vision_provider: str,
    max_identifiers_per_platform: int,
    poll_interval: float,
    creator_cache_db_path: str,
    force_refresh_creator_cache: bool,
    probe_vision_provider_only: bool,
    skip_scrape: bool,
    skip_visual: bool,
    skip_positioning_card_analysis: bool,
) -> str:
    parts = [
        "backend/.venv/bin/python",
        "scripts/run_keep_list_screening_pipeline.py",
        f'--keep-workbook "{keep_workbook}"',
        f'--env-file "{env_file}"',
    ]
    if template_workbook:
        parts.append(f'--template-workbook "{template_workbook}"')
    elif task_name:
        parts.append(f'--task-name "{task_name}"')
        if task_upload_url:
            parts.append(f'--task-upload-url "{task_upload_url}"')
    elif task_upload_url:
        parts.append(f'--task-upload-url "{task_upload_url}"')
    for platform in requested_platforms:
        parts.append(f"--platform {platform}")
    if max_identifiers_per_platform > 0:
        parts.append(f"--max-identifiers-per-platform {max_identifiers_per_platform}")
    if vision_provider:
        parts.append(f"--vision-provider {vision_provider}")
    if poll_interval > 1.0:
        parts.append(f"--poll-interval {poll_interval}")
    if creator_cache_db_path:
        parts.append(f'--creator-cache-db-path "{creator_cache_db_path}"')
    if force_refresh_creator_cache:
        parts.append("--force-refresh-creator-cache")
    if probe_vision_provider_only:
        parts.append("--probe-vision-provider-only")
    if skip_scrape:
        parts.append("--skip-scrape")
    if skip_visual:
        parts.append("--skip-visual")
    if skip_positioning_card_analysis:
        parts.append("--skip-positioning-card-analysis")
    return " ".join(parts)


def _collect_final_exports(downstream_summary: dict[str, Any]) -> dict[str, dict[str, str]]:
    final_exports: dict[str, dict[str, str]] = {}
    for platform, platform_summary in (downstream_summary.get("platforms") or {}).items():
        exports_payload = platform_summary.get("exports")
        if not isinstance(exports_payload, dict):
            continue
        cleaned = {
            key: str(value).strip()
            for key, value in exports_payload.items()
            if str(value or "").strip()
        }
        if cleaned:
            final_exports[str(platform)] = cleaned
    return final_exports


def _collect_platform_statuses(downstream_summary: dict[str, Any]) -> dict[str, str]:
    statuses: dict[str, str] = {}
    for platform, platform_summary in (downstream_summary.get("platforms") or {}).items():
        platform_name = str(platform or "").strip()
        if not platform_name:
            continue
        status = str((platform_summary or {}).get("status") or "").strip()
        if status:
            statuses[platform_name] = status
    return statuses


def _collect_positioning_artifacts(downstream_summary: dict[str, Any]) -> dict[str, dict[str, str]]:
    artifacts: dict[str, dict[str, str]] = {}
    for platform, platform_summary in (downstream_summary.get("platforms") or {}).items():
        exports_payload = (platform_summary or {}).get("exports")
        if not isinstance(exports_payload, dict):
            continue
        cleaned = {
            key: str(value).strip()
            for key, value in exports_payload.items()
            if key.startswith("positioning_card_") and str(value or "").strip()
        }
        if cleaned:
            artifacts[str(platform)] = cleaned
    return artifacts


def _collect_positioning_stage_summaries(downstream_summary: dict[str, Any]) -> dict[str, dict[str, Any]]:
    stages: dict[str, dict[str, Any]] = {}
    for platform, platform_summary in (downstream_summary.get("platforms") or {}).items():
        stage_payload = (platform_summary or {}).get("positioning_card_analysis")
        if isinstance(stage_payload, dict) and stage_payload:
            stages[str(platform)] = dict(stage_payload)
    return stages


def _collect_downstream_artifact_path(downstream_summary: dict[str, Any], key: str) -> str:
    return str(((downstream_summary.get("artifacts") or {}).get(key) or "")).strip()


def _normalize_platform_filters(platform_filters: list[str] | None) -> list[str]:
    return normalize_platform_filters(platform_filters)


def _build_resolved_config_sources(
    *,
    env_file: str,
    task_upload_url: str,
    employee_info_url: str,
    feishu_app_id: str,
    feishu_app_secret: str,
    feishu_base_url: str,
    timeout_seconds: float,
    matching_strategy: str,
    brand_keyword: str,
    task_name: str,
    platform_filters: list[str] | None,
    vision_provider: str,
    max_identifiers_per_platform: int,
    mail_limit: int,
    mail_workers: int,
    sent_since: str,
    reset_state: bool,
    reuse_existing: bool,
    probe_vision_provider_only: bool,
    skip_scrape: bool,
    skip_visual: bool,
    skip_positioning_card_analysis: bool,
) -> tuple[dict[str, Any], dict[str, Any]]:
    return resolve_final_runner_config(
        env_file=env_file,
        task_upload_url=task_upload_url,
        employee_info_url=employee_info_url,
        feishu_app_id=feishu_app_id,
        feishu_app_secret=feishu_app_secret,
        feishu_base_url=feishu_base_url,
        timeout_seconds=timeout_seconds,
        matching_strategy=matching_strategy,
        brand_keyword=brand_keyword,
        task_name=task_name,
        platform_filters=platform_filters,
        vision_provider=vision_provider,
        max_identifiers_per_platform=max_identifiers_per_platform,
        mail_limit=mail_limit,
        mail_workers=mail_workers,
        sent_since=sent_since,
        reset_state=reset_state,
        reuse_existing=reuse_existing,
        probe_vision_provider_only=probe_vision_provider_only,
        skip_scrape=skip_scrape,
        skip_visual=skip_visual,
        skip_positioning_card_analysis=skip_positioning_card_analysis,
    )


def _build_final_runner_preflight(
    *,
    task_name: str,
    matching_strategy: str,
    run_root: Path,
    env_snapshot: Any,
    resolved_task_upload_url: Any,
    resolved_employee_info_url: Any,
    resolved_feishu_app_id: Any,
    resolved_feishu_app_secret: Any,
    requested_platforms: list[str],
) -> dict[str, Any]:
    errors: list[dict[str, Any]] = []
    run_root_target = inspect_directory_materialization_target(run_root)
    if not str(task_name or "").strip():
        errors.append(
            build_preflight_error(
                error_code="TASK_NAME_MISSING",
                message="缺少 task_name。",
                remediation="通过 `--task-name` 传入任务名后重试。",
            )
        )
    if str(matching_strategy or "").strip().lower() not in MATCHING_STRATEGIES:
        errors.append(
            build_preflight_error(
                error_code="MATCHING_STRATEGY_INVALID",
                message=f"不支持的 matching_strategy: {matching_strategy}",
                remediation=f"改用 {', '.join(MATCHING_STRATEGIES)} 之一后重试。",
                details={"matching_strategy": str(matching_strategy or "")},
            )
        )
    errors.extend(
        build_required_config_errors(
            [
                RequiredConfigSpec(
                    resolved=resolved_task_upload_url,
                    error_code="TASK_UPLOAD_URL_MISSING",
                    message="缺少 TASK_UPLOAD_URL。",
                    remediation="在 `.env` 或 `--task-upload-url` 中提供 TASK_UPLOAD_URL 后重试。",
                ),
                RequiredConfigSpec(
                    resolved=resolved_employee_info_url,
                    error_code="EMPLOYEE_INFO_URL_MISSING",
                    message="缺少 EMPLOYEE_INFO_URL。",
                    remediation="在 `.env` 或 `--employee-info-url` 中提供 EMPLOYEE_INFO_URL 后重试。",
                ),
                RequiredConfigSpec(
                    resolved=resolved_feishu_app_id,
                    error_code="FEISHU_APP_ID_MISSING",
                    message="缺少 FEISHU_APP_ID。",
                    remediation="在 `.env` 或 `--feishu-app-id` 中提供 FEISHU_APP_ID 后重试。",
                ),
                RequiredConfigSpec(
                    resolved=resolved_feishu_app_secret,
                    error_code="FEISHU_APP_SECRET_MISSING",
                    message="缺少 FEISHU_APP_SECRET。",
                    remediation="在 `.env` 或 `--feishu-app-secret` 中提供 FEISHU_APP_SECRET 后重试。",
                ),
            ]
        )
    )
    if not bool(run_root_target["materializable"]):
        errors.append(
            build_preflight_error(
                error_code="RUN_ROOT_UNAVAILABLE",
                message=f"run_root 无法创建: {run_root}",
                remediation="检查输出目录权限或显式传入可写的 `--output-root` 后重试。",
                details={
                    "path": str(run_root),
                    "nearest_existing_parent": str(run_root_target["nearest_existing_parent"]),
                },
            )
        )
    return build_preflight_payload(
        checks={
            "scope": "task-upload-to-final-export",
            "lightweight_only": True,
            "task_name_present": bool(str(task_name or "").strip()),
            "matching_strategy": str(matching_strategy or "").strip().lower(),
            "env_file_exists": bool(getattr(env_snapshot, "exists", False)),
            "task_upload_url_present": bool(getattr(resolved_task_upload_url, "present", False)),
            "employee_info_url_present": bool(getattr(resolved_employee_info_url, "present", False)),
            "feishu_app_id_present": bool(getattr(resolved_feishu_app_id, "present", False)),
            "feishu_app_secret_present": bool(getattr(resolved_feishu_app_secret, "present", False)),
            "requested_platforms": list(requested_platforms),
            "run_root_exists": run_root.exists(),
            "run_root_materializable": bool(run_root_target["materializable"]),
        },
        errors=errors,
    )


def _json_clone(payload: Any) -> Any:
    return json.loads(json.dumps(payload, ensure_ascii=False))


def _resolve_task_group_members(
    *,
    runtime: dict[str, Any],
    resolved_config: dict[str, Any],
    task_name: str,
) -> list[str]:
    resolved_task_upload_url = str(resolved_config["task_upload_url"].value or "").strip()
    if not resolved_task_upload_url:
        return [str(task_name or "").strip()]
    timeout_value = str(resolved_config["timeout_seconds"].value or "").strip()
    timeout_seconds = float(timeout_value or "30")
    base_url = str(
        resolved_config["feishu_base_url"].value or runtime["DEFAULT_FEISHU_BASE_URL"]
    ).strip() or runtime["DEFAULT_FEISHU_BASE_URL"]
    client = runtime["FeishuOpenClient"](
        app_id=str(resolved_config["feishu_app_id"].value or "").strip(),
        app_secret=str(resolved_config["feishu_app_secret"].value or "").strip(),
        base_url=base_url,
        timeout_seconds=timeout_seconds,
    )
    entries = runtime["resolve_task_upload_entries"](
        client=client,
        task_upload_url=resolved_task_upload_url,
        task_name=task_name,
    )
    resolved_task_names: list[str] = []
    seen: set[str] = set()
    for entry in entries:
        child_task_name = str(getattr(entry, "task_name", "") or "").strip()
        child_key = child_task_name.casefold()
        if not child_task_name or child_key in seen:
            continue
        seen.add(child_key)
        resolved_task_names.append(child_task_name)
    return resolved_task_names or [str(task_name or "").strip()]


def _summarize_child_run(summary: dict[str, Any]) -> dict[str, Any]:
    return {
        "task_name": str(summary.get("task_name") or "").strip(),
        "status": str(summary.get("status") or "").strip(),
        "delivery_status": str(summary.get("delivery_status") or "").strip(),
        "run_root": str(summary.get("run_root") or "").strip(),
        "summary_json": str(summary.get("summary_json") or "").strip(),
        "workflow_handoff_json": str(summary.get("workflow_handoff_json") or "").strip(),
        "error_code": str(summary.get("error_code") or "").strip(),
        "final_exports": _json_clone((summary.get("artifacts") or {}).get("final_exports") or {}),
        "all_platforms_final_review": str(
            ((summary.get("artifacts") or {}).get("all_platforms_final_review") or "")
        ).strip(),
    }


def _aggregate_fan_out_status(child_runs: list[dict[str, Any]]) -> str:
    statuses = [str(item.get("status") or "").strip() for item in child_runs if str(item.get("status") or "").strip()]
    if not statuses:
        return "failed"
    if any(status == "failed" for status in statuses):
        return "failed"
    if len(set(statuses)) == 1:
        return statuses[0]
    if "completed_with_platform_failures" in statuses:
        return "completed_with_platform_failures"
    if "completed_with_partial_scrape" in statuses:
        return "completed_with_partial_scrape"
    if "completed" in statuses:
        return "completed"
    if "staged_only" in statuses:
        return "staged_only"
    if "vision_probe_only" in statuses:
        return "vision_probe_only"
    return "completed"


def _run_single_task_upload_to_final_export_pipeline(
    *,
    task_name: str,
    env_file: str = ".env",
    task_upload_url: str = "",
    employee_info_url: str = "",
    output_root: Path | None = None,
    summary_json: Path | None = None,
    task_download_dir: str | Path = "",
    mail_data_dir: str | Path = "",
    existing_mail_db_path: str | Path = "",
    existing_mail_raw_dir: str | Path = "",
    existing_mail_data_dir: str | Path = "",
    feishu_app_id: str = "",
    feishu_app_secret: str = "",
    feishu_base_url: str = "",
    timeout_seconds: float = 0.0,
    folder_prefixes: list[str] | None = None,
    owner_email_overrides: dict[str, str] | None = None,
    imap_host: str = "",
    imap_port: int = 0,
    mail_limit: int = 0,
    mail_workers: int = 1,
    sent_since: str = "",
    reset_state: bool = False,
    reuse_existing: bool = True,
    matching_strategy: str = DEFAULT_MATCHING_STRATEGY,
    brand_keyword: str = "",
    brand_match_include_from: bool = False,
    base_url: str = "",
    api_key: str = "",
    model: str = "",
    wire_api: str = "",
    platform_filters: list[str] | None = None,
    vision_provider: str = "",
    max_identifiers_per_platform: int = 0,
    poll_interval: float = 5.0,
    creator_cache_db_path: str = "",
    force_refresh_creator_cache: bool = False,
    probe_vision_provider_only: bool = False,
    skip_scrape: bool = False,
    skip_visual: bool = False,
    skip_positioning_card_analysis: bool = False,
    _runtime_dependencies: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_task_name = str(task_name or "").strip()
    normalized_matching_strategy = str(matching_strategy or "").strip().lower() or DEFAULT_MATCHING_STRATEGY
    normalized_brand_keyword = str(brand_keyword or "").strip() or normalized_task_name
    requested_platforms = _normalize_platform_filters(platform_filters)
    runner_paths = resolve_final_runner_paths(
        task_name=normalized_task_name or "task",
        output_root=output_root,
        summary_json=summary_json,
    )
    resolved_output_root = runner_paths.output_root
    upstream_output_root = runner_paths.upstream_output_root
    downstream_output_root = runner_paths.downstream_output_root
    upstream_summary_path = runner_paths.upstream_summary_json
    downstream_summary_path = runner_paths.downstream_summary_json
    task_spec_path = runner_paths.task_spec_json
    run_summary_path = runner_paths.summary_json
    resolved_config_sources, resolved_config = _build_resolved_config_sources(
        env_file=env_file,
        task_upload_url=task_upload_url,
        employee_info_url=employee_info_url,
        feishu_app_id=feishu_app_id,
        feishu_app_secret=feishu_app_secret,
        feishu_base_url=feishu_base_url,
        timeout_seconds=timeout_seconds,
        matching_strategy=normalized_matching_strategy,
        brand_keyword=brand_keyword,
        task_name=normalized_task_name,
        platform_filters=platform_filters,
        vision_provider=vision_provider,
        max_identifiers_per_platform=max_identifiers_per_platform,
        mail_limit=mail_limit,
        mail_workers=mail_workers,
        sent_since=sent_since,
        reset_state=reset_state,
        reuse_existing=reuse_existing,
        probe_vision_provider_only=probe_vision_provider_only,
        skip_scrape=skip_scrape,
        skip_visual=skip_visual,
        skip_positioning_card_analysis=skip_positioning_card_analysis,
    )
    preflight = _build_final_runner_preflight(
        task_name=normalized_task_name,
        matching_strategy=normalized_matching_strategy,
        run_root=runner_paths.run_root,
        env_snapshot=resolved_config["env_snapshot"],
        resolved_task_upload_url=resolved_config["task_upload_url"],
        resolved_employee_info_url=resolved_config["employee_info_url"],
        resolved_feishu_app_id=resolved_config["feishu_app_id"],
        resolved_feishu_app_secret=resolved_config["feishu_app_secret"],
        requested_platforms=requested_platforms,
    )

    started_at = iso_now()
    task_spec = build_final_runner_task_spec(
        generated_at=started_at,
        runner_paths=runner_paths,
        env_snapshot=resolved_config["env_snapshot"],
        env_file_raw=str(env_file),
        resolved_config_sources=resolved_config_sources,
        task_name=normalized_task_name,
        task_upload_url=resolved_config["task_upload_url"].value,
        employee_info_url=resolved_config["employee_info_url"].value,
        task_download_dir=str(task_download_dir or "").strip(),
        mail_data_dir=str(mail_data_dir or "").strip(),
        existing_mail_db_path=str(existing_mail_db_path or "").strip(),
        existing_mail_raw_dir=str(existing_mail_raw_dir or "").strip(),
        existing_mail_data_dir=str(existing_mail_data_dir or "").strip(),
        owner_email_overrides=dict(owner_email_overrides or {}),
        matching_strategy=normalized_matching_strategy,
        brand_keyword=normalized_brand_keyword,
        brand_match_include_from=bool(brand_match_include_from),
        mail_limit=int(max(0, int(mail_limit))),
        mail_workers=int(max(1, int(mail_workers))),
        sent_since=str(sent_since or "").strip(),
        reset_state=bool(reset_state),
        reuse_existing=bool(reuse_existing),
        requested_platforms=requested_platforms,
        vision_provider=str(vision_provider or "").strip().lower(),
        max_identifiers_per_platform=int(max(0, int(max_identifiers_per_platform))),
        poll_interval=max(1.0, float(poll_interval)),
        creator_cache_db_path=str(creator_cache_db_path or "").strip(),
        force_refresh_creator_cache=bool(force_refresh_creator_cache),
        probe_vision_provider_only=bool(probe_vision_provider_only),
        skip_scrape=bool(skip_scrape),
        skip_visual=bool(skip_visual),
        skip_positioning_card_analysis=bool(skip_positioning_card_analysis),
    )

    summary: dict[str, Any] = {
        "started_at": started_at,
        "finished_at": "",
        "status": "running" if preflight["ready"] else "failed",
        "run_id": runner_paths.run_id,
        "run_root": str(runner_paths.run_root),
        "task_name": normalized_task_name,
        "env_file_raw": str(env_file),
        "env_file": str(resolved_config["env_snapshot"].path),
        "output_root": str(resolved_output_root),
        "summary_json": str(run_summary_path),
        "task_spec_json": str(task_spec_path),
        "workflow_handoff_json": str(runner_paths.workflow_handoff_json),
        "resolved_config_sources": resolved_config_sources,
        "matching_strategy": normalized_matching_strategy,
        "brand_keyword": normalized_brand_keyword,
        "inputs": {
            "task_upload_url": str(task_upload_url or "").strip(),
            "employee_info_url": str(employee_info_url or "").strip(),
            "task_download_dir": str(task_download_dir or "").strip(),
            "mail_data_dir": str(mail_data_dir or "").strip(),
            "existing_mail_db_path": str(existing_mail_db_path or "").strip(),
            "existing_mail_raw_dir": str(existing_mail_raw_dir or "").strip(),
            "existing_mail_data_dir": str(existing_mail_data_dir or "").strip(),
            "owner_email_overrides": dict(owner_email_overrides or {}),
            "mail_limit": int(max(0, int(mail_limit))),
            "mail_workers": int(max(1, int(mail_workers))),
            "sent_since": str(sent_since or "").strip(),
            "reset_state": bool(reset_state),
            "reuse_existing": bool(reuse_existing),
            "creator_cache_db_path": str(creator_cache_db_path or "").strip(),
            "force_refresh_creator_cache": bool(force_refresh_creator_cache),
        },
        "resolved_inputs": {
            "env_file": {
                "path": str(resolved_config["env_snapshot"].path),
                "exists": resolved_config["env_snapshot"].exists,
                "source": resolved_config["env_snapshot"].source,
            },
        },
        "preflight": preflight,
        "bounded_controls": {
            "upstream": {
                "matching_strategy": normalized_matching_strategy,
                "brand_keyword": normalized_brand_keyword,
                "brand_match_include_from": bool(brand_match_include_from),
                "mail_limit": int(max(0, int(mail_limit))),
                "mail_workers": int(max(1, int(mail_workers))),
                "sent_since": str(sent_since or "").strip(),
                "reuse_existing": bool(reuse_existing),
            },
            "downstream": {
                "platform_filters": requested_platforms,
                "vision_provider": str(vision_provider or "").strip().lower(),
                "max_identifiers_per_platform": int(max(0, int(max_identifiers_per_platform))),
                "poll_interval": max(1.0, float(poll_interval)),
                "creator_cache_db_path": str(creator_cache_db_path or "").strip(),
                "force_refresh_creator_cache": bool(force_refresh_creator_cache),
                "probe_vision_provider_only": bool(probe_vision_provider_only),
                "skip_scrape": bool(skip_scrape),
                "skip_visual": bool(skip_visual),
                "skip_positioning_card_analysis": bool(skip_positioning_card_analysis),
            },
        },
        "resolved_paths": {
            "run_root": str(runner_paths.run_root),
            "output_root": str(resolved_output_root),
            "task_spec_json": str(task_spec_path),
            "workflow_handoff_json": str(runner_paths.workflow_handoff_json),
            "upstream_output_root": str(upstream_output_root),
            "upstream_summary_json": str(upstream_summary_path),
            "upstream_task_spec_json": str(runner_paths.upstream_task_spec_json),
            "upstream_workflow_handoff_json": str(runner_paths.upstream_workflow_handoff_json),
            "downstream_output_root": str(downstream_output_root),
            "downstream_summary_json": str(downstream_summary_path),
            "downstream_task_spec_json": str(runner_paths.downstream_task_spec_json),
            "downstream_workflow_handoff_json": str(runner_paths.downstream_workflow_handoff_json),
        },
        "contract": {
            "scope": "task-upload-to-final-export",
            "upstream_runner": "scripts/run_task_upload_to_keep_list_pipeline.py",
            "downstream_runner": "scripts/run_keep_list_screening_pipeline.py",
            "canonical_internal_boundary": "keep-list",
            "canonical_resume_point": "keep_list",
        },
        "setup": {
            "scope": "task-upload-to-final-export",
            "completed": False,
            "skipped": not preflight["ready"],
            "errors": [],
        },
        "steps": {},
        "artifacts": {
            "upstream_summary_json": str(upstream_summary_path),
            "downstream_summary_json": str(downstream_summary_path),
            "keep_workbook": "",
            "template_workbook": "",
            "template_prepare_summary_json": "",
            "template_runtime_prompt_artifacts_json": "",
            "all_platforms_final_review": "",
            "all_platforms_upload_payload_json": "",
            "final_exports": {},
            "positioning_artifacts": {},
        },
        "resume_points": {},
    }
    attach_run_contract(summary)

    def persist_summary(payload: dict[str, Any]) -> None:
        _write_summary(run_summary_path, payload)
        write_workflow_handoff(
            runner_paths.workflow_handoff_json,
            summary=payload,
            task_spec=task_spec,
            task_spec_available=bool(payload.get("setup", {}).get("completed")),
        )

    def finalize(status: str, **extra: Any) -> dict[str, Any]:
        summary["status"] = status
        summary["finished_at"] = iso_now()
        summary.update(extra)
        failure = extra.get("failure")
        if isinstance(failure, dict):
            attach_failure_to_summary(summary, failure)
        attach_run_contract(summary)
        persist_summary(summary)
        return summary

    if not preflight["ready"]:
        failure = preflight["errors"][0]
        return finalize(
            "failed",
            failure={**failure, "failure_layer": "preflight"},
        )

    setup = materialize_setup(
        scope="task-upload-to-final-export",
        directories=[
            {
                "label": "run_root",
                "path": runner_paths.run_root,
                "error_code": "RUN_ROOT_UNAVAILABLE",
                "message": "run_root 无法创建: {path}",
                "remediation": "检查输出目录权限或显式传入可写的 `--output-root` 后重试。",
            },
            {
                "label": "upstream_output_root",
                "path": upstream_output_root,
                "error_code": "UPSTREAM_OUTPUT_ROOT_UNAVAILABLE",
                "message": "upstream_output_root 无法创建: {path}",
                "remediation": "检查输出目录权限后重试。",
            },
            {
                "label": "downstream_output_root",
                "path": downstream_output_root,
                "error_code": "DOWNSTREAM_OUTPUT_ROOT_UNAVAILABLE",
                "message": "downstream_output_root 无法创建: {path}",
                "remediation": "检查输出目录权限后重试。",
            },
        ],
        files=[
            {
                "label": "task_spec",
                "path": task_spec_path,
                "writer": lambda path: write_task_spec(path, task_spec),
                "error_code": "TASK_SPEC_WRITE_FAILED",
                "message": "task_spec 无法写入: {path}",
                "remediation": "检查 run root 权限或 task spec 序列化逻辑后重试。",
            }
        ],
    )
    summary["setup"] = {**setup, "skipped": False}
    if not setup["completed"]:
        failure = setup["errors"][0]
        return finalize(
            "failed",
            failure=failure,
        )
    summary["resolved_paths"]["run_root_exists"] = runner_paths.run_root.exists()
    persist_summary(summary)

    runtime = _runtime_dependencies
    if runtime is None:
        try:
            runtime = _load_runtime_dependencies()
        except Exception as exc:  # noqa: BLE001
            failure = _build_failure_payload(
                stage="runtime_import",
                error_code="FINAL_RUNNER_RUNTIME_IMPORT_FAILED",
                message=f"final runner runtime 加载失败: {exc}",
                remediation="检查 final runner 的本地依赖与脚本导入链后重试。",
                details={"exception_type": exc.__class__.__name__},
            )
            return finalize("failed", failure=failure)
    run_upstream = runtime["run_task_upload_to_keep_list_pipeline"]
    run_downstream = runtime["run_keep_list_screening_pipeline"]

    try:
        upstream_summary = run_upstream(
            task_name=normalized_task_name,
            env_file=env_file,
            task_upload_url=task_upload_url,
            employee_info_url=employee_info_url,
            output_root=upstream_output_root,
            summary_json=upstream_summary_path,
            task_download_dir=task_download_dir,
            mail_data_dir=mail_data_dir,
            existing_mail_db_path=existing_mail_db_path,
            existing_mail_raw_dir=existing_mail_raw_dir,
            existing_mail_data_dir=existing_mail_data_dir,
            feishu_app_id=feishu_app_id,
            feishu_app_secret=feishu_app_secret,
            feishu_base_url=feishu_base_url,
            timeout_seconds=timeout_seconds,
            folder_prefixes=folder_prefixes,
            owner_email_overrides=owner_email_overrides,
            imap_host=imap_host,
            imap_port=imap_port,
            mail_limit=max(0, int(mail_limit)),
            mail_workers=max(1, int(mail_workers)),
            sent_since=sent_since,
            reset_state=bool(reset_state),
            stop_after="keep-list",
            reuse_existing=bool(reuse_existing),
            matching_strategy=normalized_matching_strategy,
            brand_keyword=normalized_brand_keyword,
            brand_match_include_from=bool(brand_match_include_from),
            base_url=base_url,
            api_key=api_key,
            model=model,
            wire_api=wire_api,
        )
    except Exception as exc:  # noqa: BLE001
        failure = _build_failure_payload(
            stage="upstream",
            error_code="TASK_UPLOAD_TO_KEEP_LIST_FAILED",
            message=str(exc) or exc.__class__.__name__,
            remediation="检查上游 runner 的 summary、env、任务上传依赖和邮件同步日志后重试。",
            details={"exception_type": exc.__class__.__name__},
        )
        return finalize("failed", failure=failure)

    keep_list_resume = ((upstream_summary.get("resume_points") or {}).get("keep_list") or {})
    keep_workbook = str(
        keep_list_resume.get("keep_workbook")
        or (upstream_summary.get("artifacts") or {}).get("keep_workbook")
        or ""
    ).strip()
    template_workbook = str(
        keep_list_resume.get("template_workbook")
        or (upstream_summary.get("artifacts") or {}).get("template_workbook")
        or ""
    ).strip()

    summary["steps"]["upstream"] = {
        "status": upstream_summary.get("status"),
        "summary_json": str(upstream_summary_path),
        "output_root": str(upstream_output_root),
        "canonical_boundary": ((upstream_summary.get("contract") or {}).get("canonical_boundary") or "keep-list"),
        "keep_workbook": keep_workbook,
        "template_workbook": template_workbook,
        "template_prepare_summary_json": str((upstream_summary.get("artifacts") or {}).get("template_prepare_summary_json") or "").strip(),
        "template_runtime_prompt_artifacts_json": str((upstream_summary.get("artifacts") or {}).get("template_runtime_prompt_artifacts_json") or "").strip(),
        "downstream_handoff": upstream_summary.get("downstream_handoff") or {},
    }
    summary["artifacts"]["keep_workbook"] = keep_workbook
    summary["artifacts"]["template_workbook"] = template_workbook
    summary["artifacts"]["template_prepare_summary_json"] = summary["steps"]["upstream"]["template_prepare_summary_json"]
    summary["artifacts"]["template_runtime_prompt_artifacts_json"] = summary["steps"]["upstream"]["template_runtime_prompt_artifacts_json"]
    task_owner_context = extract_task_owner_context(upstream_summary)
    summary["resume_points"]["keep_list"] = {
        "keep_workbook": keep_workbook,
        "template_workbook": template_workbook,
        "upstream_summary_json": str(upstream_summary_path),
        "recommended_command": _build_keep_list_resume_command(
            keep_workbook=keep_workbook,
            template_workbook=template_workbook,
            task_name=normalized_task_name,
            task_upload_url=str(task_upload_url or "").strip(),
            env_file=env_file,
            requested_platforms=requested_platforms,
            vision_provider=str(vision_provider or "").strip().lower(),
            max_identifiers_per_platform=int(max(0, int(max_identifiers_per_platform))),
            poll_interval=max(1.0, float(poll_interval)),
            creator_cache_db_path=str(creator_cache_db_path or "").strip(),
            force_refresh_creator_cache=bool(force_refresh_creator_cache),
            probe_vision_provider_only=bool(probe_vision_provider_only),
            skip_scrape=bool(skip_scrape),
            skip_visual=bool(skip_visual),
            skip_positioning_card_analysis=bool(skip_positioning_card_analysis),
        ),
    }
    persist_summary(summary)

    if str(upstream_summary.get("status") or "") == "failed":
        failure = _build_failure_payload(
            stage="upstream",
            error_code=str(upstream_summary.get("error_code") or "TASK_UPLOAD_TO_KEEP_LIST_FAILED"),
            message=str(upstream_summary.get("error") or "上游 keep-list 运行失败"),
            remediation="打开上游 summary，先修复 task upload -> keep-list 的失败，再继续下游。",
            details={"upstream_summary_json": str(upstream_summary_path)},
        )
        return finalize("failed", failure=failure)

    if not keep_workbook or not Path(keep_workbook).exists():
        failure = _build_failure_payload(
            stage="upstream",
            error_code="KEEP_LIST_ARTIFACT_MISSING",
            message="上游 runner 没有留下可用的 keep workbook，无法继续下游。",
            remediation="检查上游 summary 的 `resume_points.keep_list.keep_workbook` 和 `artifacts.keep_workbook` 是否存在。",
            details={
                "keep_workbook": keep_workbook,
                "upstream_summary_json": str(upstream_summary_path),
            },
        )
        return finalize("failed", failure=failure)

    try:
        downstream_summary = run_downstream(
            keep_workbook=Path(keep_workbook),
            template_workbook=Path(template_workbook) if template_workbook else None,
            task_name=normalized_task_name,
            task_upload_url=str(task_upload_url or "").strip(),
            env_file=env_file,
            output_root=downstream_output_root,
            summary_json=downstream_summary_path,
            platform_filters=requested_platforms or None,
            vision_provider=str(vision_provider or "").strip().lower(),
            max_identifiers_per_platform=int(max(0, int(max_identifiers_per_platform))),
            poll_interval=max(1.0, float(poll_interval)),
            creator_cache_db_path=str(creator_cache_db_path or "").strip(),
            force_refresh_creator_cache=bool(force_refresh_creator_cache),
            probe_vision_provider_only=bool(probe_vision_provider_only),
            skip_scrape=bool(skip_scrape),
            skip_visual=bool(skip_visual),
            skip_positioning_card_analysis=bool(skip_positioning_card_analysis),
            task_owner_name=str(task_owner_context.get("responsible_name") or "").strip(),
            task_owner_employee_id=str(task_owner_context.get("employee_id") or "").strip(),
            task_owner_employee_record_id=str(task_owner_context.get("employee_record_id") or "").strip(),
            task_owner_employee_email=str(task_owner_context.get("employee_email") or "").strip(),
            task_owner_owner_name=str(task_owner_context.get("owner_name") or "").strip(),
            linked_bitable_url=str(task_owner_context.get("linked_bitable_url") or "").strip(),
        )
    except Exception as exc:  # noqa: BLE001
        failure = _build_failure_payload(
            stage="downstream",
            error_code="KEEP_LIST_TO_FINAL_EXPORT_FAILED",
            message=str(exc) or exc.__class__.__name__,
            remediation="检查下游 runner summary、vision preflight、平台 job 和导出状态后重试。",
            details={
                "exception_type": exc.__class__.__name__,
                "keep_workbook": keep_workbook,
            },
        )
        return finalize("failed", failure=failure)

    final_exports = _collect_final_exports(downstream_summary)
    positioning_artifacts = _collect_positioning_artifacts(downstream_summary)
    positioning_stage_summaries = _collect_positioning_stage_summaries(downstream_summary)
    all_platforms_final_review = _collect_downstream_artifact_path(downstream_summary, "all_platforms_final_review")
    all_platforms_upload_payload_json = _collect_downstream_artifact_path(
        downstream_summary,
        "all_platforms_upload_payload_json",
    )
    summary["steps"]["downstream"] = {
        "status": downstream_summary.get("status"),
        "summary_json": str(downstream_summary_path),
        "output_root": str(downstream_output_root),
        "requested_platforms": requested_platforms,
        "final_exports": final_exports,
        "all_platforms_final_review": all_platforms_final_review,
        "all_platforms_upload_payload_json": all_platforms_upload_payload_json,
        "positioning_artifacts": positioning_artifacts,
        "positioning_card_analysis": positioning_stage_summaries,
        "platform_statuses": _collect_platform_statuses(downstream_summary),
        "vision_probe": downstream_summary.get("vision_probe") or {},
    }
    summary["artifacts"]["final_exports"] = final_exports
    summary["artifacts"]["all_platforms_final_review"] = all_platforms_final_review
    summary["artifacts"]["all_platforms_upload_payload_json"] = all_platforms_upload_payload_json
    summary["artifacts"]["positioning_artifacts"] = positioning_artifacts
    summary["artifacts"]["all_platforms_upload_local_archive_dir"] = str(
        (downstream_summary.get("artifacts") or {}).get("all_platforms_upload_local_archive_dir") or ""
    )
    summary["artifacts"]["all_platforms_upload_skipped_archive_json"] = str(
        (downstream_summary.get("artifacts") or {}).get("all_platforms_upload_skipped_archive_json") or ""
    )
    summary["artifacts"]["all_platforms_upload_skipped_archive_xlsx"] = str(
        (downstream_summary.get("artifacts") or {}).get("all_platforms_upload_skipped_archive_xlsx") or ""
    )
    summary["artifacts"]["all_platforms_upload_row_count"] = int(
        (downstream_summary.get("artifacts") or {}).get("all_platforms_upload_row_count") or 0
    )
    summary["artifacts"]["all_platforms_upload_source_row_count"] = int(
        (downstream_summary.get("artifacts") or {}).get("all_platforms_upload_source_row_count") or 0
    )
    summary["artifacts"]["all_platforms_upload_skipped_row_count"] = int(
        (downstream_summary.get("artifacts") or {}).get("all_platforms_upload_skipped_row_count") or 0
    )

    downstream_status = str(downstream_summary.get("status") or "")
    if downstream_status not in SUCCESSFUL_DOWNSTREAM_STATUSES:
        failure = _build_failure_payload(
            stage="downstream",
            error_code=str(downstream_summary.get("error_code") or f"DOWNSTREAM_{downstream_status.upper() or 'FAILED'}"),
            message=str(
                downstream_summary.get("error")
                or f"下游 final export 未完成，最终状态为 {downstream_status or 'unknown'}。"
            ),
            remediation="检查下游 summary、vision preflight、平台 job 和导出状态后重试。",
            details={"downstream_summary_json": str(downstream_summary_path)},
        )
        return finalize("failed", failure=failure)

    summary["delivery_status"] = downstream_status or "completed"
    return finalize(downstream_status or "completed")


def run_task_upload_to_final_export_pipeline(
    *,
    task_name: str,
    env_file: str = ".env",
    task_upload_url: str = "",
    employee_info_url: str = "",
    output_root: Path | None = None,
    summary_json: Path | None = None,
    task_download_dir: str | Path = "",
    mail_data_dir: str | Path = "",
    existing_mail_db_path: str | Path = "",
    existing_mail_raw_dir: str | Path = "",
    existing_mail_data_dir: str | Path = "",
    feishu_app_id: str = "",
    feishu_app_secret: str = "",
    feishu_base_url: str = "",
    timeout_seconds: float = 0.0,
    folder_prefixes: list[str] | None = None,
    owner_email_overrides: dict[str, str] | None = None,
    imap_host: str = "",
    imap_port: int = 0,
    mail_limit: int = 0,
    mail_workers: int = 1,
    sent_since: str = "",
    reset_state: bool = False,
    reuse_existing: bool = True,
    matching_strategy: str = DEFAULT_MATCHING_STRATEGY,
    brand_keyword: str = "",
    brand_match_include_from: bool = False,
    base_url: str = "",
    api_key: str = "",
    model: str = "",
    wire_api: str = "",
    platform_filters: list[str] | None = None,
    vision_provider: str = "",
    max_identifiers_per_platform: int = 0,
    poll_interval: float = 5.0,
    creator_cache_db_path: str = "",
    force_refresh_creator_cache: bool = False,
    probe_vision_provider_only: bool = False,
    skip_scrape: bool = False,
    skip_visual: bool = False,
    skip_positioning_card_analysis: bool = False,
    _runtime_dependencies: dict[str, Any] | None = None,
    _allow_task_group_fan_out: bool = True,
) -> dict[str, Any]:
    if not _allow_task_group_fan_out:
        return _run_single_task_upload_to_final_export_pipeline(
            task_name=task_name,
            env_file=env_file,
            task_upload_url=task_upload_url,
            employee_info_url=employee_info_url,
            output_root=output_root,
            summary_json=summary_json,
            task_download_dir=task_download_dir,
            mail_data_dir=mail_data_dir,
            existing_mail_db_path=existing_mail_db_path,
            existing_mail_raw_dir=existing_mail_raw_dir,
            existing_mail_data_dir=existing_mail_data_dir,
            feishu_app_id=feishu_app_id,
            feishu_app_secret=feishu_app_secret,
            feishu_base_url=feishu_base_url,
            timeout_seconds=timeout_seconds,
            folder_prefixes=folder_prefixes,
            owner_email_overrides=owner_email_overrides,
            imap_host=imap_host,
            imap_port=imap_port,
            mail_limit=mail_limit,
            mail_workers=mail_workers,
            sent_since=sent_since,
            reset_state=reset_state,
            reuse_existing=reuse_existing,
            matching_strategy=matching_strategy,
            brand_keyword=brand_keyword,
            brand_match_include_from=brand_match_include_from,
            base_url=base_url,
            api_key=api_key,
            model=model,
            wire_api=wire_api,
            platform_filters=platform_filters,
            vision_provider=vision_provider,
            max_identifiers_per_platform=max_identifiers_per_platform,
            poll_interval=poll_interval,
            creator_cache_db_path=creator_cache_db_path,
            force_refresh_creator_cache=force_refresh_creator_cache,
            probe_vision_provider_only=probe_vision_provider_only,
            skip_scrape=skip_scrape,
            skip_visual=skip_visual,
            skip_positioning_card_analysis=skip_positioning_card_analysis,
            _runtime_dependencies=_runtime_dependencies,
        )

    normalized_task_name = str(task_name or "").strip()
    normalized_matching_strategy = str(matching_strategy or "").strip().lower() or DEFAULT_MATCHING_STRATEGY
    normalized_brand_keyword = str(brand_keyword or "").strip() or normalized_task_name
    requested_platforms = _normalize_platform_filters(platform_filters)
    resolved_config_sources, resolved_config = _build_resolved_config_sources(
        env_file=env_file,
        task_upload_url=task_upload_url,
        employee_info_url=employee_info_url,
        feishu_app_id=feishu_app_id,
        feishu_app_secret=feishu_app_secret,
        feishu_base_url=feishu_base_url,
        timeout_seconds=timeout_seconds,
        matching_strategy=normalized_matching_strategy,
        brand_keyword=brand_keyword,
        task_name=normalized_task_name,
        platform_filters=platform_filters,
        vision_provider=vision_provider,
        max_identifiers_per_platform=max_identifiers_per_platform,
        mail_limit=mail_limit,
        mail_workers=mail_workers,
        sent_since=sent_since,
        reset_state=reset_state,
        reuse_existing=reuse_existing,
        probe_vision_provider_only=probe_vision_provider_only,
        skip_scrape=skip_scrape,
        skip_visual=skip_visual,
        skip_positioning_card_analysis=skip_positioning_card_analysis,
    )
    preflight = _build_final_runner_preflight(
        task_name=normalized_task_name,
        matching_strategy=normalized_matching_strategy,
        run_root=resolve_final_runner_paths(
            task_name=normalized_task_name or "task",
            output_root=output_root,
            summary_json=summary_json,
        ).run_root,
        env_snapshot=resolved_config["env_snapshot"],
        resolved_task_upload_url=resolved_config["task_upload_url"],
        resolved_employee_info_url=resolved_config["employee_info_url"],
        resolved_feishu_app_id=resolved_config["feishu_app_id"],
        resolved_feishu_app_secret=resolved_config["feishu_app_secret"],
        requested_platforms=requested_platforms,
    )
    if not preflight["ready"]:
        return _run_single_task_upload_to_final_export_pipeline(
            task_name=task_name,
            env_file=env_file,
            task_upload_url=task_upload_url,
            employee_info_url=employee_info_url,
            output_root=output_root,
            summary_json=summary_json,
            task_download_dir=task_download_dir,
            mail_data_dir=mail_data_dir,
            existing_mail_db_path=existing_mail_db_path,
            existing_mail_raw_dir=existing_mail_raw_dir,
            existing_mail_data_dir=existing_mail_data_dir,
            feishu_app_id=feishu_app_id,
            feishu_app_secret=feishu_app_secret,
            feishu_base_url=feishu_base_url,
            timeout_seconds=timeout_seconds,
            folder_prefixes=folder_prefixes,
            owner_email_overrides=owner_email_overrides,
            imap_host=imap_host,
            imap_port=imap_port,
            mail_limit=mail_limit,
            mail_workers=mail_workers,
            sent_since=sent_since,
            reset_state=reset_state,
            reuse_existing=reuse_existing,
            matching_strategy=matching_strategy,
            brand_keyword=brand_keyword,
            brand_match_include_from=brand_match_include_from,
            base_url=base_url,
            api_key=api_key,
            model=model,
            wire_api=wire_api,
            platform_filters=platform_filters,
            vision_provider=vision_provider,
            max_identifiers_per_platform=max_identifiers_per_platform,
            poll_interval=poll_interval,
            creator_cache_db_path=creator_cache_db_path,
            force_refresh_creator_cache=force_refresh_creator_cache,
            probe_vision_provider_only=probe_vision_provider_only,
            skip_scrape=skip_scrape,
            skip_visual=skip_visual,
            skip_positioning_card_analysis=skip_positioning_card_analysis,
            _runtime_dependencies=_runtime_dependencies,
        )

    runtime = _runtime_dependencies
    if runtime is None:
        try:
            runtime = _load_runtime_dependencies()
        except Exception as exc:  # noqa: BLE001
            return _run_single_task_upload_to_final_export_pipeline(
                task_name=task_name,
                env_file=env_file,
                task_upload_url=task_upload_url,
                employee_info_url=employee_info_url,
                output_root=output_root,
                summary_json=summary_json,
                task_download_dir=task_download_dir,
                mail_data_dir=mail_data_dir,
                existing_mail_db_path=existing_mail_db_path,
                existing_mail_raw_dir=existing_mail_raw_dir,
                existing_mail_data_dir=existing_mail_data_dir,
                feishu_app_id=feishu_app_id,
                feishu_app_secret=feishu_app_secret,
                feishu_base_url=feishu_base_url,
                timeout_seconds=timeout_seconds,
                folder_prefixes=folder_prefixes,
                owner_email_overrides=owner_email_overrides,
                imap_host=imap_host,
                imap_port=imap_port,
                mail_limit=mail_limit,
                mail_workers=mail_workers,
                sent_since=sent_since,
                reset_state=reset_state,
                reuse_existing=reuse_existing,
                matching_strategy=matching_strategy,
                brand_keyword=brand_keyword,
                brand_match_include_from=brand_match_include_from,
                base_url=base_url,
                api_key=api_key,
                model=model,
                wire_api=wire_api,
                platform_filters=platform_filters,
                vision_provider=vision_provider,
                max_identifiers_per_platform=max_identifiers_per_platform,
                poll_interval=poll_interval,
                creator_cache_db_path=creator_cache_db_path,
                force_refresh_creator_cache=force_refresh_creator_cache,
                probe_vision_provider_only=probe_vision_provider_only,
                skip_scrape=skip_scrape,
                skip_visual=skip_visual,
                skip_positioning_card_analysis=skip_positioning_card_analysis,
                _runtime_dependencies=_runtime_dependencies,
            )

    try:
        resolved_task_names = _resolve_task_group_members(
            runtime=runtime,
            resolved_config=resolved_config,
            task_name=normalized_task_name,
        )
    except Exception:
        resolved_task_names = [normalized_task_name]
    if len(resolved_task_names) <= 1:
        return _run_single_task_upload_to_final_export_pipeline(
            task_name=task_name,
            env_file=env_file,
            task_upload_url=task_upload_url,
            employee_info_url=employee_info_url,
            output_root=output_root,
            summary_json=summary_json,
            task_download_dir=task_download_dir,
            mail_data_dir=mail_data_dir,
            existing_mail_db_path=existing_mail_db_path,
            existing_mail_raw_dir=existing_mail_raw_dir,
            existing_mail_data_dir=existing_mail_data_dir,
            feishu_app_id=feishu_app_id,
            feishu_app_secret=feishu_app_secret,
            feishu_base_url=feishu_base_url,
            timeout_seconds=timeout_seconds,
            folder_prefixes=folder_prefixes,
            owner_email_overrides=owner_email_overrides,
            imap_host=imap_host,
            imap_port=imap_port,
            mail_limit=mail_limit,
            mail_workers=mail_workers,
            sent_since=sent_since,
            reset_state=reset_state,
            reuse_existing=reuse_existing,
            matching_strategy=matching_strategy,
            brand_keyword=brand_keyword,
            brand_match_include_from=brand_match_include_from,
            base_url=base_url,
            api_key=api_key,
            model=model,
            wire_api=wire_api,
            platform_filters=platform_filters,
            vision_provider=vision_provider,
            max_identifiers_per_platform=max_identifiers_per_platform,
            poll_interval=poll_interval,
            creator_cache_db_path=creator_cache_db_path,
            force_refresh_creator_cache=force_refresh_creator_cache,
            probe_vision_provider_only=probe_vision_provider_only,
            skip_scrape=skip_scrape,
            skip_visual=skip_visual,
            skip_positioning_card_analysis=skip_positioning_card_analysis,
            _runtime_dependencies=runtime,
        )

    runner_paths = resolve_final_runner_paths(
        task_name=normalized_task_name or "task",
        output_root=output_root,
        summary_json=summary_json,
    )
    started_at = iso_now()
    task_spec = build_final_runner_task_spec(
        generated_at=started_at,
        runner_paths=runner_paths,
        env_snapshot=resolved_config["env_snapshot"],
        env_file_raw=str(env_file),
        resolved_config_sources=resolved_config_sources,
        task_name=normalized_task_name,
        task_upload_url=resolved_config["task_upload_url"].value,
        employee_info_url=resolved_config["employee_info_url"].value,
        task_download_dir=str(task_download_dir or "").strip(),
        mail_data_dir=str(mail_data_dir or "").strip(),
        existing_mail_db_path=str(existing_mail_db_path or "").strip(),
        existing_mail_raw_dir=str(existing_mail_raw_dir or "").strip(),
        existing_mail_data_dir=str(existing_mail_data_dir or "").strip(),
        owner_email_overrides=dict(owner_email_overrides or {}),
        matching_strategy=normalized_matching_strategy,
        brand_keyword=normalized_brand_keyword,
        brand_match_include_from=bool(brand_match_include_from),
        mail_limit=int(max(0, int(mail_limit))),
        mail_workers=int(max(1, int(mail_workers))),
        sent_since=str(sent_since or "").strip(),
        reset_state=bool(reset_state),
        reuse_existing=bool(reuse_existing),
        requested_platforms=requested_platforms,
        vision_provider=str(vision_provider or "").strip().lower(),
        max_identifiers_per_platform=int(max(0, int(max_identifiers_per_platform))),
        poll_interval=max(1.0, float(poll_interval)),
        creator_cache_db_path=str(creator_cache_db_path or "").strip(),
        force_refresh_creator_cache=bool(force_refresh_creator_cache),
        probe_vision_provider_only=bool(probe_vision_provider_only),
        skip_scrape=bool(skip_scrape),
        skip_visual=bool(skip_visual),
        skip_positioning_card_analysis=bool(skip_positioning_card_analysis),
    )
    summary: dict[str, Any] = {
        "started_at": started_at,
        "finished_at": "",
        "status": "running",
        "run_id": runner_paths.run_id,
        "run_root": str(runner_paths.run_root),
        "task_name": normalized_task_name,
        "env_file_raw": str(env_file),
        "env_file": str(resolved_config["env_snapshot"].path),
        "output_root": str(runner_paths.output_root),
        "summary_json": str(runner_paths.summary_json),
        "task_spec_json": str(runner_paths.task_spec_json),
        "workflow_handoff_json": str(runner_paths.workflow_handoff_json),
        "resolved_config_sources": resolved_config_sources,
        "matching_strategy": normalized_matching_strategy,
        "brand_keyword": normalized_brand_keyword,
        "inputs": {
            "task_upload_url": str(task_upload_url or "").strip(),
            "employee_info_url": str(employee_info_url or "").strip(),
            "task_download_dir": str(task_download_dir or "").strip(),
            "mail_data_dir": str(mail_data_dir or "").strip(),
            "existing_mail_db_path": str(existing_mail_db_path or "").strip(),
            "existing_mail_raw_dir": str(existing_mail_raw_dir or "").strip(),
            "existing_mail_data_dir": str(existing_mail_data_dir or "").strip(),
            "owner_email_overrides": dict(owner_email_overrides or {}),
            "mail_limit": int(max(0, int(mail_limit))),
            "mail_workers": int(max(1, int(mail_workers))),
            "sent_since": str(sent_since or "").strip(),
            "reset_state": bool(reset_state),
            "reuse_existing": bool(reuse_existing),
            "creator_cache_db_path": str(creator_cache_db_path or "").strip(),
            "force_refresh_creator_cache": bool(force_refresh_creator_cache),
        },
        "resolved_inputs": {
            "env_file": {
                "path": str(resolved_config["env_snapshot"].path),
                "exists": resolved_config["env_snapshot"].exists,
                "source": resolved_config["env_snapshot"].source,
            },
        },
        "preflight": preflight,
        "bounded_controls": {
            "upstream": {
                "matching_strategy": normalized_matching_strategy,
                "brand_keyword": normalized_brand_keyword,
                "brand_match_include_from": bool(brand_match_include_from),
                "mail_limit": int(max(0, int(mail_limit))),
                "mail_workers": int(max(1, int(mail_workers))),
                "sent_since": str(sent_since or "").strip(),
                "reuse_existing": bool(reuse_existing),
            },
            "downstream": {
                "platform_filters": requested_platforms,
                "vision_provider": str(vision_provider or "").strip().lower(),
                "max_identifiers_per_platform": int(max(0, int(max_identifiers_per_platform))),
                "poll_interval": max(1.0, float(poll_interval)),
                "probe_vision_provider_only": bool(probe_vision_provider_only),
                "skip_scrape": bool(skip_scrape),
                "skip_visual": bool(skip_visual),
                "skip_positioning_card_analysis": bool(skip_positioning_card_analysis),
            },
        },
        "resolved_paths": {
            "run_root": str(runner_paths.run_root),
            "output_root": str(runner_paths.output_root),
            "task_spec_json": str(runner_paths.task_spec_json),
            "workflow_handoff_json": str(runner_paths.workflow_handoff_json),
            "upstream_output_root": str(runner_paths.upstream_output_root),
            "upstream_summary_json": str(runner_paths.upstream_summary_json),
            "upstream_task_spec_json": str(runner_paths.upstream_task_spec_json),
            "upstream_workflow_handoff_json": str(runner_paths.upstream_workflow_handoff_json),
            "downstream_output_root": str(runner_paths.downstream_output_root),
            "downstream_summary_json": str(runner_paths.downstream_summary_json),
            "downstream_task_spec_json": str(runner_paths.downstream_task_spec_json),
            "downstream_workflow_handoff_json": str(runner_paths.downstream_workflow_handoff_json),
        },
        "contract": {
            "scope": "task-upload-to-final-export",
            "upstream_runner": "scripts/run_task_upload_to_keep_list_pipeline.py",
            "downstream_runner": "scripts/run_keep_list_screening_pipeline.py",
            "canonical_internal_boundary": "keep-list",
            "canonical_resume_point": "keep_list",
        },
        "setup": {
            "scope": "task-upload-to-final-export",
            "completed": False,
            "skipped": False,
            "errors": [],
        },
        "steps": {
            "fan_out": {
                "status": "running",
                "mode": "serial",
                "requested_task_name": normalized_task_name,
                "resolved_task_names": list(resolved_task_names),
                "children": [],
            }
        },
        "artifacts": {
            "fan_out_children": [],
            "final_exports_by_task": {},
            "all_platforms_final_review_by_task": {},
        },
        "resume_points": {},
    }
    attach_run_contract(summary)

    def persist_summary(payload: dict[str, Any]) -> None:
        _write_summary(runner_paths.summary_json, payload)
        write_workflow_handoff(
            runner_paths.workflow_handoff_json,
            summary=payload,
            task_spec=task_spec,
            task_spec_available=bool(payload.get("setup", {}).get("completed")),
        )

    def finalize(status: str, **extra: Any) -> dict[str, Any]:
        summary["status"] = status
        summary["finished_at"] = iso_now()
        summary.update(extra)
        failure = extra.get("failure")
        if isinstance(failure, dict):
            attach_failure_to_summary(summary, failure)
        attach_run_contract(summary)
        persist_summary(summary)
        return summary

    setup = materialize_setup(
        scope="task-upload-to-final-export",
        directories=[
            {
                "label": "run_root",
                "path": runner_paths.run_root,
                "error_code": "RUN_ROOT_UNAVAILABLE",
                "message": "run_root 无法创建: {path}",
                "remediation": "检查输出目录权限或显式传入可写的 `--output-root` 后重试。",
            }
        ],
        files=[
            {
                "label": "task_spec",
                "path": runner_paths.task_spec_json,
                "writer": lambda path: write_task_spec(path, task_spec),
                "error_code": "TASK_SPEC_WRITE_FAILED",
                "message": "task_spec 无法写入: {path}",
                "remediation": "检查 run root 权限或 task spec 序列化逻辑后重试。",
            }
        ],
    )
    summary["setup"] = {**setup, "skipped": False}
    if not setup["completed"]:
        failure = setup["errors"][0]
        return finalize("failed", failure=failure)
    persist_summary(summary)

    child_runs: list[dict[str, Any]] = []
    child_runs_root = runner_paths.run_root / "children"
    for index, child_task_name in enumerate(resolved_task_names, start=1):
        child_output_root = child_runs_root / f"{index:02d}_{child_task_name}"
        child_summary = _run_single_task_upload_to_final_export_pipeline(
            task_name=child_task_name,
            env_file=env_file,
            task_upload_url=task_upload_url,
            employee_info_url=employee_info_url,
            output_root=child_output_root,
            summary_json=child_output_root / "summary.json",
            task_download_dir=task_download_dir,
            mail_data_dir=mail_data_dir,
            existing_mail_db_path=existing_mail_db_path,
            existing_mail_raw_dir=existing_mail_raw_dir,
            existing_mail_data_dir=existing_mail_data_dir,
            feishu_app_id=feishu_app_id,
            feishu_app_secret=feishu_app_secret,
            feishu_base_url=feishu_base_url,
            timeout_seconds=timeout_seconds,
            folder_prefixes=folder_prefixes,
            owner_email_overrides=owner_email_overrides,
            imap_host=imap_host,
            imap_port=imap_port,
            mail_limit=mail_limit,
            mail_workers=mail_workers,
            sent_since=sent_since,
            reset_state=reset_state,
            reuse_existing=reuse_existing,
            matching_strategy=matching_strategy,
            brand_keyword=brand_keyword,
            brand_match_include_from=brand_match_include_from,
            base_url=base_url,
            api_key=api_key,
            model=model,
            wire_api=wire_api,
            platform_filters=platform_filters,
            vision_provider=vision_provider,
            max_identifiers_per_platform=max_identifiers_per_platform,
            poll_interval=poll_interval,
            creator_cache_db_path=creator_cache_db_path,
            force_refresh_creator_cache=force_refresh_creator_cache,
            probe_vision_provider_only=probe_vision_provider_only,
            skip_scrape=skip_scrape,
            skip_visual=skip_visual,
            skip_positioning_card_analysis=skip_positioning_card_analysis,
            _runtime_dependencies=runtime,
        )
        child_run = _summarize_child_run(child_summary)
        child_runs.append(child_run)
        summary["steps"]["fan_out"]["children"] = _json_clone(child_runs)
        summary["artifacts"]["fan_out_children"] = _json_clone(child_runs)
        summary["artifacts"]["final_exports_by_task"][child_task_name] = _json_clone(child_run["final_exports"])
        summary["artifacts"]["all_platforms_final_review_by_task"][child_task_name] = child_run["all_platforms_final_review"]
        persist_summary(summary)

    fan_out_status = _aggregate_fan_out_status(child_runs)
    summary["steps"]["fan_out"]["status"] = "failed" if fan_out_status == "failed" else "completed"
    if fan_out_status == "failed":
        failed_children = [
            item["task_name"]
            for item in child_runs
            if str(item.get("status") or "").strip() == "failed"
        ]
        failure = _build_failure_payload(
            stage="fan_out",
            error_code="TASK_GROUP_CHILD_FAILED",
            message=(
                f"任务组 {normalized_task_name!r} 的子任务执行失败："
                + ", ".join(failed_children)
            ),
            remediation="检查失败子任务的 summary 和 workflow handoff，再修复后重跑该任务组。",
            details={
                "requested_task_name": normalized_task_name,
                "resolved_task_names": list(resolved_task_names),
                "failed_children": failed_children,
                "child_runs": _json_clone(child_runs),
            },
        )
        return finalize("failed", failure=failure)

    summary["delivery_status"] = fan_out_status
    return finalize(fan_out_status)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the repo-local single-entry pipeline from task upload start through final export."
    )
    parser.add_argument("--task-name", required=True, help="任务名，例如 MINISO。")
    parser.add_argument("--env-file", default=".env", help="本地 env 文件路径，默认 ./.env。")
    parser.add_argument("--task-upload-url", default="", help="飞书任务上传 wiki/base 链接。")
    parser.add_argument("--employee-info-url", default="", help="飞书员工信息表 wiki/base 链接。")
    parser.add_argument("--output-root", default="", help="输出目录；默认写到 temp/task_upload_to_final_export_<timestamp>。")
    parser.add_argument("--summary-json", default="", help="最终 summary.json 输出路径。")
    parser.add_argument("--task-download-dir", default="", help="任务附件下载目录；默认由上游 runner 决定。")
    parser.add_argument("--mail-data-dir", default="", help="任务邮件数据目录；默认由上游 runner 决定。")
    parser.add_argument("--existing-mail-db-path", default="", help="已有共享邮箱 email_sync.db 路径；透传给 upstream runner 复用侧车邮件库。")
    parser.add_argument("--existing-mail-raw-dir", default="", help="已有共享邮箱 raw 邮件目录；默认由 upstream runner 按 db 路径推导。")
    parser.add_argument("--existing-mail-data-dir", default="", help="已有共享邮箱 mail data 根目录；默认由 upstream runner 按 db 路径推导。")
    parser.add_argument("--feishu-app-id", default="", help="飞书自建应用 app_id。")
    parser.add_argument("--feishu-app-secret", default="", help="飞书自建应用 app_secret。")
    parser.add_argument("--feishu-base-url", default="", help="飞书 OpenAPI Base URL。")
    parser.add_argument("--timeout-seconds", type=float, default=0.0, help="飞书请求超时时间；默认读取 .env 或 30 秒。")
    parser.add_argument("--folder-prefix", action="append", help="任务邮箱目录前缀，可重复传入；默认 其他文件夹。")
    parser.add_argument(
        "--owner-email-override",
        action="append",
        help="负责人邮箱覆盖，格式 MINISO:eden@amagency.biz，可重复传入。",
    )
    parser.add_argument("--mail-limit", type=int, default=0, help="mail sync 只抓最新 N 封；0 表示不截断。")
    parser.add_argument("--mail-workers", type=int, default=1, help="mail sync worker 数。")
    parser.add_argument("--sent-since", default="", help="mail sync 起始日期 YYYY-MM-DD；默认最近 3 个月。")
    parser.add_argument("--reset-state", action="store_true", help="mail sync 忽略本地游标，重新全量扫描。")
    parser.add_argument(
        "--matching-strategy",
        default=DEFAULT_MATCHING_STRATEGY,
        choices=MATCHING_STRATEGIES,
        help="上游匹配策略；默认 brand-keyword-fast-path，也可选 legacy-enrichment。",
    )
    parser.add_argument("--brand-keyword", default="", help="fast path 的品牌关键词；默认复用 task-name。")
    parser.add_argument(
        "--brand-match-include-from",
        action="store_true",
        help="fast path 品牌匹配时把 from/sender 地址也纳入精确匹配候选。",
    )
    parser.add_argument("--no-reuse-existing", action="store_true", help="不要复用当前 output-root 下已存在的上游 artifact。")
    parser.add_argument("--base-url", default="", help="覆盖 duplicate review 的 LLM base URL。")
    parser.add_argument("--api-key", default="", help="覆盖 duplicate review 的 LLM API key。")
    parser.add_argument("--model", default="", help="覆盖 duplicate review 的 LLM model。")
    parser.add_argument("--wire-api", default="", help="覆盖 duplicate review 的 wire API。")
    parser.add_argument("--platform", action="append", help="只跑指定平台，可重复传入：tiktok / instagram / youtube。")
    parser.add_argument("--vision-provider", default="", help="指定视觉 provider，例如 openai / reelx。")
    parser.add_argument("--max-identifiers-per-platform", type=int, default=0, help="每个平台最多跑多少个账号；0 表示不截断。")
    parser.add_argument("--poll-interval", type=float, default=5.0, help="轮询 job 状态的秒数。")
    parser.add_argument("--creator-cache-db-path", default="", help="Creator DB SQLite 路径；默认使用仓库内共享缓存库。")
    parser.add_argument("--force-refresh-creator-cache", action="store_true", help="忽略 Creator DB 历史结果，强制重新抓取和视觉审核。")
    parser.add_argument("--probe-vision-provider-only", action="store_true", help="只做视觉 provider live probe，不继续 scrape/visual/export。")
    parser.add_argument("--skip-scrape", action="store_true", help="只做 staging，不触发 scrape/visual/export。")
    parser.add_argument("--skip-visual", action="store_true", help="跑 scrape 和导出，但跳过视觉复核。")
    parser.add_argument("--skip-positioning-card-analysis", action="store_true", help="跳过 visual-pass 后的定位卡分析。")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    owner_email_overrides: dict[str, str] = {}
    for chunk in args.owner_email_override or []:
        for item in str(chunk or "").split(","):
            normalized = item.strip()
            if not normalized or ":" not in normalized:
                continue
            key, value = normalized.split(":", 1)
            normalized_key = key.strip()
            normalized_value = value.strip()
            if normalized_key and normalized_value:
                owner_email_overrides[normalized_key] = normalized_value
    summary = run_task_upload_to_final_export_pipeline(
        task_name=args.task_name,
        env_file=args.env_file,
        task_upload_url=args.task_upload_url,
        employee_info_url=args.employee_info_url,
        output_root=Path(args.output_root) if args.output_root else None,
        summary_json=Path(args.summary_json) if args.summary_json else None,
        task_download_dir=args.task_download_dir,
        mail_data_dir=args.mail_data_dir,
        existing_mail_db_path=args.existing_mail_db_path,
        existing_mail_raw_dir=args.existing_mail_raw_dir,
        existing_mail_data_dir=args.existing_mail_data_dir,
        feishu_app_id=args.feishu_app_id,
        feishu_app_secret=args.feishu_app_secret,
        feishu_base_url=args.feishu_base_url,
        timeout_seconds=float(args.timeout_seconds),
        folder_prefixes=args.folder_prefix,
        owner_email_overrides=owner_email_overrides,
        mail_limit=max(0, int(args.mail_limit)),
        mail_workers=max(1, int(args.mail_workers)),
        sent_since=args.sent_since,
        reset_state=bool(args.reset_state),
        reuse_existing=not bool(args.no_reuse_existing),
        matching_strategy=args.matching_strategy,
        brand_keyword=args.brand_keyword,
        brand_match_include_from=bool(args.brand_match_include_from),
        base_url=args.base_url,
        api_key=args.api_key,
        model=args.model,
        wire_api=args.wire_api,
        platform_filters=args.platform,
        vision_provider=args.vision_provider,
        max_identifiers_per_platform=max(0, int(args.max_identifiers_per_platform)),
        poll_interval=max(1.0, float(args.poll_interval)),
        creator_cache_db_path=args.creator_cache_db_path or "",
        force_refresh_creator_cache=bool(args.force_refresh_creator_cache),
        probe_vision_provider_only=bool(args.probe_vision_provider_only),
        skip_scrape=bool(args.skip_scrape),
        skip_visual=bool(args.skip_visual),
        skip_positioning_card_analysis=bool(args.skip_positioning_card_analysis),
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if summary.get("status") != "failed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
