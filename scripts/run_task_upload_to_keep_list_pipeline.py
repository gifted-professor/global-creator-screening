from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from harness.contract import attach_run_contract
from harness.config import (
    RequiredConfigSpec,
    build_required_config_errors,
    resolve_keep_list_upstream_config,
)
from harness.failures import attach_failure_to_summary, build_failure_payload as build_harness_failure_payload
from harness.handoff import write_workflow_handoff
from harness.paths import resolve_keep_list_upstream_paths
from harness.preflight import (
    build_preflight_error,
    build_preflight_payload,
    inspect_directory_materialization_target,
)
from harness.setup import materialize_setup
from harness.spec import build_keep_list_upstream_task_spec, write_task_spec


def _load_runtime_dependencies():
    from email_sync.brand_keyword_match import match_brand_keyword
    from email_sync.config import Settings
    from email_sync.creator_enrichment import enrich_creator_workbook
    from email_sync.db import Database
    from email_sync.llm_review import prepare_llm_review_candidates, run_and_apply_llm_review
    from email_sync.mail_thread_funnel import build_mail_thread_funnel_keep_workbook
    from email_sync.date_windows import resolve_sync_sent_since
    from email_sync.shared_email_resolution import resolve_shared_email_candidates, run_shared_email_final_review
    from feishu_screening_bridge.feishu_api import DEFAULT_FEISHU_BASE_URL, FeishuOpenClient
    from feishu_screening_bridge.local_env import get_preferred_value, load_local_env
    from feishu_screening_bridge.task_upload_sync import (
        download_task_upload_screening_assets,
        inspect_task_upload_assignments,
        resolve_task_upload_entry,
        sync_task_upload_mailboxes,
    )

    return {
        "Settings": Settings,
        "Database": Database,
        "FeishuOpenClient": FeishuOpenClient,
        "DEFAULT_FEISHU_BASE_URL": DEFAULT_FEISHU_BASE_URL,
        "download_task_upload_screening_assets": download_task_upload_screening_assets,
        "inspect_task_upload_assignments": inspect_task_upload_assignments,
        "resolve_task_upload_entry": resolve_task_upload_entry,
        "sync_task_upload_mailboxes": sync_task_upload_mailboxes,
        "match_brand_keyword": match_brand_keyword,
        "resolve_shared_email_candidates": resolve_shared_email_candidates,
        "run_shared_email_final_review": run_shared_email_final_review,
        "enrich_creator_workbook": enrich_creator_workbook,
        "prepare_llm_review_candidates": prepare_llm_review_candidates,
        "run_and_apply_llm_review": run_and_apply_llm_review,
        "build_mail_thread_funnel_keep_workbook": build_mail_thread_funnel_keep_workbook,
        "resolve_sync_sent_since": resolve_sync_sent_since,
        "load_local_env": load_local_env,
        "get_preferred_value": get_preferred_value,
    }


STOP_AFTER_CHOICES = (
    "task-assets",
    "mail-sync",
    "enrichment",
    "llm-candidates",
    "brand-match",
    "shared-resolution",
    "keep-list",
)
MATCHING_STRATEGIES = ("legacy-enrichment", "brand-keyword-fast-path")
DEFAULT_MATCHING_STRATEGY = "brand-keyword-fast-path"
CONTRACT_VERSION = "phase16.keep-list.v2"


def default_output_root() -> Path:
    return resolve_keep_list_upstream_paths(task_name="task").run_root


def iso_now() -> str:
    return datetime.now().astimezone().isoformat()


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
    normalized = "".join(cleaned).strip("_")
    return normalized or "task"


def _normalize_task_lookup_key(value: str) -> str:
    return str(value or "").strip().casefold()


def _parse_mapping_overrides(values: Sequence[str] | str | None) -> dict[str, str]:
    if isinstance(values, str):
        chunks = [values]
    else:
        chunks = list(values or [])
    result: dict[str, str] = {}
    for chunk in chunks:
        for item in str(chunk or "").split(","):
            normalized_item = item.strip()
            if not normalized_item or ":" not in normalized_item:
                continue
            key, value = normalized_item.split(":", 1)
            normalized_key = key.strip()
            normalized_value = value.strip()
            if normalized_key and normalized_value:
                result[normalized_key] = normalized_value
    return result


def _json_clone(payload: Any) -> Any:
    return json.loads(json.dumps(payload, ensure_ascii=False))


def _write_summary(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _emit_runtime_progress(scope: str, message: str) -> None:
    print(f"[{iso_now()}] [{scope}] {message}", flush=True)


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


def _classify_failure(exc: Exception, *, failed_step: str) -> dict[str, Any]:
    message = str(exc) or exc.__class__.__name__
    stage = failed_step or "preflight"
    error_code = "TASK_UPLOAD_KEEP_LIST_PIPELINE_FAILED"
    remediation = "检查 summary 里的 resolved_inputs、preflight 和失败步骤产物后重试。"
    if "缺少 FEISHU_APP_ID" in message:
        error_code = "FEISHU_APP_ID_MISSING"
        remediation = "在 `.env` 或 `--feishu-app-id` 里填写 FEISHU_APP_ID。"
    elif "缺少 FEISHU_APP_SECRET" in message:
        error_code = "FEISHU_APP_SECRET_MISSING"
        remediation = "在 `.env` 或 `--feishu-app-secret` 里填写 FEISHU_APP_SECRET。"
    elif "缺少 TASK_UPLOAD_URL" in message:
        error_code = "TASK_UPLOAD_URL_MISSING"
        remediation = "在 `.env` 或 `--task-upload-url` 里填写 TASK_UPLOAD_URL。"
    elif "缺少 EMPLOYEE_INFO_URL" in message:
        error_code = "EMPLOYEE_INFO_URL_MISSING"
        remediation = "在 `.env` 或 `--employee-info-url` 里填写 EMPLOYEE_INFO_URL。"
    elif "没有产生 mail sync 结果" in message:
        error_code = "MAIL_SYNC_RESULT_MISSING"
        remediation = "检查任务上传任务名、员工映射和 IMAP 文件夹解析是否命中。"
    elif "mail sync failed" in message:
        error_code = "MAIL_SYNC_FAILED"
        remediation = "检查 `mail_sync` step 的 `mail_sync_error`、员工邮箱映射和 IMAP 配置。"
    elif "默认抓取邮箱模式要求同时提供" in message:
        error_code = "MAIL_SYNC_DEFAULT_CREDENTIALS_INCOMPLETE"
        remediation = "同时配置 `TASK_UPLOAD_MAIL_ACCOUNT` / `TASK_UPLOAD_MAIL_AUTH_CODE`，或同时配置 `EMAIL_ACCOUNT` / `EMAIL_AUTH_CODE`。"
    return _build_failure_payload(
        stage=stage,
        error_code=error_code,
        message=message,
        remediation=remediation,
        details={
            "exception_type": exc.__class__.__name__,
            "failed_step": failed_step,
        },
    )


def _load_existing_summary(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _step_artifacts_exist(step_payload: dict[str, Any], artifact_keys: tuple[str, ...]) -> bool:
    artifacts = step_payload.get("artifacts")
    if not isinstance(artifacts, dict):
        return False
    for key in artifact_keys:
        value = str(artifacts.get(key) or "").strip()
        if not value or not Path(value).exists():
            return False
    return True


def _compact_refs(payload: dict[str, Any]) -> dict[str, Any]:
    refs: dict[str, Any] = {}
    for key, value in payload.items():
        if value is None:
            continue
        if isinstance(value, str):
            if not value.strip():
                continue
            refs[key] = value
            continue
        refs[key] = value
    return refs


def _resolve_cli_env_value(
    cli_value: object,
    env_values: dict[str, str],
    env_key: str,
    default: str = "",
) -> tuple[str, str]:
    candidate = str(cli_value or "").strip()
    if candidate:
        return candidate, "cli"
    env_candidate = str(env_values.get(env_key, "") or "").strip()
    if env_candidate:
        return env_candidate, "env_file"
    return str(default or "").strip(), "default"


def _build_resolved_config_sources(
    *,
    env_file: str,
    task_upload_url: str,
    employee_info_url: str,
    feishu_app_id: str,
    feishu_app_secret: str,
    feishu_base_url: str,
    timeout_seconds: float,
    imap_host: str,
    imap_port: int,
    matching_strategy: str,
    brand_keyword: str,
    task_name: str,
    mail_limit: int,
    mail_workers: int,
    sent_since: str,
    reset_state: bool,
    stop_after: str,
    reuse_existing: bool,
    task_download_dir_source: str,
    mail_data_dir_source: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    return resolve_keep_list_upstream_config(
        env_file=env_file,
        task_upload_url=task_upload_url,
        employee_info_url=employee_info_url,
        feishu_app_id=feishu_app_id,
        feishu_app_secret=feishu_app_secret,
        feishu_base_url=feishu_base_url,
        timeout_seconds=timeout_seconds,
        imap_host=imap_host,
        imap_port=imap_port,
        matching_strategy=matching_strategy,
        brand_keyword=brand_keyword,
        task_name=task_name,
        mail_limit=mail_limit,
        mail_workers=mail_workers,
        sent_since=sent_since,
        reset_state=reset_state,
        stop_after=stop_after,
        reuse_existing=reuse_existing,
        task_download_dir_source=task_download_dir_source,
        mail_data_dir_source=mail_data_dir_source,
    )


def _build_upstream_preflight(
    *,
    task_name: str,
    matching_strategy: str,
    stop_after: str,
    run_root: Path,
    downloads_dir: Path,
    mail_root: Path,
    exports_dir: Path,
    env_snapshot: Any,
    resolved_task_upload_url: Any,
    resolved_employee_info_url: Any,
    resolved_feishu_app_id: Any,
    resolved_feishu_app_secret: Any,
) -> dict[str, Any]:
    errors: list[dict[str, Any]] = []
    run_root_target = inspect_directory_materialization_target(run_root)
    downloads_dir_target = inspect_directory_materialization_target(downloads_dir)
    mail_root_target = inspect_directory_materialization_target(mail_root)
    exports_dir_target = inspect_directory_materialization_target(exports_dir)
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
            )
        )
    if str(stop_after or "").strip().lower() and str(stop_after).strip().lower() not in STOP_AFTER_CHOICES:
        errors.append(
            build_preflight_error(
                error_code="STOP_AFTER_INVALID",
                message=f"不支持的 stop_after: {stop_after}",
                remediation=f"改用 {', '.join(STOP_AFTER_CHOICES)} 之一后重试。",
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
    dir_error_map = (
        ("RUN_ROOT_UNAVAILABLE", run_root, "run_root", run_root_target),
        ("DOWNLOADS_DIR_UNAVAILABLE", downloads_dir, "downloads_dir", downloads_dir_target),
        ("MAIL_ROOT_UNAVAILABLE", mail_root, "mail_root", mail_root_target),
        ("EXPORTS_DIR_UNAVAILABLE", exports_dir, "exports_dir", exports_dir_target),
    )
    for error_code, path_value, label, inspection in dir_error_map:
        if not bool(inspection["materializable"]):
            errors.append(
                build_preflight_error(
                    error_code=error_code,
                    message=f"{label} 无法创建: {path_value}",
                    remediation="检查路径权限或显式传入可写目录后重试。",
                    details={
                        "path": str(path_value),
                        "nearest_existing_parent": str(inspection["nearest_existing_parent"]),
                    },
                )
            )
    return build_preflight_payload(
        checks={
            "scope": "task-upload-to-keep-list",
            "lightweight_only": True,
            "canonical_boundary": "keep-list",
            "task_name_present": bool(str(task_name or "").strip()),
            "matching_strategy": str(matching_strategy or "").strip().lower(),
            "stop_after": str(stop_after or "").strip().lower(),
            "env_file_exists": bool(getattr(env_snapshot, "exists", False)),
            "task_upload_url_present": bool(getattr(resolved_task_upload_url, "present", False)),
            "employee_info_url_present": bool(getattr(resolved_employee_info_url, "present", False)),
            "feishu_app_id_present": bool(getattr(resolved_feishu_app_id, "present", False)),
            "feishu_app_secret_present": bool(getattr(resolved_feishu_app_secret, "present", False)),
            "run_root_materializable": bool(run_root_target["materializable"]),
            "downloads_dir_materializable": bool(downloads_dir_target["materializable"]),
            "mail_root_materializable": bool(mail_root_target["materializable"]),
            "exports_dir_materializable": bool(exports_dir_target["materializable"]),
        },
        errors=errors,
    )


def _resolve_env_fallback_value(
    env_values: dict[str, str],
    env_keys: Sequence[str],
    default: str = "",
) -> tuple[str, str]:
    for env_key in env_keys:
        env_candidate = str(env_values.get(env_key, "") or "").strip()
        if env_candidate:
            return env_candidate, f"env_file:{env_key}"
    return str(default or "").strip(), "default"


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


def _resolve_execution_details(
    *,
    reused: bool,
    existing_summary_accepted: bool,
    rerun_reason: str,
) -> tuple[str, str]:
    if reused:
        return "reused", "existing_summary_artifacts_present"
    if existing_summary_accepted:
        return "rerun", rerun_reason
    return "produced", "fresh_run"


def _annotate_step_payload(
    payload: dict[str, Any],
    *,
    execution_mode: str,
    execution_reason: str,
    input_refs: dict[str, Any],
    owned_artifact_keys: Sequence[str],
    resume_point_key: str,
    reuse_supported: bool,
    stage_policy: str,
) -> dict[str, Any]:
    annotated = _json_clone(payload)
    annotated["execution_mode"] = execution_mode
    annotated["execution_reason"] = execution_reason
    annotated["input_refs"] = _compact_refs(input_refs)
    annotated["owned_artifact_keys"] = list(owned_artifact_keys)
    annotated["resume_policy"] = {
        "reuse_supported": bool(reuse_supported),
        "stage_policy": stage_policy,
        "resume_point_key": resume_point_key,
    }
    return annotated


def _resolve_downstream_reuse(
    *,
    existing_summary_accepted: bool,
    task_assets_reused: bool,
    reset_state: bool,
    mail_fetched_count: int,
) -> tuple[bool, str]:
    if not existing_summary_accepted:
        return False, "no_accepted_existing_summary"
    if not task_assets_reused:
        return False, "task_assets_reran_or_changed"
    if reset_state:
        return False, "mail_sync_reset_state_requested"
    if int(mail_fetched_count) > 0:
        return False, "mail_sync_fetched_new_mail"
    return True, "upstream_inputs_unchanged"


def _resolve_mail_sync_sent_since(
    *,
    explicit_sent_since: str,
    task_start_date: str,
    resolve_sync_sent_since: Any,
) -> tuple[str, str]:
    raw_explicit_sent_since = str(explicit_sent_since or "").strip()
    if raw_explicit_sent_since:
        return resolve_sync_sent_since(raw_explicit_sent_since).isoformat(), "cli"
    normalized_task_start_date = str(task_start_date or "").strip()
    if normalized_task_start_date:
        return resolve_sync_sent_since(normalized_task_start_date).isoformat(), "task_upload_start_time"
    return resolve_sync_sent_since(None).isoformat(), "default_today_only"


def _build_template_prompt_artifacts(
    *,
    template_workbook: Path,
    output_root: Path,
    env_file: str,
) -> dict[str, Any]:
    from scripts.prepare_screening_inputs import prepare_screening_inputs

    runtime_root = output_root.expanduser().resolve()
    summary_path = runtime_root / "summary.json"
    prepared = prepare_screening_inputs(
        template_workbook=template_workbook.expanduser().resolve(),
        env_file=env_file,
        template_output_dir=runtime_root / "parsed_outputs",
        screening_data_dir=runtime_root / "screening_data",
        config_dir=runtime_root / "config",
        temp_dir=runtime_root / "temp",
        summary_json=summary_path,
    )
    return {
        "status": "completed",
        "summary_json": str(summary_path),
        "runtime_prompt_artifacts_json": str((prepared.get("prompts") or {}).get("runtime_prompt_artifacts_json_path") or ""),
        "compile_report_json": str((prepared.get("rulespec") or {}).get("compile_report_path") or ""),
        "compile_output_dir": str((prepared.get("rulespec") or {}).get("compile_output_dir") or ""),
        "prompt_platform_count": int((prepared.get("prompts") or {}).get("platform_count") or 0),
        "selected_provider": str((prepared.get("prompts") or {}).get("selected_provider") or ""),
        "selected_model": str((prepared.get("prompts") or {}).get("selected_model") or ""),
    }


def _build_feishu_client(
    *,
    env_file: str,
    feishu_app_id: str,
    feishu_app_secret: str,
    feishu_base_url: str,
    timeout_seconds: float,
) -> tuple[Any, dict[str, str], dict[str, Any]]:
    runtime = _load_runtime_dependencies()
    load_local_env = runtime["load_local_env"]
    FeishuOpenClient = runtime["FeishuOpenClient"]

    env_values = load_local_env(env_file)
    app_id, app_id_source = _resolve_cli_env_value(feishu_app_id, env_values, "FEISHU_APP_ID")
    app_secret, app_secret_source = _resolve_cli_env_value(feishu_app_secret, env_values, "FEISHU_APP_SECRET")
    if not app_id:
        raise ValueError("缺少 FEISHU_APP_ID，请在本地 .env 或参数里填写。")
    if not app_secret:
        raise ValueError("缺少 FEISHU_APP_SECRET，请在本地 .env 或参数里填写。")

    resolved_timeout_value, timeout_source = _resolve_cli_env_value(
        timeout_seconds if timeout_seconds > 0 else "",
        env_values,
        "TIMEOUT_SECONDS",
        "30",
    )
    resolved_timeout_seconds = float(resolved_timeout_value)
    base_url, base_url_source = _resolve_cli_env_value(
        feishu_base_url,
        env_values,
        "FEISHU_OPEN_BASE_URL",
        runtime["DEFAULT_FEISHU_BASE_URL"],
    )
    client = FeishuOpenClient(
        app_id=app_id,
        app_secret=app_secret,
        base_url=base_url,
        timeout_seconds=resolved_timeout_seconds,
    )
    return client, env_values, {
        "base_url": base_url,
        "base_url_source": base_url_source,
        "timeout_seconds": resolved_timeout_seconds,
        "timeout_seconds_source": timeout_source,
        "feishu_app_id_source": app_id_source,
        "feishu_app_secret_source": app_secret_source,
    }


def run_task_upload_to_keep_list_pipeline(
    *,
    task_name: str,
    env_file: str = ".env",
    task_upload_url: str = "",
    employee_info_url: str = "",
    output_root: Path | None = None,
    summary_json: Path | None = None,
    task_download_dir: str | Path = "",
    mail_data_dir: str | Path = "",
    feishu_app_id: str = "",
    feishu_app_secret: str = "",
    feishu_base_url: str = "",
    timeout_seconds: float = 0.0,
    folder_prefixes: list[str] | None = None,
    owner_email_overrides: dict[str, str] | None = None,
    folder_overrides: dict[str, str] | None = None,
    imap_host: str = "",
    imap_port: int = 0,
    existing_mail_db_path: str | Path = "",
    existing_mail_raw_dir: str | Path = "",
    existing_mail_data_dir: str | Path = "",
    mail_limit: int = 0,
    mail_workers: int = 1,
    sent_since: str = "",
    reset_state: bool = False,
    stop_after: str = "",
    reuse_existing: bool = True,
    matching_strategy: str = DEFAULT_MATCHING_STRATEGY,
    brand_keyword: str = "",
    brand_match_include_from: bool = False,
    base_url: str = "",
    api_key: str = "",
    model: str = "",
    wire_api: str = "",
) -> dict[str, Any]:
    normalized_task_name = str(task_name or "").strip()
    normalized_matching_strategy = str(matching_strategy or "").strip().lower() or DEFAULT_MATCHING_STRATEGY
    resolved_brand_keyword = str(brand_keyword or "").strip() or normalized_task_name
    normalized_stop_after = str(stop_after or "").strip().lower()
    runner_paths = resolve_keep_list_upstream_paths(
        task_name=normalized_task_name or "task",
        output_root=output_root,
        summary_json=summary_json,
        task_download_dir=task_download_dir,
        mail_data_dir=mail_data_dir,
    )
    resolved_output_root = runner_paths.output_root
    run_summary_path = runner_paths.summary_json
    summary_path_exists = run_summary_path.exists()
    existing_summary = _load_existing_summary(run_summary_path) if reuse_existing else None
    resume_reset_reason = ""
    if reuse_existing:
        if existing_summary and str(existing_summary.get("matching_strategy") or DEFAULT_MATCHING_STRATEGY).strip().lower() != normalized_matching_strategy:
            existing_summary = None
            resume_reset_reason = "matching_strategy_changed"
        elif existing_summary:
            resume_reset_reason = ""
        elif summary_path_exists:
            resume_reset_reason = "existing_summary_unreadable"
        else:
            resume_reset_reason = "no_existing_summary"
    else:
        resume_reset_reason = "reuse_disabled"
    existing_summary_accepted = existing_summary is not None

    resolved_config_sources, resolved_config = _build_resolved_config_sources(
        env_file=env_file,
        task_upload_url=task_upload_url,
        employee_info_url=employee_info_url,
        feishu_app_id=feishu_app_id,
        feishu_app_secret=feishu_app_secret,
        feishu_base_url=feishu_base_url,
        timeout_seconds=timeout_seconds,
        imap_host=imap_host,
        imap_port=imap_port,
        matching_strategy=normalized_matching_strategy,
        brand_keyword=brand_keyword,
        task_name=normalized_task_name,
        mail_limit=mail_limit,
        mail_workers=mail_workers,
        sent_since=sent_since,
        reset_state=reset_state,
        stop_after=normalized_stop_after,
        reuse_existing=reuse_existing,
        task_download_dir_source=runner_paths.downloads_dir_source,
        mail_data_dir_source=runner_paths.mail_root_source,
    )
    preflight = _build_upstream_preflight(
        task_name=normalized_task_name,
        matching_strategy=normalized_matching_strategy,
        stop_after=normalized_stop_after,
        run_root=runner_paths.run_root,
        downloads_dir=runner_paths.downloads_dir,
        mail_root=runner_paths.mail_root,
        exports_dir=runner_paths.exports_dir,
        env_snapshot=resolved_config["env_snapshot"],
        resolved_task_upload_url=resolved_config["task_upload_url"],
        resolved_employee_info_url=resolved_config["employee_info_url"],
        resolved_feishu_app_id=resolved_config["feishu_app_id"],
        resolved_feishu_app_secret=resolved_config["feishu_app_secret"],
    )

    resolved_sent_since = str(sent_since or "").strip()

    task_slug = _safe_name(normalized_task_name)
    downloads_dir = runner_paths.downloads_dir
    mail_root = runner_paths.mail_root
    exports_dir = runner_paths.exports_dir
    enrichment_prefix = exports_dir / f"{task_slug}_匹配结果"
    llm_prefix = exports_dir / f"{task_slug}_匹配结果_高置信_按我们去重"
    brand_match_prefix = exports_dir / f"{task_slug}_brand_keyword_match"
    shared_resolution_prefix = exports_dir / f"{task_slug}_shared_email_resolution"
    mail_funnel_prefix = exports_dir / f"{task_slug}_mail_thread_funnel"
    step_order = (
        ["task_assets", "mail_sync", "brand_match", "mail_funnel"]
        if normalized_matching_strategy == "brand-keyword-fast-path"
        else ["task_assets", "mail_sync", "enrichment", "llm_candidates", "llm_review"]
    )

    summary: dict[str, Any] = {
        "started_at": iso_now(),
        "finished_at": "",
        "status": "running" if preflight["ready"] else "failed",
        "run_id": runner_paths.run_id,
        "run_root": str(runner_paths.run_root),
        "task_name": normalized_task_name,
        "matching_strategy": normalized_matching_strategy,
        "brand_keyword": resolved_brand_keyword,
        "env_file_raw": str(env_file),
        "env_file": str(resolved_config["env_snapshot"].path),
        "output_root": str(resolved_output_root),
        "summary_json": str(run_summary_path),
        "task_spec_json": str(runner_paths.task_spec_json),
        "workflow_handoff_json": str(runner_paths.workflow_handoff_json),
        "resolved_config_sources": resolved_config_sources,
        "stop_after": normalized_stop_after,
        "reuse_existing": bool(reuse_existing),
        "inputs": {
            "task_upload_url": str(task_upload_url or "").strip(),
            "employee_info_url": str(employee_info_url or "").strip(),
            "task_download_dir": str(task_download_dir or "").strip(),
            "mail_data_dir": str(mail_data_dir or "").strip(),
            "existing_mail_db_path": str(existing_mail_db_path or "").strip(),
            "existing_mail_raw_dir": str(existing_mail_raw_dir or "").strip(),
            "existing_mail_data_dir": str(existing_mail_data_dir or "").strip(),
            "mail_limit": int(max(0, int(mail_limit))),
            "mail_workers": int(max(1, int(mail_workers))),
            "sent_since": str(sent_since or "").strip(),
            "reset_state": bool(reset_state),
            "matching_strategy": normalized_matching_strategy,
            "brand_keyword": resolved_brand_keyword,
            "brand_match_include_from": bool(brand_match_include_from),
        },
        "resolved_paths": {
            "run_root": str(runner_paths.run_root),
            "task_spec_json": str(runner_paths.task_spec_json),
            "workflow_handoff_json": str(runner_paths.workflow_handoff_json),
            "downloads_dir": str(downloads_dir),
            "mail_root": str(mail_root),
            "exports_dir": str(exports_dir),
            "enrichment_prefix": str(enrichment_prefix),
            "llm_review_prefix": str(llm_prefix),
            "brand_match_prefix": str(brand_match_prefix),
            "shared_resolution_prefix": str(shared_resolution_prefix),
        },
        "resolved_inputs": {
            "env_file": {
                "path": str(resolved_config["env_snapshot"].path),
                "exists": resolved_config["env_snapshot"].exists,
                "source": resolved_config["env_snapshot"].source,
            },
            "paths": {
                "downloads_dir": _path_summary(downloads_dir, source=runner_paths.downloads_dir_source, kind="dir"),
                "mail_root": _path_summary(mail_root, source=runner_paths.mail_root_source, kind="dir"),
                "exports_dir": _path_summary(exports_dir, source="output_root", kind="dir"),
            },
            "mail_sync": {
                "sent_since": resolved_sent_since,
                "sent_since_source": "cli" if str(sent_since or "").strip() else "pending_task_upload_start_time_or_default",
                "task_start_date": "",
                "source_mode": "pre_synced_mail_db" if str(existing_mail_db_path or "").strip() else "task_mail_sync",
                "mail_limit": int(max(0, int(mail_limit))),
                "mail_workers": int(max(1, int(mail_workers))),
                "folder_prefixes": list(folder_prefixes or ["其他文件夹"]),
                "owner_email_overrides": dict(owner_email_overrides or {}),
                "folder_overrides": dict(folder_overrides or {}),
            },
        },
        "preflight": preflight,
        "contract": {
            "contract_version": CONTRACT_VERSION,
            "scope": "task-upload-to-keep-list",
            "step_order": step_order,
            "canonical_boundary": "keep-list",
            "canonical_resume_point": "keep_list",
            "downstream_runner": "scripts/run_keep_list_screening_pipeline.py",
        },
        "setup": {
            "scope": "task-upload-to-keep-list",
            "completed": False,
            "skipped": not preflight["ready"],
            "errors": [],
        },
        "resume_context": {
            "reuse_requested": bool(reuse_existing),
            "existing_summary_found": bool(summary_path_exists),
            "existing_summary_accepted": bool(existing_summary_accepted),
            "reset_reason": resume_reset_reason,
            "mail_sync_policy": "always_rerun_incremental",
            "downstream_reuse_allowed": False,
            "downstream_reuse_reason": "pending_mail_sync",
        },
        "steps": {},
        "artifacts": {},
        "resume_points": {},
        "canonical_artifacts": {},
        "downstream_handoff": {},
    }
    attach_run_contract(summary)
    task_spec = build_keep_list_upstream_task_spec(
        generated_at=summary["started_at"],
        runner_paths=runner_paths,
        env_snapshot=resolved_config["env_snapshot"],
        env_file_raw=str(env_file),
        resolved_config_sources=resolved_config_sources,
        task_name=normalized_task_name,
        task_upload_url=resolved_config["task_upload_url"].value,
        employee_info_url=resolved_config["employee_info_url"].value,
        matching_strategy=normalized_matching_strategy,
        brand_keyword=resolved_brand_keyword,
        brand_match_include_from=bool(brand_match_include_from),
        stop_after=normalized_stop_after,
        reuse_existing=bool(reuse_existing),
        reset_state=bool(reset_state),
        mail_limit=int(max(0, int(mail_limit))),
        mail_workers=int(max(1, int(mail_workers))),
        sent_since=str(sent_since or "").strip(),
        task_download_dir=str(task_download_dir or "").strip(),
        mail_data_dir=str(mail_data_dir or "").strip(),
        folder_prefixes=list(folder_prefixes or ["其他文件夹"]),
        owner_email_overrides=dict(owner_email_overrides or {}),
        folder_overrides=dict(folder_overrides or {}),
    )
    progress_scope = f"keep-upstream:{normalized_task_name or runner_paths.run_id}"
    progress_state: dict[str, Any] = {
        "status": "",
        "step_signatures": {},
    }

    def persist_summary(payload: dict[str, Any]) -> None:
        current_status = str(payload.get("status") or "").strip()
        if current_status and current_status != progress_state["status"]:
            progress_state["status"] = current_status
            _emit_runtime_progress(progress_scope, f"run_status={current_status}")

        step_signatures = progress_state["step_signatures"]
        for step_name, step_payload in (payload.get("steps") or {}).items():
            if not isinstance(step_payload, dict):
                continue
            signature = (
                str(step_payload.get("status") or "").strip(),
                str(step_payload.get("execution_mode") or "").strip(),
                bool(step_payload.get("reused")),
            )
            if step_signatures.get(step_name) == signature:
                continue
            step_signatures[step_name] = signature
            detail_parts = []
            if signature[0]:
                detail_parts.append(f"status={signature[0]}")
            if signature[1]:
                detail_parts.append(f"mode={signature[1]}")
            stats = step_payload.get("stats")
            if isinstance(stats, dict):
                for key in (
                    "matched_email_count",
                    "keep_row_count",
                    "review_group_count",
                    "high_confidence_rows",
                    "llm_candidate_group_count",
                ):
                    value = stats.get(key)
                    if value not in (None, "", 0):
                        detail_parts.append(f"{key}={value}")
            _emit_runtime_progress(progress_scope, f"{step_name} " + " ".join(detail_parts or ["updated"]))

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

    def mark_stop(step_name: str) -> dict[str, Any]:
        return finalize(
            f"stopped_after_{step_name}",
            stopped_after=step_name,
        )

    if not preflight["ready"]:
        failure = preflight["errors"][0]
        return finalize(
            "failed",
            failure={**failure, "failure_layer": "preflight"},
        )

    _emit_runtime_progress(progress_scope, f"starting task={normalized_task_name or 'unknown'}")

    setup = materialize_setup(
        scope="task-upload-to-keep-list",
        directories=[
            {
                "label": "run_root",
                "path": runner_paths.run_root,
                "error_code": "RUN_ROOT_UNAVAILABLE",
                "message": "run_root 无法创建: {path}",
                "remediation": "检查路径权限或显式传入可写目录后重试。",
            },
            {
                "label": "downloads_dir",
                "path": downloads_dir,
                "error_code": "DOWNLOADS_DIR_UNAVAILABLE",
                "message": "downloads_dir 无法创建: {path}",
                "remediation": "检查路径权限或显式传入可写目录后重试。",
            },
            {
                "label": "mail_root",
                "path": mail_root,
                "error_code": "MAIL_ROOT_UNAVAILABLE",
                "message": "mail_root 无法创建: {path}",
                "remediation": "检查路径权限或显式传入可写目录后重试。",
            },
            {
                "label": "exports_dir",
                "path": exports_dir,
                "error_code": "EXPORTS_DIR_UNAVAILABLE",
                "message": "exports_dir 无法创建: {path}",
                "remediation": "检查路径权限或显式传入可写目录后重试。",
            },
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
        return finalize(
            "failed",
            failure=failure,
        )
    summary["resolved_inputs"]["paths"] = {
        "downloads_dir": _path_summary(downloads_dir, source=runner_paths.downloads_dir_source, kind="dir"),
        "mail_root": _path_summary(mail_root, source=runner_paths.mail_root_source, kind="dir"),
        "exports_dir": _path_summary(exports_dir, source="output_root", kind="dir"),
    }
    persist_summary(summary)

    try:
        runtime = _load_runtime_dependencies()
        resolve_sync_sent_since = runtime["resolve_sync_sent_since"]
        download_task_upload_screening_assets = runtime["download_task_upload_screening_assets"]
        inspect_task_upload_assignments = runtime.get("inspect_task_upload_assignments")
        sync_task_upload_mailboxes = runtime["sync_task_upload_mailboxes"]
        Database = runtime["Database"]
        match_brand_keyword = runtime["match_brand_keyword"]
        build_mail_thread_funnel_keep_workbook = runtime.get("build_mail_thread_funnel_keep_workbook")
        resolve_shared_email_candidates = runtime["resolve_shared_email_candidates"]
        run_shared_email_final_review = runtime["run_shared_email_final_review"]
        enrich_creator_workbook = runtime["enrich_creator_workbook"]
        prepare_llm_review_candidates = runtime["prepare_llm_review_candidates"]
        run_and_apply_llm_review = runtime["run_and_apply_llm_review"]
        resolve_task_upload_entry = runtime.get("resolve_task_upload_entry")
    except Exception as exc:  # noqa: BLE001
        failure = _build_failure_payload(
            stage="runtime_import",
            error_code="KEEP_LIST_RUNTIME_IMPORT_FAILED",
            message=f"上游 runtime 加载失败: {exc}",
            remediation="检查邮件同步、飞书桥接和 review 相关本地依赖后重试。",
            details={"exception_type": exc.__class__.__name__},
        )
        return finalize("failed", failure=failure)

    try:
        client, env_values, feishu_resolution = _build_feishu_client(
            env_file=env_file,
            feishu_app_id=feishu_app_id,
            feishu_app_secret=feishu_app_secret,
            feishu_base_url=feishu_base_url,
            timeout_seconds=timeout_seconds,
        )
        resolved_task_upload_url, task_upload_url_source = _resolve_cli_env_value(
            task_upload_url,
            env_values,
            "TASK_UPLOAD_URL",
        )
        if not resolved_task_upload_url:
            resolved_task_upload_url, task_upload_url_source = _resolve_cli_env_value(
                task_upload_url,
                env_values,
                "FEISHU_SOURCE_URL",
            )
        if not resolved_task_upload_url:
            raise ValueError("缺少 TASK_UPLOAD_URL，请在本地 .env 或参数里填写。")
        resolved_employee_info_url, employee_info_url_source = _resolve_cli_env_value(
            employee_info_url,
            env_values,
            "EMPLOYEE_INFO_URL",
        )
        if not resolved_employee_info_url:
            resolved_employee_info_url, employee_info_url_source = _resolve_cli_env_value(
                employee_info_url,
                env_values,
                "FEISHU_SOURCE_URL",
            )
        if not resolved_employee_info_url:
            raise ValueError("缺少 EMPLOYEE_INFO_URL，请在本地 .env 或参数里填写。")
        resolved_imap_host, imap_host_source = _resolve_cli_env_value(
            imap_host,
            env_values,
            "IMAP_HOST",
            "imap.qq.com",
        )
        resolved_imap_port_raw, imap_port_source = _resolve_cli_env_value(
            imap_port if int(imap_port) > 0 else "",
            env_values,
            "IMAP_PORT",
            "993",
        )
        resolved_imap_port = int(resolved_imap_port_raw or "993")
        resolved_default_account_email, default_account_email_source = _resolve_env_fallback_value(
            env_values,
            ("TASK_UPLOAD_MAIL_ACCOUNT", "EMAIL_ACCOUNT"),
        )
        resolved_default_auth_code, default_auth_code_source = _resolve_env_fallback_value(
            env_values,
            ("TASK_UPLOAD_MAIL_AUTH_CODE", "EMAIL_AUTH_CODE"),
        )
        if resolved_default_account_email and resolved_default_auth_code:
            credential_mode = "default_account_preferred_with_employee_fallback"
        elif resolved_default_account_email or resolved_default_auth_code:
            credential_mode = "incomplete_default_account"
        else:
            credential_mode = "employee_directory"

        summary["resolved_urls"] = {
            "task_upload_url": resolved_task_upload_url,
            "employee_info_url": resolved_employee_info_url,
            "feishu_base_url": feishu_resolution["base_url"],
        }
        summary["resolved_inputs"]["feishu"] = {
            "task_upload_url": resolved_task_upload_url,
            "task_upload_url_source": task_upload_url_source,
            "employee_info_url": resolved_employee_info_url,
            "employee_info_url_source": employee_info_url_source,
            "feishu_base_url": feishu_resolution["base_url"],
            "feishu_base_url_source": feishu_resolution["base_url_source"],
            "timeout_seconds": feishu_resolution["timeout_seconds"],
            "timeout_seconds_source": feishu_resolution["timeout_seconds_source"],
            "feishu_app_id": {
                "present": True,
                "source": feishu_resolution["feishu_app_id_source"],
            },
            "feishu_app_secret": {
                "present": True,
                "source": feishu_resolution["feishu_app_secret_source"],
            },
        }
        summary["resolved_inputs"]["mail_sync"].update({
            "imap_host": resolved_imap_host,
            "imap_host_source": imap_host_source,
            "imap_port": resolved_imap_port,
            "imap_port_source": imap_port_source,
            "default_account_email": resolved_default_account_email,
            "default_account_email_source": default_account_email_source,
            "default_auth_code_present": bool(resolved_default_auth_code),
            "default_auth_code_source": default_auth_code_source,
            "credential_mode": credential_mode,
        })
        if credential_mode == "incomplete_default_account":
            raise ValueError(
                "默认抓取邮箱模式要求同时提供 default_account_email 和 default_auth_code。"
            )
        summary["preflight"].update({
            "ready": True,
            "task_upload_url_present": True,
            "employee_info_url_present": True,
            "imap_ready": True,
            "errors": [],
        })

        task_assets_step = existing_summary.get("steps", {}).get("task_assets", {}) if existing_summary else {}
        task_assets_reused = bool(
            reuse_existing
            and existing_summary_accepted
            and _step_artifacts_exist(task_assets_step, ("template_workbook", "sending_list_workbook"))
        )
        if task_assets_reused:
            task_assets = _json_clone(task_assets_step)
            task_assets["status"] = "reused"
            task_assets["reused"] = True
        else:
            _emit_runtime_progress(progress_scope, "task_assets=running")
            task_assets_result = download_task_upload_screening_assets(
                client=client,
                task_upload_url=resolved_task_upload_url,
                task_name=normalized_task_name,
                download_dir=downloads_dir,
                download_template=True,
                download_sending_list=True,
            )
            task_assets = {
                "status": "completed",
                "reused": False,
                "record_id": task_assets_result.get("recordId", ""),
                "linked_bitable_url": task_assets_result.get("linkedBitableUrl", ""),
                "task_start_date": str(task_assets_result.get("taskStartDate") or "").strip(),
                "artifacts": {
                    "template_workbook": str(task_assets_result.get("templateDownloadedPath") or ""),
                    "sending_list_workbook": str(task_assets_result.get("sendingListDownloadedPath") or ""),
                },
                "raw": task_assets_result,
            }
        task_assets_mode, task_assets_reason = _resolve_execution_details(
            reused=task_assets_reused,
            existing_summary_accepted=existing_summary_accepted,
            rerun_reason="task_assets_missing_or_invalid_for_resume",
        )
        task_assets = _annotate_step_payload(
            task_assets,
            execution_mode=task_assets_mode,
            execution_reason=task_assets_reason,
            input_refs={
                "task_upload_url": resolved_task_upload_url,
                "download_dir": str(downloads_dir),
            },
            owned_artifact_keys=("template_workbook", "sending_list_workbook"),
            resume_point_key="task_assets",
            reuse_supported=True,
            stage_policy="reuse_if_artifacts_exist",
        )
        summary["steps"]["task_assets"] = task_assets
        summary["artifacts"]["template_workbook"] = task_assets["artifacts"]["template_workbook"]
        summary["artifacts"]["sending_list_workbook"] = task_assets["artifacts"]["sending_list_workbook"]
        summary["resume_points"]["task_assets"] = {
            "template_workbook": task_assets["artifacts"]["template_workbook"],
            "sending_list_workbook": task_assets["artifacts"]["sending_list_workbook"],
        }
        summary["canonical_artifacts"]["task_assets"] = _json_clone(summary["resume_points"]["task_assets"])
        resolved_task_start_date = str(
            task_assets.get("task_start_date")
            or task_assets.get("raw", {}).get("taskStartDate")
            or ""
        ).strip()
        if not resolved_task_start_date and not str(sent_since or "").strip() and callable(resolve_task_upload_entry):
            try:
                resolved_task_entry = resolve_task_upload_entry(
                    client=client,
                    task_upload_url=resolved_task_upload_url,
                    task_name=normalized_task_name,
                )
                resolved_task_start_date = str(getattr(resolved_task_entry, "task_start_date", "") or "").strip()
            except Exception:
                resolved_task_start_date = ""
        resolved_sent_since, resolved_sent_since_source = _resolve_mail_sync_sent_since(
            explicit_sent_since=str(sent_since or "").strip(),
            task_start_date=resolved_task_start_date,
            resolve_sync_sent_since=resolve_sync_sent_since,
        )
        summary["resolved_inputs"]["mail_sync"]["sent_since"] = resolved_sent_since
        summary["resolved_inputs"]["mail_sync"]["sent_since_source"] = resolved_sent_since_source
        summary["resolved_inputs"]["mail_sync"]["task_start_date"] = resolved_task_start_date
        persist_summary(summary)
        if normalized_stop_after == "task-assets":
            return mark_stop("task-assets")

        prompt_artifact_summary_json = str(task_assets["artifacts"].get("template_prepare_summary_json") or "").strip()
        prompt_artifact_json = str(task_assets["artifacts"].get("template_runtime_prompt_artifacts_json") or "").strip()
        prompt_artifacts_reused = bool(
            task_assets_reused
            and prompt_artifact_summary_json
            and Path(prompt_artifact_summary_json).exists()
            and prompt_artifact_json
            and Path(prompt_artifact_json).exists()
        )
        if prompt_artifacts_reused:
            task_assets["template_prompt_artifacts"] = {
                "status": "reused",
                "summary_json": prompt_artifact_summary_json,
                "runtime_prompt_artifacts_json": prompt_artifact_json,
                "compile_report_json": str(task_assets["artifacts"].get("template_compile_report_json") or "").strip(),
                "compile_output_dir": str(task_assets["artifacts"].get("template_compile_output_dir") or "").strip(),
                "prompt_platform_count": int(task_assets["artifacts"].get("template_prompt_platform_count") or 0),
                "selected_provider": str(task_assets["artifacts"].get("template_prompt_selected_provider") or "").strip(),
                "selected_model": str(task_assets["artifacts"].get("template_prompt_selected_model") or "").strip(),
            }
        else:
            try:
                prompt_artifacts = _build_template_prompt_artifacts(
                    template_workbook=Path(task_assets["artifacts"]["template_workbook"]),
                    output_root=resolved_output_root / "task_assets_prompt_artifacts",
                    env_file=env_file,
                )
                task_assets["artifacts"]["template_prepare_summary_json"] = prompt_artifacts["summary_json"]
                task_assets["artifacts"]["template_runtime_prompt_artifacts_json"] = prompt_artifacts["runtime_prompt_artifacts_json"]
                task_assets["artifacts"]["template_compile_report_json"] = prompt_artifacts["compile_report_json"]
                task_assets["artifacts"]["template_compile_output_dir"] = prompt_artifacts["compile_output_dir"]
                task_assets["artifacts"]["template_prompt_platform_count"] = prompt_artifacts["prompt_platform_count"]
                task_assets["artifacts"]["template_prompt_selected_provider"] = prompt_artifacts["selected_provider"]
                task_assets["artifacts"]["template_prompt_selected_model"] = prompt_artifacts["selected_model"]
                task_assets["template_prompt_artifacts"] = dict(prompt_artifacts)
            except Exception as exc:  # noqa: BLE001
                task_assets["template_prompt_artifacts"] = {
                    "status": "failed",
                    "error": str(exc) or exc.__class__.__name__,
                }

        summary["steps"]["task_assets"] = task_assets
        summary["artifacts"]["template_prepare_summary_json"] = str(
            task_assets["artifacts"].get("template_prepare_summary_json") or ""
        ).strip()
        summary["artifacts"]["template_runtime_prompt_artifacts_json"] = str(
            task_assets["artifacts"].get("template_runtime_prompt_artifacts_json") or ""
        ).strip()
        summary["resume_points"]["task_assets"]["template_prepare_summary_json"] = summary["artifacts"]["template_prepare_summary_json"]
        summary["resume_points"]["task_assets"]["template_runtime_prompt_artifacts_json"] = summary["artifacts"]["template_runtime_prompt_artifacts_json"]
        summary["canonical_artifacts"]["task_assets"] = _json_clone(summary["resume_points"]["task_assets"])
        persist_summary(summary)

        normalized_existing_mail_db_path = str(existing_mail_db_path or "").strip()
        if normalized_existing_mail_db_path:
            if not callable(inspect_task_upload_assignments):
                raise RuntimeError("existing_mail_db_path 模式缺少 inspect_task_upload_assignments runtime。")
            existing_db_path = Path(normalized_existing_mail_db_path).expanduser().resolve()
            if not existing_db_path.exists():
                raise FileNotFoundError(f"existing_mail_db_path 不存在: {existing_db_path}")
            existing_raw_dir_path = (
                Path(str(existing_mail_raw_dir)).expanduser().resolve()
                if str(existing_mail_raw_dir or "").strip()
                else existing_db_path.parent / "raw"
            )
            existing_data_dir_path = (
                Path(str(existing_mail_data_dir)).expanduser().resolve()
                if str(existing_mail_data_dir or "").strip()
                else existing_db_path.parent
            )
            inspection = inspect_task_upload_assignments(
                client=client,
                task_upload_url=resolved_task_upload_url,
                employee_info_url=resolved_employee_info_url,
                download_dir=downloads_dir,
                download_templates=False,
                parse_templates=False,
                owner_email_overrides=owner_email_overrides or {},
            )
            mail_item = next(
                (
                    item
                    for item in (inspection.get("items") or [])
                    if _normalize_task_lookup_key(item.get("taskName") or "") == _normalize_task_lookup_key(normalized_task_name)
                ),
                {},
            )
            if not isinstance(mail_item, dict) or not mail_item:
                raise RuntimeError(f"任务 {normalized_task_name!r} 在任务上传里找不到可用映射。")
            mail_item = dict(mail_item)
            mail_item.update(
                {
                    "mailSyncOk": True,
                    "mailSyncError": "",
                    "mailCredentialSource": "external_shared_mailbox_cache",
                    "mailLoginEmail": resolved_default_account_email or str(mail_item.get("employeeEmail") or "").strip(),
                    "mailSyncStrategy": "pre_synced_mail_db",
                    "mailFallbackReason": "shared_mailbox_sync_managed_externally",
                    "resolvedFolder": str((folder_prefixes or ["其他文件夹/邮件备份"])[0]),
                    "resolvedFolders": list(folder_prefixes or ["其他文件夹/邮件备份"]),
                    "mailScannedFolderCount": 1,
                    "mailFetchedCount": 0,
                    "mailServerTotalCount": None,
                    "mailSyncDurationSeconds": 0.0,
                    "mailDbPath": str(existing_db_path),
                    "mailRawDir": str(existing_raw_dir_path),
                    "mailDataDir": str(existing_data_dir_path),
                }
            )
            mail_sync_result = {
                "ok": True,
                "selectedCount": 1,
                "syncedCount": 1,
                "failedCount": 0,
                "mailDataDir": str(existing_data_dir_path),
                "imapHost": resolved_imap_host,
                "imapPort": resolved_imap_port,
                "defaultCredentialMode": "external_pre_synced_mail_db",
                "defaultAccountEmail": resolved_default_account_email,
                "sentSince": resolved_sent_since,
                "items": [mail_item],
            }
        else:
            _emit_runtime_progress(progress_scope, "mail_sync=running")
            mail_sync_result = sync_task_upload_mailboxes(
                client=client,
                task_upload_url=resolved_task_upload_url,
                employee_info_url=resolved_employee_info_url,
                download_dir=downloads_dir,
                mail_data_dir=mail_root,
                task_names=[normalized_task_name],
                owner_email_overrides=owner_email_overrides or {},
                folder_overrides=folder_overrides or {},
                folder_prefixes=folder_prefixes or ["其他文件夹"],
                limit=int(mail_limit) if int(mail_limit) > 0 else None,
                workers=max(1, int(mail_workers)),
                reset_state=bool(reset_state),
                sent_since=resolved_sent_since,
                imap_host=resolved_imap_host,
                imap_port=resolved_imap_port,
                default_account_email=resolved_default_account_email,
                default_auth_code=resolved_default_auth_code,
            )
        mail_item = next(iter(mail_sync_result.get("items") or []), {})
        if not mail_item:
            raise RuntimeError(f"任务 {normalized_task_name!r} 没有产生 mail sync 结果。")
        downstream_reuse_allowed, downstream_reuse_reason = _resolve_downstream_reuse(
            existing_summary_accepted=existing_summary_accepted,
            task_assets_reused=task_assets_reused,
            reset_state=bool(reset_state),
            mail_fetched_count=int(mail_item.get("mailFetchedCount") or 0),
        )
        mail_sync_step = {
            "status": "completed" if mail_item.get("mailSyncOk") else "failed",
            "reused": False,
            "selected_count": mail_sync_result.get("selectedCount", 0),
            "synced_count": mail_sync_result.get("syncedCount", 0),
            "failed_count": mail_sync_result.get("failedCount", 0),
            "task": {
                "task_name": mail_item.get("taskName", ""),
                "task_start_date": resolved_task_start_date,
                "employee_name": mail_item.get("employeeName", ""),
                "credential_source": mail_item.get("mailCredentialSource", ""),
                "login_email": mail_item.get("mailLoginEmail", ""),
                "sync_strategy": mail_item.get("mailSyncStrategy", ""),
                "fallback_reason": mail_item.get("mailFallbackReason", ""),
                "resolved_folder": mail_item.get("resolvedFolder", ""),
                "resolved_folders": list(mail_item.get("resolvedFolders") or []),
                "scanned_folder_count": mail_item.get("mailScannedFolderCount", 0),
                "mail_fetched_count": mail_item.get("mailFetchedCount", 0),
                "mail_server_total_count": mail_item.get("mailServerTotalCount"),
                "mail_sync_duration_seconds": mail_item.get("mailSyncDurationSeconds", 0.0),
                "mail_sync_error": mail_item.get("mailSyncError", ""),
            },
            "artifacts": {
                "mail_db_path": str(mail_item.get("mailDbPath") or ""),
                "mail_raw_dir": str(mail_item.get("mailRawDir") or ""),
                "mail_data_dir": str(mail_item.get("mailDataDir") or ""),
            },
            "raw": mail_sync_result,
        }
        mail_sync_mode, mail_sync_reason = _resolve_execution_details(
            reused=bool(normalized_existing_mail_db_path and existing_summary_accepted and _step_artifacts_exist((existing_summary.get("steps", {}) or {}).get("mail_sync", {}), ("mail_db_path",))),
            existing_summary_accepted=existing_summary_accepted,
            rerun_reason=(
                "mail_sync_is_owned_by_external_shared_db"
                if normalized_existing_mail_db_path
                else "mail_sync_is_incremental_and_owned_by_current_run"
            ),
        )
        mail_sync_step = _annotate_step_payload(
            mail_sync_step,
            execution_mode=mail_sync_mode,
            execution_reason=mail_sync_reason,
            input_refs={
                "sending_list_workbook": summary["artifacts"]["sending_list_workbook"],
                "mail_data_dir": str(mail_root if not normalized_existing_mail_db_path else existing_mail_data_dir or Path(normalized_existing_mail_db_path).expanduser().resolve().parent),
                "sent_since": resolved_sent_since,
                "reset_state": bool(reset_state),
            },
            owned_artifact_keys=("mail_db_path", "mail_raw_dir", "mail_data_dir"),
            resume_point_key="mail_sync",
            reuse_supported=bool(normalized_existing_mail_db_path),
            stage_policy=(
                "reuse_external_shared_mail_db_reference"
                if normalized_existing_mail_db_path
                else "always_rerun_incremental"
            ),
        )
        if not mail_item.get("mailSyncOk"):
            raise RuntimeError(str(mail_item.get("mailSyncError") or "mail sync failed"))
        summary["steps"]["mail_sync"] = mail_sync_step
        summary["artifacts"]["mail_db_path"] = mail_sync_step["artifacts"]["mail_db_path"]
        summary["resume_points"]["mail_sync"] = {
            "mail_db_path": mail_sync_step["artifacts"]["mail_db_path"],
            "sending_list_workbook": summary["artifacts"]["sending_list_workbook"],
        }
        summary["canonical_artifacts"]["mail_sync"] = _json_clone(summary["resume_points"]["mail_sync"])
        summary["resume_context"]["downstream_reuse_allowed"] = bool(downstream_reuse_allowed)
        summary["resume_context"]["downstream_reuse_reason"] = downstream_reuse_reason
        persist_summary(summary)
        if normalized_stop_after == "mail-sync":
            return mark_stop("mail-sync")

        if normalized_matching_strategy == "brand-keyword-fast-path":
            brand_match_existing = existing_summary.get("steps", {}).get("brand_match", {}) if existing_summary else {}
            brand_match_reused = bool(
                reuse_existing
                and downstream_reuse_allowed
                and _step_artifacts_exist(
                    brand_match_existing,
                    ("all_xlsx", "deduped_xlsx", "unique_xlsx", "shared_xlsx"),
                )
            )
            if brand_match_reused:
                brand_match_step = _json_clone(brand_match_existing)
                brand_match_step["status"] = "reused"
                brand_match_step["reused"] = True
            else:
                _emit_runtime_progress(progress_scope, "brand_match=running")
                db = Database(Path(mail_sync_step["artifacts"]["mail_db_path"]))
                try:
                    brand_match_result = match_brand_keyword(
                        db=db,
                        input_path=Path(summary["artifacts"]["sending_list_workbook"]),
                        output_prefix=brand_match_prefix,
                        keyword=resolved_brand_keyword,
                        sent_since=resolve_sync_sent_since(resolved_sent_since or None),
                        include_from=bool(brand_match_include_from),
                    )
                finally:
                    db.close()
                brand_match_step = {
                    "status": "completed",
                    "reused": False,
                    "stats": {
                        "source_kind": brand_match_result.get("source_kind", ""),
                        "message_hit_count": brand_match_result.get("message_hit_count", 0),
                        "matched_email_count": brand_match_result.get("matched_email_count", 0),
                        "email_direct_match_row_count": brand_match_result.get("email_direct_match_row_count", 0),
                        "profile_deduped_row_count": brand_match_result.get("profile_deduped_row_count", 0),
                        "unique_email_row_count": brand_match_result.get("unique_email_row_count", 0),
                        "shared_email_row_count": brand_match_result.get("shared_email_row_count", 0),
                        "shared_email_group_count": brand_match_result.get("shared_email_group_count", 0),
                    },
                    "artifacts": {
                        "all_xlsx": str(brand_match_result.get("xlsx_path") or ""),
                        "deduped_xlsx": str(brand_match_result.get("deduped_xlsx_path") or ""),
                        "unique_xlsx": str(brand_match_result.get("unique_xlsx_path") or ""),
                        "shared_xlsx": str(brand_match_result.get("shared_xlsx_path") or ""),
                    },
                }
            brand_match_mode, brand_match_reason = _resolve_execution_details(
                reused=brand_match_reused,
                existing_summary_accepted=existing_summary_accepted,
                rerun_reason="brand_match_inputs_changed_or_resume_artifacts_missing",
            )
            brand_match_step = _annotate_step_payload(
                brand_match_step,
                execution_mode=brand_match_mode,
                execution_reason=brand_match_reason,
                input_refs={
                    "mail_db_path": mail_sync_step["artifacts"]["mail_db_path"],
                    "sending_list_workbook": summary["artifacts"]["sending_list_workbook"],
                },
                owned_artifact_keys=("all_xlsx", "deduped_xlsx", "unique_xlsx", "shared_xlsx"),
                resume_point_key="brand_match",
                reuse_supported=True,
                stage_policy="reuse_if_mail_db_and_sending_list_unchanged",
            )
            summary["steps"]["brand_match"] = brand_match_step
            summary["artifacts"]["brand_match_deduped_xlsx"] = brand_match_step["artifacts"]["deduped_xlsx"]
            summary["artifacts"]["brand_match_unique_xlsx"] = brand_match_step["artifacts"]["unique_xlsx"]
            summary["artifacts"]["brand_match_shared_xlsx"] = brand_match_step["artifacts"]["shared_xlsx"]
            summary["resume_points"]["brand_match"] = {
                "deduped_workbook": brand_match_step["artifacts"]["deduped_xlsx"],
                "unique_email_workbook": brand_match_step["artifacts"]["unique_xlsx"],
                "shared_email_workbook": brand_match_step["artifacts"]["shared_xlsx"],
            }
            persist_summary(summary)
            if normalized_stop_after == "brand-match":
                return mark_stop("brand-match")
            if build_mail_thread_funnel_keep_workbook is not None:
                mail_funnel_existing = existing_summary.get("steps", {}).get("mail_funnel", {}) if existing_summary else {}
                mail_funnel_reused = bool(
                    reuse_existing
                    and downstream_reuse_allowed
                    and brand_match_reused
                    and _step_artifacts_exist(
                        mail_funnel_existing,
                        ("review_xlsx", "keep_xlsx", "manual_tail_xlsx"),
                    )
                )
                if mail_funnel_reused:
                    mail_funnel_step = _json_clone(mail_funnel_existing)
                    mail_funnel_step["status"] = "reused"
                    mail_funnel_step["reused"] = True
                else:
                    _emit_runtime_progress(progress_scope, "mail_funnel=running")
                    db = Database(Path(mail_sync_step["artifacts"]["mail_db_path"]))
                    try:
                        mail_funnel_result = build_mail_thread_funnel_keep_workbook(
                            db=db,
                            input_path=Path(summary["artifacts"]["sending_list_workbook"]),
                            output_prefix=mail_funnel_prefix,
                            keyword=resolved_brand_keyword,
                            sent_since=resolve_sync_sent_since(resolved_sent_since or None),
                            include_from=bool(brand_match_include_from),
                            env_path=env_file,
                            base_url=str(base_url or "").strip() or None,
                            api_key=str(api_key or "").strip() or None,
                            model=str(model or "").strip() or None,
                            wire_api=str(wire_api or "").strip() or None,
                        )
                    finally:
                        db.close()
                    mail_funnel_step = {
                        "status": "completed",
                        "reused": False,
                        "stats": {
                            "message_hit_count": mail_funnel_result.get("message_hit_count", 0),
                            "external_message_count": mail_funnel_result.get("external_message_count", 0),
                            "pass0_sending_list_email_count": mail_funnel_result.get("pass0_sending_list_email_count", 0),
                            "regex_pass1_count": mail_funnel_result.get("regex_pass1_count", 0),
                            "regex_pass2_count": mail_funnel_result.get("regex_pass2_count", 0),
                            "llm_high_count": mail_funnel_result.get("llm_high_count", 0),
                            "manual_row_count": mail_funnel_result.get("manual_row_count", 0),
                            "filtered_auto_reply_count": mail_funnel_result.get("filtered_auto_reply_count", 0),
                            "no_match_count": mail_funnel_result.get("no_match_count", 0),
                            "keep_row_count": mail_funnel_result.get("keep_row_count", 0),
                        },
                        "artifacts": {
                            "review_xlsx": str(mail_funnel_result.get("review_xlsx_path") or ""),
                            "keep_xlsx": str(mail_funnel_result.get("keep_xlsx_path") or ""),
                            "manual_tail_xlsx": str(mail_funnel_result.get("manual_tail_xlsx_path") or ""),
                        },
                    }
                mail_funnel_mode, mail_funnel_reason = _resolve_execution_details(
                    reused=mail_funnel_reused,
                    existing_summary_accepted=existing_summary_accepted,
                    rerun_reason="mail_funnel_inputs_changed_or_resume_artifacts_missing",
                )
                mail_funnel_step = _annotate_step_payload(
                    mail_funnel_step,
                    execution_mode=mail_funnel_mode,
                    execution_reason=mail_funnel_reason,
                    input_refs={
                        "mail_db_path": mail_sync_step["artifacts"]["mail_db_path"],
                        "sending_list_workbook": summary["artifacts"]["sending_list_workbook"],
                        "brand_match_workbook": brand_match_step["artifacts"]["all_xlsx"],
                    },
                    owned_artifact_keys=("review_xlsx", "keep_xlsx", "manual_tail_xlsx"),
                    resume_point_key="keep_list",
                    reuse_supported=True,
                    stage_policy="reuse_only_if_brand_match_reused",
                )
                summary["steps"]["mail_funnel"] = mail_funnel_step
                summary["artifacts"]["mail_funnel_review_xlsx"] = mail_funnel_step["artifacts"]["review_xlsx"]
                summary["artifacts"]["manual_tail_xlsx"] = mail_funnel_step["artifacts"]["manual_tail_xlsx"]
                summary["artifacts"]["keep_workbook"] = mail_funnel_step["artifacts"]["keep_xlsx"]
                summary["resume_points"]["keep_list"] = {
                    "keep_workbook": mail_funnel_step["artifacts"]["keep_xlsx"],
                    "manual_tail_xlsx": mail_funnel_step["artifacts"]["manual_tail_xlsx"],
                    "mail_funnel_review_xlsx": mail_funnel_step["artifacts"]["review_xlsx"],
                }
                persist_summary(summary)
                if normalized_stop_after == "shared-resolution":
                    return mark_stop("shared-resolution")
            else:
                shared_resolution_existing = existing_summary.get("steps", {}).get("shared_resolution", {}) if existing_summary else {}
                shared_resolution_reused = bool(
                    reuse_existing
                    and downstream_reuse_allowed
                    and brand_match_reused
                    and _step_artifacts_exist(
                        shared_resolution_existing,
                        ("resolved_xlsx", "unresolved_xlsx", "llm_candidates_jsonl"),
                    )
                )
                if shared_resolution_reused:
                    shared_resolution_step = _json_clone(shared_resolution_existing)
                    shared_resolution_step["status"] = "reused"
                    shared_resolution_step["reused"] = True
                else:
                    _emit_runtime_progress(progress_scope, "shared_resolution=running")
                    db = Database(Path(mail_sync_step["artifacts"]["mail_db_path"]))
                    try:
                        shared_resolution_result = resolve_shared_email_candidates(
                            db=db,
                            input_path=Path(brand_match_step["artifacts"]["shared_xlsx"]),
                            output_prefix=shared_resolution_prefix,
                        )
                    finally:
                        db.close()
                    shared_resolution_step = {
                        "status": "completed",
                        "reused": False,
                        "stats": {
                            "resolved_group_count": shared_resolution_result.get("resolved_group_count", 0),
                            "resolved_row_count": shared_resolution_result.get("resolved_row_count", 0),
                            "unresolved_group_count": shared_resolution_result.get("unresolved_group_count", 0),
                            "unresolved_row_count": shared_resolution_result.get("unresolved_row_count", 0),
                            "llm_candidate_group_count": shared_resolution_result.get("llm_candidate_group_count", 0),
                        },
                        "artifacts": {
                            "resolved_xlsx": str(shared_resolution_result.get("resolved_xlsx_path") or ""),
                            "unresolved_xlsx": str(shared_resolution_result.get("unresolved_xlsx_path") or ""),
                            "llm_candidates_jsonl": str(shared_resolution_result.get("llm_candidates_jsonl_path") or ""),
                        },
                    }
                shared_resolution_mode, shared_resolution_reason = _resolve_execution_details(
                    reused=shared_resolution_reused,
                    existing_summary_accepted=existing_summary_accepted,
                    rerun_reason="shared_resolution_inputs_changed_or_resume_artifacts_missing",
                )
                shared_resolution_step = _annotate_step_payload(
                    shared_resolution_step,
                    execution_mode=shared_resolution_mode,
                    execution_reason=shared_resolution_reason,
                    input_refs={
                        "mail_db_path": mail_sync_step["artifacts"]["mail_db_path"],
                        "shared_email_workbook": brand_match_step["artifacts"]["shared_xlsx"],
                    },
                    owned_artifact_keys=("resolved_xlsx", "unresolved_xlsx", "llm_candidates_jsonl"),
                    resume_point_key="shared_resolution",
                    reuse_supported=True,
                    stage_policy="reuse_only_if_brand_match_reused",
                )
                summary["steps"]["shared_resolution"] = shared_resolution_step
                summary["artifacts"]["content_resolved_xlsx"] = shared_resolution_step["artifacts"]["resolved_xlsx"]
                summary["artifacts"]["content_unresolved_xlsx"] = shared_resolution_step["artifacts"]["unresolved_xlsx"]
                summary["resume_points"]["shared_resolution"] = {
                    "resolved_workbook": shared_resolution_step["artifacts"]["resolved_xlsx"],
                    "unresolved_workbook": shared_resolution_step["artifacts"]["unresolved_xlsx"],
                    "llm_candidates_jsonl": shared_resolution_step["artifacts"]["llm_candidates_jsonl"],
                }
                persist_summary(summary)
                if normalized_stop_after == "shared-resolution":
                    return mark_stop("shared-resolution")

                final_review_existing = existing_summary.get("steps", {}).get("final_review", {}) if existing_summary else {}
                final_review_reused = bool(
                    reuse_existing
                    and downstream_reuse_allowed
                    and brand_match_reused
                    and shared_resolution_reused
                    and _step_artifacts_exist(
                        final_review_existing,
                        ("llm_review_jsonl", "manual_tail_xlsx", "keep_xlsx"),
                    )
                )
                if final_review_reused:
                    final_review_step = _json_clone(final_review_existing)
                    final_review_step["status"] = "reused"
                    final_review_step["reused"] = True
                else:
                    _emit_runtime_progress(progress_scope, "final_review=running")
                    final_review_result = run_shared_email_final_review(
                        input_prefix=shared_resolution_prefix,
                        env_path=env_file,
                        auto_keep_paths=[
                            Path(brand_match_step["artifacts"]["unique_xlsx"]),
                            Path(shared_resolution_step["artifacts"]["resolved_xlsx"]),
                        ],
                        base_url=str(base_url or "").strip() or None,
                        api_key=str(api_key or "").strip() or None,
                        model=str(model or "").strip() or None,
                        wire_api=str(wire_api or "").strip() or None,
                    )
                    final_review_step = {
                        "status": "completed",
                        "reused": False,
                        "stats": {
                            "review_group_count": final_review_result.get("review_group_count", 0),
                            "llm_resolved_row_count": final_review_result.get("llm_resolved_row_count", 0),
                            "manual_row_count": final_review_result.get("manual_row_count", 0),
                            "final_keep_row_count": final_review_result.get("final_keep_row_count", 0),
                            "retryable_failure_count": final_review_result.get("retryable_failure_count", 0),
                        },
                        "selected_provider": str(final_review_result.get("selected_provider") or ""),
                        "selected_model": str(final_review_result.get("selected_model") or ""),
                        "selected_wire_api": str(final_review_result.get("selected_wire_api") or ""),
                        "provider_attempts": list(final_review_result.get("provider_attempts") or []),
                        "absorbed_failures": list(final_review_result.get("absorbed_failures") or []),
                        "artifacts": {
                            "llm_review_jsonl": str(final_review_result.get("llm_review_jsonl_path") or ""),
                            "llm_resolved_xlsx": str(final_review_result.get("llm_resolved_xlsx_path") or ""),
                            "manual_tail_xlsx": str(final_review_result.get("manual_tail_xlsx_path") or ""),
                            "keep_xlsx": str(final_review_result.get("final_keep_xlsx_path") or ""),
                        },
                    }
                final_review_mode, final_review_reason = _resolve_execution_details(
                    reused=final_review_reused,
                    existing_summary_accepted=existing_summary_accepted,
                    rerun_reason="final_review_inputs_changed_or_resume_artifacts_missing",
                )
                final_review_step = _annotate_step_payload(
                    final_review_step,
                    execution_mode=final_review_mode,
                    execution_reason=final_review_reason,
                    input_refs={
                        "unique_email_workbook": brand_match_step["artifacts"]["unique_xlsx"],
                        "resolved_workbook": shared_resolution_step["artifacts"]["resolved_xlsx"],
                        "llm_candidates_jsonl": shared_resolution_step["artifacts"]["llm_candidates_jsonl"],
                    },
                    owned_artifact_keys=("llm_review_jsonl", "llm_resolved_xlsx", "manual_tail_xlsx", "keep_xlsx"),
                    resume_point_key="keep_list",
                    reuse_supported=True,
                    stage_policy="reuse_only_if_shared_resolution_reused",
                )
                summary["steps"]["final_review"] = final_review_step
                summary["artifacts"]["manual_tail_xlsx"] = final_review_step["artifacts"]["manual_tail_xlsx"]
                summary["artifacts"]["keep_workbook"] = final_review_step["artifacts"]["keep_xlsx"]
        else:
            enrichment_existing = existing_summary.get("steps", {}).get("enrichment", {}) if existing_summary else {}
            enrichment_reused = bool(
                reuse_existing
                and downstream_reuse_allowed
                and _step_artifacts_exist(
                    enrichment_existing,
                    ("all_xlsx", "high_xlsx"),
                )
            )
            if enrichment_reused:
                enrichment_step = _json_clone(enrichment_existing)
                enrichment_step["status"] = "reused"
                enrichment_step["reused"] = True
            else:
                _emit_runtime_progress(progress_scope, "enrichment=running")
                db = Database(Path(mail_sync_step["artifacts"]["mail_db_path"]))
                try:
                    enrichment_result = enrich_creator_workbook(
                        db=db,
                        input_path=Path(summary["artifacts"]["sending_list_workbook"]),
                        output_prefix=enrichment_prefix,
                    )
                finally:
                    db.close()
                enrichment_step = {
                    "status": "completed",
                    "reused": False,
                    "stats": {
                        "source_kind": enrichment_result.get("source_kind", ""),
                        "rows": enrichment_result.get("rows", 0),
                        "matched_rows": enrichment_result.get("matched_rows", 0),
                        "high_confidence_rows": enrichment_result.get("high_confidence_rows", 0),
                    },
                    "artifacts": {
                        "all_csv": str(enrichment_result.get("csv_path") or ""),
                        "all_xlsx": str(enrichment_result.get("xlsx_path") or ""),
                        "high_csv": str(enrichment_result.get("high_csv_path") or ""),
                        "high_xlsx": str(enrichment_result.get("high_xlsx_path") or ""),
                    },
                }
            enrichment_mode, enrichment_reason = _resolve_execution_details(
                reused=enrichment_reused,
                existing_summary_accepted=existing_summary_accepted,
                rerun_reason="enrichment_inputs_changed_or_resume_artifacts_missing",
            )
            enrichment_step = _annotate_step_payload(
                enrichment_step,
                execution_mode=enrichment_mode,
                execution_reason=enrichment_reason,
                input_refs={
                    "mail_db_path": mail_sync_step["artifacts"]["mail_db_path"],
                    "sending_list_workbook": summary["artifacts"]["sending_list_workbook"],
                },
                owned_artifact_keys=("all_csv", "all_xlsx", "high_csv", "high_xlsx"),
                resume_point_key="enrichment",
                reuse_supported=True,
                stage_policy="reuse_if_mail_db_and_sending_list_unchanged",
            )
            summary["steps"]["enrichment"] = enrichment_step
            summary["artifacts"]["enrichment_high_xlsx"] = enrichment_step["artifacts"]["high_xlsx"]
            summary["resume_points"]["enrichment"] = {
                "high_confidence_workbook": enrichment_step["artifacts"]["high_xlsx"],
            }
            persist_summary(summary)
            if normalized_stop_after == "enrichment":
                return mark_stop("enrichment")

            llm_candidates_existing = existing_summary.get("steps", {}).get("llm_candidates", {}) if existing_summary else {}
            llm_candidates_reused = bool(
                reuse_existing
                and downstream_reuse_allowed
                and enrichment_reused
                and _step_artifacts_exist(
                    llm_candidates_existing,
                    ("prep_xlsx", "deduped_xlsx", "llm_candidates_jsonl"),
                )
            )
            if llm_candidates_reused:
                llm_candidates_step = _json_clone(llm_candidates_existing)
                llm_candidates_step["status"] = "reused"
                llm_candidates_step["reused"] = True
            else:
                _emit_runtime_progress(progress_scope, "llm_candidates=running")
                db = Database(Path(mail_sync_step["artifacts"]["mail_db_path"]))
                try:
                    llm_candidates_result = prepare_llm_review_candidates(
                        db=db,
                        input_path=Path(summary["artifacts"]["enrichment_high_xlsx"]),
                        output_prefix=llm_prefix,
                    )
                finally:
                    db.close()
                llm_candidates_step = {
                    "status": "completed",
                    "reused": False,
                    "stats": {
                        "source_row_count": llm_candidates_result.get("source_row_count", 0),
                        "prep_row_count": llm_candidates_result.get("prep_row_count", 0),
                        "deduped_row_count": llm_candidates_result.get("deduped_row_count", 0),
                        "llm_candidate_group_count": llm_candidates_result.get("llm_candidate_group_count", 0),
                    },
                    "artifacts": {
                        "prep_xlsx": str(llm_candidates_result.get("prep_xlsx_path") or ""),
                        "deduped_xlsx": str(llm_candidates_result.get("deduped_xlsx_path") or ""),
                        "llm_candidates_jsonl": str(llm_candidates_result.get("llm_candidates_jsonl_path") or ""),
                    },
                }
            llm_candidates_mode, llm_candidates_reason = _resolve_execution_details(
                reused=llm_candidates_reused,
                existing_summary_accepted=existing_summary_accepted,
                rerun_reason="llm_candidates_inputs_changed_or_resume_artifacts_missing",
            )
            llm_candidates_step = _annotate_step_payload(
                llm_candidates_step,
                execution_mode=llm_candidates_mode,
                execution_reason=llm_candidates_reason,
                input_refs={
                    "high_confidence_workbook": summary["artifacts"]["enrichment_high_xlsx"],
                    "mail_db_path": mail_sync_step["artifacts"]["mail_db_path"],
                },
                owned_artifact_keys=("prep_xlsx", "deduped_xlsx", "llm_candidates_jsonl"),
                resume_point_key="llm_candidates",
                reuse_supported=True,
                stage_policy="reuse_only_if_enrichment_reused",
            )
            summary["steps"]["llm_candidates"] = llm_candidates_step
            summary["artifacts"]["llm_review_input_prefix"] = str(llm_prefix)
            summary["resume_points"]["llm_candidates"] = {
                "llm_review_input_prefix": str(llm_prefix),
                "llm_candidates_jsonl": llm_candidates_step["artifacts"]["llm_candidates_jsonl"],
            }
            persist_summary(summary)
            if normalized_stop_after == "llm-candidates":
                return mark_stop("llm-candidates")

            llm_review_existing = existing_summary.get("steps", {}).get("llm_review", {}) if existing_summary else {}
            llm_review_reused = bool(
                reuse_existing
                and downstream_reuse_allowed
                and enrichment_reused
                and llm_candidates_reused
                and _step_artifacts_exist(
                    llm_review_existing,
                    ("review_jsonl", "reviewed_xlsx", "keep_xlsx"),
                )
            )
            if llm_review_reused:
                llm_review_step = _json_clone(llm_review_existing)
                llm_review_step["status"] = "reused"
                llm_review_step["reused"] = True
            else:
                _emit_runtime_progress(progress_scope, "llm_review=running")
                llm_review_result = run_and_apply_llm_review(
                    input_prefix=llm_prefix,
                    env_path=env_file,
                    base_url=str(base_url or "").strip() or None,
                    api_key=str(api_key or "").strip() or None,
                    model=str(model or "").strip() or None,
                    wire_api=str(wire_api or "").strip() or None,
                )
                llm_review_step = {
                    "status": "completed",
                    "reused": False,
                    "stats": {
                        "review_group_count": llm_review_result.get("review_group_count", 0),
                        "reviewed_row_count": llm_review_result.get("reviewed_row_count", 0),
                        "keep_row_count": llm_review_result.get("keep_row_count", 0),
                    },
                    "artifacts": {
                        "review_jsonl": str(llm_review_result.get("llm_review_jsonl_path") or ""),
                        "reviewed_xlsx": str(llm_review_result.get("llm_reviewed_xlsx_path") or ""),
                        "keep_xlsx": str(llm_review_result.get("llm_reviewed_keep_xlsx_path") or ""),
                    },
                }
            llm_review_mode, llm_review_reason = _resolve_execution_details(
                reused=llm_review_reused,
                existing_summary_accepted=existing_summary_accepted,
                rerun_reason="llm_review_inputs_changed_or_resume_artifacts_missing",
            )
            llm_review_step = _annotate_step_payload(
                llm_review_step,
                execution_mode=llm_review_mode,
                execution_reason=llm_review_reason,
                input_refs={
                    "llm_review_input_prefix": str(llm_prefix),
                    "llm_candidates_jsonl": llm_candidates_step["artifacts"]["llm_candidates_jsonl"],
                },
                owned_artifact_keys=("review_jsonl", "reviewed_xlsx", "keep_xlsx"),
                resume_point_key="keep_list",
                reuse_supported=True,
                stage_policy="reuse_only_if_llm_candidates_reused",
            )
            summary["steps"]["llm_review"] = llm_review_step
            summary["artifacts"]["keep_workbook"] = llm_review_step["artifacts"]["keep_xlsx"]

        keep_list_resume_point = dict(summary["resume_points"].get("keep_list") or {})
        keep_list_resume_point.update(
            {
                "keep_workbook": summary["artifacts"]["keep_workbook"],
                "template_workbook": summary["artifacts"]["template_workbook"],
            }
        )
        summary["resume_points"]["keep_list"] = keep_list_resume_point
        summary["canonical_artifacts"]["keep_list"] = _json_clone(summary["resume_points"]["keep_list"])
        summary["downstream_handoff"] = {
            "boundary_step": "keep-list",
            "resume_point_key": "keep_list",
            "matching_strategy": normalized_matching_strategy,
            "runner_script": "scripts/run_keep_list_screening_pipeline.py",
            "keep_workbook": summary["artifacts"]["keep_workbook"],
            "template_workbook": summary["artifacts"]["template_workbook"],
            "recommended_command": (
                'backend/.venv/bin/python scripts/run_keep_list_screening_pipeline.py '
                f'--keep-workbook "{summary["artifacts"]["keep_workbook"]}" '
                f'--template-workbook "{summary["artifacts"]["template_workbook"]}" '
                "--task-name "
                f'"{normalized_task_name}" '
                '--summary-json "temp/keep_list_pipeline_summary.json" '
                "--platform instagram --max-identifiers-per-platform 1"
            ),
        }
        persist_summary(summary)
        if normalized_stop_after == "keep-list":
            return mark_stop("keep-list")
    except Exception as exc:  # noqa: BLE001
        failed_step = next(reversed(summary.get("steps") or {}), "")
        failure = _classify_failure(exc, failed_step=failed_step)
        if failure["stage"] == "preflight":
            summary["preflight"]["ready"] = False
            summary["preflight"]["errors"] = [failure]
        return finalize(
            "failed",
            failed_step=failed_step,
            failure=failure,
        )

    return finalize("completed")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the task-upload -> keep-list upstream pipeline through a single repo-local entrypoint."
    )
    parser.add_argument("--task-name", required=True, help="任务名，例如 MINISO。")
    parser.add_argument("--env-file", default=".env", help="本地 env 文件路径，默认 ./.env。")
    parser.add_argument("--task-upload-url", default="", help="飞书任务上传 wiki/base 链接。")
    parser.add_argument("--employee-info-url", default="", help="飞书员工信息表 wiki/base 链接。")
    parser.add_argument("--output-root", default="", help="输出目录；默认写到 temp/task_upload_to_keep_list_<timestamp>。")
    parser.add_argument("--summary-json", default="", help="最终 summary.json 输出路径。")
    parser.add_argument("--task-download-dir", default="", help="任务附件下载目录；默认写到 output-root/downloads。")
    parser.add_argument("--mail-data-dir", default="", help="任务邮件数据目录；默认写到 output-root/mail_sync。")
    parser.add_argument("--existing-mail-db-path", default="", help="已有共享邮箱 email_sync.db 路径；传入后跳过 IMAP 抓信。")
    parser.add_argument("--existing-mail-raw-dir", default="", help="已有共享邮箱 raw 邮件目录；默认 <existing-mail-db-path>/../raw。")
    parser.add_argument("--existing-mail-data-dir", default="", help="已有共享邮箱 mail data 根目录；默认 <existing-mail-db-path>/..。")
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
    parser.add_argument("--sent-since", default="", help="mail sync 起始日期 YYYY-MM-DD；默认今天。")
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
    parser.add_argument("--stop-after", default="", choices=("",) + STOP_AFTER_CHOICES, help="在指定边界后停止。")
    parser.add_argument("--no-reuse-existing", action="store_true", help="不要复用当前 output-root 下已存在的上游 artifact。")
    parser.add_argument("--base-url", default="", help="覆盖 duplicate review 的 LLM base URL。")
    parser.add_argument("--api-key", default="", help="覆盖 duplicate review 的 LLM API key。")
    parser.add_argument("--model", default="", help="覆盖 duplicate review 的 LLM model。")
    parser.add_argument("--wire-api", default="", help="覆盖 duplicate review 的 wire API。")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    summary = run_task_upload_to_keep_list_pipeline(
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
        owner_email_overrides=_parse_mapping_overrides(args.owner_email_override),
        mail_limit=max(0, int(args.mail_limit)),
        mail_workers=max(1, int(args.mail_workers)),
        sent_since=args.sent_since,
        reset_state=bool(args.reset_state),
        matching_strategy=args.matching_strategy,
        brand_keyword=args.brand_keyword,
        brand_match_include_from=bool(args.brand_match_include_from),
        stop_after=args.stop_after,
        reuse_existing=not bool(args.no_reuse_existing),
        base_url=args.base_url,
        api_key=args.api_key,
        model=args.model,
        wire_api=args.wire_api,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if summary.get("status") != "failed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
