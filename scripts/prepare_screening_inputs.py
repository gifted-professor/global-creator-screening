from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


import backend.app as backend_app
from backend.timezone_utils import isoformat_shanghai_datetime
from feishu_screening_bridge import download_task_upload_screening_assets
from feishu_screening_bridge.feishu_api import DEFAULT_FEISHU_BASE_URL, FeishuOpenClient
from feishu_screening_bridge.local_env import get_preferred_value, load_local_env
from harness.config import load_env_file_snapshot, resolve_string, source_record
from workbook_template_parser import compile_workbook


DEFAULT_TEMPLATE_OUTPUT_DIR = REPO_ROOT / "downloads" / "task_upload_attachments" / "parsed_outputs"
DEFAULT_TASK_UPLOAD_DOWNLOAD_DIR = REPO_ROOT / "downloads" / "task_upload_attachments"
NORMALIZED_UPLOAD_OUTPUT_DIRNAME = "normalized_upload_workbooks"

CANONICAL_UPLOAD_EXPORT_COLUMNS = [
    "Platform",
    "@username",
    "URL",
    "nickname",
    "Region",
    "email",
    "tiktok_url",
    "instagram_url",
    "youtube_url",
    "platform_attempt_order",
    "mail_thread_key",
    "mail_resolution_stage",
    "mail_resolution_confidence",
    "mail_apify_gate",
    "mail_evidence",
    "resolution_evidence",
    "mail_raw_path",
    "latest_external_from",
    "latest_external_sent_at",
    "subject",
    "candidate_sources",
    "original_decision",
    "final_decision",
    "original_reject_reason",
    "business_signal_detected",
    "review_priority",
    "rescue_rule_applied",
    "confidence_before_rescue",
    "confidence_after_rescue",
    "hard_reject_blocked_rescue",
]
SENDING_LIST_COUNTRY_ALIASES = ("country", "国家", "region", "地区")
SENDING_LIST_CREATOR_ALIASES = ("creator", "nickname", "达人", "红人", "博主")
SENDING_LIST_HANDLE_ALIASES = ("@username", "username", "用户名", "博主用户名", "handle", "账号", "达人账号")
SENDING_LIST_EMAIL_ALIASES = ("邮箱地址", "邮箱", "email", "emailaddress", "mail")
SENDING_LIST_GENERIC_LINK_ALIASES = ("link", "url", "主页链接", "账号链接", "profilelink", "profileurl")
SENDING_LIST_PLATFORM_LINK_ALIASES = {
    "instagram": ("iglink", "igurl", "instagramlink", "instagramurl", "inslink", "insurl"),
    "tiktok": ("ttlink", "tturl", "tiktoklink", "tiktokurl", "douyinlink"),
    "youtube": ("ytlink", "yturl", "youtubelink", "youtubeurl", "channelurl", "channellink"),
}
MAIL_THREAD_FINAL_ID_ALIASES = ("final_id_final", "final_id")
MAIL_THREAD_STAGE_ALIASES = ("resolution_stage_final", "resolution_stage")
MAIL_THREAD_CONFIDENCE_ALIASES = (
    "resolution_confidence_final",
    "llm_confidence",
    "resolution_confidence",
    "confidence",
)
MAIL_THREAD_LLM_HANDLE_ALIASES = ("llm_handle",)
MAIL_THREAD_EVIDENCE_ALIASES = ("llm_evidence", "latest_external_clean_body", "latest_external_body_preview")
MAIL_THREAD_PLATFORM_ALIASES = ("Platform", "平台", "platform")
MAIL_THREAD_AUTO_REPLY_PATTERNS = (
    re.compile(
        r"(out of office|automatic reply|auto.?reply|ooo\b|vacation|abwesenheitsnotiz|thank you for your email|derzeit keine erreichbarkeit)",
        re.I,
    ),
    re.compile(r"(deine anfrage ist bei uns gelandet|ticket#|\[##gl-\d+##\])", re.I),
)
MAIL_THREAD_APIFY_READY_STAGES = {
    "pass0_sending_list_email",
    "pass0_sending_list_handle",
    "sending_list",
    "regex_pass1",
    "regex_pass2",
    "regex_in_sending_list",
    "regex_out_of_sending_list",
}


def _clean_mail_thread_value(value: Any) -> str:
    return clean_source_cell(value)


def _mail_thread_row_is_empty(row_dict: dict[str, Any]) -> bool:
    return not any(_clean_mail_thread_value(value) for value in (row_dict or {}).values())


def _select_mail_thread_value(row_dict: dict[str, Any], aliases: tuple[str, ...]) -> str:
    normalized_row = {
        normalize_source_column_name(column): value
        for column, value in (row_dict or {}).items()
        if not str(column).startswith("__")
    }
    for alias in aliases:
        value = _clean_mail_thread_value(normalized_row.get(normalize_source_column_name(alias)))
        if value:
            return value
    return ""


def _has_mail_thread_funnel_columns(headers: list[str]) -> bool:
    normalized_headers = {normalize_source_column_name(header) for header in headers if not str(header).startswith("__")}
    return (
        any(normalize_source_column_name(alias) in normalized_headers for alias in MAIL_THREAD_FINAL_ID_ALIASES)
        and (
            normalize_source_column_name("latest_external_full_body") in normalized_headers
            or normalize_source_column_name("raw_path") in normalized_headers
        )
    )


def _resolve_mail_thread_effective_body(row_dict: dict[str, Any]) -> str:
    clean_body = _clean_mail_thread_value(row_dict.get("latest_external_clean_body"))
    if clean_body:
        return clean_body
    return _clean_mail_thread_value(row_dict.get("latest_external_full_body"))


def _normalize_mail_thread_stage(value: Any) -> str:
    return _clean_mail_thread_value(value).strip().lower()


def _normalize_mail_thread_confidence(value: Any) -> str:
    return _clean_mail_thread_value(value).strip().lower()


def _normalize_mail_thread_sent_at(value: Any) -> str:
    return isoformat_shanghai_datetime(value) or _clean_mail_thread_value(value)


def _normalize_mail_thread_flag(value: Any) -> str:
    text = _clean_mail_thread_value(value).strip().casefold()
    if text in {"true", "false"}:
        return text
    return text


def _normalize_mail_thread_platform(value: Any) -> str:
    text = _clean_mail_thread_value(value).strip().lower()
    if not text:
        return ""
    inferred = infer_platform_from_value(text)
    if inferred:
        return inferred
    if "instagram" in text or text in {"ig", "ins"}:
        return "instagram"
    if "tiktok" in text or "douyin" in text or text == "tt":
        return "tiktok"
    if "youtube" in text or text == "yt":
        return "youtube"
    return ""


def _normalize_brand_token(value: Any) -> str:
    text = _clean_mail_thread_value(value).strip().lower()
    return re.sub(r"[^a-z0-9]+", "", text)


def _mail_thread_matches_target_brand(row_dict: dict[str, Any], target_brand: str) -> bool:
    normalized_brand = _normalize_brand_token(target_brand)
    if not normalized_brand:
        return True
    haystacks = (
        _clean_mail_thread_value(row_dict.get("subject")),
        _resolve_mail_thread_effective_body(row_dict),
    )
    return any(normalized_brand in _normalize_brand_token(haystack) for haystack in haystacks if haystack)


def _is_mail_thread_auto_reply_like(row_dict: dict[str, Any]) -> bool:
    subject = _clean_mail_thread_value(row_dict.get("subject"))
    effective_body = _resolve_mail_thread_effective_body(row_dict)
    for pattern in MAIL_THREAD_AUTO_REPLY_PATTERNS:
        if pattern.search(subject) or pattern.search(effective_body):
            return True
    return False


def _resolve_mail_thread_apify_gate(row_dict: dict[str, Any]) -> tuple[str, str, str]:
    stage = _normalize_mail_thread_stage(_select_mail_thread_value(row_dict, MAIL_THREAD_STAGE_ALIASES))
    confidence = _normalize_mail_thread_confidence(_select_mail_thread_value(row_dict, MAIL_THREAD_CONFIDENCE_ALIASES))
    if stage in MAIL_THREAD_APIFY_READY_STAGES:
        return "ready_for_apify", stage, confidence
    if stage == "llm":
        if confidence == "high":
            return "ready_for_apify", stage, confidence
        return "manual_review", stage, confidence or "unknown"
    if stage in {"weak_rule", "regex_out_list_ambiguous", "uncertain"}:
        return "manual_review", stage, confidence or "unknown"
    if stage.startswith("filtered_"):
        return "filtered_out", stage, confidence or "unknown"
    return "manual_review", stage, confidence or "unknown"


def _resolve_mail_thread_handle_stage_and_confidence(row_dict: dict[str, Any]) -> tuple[str, str, str]:
    handle = _clean_mail_thread_value(_select_mail_thread_value(row_dict, MAIL_THREAD_FINAL_ID_ALIASES))
    stage = _normalize_mail_thread_stage(_select_mail_thread_value(row_dict, MAIL_THREAD_STAGE_ALIASES))
    confidence = _normalize_mail_thread_confidence(_select_mail_thread_value(row_dict, MAIL_THREAD_CONFIDENCE_ALIASES))
    llm_handle = _clean_mail_thread_value(_select_mail_thread_value(row_dict, MAIL_THREAD_LLM_HANDLE_ALIASES))
    if stage == "weak_rule" and llm_handle and confidence == "high":
        return llm_handle, "llm", "high"
    return handle, stage, confidence


def build_canonical_upload_from_mail_thread_funnel(
    source_path: Path,
    frames: list[Any],
    *,
    target_brand: str = "",
) -> tuple[Any | None, dict[str, Any]]:
    records_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    record_sort_key: dict[tuple[str, str], tuple[str, int]] = {}
    input_row_count = 0
    skipped_row_count = 0
    converted_row_count = 0
    auto_reply_skipped_count = 0
    evidence_fallback_count = 0
    manual_review_skipped_count = 0
    filtered_out_count = 0
    brand_filtered_skipped_count = 0
    llm_high_accepted_count = 0
    llm_non_high_skipped_count = 0

    for frame in frames:
        if frame is None or frame.empty:
            continue
        for _, row_series in frame.iterrows():
            row_dict = row_series.to_dict()
            if _mail_thread_row_is_empty(row_dict):
                continue
            input_row_count += 1
            if not _mail_thread_matches_target_brand(row_dict, target_brand):
                brand_filtered_skipped_count += 1
                continue
            handle, normalized_stage_override, normalized_confidence_override = _resolve_mail_thread_handle_stage_and_confidence(row_dict)
            if not handle:
                skipped_row_count += 1
                continue
            if _is_mail_thread_auto_reply_like(row_dict):
                auto_reply_skipped_count += 1
                continue
            apify_gate, normalized_stage, normalized_confidence = _resolve_mail_thread_apify_gate(row_dict)
            if normalized_stage_override:
                normalized_stage = normalized_stage_override
            if normalized_confidence_override:
                normalized_confidence = normalized_confidence_override
            if normalized_stage == "llm" and normalized_confidence == "high":
                apify_gate = "ready_for_apify"
            if apify_gate == "filtered_out":
                filtered_out_count += 1
                continue
            if apify_gate != "ready_for_apify":
                manual_review_skipped_count += 1
                if normalized_stage == "llm":
                    llm_non_high_skipped_count += 1
                continue
            if normalized_stage == "llm" and normalized_confidence == "high":
                llm_high_accepted_count += 1
            row_platform = _normalize_mail_thread_platform(_select_mail_thread_value(row_dict, MAIL_THREAD_PLATFORM_ALIASES))
            normalized_platform = row_platform or "tiktok"
            normalized_handle = (
                backend_app.screening.extract_platform_identifier(normalized_platform, handle)
                or _clean_text(handle).lstrip("@")
            )
            if not normalized_handle:
                skipped_row_count += 1
                continue

            platform = backend_app.UPLOAD_PLATFORM_RESPONSE_LABELS.get(normalized_platform, normalized_platform)
            tiktok_url = backend_app.screening.build_canonical_profile_url("tiktok", normalized_handle)
            instagram_url = backend_app.screening.build_canonical_profile_url("instagram", normalized_handle)
            youtube_url = backend_app.screening.build_canonical_profile_url("youtube", normalized_handle)
            canonical_url = backend_app.screening.build_canonical_profile_url(normalized_platform, normalized_handle)
            attempt_order = ",".join(
                [normalized_platform, *[item for item in ("tiktok", "instagram", "youtube") if item != normalized_platform]]
            )
            resolution_stage = normalized_stage or _select_mail_thread_value(row_dict, MAIL_THREAD_STAGE_ALIASES)
            resolution_confidence = normalized_confidence or _select_mail_thread_value(row_dict, MAIL_THREAD_CONFIDENCE_ALIASES)
            mail_evidence = _select_mail_thread_value(row_dict, MAIL_THREAD_EVIDENCE_ALIASES)
            if not mail_evidence:
                mail_evidence = _clean_mail_thread_value(row_dict.get("latest_external_full_body"))
                if mail_evidence:
                    evidence_fallback_count += 1
            latest_external_sent_at = _normalize_mail_thread_sent_at(row_dict.get("latest_external_sent_at"))
            source_row_number = int(row_dict.get("__source_row_number") or 0)
            record_key = (normalized_platform, normalized_handle)
            sort_key = (latest_external_sent_at, source_row_number)
            if record_key in record_sort_key and record_sort_key[record_key] >= sort_key:
                continue
            record_sort_key[record_key] = sort_key
            records_by_key[record_key] = {
                "Platform": platform,
                "@username": normalized_handle,
                "URL": canonical_url,
                "nickname": normalized_handle,
                "Region": "",
                "email": _clean_mail_thread_value(row_dict.get("latest_external_from")),
                "tiktok_url": tiktok_url,
                "instagram_url": instagram_url,
                "youtube_url": youtube_url,
                "platform_attempt_order": attempt_order,
                "mail_thread_key": _clean_mail_thread_value(row_dict.get("thread_key")),
                "mail_resolution_stage": resolution_stage,
                "mail_resolution_confidence": resolution_confidence,
                "mail_apify_gate": apify_gate,
                "mail_evidence": mail_evidence,
                "resolution_evidence": _clean_mail_thread_value(row_dict.get("resolution_evidence")),
                "mail_raw_path": _clean_mail_thread_value(row_dict.get("raw_path")),
                "latest_external_from": _clean_mail_thread_value(row_dict.get("latest_external_from")),
                "latest_external_sent_at": latest_external_sent_at,
                "subject": _clean_mail_thread_value(row_dict.get("subject")),
                "candidate_sources": _clean_mail_thread_value(row_dict.get("candidate_sources")),
                "original_decision": _clean_mail_thread_value(row_dict.get("original_decision")),
                "final_decision": _clean_mail_thread_value(row_dict.get("final_decision")),
                "original_reject_reason": _clean_mail_thread_value(row_dict.get("original_reject_reason")),
                "business_signal_detected": _normalize_mail_thread_flag(row_dict.get("business_signal_detected")),
                "review_priority": _clean_mail_thread_value(row_dict.get("review_priority")),
                "rescue_rule_applied": _clean_mail_thread_value(row_dict.get("rescue_rule_applied")),
                "confidence_before_rescue": _clean_mail_thread_value(row_dict.get("confidence_before_rescue")),
                "confidence_after_rescue": _clean_mail_thread_value(row_dict.get("confidence_after_rescue")),
                "hard_reject_blocked_rescue": _normalize_mail_thread_flag(row_dict.get("hard_reject_blocked_rescue")),
            }
            converted_row_count += 1

    if not records_by_key:
        return None, {}

    dataframe = backend_app.pd.DataFrame(list(records_by_key.values()), columns=CANONICAL_UPLOAD_EXPORT_COLUMNS)
    return dataframe, {
        "sourceType": "mail_thread_funnel",
        "sourcePath": str(source_path),
        "inputRowCount": input_row_count,
        "recordCount": len(records_by_key),
        "convertedRowCount": converted_row_count,
        "skippedRowCount": skipped_row_count,
        "autoReplySkippedCount": auto_reply_skipped_count,
        "manualReviewSkippedCount": manual_review_skipped_count,
        "filteredOutCount": filtered_out_count,
        "brandFilteredSkippedCount": brand_filtered_skipped_count,
        "llmHighAcceptedCount": llm_high_accepted_count,
        "llmNonHighSkippedCount": llm_non_high_skipped_count,
        "evidenceFallbackCount": evidence_fallback_count,
    }


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


def _build_resolved_config_sources(
    *,
    env_file: str | Path,
    task_name: str,
    task_upload_url: str,
    feishu_app_id: str,
    feishu_app_secret: str,
    feishu_base_url: str,
    timeout_seconds: float,
    task_download_dir: Path | None,
    template_output_dir: Path | None,
    screening_data_dir: Path | None,
    config_dir: Path | None,
    temp_dir: Path | None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    env_snapshot = load_env_file_snapshot(env_file)
    resolved_task_upload_url = resolve_string(
        cli_value=task_upload_url,
        env_snapshot=env_snapshot,
        env_keys=("TASK_UPLOAD_URL", "FEISHU_SOURCE_URL"),
    )
    resolved_feishu_app_id = resolve_string(
        cli_value=feishu_app_id,
        env_snapshot=env_snapshot,
        env_keys=("FEISHU_APP_ID",),
    )
    resolved_feishu_app_secret = resolve_string(
        cli_value=feishu_app_secret,
        env_snapshot=env_snapshot,
        env_keys=("FEISHU_APP_SECRET",),
    )
    resolved_feishu_base_url = resolve_string(
        cli_value=feishu_base_url,
        env_snapshot=env_snapshot,
        env_keys=("FEISHU_OPEN_BASE_URL",),
        default=DEFAULT_FEISHU_BASE_URL,
    )
    resolved_timeout_seconds = resolve_string(
        cli_value=timeout_seconds if float(timeout_seconds or 0.0) > 0 else "",
        env_snapshot=env_snapshot,
        env_keys=("TIMEOUT_SECONDS",),
        default="30",
    )
    resolved_task_name = resolve_string(
        cli_value=task_name,
        env_snapshot=env_snapshot,
    )
    return (
        {
            "env_file": env_snapshot.source,
            "task_name": source_record(resolved_task_name),
            "task_upload_url": source_record(resolved_task_upload_url),
            "feishu_app_id": source_record(resolved_feishu_app_id, sensitive=True),
            "feishu_app_secret": source_record(resolved_feishu_app_secret, sensitive=True),
            "feishu_base_url": source_record(resolved_feishu_base_url),
            "timeout_seconds": source_record(resolved_timeout_seconds),
            "task_download_dir": "cli" if task_download_dir is not None else "default",
            "template_output_dir": "cli" if template_output_dir is not None else "default",
            "screening_data_dir": "cli" if screening_data_dir is not None else "runtime_default",
            "config_dir": "cli" if config_dir is not None else "runtime_default",
            "temp_dir": "cli" if temp_dir is not None else "runtime_default",
        },
        {"env_snapshot": env_snapshot},
    )


def configure_backend_runtime(
    *,
    screening_data_dir: Path | None = None,
    config_dir: Path | None = None,
    temp_dir: Path | None = None,
) -> None:
    if screening_data_dir is not None:
        backend_app.DATA_DIR = str(screening_data_dir)
    if config_dir is not None:
        backend_app.CONFIG_DIR = str(config_dir)
    if temp_dir is not None:
        backend_app.TEMP_DIR = str(temp_dir)

    backend_app.UPLOAD_FOLDER = str(Path(backend_app.DATA_DIR) / "uploads")
    backend_app.ACTIVE_RULESPEC_PATH = str(Path(backend_app.CONFIG_DIR) / "active_rulespec.json")
    backend_app.ACTIVE_VISUAL_PROMPTS_PATH = str(Path(backend_app.CONFIG_DIR) / "active_visual_prompts.json")
    backend_app.FIELD_MATCH_REPORT_PATH = str(Path(backend_app.CONFIG_DIR) / "field_match_report.json")
    backend_app.MISSING_CAPABILITIES_PATH = str(Path(backend_app.CONFIG_DIR) / "missing_capabilities.json")
    backend_app.REVIEW_NOTES_PATH = str(Path(backend_app.CONFIG_DIR) / "review_notes.md")
    backend_app.APIFY_TOKEN_POOL_STATE_FILE = str(Path(backend_app.DATA_DIR) / "apify_token_pool_state.json")
    backend_app.APIFY_BALANCE_CACHE_FILE = str(Path(backend_app.DATA_DIR) / "apify_balance_cache.json")
    backend_app.APIFY_RUN_GUARDS_FILE = str(Path(backend_app.DATA_DIR) / "apify_run_guards.json")
    backend_app.app.config["UPLOAD_FOLDER"] = backend_app.UPLOAD_FOLDER


def snapshot_backend_runtime_state() -> dict[str, Any]:
    return {
        "DATA_DIR": str(getattr(backend_app, "DATA_DIR", "")),
        "CONFIG_DIR": str(getattr(backend_app, "CONFIG_DIR", "")),
        "TEMP_DIR": str(getattr(backend_app, "TEMP_DIR", "")),
        "UPLOAD_FOLDER": str(getattr(backend_app, "UPLOAD_FOLDER", "")),
        "ACTIVE_RULESPEC_PATH": str(getattr(backend_app, "ACTIVE_RULESPEC_PATH", "")),
        "ACTIVE_VISUAL_PROMPTS_PATH": str(getattr(backend_app, "ACTIVE_VISUAL_PROMPTS_PATH", "")),
        "FIELD_MATCH_REPORT_PATH": str(getattr(backend_app, "FIELD_MATCH_REPORT_PATH", "")),
        "MISSING_CAPABILITIES_PATH": str(getattr(backend_app, "MISSING_CAPABILITIES_PATH", "")),
        "REVIEW_NOTES_PATH": str(getattr(backend_app, "REVIEW_NOTES_PATH", "")),
        "APIFY_TOKEN_POOL_STATE_FILE": str(getattr(backend_app, "APIFY_TOKEN_POOL_STATE_FILE", "")),
        "APIFY_BALANCE_CACHE_FILE": str(getattr(backend_app, "APIFY_BALANCE_CACHE_FILE", "")),
        "APIFY_RUN_GUARDS_FILE": str(getattr(backend_app, "APIFY_RUN_GUARDS_FILE", "")),
        "app_upload_folder": str(getattr(getattr(backend_app, "app", None), "config", {}).get("UPLOAD_FOLDER", "")),
    }


def restore_backend_runtime_state(snapshot: dict[str, Any]) -> None:
    for attribute in (
        "DATA_DIR",
        "CONFIG_DIR",
        "TEMP_DIR",
        "UPLOAD_FOLDER",
        "ACTIVE_RULESPEC_PATH",
        "ACTIVE_VISUAL_PROMPTS_PATH",
        "FIELD_MATCH_REPORT_PATH",
        "MISSING_CAPABILITIES_PATH",
        "REVIEW_NOTES_PATH",
        "APIFY_TOKEN_POOL_STATE_FILE",
        "APIFY_BALANCE_CACHE_FILE",
        "APIFY_RUN_GUARDS_FILE",
    ):
        if attribute in snapshot:
            setattr(backend_app, attribute, snapshot[attribute])
    app_config = getattr(getattr(backend_app, "app", None), "config", None)
    if isinstance(app_config, dict):
        app_config["UPLOAD_FOLDER"] = str(snapshot.get("app_upload_folder", snapshot.get("UPLOAD_FOLDER", "")))


def resolve_task_upload_source_files(
    *,
    task_name: str,
    task_upload_url: str = "",
    env_file: str | Path = ".env",
    feishu_app_id: str = "",
    feishu_app_secret: str = "",
    feishu_base_url: str = "",
    task_download_dir: Path | None = None,
    timeout_seconds: float = 30.0,
    download_template: bool = True,
    download_sending_list: bool = True,
) -> dict[str, Any]:
    env_values = load_local_env(env_file)
    app_id, app_id_source = _resolve_cli_env_value(feishu_app_id, env_values, "FEISHU_APP_ID")
    app_secret, app_secret_source = _resolve_cli_env_value(feishu_app_secret, env_values, "FEISHU_APP_SECRET")
    if not app_id:
        raise ValueError("缺少 FEISHU_APP_ID，请在本地 .env 或参数里填写。")
    if not app_secret:
        raise ValueError("缺少 FEISHU_APP_SECRET，请在本地 .env 或参数里填写。")

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

    download_dir_value, download_dir_source = _resolve_cli_env_value(
        str(task_download_dir or ""),
        env_values,
        "TASK_UPLOAD_DOWNLOAD_DIR",
        str(DEFAULT_TASK_UPLOAD_DOWNLOAD_DIR),
    )
    resolved_download_dir = Path(
        download_dir_value
    ).expanduser()
    timeout_value, timeout_source = _resolve_cli_env_value(
        timeout_seconds if timeout_seconds > 0 else "",
        env_values,
        "TIMEOUT_SECONDS",
        "30",
    )
    resolved_timeout_seconds = float(timeout_value)
    feishu_base_url_value, feishu_base_url_source = _resolve_cli_env_value(
        feishu_base_url,
        env_values,
        "FEISHU_OPEN_BASE_URL",
        DEFAULT_FEISHU_BASE_URL,
    )
    client = FeishuOpenClient(
        app_id=app_id,
        app_secret=app_secret,
        base_url=feishu_base_url_value,
        timeout_seconds=resolved_timeout_seconds,
    )
    result = download_task_upload_screening_assets(
        client=client,
        task_upload_url=resolved_task_upload_url,
        task_name=task_name,
        download_dir=resolved_download_dir,
        download_template=download_template,
        download_sending_list=download_sending_list,
    )
    result["taskUploadUrl"] = resolved_task_upload_url
    result["downloadDir"] = str(resolved_download_dir)
    result["resolvedConfig"] = {
        "env_file": str(Path(env_file).expanduser().resolve()),
        "env_file_exists": Path(env_file).expanduser().exists(),
        "feishu_app_id_source": app_id_source,
        "feishu_app_secret_source": app_secret_source,
        "task_upload_url_source": task_upload_url_source,
        "task_download_dir_source": download_dir_source,
        "timeout_seconds": resolved_timeout_seconds,
        "timeout_seconds_source": timeout_source,
        "feishu_base_url": feishu_base_url_value,
        "feishu_base_url_source": feishu_base_url_source,
    }
    return result


def load_upload_frames(source_path: Path) -> list[Any]:
    suffix = source_path.suffix.lower()
    if suffix == ".csv":
        dataframe = backend_app.pd.read_csv(source_path, encoding="utf-8-sig")
        if dataframe.empty:
            return []
        prepared = dataframe.copy()
        prepared["__sheet_name"] = "Sheet1"
        prepared["__sheet_row_num"] = [index + 2 for index in range(len(prepared))]
        return [prepared]
    return backend_app.load_canonical_upload_workbook_frames(str(source_path))


def normalize_source_column_name(name: Any) -> str:
    return re.sub(r"[\s_\-./（）()]+", "", str(name or "").strip().lower())


def resolve_source_column(columns: list[Any], aliases: tuple[str, ...]) -> Any:
    normalized_aliases = {normalize_source_column_name(alias) for alias in aliases if str(alias or "").strip()}
    for column in columns:
        if normalize_source_column_name(column) in normalized_aliases:
            return column
    return None


def clean_source_cell(value: Any) -> str:
    cleaned = backend_app.clean_upload_metadata_value(value)
    return str(cleaned).strip() if cleaned not in ("", None) else ""


def infer_platform_from_value(value: Any) -> str:
    text = clean_source_cell(value).lower()
    if not text:
        return ""
    if "instagram.com" in text:
        return "instagram"
    if "tiktok.com" in text:
        return "tiktok"
    if "youtube.com" in text or "youtu.be/" in text:
        return "youtube"
    return ""


def infer_platform_from_series(series: Any, limit: int = 20) -> str:
    hits: dict[str, int] = {}
    checked = 0
    for value in series.tolist():
        platform = infer_platform_from_value(value)
        if platform:
            hits[platform] = hits.get(platform, 0) + 1
        if clean_source_cell(value):
            checked += 1
        if checked >= limit:
            break
    if not hits:
        return ""
    return max(hits.items(), key=lambda item: item[1])[0]


def resolve_sending_list_link_columns(frame: Any) -> list[tuple[Any, str]]:
    resolved: list[tuple[Any, str]] = []
    seen_columns: set[Any] = set()
    normalized_generic_aliases = {
        normalize_source_column_name(alias)
        for alias in SENDING_LIST_GENERIC_LINK_ALIASES
    }
    normalized_platform_aliases = {
        platform: {
            normalize_source_column_name(alias)
            for alias in aliases
        }
        for platform, aliases in SENDING_LIST_PLATFORM_LINK_ALIASES.items()
    }

    for column in frame.columns:
        if str(column).startswith("__") or column in seen_columns:
            continue
        normalized_column = normalize_source_column_name(column)
        explicit_platform = ""
        for platform, aliases in normalized_platform_aliases.items():
            if normalized_column in aliases:
                explicit_platform = platform
                break
        inferred_platform = infer_platform_from_series(frame[column])
        if explicit_platform or inferred_platform or normalized_column in normalized_generic_aliases:
            resolved.append((column, inferred_platform or explicit_platform))
            seen_columns.add(column)
    return resolved


def build_canonical_upload_from_sending_list(
    source_path: Path,
    frames: list[Any],
) -> tuple[Any | None, dict[str, Any]]:
    records_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    input_row_count = 0
    skipped_row_count = 0
    matched_link_count = 0
    sheet_summaries: list[dict[str, Any]] = []

    for frame in frames:
        if frame is None or frame.empty:
            continue
        columns = list(frame.columns)
        country_column = resolve_source_column(columns, SENDING_LIST_COUNTRY_ALIASES)
        creator_column = resolve_source_column(columns, SENDING_LIST_CREATOR_ALIASES)
        handle_column = resolve_source_column(columns, SENDING_LIST_HANDLE_ALIASES)
        email_column = resolve_source_column(columns, SENDING_LIST_EMAIL_ALIASES)
        link_columns = resolve_sending_list_link_columns(frame)
        if not link_columns:
            continue

        sample_sheet_name = "Sheet1"
        if "__sheet_name" in frame.columns and not frame.empty:
            sample_sheet_name = clean_source_cell(frame.iloc[0].get("__sheet_name")) or "Sheet1"
        sheet_converted_count = 0
        sheet_skipped_count = 0

        for _, row_series in frame.iterrows():
            row_dict = row_series.to_dict()
            if backend_app.is_empty_upload_row(row_dict):
                continue
            input_row_count += 1
            nickname = clean_source_cell(row_dict.get(creator_column)) if creator_column else ""
            handle = clean_source_cell(row_dict.get(handle_column)) if handle_column else ""
            region = clean_source_cell(row_dict.get(country_column)) if country_column else ""
            email = clean_source_cell(row_dict.get(email_column)) if email_column else ""
            row_seen_keys: set[tuple[str, str]] = set()

            for link_column, default_platform in link_columns:
                raw_link_value = clean_source_cell(row_dict.get(link_column))
                if not raw_link_value:
                    continue
                platform = infer_platform_from_value(raw_link_value) or default_platform
                if not platform:
                    continue
                identifier = (
                    backend_app.screening.extract_platform_identifier(platform, raw_link_value)
                    or backend_app.screening.extract_platform_identifier(platform, handle)
                    or backend_app.screening.extract_platform_identifier(platform, nickname)
                )
                if not identifier:
                    continue
                record_key = (platform, identifier)
                if record_key in row_seen_keys:
                    continue
                row_seen_keys.add(record_key)
                matched_link_count += 1
                sheet_converted_count += 1

                canonical_url = raw_link_value
                if "://" not in canonical_url:
                    canonical_url = backend_app.screening.build_canonical_profile_url(platform, identifier)
                existing = records_by_key.get(record_key)
                if existing is None:
                    records_by_key[record_key] = {
                        "Platform": backend_app.UPLOAD_PLATFORM_RESPONSE_LABELS.get(platform, platform),
                        "@username": identifier,
                        "URL": canonical_url,
                        "nickname": nickname,
                        "Region": region,
                        "email": email,
                    }
                    continue
                if not existing.get("URL") and canonical_url:
                    existing["URL"] = canonical_url
                if not existing.get("nickname") and nickname:
                    existing["nickname"] = nickname
                if not existing.get("Region") and region:
                    existing["Region"] = region
                if not existing.get("email") and email:
                    existing["email"] = email

            if not row_seen_keys:
                skipped_row_count += 1
                sheet_skipped_count += 1

        sheet_summaries.append({
            "sheetName": sample_sheet_name,
            "linkColumns": [
                {
                    "column": str(column),
                    "defaultPlatform": platform,
                }
                for column, platform in link_columns
            ],
            "convertedLinks": sheet_converted_count,
            "skippedRows": sheet_skipped_count,
        })

    if not records_by_key:
        return None, {}

    dataframe = backend_app.pd.DataFrame(list(records_by_key.values()), columns=CANONICAL_UPLOAD_EXPORT_COLUMNS)
    return dataframe, {
        "sourceType": "sending_list",
        "sourcePath": str(source_path),
        "inputRowCount": input_row_count,
        "recordCount": len(records_by_key),
        "matchedLinkCount": matched_link_count,
        "skippedRowCount": skipped_row_count,
        "sheetSummaries": sheet_summaries,
    }


def persist_normalized_upload_dataframe(source_path: Path, dataframe: Any) -> Path:
    output_dir = Path(backend_app.TEMP_DIR) / NORMALIZED_UPLOAD_OUTPUT_DIRNAME
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{source_path.stem}__canonical_upload.csv"
    dataframe.to_csv(output_path, index=False, encoding="utf-8-sig")
    return output_path


def count_non_empty_upload_rows(frames: list[Any]) -> int:
    row_count = 0
    for frame in frames:
        if frame is None or frame.empty:
            continue
        for _, row_series in frame.iterrows():
            row_dict = row_series.to_dict()
            if backend_app.is_empty_upload_row(row_dict):
                continue
            row_count += 1
    return row_count


def infer_creator_source_kind(source_path: Path, dataframe: Any) -> str:
    normalized_name = normalize_source_column_name(source_path.stem)
    normalized_columns = {
        normalize_source_column_name(column)
        for column in list(dataframe.columns)
        if not str(column).startswith("__")
    }
    if (
        "llmreviewedkeep" in normalized_name
        or "reviewedkeep" in normalized_name
        or {"llmreviewstatus", "llmreviewdecision", "llmkeep", "creatordedupekey"} & normalized_columns
    ):
        return "keep_list"
    return "canonical_upload"


def _raise_upload_error(error_response: Any) -> None:
    response, _status_code = error_response
    payload = response.get_json(silent=True) or {}
    message = payload.get("error") or "上传名单解析失败"
    details = payload.get("details") or []
    if details:
        message = f"{message}: {'; '.join(str(item) for item in details)}"
    raise ValueError(message)


def persist_active_rulespec(rulespec_json_path: Path) -> dict[str, Any]:
    payload = backend_app.load_json_payload(str(rulespec_json_path), default={}) or {}
    if not isinstance(payload, dict) or not payload:
        raise ValueError(f"rulespec 文件无效: {rulespec_json_path}")
    backend_app.write_json_file(backend_app.ACTIVE_RULESPEC_PATH, payload)
    return payload


def persist_active_visual_prompts(visual_prompts_json_path: Path) -> dict[str, Any]:
    payload = backend_app.load_json_payload(str(visual_prompts_json_path), default={}) or {}
    if not isinstance(payload, dict) or not payload:
        raise ValueError(f"visual_prompts 文件无效: {visual_prompts_json_path}")
    backend_app.write_json_file(backend_app.ACTIVE_VISUAL_PROMPTS_PATH, payload)
    return payload


def clear_active_visual_prompts() -> None:
    active_path = Path(backend_app.ACTIVE_VISUAL_PROMPTS_PATH)
    if active_path.exists():
        active_path.unlink()


def _append_prompt_platform(platforms: list[str], seen: set[str], candidate: Any) -> None:
    normalized = backend_app.normalize_visual_prompt_lookup_key(candidate)
    if normalized not in backend_app.PLATFORM_ACTORS or normalized in seen:
        return
    seen.add(normalized)
    platforms.append(normalized)


def resolve_runtime_prompt_platforms(
    *,
    active_rulespec: dict[str, Any] | None,
    active_visual_prompts: dict[str, Any] | None,
) -> list[str]:
    platforms: list[str] = []
    seen: set[str] = set()
    prompt_payload = active_visual_prompts if isinstance(active_visual_prompts, dict) else {}
    rulespec_payload = active_rulespec if isinstance(active_rulespec, dict) else {}

    for container_key in ("platform_prompts", "platforms"):
        container = prompt_payload.get(container_key)
        if not isinstance(container, dict):
            continue
        for candidate_key, candidate_value in container.items():
            if isinstance(candidate_value, dict):
                _append_prompt_platform(platforms, seen, candidate_key)

    for candidate_key, candidate_value in prompt_payload.items():
        if candidate_key in {"platform_prompts", "platforms"}:
            continue
        if isinstance(candidate_value, dict):
            _append_prompt_platform(platforms, seen, candidate_key)

    overrides = rulespec_payload.get("platform_overrides")
    if isinstance(overrides, dict):
        for candidate_key in overrides.keys():
            _append_prompt_platform(platforms, seen, candidate_key)

    if not platforms:
        platforms.extend(list(backend_app.PLATFORM_ACTORS.keys()))
    return platforms


def resolve_runtime_prompt_provider(provider_name: str = "") -> dict[str, Any]:
    preflight = backend_app.build_vision_preflight(provider_name)
    provider_snapshots = {
        backend_app.normalize_vision_provider_name(item.get("name")): dict(item)
        for item in (preflight.get("providers") or [])
        if isinstance(item, dict)
    }
    requested_provider = backend_app.normalize_vision_provider_name(preflight.get("requested_provider") or provider_name)
    preferred_provider = backend_app.normalize_vision_provider_name(preflight.get("preferred_provider"))
    selected_provider = preferred_provider or requested_provider or "openai"
    provider_source = (
        "preferred_provider"
        if preferred_provider
        else ("requested_provider" if requested_provider else "default_openai")
    )
    selected_snapshot = provider_snapshots.get(selected_provider) or {}
    selected_model = str(selected_snapshot.get("model") or "").strip()
    if not selected_model:
        for provider in backend_app.VISION_PROVIDER_CONFIGS:
            if backend_app.normalize_vision_provider_name(provider.get("name")) != selected_provider:
                continue
            selected_model = str(backend_app.resolve_vision_provider_model(provider) or "").strip()
            break
    if not selected_model:
        selected_model = str(getattr(backend_app, "DEFAULT_VISION_MODEL", "gpt-5.4") or "gpt-5.4").strip()
    return {
        "requested_provider": requested_provider,
        "preferred_provider": preferred_provider,
        "selected_provider": selected_provider,
        "provider_source": provider_source,
        "selected_model": selected_model,
        "preflight_status": str(preflight.get("status") or "").strip(),
        "preflight_message": str(preflight.get("message") or "").strip(),
        "configured_provider_names": list(preflight.get("configured_provider_names") or []),
        "runnable_provider_names": list(preflight.get("runnable_provider_names") or []),
        "provider_snapshot": selected_snapshot,
    }


def build_runtime_prompt_artifacts(
    *,
    active_rulespec: dict[str, Any] | None,
    active_visual_prompts: dict[str, Any] | None,
    provider_name: str = "",
) -> dict[str, Any]:
    resolved_rulespec = active_rulespec if isinstance(active_rulespec, dict) else {}
    resolved_visual_prompts = active_visual_prompts if isinstance(active_visual_prompts, dict) else {}
    provider_resolution = resolve_runtime_prompt_provider(provider_name)
    selected_provider = str(provider_resolution.get("selected_provider") or "").strip()
    selected_model = str(provider_resolution.get("selected_model") or "").strip()
    platforms = resolve_runtime_prompt_platforms(
        active_rulespec=resolved_rulespec,
        active_visual_prompts=resolved_visual_prompts,
    )

    platform_payloads: dict[str, Any] = {}
    for platform in platforms:
        visual_selection = backend_app.resolve_visual_review_prompt_selection(
            selected_provider,
            platform,
            model_name=selected_model,
            active_visual_prompts=resolved_visual_prompts,
            active_rulespec=resolved_rulespec,
        )
        positioning_selection = backend_app.resolve_positioning_card_prompt_selection(
            selected_provider,
            platform,
            model_name=selected_model,
            active_rulespec=resolved_rulespec,
        )
        platform_payloads[platform] = {
            "platform_label": backend_app.UPLOAD_PLATFORM_RESPONSE_LABELS.get(platform, platform),
            "visual_review": {
                "prompt_source": str(visual_selection.get("source") or "").strip(),
                "visual_contract_source": str(visual_selection.get("visual_contract_source") or "").strip(),
                "resolved_cover_limit": int(visual_selection.get("resolved_cover_limit") or 0),
                "prompt": str(visual_selection.get("prompt") or ""),
                "preview_prompt": backend_app.build_visual_review_prompt(
                    selected_provider,
                    platform,
                    "{{username}}",
                    model_name=selected_model,
                ),
            },
            "positioning_card_analysis": {
                "prompt_source": str(positioning_selection.get("source") or "").strip(),
                "visual_contract_source": str(positioning_selection.get("visual_contract_source") or "").strip(),
                "resolved_cover_limit": int(positioning_selection.get("resolved_cover_limit") or 0),
                "prompt": str(positioning_selection.get("prompt") or ""),
                "preview_prompt": backend_app.build_positioning_card_prompt(
                    selected_provider,
                    platform,
                    "{{username}}",
                    model_name=selected_model,
                ),
            },
        }

    return {
        "generated_at": backend_app.iso_now(),
        "provider": provider_resolution,
        "platform_count": len(platform_payloads),
        "platforms": platform_payloads,
    }


def write_runtime_prompt_artifacts(
    *,
    output_dir: Path,
    active_rulespec: dict[str, Any] | None,
    active_visual_prompts: dict[str, Any] | None,
    provider_name: str = "",
) -> tuple[dict[str, Any], Path]:
    resolved_output_dir = output_dir.expanduser().resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    payload = build_runtime_prompt_artifacts(
        active_rulespec=active_rulespec,
        active_visual_prompts=active_visual_prompts,
        provider_name=provider_name,
    )
    output_path = resolved_output_dir / "runtime_prompt_artifacts.json"
    backend_app.write_json_file(str(output_path), payload)
    return payload, output_path


def prepare_upload_metadata(source_path: Path, *, task_name: str = "") -> dict[str, Any]:
    frames = load_upload_frames(source_path)
    if not frames:
        raise ValueError(f"上传名单为空或无法读取: {source_path}")

    dataframe = backend_app.pd.concat(frames, ignore_index=True)
    input_row_count = count_non_empty_upload_rows(frames)
    parsed_source_kind = infer_creator_source_kind(source_path, dataframe)
    normalized_upload_source_path = ""
    normalized_upload_summary: dict[str, Any] = {}

    resolved_columns, missing = backend_app.resolve_canonical_upload_columns(dataframe.columns)
    if missing:
        normalized_dataframe = None
        if _has_mail_thread_funnel_columns(list(dataframe.columns)):
            normalized_dataframe, normalized_upload_summary = build_canonical_upload_from_mail_thread_funnel(
                source_path,
                frames,
                target_brand=task_name,
            )
            if normalized_dataframe is not None:
                parsed_source_kind = "mail_thread_funnel"
        if normalized_dataframe is None:
            normalized_dataframe, normalized_upload_summary = build_canonical_upload_from_sending_list(source_path, frames)
        if normalized_dataframe is not None:
            if parsed_source_kind != "mail_thread_funnel":
                parsed_source_kind = "sending_list"
            normalized_upload_source_path = str(
                persist_normalized_upload_dataframe(source_path, normalized_dataframe)
            )
            dataframe = normalized_dataframe
            input_row_count = int(normalized_upload_summary.get("inputRowCount") or input_row_count)

    with backend_app.app.app_context():
        parsed, error_response = backend_app.parse_canonical_upload_workbook(dataframe, source_path.name)
    if error_response:
        _raise_upload_error(error_response)

    for platform in backend_app.PLATFORM_ACTORS:
        backend_app.save_upload_metadata(platform, parsed["metadata_by_platform"].get(platform, {}), replace=True)

    return {
        "source_path": str(source_path),
        "creator_workbook": str(source_path),
        "parsed_source_kind": parsed_source_kind,
        "input_row_count": input_row_count,
        "normalized_upload_source_path": normalized_upload_source_path,
        "normalized_upload_summary": normalized_upload_summary,
        "stats": dict(parsed["stats"]),
        "grouped_count_by_platform": {
            platform: len(parsed["grouped_data"].get(platform, []))
            for platform in backend_app.PLATFORM_ACTORS
        },
        "metadata_count_by_platform": {
            platform: len(parsed["metadata_by_platform"].get(platform, {}))
            for platform in backend_app.PLATFORM_ACTORS
        },
        "preview": list(parsed["preview"]),
        "upload_metadata_paths": {
            platform: backend_app.get_upload_metadata_path(platform)
            for platform in backend_app.PLATFORM_ACTORS
        },
    }


def prepare_screening_inputs(
    *,
    creator_workbook: Path | None = None,
    template_workbook: Path | None = None,
    rulespec_json: Path | None = None,
    task_name: str = "",
    task_upload_url: str = "",
    env_file: str | Path = ".env",
    feishu_app_id: str = "",
    feishu_app_secret: str = "",
    feishu_base_url: str = "",
    task_download_dir: Path | None = None,
    timeout_seconds: float = 30.0,
    template_output_dir: Path | None = None,
    screening_data_dir: Path | None = None,
    config_dir: Path | None = None,
    temp_dir: Path | None = None,
    summary_json: Path | None = None,
) -> dict[str, Any]:
    if template_workbook and rulespec_json:
        raise ValueError("`template_workbook` 和 `rulespec_json` 只能二选一。")
    if not creator_workbook and not template_workbook and not rulespec_json and not str(task_name or "").strip():
        raise ValueError("至少提供任务名，或 `creator_workbook`、`template_workbook`、`rulespec_json` 其中之一。")

    configure_backend_runtime(
        screening_data_dir=screening_data_dir,
        config_dir=config_dir,
        temp_dir=temp_dir,
    )
    backend_app.ensure_runtime_dirs()
    resolved_config_sources, resolved_config = _build_resolved_config_sources(
        env_file=env_file,
        task_name=task_name,
        task_upload_url=task_upload_url,
        feishu_app_id=feishu_app_id,
        feishu_app_secret=feishu_app_secret,
        feishu_base_url=feishu_base_url,
        timeout_seconds=timeout_seconds,
        task_download_dir=task_download_dir,
        template_output_dir=template_output_dir,
        screening_data_dir=screening_data_dir,
        config_dir=config_dir,
        temp_dir=temp_dir,
    )

    summary: dict[str, Any] = {
        "prepared_at": backend_app.iso_now(),
        "env_file_raw": str(env_file),
        "env_file": str(resolved_config["env_snapshot"].path),
        "resolved_config_sources": resolved_config_sources,
        "screening_data_dir": backend_app.DATA_DIR,
        "config_dir": backend_app.CONFIG_DIR,
        "temp_dir": backend_app.TEMP_DIR,
        "active_rulespec_path": backend_app.ACTIVE_RULESPEC_PATH,
        "active_visual_prompts_path": backend_app.ACTIVE_VISUAL_PROMPTS_PATH,
        "resolved_inputs": {},
        "preflight": {},
        "rulespec": {},
        "prompts": {},
        "upload": {},
        "taskSource": {},
    }

    resolved_creator_workbook = creator_workbook
    resolved_template_workbook = template_workbook
    normalized_task_name = str(task_name or "").strip()
    summary["resolved_inputs"] = {
        "env_file": {
            "path": str(resolved_config["env_snapshot"].path),
            "exists": resolved_config["env_snapshot"].exists,
            "source": resolved_config["env_snapshot"].source,
        },
        "runtime_dirs": {
            "screening_data_dir": _path_summary(Path(backend_app.DATA_DIR), source="runtime_config", kind="dir"),
            "config_dir": _path_summary(Path(backend_app.CONFIG_DIR), source="runtime_config", kind="dir"),
            "temp_dir": _path_summary(Path(backend_app.TEMP_DIR), source="runtime_config", kind="dir"),
        },
        "task_download_dir": _path_summary(
            task_download_dir.expanduser() if task_download_dir is not None else DEFAULT_TASK_UPLOAD_DOWNLOAD_DIR,
            source="cli" if task_download_dir is not None else "default",
            kind="dir",
        ),
        "template_output_dir": _path_summary(
            template_output_dir.expanduser() if template_output_dir is not None else DEFAULT_TEMPLATE_OUTPUT_DIR,
            source="cli" if template_output_dir is not None else "default",
            kind="dir",
        ),
        "creator_input": _path_summary(
            resolved_creator_workbook.expanduser() if resolved_creator_workbook else None,
            source="cli" if resolved_creator_workbook is not None else "pending",
            kind="file",
        ),
        "template_input": _path_summary(
            resolved_template_workbook.expanduser() if resolved_template_workbook else None,
            source="cli" if resolved_template_workbook is not None else "pending",
            kind="file",
        ),
        "rulespec_input": _path_summary(
            rulespec_json.expanduser() if rulespec_json is not None else None,
            source="cli" if rulespec_json is not None else "pending",
            kind="file",
        ),
        "task_name": normalized_task_name,
    }
    should_resolve_task_upload = bool(
        normalized_task_name
        and (
            resolved_creator_workbook is None
            or (resolved_template_workbook is None and rulespec_json is None)
        )
    )
    if should_resolve_task_upload:
        task_source = resolve_task_upload_source_files(
            task_name=normalized_task_name,
            task_upload_url=task_upload_url,
            env_file=env_file,
            feishu_app_id=feishu_app_id,
            feishu_app_secret=feishu_app_secret,
            feishu_base_url=feishu_base_url,
            task_download_dir=task_download_dir,
            timeout_seconds=timeout_seconds,
            download_template=resolved_template_workbook is None and rulespec_json is None,
            download_sending_list=resolved_creator_workbook is None,
        )
        summary["taskSource"] = dict(task_source)
        if resolved_creator_workbook is None:
            resolved_creator_workbook = Path(task_source["sendingListDownloadedPath"]).expanduser()
        if resolved_template_workbook is None and rulespec_json is None:
            resolved_template_workbook = Path(task_source["templateDownloadedPath"]).expanduser()
        summary["resolved_inputs"]["task_upload"] = {
            "task_upload_url": task_source.get("taskUploadUrl", ""),
            "download_dir": task_source.get("downloadDir", ""),
            "resolved_config": dict(task_source.get("resolvedConfig") or {}),
        }

    if resolved_template_workbook is not None:
        output_root = template_output_dir or DEFAULT_TEMPLATE_OUTPUT_DIR
        report = compile_workbook(resolved_template_workbook, output_root)
        rulespec_path = Path(report["artifacts"]["rulespec_json"])
        visual_prompts_path = Path(report["artifacts"]["visual_prompts_json"]) if report.get("artifacts", {}).get("visual_prompts_json") else None
        payload = persist_active_rulespec(rulespec_path)
        active_visual_prompts_payload: dict[str, Any] = {}
        if visual_prompts_path is not None:
            active_visual_prompts_payload = persist_active_visual_prompts(visual_prompts_path)
        else:
            clear_active_visual_prompts()
        prompt_artifacts_payload, prompt_artifacts_path = write_runtime_prompt_artifacts(
            output_dir=Path(report["output_dir"]),
            active_rulespec=payload,
            active_visual_prompts=active_visual_prompts_payload,
        )
        summary["rulespec"] = {
            "source": "task_upload_template" if should_resolve_task_upload and template_workbook is None else "template_workbook",
            "template_workbook": str(resolved_template_workbook),
            "compile_output_dir": report["output_dir"],
            "compile_report_path": str(Path(report["output_dir"]) / "compile_report.json"),
            "rulespec_json_path": str(rulespec_path),
            "visual_prompts_json_path": str(visual_prompts_path) if visual_prompts_path is not None else "",
            "runtime_prompt_artifacts_json_path": str(prompt_artifacts_path),
            "warning_count": len(report.get("warnings") or []),
            "rule_count": len(payload.get("rules") or []),
            "runtime_prompt_platform_count": int(prompt_artifacts_payload.get("platform_count") or 0),
        }
        summary["prompts"] = {
            "runtime_prompt_artifacts_json_path": str(prompt_artifacts_path),
            "selected_provider": str((prompt_artifacts_payload.get("provider") or {}).get("selected_provider") or ""),
            "selected_model": str((prompt_artifacts_payload.get("provider") or {}).get("selected_model") or ""),
            "platform_count": int(prompt_artifacts_payload.get("platform_count") or 0),
        }
    elif rulespec_json is not None:
        payload = persist_active_rulespec(rulespec_json)
        clear_active_visual_prompts()
        prompt_output_dir = Path(backend_app.TEMP_DIR) / "runtime_prompt_artifacts" / rulespec_json.stem
        prompt_artifacts_payload, prompt_artifacts_path = write_runtime_prompt_artifacts(
            output_dir=prompt_output_dir,
            active_rulespec=payload,
            active_visual_prompts={},
        )
        summary["rulespec"] = {
            "source": "rulespec_json",
            "rulespec_json_path": str(rulespec_json),
            "visual_prompts_json_path": "",
            "rule_count": len(payload.get("rules") or []),
            "runtime_prompt_artifacts_json_path": str(prompt_artifacts_path),
            "runtime_prompt_platform_count": int(prompt_artifacts_payload.get("platform_count") or 0),
        }
        summary["prompts"] = {
            "runtime_prompt_artifacts_json_path": str(prompt_artifacts_path),
            "selected_provider": str((prompt_artifacts_payload.get("provider") or {}).get("selected_provider") or ""),
            "selected_model": str((prompt_artifacts_payload.get("provider") or {}).get("selected_model") or ""),
            "platform_count": int(prompt_artifacts_payload.get("platform_count") or 0),
        }

    if resolved_creator_workbook is not None:
        summary["creator_workbook"] = str(resolved_creator_workbook)
        summary["upload"] = prepare_upload_metadata(resolved_creator_workbook, task_name=normalized_task_name)
        summary["parsed_source_kind"] = summary["upload"].get("parsed_source_kind", "")
        summary["input_row_count"] = int(summary["upload"].get("input_row_count") or 0)

    summary["resolved_inputs"]["creator_input"] = _path_summary(
        resolved_creator_workbook.expanduser() if resolved_creator_workbook is not None else None,
        source=(
            "task_upload_sending_list"
            if should_resolve_task_upload and creator_workbook is None and resolved_creator_workbook is not None
            else ("cli" if resolved_creator_workbook is not None else "none")
        ),
        kind="file",
    )
    summary["resolved_inputs"]["template_input"] = _path_summary(
        resolved_template_workbook.expanduser() if resolved_template_workbook is not None else None,
        source=(
            "task_upload_template"
            if should_resolve_task_upload and template_workbook is None and rulespec_json is None and resolved_template_workbook is not None
            else ("cli" if resolved_template_workbook is not None else "none")
        ),
        kind="file",
    )
    summary["resolved_inputs"]["rulespec_input"] = _path_summary(
        rulespec_json.expanduser() if rulespec_json is not None else None,
        source="cli" if rulespec_json is not None else "none",
        kind="file",
    )
    summary["preflight"] = {
        "runtime_dirs_ready": True,
        "creator_input_mode": (
            "task_upload_sending_list"
            if should_resolve_task_upload and creator_workbook is None and resolved_creator_workbook is not None
            else ("creator_workbook" if resolved_creator_workbook is not None else "none")
        ),
        "template_input_mode": (
            "task_upload_template"
            if should_resolve_task_upload and template_workbook is None and rulespec_json is None and resolved_template_workbook is not None
            else ("template_workbook" if resolved_template_workbook is not None else ("rulespec_json" if rulespec_json is not None else "none"))
        ),
        "creator_input_exists": summary["resolved_inputs"]["creator_input"]["exists"],
        "template_input_exists": summary["resolved_inputs"]["template_input"]["exists"],
        "rulespec_input_exists": summary["resolved_inputs"]["rulespec_input"]["exists"],
        "active_rulespec_path": backend_app.ACTIVE_RULESPEC_PATH,
        "active_visual_prompts_path": backend_app.ACTIVE_VISUAL_PROMPTS_PATH,
    }

    if summary_json is not None:
        backend_app.write_json_file(str(summary_json), summary)

    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prepare screening backend inputs from parsed templates and creator workbooks.")
    parser.add_argument("--env-file", default=".env", help="本地 env 文件路径，默认当前目录 ./.env。")
    parser.add_argument("--creator-workbook", help="达人匹配结果 xlsx/csv，至少要包含 Platform 和 @username。")
    parser.add_argument("--template-workbook", help="需求模板 xlsx，会先编译再把 rulespec 写入 config/active_rulespec.json。")
    parser.add_argument("--rulespec-json", help="已存在的 rulespec.json，直接写入 config/active_rulespec.json。")
    parser.add_argument("--task-name", help="任务名。提供后会优先从任务上传下载发信名单和模板。")
    parser.add_argument("--task-upload-url", help="飞书任务上传 wiki/base 链接；不传时可从 .env 里的 TASK_UPLOAD_URL 读取。")
    parser.add_argument("--feishu-app-id", default="", help="飞书自建应用 app_id。")
    parser.add_argument("--feishu-app-secret", default="", help="飞书自建应用 app_secret。")
    parser.add_argument("--feishu-base-url", default="", help="飞书 OpenAPI Base URL。")
    parser.add_argument("--task-download-dir", help="任务上传附件下载目录，默认 ./downloads/task_upload_attachments。")
    parser.add_argument("--timeout-seconds", type=float, default=0.0, help="飞书请求超时时间，默认读取 .env 或 30 秒。")
    parser.add_argument("--template-output-dir", help="模板编译输出目录。默认写到 downloads/task_upload_attachments/parsed_outputs。")
    parser.add_argument("--screening-data-dir", help="覆盖筛号 DATA_DIR，默认沿用 .env.local / backend.app 配置。")
    parser.add_argument("--config-dir", help="覆盖筛号 config 目录，默认沿用仓库 config/。")
    parser.add_argument("--temp-dir", help="覆盖筛号 temp 目录，默认沿用仓库 temp/。")
    parser.add_argument("--summary-json", help="把准备结果落盘成 summary.json。")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    summary = prepare_screening_inputs(
        creator_workbook=Path(args.creator_workbook).expanduser() if args.creator_workbook else None,
        template_workbook=Path(args.template_workbook).expanduser() if args.template_workbook else None,
        rulespec_json=Path(args.rulespec_json).expanduser() if args.rulespec_json else None,
        task_name=args.task_name or "",
        task_upload_url=args.task_upload_url or "",
        env_file=args.env_file,
        feishu_app_id=args.feishu_app_id or "",
        feishu_app_secret=args.feishu_app_secret or "",
        feishu_base_url=args.feishu_base_url or "",
        task_download_dir=Path(args.task_download_dir).expanduser() if args.task_download_dir else None,
        timeout_seconds=float(args.timeout_seconds or 0.0),
        template_output_dir=Path(args.template_output_dir).expanduser() if args.template_output_dir else None,
        screening_data_dir=Path(args.screening_data_dir).expanduser() if args.screening_data_dir else None,
        config_dir=Path(args.config_dir).expanduser() if args.config_dir else None,
        temp_dir=Path(args.temp_dir).expanduser() if args.temp_dir else None,
        summary_json=Path(args.summary_json).expanduser() if args.summary_json else None,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
