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
from feishu_screening_bridge import download_task_upload_screening_assets
from feishu_screening_bridge.feishu_api import DEFAULT_FEISHU_BASE_URL, FeishuOpenClient
from feishu_screening_bridge.local_env import get_preferred_value, load_local_env
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
]
SENDING_LIST_COUNTRY_ALIASES = ("country", "国家", "region", "地区")
SENDING_LIST_CREATOR_ALIASES = ("creator", "nickname", "达人", "红人", "博主")
SENDING_LIST_EMAIL_ALIASES = ("邮箱地址", "邮箱", "email", "emailaddress", "mail")
SENDING_LIST_GENERIC_LINK_ALIASES = ("link", "url", "主页链接", "账号链接", "profilelink", "profileurl")
SENDING_LIST_PLATFORM_LINK_ALIASES = {
    "instagram": ("iglink", "igurl", "instagramlink", "instagramurl", "inslink", "insurl"),
    "tiktok": ("ttlink", "tturl", "tiktoklink", "tiktokurl", "douyinlink"),
    "youtube": ("ytlink", "yturl", "youtubelink", "youtubeurl", "channelurl", "channellink"),
}


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
    backend_app.FIELD_MATCH_REPORT_PATH = str(Path(backend_app.CONFIG_DIR) / "field_match_report.json")
    backend_app.MISSING_CAPABILITIES_PATH = str(Path(backend_app.CONFIG_DIR) / "missing_capabilities.json")
    backend_app.REVIEW_NOTES_PATH = str(Path(backend_app.CONFIG_DIR) / "review_notes.md")
    backend_app.APIFY_TOKEN_POOL_STATE_FILE = str(Path(backend_app.DATA_DIR) / "apify_token_pool_state.json")
    backend_app.APIFY_BALANCE_CACHE_FILE = str(Path(backend_app.DATA_DIR) / "apify_balance_cache.json")
    backend_app.APIFY_RUN_GUARDS_FILE = str(Path(backend_app.DATA_DIR) / "apify_run_guards.json")
    backend_app.app.config["UPLOAD_FOLDER"] = backend_app.UPLOAD_FOLDER


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
    app_id = get_preferred_value(feishu_app_id, env_values, "FEISHU_APP_ID")
    app_secret = get_preferred_value(feishu_app_secret, env_values, "FEISHU_APP_SECRET")
    if not app_id:
        raise ValueError("缺少 FEISHU_APP_ID，请在本地 .env 或参数里填写。")
    if not app_secret:
        raise ValueError("缺少 FEISHU_APP_SECRET，请在本地 .env 或参数里填写。")

    resolved_task_upload_url = (
        get_preferred_value(task_upload_url, env_values, "TASK_UPLOAD_URL")
        or get_preferred_value(task_upload_url, env_values, "FEISHU_SOURCE_URL")
    )
    if not resolved_task_upload_url:
        raise ValueError("缺少 TASK_UPLOAD_URL，请在本地 .env 或参数里填写。")

    resolved_download_dir = Path(
        get_preferred_value(
            str(task_download_dir or ""),
            env_values,
            "TASK_UPLOAD_DOWNLOAD_DIR",
            str(DEFAULT_TASK_UPLOAD_DOWNLOAD_DIR),
        )
    ).expanduser()
    resolved_timeout_seconds = float(
        get_preferred_value(
            timeout_seconds if timeout_seconds > 0 else "",
            env_values,
            "TIMEOUT_SECONDS",
            "30",
        )
    )
    client = FeishuOpenClient(
        app_id=app_id,
        app_secret=app_secret,
        base_url=get_preferred_value(
            feishu_base_url,
            env_values,
            "FEISHU_OPEN_BASE_URL",
            DEFAULT_FEISHU_BASE_URL,
        ),
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


def prepare_upload_metadata(source_path: Path) -> dict[str, Any]:
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
        normalized_dataframe, normalized_upload_summary = build_canonical_upload_from_sending_list(source_path, frames)
        if normalized_dataframe is not None:
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

    summary: dict[str, Any] = {
        "prepared_at": backend_app.iso_now(),
        "screening_data_dir": backend_app.DATA_DIR,
        "config_dir": backend_app.CONFIG_DIR,
        "temp_dir": backend_app.TEMP_DIR,
        "active_rulespec_path": backend_app.ACTIVE_RULESPEC_PATH,
        "rulespec": {},
        "upload": {},
        "taskSource": {},
    }

    resolved_creator_workbook = creator_workbook
    resolved_template_workbook = template_workbook
    normalized_task_name = str(task_name or "").strip()
    if normalized_task_name:
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

    if resolved_template_workbook is not None:
        output_root = template_output_dir or DEFAULT_TEMPLATE_OUTPUT_DIR
        report = compile_workbook(resolved_template_workbook, output_root)
        rulespec_path = Path(report["artifacts"]["rulespec_json"])
        payload = persist_active_rulespec(rulespec_path)
        summary["rulespec"] = {
            "source": "task_upload_template" if normalized_task_name and template_workbook is None else "template_workbook",
            "template_workbook": str(resolved_template_workbook),
            "compile_output_dir": report["output_dir"],
            "compile_report_path": str(Path(report["output_dir"]) / "compile_report.json"),
            "rulespec_json_path": str(rulespec_path),
            "warning_count": len(report.get("warnings") or []),
            "rule_count": len(payload.get("rules") or []),
        }
    elif rulespec_json is not None:
        payload = persist_active_rulespec(rulespec_json)
        summary["rulespec"] = {
            "source": "rulespec_json",
            "rulespec_json_path": str(rulespec_json),
            "rule_count": len(payload.get("rules") or []),
        }

    if resolved_creator_workbook is not None:
        summary["creator_workbook"] = str(resolved_creator_workbook)
        summary["upload"] = prepare_upload_metadata(resolved_creator_workbook)
        summary["parsed_source_kind"] = summary["upload"].get("parsed_source_kind", "")
        summary["input_row_count"] = int(summary["upload"].get("input_row_count") or 0)

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
