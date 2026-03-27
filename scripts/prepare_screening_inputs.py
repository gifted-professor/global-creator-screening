from __future__ import annotations

import argparse
import json
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
    with backend_app.app.app_context():
        parsed, error_response = backend_app.parse_canonical_upload_workbook(dataframe, source_path.name)
    if error_response:
        _raise_upload_error(error_response)

    for platform in backend_app.PLATFORM_ACTORS:
        backend_app.save_upload_metadata(platform, parsed["metadata_by_platform"].get(platform, {}), replace=True)

    return {
        "source_path": str(source_path),
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
        summary["upload"] = prepare_upload_metadata(resolved_creator_workbook)

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
