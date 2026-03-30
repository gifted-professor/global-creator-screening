from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from email_sync.date_windows import resolve_sync_sent_since

from .attachment_download import download_bitable_attachments
from .bitable_export import export_bitable_view
from .bridge import (
    DEFAULT_MANUAL_UPDATE_ENDPOINT_URL,
    DEFAULT_UPLOAD_ENDPOINT_URL,
    import_screening_workbook_from_feishu,
)
from .email_project import inspect_email_project_dependency
from .feishu_api import DEFAULT_FEISHU_BASE_URL, FeishuOpenClient
from .local_env import get_preferred_value, load_local_env
from .task_upload_sync import inspect_task_upload_assignments, sync_task_upload_mailboxes, sync_task_upload_view_to_email_project


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="从飞书下载筛号 workbook，并复用 email_sync 导入写回。")
    subparsers = parser.add_subparsers(dest="command", required=True)

    import_parser = subparsers.add_parser(
        "import-from-feishu",
        help="下载飞书文件后写入本地 workbook，并调用 email_sync 现有链路导入项目",
    )
    import_parser.add_argument("--env-file", default=".env", help="本地 env 文件路径，默认当前目录 ./.env")
    source_group = import_parser.add_mutually_exclusive_group(required=False)
    source_group.add_argument("--file-token", help="飞书 drive 文件 file_token，例如 boxcnxxx")
    source_group.add_argument("--file-url", help="飞书文件链接，会自动尝试提取 file_token")
    import_parser.add_argument(
        "--email-project-root",
        default="",
        help="兼容模式下 legacy email 项目根目录；不填则优先提示当前仓库 repo-local 主线",
    )
    import_parser.add_argument("--email-env-file", default="", help="兼容模式下 legacy email 项目的配置文件路径，默认相对 legacy 根目录的 .env")
    import_parser.add_argument("--feishu-app-id", default="", help="飞书自建应用 app_id")
    import_parser.add_argument("--feishu-app-secret", default="", help="飞书自建应用 app_secret")
    import_parser.add_argument("--feishu-base-url", default="", help="飞书 OpenAPI Base URL")
    import_parser.add_argument("--project-code", help="目标 project_code")
    import_parser.add_argument("--primary-category", help="canonical 主类目")
    import_parser.add_argument("--owner-name", default="", help="项目 owner_name")
    import_parser.add_argument("--download-dir", default="", help="下载到本地的目录，默认 ./downloads")
    import_parser.add_argument("--download-name", help="覆盖飞书返回的文件名")
    import_parser.add_argument("--overwrite-download", action="store_true", help="如果本地已存在同名文件则覆盖")
    import_parser.add_argument("--dashboard-output", help="导出 dashboard HTML 的目标路径；兼容模式下默认 legacy 项目 exports/index.html")
    import_parser.add_argument(
        "--manual-update-endpoint-url",
        default="",
        help="写回 dashboard 时嵌入的 manual update endpoint",
    )
    import_parser.add_argument(
        "--upload-endpoint-url",
        default="",
        help="写回 dashboard 时嵌入的 upload endpoint",
    )
    import_parser.add_argument("--timeout-seconds", type=float, default=0.0, help="飞书请求超时时间，默认 30 秒")
    import_parser.add_argument("--json", action="store_true", help="输出完整 JSON 结果")

    export_parser = subparsers.add_parser(
        "export-bitable-url",
        help="读取飞书 wiki/base 多维表格 URL，并导出本地 JSON/XLSX 文件",
    )
    export_parser.add_argument("--env-file", default=".env", help="本地 env 文件路径，默认当前目录 ./.env")
    export_parser.add_argument("--url", default="", help="飞书 wiki/base 多维表格 URL")
    export_parser.add_argument("--feishu-app-id", default="", help="飞书自建应用 app_id")
    export_parser.add_argument("--feishu-app-secret", default="", help="飞书自建应用 app_secret")
    export_parser.add_argument("--feishu-base-url", default="", help="飞书 OpenAPI Base URL")
    export_parser.add_argument("--output", default="", help="输出路径，默认 ./exports/feishu_bitable_export.json")
    export_parser.add_argument("--format", default="", help="输出格式：json 或 xlsx，默认 json")
    export_parser.add_argument("--timeout-seconds", type=float, default=0.0, help="飞书请求超时时间，默认 30 秒")
    export_parser.add_argument("--json", action="store_true", help="输出完整 JSON 结果")

    attachment_parser = subparsers.add_parser(
        "download-bitable-attachments",
        help="读取飞书 wiki/base 多维表格 URL，并把真实附件字段下载到本地",
    )
    attachment_parser.add_argument("--env-file", default=".env", help="本地 env 文件路径，默认当前目录 ./.env")
    attachment_parser.add_argument("--url", default="", help="飞书 wiki/base 多维表格 URL")
    attachment_parser.add_argument("--feishu-app-id", default="", help="飞书自建应用 app_id")
    attachment_parser.add_argument("--feishu-app-secret", default="", help="飞书自建应用 app_secret")
    attachment_parser.add_argument("--feishu-base-url", default="", help="飞书 OpenAPI Base URL")
    attachment_parser.add_argument("--output-dir", default="", help="附件输出目录，默认 ./downloads/bitable_attachments")
    attachment_parser.add_argument("--timeout-seconds", type=float, default=0.0, help="飞书请求超时时间，默认 30 秒")
    attachment_parser.add_argument("--json", action="store_true", help="输出完整 JSON 结果")

    sync_parser = subparsers.add_parser(
        "sync-task-upload-view",
        help="把飞书任务上传表里的 Excel 下载到本地，并导入 email_sync 项目库与 dashboard",
    )
    sync_parser.add_argument("--env-file", default=".env", help="本地 env 文件路径，默认当前目录 ./.env")
    sync_parser.add_argument("--url", default="", help="飞书任务上传 wiki/base 链接")
    sync_parser.add_argument("--email-project-root", default="", help="兼容模式下 legacy email 项目根目录；不填则优先提示当前仓库 repo-local 主线")
    sync_parser.add_argument("--email-env-file", default="", help="兼容模式下 legacy email 项目的配置文件路径")
    sync_parser.add_argument("--feishu-app-id", default="", help="飞书自建应用 app_id")
    sync_parser.add_argument("--feishu-app-secret", default="", help="飞书自建应用 app_secret")
    sync_parser.add_argument("--feishu-base-url", default="", help="飞书 OpenAPI Base URL")
    sync_parser.add_argument("--download-dir", default="", help="下载到本地的目录，默认 ./downloads/task_upload_attachments")
    sync_parser.add_argument("--dashboard-output", default="", help="导出 dashboard HTML 的目标路径")
    sync_parser.add_argument("--project-code-prefix", default="", help="项目编码前缀，默认 P-FSH-")
    sync_parser.add_argument("--default-primary-category", default="", help="默认 primary_category，默认 lifestyle")
    sync_parser.add_argument("--category-overrides", default="", help="任务名类目覆盖，格式 Tapo:smart_home,duet:lifestyle")
    sync_parser.add_argument("--timeout-seconds", type=float, default=0.0, help="飞书请求超时时间，默认 30 秒")
    sync_parser.add_argument("--json", action="store_true", help="输出完整 JSON 结果")

    inspect_parser = subparsers.add_parser(
        "inspect-task-upload",
        help="只读检查任务上传与员工信息表映射，并可选下载需求模板",
    )
    inspect_parser.add_argument("--env-file", default=".env", help="本地 env 文件路径，默认当前目录 ./.env")
    inspect_parser.add_argument("--task-url", default="", help="飞书任务上传 wiki/base 链接")
    inspect_parser.add_argument("--employee-url", default="", help="飞书员工信息表 wiki/base 链接")
    inspect_parser.add_argument("--feishu-app-id", default="", help="飞书自建应用 app_id")
    inspect_parser.add_argument("--feishu-app-secret", default="", help="飞书自建应用 app_secret")
    inspect_parser.add_argument("--feishu-base-url", default="", help="飞书 OpenAPI Base URL")
    inspect_parser.add_argument("--download-dir", default="", help="模板下载目录，默认 ./downloads/task_upload_attachments")
    inspect_parser.add_argument("--download-templates", action="store_true", help="下载任务上传里的需求模板")
    inspect_parser.add_argument("--parse-templates", action="store_true", help="下载后立即解析模板，自动隐含 --download-templates")
    inspect_parser.add_argument("--parse-output-dir", default="", help="解析产物输出目录，默认 <download-dir>/parsed_outputs")
    inspect_parser.add_argument(
        "--owner-email-overrides",
        default="",
        help="负责人邮箱覆盖，格式 MINISO:eden@amagency.biz，可逗号分隔多个任务",
    )
    inspect_parser.add_argument("--timeout-seconds", type=float, default=0.0, help="飞书请求超时时间，默认 30 秒")
    inspect_parser.add_argument("--json", action="store_true", help="输出完整 JSON 结果")

    mail_sync_parser = subparsers.add_parser(
        "sync-task-upload-mail",
        help="按任务上传 -> 员工信息映射结果抓取对应邮箱文件夹里的邮件",
    )
    mail_sync_parser.add_argument("--env-file", default=".env", help="本地 env 文件路径，默认当前目录 ./.env")
    mail_sync_parser.add_argument("--task-url", default="", help="飞书任务上传 wiki/base 链接")
    mail_sync_parser.add_argument("--employee-url", default="", help="飞书员工信息表 wiki/base 链接")
    mail_sync_parser.add_argument("--task-name", action="append", help="只同步指定任务名，可重复传入")
    mail_sync_parser.add_argument("--download-dir", default="", help="任务模板下载目录，默认 ./downloads/task_upload_attachments")
    mail_sync_parser.add_argument("--mail-data-dir", default="", help="任务邮件数据目录，默认 ./data/task_upload_mail_sync")
    mail_sync_parser.add_argument("--folder-prefixes", default="", help="任务邮箱目录前缀，逗号分隔，默认 其他文件夹")
    mail_sync_parser.add_argument(
        "--owner-email-overrides",
        default="",
        help="负责人邮箱覆盖，格式 MINISO:eden@amagency.biz，可逗号分隔多个任务",
    )
    mail_sync_parser.add_argument("--folder-overrides", default="", help="任务邮箱目录覆盖，格式 MINISO:其他文件夹/MINISO")
    mail_sync_parser.add_argument("--limit", type=int, default=0, help="只抓最新 N 封用于测试，不推进增量游标")
    mail_sync_parser.add_argument("--workers", type=int, default=1, help="并发抓取 worker 数，默认 1")
    mail_sync_parser.add_argument("--reset-state", action="store_true", help="忽略本地游标，重新全量扫描")
    mail_sync_parser.add_argument("--sent-since", default="", help="只抓这个日期及之后的邮件，格式 YYYY-MM-DD；默认最近 3 个月")
    mail_sync_parser.add_argument("--imap-host", default="", help="IMAP Host，默认 imap.qq.com")
    mail_sync_parser.add_argument("--imap-port", type=int, default=0, help="IMAP Port，默认 993")
    mail_sync_parser.add_argument("--feishu-app-id", default="", help="飞书自建应用 app_id")
    mail_sync_parser.add_argument("--feishu-app-secret", default="", help="飞书自建应用 app_secret")
    mail_sync_parser.add_argument("--feishu-base-url", default="", help="飞书 OpenAPI Base URL")
    mail_sync_parser.add_argument("--timeout-seconds", type=float, default=0.0, help="飞书请求超时时间，默认 30 秒")
    mail_sync_parser.add_argument("--json", action="store_true", help="输出完整 JSON 结果")
    return parser


def _cmd_import_from_feishu(args: argparse.Namespace) -> int:
    env_values = load_local_env(args.env_file)
    resolved_legacy_root = get_preferred_value(args.email_project_root, env_values, "EMAIL_PROJECT_ROOT")
    resolved_legacy_env_file = get_preferred_value(args.email_env_file, env_values, "EMAIL_ENV_FILE", ".env")
    if resolved_legacy_root:
        legacy_dependency = _require_legacy_email_project_dependency(
            command_name="import-from-feishu",
            env_values=env_values,
            email_project_root=args.email_project_root,
            email_env_file=args.email_env_file,
            json_output=bool(args.json),
        )
        if legacy_dependency is None:
            return 2
    else:
        legacy_dependency = {
            "email_project_root": "",
            "email_env_file": resolved_legacy_env_file,
            "diagnostic": inspect_email_project_dependency(
                "",
                resolved_legacy_env_file,
                validate_import=False,
            ),
        }
    app_id = get_preferred_value(args.feishu_app_id, env_values, "FEISHU_APP_ID")
    app_secret = get_preferred_value(args.feishu_app_secret, env_values, "FEISHU_APP_SECRET")
    if not app_id:
        raise ValueError("缺少 FEISHU_APP_ID，请在本地 .env 或 --feishu-app-id 里填写。")
    if not app_secret:
        raise ValueError("缺少 FEISHU_APP_SECRET，请在本地 .env 或 --feishu-app-secret 里填写。")

    file_token = get_preferred_value(args.file_token, env_values, "FEISHU_FILE_TOKEN")
    file_url = get_preferred_value(args.file_url, env_values, "FEISHU_FILE_URL")
    if not file_token and not file_url:
        raise ValueError("缺少 FEISHU_FILE_TOKEN 或 FEISHU_FILE_URL，请在本地 .env 或命令行里填写。")

    project_code = get_preferred_value(args.project_code, env_values, "PROJECT_CODE")
    if not project_code:
        raise ValueError("缺少 PROJECT_CODE，请在本地 .env 或 --project-code 里填写。")

    primary_category = get_preferred_value(args.primary_category, env_values, "PRIMARY_CATEGORY")
    if not primary_category:
        raise ValueError("缺少 PRIMARY_CATEGORY，请在本地 .env 或 --primary-category 里填写。")

    timeout_raw = get_preferred_value(args.timeout_seconds if args.timeout_seconds > 0 else "", env_values, "TIMEOUT_SECONDS", "30")
    timeout_seconds = float(timeout_raw)

    dashboard_output = get_preferred_value(args.dashboard_output, env_values, "DASHBOARD_OUTPUT") or None
    download_name = get_preferred_value(args.download_name, env_values, "DOWNLOAD_NAME") or None

    result = import_screening_workbook_from_feishu(
        email_project_root=legacy_dependency["email_project_root"],
        email_env_file=legacy_dependency["email_env_file"],
        feishu_app_id=app_id,
        feishu_app_secret=app_secret,
        file_token_or_url=file_token or file_url,
        project_code=project_code,
        primary_category=primary_category,
        owner_name=get_preferred_value(args.owner_name, env_values, "OWNER_NAME"),
        dashboard_output=dashboard_output,
        download_dir=get_preferred_value(args.download_dir, env_values, "DOWNLOAD_DIR", "./downloads"),
        download_name=download_name,
        overwrite_download=bool(args.overwrite_download),
        manual_update_endpoint_url=get_preferred_value(
            args.manual_update_endpoint_url,
            env_values,
            "MANUAL_UPDATE_ENDPOINT_URL",
            DEFAULT_MANUAL_UPDATE_ENDPOINT_URL,
        ),
        upload_endpoint_url=get_preferred_value(
            args.upload_endpoint_url,
            env_values,
            "UPLOAD_ENDPOINT_URL",
            DEFAULT_UPLOAD_ENDPOINT_URL,
        ),
        feishu_base_url=get_preferred_value(args.feishu_base_url, env_values, "FEISHU_OPEN_BASE_URL", DEFAULT_FEISHU_BASE_URL),
        timeout_seconds=timeout_seconds,
    )
    result["legacyDependency"] = legacy_dependency["diagnostic"]

    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0

    import_result = result["importResult"]
    print(
        "feishu workbook imported: "
        f"project={import_result['projectCode']}/{import_result['projectName']}  "
        f"saved={result['savedWorkbookPath']}  "
        f"rows={import_result['compiledRowCount']}  "
        f"summary={result.get('summaryJson') or import_result.get('summaryJson') or '-'}  "
        f"dashboard={result['dashboardOutput']}"
    )
    return 0


def _cmd_export_bitable_url(args: argparse.Namespace) -> int:
    env_values = load_local_env(args.env_file)
    app_id = get_preferred_value(args.feishu_app_id, env_values, "FEISHU_APP_ID")
    app_secret = get_preferred_value(args.feishu_app_secret, env_values, "FEISHU_APP_SECRET")
    if not app_id:
        raise ValueError("缺少 FEISHU_APP_ID，请在本地 .env 或 --feishu-app-id 里填写。")
    if not app_secret:
        raise ValueError("缺少 FEISHU_APP_SECRET，请在本地 .env 或 --feishu-app-secret 里填写。")

    url = get_preferred_value(args.url, env_values, "FEISHU_SOURCE_URL")
    if not url:
        raise ValueError("缺少 FEISHU_SOURCE_URL，请在本地 .env 或 --url 里填写。")

    timeout_raw = get_preferred_value(args.timeout_seconds if args.timeout_seconds > 0 else "", env_values, "TIMEOUT_SECONDS", "30")
    timeout_seconds = float(timeout_raw)
    output_format = get_preferred_value(args.format, env_values, "EXPORT_FORMAT", "json").lower()
    output_path = get_preferred_value(
        args.output,
        env_values,
        "EXPORT_OUTPUT",
        f"./exports/feishu_bitable_export.{ 'xlsx' if output_format == 'xlsx' else 'json' }",
    )

    client = FeishuOpenClient(
        app_id=app_id,
        app_secret=app_secret,
        base_url=get_preferred_value(args.feishu_base_url, env_values, "FEISHU_OPEN_BASE_URL", DEFAULT_FEISHU_BASE_URL),
        timeout_seconds=timeout_seconds,
    )
    result = export_bitable_view(
        client,
        url=url,
        output_path=output_path,
        output_format=output_format,
    )
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0

    print(
        "feishu bitable exported: "
        f"title={result['title'] or '-'}  "
        f"table={result['tableName'] or result['tableId']}  "
        f"view={result['viewName'] or result['viewId']}  "
        f"records={result['recordCount']}  "
        f"output={result['outputPath']}"
    )
    return 0


def _cmd_download_bitable_attachments(args: argparse.Namespace) -> int:
    env_values = load_local_env(args.env_file)
    app_id = get_preferred_value(args.feishu_app_id, env_values, "FEISHU_APP_ID")
    app_secret = get_preferred_value(args.feishu_app_secret, env_values, "FEISHU_APP_SECRET")
    if not app_id:
        raise ValueError("缺少 FEISHU_APP_ID，请在本地 .env 或 --feishu-app-id 里填写。")
    if not app_secret:
        raise ValueError("缺少 FEISHU_APP_SECRET，请在本地 .env 或 --feishu-app-secret 里填写。")

    url = get_preferred_value(args.url, env_values, "FEISHU_SOURCE_URL")
    if not url:
        raise ValueError("缺少 FEISHU_SOURCE_URL，请在本地 .env 或 --url 里填写。")

    timeout_raw = get_preferred_value(args.timeout_seconds if args.timeout_seconds > 0 else "", env_values, "TIMEOUT_SECONDS", "30")
    timeout_seconds = float(timeout_raw)
    output_dir = get_preferred_value(args.output_dir, env_values, "ATTACHMENT_OUTPUT_DIR", "./downloads/bitable_attachments")

    client = FeishuOpenClient(
        app_id=app_id,
        app_secret=app_secret,
        base_url=get_preferred_value(args.feishu_base_url, env_values, "FEISHU_OPEN_BASE_URL", DEFAULT_FEISHU_BASE_URL),
        timeout_seconds=timeout_seconds,
    )
    result = download_bitable_attachments(
        client,
        url=url,
        output_dir=output_dir,
    )
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0

    print(
        "feishu bitable attachments downloaded: "
        f"title={result['title'] or '-'}  "
        f"table={result['tableName'] or result['tableId']}  "
        f"view={result['viewName'] or result['viewId']}  "
        f"attachments={result['attachmentCount']}  "
        f"saved_dir={result['outputDir']}"
    )
    return 0


def _cmd_sync_task_upload_view(args: argparse.Namespace) -> int:
    env_values = load_local_env(args.env_file)
    resolved_legacy_root = get_preferred_value(args.email_project_root, env_values, "EMAIL_PROJECT_ROOT")
    resolved_legacy_env_file = get_preferred_value(args.email_env_file, env_values, "EMAIL_ENV_FILE", ".env")
    if resolved_legacy_root:
        legacy_dependency = _require_legacy_email_project_dependency(
            command_name="sync-task-upload-view",
            env_values=env_values,
            email_project_root=args.email_project_root,
            email_env_file=args.email_env_file,
            json_output=bool(args.json),
        )
        if legacy_dependency is None:
            return 2
    else:
        legacy_dependency = {
            "email_project_root": "",
            "email_env_file": resolved_legacy_env_file,
            "diagnostic": inspect_email_project_dependency(
                "",
                resolved_legacy_env_file,
                validate_import=False,
            ),
        }
    app_id = get_preferred_value(args.feishu_app_id, env_values, "FEISHU_APP_ID")
    app_secret = get_preferred_value(args.feishu_app_secret, env_values, "FEISHU_APP_SECRET")
    if not app_id:
        raise ValueError("缺少 FEISHU_APP_ID，请在本地 .env 或 --feishu-app-id 里填写。")
    if not app_secret:
        raise ValueError("缺少 FEISHU_APP_SECRET，请在本地 .env 或 --feishu-app-secret 里填写。")

    task_upload_url = get_preferred_value(args.url, env_values, "TASK_UPLOAD_URL") or get_preferred_value(args.url, env_values, "FEISHU_SOURCE_URL")
    if not task_upload_url:
        raise ValueError("缺少 TASK_UPLOAD_URL，请在本地 .env 或 --url 里填写。")

    timeout_raw = get_preferred_value(args.timeout_seconds if args.timeout_seconds > 0 else "", env_values, "TIMEOUT_SECONDS", "30")
    timeout_seconds = float(timeout_raw)
    client = FeishuOpenClient(
        app_id=app_id,
        app_secret=app_secret,
        base_url=get_preferred_value(args.feishu_base_url, env_values, "FEISHU_OPEN_BASE_URL", DEFAULT_FEISHU_BASE_URL),
        timeout_seconds=timeout_seconds,
    )
    result = sync_task_upload_view_to_email_project(
        client=client,
        task_upload_url=task_upload_url,
        email_project_root=legacy_dependency["email_project_root"],
        email_env_file=legacy_dependency["email_env_file"],
        download_dir=get_preferred_value(args.download_dir, env_values, "TASK_UPLOAD_DOWNLOAD_DIR", "./downloads/task_upload_attachments"),
        dashboard_output=get_preferred_value(args.dashboard_output, env_values, "DASHBOARD_OUTPUT") or None,
        project_code_prefix=get_preferred_value(args.project_code_prefix, env_values, "PROJECT_CODE_PREFIX", "P-FSH-"),
        default_primary_category=get_preferred_value(
            args.default_primary_category,
            env_values,
            "DEFAULT_PRIMARY_CATEGORY",
            "lifestyle",
        ),
        category_overrides=_parse_category_overrides(get_preferred_value(args.category_overrides, env_values, "TASK_UPLOAD_CATEGORY_OVERRIDES")),
    )
    result["legacyDependency"] = legacy_dependency["diagnostic"]
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0

    print(
        "task upload synced: "
        f"records={result['recordCount']}  "
        f"imported={result['importedCount']}  "
        f"summary={result.get('summaryJson') or '-'}  "
        f"dashboard={result['dashboardOutput']}"
    )
    return 0


def _cmd_inspect_task_upload(args: argparse.Namespace) -> int:
    env_values = load_local_env(args.env_file)
    app_id = get_preferred_value(args.feishu_app_id, env_values, "FEISHU_APP_ID")
    app_secret = get_preferred_value(args.feishu_app_secret, env_values, "FEISHU_APP_SECRET")
    if not app_id:
        raise ValueError("缺少 FEISHU_APP_ID，请在本地 .env 或 --feishu-app-id 里填写。")
    if not app_secret:
        raise ValueError("缺少 FEISHU_APP_SECRET，请在本地 .env 或 --feishu-app-secret 里填写。")

    task_upload_url = get_preferred_value(args.task_url, env_values, "TASK_UPLOAD_URL") or get_preferred_value(args.task_url, env_values, "FEISHU_SOURCE_URL")
    if not task_upload_url:
        raise ValueError("缺少 TASK_UPLOAD_URL，请在本地 .env 或 --task-url 里填写。")

    employee_info_url = get_preferred_value(args.employee_url, env_values, "EMPLOYEE_INFO_URL") or get_preferred_value(args.employee_url, env_values, "FEISHU_SOURCE_URL")
    if not employee_info_url:
        raise ValueError("缺少 EMPLOYEE_INFO_URL，请在本地 .env 或 --employee-url 里填写。")

    timeout_raw = get_preferred_value(args.timeout_seconds if args.timeout_seconds > 0 else "", env_values, "TIMEOUT_SECONDS", "30")
    timeout_seconds = float(timeout_raw)
    client = FeishuOpenClient(
        app_id=app_id,
        app_secret=app_secret,
        base_url=get_preferred_value(args.feishu_base_url, env_values, "FEISHU_OPEN_BASE_URL", DEFAULT_FEISHU_BASE_URL),
        timeout_seconds=timeout_seconds,
    )
    result = inspect_task_upload_assignments(
        client=client,
        task_upload_url=task_upload_url,
        employee_info_url=employee_info_url,
        download_dir=get_preferred_value(args.download_dir, env_values, "TASK_UPLOAD_DOWNLOAD_DIR", "./downloads/task_upload_attachments"),
        download_templates=bool(args.download_templates or args.parse_templates),
        parse_templates=bool(args.parse_templates),
        parse_output_dir=get_preferred_value(args.parse_output_dir, env_values, "TASK_UPLOAD_PARSE_OUTPUT_DIR") or None,
        owner_email_overrides=_parse_mapping_overrides(
            get_preferred_value(args.owner_email_overrides, env_values, "TASK_UPLOAD_OWNER_EMAIL_OVERRIDES")
        ),
    )
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0

    print(
        "task upload inspected: "
        f"records={result['recordCount']}  "
        f"matched={result['matchedCount']}  "
        f"downloaded={result['downloadedCount']}  "
        f"parsed={result['parsedCount']}  "
        f"parse_failed={result['parseFailedCount']}"
    )
    for item in result["items"]:
        parse_display = "-"
        if item["templateParseRequested"]:
            parse_display = "ok" if item["templateParsed"] else "failed"
        print(
            " - "
            f"task={item['taskName']}  "
            f"owner={_mask_email(item['ownerEmail']) or item['responsibleName'] or '-'}  "
            f"employee={item['employeeName'] or '-'}  "
            f"matched_by={item['matchedBy'] or '-'}  "
            f"imap={_mask_secret(item['imapCode'])}  "
            f"sending_list={item['sendingListFileName'] or '-'}  "
            f"template={item['templateFileName']}  "
            f"saved={item['templateDownloadedPath'] or '-'}  "
            f"parsed={parse_display}  "
            f"report={item['templateParseReportPath'] or '-'}"
        )
        if item["templateParseError"]:
            print(f"   parse_error={item['templateParseError']}")
    return 0


def _cmd_sync_task_upload_mail(args: argparse.Namespace) -> int:
    env_values = load_local_env(args.env_file)
    app_id = get_preferred_value(args.feishu_app_id, env_values, "FEISHU_APP_ID")
    app_secret = get_preferred_value(args.feishu_app_secret, env_values, "FEISHU_APP_SECRET")
    if not app_id:
        raise ValueError("缺少 FEISHU_APP_ID，请在本地 .env 或 --feishu-app-id 里填写。")
    if not app_secret:
        raise ValueError("缺少 FEISHU_APP_SECRET，请在本地 .env 或 --feishu-app-secret 里填写。")

    task_upload_url = get_preferred_value(args.task_url, env_values, "TASK_UPLOAD_URL") or get_preferred_value(args.task_url, env_values, "FEISHU_SOURCE_URL")
    if not task_upload_url:
        raise ValueError("缺少 TASK_UPLOAD_URL，请在本地 .env 或 --task-url 里填写。")

    employee_info_url = get_preferred_value(args.employee_url, env_values, "EMPLOYEE_INFO_URL") or get_preferred_value(args.employee_url, env_values, "FEISHU_SOURCE_URL")
    if not employee_info_url:
        raise ValueError("缺少 EMPLOYEE_INFO_URL，请在本地 .env 或 --employee-url 里填写。")

    default_account_email = get_preferred_value("", env_values, "TASK_UPLOAD_MAIL_ACCOUNT") or get_preferred_value(
        "",
        env_values,
        "EMAIL_ACCOUNT",
    )
    default_auth_code = get_preferred_value("", env_values, "TASK_UPLOAD_MAIL_AUTH_CODE") or get_preferred_value(
        "",
        env_values,
        "EMAIL_AUTH_CODE",
    )
    timeout_raw = get_preferred_value(args.timeout_seconds if args.timeout_seconds > 0 else "", env_values, "TIMEOUT_SECONDS", "30")
    timeout_seconds = float(timeout_raw)
    client = FeishuOpenClient(
        app_id=app_id,
        app_secret=app_secret,
        base_url=get_preferred_value(args.feishu_base_url, env_values, "FEISHU_OPEN_BASE_URL", DEFAULT_FEISHU_BASE_URL),
        timeout_seconds=timeout_seconds,
    )
    result = sync_task_upload_mailboxes(
        client=client,
        task_upload_url=task_upload_url,
        employee_info_url=employee_info_url,
        download_dir=get_preferred_value(args.download_dir, env_values, "TASK_UPLOAD_DOWNLOAD_DIR", "./downloads/task_upload_attachments"),
        mail_data_dir=get_preferred_value(args.mail_data_dir, env_values, "TASK_UPLOAD_MAIL_DATA_DIR", "./data/task_upload_mail_sync"),
        task_names=list(args.task_name or []),
        owner_email_overrides=_parse_mapping_overrides(
            get_preferred_value(args.owner_email_overrides, env_values, "TASK_UPLOAD_OWNER_EMAIL_OVERRIDES")
        ),
        folder_overrides=_parse_mapping_overrides(get_preferred_value(args.folder_overrides, env_values, "TASK_UPLOAD_MAIL_FOLDER_OVERRIDES")),
        folder_prefixes=_parse_csv_values(get_preferred_value(args.folder_prefixes, env_values, "TASK_UPLOAD_MAIL_FOLDER_PREFIXES", "其他文件夹")),
        limit=args.limit if args.limit > 0 else None,
        workers=args.workers,
        reset_state=bool(args.reset_state),
        sent_since=resolve_sync_sent_since(
            get_preferred_value(args.sent_since, env_values, "TASK_UPLOAD_MAIL_SENT_SINCE") or None
        ).isoformat(),
        imap_host=get_preferred_value(args.imap_host, env_values, "IMAP_HOST", "imap.qq.com"),
        imap_port=int(get_preferred_value(args.imap_port if args.imap_port > 0 else "", env_values, "IMAP_PORT", "993") or "993"),
        default_account_email=default_account_email,
        default_auth_code=default_auth_code,
    )
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0

    print(
        "task upload mail synced: "
        f"selected={result['selectedCount']}  "
        f"synced={result['syncedCount']}  "
        f"failed={result['failedCount']}  "
        f"data_dir={result['mailDataDir']}"
    )
    for item in result["items"]:
        print(
            " - "
            f"task={item['taskName']}  "
            f"employee={item['employeeName'] or '-'}  "
            f"folder={item['resolvedFolder'] or item['requestedFolder'] or '-'}  "
            f"fetched={item['mailFetchedCount']}  "
            f"db={item['mailDbPath'] or '-'}"
        )
        if item["mailSyncError"]:
            print(f"   mail_error={item['mailSyncError']}")
    return 0


def _parse_category_overrides(raw: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for chunk in str(raw or "").split(","):
        item = chunk.strip()
        if not item or ":" not in item:
            continue
        key, value = item.split(":", 1)
        normalized_key = key.strip().casefold()
        normalized_value = value.strip()
        if normalized_key and normalized_value:
            result[normalized_key] = normalized_value
    return result


def _require_legacy_email_project_dependency(
    *,
    command_name: str,
    env_values: dict[str, str],
    email_project_root: str,
    email_env_file: str,
    json_output: bool,
) -> dict[str, Any] | None:
    resolved_root = get_preferred_value(
        email_project_root,
        env_values,
        "EMAIL_PROJECT_ROOT",
    )
    resolved_env_file = get_preferred_value(
        email_env_file,
        env_values,
        "EMAIL_ENV_FILE",
        ".env",
    )
    diagnostic = inspect_email_project_dependency(
        resolved_root,
        resolved_env_file,
        validate_import=True,
    )
    if diagnostic["available"]:
        return {
            "email_project_root": resolved_root,
            "email_env_file": resolved_env_file,
            "diagnostic": diagnostic,
        }
    failure_payload = {
        "ok": False,
        "command": command_name,
        "error_code": diagnostic.get("error_code") or "EMAIL_PROJECT_DEPENDENCY_UNAVAILABLE",
        "error": diagnostic.get("message") or "legacy bridge 外部 email 项目依赖不可用",
        "remediation": diagnostic.get("remediation") or "",
        "legacyDependency": diagnostic,
    }
    if json_output:
        print(json.dumps(failure_payload, ensure_ascii=False, indent=2))
    else:
        print(
            f"[{failure_payload['error_code']}] {failure_payload['error']}\n"
            f"remediation: {failure_payload['remediation']}",
            file=sys.stderr,
        )
    return None


def _parse_mapping_overrides(raw: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for chunk in str(raw or "").split(","):
        item = chunk.strip()
        if not item or ":" not in item:
            continue
        key, value = item.split(":", 1)
        normalized_key = key.strip().casefold()
        normalized_value = value.strip()
        if normalized_key and normalized_value:
            result[normalized_key] = normalized_value
    return result


def _parse_csv_values(raw: str) -> list[str]:
    return [item.strip() for item in str(raw or "").split(",") if item.strip()]


def _mask_email(value: str) -> str:
    raw = str(value or "").strip()
    if "@" not in raw:
        return raw
    local_part, domain = raw.split("@", 1)
    if len(local_part) <= 2:
        return f"***@{domain}"
    return f"{local_part[:2]}***@{domain}"


def _mask_secret(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "-"
    if len(raw) <= 6:
        return "***"
    return f"{raw[:3]}***{raw[-2:]}"


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    if args.command == "import-from-feishu":
        return _cmd_import_from_feishu(args)
    if args.command == "export-bitable-url":
        return _cmd_export_bitable_url(args)
    if args.command == "download-bitable-attachments":
        return _cmd_download_bitable_attachments(args)
    if args.command == "sync-task-upload-view":
        return _cmd_sync_task_upload_view(args)
    if args.command == "inspect-task-upload":
        return _cmd_inspect_task_upload(args)
    if args.command == "sync-task-upload-mail":
        return _cmd_sync_task_upload_mail(args)
    parser.error(f"unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    sys.exit(main())
