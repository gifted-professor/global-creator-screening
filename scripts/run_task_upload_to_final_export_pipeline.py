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


MATCHING_STRATEGIES = ("legacy-enrichment", "brand-keyword-fast-path")
SUCCESSFUL_DOWNSTREAM_STATUSES = {"completed", "completed_with_partial_scrape"}


def _load_runtime_dependencies():
    from scripts.run_keep_list_screening_pipeline import run_keep_list_screening_pipeline
    from scripts.run_task_upload_to_keep_list_pipeline import run_task_upload_to_keep_list_pipeline

    return {
        "run_task_upload_to_keep_list_pipeline": run_task_upload_to_keep_list_pipeline,
        "run_keep_list_screening_pipeline": run_keep_list_screening_pipeline,
    }


def default_output_root() -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return REPO_ROOT / "temp" / f"task_upload_to_final_export_{timestamp}"


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
    probe_vision_provider_only: bool,
    skip_scrape: bool,
    skip_visual: bool,
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
    if probe_vision_provider_only:
        parts.append("--probe-vision-provider-only")
    if skip_scrape:
        parts.append("--skip-scrape")
    if skip_visual:
        parts.append("--skip-visual")
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
    matching_strategy: str = MATCHING_STRATEGIES[0],
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
    probe_vision_provider_only: bool = False,
    skip_scrape: bool = False,
    skip_visual: bool = False,
) -> dict[str, Any]:
    runtime = _load_runtime_dependencies()
    run_upstream = runtime["run_task_upload_to_keep_list_pipeline"]
    run_downstream = runtime["run_keep_list_screening_pipeline"]

    normalized_task_name = str(task_name or "").strip()
    if not normalized_task_name:
        raise ValueError("缺少 task_name。")
    normalized_matching_strategy = str(matching_strategy or "").strip().lower() or MATCHING_STRATEGIES[0]
    if normalized_matching_strategy not in MATCHING_STRATEGIES:
        raise ValueError(f"不支持的 matching_strategy: {matching_strategy}")
    normalized_brand_keyword = str(brand_keyword or "").strip() or normalized_task_name
    requested_platforms = [str(value).strip().lower() for value in (platform_filters or []) if str(value).strip()]

    resolved_output_root = (output_root or default_output_root()).expanduser().resolve()
    resolved_output_root.mkdir(parents=True, exist_ok=True)
    upstream_output_root = (resolved_output_root / "upstream").resolve()
    downstream_output_root = (resolved_output_root / "downstream").resolve()
    upstream_summary_path = upstream_output_root / "summary.json"
    downstream_summary_path = downstream_output_root / "summary.json"
    run_summary_path = summary_json.expanduser().resolve() if summary_json else resolved_output_root / "summary.json"

    summary: dict[str, Any] = {
        "started_at": iso_now(),
        "finished_at": "",
        "status": "running",
        "task_name": normalized_task_name,
        "env_file": str(env_file),
        "output_root": str(resolved_output_root),
        "summary_json": str(run_summary_path),
        "matching_strategy": normalized_matching_strategy,
        "brand_keyword": normalized_brand_keyword,
        "inputs": {
            "task_upload_url": str(task_upload_url or "").strip(),
            "employee_info_url": str(employee_info_url or "").strip(),
            "task_download_dir": str(task_download_dir or "").strip(),
            "mail_data_dir": str(mail_data_dir or "").strip(),
            "owner_email_overrides": dict(owner_email_overrides or {}),
            "mail_limit": int(max(0, int(mail_limit))),
            "mail_workers": int(max(1, int(mail_workers))),
            "sent_since": str(sent_since or "").strip(),
            "reset_state": bool(reset_state),
            "reuse_existing": bool(reuse_existing),
        },
        "resolved_inputs": {
            "env_file": _path_summary(Path(env_file), source="cli_or_default", kind="file"),
        },
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
            },
        },
        "resolved_paths": {
            "output_root": str(resolved_output_root),
            "upstream_output_root": str(upstream_output_root),
            "upstream_summary_json": str(upstream_summary_path),
            "downstream_output_root": str(downstream_output_root),
            "downstream_summary_json": str(downstream_summary_path),
        },
        "contract": {
            "scope": "task-upload-to-final-export",
            "upstream_runner": "scripts/run_task_upload_to_keep_list_pipeline.py",
            "downstream_runner": "scripts/run_keep_list_screening_pipeline.py",
            "canonical_internal_boundary": "keep-list",
            "canonical_resume_point": "keep_list",
        },
        "steps": {},
        "artifacts": {
            "upstream_summary_json": str(upstream_summary_path),
            "downstream_summary_json": str(downstream_summary_path),
            "keep_workbook": "",
            "template_workbook": "",
            "final_exports": {},
        },
        "resume_points": {},
    }
    _write_summary(run_summary_path, summary)

    def finalize(status: str, **extra: Any) -> dict[str, Any]:
        summary["status"] = status
        summary["finished_at"] = iso_now()
        summary.update(extra)
        _write_summary(run_summary_path, summary)
        return summary

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
        return finalize("failed", error=failure["message"], error_code=failure["error_code"], failure=failure)

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
        "downstream_handoff": upstream_summary.get("downstream_handoff") or {},
    }
    summary["artifacts"]["keep_workbook"] = keep_workbook
    summary["artifacts"]["template_workbook"] = template_workbook
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
            probe_vision_provider_only=bool(probe_vision_provider_only),
            skip_scrape=bool(skip_scrape),
            skip_visual=bool(skip_visual),
        ),
    }
    _write_summary(run_summary_path, summary)

    if str(upstream_summary.get("status") or "") == "failed":
        failure = _build_failure_payload(
            stage="upstream",
            error_code=str(upstream_summary.get("error_code") or "TASK_UPLOAD_TO_KEEP_LIST_FAILED"),
            message=str(upstream_summary.get("error") or "上游 keep-list 运行失败"),
            remediation="打开上游 summary，先修复 task upload -> keep-list 的失败，再继续下游。",
            details={"upstream_summary_json": str(upstream_summary_path)},
        )
        return finalize("failed", error=failure["message"], error_code=failure["error_code"], failure=failure)

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
        return finalize("failed", error=failure["message"], error_code=failure["error_code"], failure=failure)

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
            probe_vision_provider_only=bool(probe_vision_provider_only),
            skip_scrape=bool(skip_scrape),
            skip_visual=bool(skip_visual),
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
        return finalize("failed", error=failure["message"], error_code=failure["error_code"], failure=failure)

    final_exports = _collect_final_exports(downstream_summary)
    summary["steps"]["downstream"] = {
        "status": downstream_summary.get("status"),
        "summary_json": str(downstream_summary_path),
        "output_root": str(downstream_output_root),
        "requested_platforms": requested_platforms,
        "final_exports": final_exports,
        "platform_statuses": _collect_platform_statuses(downstream_summary),
        "vision_probe": downstream_summary.get("vision_probe") or {},
    }
    summary["artifacts"]["final_exports"] = final_exports

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
        return finalize("failed", error=failure["message"], error_code=failure["error_code"], failure=failure)

    summary["delivery_status"] = downstream_status or "completed"
    return finalize(downstream_status or "completed")


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
        default=MATCHING_STRATEGIES[0],
        choices=MATCHING_STRATEGIES,
        help="上游匹配策略；默认 legacy-enrichment，也可选 brand-keyword-fast-path。",
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
    parser.add_argument("--vision-provider", default="", help="指定视觉 provider，例如 openai / mimo / quan2go / lemonapi。")
    parser.add_argument("--max-identifiers-per-platform", type=int, default=0, help="每个平台最多跑多少个账号；0 表示不截断。")
    parser.add_argument("--poll-interval", type=float, default=5.0, help="轮询 job 状态的秒数。")
    parser.add_argument("--probe-vision-provider-only", action="store_true", help="只做视觉 provider live probe，不继续 scrape/visual/export。")
    parser.add_argument("--skip-scrape", action="store_true", help="只做 staging，不触发 scrape/visual/export。")
    parser.add_argument("--skip-visual", action="store_true", help="跑 scrape 和导出，但跳过视觉复核。")
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
        probe_vision_provider_only=bool(args.probe_vision_provider_only),
        skip_scrape=bool(args.skip_scrape),
        skip_visual=bool(args.skip_visual),
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if summary.get("status") != "failed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
