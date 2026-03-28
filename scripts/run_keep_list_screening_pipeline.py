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


def normalize_platforms(values: list[str] | None) -> list[str]:
    runtime = _load_runtime_dependencies()
    backend_app = runtime["backend_app"]
    if not values:
        return list(DEFAULT_PLATFORM_ORDER)
    normalized: list[str] = []
    seen: set[str] = set()
    for value in values:
        platform = str(value or "").strip().lower()
        if platform not in backend_app.PLATFORM_ACTORS:
            raise ValueError(f"不支持的平台: {value}")
        if platform in seen:
            continue
        seen.add(platform)
        normalized.append(platform)
    return normalized


def select_platform_identifiers(platform: str, max_identifiers_per_platform: int) -> list[str]:
    runtime = _load_runtime_dependencies()
    backend_app = runtime["backend_app"]
    metadata_lookup = backend_app.load_upload_metadata(platform)
    identifiers = [str(item).strip() for item in metadata_lookup.keys() if str(item).strip()]
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
    max_identifiers_per_platform: int = 0,
    poll_interval: float = 5.0,
    skip_scrape: bool = False,
    skip_visual: bool = False,
) -> dict[str, Any]:
    runtime = _load_runtime_dependencies()
    backend_app = runtime["backend_app"]
    prepare_screening_inputs = runtime["prepare_screening_inputs"]
    count_passed_profiles = runtime["count_passed_profiles"]
    export_platform_artifacts = runtime["export_platform_artifacts"]
    poll_job = runtime["poll_job"]
    require_success = runtime["require_success"]
    reset_backend_runtime_state = runtime["reset_backend_runtime_state"]

    resolved_output_root = (output_root or default_output_root()).expanduser().resolve()
    resolved_output_root.mkdir(parents=True, exist_ok=True)

    run_summary_path = (summary_json.expanduser().resolve() if summary_json else resolved_output_root / "summary.json")
    staging_summary_path = resolved_output_root / "staging_summary.json"
    screening_data_dir = resolved_output_root / "data"
    config_dir = resolved_output_root / "config"
    temp_dir = resolved_output_root / "temp"
    exports_dir = resolved_output_root / "exports"
    requested_platforms = normalize_platforms(platform_filters)

    reset_backend_runtime_state()
    staging_summary = prepare_screening_inputs(
        creator_workbook=keep_workbook.expanduser().resolve(),
        template_workbook=template_workbook.expanduser().resolve() if template_workbook else None,
        task_name=str(task_name or "").strip(),
        task_upload_url=str(task_upload_url or "").strip(),
        env_file=env_file,
        screening_data_dir=screening_data_dir,
        config_dir=config_dir,
        temp_dir=temp_dir,
        summary_json=staging_summary_path,
    )

    summary: dict[str, Any] = {
        "started_at": backend_app.iso_now(),
        "keep_workbook": str(keep_workbook.expanduser().resolve()),
        "template_workbook": str(template_workbook.expanduser().resolve()) if template_workbook else "",
        "task_name": str(task_name or "").strip(),
        "task_upload_url": str(task_upload_url or "").strip(),
        "env_file": str(env_file),
        "output_root": str(resolved_output_root),
        "summary_json": str(run_summary_path),
        "staging_summary_json": str(staging_summary_path),
        "requested_platforms": requested_platforms,
        "max_identifiers_per_platform": int(max_identifiers_per_platform),
        "skip_scrape": bool(skip_scrape),
        "skip_visual": bool(skip_visual),
        "vision_providers": backend_app.get_available_vision_provider_names(),
        "staging": staging_summary,
        "platforms": {},
    }

    client = backend_app.app.test_client()
    for platform in requested_platforms:
        requested_identifiers = select_platform_identifiers(platform, max(0, int(max_identifiers_per_platform)))
        platform_summary: dict[str, Any] = {
            "staged_identifier_count": len(backend_app.load_upload_metadata(platform)),
            "requested_identifier_count": len(requested_identifiers),
            "requested_identifier_preview": requested_identifiers[:10],
        }

        if not requested_identifiers:
            platform_summary["status"] = "skipped"
            platform_summary["reason"] = "no staged identifiers for platform"
            summary["platforms"][platform] = platform_summary
            continue

        if skip_scrape:
            platform_summary["status"] = "staged_only"
            platform_summary["scrape_job"] = {"status": "skipped", "reason": "skip_scrape flag set"}
            summary["platforms"][platform] = platform_summary
            backend_app.write_json_file(str(run_summary_path), summary)
            continue

        scrape_payload_body = build_scrape_payload(platform, requested_identifiers)
        scrape_payload = require_success(
            client.post("/api/jobs/scrape", json={"platform": platform, "payload": scrape_payload_body}),
            f"{platform} scrape start",
        )
        scrape_job = poll_job(client, scrape_payload["job"]["id"], f"{platform} scrape poll", max(1.0, float(poll_interval)))
        platform_summary["scrape_job"] = scrape_job
        if scrape_job["status"] != "completed":
            platform_summary["status"] = "scrape_failed"
            summary["platforms"][platform] = platform_summary
            backend_app.write_json_file(str(run_summary_path), summary)
            continue

        pass_count = count_passed_profiles(scrape_job)
        platform_summary["prescreen_pass_count"] = pass_count
        if skip_visual:
            platform_summary["visual_job"] = {"status": "skipped", "reason": "skip_visual flag set"}
        elif pass_count <= 0:
            platform_summary["visual_job"] = {"status": "skipped", "reason": "no Prescreen=Pass targets"}
        elif backend_app.get_available_vision_provider_names():
            visual_payload_body = build_visual_payload(platform, requested_identifiers)
            visual_payload = require_success(
                client.post("/api/jobs/visual-review", json={"platform": platform, "payload": visual_payload_body}),
                f"{platform} visual start",
            )
            platform_summary["visual_job"] = poll_job(
                client,
                visual_payload["job"]["id"],
                f"{platform} visual poll",
                max(1.0, float(poll_interval)),
            )
        else:
            platform_summary["visual_job"] = {"status": "skipped", "reason": "missing vision provider config"}

        platform_summary["artifact_status"] = require_success(
            client.get(f"/api/artifacts/{platform}/status"),
            f"{platform} artifact status",
        )
        platform_summary["exports"] = export_platform_artifacts(client, platform, exports_dir / platform)
        platform_summary["status"] = "completed"
        summary["platforms"][platform] = platform_summary
        backend_app.write_json_file(str(run_summary_path), summary)

    summary["finished_at"] = backend_app.iso_now()
    backend_app.write_json_file(str(run_summary_path), summary)
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
    parser.add_argument("--max-identifiers-per-platform", type=int, default=0, help="每个平台最多跑多少个账号；0 表示不截断。")
    parser.add_argument("--poll-interval", type=float, default=5.0, help="轮询 job 状态的秒数。")
    parser.add_argument("--skip-scrape", action="store_true", help="只做 staging，不触发 scrape/visual/export。")
    parser.add_argument("--skip-visual", action="store_true", help="跑 scrape 和导出，但跳过视觉复核。")
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
        max_identifiers_per_platform=max(0, int(args.max_identifiers_per_platform)),
        poll_interval=max(1.0, float(args.poll_interval)),
        skip_scrape=bool(args.skip_scrape),
        skip_visual=bool(args.skip_visual),
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
