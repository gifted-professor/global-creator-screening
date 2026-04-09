from __future__ import annotations

import argparse
import json
import math
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


import backend.app as backend_app
from scripts.prepare_screening_inputs import (
    clear_active_visual_prompts,
    configure_backend_runtime,
    persist_active_rulespec,
    persist_active_visual_prompts,
)
from workbook_template_parser import compile_workbook


DEFAULT_SOURCE_WORKBOOK = REPO_ROOT / "data" / "task_upload_mail_sync" / "MINISO" / "exports" / "测试达人库_MINISO_匹配结果_高置信.xlsx"
DEFAULT_TEMPLATE_WORKBOOK = REPO_ROOT / "downloads" / "task_upload_attachments" / "recveXGV2i3BS0" / "需求上传（excel 格式）" / "miniso-星战红人筛号需求模板(1).xlsx"

PLATFORM_ORDER = ("tiktok", "instagram", "youtube")
PLATFORM_LABELS = {
    "tiktok": "TIKTOK",
    "instagram": "INSTAGRAM",
    "youtube": "YOUTUBE",
}


def normalize_handle(value: object) -> str:
    text = str(value or "").strip().lower()
    if text.startswith("@"):
        text = text[1:]
    return text


def normalize_source_platform(value: object) -> str:
    raw = str(value or "").strip().lower()
    if raw in {"tiktok", "tik_tok"}:
        return "tiktok"
    if raw in {"instagram", "ig"}:
        return "instagram"
    if raw in {"youtube", "yt"}:
        return "youtube"
    return ""


def compute_platform_quotas(counts: dict[str, int], total: int) -> dict[str, int]:
    available = {platform: int(count) for platform, count in counts.items() if int(count) > 0}
    if not available or total <= 0:
        return {platform: 0 for platform in PLATFORM_ORDER}

    if total <= len(available):
        ranked = sorted(available.items(), key=lambda item: (-item[1], PLATFORM_ORDER.index(item[0])))
        selected = {platform for platform, _count in ranked[:total]}
        return {platform: (1 if platform in selected else 0) for platform in PLATFORM_ORDER}

    total_available = sum(available.values())
    quotas = {platform: 1 for platform in available}
    remaining = total - len(available)

    exact = {
        platform: (available[platform] / total_available) * remaining
        for platform in available
    }
    floor_allocations = {platform: int(math.floor(value)) for platform, value in exact.items()}
    for platform, value in floor_allocations.items():
        quotas[platform] += value
    assigned = sum(floor_allocations.values())

    remainders = sorted(
        ((exact[platform] - floor_allocations[platform], available[platform], platform) for platform in available),
        reverse=True,
    )
    cursor = 0
    while assigned < remaining and remainders:
        _remainder, _count, platform = remainders[cursor % len(remainders)]
        quotas[platform] += 1
        assigned += 1
        cursor += 1

    return {platform: quotas.get(platform, 0) for platform in PLATFORM_ORDER}


def select_sample_rows(source_path: Path, sample_size: int) -> tuple[pd.DataFrame, dict[str, object]]:
    if source_path.suffix.lower() == ".csv":
        frame = pd.read_csv(source_path, encoding="utf-8-sig")
    else:
        frame = pd.read_excel(source_path)
    frame = frame.copy()
    frame["__platform_key"] = frame["Platform"].map(normalize_source_platform)
    frame["__handle_key"] = frame["@username"].map(normalize_handle)
    frame["__url_key"] = frame["URL"].fillna("").astype(str).str.strip()
    frame = frame[(frame["__platform_key"] != "") & (frame["__handle_key"] != "")]
    frame = frame.drop_duplicates(subset=["__platform_key", "__handle_key"])

    counts = {
        platform: int((frame["__platform_key"] == platform).sum())
        for platform in PLATFORM_ORDER
    }
    quotas = compute_platform_quotas(counts, sample_size)

    selected_frames = []
    selected_accounts: dict[str, list[dict[str, str]]] = {platform: [] for platform in PLATFORM_ORDER}
    for platform in PLATFORM_ORDER:
        quota = quotas.get(platform, 0)
        if quota <= 0:
            continue
        platform_frame = frame[frame["__platform_key"] == platform].copy()
        if platform == "instagram":
            us_first = platform_frame[platform_frame["Region"].astype(str).str.upper().eq("US")]
            remainder = platform_frame[~platform_frame.index.isin(us_first.index)]
            platform_frame = pd.concat([us_first, remainder], ignore_index=False)
        picked = platform_frame.head(quota).copy()
        if picked.empty:
            continue
        selected_frames.append(picked)
        selected_accounts[platform] = picked[["@username", "URL", "nickname"]].fillna("").to_dict(orient="records")

    if not selected_frames:
        raise ValueError(f"没有从 {source_path} 里挑到可用账号。")

    selected = pd.concat(selected_frames, ignore_index=True)
    selected = selected.drop(columns=["__platform_key", "__handle_key", "__url_key"])
    selected = selected.reset_index(drop=True)

    summary = {
        "requested_sample_size": sample_size,
        "selected_rows": len(selected),
        "source_platform_counts": counts,
        "selected_platform_counts": {
            platform: len(selected_accounts.get(platform) or [])
            for platform in PLATFORM_ORDER
        },
        "selected_accounts": selected_accounts,
    }
    return selected, summary


def write_sample_workbook(frame: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        frame.to_excel(writer, sheet_name="results", index=False)


def require_success(response, label: str) -> dict:
    if response.status_code >= 400:
        payload = response.get_json(silent=True)
        raise RuntimeError(f"{label} failed: HTTP {response.status_code} {payload or response.data[:200]!r}")
    payload = response.get_json(silent=True)
    if isinstance(payload, dict) and payload.get("success") is False:
        raise RuntimeError(f"{label} failed: {payload}")
    if not isinstance(payload, dict):
        raise RuntimeError(f"{label} returned non-json payload")
    return payload


def poll_job(client, job_id: str, label: str, poll_interval: float) -> dict:
    while True:
        payload = require_success(client.get(f"/api/jobs/{job_id}"), label)
        job = payload["job"]
        if job["status"] in backend_app.JOB_TERMINAL_STATUSES:
            return job
        time.sleep(poll_interval)


def save_binary_response(response, output_path: Path) -> None:
    if response.status_code >= 400:
        payload = response.get_json(silent=True)
        raise RuntimeError(f"download failed for {output_path.name}: HTTP {response.status_code} {payload}")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(response.data)


def _build_local_final_review_workbook(platform: str, *, profile_reviews=None) -> bytes:
    if profile_reviews is None:
        profile_reviews = backend_app.load_profile_reviews(platform)
    if not profile_reviews:
        raise RuntimeError(f"{platform} final review fallback failed: no profile review data available")
    visual_results = backend_app.load_visual_results(platform)
    rows = backend_app.build_final_review_rows(platform, profile_reviews, visual_results)
    if not rows:
        raise RuntimeError(f"{platform} final review fallback failed: no final review rows available")
    workbook = backend_app.workbook_bytes_from_sheets([("Final Review", rows)])
    if hasattr(workbook, "getvalue"):
        return workbook.getvalue()
    return bytes(workbook)


def _save_final_review_with_missing_profile_fallback(
    client,
    platform: str,
    output_path: Path,
    *,
    final_review_profile_reviews=None,
) -> str:
    request_payload = {}
    if final_review_profile_reviews is not None:
        request_payload["profile_reviews"] = list(final_review_profile_reviews)
    response = client.post(f"/api/download/{platform}/final-review", json=request_payload)
    if response.status_code < 400:
        save_binary_response(response, output_path)
        return "api"
    payload = response.get_json(silent=True) or {}
    if str(payload.get("error_code") or "").strip() != "FINAL_REVIEW_BLOCKED_BY_MISSING_PROFILES":
        raise RuntimeError(f"download failed for {output_path.name}: HTTP {response.status_code} {payload}")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(
        _build_local_final_review_workbook(
            platform,
            profile_reviews=final_review_profile_reviews,
        )
    )
    return "local_missing_profile_fallback"


def export_platform_artifacts(client, platform: str, export_dir: Path, *, final_review_profile_reviews=None) -> dict[str, str]:
    outputs = {}
    prescreen_path = export_dir / f"{platform}_prescreen_review.xlsx"
    save_binary_response(client.get(f"/api/download/{platform}/prescreen-review"), prescreen_path)
    outputs["prescreen_review"] = str(prescreen_path)

    image_review_path = export_dir / f"{platform}_image_review.xlsx"
    save_binary_response(client.get(f"/api/download/{platform}/image-review"), image_review_path)
    outputs["image_review"] = str(image_review_path)

    test_info_path = export_dir / f"{platform}_test_info.xlsx"
    save_binary_response(client.get(f"/api/download/{platform}/test-info"), test_info_path)
    outputs["test_info"] = str(test_info_path)

    test_info_json_path = export_dir / f"{platform}_test_info.json"
    json_response = client.get(f"/api/download/{platform}/test-info-json")
    if json_response.status_code >= 400:
        payload = json_response.get_json(silent=True)
        raise RuntimeError(f"download failed for {test_info_json_path.name}: HTTP {json_response.status_code} {payload}")
    test_info_json_path.write_bytes(json_response.data)
    outputs["test_info_json"] = str(test_info_json_path)

    final_review_path = export_dir / f"{platform}_final_review.xlsx"
    outputs["final_review_export_mode"] = _save_final_review_with_missing_profile_fallback(
        client,
        platform,
        final_review_path,
        final_review_profile_reviews=final_review_profile_reviews,
    )
    outputs["final_review"] = str(final_review_path)

    artifact_status = require_success(client.get(f"/api/artifacts/{platform}/status"), f"{platform} artifact status")
    if artifact_status.get("saved_positioning_card_artifacts_available"):
        positioning_review_path = export_dir / f"{platform}_positioning_card_review.xlsx"
        save_binary_response(client.get(f"/api/download/{platform}/positioning-card-review"), positioning_review_path)
        outputs["positioning_card_review"] = str(positioning_review_path)

        positioning_json_path = export_dir / f"{platform}_positioning_card_results.json"
        json_response = client.get(f"/api/download/{platform}/positioning-card-json")
        if json_response.status_code >= 400:
            payload = json_response.get_json(silent=True)
            raise RuntimeError(f"download failed for {positioning_json_path.name}: HTTP {json_response.status_code} {payload}")
        positioning_json_path.write_bytes(json_response.data)
        outputs["positioning_card_json"] = str(positioning_json_path)
    return outputs


def reset_backend_runtime_state() -> None:
    with backend_app.JOBS_LOCK:
        backend_app.JOBS.clear()


def count_passed_profiles(scrape_job: dict) -> int:
    result = dict((scrape_job or {}).get("result") or {})
    profile_reviews = list(result.get("profile_reviews") or [])
    return len([item for item in profile_reviews if str((item or {}).get("status") or "").strip() == "Pass"])


def run_smoke(
    *,
    source_workbook: Path,
    template_workbook: Path,
    sample_size: int,
    output_root: Path,
    poll_interval: float,
    skip_visual: bool,
) -> dict[str, object]:
    sample_frame, sample_summary = select_sample_rows(source_workbook, sample_size)
    output_root.mkdir(parents=True, exist_ok=True)

    data_dir = output_root / "data"
    config_dir = output_root / "config"
    temp_dir = output_root / "temp"
    parsed_output_dir = output_root / "parsed_templates"
    exports_dir = output_root / "exports"
    sample_workbook_path = output_root / "sample_input.xlsx"
    summary_path = output_root / "summary.json"

    write_sample_workbook(sample_frame, sample_workbook_path)
    configure_backend_runtime(
        screening_data_dir=data_dir,
        config_dir=config_dir,
        temp_dir=temp_dir,
    )
    backend_app.ensure_runtime_dirs()
    reset_backend_runtime_state()

    compile_report = compile_workbook(template_workbook, parsed_output_dir)
    persist_active_rulespec(Path(compile_report["artifacts"]["rulespec_json"]))
    visual_prompts_path = compile_report.get("artifacts", {}).get("visual_prompts_json")
    if visual_prompts_path:
        persist_active_visual_prompts(Path(visual_prompts_path))
    else:
        clear_active_visual_prompts()

    client = backend_app.app.test_client()
    with sample_workbook_path.open("rb") as handle:
        upload_payload = require_success(
            client.post(
                "/api/upload",
                data={"file": (handle, sample_workbook_path.name)},
                content_type="multipart/form-data",
            ),
            "upload",
        )

    summary: dict[str, object] = {
        "started_at": backend_app.iso_now(),
        "source_workbook": str(source_workbook),
        "template_workbook": str(template_workbook),
        "output_root": str(output_root),
        "sample_workbook": str(sample_workbook_path),
        "sample": sample_summary,
        "active_visual_prompts_path": backend_app.ACTIVE_VISUAL_PROMPTS_PATH,
        "rulespec": {
            "compile_report_path": str(Path(compile_report["output_dir"]) / "compile_report.json"),
            "rulespec_json_path": str(compile_report["artifacts"]["rulespec_json"]),
            "visual_prompts_json_path": str(visual_prompts_path or ""),
            "warning_count": len(compile_report.get("warnings") or []),
        },
        "upload": upload_payload,
        "vision_providers": backend_app.get_available_vision_provider_names(),
        "vision_preflight": backend_app.build_vision_preflight(),
        "platforms": {},
    }
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    active_platforms = [
        platform
        for platform in PLATFORM_ORDER
        if int((upload_payload.get("metadata_counts") or {}).get(platform) or 0) > 0
    ]

    for platform in active_platforms:
        platform_summary: dict[str, object] = {
            "vision_preflight": backend_app.build_vision_preflight(),
        }
        scrape_payload = require_success(
            client.post("/api/jobs/scrape", json={"platform": platform, "payload": {}}),
            f"{platform} scrape start",
        )
        scrape_job = poll_job(client, scrape_payload["job"]["id"], f"{platform} scrape poll", poll_interval)
        platform_summary["scrape_job"] = scrape_job
        if scrape_job["status"] != "completed":
            raise RuntimeError(f"{platform} scrape job failed: {scrape_job}")

        pass_count = count_passed_profiles(scrape_job)
        platform_summary["prescreen_pass_count"] = pass_count
        platform_summary["visual_gate"] = {
            "executed": False,
            "skip_visual_flag": bool(skip_visual),
            "preflight_status": platform_summary["vision_preflight"]["status"],
            "runnable_provider_names": platform_summary["vision_preflight"]["runnable_provider_names"],
            "configured_provider_names": platform_summary["vision_preflight"]["configured_provider_names"],
        }

        if skip_visual:
            platform_summary["visual_job"] = {"status": "skipped", "reason": "skip_visual flag set"}
        elif pass_count <= 0:
            platform_summary["visual_job"] = {"status": "skipped", "reason": "no Prescreen=Pass targets"}
        elif backend_app.get_available_vision_provider_names():
            visual_payload = require_success(
                client.post("/api/jobs/visual-review", json={"platform": platform, "payload": {}}),
                f"{platform} visual start",
            )
            visual_job = poll_job(client, visual_payload["job"]["id"], f"{platform} visual poll", poll_interval)
            platform_summary["visual_job"] = visual_job
            platform_summary["visual_gate"]["executed"] = True
        else:
            platform_summary["visual_job"] = {
                "status": "skipped",
                "reason": platform_summary["vision_preflight"]["message"],
                "error_code": platform_summary["vision_preflight"]["error_code"],
                "vision_preflight": platform_summary["vision_preflight"],
            }

        platform_summary["artifact_status"] = require_success(
            client.get(f"/api/artifacts/{platform}/status"),
            f"{platform} artifact status",
        )
        platform_summary["exports"] = export_platform_artifacts(client, platform, exports_dir / platform)
        summary["platforms"][platform] = platform_summary
        summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    summary["finished_at"] = backend_app.iso_now()
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return summary


def default_output_root() -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return REPO_ROOT / "temp" / f"screening_smoke_{timestamp}"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a real screening smoke test against the local backend stack.")
    parser.add_argument("--source-workbook", default=str(DEFAULT_SOURCE_WORKBOOK), help="高置信达人名单 xlsx/csv。")
    parser.add_argument("--template-workbook", default=str(DEFAULT_TEMPLATE_WORKBOOK), help="需求模板 xlsx。")
    parser.add_argument("--sample-size", type=int, default=10, help="总共抽多少个账号做 smoke run。")
    parser.add_argument("--output-root", default="", help="输出目录；默认写到 temp/screening_smoke_<timestamp>。")
    parser.add_argument("--poll-interval", type=float, default=5.0, help="轮询 job 状态间隔秒数。")
    parser.add_argument("--skip-visual", action="store_true", help="只跑到 prescreen，不调用真实视觉接口。")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    summary = run_smoke(
        source_workbook=Path(args.source_workbook).expanduser().resolve(),
        template_workbook=Path(args.template_workbook).expanduser().resolve(),
        sample_size=max(1, int(args.sample_size)),
        output_root=Path(args.output_root).expanduser().resolve() if args.output_root else default_output_root(),
        poll_interval=max(1.0, float(args.poll_interval)),
        skip_visual=bool(args.skip_visual),
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
