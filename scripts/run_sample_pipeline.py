import argparse
import json
import sys
import time
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.app import app, get_available_vision_provider_names


PLATFORM_SHEETS = (
    ("YouTube", "youtube"),
    ("TikTok", "tiktok"),
    ("Instagram", "instagram"),
)


def normalize_handle(value):
    text = str(value or "").strip()
    if not text:
        return ""
    return text.lstrip("@").lower()


def pick_sample_rows(source_path, sample_count):
    sample_frames = {}
    selected_summary = {}
    for sheet_name, platform in PLATFORM_SHEETS:
        frame = pd.read_excel(source_path, sheet_name=sheet_name)
        frame = frame[frame["@username"].notna() & frame["Platform"].notna()].copy()
        frame["__handle_key"] = frame["@username"].map(normalize_handle)
        frame = frame[frame["__handle_key"] != ""]

        if platform == "instagram":
            us_rows = frame[frame["Region"].astype(str).str.upper().eq("US")].copy()
            us_rows = us_rows.drop_duplicates(subset=["__handle_key"])
            picked = us_rows.head(sample_count).copy()
            if len(picked) < sample_count:
                remainder = frame[~frame["__handle_key"].isin(picked["__handle_key"])].copy()
                remainder = remainder.drop_duplicates(subset=["__handle_key"])
                picked = pd.concat([picked, remainder.head(sample_count - len(picked))], ignore_index=True)
        else:
            picked = frame.drop_duplicates(subset=["__handle_key"]).head(sample_count).copy()

        picked = picked.drop(columns=["__handle_key"])
        sample_frames[sheet_name] = picked
        selected_summary[platform] = picked[["@username", "URL"]].to_dict(orient="records")
    return sample_frames, selected_summary


def write_sample_workbook(sample_frames, output_path):
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for sheet_name, frame in sample_frames.items():
            frame.to_excel(writer, sheet_name=sheet_name, index=False)


def require_success(response, label):
    if response.status_code >= 400:
        payload = response.get_json(silent=True)
        raise RuntimeError(f"{label} failed: HTTP {response.status_code} {payload or response.data[:200]!r}")
    payload = response.get_json(silent=True)
    if isinstance(payload, dict) and payload.get("success") is False:
        raise RuntimeError(f"{label} failed: {payload}")
    return payload


def poll_job(client, job_id, label, poll_interval):
    while True:
        response = client.get(f"/api/jobs/{job_id}")
        payload = require_success(response, label)
        job = payload["job"]
        if job["status"] in {"completed", "failed", "cancelled"}:
            return job
        time.sleep(poll_interval)


def save_binary_response(response, output_path):
    if response.status_code >= 400:
        payload = response.get_json(silent=True)
        raise RuntimeError(f"download failed for {output_path.name}: HTTP {response.status_code} {payload}")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(response.data)


def export_platform_artifacts(client, platform, export_dir):
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
    save_binary_response(client.post(f"/api/download/{platform}/final-review", json={}), final_review_path)
    outputs["final_review"] = str(final_review_path)
    return outputs


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source-workbook",
        default="/Users/a1234/Desktop/Coding/网红/chuhai/【测试】达人库.xlsx",
    )
    parser.add_argument(
        "--sample-output",
        default=str(Path("temp") / "sample_4_each.xlsx"),
    )
    parser.add_argument(
        "--export-dir",
        default=str(Path("temp") / "sample_pipeline"),
    )
    parser.add_argument("--sample-count", type=int, default=4)
    parser.add_argument("--poll-interval", type=float, default=3.0)
    parser.add_argument("--skip-visual", action="store_true")
    args = parser.parse_args()

    source_path = Path(args.source_workbook).resolve()
    sample_output = Path(args.sample_output).resolve()
    export_dir = Path(args.export_dir).resolve()
    summary_path = export_dir / "summary.json"

    sample_frames, selected_summary = pick_sample_rows(source_path, args.sample_count)
    write_sample_workbook(sample_frames, sample_output)

    export_dir.mkdir(parents=True, exist_ok=True)

    client = app.test_client()
    with sample_output.open("rb") as handle:
        upload_response = client.post(
            "/api/upload",
            data={"file": (handle, sample_output.name)},
            content_type="multipart/form-data",
        )
    upload_payload = require_success(upload_response, "upload")

    summary = {
        "source_workbook": str(source_path),
        "sample_workbook": str(sample_output),
        "selected_accounts": selected_summary,
        "upload": upload_payload,
        "vision_providers": get_available_vision_provider_names(),
        "platforms": {},
    }

    for _, platform in PLATFORM_SHEETS:
        platform_summary = {}
        scrape_response = client.post(
            "/api/jobs/scrape",
            json={"platform": platform, "payload": {}},
        )
        scrape_payload = require_success(scrape_response, f"{platform} scrape start")
        scrape_job = poll_job(client, scrape_payload["job"]["id"], f"{platform} scrape poll", args.poll_interval)
        platform_summary["scrape_job"] = scrape_job
        if scrape_job["status"] != "completed":
            raise RuntimeError(f"{platform} scrape job failed: {scrape_job}")

        if args.skip_visual:
            platform_summary["visual_job"] = {"status": "skipped", "reason": "skip_visual flag set"}
        elif get_available_vision_provider_names():
            visual_response = client.post(
                "/api/jobs/visual-review",
                json={"platform": platform, "payload": {}},
            )
            visual_payload = require_success(visual_response, f"{platform} visual start")
            visual_job = poll_job(client, visual_payload["job"]["id"], f"{platform} visual poll", args.poll_interval)
            platform_summary["visual_job"] = visual_job
        else:
            platform_summary["visual_job"] = {
                "status": "skipped",
                "reason": "missing vision provider config",
            }

        platform_export_dir = export_dir / platform
        platform_summary["exports"] = export_platform_artifacts(client, platform, platform_export_dir)

        artifact_response = client.get(f"/api/artifacts/{platform}/status")
        platform_summary["artifact_status"] = require_success(artifact_response, f"{platform} artifact status")
        summary["platforms"][platform] = platform_summary

        summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
