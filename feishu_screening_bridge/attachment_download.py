from __future__ import annotations

from pathlib import Path
from typing import Any

from .bitable_export import ResolvedBitableView, resolve_bitable_view_from_url
from .feishu_api import FeishuOpenClient


def download_bitable_attachments(
    client: FeishuOpenClient,
    *,
    url: str,
    output_dir: str | Path,
    page_size: int = 500,
) -> dict[str, Any]:
    resolved = resolve_bitable_view_from_url(client, url)
    records = _fetch_all_records(client, resolved, page_size=page_size)
    attachment_jobs = _collect_attachment_jobs(records)

    output_root = Path(output_dir).expanduser()
    output_root.mkdir(parents=True, exist_ok=True)
    downloaded_items: list[dict[str, Any]] = []
    for job in attachment_jobs:
        record_dir = output_root / (job["record_id"] or "unknown_record")
        field_dir = record_dir / _safe_dirname(job["field_name"] or "attachment_field")
        field_dir.mkdir(parents=True, exist_ok=True)
        downloaded = client.download_file(job["file_token"], desired_name=job["file_name"])
        destination = _write_unique_file(field_dir, downloaded.file_name, downloaded.content)
        downloaded_items.append(
            {
                "recordId": job["record_id"],
                "fieldName": job["field_name"],
                "fileToken": job["file_token"],
                "fileName": downloaded.file_name,
                "savedPath": str(destination),
                "sourceUrl": downloaded.source_url,
            }
        )

    return {
        "ok": True,
        "sourceUrl": resolved.source_url,
        "sourceKind": resolved.source_kind,
        "title": resolved.title,
        "tableId": resolved.table_id,
        "tableName": resolved.table_name,
        "viewId": resolved.view_id,
        "viewName": resolved.view_name,
        "recordCount": len(records),
        "attachmentCount": len(attachment_jobs),
        "downloadedCount": len(downloaded_items),
        "outputDir": str(output_root),
        "items": downloaded_items,
    }


def _fetch_all_records(
    client: FeishuOpenClient,
    resolved: ResolvedBitableView,
    *,
    page_size: int,
) -> list[dict[str, Any]]:
    collected: list[dict[str, Any]] = []
    page_token = ""
    while True:
        body: dict[str, Any] = {"view_id": resolved.view_id, "page_size": int(page_size)}
        if page_token:
            body["page_token"] = page_token
        payload = client.post_api_json(
            f"/bitable/v1/apps/{resolved.app_token}/tables/{resolved.table_id}/records/search",
            body=body,
        )
        data = payload.get("data", {}) or {}
        items = data.get("items") or []
        for item in items:
            if isinstance(item, dict):
                collected.append(
                    {
                        "record_id": str(item.get("record_id") or ""),
                        "fields": item.get("fields") or {},
                    }
                )
        if not bool(data.get("has_more")):
            break
        page_token = str(data.get("page_token") or "").strip()
        if not page_token:
            break
    return collected


def _collect_attachment_jobs(records: list[dict[str, Any]]) -> list[dict[str, str]]:
    jobs: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    for record in records:
        record_id = str(record.get("record_id") or "")
        fields = record.get("fields") or {}
        if not isinstance(fields, dict):
            continue
        for field_name, value in fields.items():
            if not isinstance(value, list):
                continue
            for item in value:
                if not isinstance(item, dict):
                    continue
                file_token = str(item.get("file_token") or item.get("fileToken") or "").strip()
                if not file_token:
                    continue
                file_name = str(item.get("name") or item.get("file_name") or file_token).strip() or file_token
                dedupe_key = (record_id, str(field_name or ""), file_token)
                if dedupe_key in seen:
                    continue
                seen.add(dedupe_key)
                jobs.append(
                    {
                        "record_id": record_id,
                        "field_name": str(field_name or ""),
                        "file_token": file_token,
                        "file_name": file_name,
                    }
                )
    return jobs


def _safe_dirname(name: str) -> str:
    candidate = str(name or "").strip().replace("/", "_").replace("\x00", "")
    return candidate or "attachment_field"


def _write_unique_file(directory: Path, file_name: str, content: bytes) -> Path:
    candidate = directory / Path(file_name).name
    if not candidate.exists():
        candidate.write_bytes(content)
        return candidate
    stem = candidate.stem
    suffix = candidate.suffix
    counter = 1
    while True:
        next_candidate = candidate.with_name(f"{stem}-{counter}{suffix}")
        if not next_candidate.exists():
            next_candidate.write_bytes(content)
            return next_candidate
        counter += 1
