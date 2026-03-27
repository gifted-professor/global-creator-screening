from __future__ import annotations

from contextlib import contextmanager
import os
from pathlib import Path
from typing import Any

from .email_project import load_email_project, resolve_email_env_file, resolve_email_project_root
from .feishu_api import DEFAULT_FEISHU_BASE_URL, FeishuOpenClient


DEFAULT_MANUAL_UPDATE_ENDPOINT_URL = "http://127.0.0.1:8765/api/project-workbench/manual-update"
DEFAULT_UPLOAD_ENDPOINT_URL = "http://127.0.0.1:8765/api/project-home/import-screening-workbook"


def import_screening_workbook_from_feishu(
    *,
    email_project_root: str | Path | None,
    email_env_file: str | Path | None,
    feishu_app_id: str,
    feishu_app_secret: str,
    file_token_or_url: str,
    project_code: str,
    primary_category: str,
    owner_name: str = "",
    dashboard_output: str | Path | None = None,
    download_dir: str | Path = "./downloads",
    download_name: str | None = None,
    overwrite_download: bool = False,
    manual_update_endpoint_url: str = DEFAULT_MANUAL_UPDATE_ENDPOINT_URL,
    upload_endpoint_url: str = DEFAULT_UPLOAD_ENDPOINT_URL,
    feishu_base_url: str = DEFAULT_FEISHU_BASE_URL,
    timeout_seconds: float = 30.0,
) -> dict[str, Any]:
    resolved_project_root = resolve_email_project_root(email_project_root)
    resolved_env_file = resolve_email_env_file(resolved_project_root, email_env_file)
    modules = load_email_project(resolved_project_root)

    env_base_dir = resolved_env_file.parent if resolved_env_file.exists() else resolved_project_root
    with _pushd(env_base_dir):
        settings = modules.Settings.from_environment(str(resolved_env_file), require_credentials=False)
    settings.data_dir = _resolve_path_from(env_base_dir, settings.data_dir)
    settings.db_path = _resolve_path_from(env_base_dir, settings.db_path)
    settings.raw_dir = _resolve_path_from(env_base_dir, settings.raw_dir)
    settings.ensure_directories()

    output_path = (
        Path(dashboard_output).expanduser()
        if dashboard_output is not None
        else resolved_project_root / "exports" / "index.html"
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)

    client = FeishuOpenClient(
        app_id=feishu_app_id,
        app_secret=feishu_app_secret,
        base_url=feishu_base_url,
        timeout_seconds=timeout_seconds,
    )
    downloaded = client.download_file(file_token_or_url, desired_name=download_name)

    download_root = Path(download_dir).expanduser()
    download_root.mkdir(parents=True, exist_ok=True)
    saved_workbook_path = _write_downloaded_workbook(
        download_root,
        downloaded.file_name,
        downloaded.content,
        overwrite=overwrite_download,
    )

    import_result = modules.build_screening_workbook_upload_bridge_payload(
        settings.db_path,
        output_path=output_path,
        manual_update_endpoint_url=manual_update_endpoint_url,
        upload_endpoint_url=upload_endpoint_url,
        workbook_bytes=downloaded.content,
        workbook_filename=downloaded.file_name,
        project_code=project_code,
        primary_category=primary_category,
        owner_name=owner_name,
    )

    return {
        "ok": True,
        "emailProjectRoot": str(resolved_project_root),
        "emailEnvFile": str(resolved_env_file),
        "dbPath": str(settings.db_path),
        "dashboardOutput": str(output_path),
        "fileToken": downloaded.file_token,
        "downloadedFileName": downloaded.file_name,
        "savedWorkbookPath": str(saved_workbook_path),
        "feishuSourceUrl": downloaded.source_url,
        "importResult": import_result,
    }


def _write_downloaded_workbook(download_root: Path, file_name: str, workbook_bytes: bytes, *, overwrite: bool) -> Path:
    candidate = download_root / Path(file_name).name
    if overwrite:
        candidate.write_bytes(workbook_bytes)
        return candidate

    if not candidate.exists():
        candidate.write_bytes(workbook_bytes)
        return candidate

    stem = candidate.stem
    suffix = candidate.suffix
    counter = 1
    while True:
        next_candidate = candidate.with_name(f"{stem}-{counter}{suffix}")
        if not next_candidate.exists():
            next_candidate.write_bytes(workbook_bytes)
            return next_candidate
        counter += 1


def _resolve_path_from(base_dir: Path, path_value: Path) -> Path:
    path = Path(path_value).expanduser()
    if path.is_absolute():
        return path
    return (base_dir / path).resolve()


@contextmanager
def _pushd(path: Path) -> Any:
    previous = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(previous)
