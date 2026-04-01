from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import re
import secrets


REPO_ROOT = Path(__file__).resolve().parents[1]


def safe_name(value: str) -> str:
    normalized = re.sub(r"[^0-9A-Za-z._-]+", "_", str(value or "").strip())
    normalized = normalized.strip("._-")
    return normalized or "task"


def build_run_id(task_name: str) -> str:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{timestamp}_{safe_name(task_name)}_{secrets.token_hex(4)}"


def repo_root() -> Path:
    return REPO_ROOT


@dataclass(frozen=True)
class FinalRunnerPaths:
    run_id: str
    run_root: Path
    output_root: Path
    summary_json: Path
    task_spec_json: Path
    upstream_output_root: Path
    upstream_summary_json: Path
    upstream_task_spec_json: Path
    downstream_output_root: Path
    downstream_summary_json: Path
    downstream_task_spec_json: Path
    output_root_source: str
    summary_json_source: str


@dataclass(frozen=True)
class UpstreamRunnerPaths:
    run_id: str
    run_root: Path
    output_root: Path
    summary_json: Path
    task_spec_json: Path
    downloads_dir: Path
    mail_root: Path
    exports_dir: Path
    output_root_source: str
    summary_json_source: str
    downloads_dir_source: str
    mail_root_source: str


@dataclass(frozen=True)
class DownstreamRunnerPaths:
    run_id: str
    run_root: Path
    output_root: Path
    summary_json: Path
    task_spec_json: Path
    staging_summary_json: Path
    screening_data_dir: Path
    config_dir: Path
    temp_dir: Path
    exports_dir: Path
    downloads_dir: Path
    template_output_dir: Path
    output_root_source: str
    summary_json_source: str


@dataclass(frozen=True)
class OperatorRunPaths:
    run_id: str
    run_root: Path
    output_root: Path
    summary_json: Path
    task_spec_json: Path
    log_path: Path
    output_root_source: str
    summary_json_source: str


def resolve_final_runner_paths(
    *,
    task_name: str,
    output_root: Path | None = None,
    summary_json: Path | None = None,
) -> FinalRunnerPaths:
    run_id = build_run_id(task_name)
    if output_root is not None:
        resolved_run_root = output_root.expanduser().resolve()
        output_root_source = "cli"
    else:
        resolved_run_root = (
            repo_root() / "temp" / "runs" / "task_upload_to_final_export" / run_id
        ).resolve()
        output_root_source = "default_run_root"
    resolved_summary_json = (
        summary_json.expanduser().resolve()
        if summary_json is not None
        else (resolved_run_root / "summary.json").resolve()
    )
    summary_json_source = "cli" if summary_json is not None else "output_root_default"
    return FinalRunnerPaths(
        run_id=run_id,
        run_root=resolved_run_root,
        output_root=resolved_run_root,
        summary_json=resolved_summary_json,
        task_spec_json=(resolved_run_root / "task_spec.json").resolve(),
        upstream_output_root=(resolved_run_root / "upstream").resolve(),
        upstream_summary_json=(resolved_run_root / "upstream" / "summary.json").resolve(),
        upstream_task_spec_json=(resolved_run_root / "upstream" / "task_spec.json").resolve(),
        downstream_output_root=(resolved_run_root / "downstream").resolve(),
        downstream_summary_json=(resolved_run_root / "downstream" / "summary.json").resolve(),
        downstream_task_spec_json=(resolved_run_root / "downstream" / "task_spec.json").resolve(),
        output_root_source=output_root_source,
        summary_json_source=summary_json_source,
    )


def resolve_operator_run_paths(
    *,
    task_name: str,
    runs_root: Path | None = None,
    output_root: Path | None = None,
    summary_json: Path | None = None,
) -> OperatorRunPaths:
    run_id = build_run_id(task_name)
    effective_runs_root = (runs_root or (repo_root() / "temp" / "operator_runs")).expanduser().resolve()
    if output_root is not None:
        resolved_run_root = output_root.expanduser().resolve()
        output_root_source = "cli"
    else:
        resolved_run_root = (effective_runs_root / run_id).resolve()
        output_root_source = "default_run_root"
    resolved_summary_json = (
        summary_json.expanduser().resolve()
        if summary_json is not None
        else (resolved_run_root / "summary.json").resolve()
    )
    return OperatorRunPaths(
        run_id=run_id,
        run_root=resolved_run_root,
        output_root=resolved_run_root,
        summary_json=resolved_summary_json,
        task_spec_json=(resolved_run_root / "task_spec.json").resolve(),
        log_path=(resolved_run_root / "operator_run.log").resolve(),
        output_root_source=output_root_source,
        summary_json_source="cli" if summary_json is not None else "output_root_default",
    )


def resolve_keep_list_upstream_paths(
    *,
    task_name: str,
    output_root: Path | None = None,
    summary_json: Path | None = None,
    task_download_dir: str | Path = "",
    mail_data_dir: str | Path = "",
) -> UpstreamRunnerPaths:
    run_id = build_run_id(task_name)
    if output_root is not None:
        resolved_run_root = output_root.expanduser().resolve()
        output_root_source = "cli"
    else:
        resolved_run_root = (
            repo_root() / "temp" / "runs" / "task_upload_to_keep_list" / run_id
        ).resolve()
        output_root_source = "default_run_root"
    resolved_summary_json = (
        summary_json.expanduser().resolve()
        if summary_json is not None
        else (resolved_run_root / "summary.json").resolve()
    )
    resolved_downloads_dir = (
        Path(task_download_dir).expanduser().resolve()
        if str(task_download_dir or "").strip()
        else (resolved_run_root / "downloads").resolve()
    )
    resolved_mail_root = (
        Path(mail_data_dir).expanduser().resolve()
        if str(mail_data_dir or "").strip()
        else (resolved_run_root / "mail_sync").resolve()
    )
    return UpstreamRunnerPaths(
        run_id=run_id,
        run_root=resolved_run_root,
        output_root=resolved_run_root,
        summary_json=resolved_summary_json,
        task_spec_json=(resolved_run_root / "task_spec.json").resolve(),
        downloads_dir=resolved_downloads_dir,
        mail_root=resolved_mail_root,
        exports_dir=(resolved_run_root / "exports").resolve(),
        output_root_source=output_root_source,
        summary_json_source="cli" if summary_json is not None else "output_root_default",
        downloads_dir_source="cli" if str(task_download_dir or "").strip() else "output_root_default",
        mail_root_source="cli" if str(mail_data_dir or "").strip() else "output_root_default",
    )


def resolve_keep_list_downstream_paths(
    *,
    task_name: str,
    output_root: Path | None = None,
    summary_json: Path | None = None,
) -> DownstreamRunnerPaths:
    run_id = build_run_id(task_name)
    if output_root is not None:
        resolved_run_root = output_root.expanduser().resolve()
        output_root_source = "cli"
    else:
        resolved_run_root = (
            repo_root() / "temp" / "runs" / "keep_list_screening" / run_id
        ).resolve()
        output_root_source = "default_run_root"
    return DownstreamRunnerPaths(
        run_id=run_id,
        run_root=resolved_run_root,
        output_root=resolved_run_root,
        summary_json=(
            summary_json.expanduser().resolve()
            if summary_json is not None
            else (resolved_run_root / "summary.json").resolve()
        ),
        task_spec_json=(resolved_run_root / "task_spec.json").resolve(),
        staging_summary_json=(resolved_run_root / "staging_summary.json").resolve(),
        screening_data_dir=(resolved_run_root / "data").resolve(),
        config_dir=(resolved_run_root / "config").resolve(),
        temp_dir=(resolved_run_root / "temp").resolve(),
        exports_dir=(resolved_run_root / "exports").resolve(),
        downloads_dir=(resolved_run_root / "downloads").resolve(),
        template_output_dir=(
            resolved_run_root / "downloads" / "task_upload_attachments" / "parsed_outputs"
        ).resolve(),
        output_root_source=output_root_source,
        summary_json_source="cli" if summary_json is not None else "output_root_default",
    )
