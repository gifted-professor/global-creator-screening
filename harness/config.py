from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

from harness.failures import build_failure_payload


@dataclass(frozen=True)
class EnvFileSnapshot:
    raw: str
    path: Path
    exists: bool
    values: dict[str, str]
    source: str


@dataclass(frozen=True)
class ResolvedConfig:
    value: str
    source: str

    @property
    def present(self) -> bool:
        return bool(str(self.value or "").strip())


@dataclass(frozen=True)
class RequiredConfigSpec:
    resolved: ResolvedConfig
    error_code: str
    message: str
    remediation: str
    details: dict[str, Any] | None = None


def _parse_env_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        cleaned_key = key.strip()
        if cleaned_key:
            values[cleaned_key] = value.strip().strip('"').strip("'")
    return values


def load_env_file_snapshot(env_file: str | Path, *, default_env_file: str = ".env") -> EnvFileSnapshot:
    raw = str(env_file or default_env_file).strip() or default_env_file
    path = Path(raw).expanduser()
    resolved_path = path.resolve()
    source = "cli" if raw != default_env_file else "default"
    return EnvFileSnapshot(
        raw=raw,
        path=resolved_path,
        exists=resolved_path.exists(),
        values=_parse_env_file(resolved_path),
        source=source,
    )


def resolve_string(
    *,
    cli_value: object,
    env_snapshot: EnvFileSnapshot,
    env_keys: tuple[str, ...] = (),
    default: str = "",
    derived_from: str = "",
) -> ResolvedConfig:
    candidate = str(cli_value or "").strip()
    if candidate:
        return ResolvedConfig(value=candidate, source="cli")
    for env_key in env_keys:
        env_candidate = str(env_snapshot.values.get(env_key, "") or "").strip()
        if env_candidate:
            return ResolvedConfig(value=env_candidate, source=f"env_file:{env_key}")
    default_value = str(default or "").strip()
    if default_value:
        source = f"derived:{derived_from}" if derived_from else "default"
        return ResolvedConfig(value=default_value, source=source)
    return ResolvedConfig(value="", source="unset")


def source_record(resolved: ResolvedConfig, *, sensitive: bool = False) -> str | dict[str, object]:
    if sensitive:
        return {
            "present": resolved.present,
            "source": resolved.source,
        }
    return resolved.source


def build_required_config_errors(requirements: Sequence[RequiredConfigSpec]) -> list[dict[str, Any]]:
    errors: list[dict[str, Any]] = []
    for requirement in requirements:
        if requirement.resolved.present:
            continue
        errors.append(
            build_failure_payload(
                stage="preflight",
                error_code=requirement.error_code,
                message=requirement.message,
                remediation=requirement.remediation,
                failure_layer="preflight",
                details=dict(requirement.details or {}),
            )
        )
    return errors


def normalize_platform_filters(platform_filters: list[str] | None) -> list[str]:
    return [str(value).strip().lower() for value in (platform_filters or []) if str(value).strip()]


def resolve_final_runner_config(
    *,
    env_file: str | Path,
    task_upload_url: str,
    employee_info_url: str,
    feishu_app_id: str,
    feishu_app_secret: str,
    feishu_base_url: str,
    timeout_seconds: float,
    matching_strategy: str,
    brand_keyword: str,
    task_name: str,
    platform_filters: list[str] | None,
    vision_provider: str,
    max_identifiers_per_platform: int,
    mail_limit: int,
    mail_workers: int,
    sent_since: str,
    reset_state: bool,
    reuse_existing: bool,
    probe_vision_provider_only: bool,
    skip_scrape: bool,
    skip_visual: bool,
    skip_positioning_card_analysis: bool,
) -> tuple[dict[str, Any], dict[str, Any]]:
    env_snapshot = load_env_file_snapshot(env_file)
    resolved_task_upload_url = resolve_string(
        cli_value=task_upload_url,
        env_snapshot=env_snapshot,
        env_keys=("TASK_UPLOAD_URL", "FEISHU_SOURCE_URL"),
    )
    resolved_employee_info_url = resolve_string(
        cli_value=employee_info_url,
        env_snapshot=env_snapshot,
        env_keys=("EMPLOYEE_INFO_URL", "FEISHU_SOURCE_URL"),
    )
    resolved_feishu_app_id = resolve_string(
        cli_value=feishu_app_id,
        env_snapshot=env_snapshot,
        env_keys=("FEISHU_APP_ID",),
    )
    resolved_feishu_app_secret = resolve_string(
        cli_value=feishu_app_secret,
        env_snapshot=env_snapshot,
        env_keys=("FEISHU_APP_SECRET",),
    )
    resolved_feishu_base_url = resolve_string(
        cli_value=feishu_base_url,
        env_snapshot=env_snapshot,
        env_keys=("FEISHU_OPEN_BASE_URL",),
        default="https://open.feishu.cn",
    )
    resolved_timeout_seconds = resolve_string(
        cli_value=timeout_seconds if float(timeout_seconds or 0.0) > 0 else "",
        env_snapshot=env_snapshot,
        env_keys=("TIMEOUT_SECONDS",),
        default="30",
    )
    resolved_brand_keyword = resolve_string(
        cli_value=brand_keyword,
        env_snapshot=env_snapshot,
        default=task_name,
        derived_from="task_name",
    )
    normalized_platforms = normalize_platform_filters(platform_filters)
    return (
        {
            "env_file": env_snapshot.source,
            "task_upload_url": source_record(resolved_task_upload_url),
            "employee_info_url": source_record(resolved_employee_info_url),
            "feishu_app_id": source_record(resolved_feishu_app_id, sensitive=True),
            "feishu_app_secret": source_record(resolved_feishu_app_secret, sensitive=True),
            "feishu_base_url": source_record(resolved_feishu_base_url),
            "timeout_seconds": source_record(resolved_timeout_seconds),
            "matching_strategy": "cli" if str(matching_strategy or "").strip() else "default",
            "brand_keyword": source_record(resolved_brand_keyword),
            "platform_filters": "cli" if normalized_platforms else "default",
            "vision_provider": "cli" if str(vision_provider or "").strip() else "default",
            "max_identifiers_per_platform": "cli" if int(max_identifiers_per_platform or 0) > 0 else "default",
            "mail_limit": "cli" if int(mail_limit or 0) > 0 else "default",
            "mail_workers": "cli" if int(mail_workers or 1) != 1 else "default",
            "sent_since": "cli" if str(sent_since or "").strip() else "default",
            "reset_state": "cli" if bool(reset_state) else "default",
            "reuse_existing": "cli" if not bool(reuse_existing) else "default",
            "probe_vision_provider_only": "cli" if bool(probe_vision_provider_only) else "default",
            "skip_scrape": "cli" if bool(skip_scrape) else "default",
            "skip_visual": "cli" if bool(skip_visual) else "default",
            "skip_positioning_card_analysis": "cli" if bool(skip_positioning_card_analysis) else "default",
        },
        {
            "env_snapshot": env_snapshot,
            "task_upload_url": resolved_task_upload_url,
            "employee_info_url": resolved_employee_info_url,
            "feishu_app_id": resolved_feishu_app_id,
            "feishu_app_secret": resolved_feishu_app_secret,
            "feishu_base_url": resolved_feishu_base_url,
            "timeout_seconds": resolved_timeout_seconds,
            "brand_keyword": resolved_brand_keyword,
        },
    )


def resolve_keep_list_upstream_config(
    *,
    env_file: str | Path,
    task_upload_url: str,
    employee_info_url: str,
    feishu_app_id: str,
    feishu_app_secret: str,
    feishu_base_url: str,
    timeout_seconds: float,
    imap_host: str,
    imap_port: int,
    matching_strategy: str,
    brand_keyword: str,
    task_name: str,
    mail_limit: int,
    mail_workers: int,
    sent_since: str,
    reset_state: bool,
    stop_after: str,
    reuse_existing: bool,
    task_download_dir_source: str,
    mail_data_dir_source: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    env_snapshot = load_env_file_snapshot(env_file)
    resolved_task_upload_url = resolve_string(
        cli_value=task_upload_url,
        env_snapshot=env_snapshot,
        env_keys=("TASK_UPLOAD_URL", "FEISHU_SOURCE_URL"),
    )
    resolved_employee_info_url = resolve_string(
        cli_value=employee_info_url,
        env_snapshot=env_snapshot,
        env_keys=("EMPLOYEE_INFO_URL", "FEISHU_SOURCE_URL"),
    )
    resolved_feishu_app_id = resolve_string(
        cli_value=feishu_app_id,
        env_snapshot=env_snapshot,
        env_keys=("FEISHU_APP_ID",),
    )
    resolved_feishu_app_secret = resolve_string(
        cli_value=feishu_app_secret,
        env_snapshot=env_snapshot,
        env_keys=("FEISHU_APP_SECRET",),
    )
    resolved_feishu_base_url = resolve_string(
        cli_value=feishu_base_url,
        env_snapshot=env_snapshot,
        env_keys=("FEISHU_OPEN_BASE_URL",),
        default="https://open.feishu.cn",
    )
    resolved_timeout_seconds = resolve_string(
        cli_value=timeout_seconds if float(timeout_seconds or 0.0) > 0 else "",
        env_snapshot=env_snapshot,
        env_keys=("TIMEOUT_SECONDS",),
        default="30",
    )
    resolved_imap_host = resolve_string(
        cli_value=imap_host,
        env_snapshot=env_snapshot,
        env_keys=("IMAP_HOST",),
        default="imap.qq.com",
    )
    resolved_imap_port = resolve_string(
        cli_value=imap_port if int(imap_port or 0) > 0 else "",
        env_snapshot=env_snapshot,
        env_keys=("IMAP_PORT",),
        default="993",
    )
    resolved_brand_keyword = resolve_string(
        cli_value=brand_keyword,
        env_snapshot=env_snapshot,
        default=task_name,
        derived_from="task_name",
    )
    return (
        {
            "env_file": env_snapshot.source,
            "task_upload_url": source_record(resolved_task_upload_url),
            "employee_info_url": source_record(resolved_employee_info_url),
            "feishu_app_id": source_record(resolved_feishu_app_id, sensitive=True),
            "feishu_app_secret": source_record(resolved_feishu_app_secret, sensitive=True),
            "feishu_base_url": source_record(resolved_feishu_base_url),
            "timeout_seconds": source_record(resolved_timeout_seconds),
            "imap_host": source_record(resolved_imap_host),
            "imap_port": source_record(resolved_imap_port),
            "matching_strategy": "cli" if str(matching_strategy or "").strip() else "default",
            "brand_keyword": source_record(resolved_brand_keyword),
            "mail_limit": "cli" if int(mail_limit or 0) > 0 else "default",
            "mail_workers": "cli" if int(mail_workers or 1) != 1 else "default",
            "sent_since": "cli" if str(sent_since or "").strip() else "default",
            "reset_state": "cli" if bool(reset_state) else "default",
            "stop_after": "cli" if str(stop_after or "").strip() else "default",
            "reuse_existing": "cli" if not bool(reuse_existing) else "default",
            "task_download_dir": task_download_dir_source,
            "mail_data_dir": mail_data_dir_source,
        },
        {
            "env_snapshot": env_snapshot,
            "task_upload_url": resolved_task_upload_url,
            "employee_info_url": resolved_employee_info_url,
            "feishu_app_id": resolved_feishu_app_id,
            "feishu_app_secret": resolved_feishu_app_secret,
        },
    )


def resolve_keep_list_downstream_config(
    *,
    env_file: str | Path,
    keep_workbook: Path,
    template_workbook: Path | None,
    task_name: str,
    task_upload_url: str,
    platform_filters: list[str] | None,
    vision_provider: str,
    max_identifiers_per_platform: int,
    poll_interval: float,
    probe_vision_provider_only: bool,
    skip_scrape: bool,
    skip_visual: bool,
    skip_positioning_card_analysis: bool,
    output_root_source: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    env_snapshot = load_env_file_snapshot(env_file)
    resolved_task_upload_url = resolve_string(
        cli_value=task_upload_url,
        env_snapshot=env_snapshot,
        env_keys=("TASK_UPLOAD_URL", "FEISHU_SOURCE_URL"),
    )
    return (
        {
            "env_file": env_snapshot.source,
            "keep_workbook": "cli_or_default",
            "template_workbook": "cli" if template_workbook is not None else "task_upload_or_none",
            "task_name": "cli" if str(task_name or "").strip() else "default",
            "task_upload_url": source_record(resolved_task_upload_url),
            "platform_filters": "cli" if platform_filters else "default",
            "vision_provider": "cli" if str(vision_provider or "").strip() else "default",
            "max_identifiers_per_platform": "cli" if int(max_identifiers_per_platform or 0) > 0 else "default",
            "poll_interval": "cli" if float(poll_interval or 5.0) != 5.0 else "default",
            "probe_vision_provider_only": "cli" if bool(probe_vision_provider_only) else "default",
            "skip_scrape": "cli" if bool(skip_scrape) else "default",
            "skip_visual": "cli" if bool(skip_visual) else "default",
            "skip_positioning_card_analysis": "cli" if bool(skip_positioning_card_analysis) else "default",
            "output_root": output_root_source,
        },
        {"env_snapshot": env_snapshot, "task_upload_url": resolved_task_upload_url},
    )


def resolve_operator_launch_config(
    *,
    env_file: str | Path,
    task_upload_url: str,
    employee_info_url: str,
    matching_strategy: str,
    brand_keyword: str,
    platforms: list[str] | None,
    vision_provider: str,
    max_identifiers_per_platform: int,
    mail_limit: int,
    sent_since: str,
    reuse_existing: bool,
    probe_vision_provider_only: bool,
    skip_scrape: bool,
    skip_visual: bool,
    skip_positioning_card_analysis: bool,
) -> tuple[dict[str, Any], EnvFileSnapshot]:
    env_snapshot = load_env_file_snapshot(env_file)
    resolved_task_upload_url = resolve_string(
        cli_value=task_upload_url,
        env_snapshot=env_snapshot,
        env_keys=("TASK_UPLOAD_URL", "FEISHU_SOURCE_URL"),
    )
    resolved_employee_info_url = resolve_string(
        cli_value=employee_info_url,
        env_snapshot=env_snapshot,
        env_keys=("EMPLOYEE_INFO_URL", "FEISHU_SOURCE_URL"),
    )
    return (
        {
            "env_file": env_snapshot.source,
            "task_upload_url": source_record(resolved_task_upload_url),
            "employee_info_url": source_record(resolved_employee_info_url),
            "matching_strategy": "cli" if str(matching_strategy or "").strip() else "default",
            "brand_keyword": "cli" if str(brand_keyword or "").strip() else "default",
            "platforms": "cli" if platforms else "default",
            "vision_provider": "cli" if str(vision_provider or "").strip() else "default",
            "max_identifiers_per_platform": "cli" if int(max_identifiers_per_platform or 0) > 0 else "default",
            "mail_limit": "cli" if int(mail_limit or 0) > 0 else "default",
            "sent_since": "cli" if str(sent_since or "").strip() else "default",
            "reuse_existing": "default" if bool(reuse_existing) else "cli",
            "probe_vision_provider_only": "cli" if bool(probe_vision_provider_only) else "default",
            "skip_scrape": "cli" if bool(skip_scrape) else "default",
            "skip_visual": "cli" if bool(skip_visual) else "default",
            "skip_positioning_card_analysis": "cli" if bool(skip_positioning_card_analysis) else "default",
        },
        {
            "env_snapshot": env_snapshot,
            "task_upload_url": resolved_task_upload_url,
            "employee_info_url": resolved_employee_info_url,
        },
    )


def resolve_operator_task_candidates_config(
    *,
    env_file: str | Path,
    task_upload_url: str,
    employee_info_url: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    env_snapshot = load_env_file_snapshot(env_file)
    resolved_task_upload_url = resolve_string(
        cli_value=task_upload_url,
        env_snapshot=env_snapshot,
        env_keys=("TASK_UPLOAD_URL", "FEISHU_SOURCE_URL"),
    )
    resolved_employee_info_url = resolve_string(
        cli_value=employee_info_url,
        env_snapshot=env_snapshot,
        env_keys=("EMPLOYEE_INFO_URL", "FEISHU_SOURCE_URL"),
    )
    resolved_feishu_app_id = resolve_string(
        cli_value="",
        env_snapshot=env_snapshot,
        env_keys=("FEISHU_APP_ID",),
    )
    resolved_feishu_app_secret = resolve_string(
        cli_value="",
        env_snapshot=env_snapshot,
        env_keys=("FEISHU_APP_SECRET",),
    )
    resolved_feishu_base_url = resolve_string(
        cli_value="",
        env_snapshot=env_snapshot,
        env_keys=("FEISHU_OPEN_BASE_URL",),
        default="https://open.feishu.cn",
    )
    resolved_timeout_seconds = resolve_string(
        cli_value="",
        env_snapshot=env_snapshot,
        env_keys=("TIMEOUT_SECONDS",),
        default="30",
    )
    return (
        {
            "env_file": env_snapshot.source,
            "task_upload_url": source_record(resolved_task_upload_url),
            "employee_info_url": source_record(resolved_employee_info_url),
            "feishu_app_id": source_record(resolved_feishu_app_id, sensitive=True),
            "feishu_app_secret": source_record(resolved_feishu_app_secret, sensitive=True),
            "feishu_base_url": source_record(resolved_feishu_base_url),
            "timeout_seconds": source_record(resolved_timeout_seconds),
        },
        {
            "env_snapshot": env_snapshot,
            "task_upload_url": resolved_task_upload_url,
            "employee_info_url": resolved_employee_info_url,
            "feishu_app_id": resolved_feishu_app_id,
            "feishu_app_secret": resolved_feishu_app_secret,
            "feishu_base_url": resolved_feishu_base_url,
            "timeout_seconds": resolved_timeout_seconds,
        },
    )
