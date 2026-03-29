from __future__ import annotations

from dataclasses import dataclass
import importlib
from contextlib import contextmanager
from functools import lru_cache
from pathlib import Path
import sys
from typing import Any, Callable


REPO_LOCAL_UPSTREAM_RUNNER = "scripts/run_task_upload_to_keep_list_pipeline.py"
REPO_LOCAL_DOWNSTREAM_RUNNER = "scripts/run_keep_list_screening_pipeline.py"
REPO_LOCAL_FINAL_RUNNER = "scripts/run_task_upload_to_final_export_pipeline.py"


class EmailProjectImportError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        error_code: str = "EMAIL_PROJECT_IMPORT_FAILED",
        diagnostic: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.error_code = error_code
        self.diagnostic = diagnostic or {}


@dataclass(frozen=True)
class EmailProjectModules:
    Settings: type[Any]
    Database: type[Any]
    RequirementRecord: type[Any]
    ScreeningWorkbookImportError: type[Any]
    build_screening_workbook_upload_bridge_payload: Callable[..., dict[str, Any]]
    compile_screening_workbook: Callable[..., Any]
    ensure_project: Callable[..., int]
    export_dashboard: Callable[..., Any]
    import_requirements: Callable[..., Any]
    init_creator_ops_schema: Callable[..., Any]
    init_influencer_schema: Callable[..., Any]
    parse_requirement_row: Callable[..., Any]
    rebuild_project_home_read_model: Callable[..., Any]
    rebuild_project_workbench_read_model: Callable[..., Any]


def _legacy_email_project_remediation() -> str:
    return (
        "当前仓库默认主线入口依次是 "
        f"`{REPO_LOCAL_UPSTREAM_RUNNER}`、`{REPO_LOCAL_DOWNSTREAM_RUNNER}`、`{REPO_LOCAL_FINAL_RUNNER}`；"
        "只有在明确需要兼容旧 bridge 命令时，才通过 `--email-project-root` 或 `EMAIL_PROJECT_ROOT` 显式指向外部 full `email` 项目。"
    )


def inspect_email_project_dependency(
    email_project_root: str | Path | None = None,
    env_file: str | Path | None = None,
    *,
    validate_import: bool = False,
) -> dict[str, Any]:
    requested_root = str(email_project_root or "").strip()
    requested_env_file = str(env_file or ".env").strip()
    diagnostic: dict[str, Any] = {
        "dependency_kind": "external_full_email_project",
        "available": False,
        "legacy_mode_requested": bool(requested_root),
        "default_root": "",
        "resolved_root": "",
        "root_exists": False,
        "email_sync_package_dir": "",
        "email_sync_package_exists": False,
        "email_env_file": requested_env_file,
        "email_env_file_exists": False,
        "uses_default_root": False,
        "repo_local_entrypoints": [
            REPO_LOCAL_UPSTREAM_RUNNER,
            REPO_LOCAL_DOWNSTREAM_RUNNER,
            REPO_LOCAL_FINAL_RUNNER,
        ],
        "error_code": "",
        "message": "",
        "remediation": _legacy_email_project_remediation(),
    }
    if not requested_root:
        diagnostic["error_code"] = "EMAIL_PROJECT_ROOT_NOT_PROVIDED"
        diagnostic["message"] = (
            "legacy bridge 命令不再隐式依赖外部 full `email` 项目；"
            "如需兼容旧链路，请显式提供 `--email-project-root` 或 `EMAIL_PROJECT_ROOT`。"
        )
        return diagnostic
    resolved_root = Path(requested_root).expanduser().resolve()
    package_dir = resolved_root / "email_sync"
    resolved_env_file = resolve_email_env_file(resolved_root, env_file)
    diagnostic["resolved_root"] = str(resolved_root)
    diagnostic["root_exists"] = resolved_root.exists()
    diagnostic["email_sync_package_dir"] = str(package_dir)
    diagnostic["email_sync_package_exists"] = package_dir.exists()
    diagnostic["email_env_file"] = str(resolved_env_file)
    diagnostic["email_env_file_exists"] = resolved_env_file.exists()
    if not diagnostic["root_exists"]:
        diagnostic["error_code"] = "EMAIL_PROJECT_ROOT_MISSING"
        diagnostic["message"] = f"legacy bridge 依赖的外部 email 项目目录不存在: {resolved_root}"
        return diagnostic
    if not diagnostic["email_sync_package_exists"]:
        diagnostic["error_code"] = "EMAIL_PROJECT_PACKAGE_MISSING"
        diagnostic["message"] = f"legacy bridge 指向的目录缺少 email_sync 包: {package_dir}"
        return diagnostic
    if validate_import:
        try:
            _load_email_project_cached(str(resolved_root))
        except EmailProjectImportError as exc:
            diagnostic["error_code"] = exc.error_code or "EMAIL_PROJECT_IMPORT_FAILED"
            diagnostic["message"] = str(exc)
            diagnostic["import_error"] = str(exc.__cause__ or exc)
            return diagnostic
        except Exception as exc:  # noqa: BLE001
            diagnostic["error_code"] = "EMAIL_PROJECT_IMPORT_FAILED"
            diagnostic["message"] = f"加载 legacy bridge 所需的外部 email 项目模块失败: {resolved_root}"
            diagnostic["import_error"] = str(exc)
            return diagnostic
    diagnostic["available"] = True
    diagnostic["message"] = f"legacy bridge 外部 email 项目依赖已就绪: {resolved_root}"
    return diagnostic


def resolve_email_project_root(email_project_root: str | Path | None = None) -> Path:
    diagnostic = inspect_email_project_dependency(email_project_root, validate_import=False)
    if not diagnostic["root_exists"] or not diagnostic["email_sync_package_exists"]:
        raise EmailProjectImportError(
            diagnostic["message"],
            error_code=diagnostic["error_code"] or "EMAIL_PROJECT_IMPORT_FAILED",
            diagnostic=diagnostic,
        )
    return Path(diagnostic["resolved_root"])


def resolve_email_env_file(email_project_root: Path, env_file: str | Path | None = None) -> Path:
    candidate = Path(env_file or ".env").expanduser()
    if candidate.is_absolute():
        return candidate
    return email_project_root / candidate


def load_email_project(email_project_root: str | Path | None = None) -> EmailProjectModules:
    root = resolve_email_project_root(email_project_root)
    return _load_email_project_cached(str(root))


def _iter_email_sync_module_names() -> list[str]:
    return [
        name
        for name in sys.modules
        if name == "email_sync" or name.startswith("email_sync.")
    ]


@contextmanager
def _isolated_email_project_import(root: Path):
    root_str = str(root)
    original_sys_path = list(sys.path)
    original_modules = {
        name: sys.modules[name]
        for name in _iter_email_sync_module_names()
    }
    original_dont_write_bytecode = sys.dont_write_bytecode

    sys.path[:] = [root_str, *[entry for entry in sys.path if entry != root_str]]
    for name in _iter_email_sync_module_names():
        sys.modules.pop(name, None)
    sys.dont_write_bytecode = True
    importlib.invalidate_caches()
    try:
        yield
    finally:
        for name in _iter_email_sync_module_names():
            sys.modules.pop(name, None)
        sys.modules.update(original_modules)
        sys.path[:] = original_sys_path
        sys.dont_write_bytecode = original_dont_write_bytecode
        importlib.invalidate_caches()


@lru_cache(maxsize=8)
def _load_email_project_cached(email_project_root: str) -> EmailProjectModules:
    root = Path(email_project_root)
    try:
        with _isolated_email_project_import(root):
            config_module = importlib.import_module("email_sync.config")
            creator_ops_module = importlib.import_module("email_sync.creator_ops")
            creator_ops_schema_module = importlib.import_module("email_sync.creator_ops_schema")
            db_module = importlib.import_module("email_sync.db")
            influencer_ops_module = importlib.import_module("email_sync.influencer_ops")
            project_home_dashboard_module = importlib.import_module("email_sync.project_home_dashboard")
            project_home_module = importlib.import_module("email_sync.project_home")
            bridge_module = importlib.import_module("email_sync.project_home_manual_update_bridge")
            project_requirements_module = importlib.import_module("email_sync.project_requirements")
            project_workbench_module = importlib.import_module("email_sync.project_workbench")
            screening_workbook_import_module = importlib.import_module("email_sync.screening_workbook_import")
    except Exception as exc:  # noqa: BLE001
        diagnostic = inspect_email_project_dependency(root, validate_import=False)
        raise EmailProjectImportError(
            f"加载 legacy bridge 所需的外部 email 项目模块失败: {root}",
            error_code="EMAIL_PROJECT_IMPORT_FAILED",
            diagnostic=diagnostic,
        ) from exc
    return EmailProjectModules(
        Settings=config_module.Settings,
        Database=db_module.Database,
        RequirementRecord=project_requirements_module.RequirementRecord,
        ScreeningWorkbookImportError=screening_workbook_import_module.ScreeningWorkbookImportError,
        build_screening_workbook_upload_bridge_payload=bridge_module.build_screening_workbook_upload_bridge_payload,
        compile_screening_workbook=screening_workbook_import_module.compile_screening_workbook,
        ensure_project=creator_ops_module.ensure_project,
        export_dashboard=project_home_dashboard_module.export_dashboard,
        import_requirements=creator_ops_module.import_requirements,
        init_creator_ops_schema=creator_ops_schema_module.init_schema,
        init_influencer_schema=influencer_ops_module.init_schema,
        parse_requirement_row=project_requirements_module.parse_requirement_row,
        rebuild_project_home_read_model=project_home_module.rebuild_project_home_read_model,
        rebuild_project_workbench_read_model=project_workbench_module.rebuild_project_workbench_read_model,
    )
