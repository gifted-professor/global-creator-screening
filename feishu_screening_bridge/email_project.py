from __future__ import annotations

from dataclasses import dataclass
import importlib
from contextlib import contextmanager
from functools import lru_cache
from pathlib import Path
import sys
from typing import Any, Callable


DEFAULT_EMAIL_PROJECT_ROOT = Path("/Users/a1234/Desktop/Coding/网红/email")


class EmailProjectImportError(RuntimeError):
    pass


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


def resolve_email_project_root(email_project_root: str | Path | None = None) -> Path:
    root = Path(email_project_root or DEFAULT_EMAIL_PROJECT_ROOT).expanduser()
    if not root.exists():
        raise EmailProjectImportError(f"email 项目目录不存在: {root}")
    if not (root / "email_sync").exists():
        raise EmailProjectImportError(f"未找到 email_sync 包目录: {root / 'email_sync'}")
    return root.resolve()


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
        raise EmailProjectImportError(f"加载 email 项目模块失败: {root}") from exc
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
