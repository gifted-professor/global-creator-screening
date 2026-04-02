from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from harness.failures import build_failure_payload


def build_preflight_error(
    *,
    error_code: str,
    message: str,
    remediation: str,
    stage: str = "preflight",
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return build_failure_payload(
        stage=stage,
        error_code=error_code,
        message=message,
        remediation=remediation,
        failure_layer="preflight",
        details=details,
    )


def build_preflight_payload(
    *,
    checks: dict[str, Any],
    errors: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    normalized_errors = list(errors or [])
    return {
        **checks,
        "ready": not normalized_errors,
        "errors": normalized_errors,
    }


def inspect_directory_materialization_target(path: Path) -> dict[str, Any]:
    resolved_path = path.expanduser().resolve()
    if resolved_path.exists():
        return {
            "path": str(resolved_path),
            "exists": True,
            "is_dir": resolved_path.is_dir(),
            "nearest_existing_parent": str(resolved_path),
            "materializable": resolved_path.is_dir() and os.access(resolved_path, os.W_OK | os.X_OK),
        }
    current = resolved_path.parent
    nearest_existing_parent: Path | None = None
    while True:
        if current.exists():
            nearest_existing_parent = current
            break
        if current == current.parent:
            break
        current = current.parent
    return {
        "path": str(resolved_path),
        "exists": False,
        "is_dir": False,
        "nearest_existing_parent": str(nearest_existing_parent) if nearest_existing_parent is not None else "",
        "materializable": bool(
            nearest_existing_parent is not None
            and nearest_existing_parent.is_dir()
            and os.access(nearest_existing_parent, os.W_OK | os.X_OK)
        ),
    }
