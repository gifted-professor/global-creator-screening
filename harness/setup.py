from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Sequence

from harness.failures import build_failure_payload


def build_setup_error(
    *,
    error_code: str,
    message: str,
    remediation: str,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return build_failure_payload(
        stage="setup",
        error_code=error_code,
        message=message,
        remediation=remediation,
        failure_layer="setup",
        details=details,
    )


def build_setup_payload(
    *,
    checks: dict[str, Any],
    errors: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    normalized_errors = list(errors or [])
    return {
        **checks,
        "completed": not normalized_errors,
        "errors": normalized_errors,
    }


def materialize_setup(
    *,
    scope: str,
    directories: Sequence[dict[str, Any]],
    files: Sequence[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    errors: list[dict[str, Any]] = []
    checks: dict[str, Any] = {
        "scope": str(scope or "").strip(),
    }
    for directory in directories:
        label = str(directory["label"])
        path = Path(directory["path"]).expanduser().resolve()
        try:
            path.mkdir(parents=True, exist_ok=True)
        except Exception as exc:  # noqa: BLE001
            errors.append(
                build_setup_error(
                    error_code=str(directory["error_code"]),
                    message=str(directory["message"]).format(path=path),
                    remediation=str(directory["remediation"]),
                    details={
                        "path": str(path),
                        "exception_type": exc.__class__.__name__,
                    },
                )
            )
        checks[f"{label}_ready"] = path.exists() and path.is_dir()
        checks[f"{label}_path"] = str(path)
    if not errors:
        for file_spec in files or []:
            label = str(file_spec["label"])
            path = Path(file_spec["path"]).expanduser().resolve()
            writer = file_spec["writer"]
            try:
                writer(path)
            except Exception as exc:  # noqa: BLE001
                errors.append(
                    build_setup_error(
                        error_code=str(file_spec["error_code"]),
                        message=str(file_spec["message"]).format(path=path),
                        remediation=str(file_spec["remediation"]),
                        details={
                            "path": str(path),
                            "exception_type": exc.__class__.__name__,
                        },
                    )
                )
            checks[f"{label}_ready"] = path.exists()
            checks[f"{label}_path"] = str(path)
    return build_setup_payload(checks=checks, errors=errors)
