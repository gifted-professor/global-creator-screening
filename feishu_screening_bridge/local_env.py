from __future__ import annotations

from pathlib import Path


def load_local_env(env_file: str | Path) -> dict[str, str]:
    path = Path(env_file).expanduser()
    if not path.exists():
        return {}

    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def get_preferred_value(cli_value: object, env_values: dict[str, str], env_key: str, default: str = "") -> str:
    candidate = str(cli_value or "").strip()
    if candidate:
        return candidate
    env_candidate = str(env_values.get(env_key, "") or "").strip()
    if env_candidate:
        return env_candidate
    return default
