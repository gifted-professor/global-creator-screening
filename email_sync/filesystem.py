from __future__ import annotations

import hashlib
import re
from pathlib import Path
from typing import Tuple


def _safe_component(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", value).strip("._")
    cleaned = cleaned or "item"
    digest = hashlib.sha1(value.encode("utf-8")).hexdigest()[:8]
    return f"{cleaned[:48]}-{digest}"


def store_raw_message(
    data_dir: Path,
    raw_dir: Path,
    account_email: str,
    folder_name: str,
    uidvalidity: int,
    uid: int,
    raw_bytes: bytes,
) -> Tuple[str, str, int]:
    account_dir = _safe_component(account_email.lower())
    folder_dir = _safe_component(folder_name)
    target = raw_dir / account_dir / folder_dir / f"{uidvalidity}_{uid}.eml"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(raw_bytes)

    sha256 = hashlib.sha256(raw_bytes).hexdigest()
    relative_path = target.relative_to(data_dir)
    return str(relative_path), sha256, len(raw_bytes)
