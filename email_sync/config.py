from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional


def _load_dotenv(env_path: Path) -> Dict[str, str]:
    values: Dict[str, str] = {}
    if not env_path.exists():
        return values

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def _get_value(key: str, dotenv_values: Dict[str, str], default: Optional[str] = None) -> Optional[str]:
    if key in os.environ:
        return os.environ[key]
    return dotenv_values.get(key, default)


def _split_csv(value: Optional[str]) -> Optional[List[str]]:
    if not value:
        return None
    parts = [item.strip() for item in value.split(",")]
    result = [item for item in parts if item]
    return result or None


@dataclass
class Settings:
    account_email: str
    auth_code: str
    imap_host: str
    imap_port: int
    data_dir: Path
    db_path: Path
    raw_dir: Path
    mail_folders: Optional[List[str]]
    readonly: bool = True

    @classmethod
    def from_environment(cls, env_path: str = ".env", require_credentials: bool = True) -> "Settings":
        dotenv_values = _load_dotenv(Path(env_path))

        account_email = (_get_value("EMAIL_ACCOUNT", dotenv_values, "") or "").strip()
        auth_code = (_get_value("EMAIL_AUTH_CODE", dotenv_values, "") or "").strip()
        imap_host = (_get_value("IMAP_HOST", dotenv_values, "imap.qq.com") or "imap.qq.com").strip()
        imap_port = int(_get_value("IMAP_PORT", dotenv_values, "993") or "993")
        data_dir = Path(_get_value("DATA_DIR", dotenv_values, "./data") or "./data").expanduser()
        db_path_raw = _get_value("DB_PATH", dotenv_values)
        raw_dir_raw = _get_value("RAW_DIR", dotenv_values)
        mail_folders = _split_csv(_get_value("MAIL_FOLDERS", dotenv_values))

        settings = cls(
            account_email=account_email,
            auth_code=auth_code,
            imap_host=imap_host,
            imap_port=imap_port,
            data_dir=data_dir,
            db_path=Path(db_path_raw).expanduser() if db_path_raw else data_dir / "email_sync.db",
            raw_dir=Path(raw_dir_raw).expanduser() if raw_dir_raw else data_dir / "raw",
            mail_folders=mail_folders,
        )
        settings.validate(require_credentials=require_credentials)
        return settings

    def validate(self, require_credentials: bool = True) -> None:
        if require_credentials:
            if not self.account_email:
                raise ValueError("缺少 EMAIL_ACCOUNT，请在 .env 或环境变量里设置邮箱地址。")
            if "@" not in self.account_email:
                raise ValueError("EMAIL_ACCOUNT 格式不正确，应该是完整邮箱地址。")
            if not self.auth_code:
                raise ValueError("缺少 EMAIL_AUTH_CODE，请使用邮箱 IMAP 授权码。")
        if self.imap_port <= 0:
            raise ValueError("IMAP_PORT 必须是大于 0 的整数。")

    def ensure_directories(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.raw_dir.mkdir(parents=True, exist_ok=True)
