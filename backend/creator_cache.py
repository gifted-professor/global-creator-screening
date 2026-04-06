from __future__ import annotations

import json
import os
import sqlite3
import hashlib
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from backend import screening


BASE_DIR = Path(__file__).resolve().parents[1]
DEFAULT_CREATOR_CACHE_DB_PATH = str(
    (BASE_DIR / "temp" / "creator_cache" / "creator_cache.db").resolve()
)
VISUAL_CACHE_TABLE_NAME = "creator_visual_cache_v2"
POSITIONING_CACHE_TABLE_NAME = "creator_positioning_cache_v1"


def _parse_boolish(value: Any, default: bool) -> bool:
    if value in (None, ""):
        return bool(default)
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() not in {"0", "false", "no", "off"}


def resolve_creator_cache_db_path(payload: dict[str, Any] | None = None) -> Path:
    raw = str(
        (payload or {}).get("creator_cache_db_path")
        or os.getenv("CREATOR_CACHE_DB_PATH", DEFAULT_CREATOR_CACHE_DB_PATH)
        or DEFAULT_CREATOR_CACHE_DB_PATH
    ).strip()
    return Path(raw).expanduser().resolve()


def creator_cache_enabled(payload: dict[str, Any] | None = None) -> bool:
    if "use_creator_cache" in (payload or {}):
        return _parse_boolish((payload or {}).get("use_creator_cache"), True)
    return _parse_boolish(os.getenv("CREATOR_CACHE_ENABLED", "1"), True)


def creator_cache_force_refresh(payload: dict[str, Any] | None = None) -> bool:
    for key in ("force_refresh_creator_cache", "refresh_creator_cache", "force_refresh_cache"):
        if key in (payload or {}):
            return _parse_boolish((payload or {}).get(key), False)
    return _parse_boolish(os.getenv("FORCE_REFRESH_CREATOR_CACHE", "0"), False)


def stable_cache_key(payload: dict[str, Any] | list[Any] | str | None) -> str:
    if isinstance(payload, str):
        text = payload
    else:
        text = json.dumps(payload or {}, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


@contextmanager
def creator_cache_connection(db_path: str | Path) -> Iterator[sqlite3.Connection]:
    resolved = Path(db_path).expanduser().resolve()
    resolved.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(resolved)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS creator_scrape_cache (
                platform TEXT NOT NULL,
                identifier TEXT NOT NULL,
                raw_items_json TEXT NOT NULL,
                item_count INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (platform, identifier)
            );

            CREATE TABLE IF NOT EXISTS creator_visual_cache (
                platform TEXT NOT NULL,
                identifier TEXT NOT NULL,
                visual_result_json TEXT NOT NULL,
                decision TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL,
                PRIMARY KEY (platform, identifier)
            );

            CREATE TABLE IF NOT EXISTS creator_visual_cache_v2 (
                platform TEXT NOT NULL,
                identifier TEXT NOT NULL,
                context_key TEXT NOT NULL,
                context_json TEXT NOT NULL DEFAULT '{}',
                visual_result_json TEXT NOT NULL,
                decision TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL,
                PRIMARY KEY (platform, identifier, context_key)
            );

            CREATE INDEX IF NOT EXISTS idx_creator_visual_cache_v2_lookup
            ON creator_visual_cache_v2(platform, context_key, identifier);

            CREATE TABLE IF NOT EXISTS creator_positioning_cache_v1 (
                platform TEXT NOT NULL,
                identifier TEXT NOT NULL,
                context_key TEXT NOT NULL,
                context_json TEXT NOT NULL DEFAULT '{}',
                positioning_result_json TEXT NOT NULL,
                fit_recommendation TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL,
                PRIMARY KEY (platform, identifier, context_key)
            );

            CREATE INDEX IF NOT EXISTS idx_creator_positioning_cache_v1_lookup
            ON creator_positioning_cache_v1(platform, context_key, identifier);
            """
        )
        yield conn
        conn.commit()
    finally:
        conn.close()


def _resolve_scraped_item_identifier(platform: str, item: dict[str, Any]) -> str:
    normalized_platform = str(platform or "").strip().lower()
    if normalized_platform == "tiktok":
        author_meta = item.get("authorMeta") or {}
        candidate = (
            author_meta.get("profileUrl")
            or author_meta.get("name")
            or item.get("url")
            or item.get("input")
            or item.get("webVideoUrl")
        )
        return screening.extract_platform_identifier(normalized_platform, candidate)
    if normalized_platform == "instagram":
        candidate = item.get("url") or item.get("username")
        return screening.extract_platform_identifier(normalized_platform, candidate)
    if normalized_platform == "youtube":
        candidate = (
            item.get("inputChannelUrl")
            or item.get("input")
            or item.get("channelUsername")
            or item.get("channelUrl")
            or item.get("channelName")
            or ((item.get("aboutChannelInfo") or {}).get("channelUrl"))
            or ((item.get("aboutChannelInfo") or {}).get("inputChannelUrl"))
            or ((item.get("aboutChannelInfo") or {}).get("channelUsername"))
        )
        return screening.extract_platform_identifier(normalized_platform, candidate)
    return ""


def _normalize_requested_identifiers(platform: str, identifiers: list[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for item in identifiers or []:
        identifier = screening.extract_platform_identifier(platform, item)
        if not identifier or identifier in seen:
            continue
        seen.add(identifier)
        normalized.append(identifier)
    return normalized


def group_scrape_items_by_identifier(platform: str, items: list[dict[str, Any]] | None) -> dict[str, list[dict[str, Any]]]:
    normalized_platform = str(platform or "").strip().lower()
    grouped: dict[str, list[dict[str, Any]]] = {}
    for item in items or []:
        if not isinstance(item, dict):
            continue
        identifier = _resolve_scraped_item_identifier(normalized_platform, item)
        if not identifier:
            continue
        if normalized_platform == "instagram":
            grouped[identifier] = [dict(item)]
        else:
            grouped.setdefault(identifier, []).append(dict(item))
    return grouped


def load_scrape_cache_entries(
    platform: str,
    identifiers: list[str],
    db_path: str | Path,
) -> dict[str, list[dict[str, Any]]]:
    normalized_identifiers = _normalize_requested_identifiers(platform, identifiers)
    if not normalized_identifiers:
        return {}
    placeholders = ",".join("?" for _ in normalized_identifiers)
    query = (
        "SELECT identifier, raw_items_json FROM creator_scrape_cache "
        f"WHERE platform = ? AND identifier IN ({placeholders})"
    )
    rows: dict[str, list[dict[str, Any]]] = {}
    with creator_cache_connection(db_path) as conn:
        cursor = conn.execute(query, [str(platform or "").strip().lower(), *normalized_identifiers])
        for identifier, raw_items_json in cursor.fetchall():
            try:
                payload = json.loads(raw_items_json or "[]")
            except Exception:
                continue
            if isinstance(payload, list) and payload:
                rows[str(identifier or "").strip().lower()] = [item for item in payload if isinstance(item, dict)]
    return rows


def persist_scrape_cache_entries(
    platform: str,
    items: list[dict[str, Any]] | None,
    db_path: str | Path,
    *,
    updated_at: str,
) -> int:
    grouped = group_scrape_items_by_identifier(platform, items)
    if not grouped:
        return 0
    normalized_platform = str(platform or "").strip().lower()
    with creator_cache_connection(db_path) as conn:
        conn.executemany(
            """
            INSERT INTO creator_scrape_cache(platform, identifier, raw_items_json, item_count, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(platform, identifier) DO UPDATE SET
                raw_items_json = excluded.raw_items_json,
                item_count = excluded.item_count,
                updated_at = excluded.updated_at
            """,
            [
                (
                    normalized_platform,
                    identifier,
                    json.dumps(raw_items, ensure_ascii=False),
                    len(raw_items),
                    str(updated_at or "").strip(),
                )
                for identifier, raw_items in grouped.items()
            ],
        )
    return len(grouped)


def is_cacheable_visual_result(result: dict[str, Any] | None) -> bool:
    if not isinstance(result, dict):
        return False
    if result.get("success") is False:
        return False
    return bool(str(result.get("decision") or "").strip())


def is_cacheable_positioning_result(result: dict[str, Any] | None) -> bool:
    if not isinstance(result, dict):
        return False
    if result.get("success") is False:
        return False
    if str(result.get("fit_recommendation") or "").strip():
        return True
    return bool(
        list(result.get("positioning_labels") or [])
        or str(result.get("fit_summary") or "").strip()
        or list(result.get("evidence_signals") or [])
    )


def load_visual_cache_entries(
    platform: str,
    identifiers: list[str],
    db_path: str | Path,
    context_key: str,
) -> dict[str, dict[str, Any]]:
    normalized_identifiers = _normalize_requested_identifiers(platform, identifiers)
    normalized_context_key = str(context_key or "").strip()
    if not normalized_identifiers or not normalized_context_key:
        return {}
    placeholders = ",".join("?" for _ in normalized_identifiers)
    query = (
        f"SELECT identifier, visual_result_json FROM {VISUAL_CACHE_TABLE_NAME} "
        f"WHERE platform = ? AND context_key = ? AND identifier IN ({placeholders})"
    )
    rows: dict[str, dict[str, Any]] = {}
    with creator_cache_connection(db_path) as conn:
        cursor = conn.execute(query, [str(platform or "").strip().lower(), normalized_context_key, *normalized_identifiers])
        for identifier, visual_result_json in cursor.fetchall():
            try:
                payload = json.loads(visual_result_json or "{}")
            except Exception:
                continue
            if is_cacheable_visual_result(payload):
                rows[str(identifier or "").strip().lower()] = dict(payload)
    return rows


def persist_visual_cache_entry(
    platform: str,
    identifier: str,
    visual_result: dict[str, Any] | None,
    db_path: str | Path,
    *,
    updated_at: str,
    context_key: str,
    context_payload: dict[str, Any] | None = None,
) -> bool:
    normalized_identifier = screening.normalize_identifier(identifier)
    normalized_context_key = str(context_key or "").strip()
    if not normalized_identifier or not normalized_context_key or not is_cacheable_visual_result(visual_result):
        return False
    with creator_cache_connection(db_path) as conn:
        conn.execute(
            """
            INSERT INTO creator_visual_cache_v2(platform, identifier, context_key, context_json, visual_result_json, decision, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(platform, identifier, context_key) DO UPDATE SET
                context_json = excluded.context_json,
                visual_result_json = excluded.visual_result_json,
                decision = excluded.decision,
                updated_at = excluded.updated_at
            """,
            (
                str(platform or "").strip().lower(),
                normalized_identifier,
                normalized_context_key,
                json.dumps(context_payload or {}, ensure_ascii=False),
                json.dumps(dict(visual_result or {}), ensure_ascii=False),
                str((visual_result or {}).get("decision") or "").strip(),
                str(updated_at or "").strip(),
            ),
        )
    return True


def load_positioning_cache_entries(
    platform: str,
    identifiers: list[str],
    db_path: str | Path,
    context_key: str,
) -> dict[str, dict[str, Any]]:
    normalized_identifiers = _normalize_requested_identifiers(platform, identifiers)
    normalized_context_key = str(context_key or "").strip()
    if not normalized_identifiers or not normalized_context_key:
        return {}
    placeholders = ",".join("?" for _ in normalized_identifiers)
    query = (
        f"SELECT identifier, positioning_result_json FROM {POSITIONING_CACHE_TABLE_NAME} "
        f"WHERE platform = ? AND context_key = ? AND identifier IN ({placeholders})"
    )
    rows: dict[str, dict[str, Any]] = {}
    with creator_cache_connection(db_path) as conn:
        cursor = conn.execute(query, [str(platform or "").strip().lower(), normalized_context_key, *normalized_identifiers])
        for identifier, positioning_result_json in cursor.fetchall():
            try:
                payload = json.loads(positioning_result_json or "{}")
            except Exception:
                continue
            if is_cacheable_positioning_result(payload):
                rows[str(identifier or "").strip().lower()] = dict(payload)
    return rows


def persist_positioning_cache_entry(
    platform: str,
    identifier: str,
    positioning_result: dict[str, Any] | None,
    db_path: str | Path,
    *,
    updated_at: str,
    context_key: str,
    context_payload: dict[str, Any] | None = None,
) -> bool:
    normalized_identifier = screening.normalize_identifier(identifier)
    normalized_context_key = str(context_key or "").strip()
    if not normalized_identifier or not normalized_context_key or not is_cacheable_positioning_result(positioning_result):
        return False
    with creator_cache_connection(db_path) as conn:
        conn.execute(
            f"""
            INSERT INTO {POSITIONING_CACHE_TABLE_NAME}(platform, identifier, context_key, context_json, positioning_result_json, fit_recommendation, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(platform, identifier, context_key) DO UPDATE SET
                context_json = excluded.context_json,
                positioning_result_json = excluded.positioning_result_json,
                fit_recommendation = excluded.fit_recommendation,
                updated_at = excluded.updated_at
            """,
            (
                str(platform or "").strip().lower(),
                normalized_identifier,
                normalized_context_key,
                json.dumps(context_payload or {}, ensure_ascii=False),
                json.dumps(dict(positioning_result or {}), ensure_ascii=False),
                str((positioning_result or {}).get("fit_recommendation") or "").strip(),
                str(updated_at or "").strip(),
            ),
        )
    return True
