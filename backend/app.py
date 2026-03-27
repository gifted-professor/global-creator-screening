import json
import os
import re
import threading
import time
import uuid
import base64
import hashlib
import random
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime
from io import BytesIO
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
import requests
from flask import Flask, jsonify, render_template, request, send_file
from openpyxl import Workbook
from werkzeug.utils import secure_filename

try:
    from flask_cors import CORS
except Exception:  # pragma: no cover - optional dependency for local runtime only
    CORS = None

from backend import rules as rules_module
from backend import screening


BASE_DIR = Path(__file__).resolve().parents[1]


def load_dotenv_local():
    env_path = BASE_DIR / ".env.local"
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


load_dotenv_local()


def parse_env_flag(name, default=False):
    raw_value = os.getenv(name)
    if raw_value is None:
        return bool(default)
    normalized = str(raw_value).strip().lower()
    if not normalized:
        return bool(default)
    return normalized not in {"0", "false", "no", "off"}


DATA_DIR = os.environ.get("SCREENING_DATA_DIR", str(BASE_DIR / "data"))
CONFIG_DIR = str(BASE_DIR / "config")
TEMP_DIR = str(BASE_DIR / "temp")
UPLOAD_FOLDER = str(Path(DATA_DIR) / "uploads")
ACTIVE_RULESPEC_PATH = str(Path(CONFIG_DIR) / "active_rulespec.json")
FIELD_MATCH_REPORT_PATH = str(Path(CONFIG_DIR) / "field_match_report.json")
MISSING_CAPABILITIES_PATH = str(Path(CONFIG_DIR) / "missing_capabilities.json")
REVIEW_NOTES_PATH = str(Path(CONFIG_DIR) / "review_notes.md")
APIFY_API_BASE = "https://api.apify.com/v2"
APIFY_POLL_INTERVAL_SECONDS = int(os.getenv("APIFY_POLL_INTERVAL_SECONDS", "5"))
APIFY_REQUEST_TIMEOUT = int(os.getenv("APIFY_REQUEST_TIMEOUT", "60"))
APIFY_TRANSPORT_MAX_RETRIES = max(1, int(os.getenv("APIFY_TRANSPORT_MAX_RETRIES", "3")))
APIFY_TRANSPORT_RETRY_BACKOFF_SECONDS = max(
    0.5,
    float(os.getenv("APIFY_TRANSPORT_RETRY_BACKOFF_SECONDS", "1.5")),
)
APIFY_TOKEN_POOL_STATE_FILE = str(Path(DATA_DIR) / "apify_token_pool_state.json")
APIFY_BALANCE_CACHE_FILE = str(Path(DATA_DIR) / "apify_balance_cache.json")
APIFY_RUN_GUARDS_FILE = str(Path(DATA_DIR) / "apify_run_guards.json")
APIFY_BALANCE_REFRESH_INTERVAL_SECONDS = max(
    60,
    int(os.getenv("APIFY_BALANCE_REFRESH_INTERVAL_SECONDS", "300")),
)
APIFY_BALANCE_POLLER_ENABLED = parse_env_flag("APIFY_BALANCE_POLLER_ENABLED", default=True)
APIFY_GUARD_TTL_SECONDS = max(60, int(os.getenv("APIFY_GUARD_TTL_SECONDS", "1800")))
APIFY_BUDGET_SAFETY_MULTIPLIER = max(
    1.0,
    float(os.getenv("APIFY_BUDGET_SAFETY_MULTIPLIER", "1.1")),
)
APIFY_BUDGET_BUFFER_USD = max(
    0.0,
    float(os.getenv("APIFY_BUDGET_BUFFER_USD", "0.1")),
)
BACKEND_BIND_HOST = os.getenv("BACKEND_BIND_HOST", "127.0.0.1")
BACKEND_PORT = int(os.getenv("BACKEND_PORT", "5001"))
BACKEND_DEBUG = parse_env_flag("BACKEND_DEBUG", default=False)
BACKEND_USE_RELOADER = parse_env_flag("BACKEND_USE_RELOADER", default=BACKEND_DEBUG)
BACKEND_ALLOWED_ORIGINS = [
    item.strip()
    for item in (os.getenv("BACKEND_ALLOWED_ORIGINS") or "http://127.0.0.1:5173,http://localhost:5173").split(",")
    if item.strip()
]
JOB_TERMINAL_STATUSES = {"completed", "failed", "cancelled"}
PLATFORM_ACTORS = {
    "tiktok": "clockworks/tiktok-profile-scraper",
    "instagram": "apify/instagram-profile-scraper",
    "youtube": "streamers/youtube-scraper",
}
PLATFORM_BATCH_SIZES = {
    "tiktok": int(os.getenv("TIKTOK_BATCH_SIZE", "20")),
    "instagram": int(os.getenv("INSTAGRAM_BATCH_SIZE", "50")),
    "youtube": int(os.getenv("YOUTUBE_BATCH_SIZE", "5")),
}
PLATFORM_ESTIMATED_COST_PER_IDENTIFIER_USD = {
    "tiktok": float(os.getenv("APIFY_TIKTOK_COST_PER_PROFILE_USD", "0.0")),
    "instagram": float(os.getenv("APIFY_INSTAGRAM_COST_PER_PROFILE_USD", "0.0026")),
    "youtube": float(os.getenv("APIFY_YOUTUBE_COST_PER_PROFILE_USD", "0.0")),
}
PLATFORM_ESTIMATED_COST_PER_RESULT_USD = {
    "tiktok": float(os.getenv("APIFY_TIKTOK_COST_PER_RESULT_USD", "0.004")),
    "instagram": float(os.getenv("APIFY_INSTAGRAM_COST_PER_RESULT_USD", "0.0")),
    "youtube": float(os.getenv("APIFY_YOUTUBE_COST_PER_RESULT_USD", "0.004")),
}
INSTAGRAM_ABOUT_ADDON_COST_PER_PROFILE_USD = float(
    os.getenv("APIFY_INSTAGRAM_ABOUT_ADDON_COST_PER_PROFILE_USD", "0.007")
)
TRANSIENT_STATUS_CODES = {429, 500, 502, 503, 504}
UPLOAD_PLATFORM_ALIASES = {
    "tiktok": "tiktok",
    "tik_tok": "tiktok",
    "instagram": "instagram",
    "ig": "instagram",
    "youtube": "youtube",
    "yt": "youtube",
}
UPLOAD_PLATFORM_RESPONSE_LABELS = {
    "tiktok": "TikTok",
    "instagram": "Instagram",
    "youtube": "YouTube",
}
UPLOAD_METADATA_FIELD_ALIASES = {
    "nickname": "nickname",
    "description": "description",
    "region": "region",
    "language": "language",
    "followers": "followers",
    "avgviews": "avg_views",
    "avglikes": "avg_likes",
    "avgcomments": "avg_comments",
    "avgcollects": "avg_collects",
    "tags": "tags",
    "email": "email",
    "url": "url",
}
UPLOAD_METADATA_EXPORT_FIELDS = (
    ("upload_nickname", "nickname"),
    ("upload_handle", "handle"),
    ("upload_region", "region"),
    ("upload_language", "language"),
    ("upload_followers", "followers"),
    ("upload_avg_views", "avg_views"),
    ("upload_avg_likes", "avg_likes"),
    ("upload_avg_comments", "avg_comments"),
    ("upload_avg_collects", "avg_collects"),
)
VISION_REQUEST_TIMEOUT = int(os.getenv("VISION_REQUEST_TIMEOUT", "60"))
VISION_MODEL = os.getenv("VISION_MODEL", "gpt-5.4")
VISION_API_STYLE_RESPONSES = "responses"
VISION_API_STYLE_CHAT_COMPLETIONS = "chat_completions"
VISUAL_REVIEW_REQUEST_COVER_LIMIT = max(1, int(os.getenv("VISUAL_REVIEW_REQUEST_COVER_LIMIT", "9")))
VISUAL_REVIEW_CANDIDATE_COVER_LIMIT_FLOOR = max(
    VISUAL_REVIEW_REQUEST_COVER_LIMIT,
    int(os.getenv("VISUAL_REVIEW_CANDIDATE_COVER_LIMIT_FLOOR", "12")),
)
DEFAULT_VISUAL_REVIEW_MAX_WORKERS = max(1, int(os.getenv("VISUAL_REVIEW_MAX_WORKERS", "6")))
VISUAL_REVIEW_MAX_RETRIES = max(1, int(os.getenv("VISUAL_REVIEW_MAX_RETRIES", "3")))
VISUAL_REVIEW_RETRY_BASE_DELAY_SECONDS = max(
    0.5,
    float(os.getenv("VISUAL_REVIEW_RETRY_BASE_DELAY_SECONDS", "2")),
)
VISUAL_REVIEW_RETRY_MAX_DELAY_SECONDS = max(
    VISUAL_REVIEW_RETRY_BASE_DELAY_SECONDS,
    float(os.getenv("VISUAL_REVIEW_RETRY_MAX_DELAY_SECONDS", "12")),
)
VISUAL_IMAGE_DOWNLOAD_MAX_RETRIES = max(1, int(os.getenv("VISUAL_IMAGE_DOWNLOAD_MAX_RETRIES", "3")))
VISUAL_IMAGE_CACHE_ENABLED = parse_env_flag("VISUAL_IMAGE_CACHE_ENABLED", default=True)
VISUAL_IMAGE_CACHE_DIR = str(os.getenv("VISUAL_IMAGE_CACHE_DIR", "") or "").strip()
VISUAL_REVIEW_RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}
VISION_PROVIDER_CONFIGS = (
    {
        "name": "openai",
        "base_url": os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1"),
        "env_key": "OPENAI_API_KEY",
        "api_style": VISION_API_STYLE_RESPONSES,
        "model": os.getenv("OPENAI_VISION_MODEL", "").strip(),
    },
    {
        "name": "quan2go",
        "base_url": os.getenv("VISION_QUAN2GO_BASE_URL", "https://capi.quan2go.com/openai"),
        "env_key": "VISION_QUAN2GO_API_KEY",
        "api_style": VISION_API_STYLE_RESPONSES,
        "model": os.getenv("VISION_QUAN2GO_MODEL", "").strip(),
    },
    {
        "name": "lemonapi",
        "base_url": os.getenv("VISION_LEMONAPI_BASE_URL", "https://new.lemonapi.site/v1"),
        "env_key": "VISION_LEMONAPI_API_KEY",
        "api_style": VISION_API_STYLE_CHAT_COMPLETIONS,
        "model": os.getenv("VISION_LEMONAPI_MODEL", "").strip(),
    },
)
VISION_PROMPT = """你是达人筛号流程中的视觉复核员。输入是同一位博主最近若干条内容的封面图，请综合全部图片一起判断。

只根据图片本身做初步判断，不要臆测看不到的信息。重点排查：
1. 是否明显过度性感、暴露或带强擦边倾向。
2. 是否存在明显低价平台/竞品合作痕迹，如 Temu、Shein、AliExpress、Wish、TikTok Shop。
3. 是否长期画面杂乱、昏暗、模糊、质感差。
4. 是否高度母婴/晒娃导向，主体大多是婴儿、儿童或孕期内容。
5. 是否整体过度商业化、广告摆拍感很重。
6. 是否存在大面积明显纹身等高风险视觉信号。

如果明显命中以上任一高风险情况，输出 Reject；否则输出 Pass。

请只返回 JSON，不要加 markdown，不要加额外说明，格式固定为：
{"decision":"Pass 或 Reject","reason":"一句中文原因","signals":["最多 3 个简短中文信号"]}"""

JOBS = {}
JOBS_LOCK = threading.Lock()
APIFY_TOKEN_LOCK = threading.Lock()
APIFY_BALANCE_CACHE_LOCK = threading.Lock()
APIFY_BALANCE_REFRESH_LOCK = threading.Lock()
APIFY_BALANCE_POLLER_LOCK = threading.Lock()
APIFY_RUN_GUARDS_LOCK = threading.Lock()
APIFY_BALANCE_POLLER_THREAD = None
APIFY_BALANCE_POLLER_STOP_EVENT = threading.Event()

app = Flask(__name__)
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER
app.config["JSON_AS_ASCII"] = False
if CORS:
    CORS(app, resources={r"/api/*": {"origins": BACKEND_ALLOWED_ORIGINS}})


def ensure_runtime_dirs():
    Path(DATA_DIR).mkdir(parents=True, exist_ok=True)
    Path(CONFIG_DIR).mkdir(parents=True, exist_ok=True)
    Path(TEMP_DIR).mkdir(parents=True, exist_ok=True)
    Path(UPLOAD_FOLDER).mkdir(parents=True, exist_ok=True)
    for platform in PLATFORM_ACTORS:
        Path(get_platform_dir(platform)).mkdir(parents=True, exist_ok=True)


def iso_now():
    return datetime.utcnow().isoformat() + "Z"


def safe_positive_float(value, default=0.0):
    try:
        numeric = float(value)
    except Exception:
        return float(default)
    if numeric < 0:
        return float(default)
    return numeric


def write_json_file(path, payload):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    return path


def load_json_payload(path, default=None):
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as handle:
            raw_text = handle.read().strip()
        if not raw_text:
            return default
        return json.loads(raw_text)
    except Exception:
        return default


def sanitize_json_compatible(value):
    if isinstance(value, dict):
        return {str(key): sanitize_json_compatible(item) for key, item in value.items()}
    if isinstance(value, list):
        return [sanitize_json_compatible(item) for item in value]
    if isinstance(value, tuple):
        return [sanitize_json_compatible(item) for item in value]
    if pd.isna(value):
        return None
    if isinstance(value, (int, float, str, bool)) or value is None:
        return value
    return str(value)


def get_platform_dir(platform):
    return str(Path(DATA_DIR) / platform)


def get_upload_metadata_path(platform):
    return str(Path(get_platform_dir(platform)) / f"{platform}_upload_metadata.json")


def get_raw_data_path(platform):
    return str(Path(get_platform_dir(platform)) / f"{platform}_data.json")


def get_profile_reviews_path(platform):
    return str(Path(get_platform_dir(platform)) / f"{platform}_profile_reviews.json")


def get_visual_results_path(platform):
    return str(Path(get_platform_dir(platform)) / f"{platform}_visual_results.json")


def get_visual_image_cache_dir(platform):
    if VISUAL_IMAGE_CACHE_DIR:
        return str(Path(VISUAL_IMAGE_CACHE_DIR) / platform)
    return str(Path(get_platform_dir(platform)) / "covers")


def load_upload_metadata(platform):
    return load_json_payload(get_upload_metadata_path(platform), default={}) or {}


def save_upload_metadata(platform, metadata_map, replace=False):
    current = {} if replace else load_upload_metadata(platform)
    merged = dict(current)
    merged.update(metadata_map or {})
    write_json_file(get_upload_metadata_path(platform), merged)
    return merged


def load_profile_reviews(platform):
    return load_json_payload(get_profile_reviews_path(platform), default=[]) or []


def save_profile_reviews(platform, profile_reviews):
    return write_json_file(get_profile_reviews_path(platform), profile_reviews or [])


def load_visual_results(platform):
    return load_json_payload(get_visual_results_path(platform), default={}) or {}


def save_visual_results(platform, visual_results):
    return write_json_file(get_visual_results_path(platform), visual_results or {})


def load_apify_token_state():
    return load_json_payload(APIFY_TOKEN_POOL_STATE_FILE, default={}) or {}


def save_apify_token_state(state):
    write_json_file(APIFY_TOKEN_POOL_STATE_FILE, state or {})
    return state or {}


def load_apify_balance_cache():
    return load_json_payload(APIFY_BALANCE_CACHE_FILE, default={}) or {}


def save_apify_balance_cache(cache_payload):
    with APIFY_BALANCE_CACHE_LOCK:
        write_json_file(APIFY_BALANCE_CACHE_FILE, cache_payload or {})
    return cache_payload or {}


def get_apify_token():
    pool = get_apify_token_pool()
    return pool[0] if pool else ""


def mask_apify_token(token):
    cleaned = str(token or "").strip()
    if not cleaned:
        return ""
    if len(cleaned) <= 8:
        return cleaned[:2] + "***"
    return f"{cleaned[:4]}...{cleaned[-4:]}"


def sanitize_apify_error_text(text, tokens=None):
    sanitized = str(text or "")
    for token in tokens or []:
        cleaned = str(token or "").strip()
        if not cleaned:
            continue
        sanitized = sanitized.replace(cleaned, mask_apify_token(cleaned))
    sanitized = re.sub(r"""(token=)([^&\s)"']+)""", r"\1***", sanitized)
    return sanitized


def split_apify_token_list(value):
    return [
        item.strip()
        for item in re.split(r"[\s,]+", str(value or "").strip())
        if item.strip()
    ]


def get_apify_token_pool():
    seen = set()
    pool = []
    for source in (
        os.getenv("APIFY_TOKEN"),
        os.getenv("APIFY_API_TOKEN"),
        os.getenv("APIFY_BACKUP_TOKENS"),
        os.getenv("APIFY_FREE_TOKENS"),
    ):
        for token in split_apify_token_list(source):
            if token in seen:
                continue
            seen.add(token)
            pool.append(token)
    auth_payload = load_json_payload(str(Path.home() / ".apify" / "auth.json"), default={}) or {}
    auth_token = str(auth_payload.get("token") or "").strip()
    if auth_token and auth_token not in seen:
        pool.append(auth_token)
    return pool


def load_apify_run_guards():
    return load_json_payload(APIFY_RUN_GUARDS_FILE, default={}) or {}


def save_apify_run_guards(guards):
    with APIFY_RUN_GUARDS_LOCK:
        write_json_file(APIFY_RUN_GUARDS_FILE, guards or {})
    return guards or {}


def purge_expired_apify_run_guards(guards):
    now_ts = time.time()
    cleaned = {}
    for key, record in (guards or {}).items():
        try:
            expires_at_ts = float((record or {}).get("expires_at_ts") or 0)
        except Exception:
            expires_at_ts = 0
        if expires_at_ts and expires_at_ts <= now_ts:
            continue
        cleaned[key] = record
    return cleaned


def get_apify_run_guard(guard_key):
    with APIFY_RUN_GUARDS_LOCK:
        guards = purge_expired_apify_run_guards(load_apify_run_guards())
        write_json_file(APIFY_RUN_GUARDS_FILE, guards)
    return guards.get(guard_key)


def remember_apify_run_guard(guard_key, record):
    with APIFY_RUN_GUARDS_LOCK:
        guards = purge_expired_apify_run_guards(load_apify_run_guards())
        guards[guard_key] = record
        write_json_file(APIFY_RUN_GUARDS_FILE, guards)
    return record


def clear_apify_run_guard(guard_key):
    with APIFY_RUN_GUARDS_LOCK:
        guards = purge_expired_apify_run_guards(load_apify_run_guards())
        if guard_key in guards:
            guards.pop(guard_key, None)
            write_json_file(APIFY_RUN_GUARDS_FILE, guards)


def build_apify_guard_key(actor_id, input_data):
    canonical_payload = json.dumps(
        {
            "actor_id": str(actor_id or "").strip(),
            "input_data": input_data or {},
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical_payload.encode("utf-8")).hexdigest()


def build_apify_run_guard_record(actor_id, input_data, token, **extra):
    return {
        "actor_id": str(actor_id or "").strip(),
        "input_data": input_data or {},
        "token_masked": mask_apify_token(token),
        "created_at": iso_now(),
        "expires_at_ts": time.time() + APIFY_GUARD_TTL_SECONDS,
        **extra,
    }


def get_requested_results_per_identifier(platform, limit):
    normalized_platform = str(platform or "").strip().lower()
    try:
        resolved_limit = max(1, int(limit or 1))
    except Exception:
        resolved_limit = 1
    if normalized_platform in {"tiktok", "youtube"}:
        return resolved_limit
    return 1


def estimate_identifier_cost_usd(platform, limit):
    normalized_platform = str(platform or "").strip().lower()
    per_identifier_cost = safe_positive_float(
        PLATFORM_ESTIMATED_COST_PER_IDENTIFIER_USD.get(normalized_platform),
        0.0,
    )
    per_result_cost = safe_positive_float(
        PLATFORM_ESTIMATED_COST_PER_RESULT_USD.get(normalized_platform),
        0.0,
    )
    requested_results = get_requested_results_per_identifier(normalized_platform, limit)
    if per_result_cost > 0 and requested_results > 0:
        per_identifier_cost = max(per_identifier_cost, per_result_cost * requested_results)
    return round(per_identifier_cost, 6)


def estimate_apify_batch_cost_usd(platform, batch, payload):
    total_cost = estimate_identifier_cost_usd(platform, payload.get("limit", 1)) * len(batch or [])
    if str(platform or "").strip().lower() == "instagram" and bool(payload.get("includeAbout", True)):
        total_cost += INSTAGRAM_ABOUT_ADDON_COST_PER_PROFILE_USD * len(batch or [])
    return round(total_cost, 6)


def apply_apify_budget_guard_band(estimated_cost_usd):
    guarded_cost = safe_positive_float(estimated_cost_usd, 0.0) * APIFY_BUDGET_SAFETY_MULTIPLIER
    guarded_cost += APIFY_BUDGET_BUFFER_USD
    return round(guarded_cost, 6)


def acquire_apify_token_candidates():
    pool = get_apify_token_pool()
    if not pool:
        return []
    with APIFY_TOKEN_LOCK:
        state = load_apify_token_state()
        next_index = int(state.get("next_index") or 0)
        slot = next_index % len(pool)
        state.update({
            "next_index": next_index + 1,
            "last_slot": slot,
            "token_count": len(pool),
            "updated_at": iso_now(),
        })
        save_apify_token_state(state)
    return pool[slot:] + pool[:slot]


def remember_apify_budget_snapshot(snapshot):
    token = str((snapshot or {}).get("token") or "").strip()
    if not token:
        return
    with APIFY_TOKEN_LOCK:
        state = load_apify_token_state()
        tokens = state.setdefault("tokens", {})
        token_state = tokens.get(token, {})
        token_state.update({
            "masked": snapshot.get("masked") or mask_apify_token(token),
            "max_monthly_usage_usd": snapshot.get("max_monthly_usage_usd"),
            "monthly_usage_usd": snapshot.get("monthly_usage_usd"),
            "remaining_monthly_usage_usd": snapshot.get("remaining_monthly_usage_usd"),
            "monthly_usage_cycle_start_at": snapshot.get("monthly_usage_cycle_start_at"),
            "monthly_usage_cycle_end_at": snapshot.get("monthly_usage_cycle_end_at"),
            "budget_checked_at": snapshot.get("checked_at") or iso_now(),
        })
        tokens[token] = token_state
        save_apify_token_state(state)


def fetch_apify_budget_snapshot(token):
    cleaned_token = str(token or "").strip()
    if not cleaned_token:
        raise RuntimeError("缺少 Apify token，无法查询月额度。")

    response = apify_request(
        "GET",
        f"{APIFY_API_BASE}/users/me/limits",
        token=cleaned_token,
    )
    if response.status_code != 200:
        raise RuntimeError(f"查询 Apify 月额度失败：{extract_apify_response_error(response)}")

    payload = (response.json() or {}).get("data") or {}
    limits = payload.get("limits") or {}
    current = payload.get("current") or {}
    cycle = payload.get("monthlyUsageCycle") or {}
    max_monthly_usage_usd = round(safe_positive_float(limits.get("maxMonthlyUsageUsd"), 0.0), 6)
    monthly_usage_usd = round(safe_positive_float(current.get("monthlyUsageUsd"), 0.0), 6)
    snapshot = {
        "token": cleaned_token,
        "masked": mask_apify_token(cleaned_token),
        "max_monthly_usage_usd": max_monthly_usage_usd,
        "monthly_usage_usd": monthly_usage_usd,
        "remaining_monthly_usage_usd": round(max(0.0, max_monthly_usage_usd - monthly_usage_usd), 6),
        "monthly_usage_cycle_start_at": (
            cycle.get("startedAt")
            or cycle.get("startAt")
            or cycle.get("startDate")
            or current.get("monthlyUsageCycleStartedAt")
        ),
        "monthly_usage_cycle_end_at": (
            cycle.get("endsAt")
            or cycle.get("endAt")
            or cycle.get("endDate")
            or current.get("monthlyUsageCycleEndsAt")
        ),
        "checked_at": iso_now(),
    }
    remember_apify_budget_snapshot(snapshot)
    return snapshot


def collect_apify_budget_snapshots_for_tokens(tokens):
    snapshots = []
    errors = []
    for token in tokens or []:
        try:
            snapshots.append(fetch_apify_budget_snapshot(token))
        except Exception as exc:
            errors.append({
                "token_masked": mask_apify_token(token),
                "error": sanitize_apify_error_text(str(exc), tokens=tokens),
            })
    return snapshots, errors


def collect_apify_budget_snapshots():
    token_pool = get_apify_token_pool()
    return collect_apify_budget_snapshots_for_tokens(token_pool)


def select_apify_token_for_batch(token_candidates, required_budget_usd):
    checked_snapshots = []
    query_errors = []
    insufficient_snapshots = []
    normalized_required_budget = round(safe_positive_float(required_budget_usd, 0.0), 6)

    for token in token_candidates or []:
        try:
            snapshot = fetch_apify_budget_snapshot(token)
        except Exception as exc:
            query_errors.append({
                "token_masked": mask_apify_token(token),
                "error": sanitize_apify_error_text(str(exc), tokens=token_candidates),
            })
            continue

        checked_snapshots.append(snapshot)
        remaining = safe_positive_float(snapshot.get("remaining_monthly_usage_usd"), 0.0)
        if remaining >= normalized_required_budget:
            return {
                "selected_token": token,
                "selected_snapshot": snapshot,
                "required_budget_usd": normalized_required_budget,
                "checked_snapshots": checked_snapshots,
                "insufficient_snapshots": insufficient_snapshots,
                "query_errors": query_errors,
            }

        insufficient_snapshots.append({
            "token_masked": snapshot.get("masked"),
            "remaining_monthly_usage_usd": snapshot.get("remaining_monthly_usage_usd"),
        })

    best_remaining = max(
        (
            safe_positive_float(item.get("remaining_monthly_usage_usd"), 0.0)
            for item in checked_snapshots
        ),
        default=0.0,
    )
    if checked_snapshots:
        error = (
            f"Apify 预算不足：当前批次需要约 {normalized_required_budget:.6f} USD，"
            f"但可用 token 的最高剩余额度只有 {best_remaining:.6f} USD。"
        )
        error_code = "APIFY_BUDGET_INSUFFICIENT"
    else:
        joined_errors = "；".join(item["error"] for item in query_errors if item.get("error"))
        error = f"Apify 预算查询失败{f'：{joined_errors}' if joined_errors else ''}"
        error_code = "APIFY_BUDGET_CHECK_FAILED"

    return {
        "selected_token": "",
        "selected_snapshot": None,
        "required_budget_usd": normalized_required_budget,
        "checked_snapshots": checked_snapshots,
        "insufficient_snapshots": insufficient_snapshots,
        "query_errors": query_errors,
        "error": error,
        "error_code": error_code,
    }


def summarize_apify_budget_snapshots(snapshots):
    return {
        "max_monthly_usage_usd_total": round(
            sum(safe_positive_float(item.get("max_monthly_usage_usd"), 0.0) for item in snapshots),
            6,
        ),
        "monthly_usage_usd_total": round(
            sum(safe_positive_float(item.get("monthly_usage_usd"), 0.0) for item in snapshots),
            6,
        ),
        "remaining_monthly_usage_usd_total": round(
            sum(safe_positive_float(item.get("remaining_monthly_usage_usd"), 0.0) for item in snapshots),
            6,
        ),
    }


def iso_to_epoch_seconds(value):
    text = str(value or "").strip()
    if not text:
        return None
    try:
        normalized = text.replace("Z", "+00:00")
        return datetime.fromisoformat(normalized).timestamp()
    except Exception:
        return None


def enrich_apify_balance_payload(payload, status_code, *, data_source, cache_record=None):
    normalized_payload = dict(payload or {})
    normalized_payload["status_code"] = int(status_code or 200)
    normalized_payload["data_source"] = data_source
    normalized_payload["refresh_interval_seconds"] = APIFY_BALANCE_REFRESH_INTERVAL_SECONDS

    record = cache_record or {}
    fetched_at = str(record.get("fetched_at") or normalized_payload.get("checked_at") or "").strip()
    normalized_payload["cache_fetched_at"] = fetched_at or None
    cache_age_seconds = None
    fetched_ts = iso_to_epoch_seconds(fetched_at)
    if fetched_ts is not None:
        cache_age_seconds = max(0, int(time.time() - fetched_ts))
    normalized_payload["cache_age_seconds"] = cache_age_seconds
    normalized_payload["cache_is_stale"] = (
        cache_age_seconds is not None and cache_age_seconds > APIFY_BALANCE_REFRESH_INTERVAL_SECONDS * 2
    )
    normalized_payload["last_refresh_attempt_at"] = record.get("last_refresh_attempt_at") or normalized_payload.get("checked_at")
    normalized_payload["background_refresh_error"] = record.get("last_refresh_error")
    normalized_payload["background_refresh_error_code"] = record.get("last_refresh_error_code")
    return normalized_payload


def build_live_apify_balance_payload():
    ensure_runtime_dirs()
    token_pool = get_apify_token_pool()
    if not token_pool:
        return {
            "success": False,
            "error_code": "MISSING_APIFY_CONFIG",
            "error": "缺少 Apify 配置：请设置 APIFY_TOKEN 或 APIFY_API_TOKEN。",
        }, 400

    snapshots, errors = collect_apify_budget_snapshots()
    if not snapshots:
        joined_errors = "；".join(item["error"] for item in errors if item.get("error"))
        return {
            "success": False,
            "error_code": "APIFY_BALANCE_QUERY_FAILED",
            "error": f"查询 Apify 余额失败{f'：{joined_errors}' if joined_errors else ''}",
            "token_pool_size": len(token_pool),
            "checked_token_count": 0,
            "failed_token_count": len(errors),
            "checked_at": iso_now(),
            "errors": errors,
            "tokens": [],
            "summary": {},
        }, 502

    return {
        "success": True,
        "token_pool_size": len(token_pool),
        "checked_token_count": len(snapshots),
        "failed_token_count": len(errors),
        "checked_at": iso_now(),
        "summary": summarize_apify_budget_snapshots(snapshots),
        "tokens": [
            {
                "token_masked": item.get("masked"),
                "max_monthly_usage_usd": item.get("max_monthly_usage_usd"),
                "monthly_usage_usd": item.get("monthly_usage_usd"),
                "remaining_monthly_usage_usd": item.get("remaining_monthly_usage_usd"),
                "monthly_usage_cycle_start_at": item.get("monthly_usage_cycle_start_at"),
                "monthly_usage_cycle_end_at": item.get("monthly_usage_cycle_end_at"),
                "checked_at": item.get("checked_at"),
            }
            for item in snapshots
        ],
        "errors": errors,
    }, 200


def read_cached_apify_balance_payload():
    cache_record = load_apify_balance_cache()
    payload = cache_record.get("payload")
    if not isinstance(payload, dict):
        return None, None
    status_code = int(cache_record.get("status_code") or 200)
    return enrich_apify_balance_payload(
        payload,
        status_code,
        data_source="cache",
        cache_record=cache_record,
    ), status_code


def refresh_apify_balance_cache():
    with APIFY_BALANCE_REFRESH_LOCK:
        previous_cache = load_apify_balance_cache()
        live_payload, status_code = build_live_apify_balance_payload()
        now = iso_now()
        cache_record = {
            "refresh_interval_seconds": APIFY_BALANCE_REFRESH_INTERVAL_SECONDS,
            "last_refresh_attempt_at": now,
        }

        previous_payload = previous_cache.get("payload")
        if status_code < 400 and live_payload.get("success") is True:
            cache_record["payload"] = live_payload
            cache_record["status_code"] = status_code
            cache_record["fetched_at"] = live_payload.get("checked_at") or now
            cache_record["last_refresh_error"] = None
            cache_record["last_refresh_error_code"] = None
        else:
            cache_record["last_refresh_error"] = live_payload.get("error")
            cache_record["last_refresh_error_code"] = live_payload.get("error_code")
            if isinstance(previous_payload, dict):
                cache_record["payload"] = previous_payload
                cache_record["status_code"] = int(previous_cache.get("status_code") or 200)
                cache_record["fetched_at"] = previous_cache.get("fetched_at") or previous_payload.get("checked_at") or now
            else:
                cache_record["payload"] = live_payload
                cache_record["status_code"] = status_code
                cache_record["fetched_at"] = now

        save_apify_balance_cache(cache_record)
        return enrich_apify_balance_payload(
            cache_record.get("payload"),
            cache_record.get("status_code"),
            data_source="live",
            cache_record=cache_record,
        ), int(cache_record.get("status_code") or status_code or 200)


def build_apify_balance_payload(force_refresh=False):
    if force_refresh:
        return refresh_apify_balance_cache()

    cached_payload, cached_status_code = read_cached_apify_balance_payload()
    if cached_payload is not None:
        return cached_payload, cached_status_code
    return refresh_apify_balance_cache()


def apify_balance_poller_loop():
    while not APIFY_BALANCE_POLLER_STOP_EVENT.is_set():
        try:
            refresh_apify_balance_cache()
        except Exception as exc:
            error_payload = {
                "success": False,
                "error_code": "APIFY_BALANCE_POLLER_FAILED",
                "error": sanitize_apify_error_text(str(exc), tokens=get_apify_token_pool()),
                "checked_at": iso_now(),
                "errors": [],
                "tokens": [],
                "summary": {},
            }
            cache_record = load_apify_balance_cache()
            cache_record["last_refresh_attempt_at"] = iso_now()
            cache_record["last_refresh_error"] = error_payload["error"]
            cache_record["last_refresh_error_code"] = error_payload["error_code"]
            if not isinstance(cache_record.get("payload"), dict):
                cache_record["payload"] = error_payload
                cache_record["status_code"] = 502
                cache_record["fetched_at"] = error_payload["checked_at"]
            save_apify_balance_cache(cache_record)
        if APIFY_BALANCE_POLLER_STOP_EVENT.wait(APIFY_BALANCE_REFRESH_INTERVAL_SECONDS):
            return


def start_apify_balance_poller():
    global APIFY_BALANCE_POLLER_THREAD
    if not APIFY_BALANCE_POLLER_ENABLED:
        return None
    with APIFY_BALANCE_POLLER_LOCK:
        if APIFY_BALANCE_POLLER_THREAD and APIFY_BALANCE_POLLER_THREAD.is_alive():
            return APIFY_BALANCE_POLLER_THREAD
        APIFY_BALANCE_POLLER_STOP_EVENT.clear()
        APIFY_BALANCE_POLLER_THREAD = threading.Thread(
            target=apify_balance_poller_loop,
            daemon=True,
            name="apify-balance-poller",
        )
        APIFY_BALANCE_POLLER_THREAD.start()
        return APIFY_BALANCE_POLLER_THREAD


def normalize_vision_provider_name(provider_name):
    return str(provider_name or "").strip().lower()


def resolve_vision_provider_api_key(provider):
    env_key = str((provider or {}).get("env_key") or "").strip()
    return str(os.getenv(env_key, "") or "").strip()


def resolve_vision_provider_model(provider):
    model = str((provider or {}).get("model") or "").strip()
    return model or VISION_MODEL


def get_available_vision_providers():
    providers = []
    for provider in VISION_PROVIDER_CONFIGS:
        api_key = resolve_vision_provider_api_key(provider)
        if not api_key:
            continue
        providers.append({
            **provider,
            "name": normalize_vision_provider_name(provider.get("name")),
            "api_key": api_key,
        })
    return providers


def get_available_vision_provider_names():
    return [provider["name"] for provider in get_available_vision_providers()]


def strip_code_fences(text):
    stripped = str(text or "").strip()
    if stripped.startswith("```"):
        stripped = re.sub(r"^```[a-zA-Z0-9_]*\n?", "", stripped)
        stripped = re.sub(r"\n?```$", "", stripped)
    return stripped.strip()


def parse_visual_review_result(raw_text):
    cleaned = strip_code_fences(raw_text)
    payload = None
    try:
        payload = json.loads(cleaned)
    except json.JSONDecodeError:
        start_idx = cleaned.find("{")
        end_idx = cleaned.rfind("}")
        if start_idx != -1 and end_idx != -1 and end_idx > start_idx:
            snippet = cleaned[start_idx:end_idx + 1]
            try:
                payload = json.loads(snippet)
            except json.JSONDecodeError:
                payload = None

    if isinstance(payload, dict):
        decision = str(payload.get("decision") or "").strip().title()
        if decision not in {"Pass", "Reject"}:
            decision = "Reject" if "reject" in cleaned.lower() else "Pass"
        signals = payload.get("signals")
        if not isinstance(signals, list):
            signals = []
        return {
            "decision": decision,
            "reason": str(payload.get("reason") or cleaned).strip() or "模型未返回原因",
            "signals": [str(item).strip() for item in signals if str(item or "").strip()][:3],
        }

    return {
        "decision": "Reject" if "reject" in cleaned.lower() else "Pass",
        "reason": cleaned or "模型未返回可解析内容",
        "signals": [],
    }


def extract_vision_response_text(payload):
    if isinstance(payload, str):
        return payload.strip()
    if not isinstance(payload, dict):
        return str(payload)

    output_text = payload.get("output_text")
    if isinstance(output_text, str) and output_text.strip():
        return output_text

    output = payload.get("output")
    if isinstance(output, list):
        text_parts = []
        for item in output:
            for content_item in (item or {}).get("content") or []:
                text_value = content_item.get("text")
                if isinstance(text_value, str) and text_value.strip():
                    text_parts.append(text_value)
            if text_parts:
                return "\n".join(text_parts)

    choices = payload.get("choices")
    if isinstance(choices, list):
        text_parts = []
        for choice in choices:
            message = (choice or {}).get("message") or {}
            content = message.get("content")
            if isinstance(content, str) and content.strip():
                text_parts.append(content)
                continue
            if isinstance(content, list):
                for content_item in content:
                    text_value = (content_item or {}).get("text")
                    if isinstance(text_value, str) and text_value.strip():
                        text_parts.append(text_value)
            if text_parts:
                return "\n".join(text_parts)

    return json.dumps(payload, ensure_ascii=False)


def guess_image_mime_type(response, fallback_url=""):
    content_type = str((response.headers or {}).get("Content-Type") or "").split(";", 1)[0].strip().lower()
    if content_type.startswith("image/"):
        return content_type
    fallback_url = str(fallback_url or "").lower()
    if fallback_url.endswith(".png"):
        return "image/png"
    if fallback_url.endswith(".webp"):
        return "image/webp"
    return "image/jpeg"


def guess_image_extension(mime_type, fallback_url=""):
    normalized_mime_type = str(mime_type or "").split(";", 1)[0].strip().lower()
    if normalized_mime_type in {"image/jpeg", "image/jpg"}:
        return ".jpg"
    if normalized_mime_type == "image/png":
        return ".png"
    if normalized_mime_type == "image/webp":
        return ".webp"
    if normalized_mime_type == "image/gif":
        return ".gif"
    if normalized_mime_type == "image/bmp":
        return ".bmp"
    if normalized_mime_type == "image/avif":
        return ".avif"
    fallback_path = urlparse(str(fallback_url or "")).path.lower()
    for extension in (".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp", ".avif"):
        if fallback_path.endswith(extension):
            return ".jpg" if extension == ".jpeg" else extension
    return ".jpg"


def build_image_data_url(image_bytes, mime_type):
    encoded = base64.b64encode(image_bytes).decode("ascii")
    return f"data:{mime_type};base64,{encoded}"


def build_visual_image_cache_key(image_url):
    normalized = str(image_url or "").strip()
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def load_cached_visual_image(platform, image_url):
    if not VISUAL_IMAGE_CACHE_ENABLED:
        return None
    cache_dir = Path(get_visual_image_cache_dir(platform))
    metadata_path = cache_dir / f"{build_visual_image_cache_key(image_url)}.json"
    metadata = load_json_payload(str(metadata_path), default={}) or {}
    filename = str(metadata.get("filename") or "").strip()
    mime_type = str(metadata.get("mime_type") or "").strip().lower()
    if not filename or not mime_type:
        return None
    image_path = cache_dir / filename
    if not image_path.exists() or not image_path.is_file():
        return None
    try:
        return {
            "bytes": image_path.read_bytes(),
            "mime_type": mime_type,
            "path": str(image_path),
        }
    except Exception:
        return None


def persist_visual_image_cache(platform, image_url, image_bytes, mime_type):
    if not VISUAL_IMAGE_CACHE_ENABLED or not image_bytes:
        return None
    cache_dir = Path(get_visual_image_cache_dir(platform))
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_key = build_visual_image_cache_key(image_url)
    image_extension = guess_image_extension(mime_type, fallback_url=image_url)
    image_filename = f"{cache_key}{image_extension}"
    image_path = cache_dir / image_filename
    metadata_path = cache_dir / f"{cache_key}.json"
    temp_image_path = cache_dir / f".{image_filename}.{uuid.uuid4().hex}.tmp"
    temp_metadata_path = cache_dir / f".{cache_key}.{uuid.uuid4().hex}.json.tmp"
    try:
        temp_image_path.write_bytes(image_bytes)
        os.replace(temp_image_path, image_path)
        metadata_payload = {
            "source_url": str(image_url or "").strip(),
            "filename": image_filename,
            "mime_type": str(mime_type or "").strip().lower() or "image/jpeg",
            "size_bytes": len(image_bytes),
            "cached_at": iso_now(),
        }
        temp_metadata_path.write_text(
            json.dumps(metadata_payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        os.replace(temp_metadata_path, metadata_path)
        return {
            "path": str(image_path),
            "mime_type": metadata_payload["mime_type"],
        }
    finally:
        for temp_path in (temp_image_path, temp_metadata_path):
            try:
                if temp_path.exists():
                    temp_path.unlink()
            except Exception:
                pass


def build_data_url_from_remote_image(platform, image_url):
    cached_image = load_cached_visual_image(platform, image_url)
    if cached_image:
        return build_image_data_url(cached_image["bytes"], cached_image["mime_type"])
    last_error = None
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "image/*,*/*;q=0.8",
        "Referer": image_url,
    }
    for attempt_index in range(1, VISUAL_IMAGE_DOWNLOAD_MAX_RETRIES + 1):
        try:
            response = requests.get(
                image_url,
                timeout=VISION_REQUEST_TIMEOUT,
                headers=headers,
            )
            if response.status_code >= 400:
                retryable = response.status_code in VISUAL_REVIEW_RETRYABLE_STATUS_CODES
                if retryable and attempt_index < VISUAL_IMAGE_DOWNLOAD_MAX_RETRIES:
                    time.sleep(compute_visual_retry_delay_seconds(attempt_index))
                    continue
                raise RuntimeError(f"下载封面失败：HTTP {response.status_code}")
            mime_type = guess_image_mime_type(response, fallback_url=image_url)
            image_bytes = response.content
            persist_visual_image_cache(platform, image_url, image_bytes, mime_type)
            return build_image_data_url(image_bytes, mime_type)
        except requests.exceptions.RequestException as exc:
            last_error = exc
            if attempt_index >= VISUAL_IMAGE_DOWNLOAD_MAX_RETRIES:
                break
            time.sleep(compute_visual_retry_delay_seconds(attempt_index))
    if last_error is not None:
        raise last_error
    raise RuntimeError("下载封面失败")


def normalize_visual_image_source(platform, image_url):
    image_url = str(image_url or "").strip()
    if not image_url:
        return ""
    if image_url.startswith("data:"):
        return image_url
    return build_data_url_from_remote_image(platform, image_url)


def dedupe_non_empty_strings(values):
    seen = set()
    normalized = []
    for value in values or []:
        cleaned = str(value or "").strip()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        normalized.append(cleaned)
    return normalized


def extract_tiktok_raw_identifier(item):
    return screening.extract_platform_identifier(
        "tiktok",
        ((item.get("authorMeta") or {}).get("profileUrl")) or ((item.get("authorMeta") or {}).get("name")),
    )


def extract_instagram_raw_identifier(item):
    return (
        screening.extract_platform_identifier("instagram", item.get("url"))
        or screening.extract_platform_identifier("instagram", item.get("username"))
    )


def extract_youtube_raw_identifier(item):
    return (
        screening.extract_platform_identifier("youtube", item.get("inputChannelUrl"))
        or screening.extract_platform_identifier("youtube", item.get("input"))
        or screening.extract_platform_identifier("youtube", item.get("channelUsername"))
        or screening.extract_platform_identifier("youtube", item.get("channelUrl"))
        or screening.extract_platform_identifier("youtube", item.get("channelName"))
        or screening.extract_platform_identifier("youtube", ((item.get("aboutChannelInfo") or {}).get("channelUrl")))
        or screening.extract_platform_identifier("youtube", ((item.get("aboutChannelInfo") or {}).get("inputChannelUrl")))
        or screening.extract_platform_identifier("youtube", ((item.get("aboutChannelInfo") or {}).get("channelUsername")))
    )


def derive_visual_review_candidate_cover_urls_from_raw(platform, identifier, candidate_limit):
    identifier = screening.normalize_identifier(identifier)
    if not identifier:
        return []
    raw_items = load_json_payload(get_raw_data_path(platform), default=[]) or []
    if not raw_items:
        return []

    if platform == "tiktok":
        matched_items = [
            item
            for item in raw_items
            if extract_tiktok_raw_identifier(item) == identifier
        ]
        sorted_items = screening.sort_items_by_latest(matched_items, "createTimeISO")
        return screening.extract_tiktok_cover_urls(sorted_items, candidate_limit)

    if platform == "instagram":
        for item in raw_items:
            if extract_instagram_raw_identifier(item) != identifier:
                continue
            sorted_posts = screening.sort_items_by_latest(item.get("latestPosts") or [], "timestamp")
            return screening.extract_instagram_cover_urls(sorted_posts, candidate_limit)
        return []

    if platform == "youtube":
        matched_items = [
            item
            for item in raw_items
            if extract_youtube_raw_identifier(item) == identifier
        ]
        sorted_items = screening.sort_items_by_latest(matched_items, "date")
        return screening.extract_youtube_cover_urls(sorted_items, candidate_limit)

    return []


def build_visual_review_candidate_cover_urls(platform, review_item):
    existing_covers = dedupe_non_empty_strings((review_item or {}).get("covers") or [])
    identifier = screening.resolve_profile_review_identifier(platform, review_item)
    candidate_limit = max(
        VISUAL_REVIEW_REQUEST_COVER_LIMIT,
        VISUAL_REVIEW_CANDIDATE_COVER_LIMIT_FLOOR,
        len(existing_covers),
    )
    derived_covers = derive_visual_review_candidate_cover_urls_from_raw(platform, identifier, candidate_limit)
    return dedupe_non_empty_strings(existing_covers + derived_covers)[:candidate_limit]


def build_visual_review_input(platform, username, cover_urls):
    header = f"平台：{UPLOAD_PLATFORM_RESPONSE_LABELS.get(platform, platform)}\n达人：{username or 'unknown'}\n{VISION_PROMPT}"
    candidate_cover_urls = dedupe_non_empty_strings(cover_urls)
    normalized_image_sources = []
    skipped_cover_count = 0
    for cover_url in candidate_cover_urls:
        try:
            image_source = normalize_visual_image_source(platform, cover_url)
        except Exception:
            skipped_cover_count += 1
            continue
        if not image_source:
            skipped_cover_count += 1
            continue
        normalized_image_sources.append(image_source)
        if len(normalized_image_sources) >= VISUAL_REVIEW_REQUEST_COVER_LIMIT:
            break
    if not normalized_image_sources:
        raise ValueError("没有可送审的有效图片")
    response_content = [{"type": "input_text", "text": header}]
    response_content.extend(
        {"type": "input_image", "image_url": image_source}
        for image_source in normalized_image_sources
    )
    chat_content = [{"type": "text", "text": header}]
    chat_content.extend(
        {"type": "image_url", "image_url": {"url": image_source}}
        for image_source in normalized_image_sources
    )
    return {
        "responses": [{"role": "user", "content": response_content}],
        "chat": [{"role": "user", "content": chat_content}],
        "selected_cover_count": len(normalized_image_sources),
        "candidate_cover_count": len(candidate_cover_urls),
        "skipped_cover_count": skipped_cover_count,
    }


class VisionProviderError(RuntimeError):
    def __init__(self, provider_name, message, status_code=None, retryable=False):
        self.provider_name = normalize_vision_provider_name(provider_name)
        self.status_code = status_code
        self.retryable = bool(retryable)
        super().__init__(f"{self.provider_name}: {message}")


def is_retryable_visual_exception(exc):
    if isinstance(exc, VisionProviderError):
        return exc.retryable
    if isinstance(exc, requests.exceptions.Timeout):
        return True
    if isinstance(exc, requests.exceptions.ConnectionError):
        return True
    return False


def compute_visual_retry_delay_seconds(attempt_index):
    capped_attempt = max(1, int(attempt_index or 1))
    delay = VISUAL_REVIEW_RETRY_BASE_DELAY_SECONDS * (2 ** (capped_attempt - 1))
    delay = min(delay, VISUAL_REVIEW_RETRY_MAX_DELAY_SECONDS)
    jitter = random.uniform(0, min(0.5, delay * 0.2))
    return round(delay + jitter, 3)


def resolve_visual_review_max_workers(payload, target_count):
    requested = (payload or {}).get("max_workers")
    try:
        requested_value = int(requested) if requested not in (None, "") else DEFAULT_VISUAL_REVIEW_MAX_WORKERS
    except Exception:
        requested_value = DEFAULT_VISUAL_REVIEW_MAX_WORKERS
    return max(1, min(int(target_count or 1), requested_value))


def call_vision_provider(provider, platform, username, cover_urls):
    provider_name = normalize_vision_provider_name(provider.get("name"))
    api_style = str(provider.get("api_style") or VISION_API_STYLE_RESPONSES).strip().lower()
    base_url = str(provider.get("base_url") or "").rstrip("/")
    if not base_url:
        raise VisionProviderError(provider_name, "base_url 未配置")

    try:
        input_payload = build_visual_review_input(platform, username, cover_urls)
    except requests.exceptions.RequestException as exc:
        raise VisionProviderError(provider_name, str(exc), retryable=True) from exc
    headers = {
        "Authorization": f"Bearer {provider['api_key']}",
        "Content-Type": "application/json",
    }
    if api_style == VISION_API_STYLE_CHAT_COMPLETIONS:
        url = f"{base_url}/chat/completions"
        body = {
            "model": resolve_vision_provider_model(provider),
            "messages": input_payload["chat"],
        }
    else:
        url = f"{base_url}/responses"
        body = {
            "model": resolve_vision_provider_model(provider),
            "input": input_payload["responses"],
        }

    try:
        response = requests.post(url, headers=headers, json=body, timeout=VISION_REQUEST_TIMEOUT)
    except requests.exceptions.RequestException as exc:
        raise VisionProviderError(provider_name, str(exc), retryable=True) from exc
    if response.status_code >= 400:
        raise VisionProviderError(
            provider_name,
            f"HTTP {response.status_code} {extract_apify_response_error(response)}",
            status_code=response.status_code,
            retryable=response.status_code in VISUAL_REVIEW_RETRYABLE_STATUS_CODES,
        )
    payload = response.json()
    raw_text = extract_vision_response_text(payload)
    parsed = parse_visual_review_result(raw_text)
    parsed["provider"] = provider_name
    parsed["raw_text"] = raw_text
    parsed["cover_count"] = input_payload.get("selected_cover_count")
    parsed["candidate_cover_count"] = input_payload.get("candidate_cover_count")
    parsed["skipped_cover_count"] = input_payload.get("skipped_cover_count")
    return parsed


def evaluate_profile_visual_review(platform, review_item):
    identifier = screening.resolve_profile_review_identifier(platform, review_item)
    cover_urls = build_visual_review_candidate_cover_urls(platform, review_item)
    if not cover_urls:
        raise ValueError("没有可送审的封面 URL")

    last_error = None
    for provider in get_available_vision_providers():
        attempt_count = 0
        while attempt_count < VISUAL_REVIEW_MAX_RETRIES:
            attempt_count += 1
            try:
                result = call_vision_provider(
                    provider,
                    platform,
                    identifier or review_item.get("username") or "",
                    cover_urls,
                )
                result["success"] = True
                result["reviewed_at"] = iso_now()
                result["cover_count"] = int(result.get("cover_count") or 0)
                result["candidate_cover_count"] = int(result.get("candidate_cover_count") or len(cover_urls))
                result["skipped_cover_count"] = int(result.get("skipped_cover_count") or 0)
                result["attempt_count"] = attempt_count
                return result
            except Exception as exc:
                last_error = exc
                should_retry = is_retryable_visual_exception(exc) and attempt_count < VISUAL_REVIEW_MAX_RETRIES
                if not should_retry:
                    break
                time.sleep(compute_visual_retry_delay_seconds(attempt_count))
                continue

    raise RuntimeError(str(last_error) if last_error else "缺少视觉模型配置")


def build_visual_review_partial_result(platform, results, targets):
    target_identifiers = {
        screening.resolve_profile_review_identifier(platform, item)
        for item in (targets or [])
        if screening.resolve_profile_review_identifier(platform, item)
    }
    passed = 0
    rejected = 0
    failed = 0
    filtered_results = {}
    for key, item in (results or {}).items():
        if not isinstance(item, dict):
            continue
        identifier = screening.normalize_identifier(key or item.get("username"))
        if target_identifiers and identifier not in target_identifiers:
            continue
        filtered_results[identifier] = item
        if item.get("success") is False:
            failed += 1
            continue
        if str(item.get("decision") or "").strip() == "Reject":
            rejected += 1
        else:
            passed += 1
    return {
        "platform": platform,
        "target_total": len(targets),
        "reviewed_total": passed + rejected + failed,
        "summary": {
            "pass": passed,
            "reject": rejected,
            "error": failed,
        },
        "visual_results_path": get_visual_results_path(platform),
        "visual_results": filtered_results,
    }


class ApifyStartError(RuntimeError):
    def __init__(self, status_code, message):
        self.status_code = int(status_code or 0)
        super().__init__(message)

    @property
    def retryable_with_next_token(self):
        return self.status_code in {401, 402, 403, 429}

    @property
    def uncertain_submission(self):
        return self.status_code in TRANSIENT_STATUS_CODES


def normalize_upload_column_name(name):
    text = str(name or "").strip().lower()
    if not text:
        return ""
    return re.sub(r"[^a-z0-9]+", "", text)


def clean_upload_metadata_value(value):
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if pd.isna(value):
        return ""
    return value


def normalize_upload_platform_value(value):
    normalized = normalize_upload_column_name(value)
    return UPLOAD_PLATFORM_ALIASES.get(normalized, "")


def is_empty_upload_row(row_dict):
    for key, value in (row_dict or {}).items():
        if str(key).startswith("__"):
            continue
        cleaned = clean_upload_metadata_value(value)
        if cleaned not in ("", None):
            return False
    return True


def format_upload_row_location(row_dict, fallback_row_num):
    sheet_name = str((row_dict or {}).get("__sheet_name") or "").strip()
    row_num = clean_upload_metadata_value((row_dict or {}).get("__sheet_row_num"))
    try:
        row_num = int(row_num)
    except Exception:
        row_num = fallback_row_num
    if sheet_name:
        return f"Sheet `{sheet_name}` 第 {row_num} 行"
    return f"第 {row_num} 行"


def build_upload_validation_error(message, details, error_code="UPLOAD_TEMPLATE_INVALID"):
    return jsonify({
        "success": False,
        "error": message,
        "error_code": error_code,
        "details": [str(item) for item in details if str(item).strip()],
    }), 400


def load_canonical_upload_workbook_frames(filepath):
    workbook = pd.read_excel(filepath, sheet_name=None)
    frames = []
    for sheet_name, df in (workbook or {}).items():
        if df is None or df.empty:
            continue
        prepared = df.copy()
        prepared["__sheet_name"] = str(sheet_name or "").strip() or "Sheet1"
        prepared["__sheet_row_num"] = [index + 2 for index in range(len(prepared))]
        if not any(
            clean_upload_metadata_value(value) not in ("", None)
            for column in prepared.columns
            if not str(column).startswith("__")
            for value in prepared[column].tolist()
        ):
            continue
        frames.append(prepared)
    return frames


def resolve_canonical_upload_columns(columns):
    normalized = {}
    for column in columns:
        normalized[normalize_upload_column_name(column)] = column
    resolved = {
        "platform": normalized.get("platform"),
        "handle": normalized.get("username") or normalized.get("handle"),
        "url": normalized.get("url"),
    }
    missing = []
    if not resolved["platform"]:
        missing.append("Platform")
    if not resolved["handle"]:
        missing.append("@username")
    return resolved, missing


def build_upload_metadata_record(row_dict, platform, canonical_url, source_filename):
    metadata = {
        "platform": platform,
        "url": canonical_url,
        "source_filename": source_filename,
    }
    for column_name, raw_value in (row_dict or {}).items():
        normalized_column = normalize_upload_column_name(column_name)
        mapped_field = UPLOAD_METADATA_FIELD_ALIASES.get(normalized_column)
        if not mapped_field:
            continue
        cleaned = clean_upload_metadata_value(raw_value)
        if cleaned in ("", None):
            continue
        metadata[mapped_field] = cleaned
    handle = screening.extract_platform_identifier(platform, canonical_url) or screening.extract_platform_identifier(platform, metadata.get("handle"))
    metadata["handle"] = handle
    return sanitize_json_compatible(metadata)


def parse_canonical_upload_workbook(df, source_filename):
    resolved_columns, missing = resolve_canonical_upload_columns(df.columns)
    if missing:
        details = [f"缺少必填列：{label}" for label in missing]
        details.append("固定模板至少需要 `Platform` 和 `@username` 两列。")
        return None, build_upload_validation_error(
            f"上传模板缺少必填列：{', '.join(missing)}",
            details,
        )

    grouped_data = {platform: [] for platform in PLATFORM_ACTORS}
    metadata_by_platform = {platform: {} for platform in PLATFORM_ACTORS}
    preview_rows = []
    invalid_rows = []
    processed = 0

    for index, (_, row_series) in enumerate(df.iterrows(), start=2):
        row_dict = row_series.to_dict()
        if is_empty_upload_row(row_dict):
            continue
        processed += 1
        row_location = format_upload_row_location(row_dict, index)
        platform = normalize_upload_platform_value(row_dict.get(resolved_columns["platform"]))
        if not platform:
            invalid_rows.append(f"{row_location} Platform 无效，只支持 Instagram / TikTok / YouTube。")
            continue

        raw_handle = clean_upload_metadata_value(row_dict.get(resolved_columns["handle"]))
        raw_url = clean_upload_metadata_value(row_dict.get(resolved_columns["url"])) if resolved_columns.get("url") else ""
        identifier = (
            screening.extract_platform_identifier(platform, raw_handle)
            or screening.extract_platform_identifier(platform, raw_url)
        )
        if not identifier:
            invalid_rows.append(f"{row_location} @username 为空或无法识别：`{raw_handle}`。")
            continue

        canonical_url = raw_url or screening.build_canonical_profile_url(platform, identifier)
        metadata = build_upload_metadata_record(row_dict, platform, canonical_url, source_filename)
        metadata_by_platform[platform][identifier] = metadata
        grouped_data[platform].append(canonical_url)

        if len(preview_rows) < 5:
            preview_rows.append({
                "Platform": UPLOAD_PLATFORM_RESPONSE_LABELS.get(platform, platform),
                "@username": identifier,
                "URL": canonical_url,
                "nickname": metadata.get("nickname", ""),
                "Region": metadata.get("region", ""),
                "Language": metadata.get("language", ""),
                "Followers": metadata.get("followers", ""),
            })

    if processed == 0:
        return None, build_upload_validation_error(
            "上传表没有可用数据行",
            ["请确认表头下方至少有一行 `Platform` 和 `@username` 都已填写的账号数据。"],
        )
    if invalid_rows:
        return None, build_upload_validation_error(
            "上传模板存在无效数据，请修正后重试",
            invalid_rows[:20],
        )

    deduped_grouped = {platform: list(dict.fromkeys(values)) for platform, values in grouped_data.items()}
    stats = {UPLOAD_PLATFORM_RESPONSE_LABELS[platform]: len(values) for platform, values in deduped_grouped.items()}
    stats["Unknown"] = 0
    return {
        "grouped_data": deduped_grouped,
        "metadata_by_platform": metadata_by_platform,
        "preview": preview_rows,
        "stats": stats,
    }, None


def load_active_rulespec():
    payload = load_json_payload(ACTIVE_RULESPEC_PATH, default={})
    return payload if isinstance(payload, dict) else {}


def persist_active_rulespec(compiled):
    rulespec = compiled.get("rule_spec") or {}
    rules_module.write_json(ACTIVE_RULESPEC_PATH, rulespec)
    rules_module.write_json(FIELD_MATCH_REPORT_PATH, compiled.get("field_match_report") or {})
    rules_module.write_json(MISSING_CAPABILITIES_PATH, compiled.get("missing_capabilities") or {})
    Path(REVIEW_NOTES_PATH).write_text(compiled.get("review_notes_markdown") or "", encoding="utf-8")


def create_job(job_type, platform=None, message="任务已创建"):
    job = {
        "id": uuid.uuid4().hex,
        "type": job_type,
        "platform": platform,
        "status": "queued",
        "stage": "queued",
        "message": message,
        "created_at": iso_now(),
        "updated_at": iso_now(),
        "progress": {"done": 0, "total": None, "percent": 0, "determinate": False},
        "partial_result": None,
        "result": None,
        "error": None,
        "cancel_requested": False,
    }
    with JOBS_LOCK:
        JOBS[job["id"]] = job
    return job


def get_job(job_id):
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        return dict(job) if job else None


def update_job(job_id, **updates):
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        if not job:
            return None
        job.update(updates)
        job["updated_at"] = iso_now()
        return dict(job)


def build_job_progress(done=None, total=None):
    resolved_done = 0 if done is None else done
    resolved_total = total
    determinate = isinstance(resolved_done, (int, float)) and isinstance(resolved_total, (int, float)) and resolved_total > 0
    percent = int((resolved_done / resolved_total) * 100) if determinate else 0
    return {
        "done": resolved_done,
        "total": resolved_total,
        "percent": max(0, min(100, percent)),
        "determinate": determinate,
    }


def start_background_job(job, worker):
    def runner():
        update_job(job["id"], status="running", stage="starting")

        def progress_callback(stage, message=None, done=None, total=None, partial_result=None, **extra):
            payload = {
                "stage": stage,
                "message": message or stage,
                "progress": build_job_progress(done=done, total=total),
            }
            if partial_result is not None:
                payload["partial_result"] = sanitize_json_compatible(partial_result)
            if extra:
                payload["metadata"] = sanitize_json_compatible(extra)
            update_job(job["id"], **payload)

        def cancel_check():
            with JOBS_LOCK:
                current = JOBS.get(job["id"]) or {}
                return bool(current.get("cancel_requested"))

        try:
            result = worker(progress_callback, cancel_check)
            if result and result.get("cancelled"):
                update_job(
                    job["id"],
                    status="cancelled",
                    stage="cancelled",
                    message=result.get("message") or "任务已取消",
                    result=sanitize_json_compatible(result),
                    progress=build_job_progress(done=1, total=1),
                )
                return
            if not result or not result.get("success"):
                update_job(
                    job["id"],
                    status="failed",
                    stage="failed",
                    message=(result or {}).get("error") or "任务失败",
                    error=(result or {}).get("error") or "任务失败",
                    result=sanitize_json_compatible(result),
                    progress=build_job_progress(done=1, total=1),
                )
                return
            update_job(
                job["id"],
                status="completed",
                stage="completed",
                message=result.get("message") or "任务完成",
                result=sanitize_json_compatible(result),
                partial_result=sanitize_json_compatible(result),
                progress=build_job_progress(done=1, total=1),
            )
        except Exception as exc:  # pragma: no cover - runtime guard
            update_job(
                job["id"],
                status="failed",
                stage="failed",
                message=str(exc),
                error=str(exc),
                progress=build_job_progress(done=1, total=1),
            )

    thread = threading.Thread(target=runner, daemon=True, name=f"job-{job['id']}")
    thread.start()


def build_cancelled_result(message="任务已取消"):
    return {"success": False, "cancelled": True, "message": message}


def chunk_list(items, chunk_size):
    chunk_size = max(1, int(chunk_size or 1))
    return [items[index:index + chunk_size] for index in range(0, len(items), chunk_size)]


def build_target_preview(identifiers, max_items=5):
    return {
        "targets": [str(item) for item in list(identifiers or [])[:max_items]],
        "target_count": len(list(identifiers or [])),
    }


def get_requested_visual_identifiers(platform, payload):
    candidates = []
    for key in ("identifiers", "usernames", "profiles", "urls"):
        for item in (payload.get(key) or []):
            identifier = screening.extract_platform_identifier(platform, item)
            if identifier:
                candidates.append(identifier)
    return list(dict.fromkeys(candidates))


def resolve_visual_review_targets(platform, payload):
    requested_identifiers = set(get_requested_visual_identifiers(platform, payload))
    targets = []
    for item in merge_upload_metadata_into_reviews(platform, load_profile_reviews(platform)):
        if str(item.get("status") or "").strip() != "Pass":
            continue
        identifier = screening.resolve_profile_review_identifier(platform, item)
        if requested_identifiers and identifier not in requested_identifiers:
            continue
        targets.append(item)
    return targets


def perform_visual_review(platform, payload, progress_callback=None, cancel_check=None):
    providers = get_available_vision_providers()
    if not providers:
        return {
            "success": False,
            "error_code": "MISSING_VISION_CONFIG",
            "error": "缺少视觉模型配置：请设置 OPENAI_API_KEY、VISION_QUAN2GO_API_KEY 或 VISION_LEMONAPI_API_KEY。",
        }

    targets = resolve_visual_review_targets(platform, payload)
    if not targets:
        return {
            "success": False,
            "error": "没有可复核的账号：请先完成抓取，并确保至少有一个 Prescreen=Pass 的账号。",
        }

    results = load_visual_results(platform)
    target_identifiers = [screening.resolve_profile_review_identifier(platform, item) for item in targets]
    max_workers = resolve_visual_review_max_workers(payload, len(targets))
    started_at = time.monotonic()
    if progress_callback:
        progress_callback(
            "preparing",
            "正在准备视觉复核任务",
            done=0,
            total=len(targets),
            providers=get_available_vision_provider_names(),
            max_workers=max_workers,
            **build_target_preview(target_identifiers),
        )

    completed = 0
    target_iter = iter(targets)
    future_map = {}

    def submit_next(executor):
        if cancel_check and cancel_check():
            return False
        try:
            review_item = next(target_iter)
        except StopIteration:
            return False
        identifier = screening.resolve_profile_review_identifier(platform, review_item)
        if progress_callback:
            progress_callback(
                "reviewing",
                f"已提交视觉复核：{identifier}",
                done=completed,
                total=len(targets),
                provider_candidates=get_available_vision_provider_names(),
                current_identifier=identifier,
                max_workers=max_workers,
            )
        future = executor.submit(evaluate_profile_visual_review, platform, review_item)
        future_map[future] = (identifier, review_item)
        return True

    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix=f"{platform}-visual") as executor:
        for _ in range(max_workers):
            if not submit_next(executor):
                break

        while future_map:
            if cancel_check and cancel_check():
                return build_cancelled_result()

            completed_futures, _ = wait(tuple(future_map.keys()), timeout=0.5, return_when=FIRST_COMPLETED)
            if not completed_futures:
                continue

            for future in completed_futures:
                identifier, review_item = future_map.pop(future)
                try:
                    result = future.result()
                    results[identifier] = {
                        "username": identifier,
                        "decision": result.get("decision"),
                        "reason": result.get("reason"),
                        "signals": result.get("signals") or [],
                        "provider": result.get("provider"),
                        "cover_count": result.get("cover_count"),
                        "candidate_cover_count": result.get("candidate_cover_count"),
                        "skipped_cover_count": result.get("skipped_cover_count"),
                        "reviewed_at": result.get("reviewed_at"),
                        "attempt_count": result.get("attempt_count"),
                    }
                except Exception as exc:
                    results[identifier] = {
                        "username": identifier,
                        "success": False,
                        "error": str(exc),
                        "reviewed_at": iso_now(),
                    }

                completed += 1
                save_visual_results(platform, results)
                partial_result = build_visual_review_partial_result(platform, results, targets)
                if progress_callback:
                    progress_callback(
                        "reviewing",
                        f"已完成第 {completed}/{len(targets)} 个账号：{identifier}",
                        done=completed,
                        total=len(targets),
                        partial_result=partial_result,
                        current_identifier=identifier,
                        max_workers=max_workers,
                    )

                submit_next(executor)

    final_result = build_visual_review_partial_result(platform, results, targets)
    final_result.update({
        "success": True,
        "message": (
            f"{UPLOAD_PLATFORM_RESPONSE_LABELS.get(platform, platform)} 视觉复核完成，"
            f"共处理 {len(targets)} 个账号。"
        ),
        "visual_results": results,
        "max_workers": max_workers,
        "elapsed_seconds": round(time.monotonic() - started_at, 3),
    })
    return final_result


def get_scrape_identifiers(platform, payload):
    if platform == "tiktok":
        return [str(item) for item in (payload.get("profiles") or []) if str(item).strip()]
    if platform == "instagram":
        return [str(item) for item in (payload.get("usernames") or []) if str(item).strip()]
    if platform == "youtube":
        mode = str(payload.get("mode") or "channel").strip().lower()
        if mode == "search":
            return [str(item) for item in (payload.get("queries") or []) if str(item).strip()]
        return [str(item) for item in (payload.get("urls") or []) if str(item).strip()]
    return []


def derive_identifiers_from_upload_metadata(platform):
    metadata_lookup = load_upload_metadata(platform)
    if platform == "youtube":
        values = []
        for identifier, metadata in metadata_lookup.items():
            if isinstance(metadata, dict) and metadata.get("url"):
                values.append(str(metadata["url"]))
            else:
                values.append(screening.build_canonical_profile_url(platform, identifier))
        return values
    return [screening.build_canonical_profile_url(platform, identifier) for identifier in metadata_lookup.keys()]


def resolve_requested_identifiers(platform, payload):
    requested = get_scrape_identifiers(platform, payload)
    if requested:
        return requested
    return derive_identifiers_from_upload_metadata(platform)


def build_actor_input(platform, batch, payload):
    if platform == "tiktok":
        return {
            "profiles": batch,
            "resultsPerPage": int(payload.get("limit", 20)),
            "excludePinnedPosts": bool(payload.get("excludePinnedPosts", False)),
            "shouldDownloadVideos": bool(payload.get("downloadVideos", False)),
            "shouldDownloadCovers": bool(payload.get("downloadCovers", True)),
            "shouldDownloadAvatars": bool(payload.get("downloadAvatars", False)),
            "shouldDownloadSlideshowImages": bool(payload.get("downloadSlideshow", False) or payload.get("downloadCovers", True)),
        }
    if platform == "instagram":
        return {
            "usernames": batch,
            "includeAboutSection": bool(payload.get("includeAbout", True)),
        }
    mode = str(payload.get("mode") or "channel").strip().lower()
    input_payload = {
        "maxResults": int(payload.get("limit", 10)),
        "maxResultsShorts": 0,
        "maxResultStreams": 0,
        "subtitlesLanguage": "en",
        "downloadSubtitles": bool(payload.get("downloadSubtitles", False)),
        "hasCC": bool(payload.get("hasCC", False)),
    }
    if mode == "search":
        input_payload["searchQueries"] = batch
    else:
        input_payload["startUrls"] = [{"url": item} for item in batch]
    return input_payload


def apify_request(method, url, *, token, params=None, json_payload=None):
    request_params = dict(params or {})
    request_params["token"] = token
    normalized_method = str(method or "GET").strip().upper()
    max_attempts = APIFY_TRANSPORT_MAX_RETRIES if normalized_method == "GET" else 1
    last_error = None
    for attempt in range(1, max_attempts + 1):
        try:
            return requests.request(
                normalized_method,
                url,
                params=request_params,
                json=json_payload,
                timeout=APIFY_REQUEST_TIMEOUT,
            )
        except requests.exceptions.RequestException as exc:
            last_error = exc
            if attempt >= max_attempts:
                raise
            time.sleep(APIFY_TRANSPORT_RETRY_BACKOFF_SECONDS * attempt)
    if last_error is not None:
        raise last_error
    raise RuntimeError("Apify request failed without a captured exception")


def extract_apify_response_error(response):
    try:
        payload = response.json()
    except Exception:
        return response.text.strip() or f"HTTP {response.status_code}"
    if isinstance(payload, dict):
        error = payload.get("error") or {}
        if isinstance(error, dict):
            return error.get("message") or json.dumps(error, ensure_ascii=False)
        return str(error or payload)
    return str(payload)


def start_apify_run(actor_id, input_data, token):
    actor_ref = actor_id.replace("/", "~")
    url = f"{APIFY_API_BASE}/acts/{actor_ref}/runs"
    response = apify_request("POST", url, token=token, json_payload=input_data)
    if response.status_code not in (200, 201):
        raise ApifyStartError(
            response.status_code,
            f"启动 Apify 任务失败：{extract_apify_response_error(response)}",
        )
    payload = response.json() or {}
    data = payload.get("data") or {}
    run_id = data.get("id")
    dataset_id = data.get("defaultDatasetId")
    if not run_id or not dataset_id:
        raise RuntimeError("Apify API 未返回 run_id 或 dataset_id")
    return data


def start_apify_run_guarded(actor_id, input_data, token):
    guard_key = build_apify_guard_key(actor_id, input_data)
    existing_guard = get_apify_run_guard(guard_key)
    if isinstance(existing_guard, dict):
        existing_run_id = str(existing_guard.get("run_id") or "").strip()
        existing_dataset_id = str(existing_guard.get("dataset_id") or "").strip()
        if existing_run_id and existing_dataset_id:
            return {
                "run_data": {
                    "id": existing_run_id,
                    "defaultDatasetId": existing_dataset_id,
                    "status": existing_guard.get("status") or "RUNNING",
                },
                "guard_key": guard_key,
                "reused_guard": True,
            }
        raise RuntimeError(
            "检测到同一批次的本地提交保护记录，但尚未拿到可恢复的 run_id。"
            "为避免重复扣费，本次不会重复提交；如确认上次未成功提交，请手动清理 apify_run_guards.json 后重试。"
        )

    remember_apify_run_guard(
        guard_key,
        build_apify_run_guard_record(
            actor_id,
            input_data,
            token,
            status="submitting",
            request_key=guard_key,
        ),
    )

    try:
        run_data = start_apify_run(actor_id, input_data, token)
    except ApifyStartError as exc:
        if exc.retryable_with_next_token:
            clear_apify_run_guard(guard_key)
        else:
            remember_apify_run_guard(
                guard_key,
                build_apify_run_guard_record(
                    actor_id,
                    input_data,
                    token,
                    status="start_failed",
                    request_key=guard_key,
                    status_code=exc.status_code,
                    retryable_with_next_token=exc.retryable_with_next_token,
                    uncertain_submission=exc.uncertain_submission,
                    error=str(exc),
                ),
            )
        raise
    except Exception as exc:
        remember_apify_run_guard(
            guard_key,
            build_apify_run_guard_record(
                actor_id,
                input_data,
                token,
                status="start_failed",
                request_key=guard_key,
                error=str(exc),
            ),
        )
        raise RuntimeError(
            f"启动 Apify run 失败，已保留本地 guard 防止重复提交：{exc}"
        ) from exc

    remember_apify_run_guard(
        guard_key,
        build_apify_run_guard_record(
            actor_id,
            input_data,
            token,
            status=run_data.get("status") or "RUNNING",
            request_key=guard_key,
            run_id=run_data.get("id"),
            dataset_id=run_data.get("defaultDatasetId"),
        ),
    )
    return {
        "run_data": run_data,
        "guard_key": guard_key,
        "reused_guard": False,
    }


def poll_apify_run(token, run_id, cancel_check=None):
    url = f"{APIFY_API_BASE}/actor-runs/{run_id}"
    while True:
        if cancel_check and cancel_check():
            return {"cancelled": True}
        response = apify_request("GET", url, token=token)
        if response.status_code != 200:
            raise RuntimeError(f"查询 Apify run 失败：{extract_apify_response_error(response)}")
        payload = response.json() or {}
        data = payload.get("data") or {}
        status = str(data.get("status") or "").upper()
        if status in {"SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"}:
            return {"status": status, "run_data": data}
        time.sleep(APIFY_POLL_INTERVAL_SECONDS)


def download_apify_dataset_items(token, dataset_id):
    url = f"{APIFY_API_BASE}/datasets/{dataset_id}/items"
    response = apify_request("GET", url, token=token)
    if response.status_code != 200:
        raise RuntimeError(f"下载 Apify 数据集失败：{extract_apify_response_error(response)}")
    payload = response.json()
    return payload if isinstance(payload, list) else [payload]


def extract_returned_identifiers(platform, items):
    identifiers = set()
    for item in items or []:
        if platform == "tiktok":
            candidate = ((item.get("authorMeta") or {}).get("profileUrl")) or ((item.get("authorMeta") or {}).get("name"))
        elif platform == "instagram":
            candidate = item.get("url") or item.get("username")
        else:
            candidate = item.get("channelUrl") or item.get("channelName") or ((item.get("aboutChannelInfo") or {}).get("channelUrl"))
        identifier = screening.extract_platform_identifier(platform, candidate)
        if identifier:
            identifiers.add(identifier)
    return identifiers


def merge_scrape_items(platform, existing_items, new_items):
    if platform == "instagram":
        merged = {}
        for item in list(existing_items or []) + list(new_items or []):
            identifier = screening.extract_platform_identifier(platform, item.get("url") or item.get("username"))
            if not identifier:
                continue
            merged[identifier] = item
        return list(merged.values())
    return list(existing_items or []) + list(new_items or [])


def run_apify_batch(platform, batch, payload, progress_callback=None, cancel_check=None):
    actor_id = PLATFORM_ACTORS[platform]
    token_candidates = acquire_apify_token_candidates()
    if not token_candidates:
        raise RuntimeError("缺少 Apify 配置：请设置 APIFY_TOKEN 或 APIFY_API_TOKEN")

    input_data = build_actor_input(platform, batch, payload)
    estimated_batch_cost_usd = estimate_apify_batch_cost_usd(platform, batch, payload)
    required_budget_usd = apply_apify_budget_guard_band(estimated_batch_cost_usd)
    attempted_messages = []
    run_data = None
    token = None
    guard_key = None
    selected_snapshot = None
    checked_snapshots = []
    insufficient_snapshots = []
    query_errors = []
    reused_guard = False
    attempted_tokens = set()

    for attempt_index in range(1, len(token_candidates) + 1):
        available_candidates = [candidate for candidate in token_candidates if candidate not in attempted_tokens]
        if not available_candidates:
            break
        token_pick = select_apify_token_for_batch(available_candidates, required_budget_usd)
        checked_snapshots = token_pick.get("checked_snapshots") or checked_snapshots
        insufficient_snapshots = token_pick.get("insufficient_snapshots") or insufficient_snapshots
        query_errors = token_pick.get("query_errors") or query_errors
        candidate = str(token_pick.get("selected_token") or "").strip()
        selected_snapshot = token_pick.get("selected_snapshot")
        if not candidate:
            raise RuntimeError(token_pick.get("error") or "没有可用的 Apify token 可以覆盖当前批次预算。")

        if progress_callback:
            rotation_hint = "，已切换备用 token" if attempt_index > 1 else ""
            progress_callback(
                "provider_start",
                f"正在提交 Apify 任务{rotation_hint}",
                done=1,
                total=4,
                token_attempt=attempt_index,
                token_pool_size=len(token_candidates),
                token_masked=selected_snapshot.get("masked") if isinstance(selected_snapshot, dict) else mask_apify_token(candidate),
                estimated_batch_cost_usd=estimated_batch_cost_usd,
                required_budget_usd=required_budget_usd,
                **build_target_preview(batch),
            )
        try:
            guarded_start = start_apify_run_guarded(actor_id, input_data, candidate)
            run_data = guarded_start.get("run_data") or {}
            guard_key = guarded_start.get("guard_key")
            reused_guard = bool(guarded_start.get("reused_guard"))
            token = candidate
            break
        except ApifyStartError as exc:
            attempted_messages.append(str(exc))
            attempted_tokens.add(candidate)
            if not exc.retryable_with_next_token or attempt_index >= len(token_candidates):
                raise
            continue

    if not token or not run_data:
        raise RuntimeError(
            "所有 Apify token 启动任务失败"
            + (f"：{attempted_messages[-1]}" if attempted_messages else "")
        )

    run_id = run_data.get("id")
    dataset_id = run_data.get("defaultDatasetId")

    if progress_callback:
        progress_callback(
            "provider_running",
            f"Apify run {'已复用本地 guard 记录并继续等待' if reused_guard else '已创建，等待远端完成'}（run_id={run_id}）",
            done=2,
            total=4,
            apify_run_id=run_id,
            apify_dataset_id=dataset_id,
            token_masked=selected_snapshot.get("masked") if isinstance(selected_snapshot, dict) else mask_apify_token(token),
            estimated_batch_cost_usd=estimated_batch_cost_usd,
            required_budget_usd=required_budget_usd,
            reused_guard=reused_guard,
            **build_target_preview(batch),
        )
    poll_result = poll_apify_run(token, run_id, cancel_check=cancel_check)
    if poll_result.get("cancelled"):
        return build_cancelled_result()

    final_status = poll_result.get("status")
    final_run_data = poll_result.get("run_data") or {}
    if final_status != "SUCCEEDED":
        if guard_key:
            clear_apify_run_guard(guard_key)
        raise RuntimeError(f"Apify run 结束状态异常：{final_status}")

    if progress_callback:
        progress_callback(
            "downloading",
            "正在下载 Apify 数据集结果",
            done=3,
            total=4,
            apify_run_id=run_id,
            apify_dataset_id=dataset_id,
            token_masked=selected_snapshot.get("masked") if isinstance(selected_snapshot, dict) else mask_apify_token(token),
            **build_target_preview(batch),
        )

    items = download_apify_dataset_items(token, dataset_id)
    if guard_key:
        clear_apify_run_guard(guard_key)
    usage_total_usd = None
    usage = final_run_data.get("usageTotalUsd")
    try:
        if usage is not None:
            usage_total_usd = float(usage)
    except Exception:
        usage_total_usd = None

    return {
        "success": True,
        "raw_items": items,
        "apify": {
            "actor_id": actor_id,
            "apify_run_id": run_id,
            "apify_dataset_id": dataset_id,
            "usage_total_usd": usage_total_usd,
            "status": final_status,
            "token_masked": selected_snapshot.get("masked") if isinstance(selected_snapshot, dict) else mask_apify_token(token),
            "estimated_batch_cost_usd": estimated_batch_cost_usd,
            "required_budget_usd": required_budget_usd,
            "remaining_monthly_usage_usd": (
                selected_snapshot.get("remaining_monthly_usage_usd")
                if isinstance(selected_snapshot, dict)
                else None
            ),
            "reused_guard": reused_guard,
            "guard_key": guard_key,
            "budget_query_errors": query_errors,
            "insufficient_budget_tokens": insufficient_snapshots,
            "checked_budget_tokens": [
                {
                    "token_masked": item.get("masked"),
                    "remaining_monthly_usage_usd": item.get("remaining_monthly_usage_usd"),
                    "checked_at": item.get("checked_at"),
                }
                for item in checked_snapshots
            ],
        },
    }


def build_partial_scrape_result(platform, raw_items, expected_identifiers):
    filtered = screening.filter_scraped_items(
        platform,
        raw_items,
        expected_profiles=expected_identifiers,
        upload_metadata_lookup=load_upload_metadata(platform),
        active_rulespec=load_active_rulespec(),
    )
    save_profile_reviews(platform, filtered.get("profile_reviews") or [])
    return {
        "platform": platform,
        "raw_count": len(raw_items or []),
        "profile_reviews": filtered.get("profile_reviews") or [],
        "successful_identifiers": filtered.get("successful_identifiers") or [],
    }


def perform_scrape(platform, payload, progress_callback=None, cancel_check=None):
    identifiers = resolve_requested_identifiers(platform, payload)
    if not identifiers:
        return {"success": False, "error": "没有可抓取的账号，请先上传名单或在 payload 中传 identifiers"}

    batches = chunk_list(identifiers, PLATFORM_BATCH_SIZES.get(platform, 20))
    aggregated_items = []
    apify_runs = []

    if progress_callback:
        progress_callback("preparing", "正在准备抓取任务", done=0, total=len(batches), **build_target_preview(identifiers))

    for index, batch in enumerate(batches, start=1):
        if cancel_check and cancel_check():
            return build_cancelled_result()
        batch_result = run_apify_batch(
            platform,
            batch,
            payload,
            progress_callback=progress_callback,
            cancel_check=cancel_check,
        )
        if batch_result.get("cancelled"):
            return batch_result
        aggregated_items = merge_scrape_items(platform, aggregated_items, batch_result.get("raw_items") or [])
        apify_runs.append(batch_result.get("apify") or {})
        write_json_file(get_raw_data_path(platform), aggregated_items)
        partial_result = build_partial_scrape_result(platform, aggregated_items, identifiers[: index * PLATFORM_BATCH_SIZES.get(platform, 20)])
        if progress_callback:
            progress_callback(
                "batch_completed",
                f"第 {index}/{len(batches)} 批完成",
                done=index,
                total=len(batches),
                partial_result=partial_result,
                batch_index=index,
                batch_total=len(batches),
                **build_target_preview(batch),
            )

    filtered = screening.filter_scraped_items(
        platform,
        aggregated_items,
        expected_profiles=identifiers,
        upload_metadata_lookup=load_upload_metadata(platform),
        active_rulespec=load_active_rulespec(),
    )
    save_profile_reviews(platform, filtered.get("profile_reviews") or [])
    result = {
        "success": True,
        "platform": platform,
        "requested_total": len(identifiers),
        "raw_count": len(aggregated_items),
        "profile_reviews": filtered.get("profile_reviews") or [],
        "successful_identifiers": filtered.get("successful_identifiers") or [],
        "profile_reviews_path": get_profile_reviews_path(platform),
        "raw_data_path": get_raw_data_path(platform),
        "apify": {
            "runs": apify_runs,
            "execution_method": "rest-batched",
            "actor_id": PLATFORM_ACTORS[platform],
            "usage_total_usd": round(
                sum(item.get("usage_total_usd") or 0 for item in apify_runs if isinstance(item, dict)),
                6,
            ) if apify_runs else None,
        },
        "message": (
            f"{UPLOAD_PLATFORM_RESPONSE_LABELS.get(platform, platform)} 抓取完成，"
            f"返回 {len(filtered.get('successful_identifiers') or [])}/{len(identifiers)} 个账号。"
        ),
    }
    return result


def merge_upload_metadata_into_review_item(platform, item):
    metadata_lookup = load_upload_metadata(platform)
    normalized = dict(item or {})
    identifier = screening.resolve_profile_review_identifier(platform, normalized)
    canonical = metadata_lookup.get(identifier) if identifier else None
    if isinstance(canonical, dict):
        normalized["upload_metadata"] = dict(canonical)
    else:
        normalized["upload_metadata"] = dict(normalized.get("upload_metadata") or {})
    if not normalized.get("profile_url") and normalized["upload_metadata"].get("url"):
        normalized["profile_url"] = normalized["upload_metadata"]["url"]
    if not normalized.get("username") and normalized["upload_metadata"].get("handle"):
        normalized["username"] = normalized["upload_metadata"]["handle"]
    return normalized


def merge_upload_metadata_into_reviews(platform, profile_reviews):
    return [merge_upload_metadata_into_review_item(platform, item) for item in (profile_reviews or []) if isinstance(item, dict)]


def append_upload_metadata_to_export_row(row, review_item):
    metadata = dict(review_item.get("upload_metadata") or {})
    for export_key, metadata_key in UPLOAD_METADATA_EXPORT_FIELDS:
        row[export_key] = metadata.get(metadata_key, "")
    return row


def append_runtime_stats_to_export_row(row, review_item):
    stats = dict(review_item.get("stats") or {})
    row["runtime_avg_views"] = stats.get("avg_views", "")
    row["runtime_median_views"] = stats.get("median_views", "")
    row["runtime_video_count"] = stats.get("video_count", "")
    return row


def format_export_review_status(status):
    mapping = {
        "Pass": "Pass",
        "Reject": "Reject",
        "Missing": "Missing",
        "Not Reviewed": "Not Reviewed",
        "Error": "Error",
    }
    return mapping.get(str(status or "").strip(), str(status or "").strip())


def build_export_row_base(platform, review_item):
    merged_item = merge_upload_metadata_into_review_item(platform, review_item)
    identifier = screening.resolve_profile_review_identifier(platform, merged_item)
    row = {
        "platform": platform,
        "identifier": identifier,
        "username": merged_item.get("username", ""),
        "profile_url": merged_item.get("profile_url", ""),
    }
    append_upload_metadata_to_export_row(row, merged_item)
    append_runtime_stats_to_export_row(row, merged_item)
    return row, merged_item


def build_prescreen_review_rows(platform, profile_reviews):
    rows = []
    for item in merge_upload_metadata_into_reviews(platform, profile_reviews):
        row, review_item = build_export_row_base(platform, item)
        covers = review_item.get("covers") or []
        row.update({
            "status": format_export_review_status(review_item.get("status")),
            "stage_status": format_export_review_status(review_item.get("status")),
            "reason": review_item.get("reason", ""),
            "latest_post_time": review_item.get("latest_post_time"),
            "soft_flags": "；".join(str(flag) for flag in (review_item.get("soft_flags") or []) if str(flag).strip()),
            "cover_count": len(covers),
        })
        rows.append(row)
    return rows


def build_image_review_rows(platform, profile_reviews):
    rows = []
    for item in merge_upload_metadata_into_reviews(platform, profile_reviews):
        row, review_item = build_export_row_base(platform, item)
        covers = review_item.get("covers") or []
        row.update({
            "status": format_export_review_status(review_item.get("status")),
            "stage_status": format_export_review_status(review_item.get("status")),
            "stage_reason": review_item.get("reason", ""),
            "reason": review_item.get("reason", ""),
            "latest_post_time": review_item.get("latest_post_time"),
            "soft_flags": "；".join(str(flag) for flag in (review_item.get("soft_flags") or []) if str(flag).strip()),
            "cover_count": len(covers),
        })
        for index in range(9):
            row[f"cover_{index + 1}"] = covers[index] if index < len(covers) else ""
        rows.append(row)
    return rows


def merge_visual_results(platform, requested_visual_results):
    saved = load_visual_results(platform)
    merged = {}
    for source in (saved, requested_visual_results or {}):
        if not isinstance(source, dict):
            continue
        for key, value in source.items():
            if isinstance(value, dict):
                merged[key] = value
    return merged


def build_final_review_rows(platform, profile_reviews, visual_results):
    visual_lookup = {}
    for key, item in (visual_results or {}).items():
        identifier = screening.normalize_identifier(key or (item or {}).get("username"))
        if identifier and isinstance(item, dict):
            visual_lookup[identifier] = item

    rows = []
    for item in merge_upload_metadata_into_reviews(platform, profile_reviews):
        row, review_item = build_export_row_base(platform, item)
        identifier = screening.resolve_profile_review_identifier(platform, review_item)
        prescreen_status = str(review_item.get("status") or "").strip()
        prescreen_reason = str(review_item.get("reason") or "").strip()
        visual = visual_lookup.get(identifier) or {}
        visual_status = "Not Reviewed"
        visual_reason = ""
        visual_signals = ""
        final_status = prescreen_status
        final_reason = prescreen_reason
        if prescreen_status == "Pass" and visual:
            if visual.get("success") is False:
                visual_status = "Error"
                visual_reason = str(visual.get("error") or "").strip()
                final_status = "Error"
                final_reason = visual_reason or prescreen_reason
            else:
                visual_status = str(visual.get("decision") or "Pass").strip() or "Pass"
                visual_reason = str(visual.get("reason") or "").strip()
                visual_signals = "；".join(str(item) for item in (visual.get("signals") or []) if str(item).strip())
                final_status = visual_status
                final_reason = visual_reason or prescreen_reason
        row.update({
            "prescreen_status": format_export_review_status(prescreen_status),
            "prescreen_reason": prescreen_reason,
            "visual_status": format_export_review_status(visual_status),
            "visual_reason": visual_reason,
            "visual_signals": visual_signals,
            "status": format_export_review_status(final_status),
            "reason": final_reason,
            "final_status": format_export_review_status(final_status),
            "final_reason": final_reason,
        })
        rows.append(row)
    return rows


def workbook_bytes_from_sheets(sheet_payloads):
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        for sheet_name, rows in sheet_payloads:
            pd.DataFrame(rows).to_excel(writer, index=False, sheet_name=sheet_name)
    output.seek(0)
    return output


@app.route("/api/health", methods=["GET"])
def health_check():
    ensure_runtime_dirs()
    token_pool = get_apify_token_pool()
    return jsonify({
        "status": "ok",
        "smoke_ready": True,
        "checks": {
            "apify": "configured" if token_pool else "unconfigured",
            "apify_token_pool_size": len(token_pool),
            "vision": "configured" if get_available_vision_provider_names() else "unconfigured",
            "vision_providers": get_available_vision_provider_names(),
            "origins": BACKEND_ALLOWED_ORIGINS,
        },
    })


@app.route("/api/apify/balance", methods=["GET"])
def apify_balance():
    payload, status_code = build_apify_balance_payload()
    return jsonify(payload), status_code


@app.route("/api/apify/balance/refresh", methods=["POST"])
def refresh_apify_balance():
    payload, status_code = build_apify_balance_payload(force_refresh=True)
    return jsonify(payload), status_code


@app.route("/apify/balance", methods=["GET"])
def apify_balance_dashboard():
    payload, status_code = build_apify_balance_payload()
    return render_template(
        "apify_balance_dashboard.html",
        refresh_interval_seconds=APIFY_BALANCE_REFRESH_INTERVAL_SECONDS,
        api_balance_path="/api/apify/balance",
        api_refresh_path="/api/apify/balance/refresh",
        initial_payload=payload,
        initial_status_code=status_code,
    )


@app.route("/api/upload", methods=["POST"])
def upload_file():
    ensure_runtime_dirs()
    if "file" not in request.files:
        return jsonify({"error": "No file part"}), 400
    file = request.files["file"]
    if not file or not file.filename:
        return jsonify({"error": "No selected file"}), 400

    filename = secure_filename(file.filename)
    filepath = os.path.join(app.config["UPLOAD_FOLDER"], filename)
    file.save(filepath)

    try:
        frames = load_canonical_upload_workbook_frames(filepath)
        if not frames:
            return build_upload_validation_error(
                "上传表没有可用数据行",
                ["请确认至少有一个 sheet 包含表头和账号数据。"],
            )
        df = pd.concat(frames, ignore_index=True)
        parsed, error_response = parse_canonical_upload_workbook(df, filename)
        if error_response:
            return error_response
        for platform in PLATFORM_ACTORS:
            save_upload_metadata(platform, parsed["metadata_by_platform"].get(platform, {}), replace=True)
        return jsonify({
            "success": True,
            "filename": f"processed_{filename}",
            "stats": parsed["stats"],
            "preview": parsed["preview"],
            "grouped_data": parsed["grouped_data"],
            "metadata_counts": {
                platform: len(parsed["metadata_by_platform"].get(platform, {}))
                for platform in PLATFORM_ACTORS
            },
        })
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/api/rulespec/compile", methods=["POST"])
def compile_rulespec_api():
    payload = request.get_json(silent=True) or {}
    sop_text = payload.get("sop_text")
    persist = bool(payload.get("persist", True))
    try:
        compiled = rules_module.compile_rulespec_from_text(sop_text)
        if persist:
            persist_active_rulespec(compiled)
        return jsonify({
            "success": True,
            "rule_spec": compiled.get("rule_spec") or {},
            "field_match_report": compiled.get("field_match_report") or {},
            "missing_capabilities": compiled.get("missing_capabilities") or {},
            "compiled_at": compiled.get("compiled_at"),
            "persisted": persist,
        })
    except ValueError as exc:
        return jsonify({"success": False, "error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"success": False, "error": str(exc)}), 500


@app.route("/api/jobs/scrape", methods=["POST"])
def start_scrape_job():
    payload = request.get_json(silent=True) or {}
    platform = str(payload.get("platform") or "").strip().lower()
    data = payload.get("payload") or {}
    if platform not in PLATFORM_ACTORS:
        return jsonify({"success": False, "error": "平台参数无效"}), 400
    if not isinstance(data, dict):
        return jsonify({"success": False, "error": "payload 必须是对象"}), 400
    if not get_apify_token_pool():
        return jsonify({
            "success": False,
            "error_code": "MISSING_APIFY_CONFIG",
            "error": "缺少 Apify 配置：请设置 APIFY_TOKEN 或 APIFY_API_TOKEN。",
        }), 400

    job = create_job("scrape", platform=platform, message="采集任务已创建")
    start_background_job(job, lambda progress_callback, cancel_check: perform_scrape(platform, data, progress_callback, cancel_check))
    return jsonify({"success": True, "job": get_job(job["id"])})


@app.route("/api/jobs/visual-review", methods=["POST"])
def start_visual_review_job():
    payload = request.get_json(silent=True) or {}
    platform = str(payload.get("platform") or "").strip().lower()
    data = payload.get("payload") or {}
    if platform not in PLATFORM_ACTORS:
        return jsonify({"success": False, "error": "平台参数无效"}), 400
    if not isinstance(data, dict):
        return jsonify({"success": False, "error": "payload 必须是对象"}), 400
    if not get_available_vision_providers():
        return jsonify({
            "success": False,
            "error_code": "MISSING_VISION_CONFIG",
            "error": "缺少视觉模型配置：请设置 OPENAI_API_KEY、VISION_QUAN2GO_API_KEY 或 VISION_LEMONAPI_API_KEY。",
        }), 400

    job = create_job("visual-review", platform=platform, message="视觉复核任务已创建")
    start_background_job(job, lambda progress_callback, cancel_check: perform_visual_review(platform, data, progress_callback, cancel_check))
    return jsonify({"success": True, "job": get_job(job["id"])})


@app.route("/api/jobs/<job_id>", methods=["GET"])
def get_job_status(job_id):
    job = get_job(job_id)
    if not job:
        return jsonify({"success": False, "error": "未找到任务"}), 404
    return jsonify({"success": True, "job": job})


@app.route("/api/jobs/<job_id>/cancel", methods=["POST"])
def cancel_job(job_id):
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        if not job:
            return jsonify({"success": False, "error": "未找到任务"}), 404
        if job["status"] in JOB_TERMINAL_STATUSES:
            return jsonify({"success": False, "error": "任务已结束，无法取消"}), 400
        job["cancel_requested"] = True
        job["status"] = "cancelling"
        job["stage"] = "cancelling"
        job["message"] = "正在取消任务"
        job["updated_at"] = iso_now()
    return jsonify({"success": True, "job": get_job(job_id)})


@app.route("/api/results/<platform>", methods=["GET"])
def get_results(platform):
    if platform not in PLATFORM_ACTORS:
        return jsonify({"error": "Invalid platform"}), 400
    return jsonify(load_json_payload(get_raw_data_path(platform), default=[]))


@app.route("/api/artifacts/<platform>/status", methods=["GET"])
def artifact_status(platform):
    if platform not in PLATFORM_ACTORS:
        return jsonify({"error": "Invalid platform"}), 400
    profile_reviews = load_profile_reviews(platform)
    visual_results = load_visual_results(platform)
    return jsonify({
        "platform": platform,
        "raw_data_path": get_raw_data_path(platform),
        "profile_reviews_path": get_profile_reviews_path(platform),
        "visual_results_path": get_visual_results_path(platform),
        "raw_count": len(load_json_payload(get_raw_data_path(platform), default=[])),
        "profile_review_count": len(profile_reviews),
        "saved_final_review_artifacts_available": bool(profile_reviews and visual_results),
    })


@app.route("/api/download/<platform>/prescreen-review", methods=["GET"])
def download_prescreen_review(platform):
    if platform not in PLATFORM_ACTORS:
        return jsonify({"error": "Invalid platform"}), 400
    rows = build_prescreen_review_rows(platform, load_profile_reviews(platform))
    if not rows:
        return jsonify({"error": "Profile review data is empty"}), 400
    output = workbook_bytes_from_sheets([("Prescreen Review", rows)])
    return send_file(
        output,
        as_attachment=True,
        download_name=f"{platform}_prescreen_review.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@app.route("/api/download/<platform>/image-review", methods=["GET"])
def download_image_review(platform):
    if platform not in PLATFORM_ACTORS:
        return jsonify({"error": "Invalid platform"}), 400
    rows = build_image_review_rows(platform, load_profile_reviews(platform))
    if not rows:
        return jsonify({"error": "Profile review data is empty"}), 400
    output = workbook_bytes_from_sheets([("Image Review", rows)])
    return send_file(
        output,
        as_attachment=True,
        download_name=f"{platform}_image_review.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@app.route("/api/download/<platform>/test-info", methods=["GET"])
def download_test_info(platform):
    if platform not in PLATFORM_ACTORS:
        return jsonify({"error": "Invalid platform"}), 400
    raw_items = load_json_payload(get_raw_data_path(platform), default=[]) or []
    profile_reviews = load_profile_reviews(platform)
    if not raw_items and not profile_reviews:
        return jsonify({"error": "No test data available to export"}), 404
    summary_rows = build_prescreen_review_rows(platform, profile_reviews)
    raw_rows = pd.json_normalize(raw_items, sep=".").to_dict(orient="records") if raw_items else []
    output = workbook_bytes_from_sheets([
        ("Profile Reviews", summary_rows),
        ("Raw Data", raw_rows),
    ])
    return send_file(
        output,
        as_attachment=True,
        download_name=f"{platform}_test_info.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@app.route("/api/download/<platform>/test-info-json", methods=["GET"])
def download_test_info_json(platform):
    if platform not in PLATFORM_ACTORS:
        return jsonify({"error": "Invalid platform"}), 400
    payload = {
        "platform": platform,
        "profile_reviews": merge_upload_metadata_into_reviews(platform, load_profile_reviews(platform)),
        "raw_items": load_json_payload(get_raw_data_path(platform), default=[]) or [],
        "upload_metadata": load_upload_metadata(platform),
        "raw_source_path": get_raw_data_path(platform),
    }
    if not payload["profile_reviews"] and not payload["raw_items"] and not payload["upload_metadata"]:
        return jsonify({"error": "No test data available to export"}), 404
    response = app.response_class(
        response=json.dumps(payload, ensure_ascii=False, indent=2),
        mimetype="application/json",
    )
    response.headers["Content-Disposition"] = f"attachment; filename={platform}_test_info.json"
    return response


@app.route("/api/download/<platform>/final-review", methods=["POST"])
def download_final_review(platform):
    if platform not in PLATFORM_ACTORS:
        return jsonify({"error": "Invalid platform"}), 400
    payload = request.get_json(silent=True) or {}
    profile_reviews = payload.get("profile_reviews")
    if profile_reviews is None:
        profile_reviews = load_profile_reviews(platform)
    if not isinstance(profile_reviews, list) or not profile_reviews:
        return jsonify({"error": "No profile review data available to export"}), 400
    visual_results = merge_visual_results(platform, payload.get("visual_results") or {})
    rows = build_final_review_rows(platform, profile_reviews, visual_results)
    if not rows:
        return jsonify({"error": "No final review rows available to export"}), 400
    output = workbook_bytes_from_sheets([("Final Review", rows)])
    return send_file(
        output,
        as_attachment=True,
        download_name=f"{platform}_final_review.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


if __name__ == "__main__":
    ensure_runtime_dirs()
    start_apify_balance_poller()
    app.run(
        host=BACKEND_BIND_HOST,
        port=BACKEND_PORT,
        debug=BACKEND_DEBUG,
        use_reloader=BACKEND_USE_RELOADER,
    )
