from __future__ import annotations

import json
import math
import re
import statistics
from pathlib import Path
from typing import Any

import pandas as pd

from backend import creator_cache
from backend.timezone_utils import format_shanghai_date
from email_sync.mail_parser import parse_email_message


_FULL_BODY_FIELD_NAME = "full body"
FINAL_UPLOAD_COLUMNS = (
    "达人ID",
    "平台",
    "主页链接",
    "# Followers(K)#",
    "Following",
    "Median Views (K)",
    "互动率",
    "当前网红报价",
    "达人最后一次回复邮件时间",
    _FULL_BODY_FIELD_NAME,
    "达人对接人",
    "ai是否通过",
    "ai筛号反馈理由",
    "标签(ai)",
    "ai评价",
)
_ROW_ATTACHMENT_PATHS_KEY = "__feishu_attachment_local_paths"
_SHARED_ATTACHMENT_PATHS_KEY = "__feishu_shared_attachment_local_paths"
_LAST_MAIL_RAW_PATH_KEY = "__last_mail_raw_path"
_ROW_UPDATE_MODE_KEY = "__feishu_update_mode"
_UPDATE_MODE_CREATE_OR_MAIL_ONLY = "create_or_mail_only_update"
_PLATFORM_ALIASES = {
    "tiktok": "tiktok",
    "tik tok": "tiktok",
    "instagram": "instagram",
    "ig": "instagram",
    "youtube": "youtube",
    "yt": "youtube",
}
_URL_HANDLE_PATTERNS = (
    re.compile(r"instagram\.com/([^/?#]+)/?", re.IGNORECASE),
    re.compile(r"tiktok\.com/@([^/?#]+)/?", re.IGNORECASE),
    re.compile(r"youtube\.com/@([^/?#]+)/?", re.IGNORECASE),
    re.compile(r"youtube\.com/c/([^/?#]+)/?", re.IGNORECASE),
    re.compile(r"youtube\.com/channel/([^/?#]+)/?", re.IGNORECASE),
)
_QUOTE_PATTERNS = (
    re.compile(r"\$\s?\d[\d,]*(?:\.\d+)?\s*[kK]?(?:\s*(?:USD))?(?:\s*(?:per|/|for)\s*[^,.;\n]{0,24})?", re.IGNORECASE),
    re.compile(r"\d[\d,]*(?:\.\d+)?\s*USD(?:\s*(?:per|/|for)\s*[^,.;\n]{0,24})?", re.IGNORECASE),
)
_PROCESSING_ERROR_PATTERNS = (
    re.compile(r"视觉复核超时", re.IGNORECASE),
    re.compile(r"定位卡分析超时", re.IGNORECASE),
    re.compile(r"HTTP\s*401", re.IGNORECASE),
    re.compile(r"认证失败", re.IGNORECASE),
    re.compile(r"额度已用尽", re.IGNORECASE),
    re.compile(r"\btimeout\b", re.IGNORECASE),
    re.compile(r"\breelx:", re.IGNORECASE),
    re.compile(r"\bquan2go:", re.IGNORECASE),
    re.compile(r"\bqiandao:", re.IGNORECASE),
    re.compile(r"\bopenai:", re.IGNORECASE),
)
_VISUAL_MANUAL_REVIEW_PATTERNS = (
    re.compile(r"视觉复核", re.IGNORECASE),
    re.compile(r"\bvisual\b", re.IGNORECASE),
    re.compile(r"\bimage review\b", re.IGNORECASE),
)
_INSTAGRAM_REEL_MARKER_PATTERNS = (
    re.compile(r"/reels?/", re.IGNORECASE),
    re.compile(r"\b(?:clips|reels?|igtv)\b", re.IGNORECASE),
)
_INSTAGRAM_VIDEO_MARKER_PATTERNS = (
    re.compile(r"\b(?:video|graphvideo|clips|reels?|igtv)\b", re.IGNORECASE),
)
_REQUIRED_UPLOAD_FIELDS = (
    "达人ID",
    "平台",
    "主页链接",
    "ai是否通过",
)


def _is_blank(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, float):
        return math.isnan(value)
    return False


def _clean_text(value: Any) -> str:
    if _is_blank(value):
        return ""
    return str(value).strip()


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        normalized = _clean_text(value)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
    return deduped


def _normalize_platform(value: Any) -> str:
    normalized = _clean_text(value).lower()
    return _PLATFORM_ALIASES.get(normalized, normalized)


def _normalize_url(value: Any) -> str:
    normalized = _clean_text(value).rstrip("/")
    return normalized.lower()


def _extract_handle(value: Any) -> str:
    raw = _clean_text(value)
    if not raw:
        return ""
    normalized = raw.strip()
    if normalized.startswith("@"):
        normalized = normalized[1:]
    normalized = normalized.rstrip("/")
    for pattern in _URL_HANDLE_PATTERNS:
        match = pattern.search(normalized)
        if match:
            return match.group(1).strip().lstrip("@").lower()
    return normalized.split("/")[-1].lstrip("@").lower()


def _coerce_number(value: Any) -> float | None:
    if _is_blank(value):
        return None
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return None if (isinstance(value, float) and math.isnan(value)) else float(value)
    raw = _clean_text(value).replace(",", "")
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _format_k_value(value: Any) -> int | float | str:
    numeric = _coerce_number(value)
    if numeric is None:
        return ""
    normalized = numeric / 1000.0
    rounded = round(normalized, 1)
    if math.isclose(rounded, round(rounded), rel_tol=0.0, abs_tol=1e-9):
        return int(round(rounded))
    return rounded


def _format_percentage(value: Any) -> str:
    numeric = _coerce_number(value)
    if numeric is None:
        return ""
    if numeric <= 1:
        numeric *= 100
    return f"{numeric:.1f}%"


def _format_date(value: Any) -> str:
    return format_shanghai_date(value)


def _first_non_blank(*values: Any) -> Any:
    for value in values:
        if not _is_blank(value):
            return value
    return ""


def _combine_reason_with_note(base_text: Any, note: str) -> str:
    cleaned_base = _clean_text(base_text)
    cleaned_note = _clean_text(note)
    if not cleaned_note:
        return cleaned_base
    if not cleaned_base:
        return cleaned_note
    if cleaned_note in cleaned_base:
        return cleaned_base
    return f"{cleaned_base}；{cleaned_note}"


def _build_quote_text(keep_row: dict[str, Any], *, mail_body_text: Any = "") -> str:
    latest_quote_text = _clean_text(keep_row.get("latest_quote_text"))
    for candidate in (
        latest_quote_text,
        _clean_text(keep_row.get("last_mail_snippet")),
        _clean_text(keep_row.get("brand_message_snippet")),
        _clean_text(mail_body_text),
    ):
        if not candidate:
            continue
        for pattern in _QUOTE_PATTERNS:
            match = pattern.search(candidate)
            if match:
                return match.group(0).strip()
    amount = _coerce_number(keep_row.get("latest_quote_amount"))
    currency = _clean_text(keep_row.get("latest_quote_currency"))
    if amount is None:
        return latest_quote_text
    if math.isclose(amount, round(amount), rel_tol=0.0, abs_tol=1e-9):
        amount_text = f"{int(round(amount)):,}"
    else:
        amount_text = f"{amount:,.2f}"
    if currency:
        return f"{currency} {amount_text}"
    return amount_text


def _compute_engagement_rate(final_row: dict[str, Any]) -> str:
    likes = _coerce_number(final_row.get("upload_avg_likes"))
    if likes is None:
        return ""
    views = _coerce_number(
        _first_non_blank(
            final_row.get("upload_median_views"),
            final_row.get("runtime_median_views"),
            final_row.get("upload_avg_views"),
            final_row.get("runtime_avg_views"),
        )
    )
    if views is None or views <= 0:
        return ""
    return _format_percentage(likes / views)


def _average(values: list[float]) -> float | None:
    normalized = [value for value in values if value is not None]
    if not normalized:
        return None
    return sum(normalized) / len(normalized)


def _median(values: list[float]) -> float | None:
    normalized = [value for value in values if value is not None]
    if not normalized:
        return None
    return float(statistics.median(normalized))


def _average_post_engagement_rate(post_pairs: list[tuple[float, float]]) -> float | None:
    rates: list[float] = []
    for likes, views in post_pairs:
        if likes is None or views is None or views <= 0:
            continue
        rates.append(float(likes) / float(views))
    if not rates:
        return None
    return sum(rates) / len(rates)


def _is_truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    normalized = _clean_text(value).casefold()
    return normalized in {"1", "true", "yes", "y"}


def _build_instagram_post_marker_text(post: dict[str, Any]) -> str:
    marker_parts: list[str] = []
    for key in (
        "url",
        "permalink",
        "shortCodeUrl",
        "postUrl",
        "type",
        "typeName",
        "__typename",
        "productType",
        "mediaType",
        "postType",
    ):
        value = _clean_text(post.get(key))
        if value:
            marker_parts.append(value.casefold())
    return " ".join(marker_parts)


def _is_instagram_reel_post(post: dict[str, Any]) -> bool:
    marker_text = _build_instagram_post_marker_text(post)
    return any(pattern.search(marker_text) for pattern in _INSTAGRAM_REEL_MARKER_PATTERNS)


def _extract_instagram_play_count(post: dict[str, Any]) -> float | None:
    for key in ("videoPlayCount", "videoViewCount", "playCount", "viewCount"):
        value = _coerce_number(post.get(key))
        if value is not None:
            return value
    return None


def _is_instagram_video_post(post: dict[str, Any]) -> bool:
    if _is_instagram_reel_post(post):
        return True
    if _extract_instagram_play_count(post) is not None:
        return True
    if _coerce_number(post.get("videoDuration")) is not None:
        return True
    if _clean_text(post.get("videoUrl")):
        return True
    if _is_truthy(post.get("isVideo")):
        return True
    marker_text = _build_instagram_post_marker_text(post)
    return any(pattern.search(marker_text) for pattern in _INSTAGRAM_VIDEO_MARKER_PATTERNS)


def _select_instagram_metric_posts(posts: Any) -> list[dict[str, Any]]:
    normalized_posts = [dict(post) for post in (posts or []) if isinstance(post, dict)]
    if not normalized_posts:
        return []
    reel_posts = [
        post
        for post in normalized_posts
        if _is_instagram_reel_post(post) and _extract_instagram_play_count(post) is not None
    ]
    if reel_posts:
        return reel_posts
    video_posts = [
        post
        for post in normalized_posts
        if _is_instagram_video_post(post) and _extract_instagram_play_count(post) is not None
    ]
    if video_posts:
        return video_posts
    return [post for post in normalized_posts if _extract_instagram_play_count(post) is not None]


def _resolve_metric_note(apify_row: dict[str, Any], avg_views: Any) -> str:
    followers = _coerce_number((apify_row or {}).get("followers"))
    avg_views_num = _coerce_number(avg_views)
    if followers is not None and avg_views_num is None:
        return "无视频播放数据"
    if followers is None and avg_views_num is None:
        return "无抓取数据，需人工确认"
    return ""


def _is_processing_error(*values: Any) -> bool:
    for value in values:
        text = _clean_text(value)
        if not text:
            continue
        if any(pattern.search(text) for pattern in _PROCESSING_ERROR_PATTERNS):
            return True
    return False


def _is_visual_manual_review_needed(*values: Any) -> bool:
    for value in values:
        text = _clean_text(value)
        if not text:
            continue
        if not _is_processing_error(text):
            continue
        if any(pattern.search(text) for pattern in _VISUAL_MANUAL_REVIEW_PATTERNS):
            return True
    return False


def _resolve_ai_pass_value(
    final_status: Any,
    *,
    visual_error_candidates: list[Any] | tuple[Any, ...] = (),
    other_error_candidates: list[Any] | tuple[Any, ...] = (),
) -> str:
    if _is_processing_error(*visual_error_candidates) or _is_visual_manual_review_needed(*visual_error_candidates):
        return "转人工"
    if _is_processing_error(*other_error_candidates):
        return "处理失败"
    normalized_status = _clean_text(final_status).lower()
    if normalized_status == "pass":
        return "是"
    return "否"


def _resolve_visual_manual_reason(*values: Any) -> str:
    normalized_values = [_clean_text(value) for value in values if _clean_text(value)]
    if not normalized_values:
        return ""
    for text in normalized_values:
        if "视觉复核超时" in text:
            return "视觉复核超时，需人工确认"
    if _is_processing_error(*normalized_values) or _is_visual_manual_review_needed(*normalized_values):
        return "视觉复核异常，需人工确认"
    return ""


def _resolve_positioning_stage_note(positioning_row: dict[str, Any]) -> tuple[str, str]:
    stage_status = _clean_text((positioning_row or {}).get("positioning_stage_status"))
    if not stage_status:
        return "", ""
    if stage_status == "Completed":
        return "", ""
    if stage_status == "Error":
        return "定位卡处理失败", "定位卡处理失败，需人工确认"
    if stage_status == "Not Reviewed":
        return "定位卡未完成", "定位卡未完成，需人工确认"
    return "", ""


def _collect_upload_validation_errors(row: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    missing_fields = [field for field in _REQUIRED_UPLOAD_FIELDS if _is_blank(row.get(field))]
    if missing_fields:
        errors.append(f"缺少关键字段: {', '.join(missing_fields)}")
    if _clean_text(row.get("ai是否通过")) == "处理失败":
        errors.append("系统处理失败")
    profile_url = _clean_text(row.get("主页链接"))
    if profile_url and "://" not in profile_url:
        errors.append("主页链接格式无效")
    return errors


def _apply_missing_ai_label_fallback(
    ai_pass_value: str,
    ai_label_value: str,
    screening_reason: str,
    screening_comment: str,
) -> tuple[str, str, str, str]:
    if _clean_text(ai_pass_value) != "是" or _clean_text(ai_label_value):
        return ai_pass_value, ai_label_value, screening_reason, screening_comment
    fallback_note = "缺少标签(ai)，已自动转人工"
    return (
        "转人工",
        ai_label_value,
        _combine_reason_with_note(screening_reason, fallback_note),
        _combine_reason_with_note(screening_comment, fallback_note),
    )


def _resolve_run_root_from_export(final_review_path: Path) -> Path | None:
    try:
        return final_review_path.expanduser().resolve().parents[2]
    except Exception:
        return None


def _resolve_existing_local_paths(*values: Any, base_dirs: list[Path | None] | None = None) -> list[str]:
    candidates: list[str] = []
    for value in values:
        if isinstance(value, (list, tuple, set)):
            for item in value:
                candidates.extend(_resolve_existing_local_paths(item, base_dirs=base_dirs))
            continue
        raw = _clean_text(value)
        if not raw:
            continue
        path = Path(raw).expanduser()
        path_candidates: list[Path] = []
        if path.is_absolute():
            path_candidates.append(path)
        else:
            path_candidates.append(path)
            for base_dir in base_dirs or []:
                if base_dir is None:
                    continue
                path_candidates.append(base_dir / path)
        for candidate in path_candidates:
            try:
                resolved = candidate.resolve()
            except Exception:
                continue
            if resolved.exists() and resolved.is_file():
                candidates.append(str(resolved))
                break
    return _dedupe_preserve_order(candidates)


def _extract_mail_body_from_raw_text(raw_text: str) -> str:
    cleaned = raw_text.replace("\r\n", "\n")
    if "\n\n" in cleaned:
        cleaned = cleaned.split("\n\n", 1)[1]
    return cleaned.strip()


def _load_mail_full_body_from_raw_path(
    raw_path: Any,
    *,
    base_dirs: list[Path | None] | None = None,
    cache: dict[str, str] | None = None,
) -> str:
    resolved_paths = _resolve_existing_local_paths(raw_path, base_dirs=base_dirs)
    if not resolved_paths:
        return ""
    resolved = str(Path(resolved_paths[0]).expanduser().resolve())
    if cache is not None and resolved in cache:
        return cache[resolved]

    full_body = ""
    try:
        raw_bytes = Path(resolved).read_bytes()
        parsed = parse_email_message(
            raw_bytes,
            account_email="",
            folder_name="",
            uid=0,
            uidvalidity=0,
            flags=[],
            internal_date_raw=None,
            size_bytes=len(raw_bytes),
        )
        full_body = _clean_text(parsed.body_text) or _clean_text(parsed.snippet)
        if not full_body:
            full_body = _extract_mail_body_from_raw_text(raw_bytes.decode("utf-8", errors="replace"))
    except Exception:
        try:
            full_body = _extract_mail_body_from_raw_text(Path(resolved).read_text(encoding="utf-8", errors="replace"))
        except Exception:
            full_body = ""

    if cache is not None:
        cache[resolved] = full_body
    return full_body


def _load_mail_snapshot_from_raw_path(
    raw_path: Any,
    *,
    base_dirs: list[Path | None] | None = None,
    cache: dict[str, dict[str, str]] | None = None,
) -> dict[str, str]:
    resolved_paths = _resolve_existing_local_paths(raw_path, base_dirs=base_dirs)
    if not resolved_paths:
        return {"full_body": "", "from_email": "", "sent_at": ""}
    resolved = str(Path(resolved_paths[0]).expanduser().resolve())
    if cache is not None and resolved in cache:
        return dict(cache[resolved])

    snapshot = {"full_body": "", "from_email": "", "sent_at": ""}
    try:
        raw_bytes = Path(resolved).read_bytes()
        parsed = parse_email_message(
            raw_bytes,
            account_email="",
            folder_name="",
            uid=0,
            uidvalidity=0,
            flags=[],
            internal_date_raw=None,
            size_bytes=len(raw_bytes),
        )
        snapshot["full_body"] = _clean_text(parsed.body_text) or _clean_text(parsed.snippet)
        if not snapshot["full_body"]:
            snapshot["full_body"] = _extract_mail_body_from_raw_text(raw_bytes.decode("utf-8", errors="replace"))
        for addresses in (parsed.from_addresses, parsed.sender_addresses, parsed.reply_to_addresses):
            for item in addresses:
                email = _clean_text((item or {}).get("address"))
                if email:
                    snapshot["from_email"] = email
                    break
            if snapshot["from_email"]:
                break
        snapshot["sent_at"] = _clean_text(parsed.sent_at) or _clean_text(parsed.sent_at_raw)
    except Exception:
        try:
            raw_text = Path(resolved).read_text(encoding="utf-8", errors="replace")
        except Exception:
            raw_text = ""
        snapshot["full_body"] = _extract_mail_body_from_raw_text(raw_text)

    if cache is not None:
        cache[resolved] = dict(snapshot)
    return snapshot


def _resolve_mail_full_body(
    keep_row: dict[str, Any],
    *,
    last_mail_raw_path: Any = "",
    base_dirs: list[Path | None] | None = None,
    cache: dict[str, str] | None = None,
) -> str:
    return _clean_text(
        _first_non_blank(
            keep_row.get("last_mail_full_body"),
            keep_row.get("brand_message_full_body"),
            keep_row.get("latest_external_full_body"),
            _load_mail_full_body_from_raw_path(
                last_mail_raw_path or keep_row.get("last_mail_raw_path") or keep_row.get("brand_message_raw_path"),
                base_dirs=base_dirs,
                cache=cache,
            ),
            keep_row.get("last_mail_snippet"),
            keep_row.get("brand_message_snippet"),
        )
    )


def _resolve_keep_row_mail_context(
    keep_row: dict[str, Any],
    *,
    last_mail_raw_path: Any = "",
    base_dirs: list[Path | None] | None = None,
    snapshot_cache: dict[str, dict[str, str]] | None = None,
) -> dict[str, str]:
    snapshot = _load_mail_snapshot_from_raw_path(
        last_mail_raw_path or keep_row.get("last_mail_raw_path") or keep_row.get("brand_message_raw_path"),
        base_dirs=base_dirs,
        cache=snapshot_cache,
    )
    full_body = _clean_text(
        _first_non_blank(
            keep_row.get("last_mail_full_body"),
            keep_row.get("brand_message_full_body"),
            keep_row.get("latest_external_full_body"),
            snapshot.get("full_body"),
            keep_row.get("last_mail_snippet"),
            keep_row.get("brand_message_snippet"),
        )
    )
    clean_body = _clean_text(
        _first_non_blank(
            keep_row.get("latest_external_clean_body"),
            keep_row.get("brand_message_snippet"),
            full_body[:1000] if full_body else "",
        )
    )
    return {
        "creator_emails": _clean_text(_first_non_blank(keep_row.get("creator_emails"), keep_row.get("Email"))),
        "matched_contact_email": _clean_text(
            _first_non_blank(
                keep_row.get("matched_contact_email"),
                keep_row.get("matched_email"),
            )
        ),
        "matched_contact_name": _clean_text(keep_row.get("matched_contact_name")),
        "latest_external_from": _clean_text(
            _first_non_blank(
                keep_row.get("latest_external_from"),
                snapshot.get("from_email"),
                keep_row.get("matched_contact_email"),
                keep_row.get("matched_email"),
            )
        ),
        "latest_external_sent_at": _clean_text(
            _first_non_blank(
                keep_row.get("latest_external_sent_at"),
                keep_row.get("last_mail_time"),
                keep_row.get("brand_message_sent_at"),
                snapshot.get("sent_at"),
            )
        ),
        "latest_external_clean_body": clean_body,
        "latest_external_full_body": full_body,
        "resolution_stage_final": _clean_text(
            _first_non_blank(
                keep_row.get("resolution_stage_final"),
                keep_row.get("resolution_stage"),
                keep_row.get("resolution_method"),
            )
        ),
    }


def _build_metrics_from_raw_platform_data(platform: str, data: Any) -> dict[str, dict[str, float | None]]:
    metrics: dict[str, dict[str, float | None]] = {}
    if platform == "instagram" and isinstance(data, list):
        for item in data:
            if not isinstance(item, dict):
                continue
            handle = _extract_handle(item.get("username") or item.get("url"))
            if not handle:
                continue
            posts = _select_instagram_metric_posts(item.get("latestPosts") or [])
            view_values: list[float] = []
            like_values: list[float] = []
            engagement_pairs: list[tuple[float, float]] = []
            for post in posts:
                if not isinstance(post, dict):
                    continue
                views = _extract_instagram_play_count(post)
                likes = _coerce_number(post.get("likesCount"))
                if views is not None:
                    view_values.append(views)
                if likes is not None:
                    like_values.append(likes)
                if likes is not None and views is not None and views > 0:
                    engagement_pairs.append((likes, views))
            avg_views = _average([value for value in view_values if value is not None])
            median_views = _median([value for value in view_values if value is not None])
            avg_likes = _average([value for value in like_values if value is not None])
            metrics[handle] = {
                "followers": _coerce_number(item.get("followersCount")),
                "following": _coerce_number(item.get("followsCount")),
                "avg_views": avg_views,
                "median_views": median_views,
                "avg_likes": avg_likes,
                "engagement_rate": _average_post_engagement_rate(engagement_pairs),
            }
        return metrics

    if platform == "tiktok" and isinstance(data, list):
        grouped: dict[str, dict[str, Any]] = {}
        for item in data:
            if not isinstance(item, dict):
                continue
            author_meta = item.get("authorMeta") or {}
            handle = _extract_handle(
                (author_meta or {}).get("name")
                or (author_meta or {}).get("profileUrl")
                or item.get("webVideoUrl")
            )
            if not handle:
                continue
            bucket = grouped.setdefault(handle, {"followers": None, "following": None, "views": [], "likes": [], "engagement_pairs": []})
            followers = _coerce_number((author_meta or {}).get("fans"))
            if followers is not None:
                bucket["followers"] = followers
            following = _coerce_number(
                (author_meta or {}).get("following")
                or (author_meta or {}).get("followingCount")
                or (author_meta or {}).get("friends")
                or (author_meta or {}).get("friendsCount")
            )
            if following is not None:
                bucket["following"] = following
            play_count = _coerce_number(item.get("playCount"))
            digg_count = _coerce_number(item.get("diggCount"))
            if play_count is not None:
                bucket["views"].append(play_count)
            if digg_count is not None:
                bucket["likes"].append(digg_count)
            if digg_count is not None and play_count is not None and play_count > 0:
                bucket["engagement_pairs"].append((digg_count, play_count))
        for handle, bucket in grouped.items():
            metrics[handle] = {
                "followers": bucket.get("followers"),
                "following": bucket.get("following"),
                "avg_views": _average(bucket.get("views") or []),
                "median_views": _median(bucket.get("views") or []),
                "avg_likes": _average(bucket.get("likes") or []),
                "engagement_rate": _average_post_engagement_rate(bucket.get("engagement_pairs") or []),
            }
        return metrics
    return {}


def _load_creator_cache_metric_rows(platform: str, final_review_path: Path) -> list[dict[str, Any]]:
    try:
        frame = pd.read_excel(final_review_path)
    except Exception:
        return []
    identifiers: list[str] = []
    seen: set[str] = set()
    for record in frame.to_dict(orient="records"):
        handle = _extract_handle(
            _first_non_blank(
                record.get("upload_handle"),
                record.get("username"),
                record.get("identifier"),
                record.get("profile_url"),
            )
        )
        if not handle or handle in seen:
            continue
        seen.add(handle)
        identifiers.append(handle)
    if not identifiers:
        return []
    try:
        rows = creator_cache.load_scrape_cache_entries(
            platform,
            identifiers,
            creator_cache.resolve_creator_cache_db_path(),
        )
    except Exception:
        return []
    merged: list[dict[str, Any]] = []
    for items in rows.values():
        merged.extend([item for item in items if isinstance(item, dict)])
    return merged


def _build_apify_metric_lookup(platform: str, final_review_path: str | Path | None) -> dict[str, dict[str, float | None]]:
    if not final_review_path:
        return {}
    export_path = Path(str(final_review_path)).expanduser()
    run_root = _resolve_run_root_from_export(export_path)
    data: Any = []
    if run_root is not None:
        data_path = run_root / "data" / platform / f"{platform}_data.json"
        if data_path.exists():
            try:
                data = json.loads(data_path.read_text(encoding="utf-8"))
            except Exception:
                data = []
    if not data:
        data = _load_creator_cache_metric_rows(platform, export_path)
    return _build_metrics_from_raw_platform_data(platform, data)


def _normalize_employee_id(value: Any) -> str:
    seen: set[str] = set()
    cleaned: list[str] = []
    for part in re.split(r"[,\n|]+", _clean_text(value)):
        normalized = part.strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        cleaned.append(normalized)
    return ",".join(cleaned)


def _build_keep_lookup(
    keep_workbook: str | Path | None,
) -> tuple[
    dict[tuple[str, str], dict[str, Any]],
    dict[tuple[str, str], dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
]:
    handle_lookup: dict[tuple[str, str], dict[str, Any]] = {}
    url_lookup: dict[tuple[str, str], dict[str, Any]] = {}
    handle_any_lookup: dict[str, dict[str, Any]] = {}
    url_any_lookup: dict[str, dict[str, Any]] = {}
    if not keep_workbook:
        return handle_lookup, url_lookup, handle_any_lookup, url_any_lookup
    keep_path = Path(str(keep_workbook)).expanduser()
    if not keep_path.exists():
        return handle_lookup, url_lookup, handle_any_lookup, url_any_lookup
    try:
        frame = pd.read_excel(keep_path)
    except Exception:
        return handle_lookup, url_lookup, handle_any_lookup, url_any_lookup
    for record in frame.to_dict(orient="records"):
        platform = _normalize_platform(record.get("Platform") or record.get("平台"))
        for candidate in (
            record.get("@username"),
            record.get("达人ID"),
            record.get("final_id_final"),
            record.get("final_id"),
            record.get("llm_handle"),
            record.get("identifier"),
            record.get("username"),
            record.get("upload_handle"),
            record.get("derived_handle"),
            record.get("URL"),
            record.get("主页链接"),
            record.get("profile_url"),
        ):
            handle = _extract_handle(candidate)
            if handle:
                if platform:
                    handle_lookup.setdefault((platform, handle), dict(record))
                handle_any_lookup.setdefault(handle, dict(record))
        for candidate in (
            record.get("URL"),
            record.get("主页链接"),
            record.get("profile_url"),
        ):
            url = _normalize_url(candidate)
            if not url:
                continue
            if platform:
                url_lookup.setdefault((platform, url), dict(record))
            url_any_lookup.setdefault(url, dict(record))
    return handle_lookup, url_lookup, handle_any_lookup, url_any_lookup


def _build_positioning_lookup(positioning_review_path: str | Path | None) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    handle_lookup: dict[str, dict[str, Any]] = {}
    url_lookup: dict[str, dict[str, Any]] = {}
    if not positioning_review_path:
        return handle_lookup, url_lookup
    review_path = Path(str(positioning_review_path)).expanduser()
    if not review_path.exists():
        return handle_lookup, url_lookup
    try:
        frame = pd.read_excel(review_path)
    except Exception:
        return handle_lookup, url_lookup
    for record in frame.to_dict(orient="records"):
        for candidate in (
            record.get("identifier"),
            record.get("username"),
            record.get("upload_handle"),
            record.get("profile_url"),
        ):
            handle = _extract_handle(candidate)
            if handle:
                handle_lookup.setdefault(handle, dict(record))
        url = _normalize_url(record.get("profile_url"))
        if url:
            url_lookup.setdefault(url, dict(record))
    return handle_lookup, url_lookup


def extract_task_owner_context(upstream_summary: dict[str, Any] | None) -> dict[str, str]:
    payload = dict(upstream_summary or {})
    mail_sync_raw = (((payload.get("steps") or {}).get("mail_sync") or {}).get("raw") or {})
    task_assets_raw = (((payload.get("steps") or {}).get("task_assets") or {}).get("raw") or {})
    first_item = next(iter(mail_sync_raw.get("items") or []), {})
    if not isinstance(first_item, dict):
        first_item = {}
    owner_name = _clean_text(first_item.get("responsibleName")) or _clean_text(first_item.get("employeeName"))
    return {
        "responsible_name": owner_name,
        "employee_name": _clean_text(first_item.get("employeeName")),
        "employee_id": _normalize_employee_id(first_item.get("employeeId")),
        "employee_record_id": _clean_text(first_item.get("employeeRecordId")),
        "employee_email": _clean_text(first_item.get("employeeEmail")),
        "owner_name": _clean_text(first_item.get("ownerName")),
        "task_record_id": _clean_text(first_item.get("recordId")),
        "task_name": _clean_text(first_item.get("taskName")) or _clean_text(payload.get("task_name")),
        "linked_bitable_url": _clean_text(first_item.get("linkedBitableUrl"))
        or _clean_text(task_assets_raw.get("linkedBitableUrl")),
    }


def _extract_row_owner_context(keep_row: dict[str, Any], task_owner: dict[str, Any] | None) -> dict[str, str]:
    owner_context = dict(task_owner or {})
    display_name = _clean_text(keep_row.get("达人对接人"))
    return {
        "responsible_name": display_name
        or _clean_text(owner_context.get("responsible_name"))
        or _clean_text(owner_context.get("employee_name"))
        or _clean_text(owner_context.get("owner_name")),
        "employee_name": display_name or _clean_text(owner_context.get("employee_name")),
        "employee_id": _normalize_employee_id(keep_row.get("达人对接人_employee_id") or owner_context.get("employee_id")),
        "employee_record_id": _clean_text(keep_row.get("达人对接人_employee_record_id"))
        or _clean_text(owner_context.get("employee_record_id")),
        "employee_email": _clean_text(keep_row.get("达人对接人_employee_email"))
        or _clean_text(owner_context.get("employee_email")),
        "owner_name": _clean_text(keep_row.get("达人对接人_owner_name"))
        or _clean_text(owner_context.get("owner_name")),
        "linked_bitable_url": _clean_text(keep_row.get("linked_bitable_url"))
        or _clean_text(owner_context.get("linked_bitable_url")),
        "task_name": _clean_text(keep_row.get("任务名")) or _clean_text(owner_context.get("task_name")),
    }


def collect_final_exports(platforms: dict[str, Any] | None) -> dict[str, dict[str, str]]:
    final_exports: dict[str, dict[str, str]] = {}
    for platform, platform_summary in (platforms or {}).items():
        exports_payload = (platform_summary or {}).get("exports")
        if not isinstance(exports_payload, dict):
            continue
        cleaned = {
            str(key): _clean_text(value)
            for key, value in exports_payload.items()
            if _clean_text(value)
        }
        if cleaned:
            final_exports[_normalize_platform(platform)] = cleaned
    return final_exports


def build_all_platforms_final_review_artifacts(
    *,
    output_path: str | Path,
    final_exports: dict[str, dict[str, str]],
    keep_workbook: str | Path | None = None,
    task_owner: dict[str, Any] | None = None,
    payload_json_path: str | Path | None = None,
    manual_review_rows: Sequence[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    workbook_path = Path(str(output_path)).expanduser().resolve()
    workbook_path.parent.mkdir(parents=True, exist_ok=True)
    archive_dir = workbook_path.parent / "feishu_upload_local_archive"
    archive_dir.mkdir(parents=True, exist_ok=True)
    keep_handle_lookup, keep_url_lookup, keep_handle_any_lookup, keep_url_any_lookup = _build_keep_lookup(keep_workbook)
    owner_context = dict(task_owner or {})
    owner_display_name = (
        _clean_text(owner_context.get("responsible_name"))
        or _clean_text(owner_context.get("employee_name"))
        or _clean_text(owner_context.get("owner_name"))
    )
    keep_workbook_path = Path(str(keep_workbook)).expanduser().resolve() if keep_workbook else None
    shared_attachment_paths = [str(workbook_path)]
    rows: list[dict[str, Any]] = []
    payload_rows: list[dict[str, Any]] = []
    skipped_payload_rows: list[dict[str, Any]] = []
    seen_creator_keys: set[tuple[str, str]] = set()
    mail_body_cache: dict[str, str] = {}
    mail_snapshot_cache: dict[str, dict[str, str]] = {}

    for platform, export_map in final_exports.items():
        final_review_path = Path(_clean_text((export_map or {}).get("final_review"))).expanduser()
        if not final_review_path.exists():
            continue
        run_root = _resolve_run_root_from_export(final_review_path)
        apify_metrics = _build_apify_metric_lookup(platform, final_review_path)
        positioning_handle_lookup, positioning_url_lookup = _build_positioning_lookup(
            (export_map or {}).get("positioning_card_review")
        )
        frame = pd.read_excel(final_review_path)
        for record in frame.to_dict(orient="records"):
            handle = _extract_handle(
                _first_non_blank(
                    record.get("upload_handle"),
                    record.get("username"),
                    record.get("identifier"),
                    record.get("profile_url"),
                )
            )
            profile_url = _clean_text(_first_non_blank(record.get("profile_url"), ""))
            normalized_url = _normalize_url(profile_url)
            keep_row = (
                keep_handle_lookup.get((platform, handle))
                or keep_url_lookup.get((platform, normalized_url))
                or keep_handle_any_lookup.get(handle)
                or keep_url_any_lookup.get(normalized_url)
                or {}
            )
            row_owner_context = _extract_row_owner_context(keep_row, owner_context)
            positioning_row = positioning_handle_lookup.get(handle) or positioning_url_lookup.get(normalized_url) or {}
            apify_row = apify_metrics.get(handle) or {}
            last_mail_raw_path = _clean_text(
                _first_non_blank(
                    keep_row.get("last_mail_raw_path"),
                    keep_row.get("brand_message_raw_path"),
                )
            )
            mail_context = _resolve_keep_row_mail_context(
                keep_row,
                last_mail_raw_path=last_mail_raw_path,
                base_dirs=[
                    Path.cwd(),
                    keep_workbook_path.parent if keep_workbook_path else None,
                    final_review_path.parent,
                    run_root,
                    run_root / "upstream" if run_root else None,
                    run_root / "upstream" / "exports" if run_root else None,
                    run_root / "upstream" / "mail_data" if run_root else None,
                ],
                snapshot_cache=mail_snapshot_cache,
            )
            row_attachment_paths = _resolve_existing_local_paths(
                last_mail_raw_path,
                base_dirs=[
                    Path.cwd(),
                    keep_workbook_path.parent if keep_workbook_path else None,
                    final_review_path.parent,
                    run_root,
                    run_root / "upstream" if run_root else None,
                    run_root / "upstream" / "exports" if run_root else None,
                    run_root / "upstream" / "mail_data" if run_root else None,
                ],
            )
            mail_body_text = _resolve_mail_full_body(
                keep_row,
                last_mail_raw_path=last_mail_raw_path,
                base_dirs=[
                    Path.cwd(),
                    keep_workbook_path.parent if keep_workbook_path else None,
                    final_review_path.parent,
                    run_root,
                    run_root / "upstream" if run_root else None,
                    run_root / "upstream" / "exports" if run_root else None,
                    run_root / "upstream" / "mail_data" if run_root else None,
                ],
                cache=mail_body_cache,
            )
            if not mail_body_text:
                mail_body_text = mail_context["latest_external_full_body"]
            median_views = _first_non_blank(
                apify_row.get("median_views"),
                record.get("runtime_median_views"),
                record.get("upload_median_views"),
                apify_row.get("avg_views"),
                record.get("runtime_avg_views"),
                record.get("upload_avg_views"),
            )
            avg_likes = _first_non_blank(apify_row.get("avg_likes"), record.get("upload_avg_likes"))
            engagement_rate = ""
            post_level_engagement_rate = _coerce_number(apify_row.get("engagement_rate"))
            median_views_num = _coerce_number(median_views)
            avg_likes_num = _coerce_number(avg_likes)
            metric_note = _resolve_metric_note(apify_row, median_views)
            if post_level_engagement_rate is not None and post_level_engagement_rate >= 0:
                engagement_rate = _format_percentage(post_level_engagement_rate)
            elif median_views_num is not None and median_views_num > 0 and avg_likes_num is not None:
                engagement_rate = _format_percentage(avg_likes_num / median_views_num)
            else:
                engagement_rate = _compute_engagement_rate(record)

            visual_manual_reason = _resolve_visual_manual_reason(
                record.get("final_reason"),
                record.get("reason"),
                record.get("visual_reason"),
            )
            if visual_manual_reason:
                metric_note = ""

            ai_pass_value = _resolve_ai_pass_value(
                _first_non_blank(record.get("final_status"), record.get("status")),
                visual_error_candidates=[
                    record.get("final_reason"),
                    record.get("reason"),
                    record.get("visual_reason"),
                ],
                other_error_candidates=[
                    positioning_row.get("positioning_error"),
                ],
            )
            positioning_label_note, positioning_comment_note = _resolve_positioning_stage_note(positioning_row)
            if visual_manual_reason:
                positioning_label_note = ""
                positioning_comment_note = ""

            base_reason = _first_non_blank(
                visual_manual_reason,
                positioning_row.get("positioning_error")
                if _is_processing_error(positioning_row.get("positioning_error"))
                else "",
                record.get("final_reason"),
                record.get("reason"),
                record.get("visual_reason"),
                positioning_row.get("positioning_error"),
            )
            screening_reason = _combine_reason_with_note(base_reason, metric_note)
            screening_reason = _combine_reason_with_note(screening_reason, positioning_comment_note)
            base_comment = _first_non_blank(
                visual_manual_reason,
                positioning_row.get("positioning_error")
                if _is_processing_error(positioning_row.get("positioning_error"))
                else "",
                positioning_row.get("fit_summary"),
                record.get("final_reason"),
                record.get("reason"),
            )
            screening_comment = _combine_reason_with_note(base_comment, metric_note)
            screening_comment = _combine_reason_with_note(screening_comment, positioning_comment_note)
            ai_label_value = _clean_text(positioning_row.get("positioning_labels")) or positioning_label_note
            ai_pass_value, ai_label_value, screening_reason, screening_comment = _apply_missing_ai_label_fallback(
                ai_pass_value,
                ai_label_value,
                screening_reason,
                screening_comment,
            )

            display_row = {
                "达人ID": _clean_text(
                    _first_non_blank(
                        record.get("upload_handle"),
                        record.get("username"),
                        record.get("identifier"),
                        keep_row.get("@username"),
                    )
                ),
                "平台": platform,
                "主页链接": profile_url or _clean_text(keep_row.get("URL")),
                "# Followers(K)#": _format_k_value(_first_non_blank(apify_row.get("followers"), record.get("upload_followers"), "")),
                "Following": _format_k_value(_first_non_blank(apify_row.get("following"), record.get("upload_following"), "")),
                "Median Views (K)": _format_k_value(median_views),
                "互动率": engagement_rate,
                "当前网红报价": _build_quote_text(keep_row, mail_body_text=mail_body_text),
                "达人最后一次回复邮件时间": _format_date(
                    _first_non_blank(
                        mail_context.get("latest_external_sent_at"),
                        keep_row.get("last_mail_time"),
                        keep_row.get("brand_message_sent_at"),
                    )
                ),
                _FULL_BODY_FIELD_NAME: mail_body_text,
                "达人对接人": _clean_text(row_owner_context.get("responsible_name"))
                or _clean_text(row_owner_context.get("employee_name"))
                or owner_display_name,
                "ai是否通过": ai_pass_value,
                "ai筛号反馈理由": screening_reason,
                "标签(ai)": ai_label_value,
                "ai评价": screening_comment,
            }
            rows.append(display_row)
            seen_creator_keys.add((platform, handle))

            payload_row = dict(display_row)
            payload_row.pop("达人对接人", None)
            payload_row.update(
                {
                    "linked_bitable_url": _clean_text(row_owner_context.get("linked_bitable_url")),
                    "任务名": _clean_text(row_owner_context.get("task_name")),
                    "creator_emails": mail_context["creator_emails"],
                    "matched_contact_email": mail_context["matched_contact_email"],
                    "matched_contact_name": mail_context["matched_contact_name"],
                    "latest_external_from": mail_context["latest_external_from"],
                    "latest_external_sent_at": mail_context["latest_external_sent_at"],
                    "latest_external_clean_body": mail_context["latest_external_clean_body"],
                    "latest_external_full_body": mail_context["latest_external_full_body"],
                    "resolution_stage_final": mail_context["resolution_stage_final"],
                    _ROW_UPDATE_MODE_KEY: _UPDATE_MODE_CREATE_OR_MAIL_ONLY,
                    "达人回复的最后一封邮件内容": mail_body_text,
                    "__brand_message_raw_path": _clean_text(keep_row.get("brand_message_raw_path")),
                    _LAST_MAIL_RAW_PATH_KEY: last_mail_raw_path,
                    _ROW_ATTACHMENT_PATHS_KEY: row_attachment_paths,
                }
            )
            validation_errors = _collect_upload_validation_errors(payload_row)
            if validation_errors:
                skipped_payload_rows.append(
                    {
                        "skip_reasons": validation_errors,
                        "row": payload_row,
                    }
                )
            else:
                payload_rows.append(payload_row)

    for manual_row in list(manual_review_rows or []):
        platform = _normalize_platform(manual_row.get("platform")) or "tiktok"
        handle = _extract_handle(
            _first_non_blank(
                manual_row.get("identifier"),
                manual_row.get("profile_url"),
            )
        )
        if not handle:
            continue
        if (platform, handle) in seen_creator_keys:
            continue
        profile_url = _clean_text(manual_row.get("profile_url"))
        normalized_url = _normalize_url(profile_url)
        keep_row = (
            keep_handle_lookup.get((platform, handle))
            or keep_url_lookup.get((platform, normalized_url))
            or keep_handle_any_lookup.get(handle)
            or keep_url_any_lookup.get(normalized_url)
            or {}
        )
        row_owner_context = _extract_row_owner_context(keep_row, owner_context)
        manual_reason = _first_non_blank(
            manual_row.get("reason"),
            "TikTok / Instagram / YouTube 均未抓取到有效资料，需人工确认。",
        )
        last_mail_raw_path = _clean_text(
            _first_non_blank(
                keep_row.get("last_mail_raw_path"),
                keep_row.get("brand_message_raw_path"),
            )
        )
        mail_context = _resolve_keep_row_mail_context(
            keep_row,
            last_mail_raw_path=last_mail_raw_path,
            base_dirs=[
                Path.cwd(),
                keep_workbook_path.parent if keep_workbook_path else None,
            ],
            snapshot_cache=mail_snapshot_cache,
        )
        row_attachment_paths = _resolve_existing_local_paths(
            last_mail_raw_path,
            base_dirs=[
                Path.cwd(),
                keep_workbook_path.parent if keep_workbook_path else None,
            ],
        )
        mail_body_text = _resolve_mail_full_body(
            keep_row,
            last_mail_raw_path=last_mail_raw_path,
            base_dirs=[
                Path.cwd(),
                keep_workbook_path.parent if keep_workbook_path else None,
            ],
            cache=mail_body_cache,
        )
        if not mail_body_text:
            mail_body_text = mail_context["latest_external_full_body"]
        display_row = {
            "达人ID": _clean_text(_first_non_blank(handle, keep_row.get("@username"))),
            "平台": platform,
            "主页链接": profile_url or _clean_text(keep_row.get("URL")),
            "# Followers(K)#": "",
            "Following": "",
            "Median Views (K)": "",
            "互动率": "",
            "当前网红报价": _build_quote_text(keep_row, mail_body_text=mail_body_text),
            "达人最后一次回复邮件时间": _format_date(
                _first_non_blank(
                    mail_context.get("latest_external_sent_at"),
                    keep_row.get("last_mail_time"),
                    keep_row.get("brand_message_sent_at"),
                )
            ),
            _FULL_BODY_FIELD_NAME: mail_body_text,
            "达人对接人": _clean_text(row_owner_context.get("responsible_name"))
            or _clean_text(row_owner_context.get("employee_name"))
            or owner_display_name,
            "ai是否通过": "转人工",
            "ai筛号反馈理由": manual_reason,
            "标签(ai)": "",
            "ai评价": manual_reason,
        }
        rows.append(display_row)
        seen_creator_keys.add((platform, handle))
        payload_row = dict(display_row)
        payload_row.pop("达人对接人", None)
        payload_row.update(
            {
                "linked_bitable_url": _clean_text(row_owner_context.get("linked_bitable_url")),
                "任务名": _clean_text(row_owner_context.get("task_name")),
                "creator_emails": mail_context["creator_emails"],
                "matched_contact_email": mail_context["matched_contact_email"],
                "matched_contact_name": mail_context["matched_contact_name"],
                "latest_external_from": mail_context["latest_external_from"],
                "latest_external_sent_at": mail_context["latest_external_sent_at"],
                "latest_external_clean_body": mail_context["latest_external_clean_body"],
                "latest_external_full_body": mail_context["latest_external_full_body"],
                "resolution_stage_final": mail_context["resolution_stage_final"],
                _ROW_UPDATE_MODE_KEY: _UPDATE_MODE_CREATE_OR_MAIL_ONLY,
                "达人回复的最后一封邮件内容": mail_body_text,
                "__brand_message_raw_path": _clean_text(keep_row.get("brand_message_raw_path")),
                _LAST_MAIL_RAW_PATH_KEY: last_mail_raw_path,
                _ROW_ATTACHMENT_PATHS_KEY: row_attachment_paths,
            }
        )
        validation_errors = _collect_upload_validation_errors(payload_row)
        if validation_errors:
            skipped_payload_rows.append(
                {
                    "skip_reasons": validation_errors,
                    "row": payload_row,
                }
            )
        else:
            payload_rows.append(payload_row)

    combined = pd.DataFrame(rows, columns=FINAL_UPLOAD_COLUMNS)
    with pd.ExcelWriter(workbook_path, engine="openpyxl") as writer:
        combined.to_excel(writer, index=False, sheet_name="总表")

    payload_path = Path(str(payload_json_path)).expanduser().resolve() if payload_json_path else workbook_path.with_suffix(".json")
    payload_path.parent.mkdir(parents=True, exist_ok=True)
    skipped_archive_json_path = archive_dir / "skipped_from_feishu_upload.json"
    skipped_archive_xlsx_path = archive_dir / "skipped_from_feishu_upload.xlsx"
    payload = {
        "task_owner": {
            "linked_bitable_url": _clean_text(owner_context.get("linked_bitable_url")),
            "task_name": _clean_text(owner_context.get("task_name")),
        },
        "columns": list(FINAL_UPLOAD_COLUMNS),
        "source_row_count": len(rows),
        "row_count": len(payload_rows),
        "skipped_row_count": len(skipped_payload_rows),
        _SHARED_ATTACHMENT_PATHS_KEY: shared_attachment_paths,
        "rows": payload_rows,
        "skipped_rows": skipped_payload_rows,
    }
    payload_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    skipped_archive_json_path.write_text(
        json.dumps(
            {
                "task_owner": payload["task_owner"],
                "source_row_count": len(rows),
                "skipped_row_count": len(skipped_payload_rows),
                "skipped_rows": skipped_payload_rows,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    skipped_archive_records: list[dict[str, Any]] = []
    for skipped in skipped_payload_rows:
        row = dict(skipped.get("row") or {})
        skip_reasons = skipped.get("skip_reasons") or []
        row["本地归档原因"] = "；".join(str(item).strip() for item in skip_reasons if str(item).strip())
        skipped_archive_records.append(row)
    skipped_archive_columns = ("本地归档原因", *FINAL_UPLOAD_COLUMNS)
    skipped_frame = pd.DataFrame(skipped_archive_records, columns=skipped_archive_columns)
    with pd.ExcelWriter(skipped_archive_xlsx_path, engine="openpyxl") as writer:
        skipped_frame.to_excel(writer, index=False, sheet_name="未上传归档")
    return {
        "all_platforms_final_review": str(workbook_path),
        "all_platforms_upload_payload_json": str(payload_path),
        "all_platforms_upload_local_archive_dir": str(archive_dir),
        "all_platforms_upload_skipped_archive_json": str(skipped_archive_json_path),
        "all_platforms_upload_skipped_archive_xlsx": str(skipped_archive_xlsx_path),
        "all_platforms_upload_shared_attachment_local_paths": shared_attachment_paths,
        "row_count": len(payload_rows),
        "source_row_count": len(rows),
        "skipped_row_count": len(skipped_payload_rows),
        "columns": list(FINAL_UPLOAD_COLUMNS),
    }
