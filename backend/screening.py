import os
import math
import re
import statistics
from datetime import datetime, timezone
from urllib.parse import parse_qs, unquote, urlparse


def parse_env_positive_int(name, default):
    try:
        resolved = int(os.getenv(name, str(default)))
    except Exception:
        resolved = int(default)
    return max(1, resolved)


VISUAL_REVIEW_CANDIDATE_COVER_LIMIT_FLOOR = parse_env_positive_int(
    "VISUAL_REVIEW_CANDIDATE_COVER_LIMIT_FLOOR",
    12,
)

PROFILE_REVIEW_ALLOWED_STATUSES = {"Pass", "Reject", "Missing"}
REASON_NO_DATA = "未抓到可用数据"
REASON_NO_POSTS = "最近内容为空"
REASON_INACTIVE = "最近 30 天无更新"
REASON_MISSING_PROFILE = "名单账号未在本次抓取结果中返回"
REASON_PROFILE_UNAVAILABLE = "抓取返回账号不存在或不可访问"

DEFAULT_RULES = {
    "shared": {
        "active_days_max": 30,
        "visual_review_cover_limit": 9,
    },
    "instagram": {
        "allowed_regions": [],
    },
    "tiktok": {
        "min_avg_views": 10000,
        "min_median_views": 10000,
    },
    "youtube": {
        "max_paid_content_hits": 8,
        "paid_content_window": 10,
    },
}


def coerce_positive_int(value):
    try:
        resolved = int(value)
    except Exception:
        return None
    return max(1, resolved)


def normalize_rule_type(rule):
    if not isinstance(rule, dict):
        return ""
    return str(rule.get("rule_type") or rule.get("type") or "").strip().lower()


def normalize_rule_platforms(rule):
    if not isinstance(rule, dict):
        return []
    raw_platforms = rule.get("platforms")
    if raw_platforms in (None, ""):
        raw_platforms = rule.get("platform_scope")
    if isinstance(raw_platforms, str):
        raw_platforms = [raw_platforms]
    return [
        str(item or "").strip().lower()
        for item in (raw_platforms or [])
        if str(item or "").strip()
    ]


def iter_rulespec_rules(active_rulespec):
    if not isinstance(active_rulespec, dict):
        return []
    rules = active_rulespec.get("rules")
    if not isinstance(rules, list):
        return []
    return [item for item in rules if isinstance(item, dict)]


def format_ratio_operator(value):
    normalized = str(value or "").strip().lower()
    return {
        "ratio_gt": ">",
        "gt": ">",
        ">": ">",
        "ratio_gte": ">=",
        "gte": ">=",
        ">=": ">=",
        "ratio_lt": "<",
        "lt": "<",
        "<": "<",
        "ratio_lte": "<=",
        "lte": "<=",
        "<=": "<=",
        "must_not_appear": "must_not_appear",
    }.get(normalized, normalized or ">")


def extract_visual_feature_label(feature):
    if not isinstance(feature, dict):
        return str(feature or "").strip()
    return str(feature.get("label") or feature.get("source_label") or feature.get("key") or "").strip()


def render_visual_exclusion_summary(rule):
    rule_type = normalize_rule_type(rule)
    operator = format_ratio_operator(rule.get("operator"))
    threshold_percent = rule.get("threshold_percent")
    threshold_text = (
        f" {operator} {int(threshold_percent)}%"
        if threshold_percent not in (None, "") and operator != "must_not_appear"
        else ""
    )

    if rule_type == "green_screen":
        return "出现绿幕背景"
    if rule_type == "multi_dance_ratio":
        return f"多人跳舞占比{threshold_text}".strip()
    if rule_type == "selfie_or_couple_ratio":
        return f"自拍/情侣出镜占比{threshold_text}".strip()
    if rule_type == "content_keyword_ratio":
        keyword_values = [str(item or "").strip() for item in (rule.get("keyword_values") or []) if str(item or "").strip()]
        if keyword_values:
            return f"{'/'.join(keyword_values[:3])} 关键词占比{threshold_text}".strip()
    return ""


def normalize_visual_notice_items(items):
    normalized = []
    for item in items or []:
        if isinstance(item, dict):
            if any(str(item.get(field) or "").strip() for field in ("label", "value", "note", "policy", "key")):
                normalized.append(dict(item))
            continue
        text = str(item or "").strip()
        if text:
            normalized.append({"value": text})
    return normalized


def resolve_visual_runtime_contract(active_rulespec, platform):
    normalized_platform = str(platform or "").strip().lower()
    if not normalized_platform:
        return {}

    goal = str((active_rulespec or {}).get("goal") or "").strip()
    positive_feature_labels = []
    exclusion_summaries = []
    cover_count = None
    min_hit_features = None
    manual_review_items = normalize_visual_notice_items((active_rulespec or {}).get("manual_review_items") or [])
    compliance_notes = normalize_visual_notice_items((active_rulespec or {}).get("compliance_notes") or [])

    for rule in iter_rulespec_rules(active_rulespec):
        platforms = normalize_rule_platforms(rule)
        if platforms and normalized_platform not in platforms:
            continue
        rule_type = normalize_rule_type(rule)
        if rule_type == "visual_feature_group":
            cover_count = coerce_positive_int(rule.get("cover_count"))
            min_hit_features = coerce_positive_int(rule.get("min_hit_features")) or 1
            for feature in rule.get("features") or []:
                label = extract_visual_feature_label(feature)
                if label and label not in positive_feature_labels:
                    positive_feature_labels.append(label)
            continue
        summary = render_visual_exclusion_summary(rule)
        if summary and summary not in exclusion_summaries:
            exclusion_summaries.append(summary)

    if (
        cover_count is None
        and min_hit_features is None
        and not positive_feature_labels
        and not exclusion_summaries
        and not manual_review_items
        and not compliance_notes
        and not goal
    ):
        return {}

    return {
        "platform": normalized_platform,
        "source": "active_rulespec.rules",
        "goal": goal,
        "cover_count": cover_count,
        "min_hit_features": min_hit_features or 1,
        "positive_feature_labels": positive_feature_labels,
        "exclusion_summaries": exclusion_summaries,
        "manual_review_items": manual_review_items,
        "compliance_notes": compliance_notes,
    }


def normalize_identifier(value):
    text = str(value or "").strip().lower()
    if not text:
        return ""
    if text.startswith("@"):
        text = text[1:]
    text = re.sub(r"[?#].*$", "", text)
    text = re.sub(r"/+$", "", text)
    return text


def extract_platform_identifier(platform, value):
    text = str(value or "").strip()
    if not text:
        return ""

    lowered = text.lower()
    if platform == "instagram":
        match = re.search(r"instagram\.com/([^/?#]+)", lowered)
        if match:
            return normalize_identifier(match.group(1))
    elif platform == "tiktok":
        match = re.search(r"tiktok\.com/@([^/?#]+)", lowered)
        if match:
            return normalize_identifier(match.group(1))
        if "tiktok.com/search" in lowered:
            try:
                parsed = urlparse(text)
                query_value = parse_qs(parsed.query or "").get("q", [""])[0]
            except Exception:
                query_value = ""
            if query_value:
                return normalize_identifier(unquote(query_value))
    elif platform == "youtube":
        patterns = (
            r"youtube\.com/@([^/?#]+)",
            r"youtube\.com/channel/([^/?#]+)",
            r"youtube\.com/c/([^/?#]+)",
            r"youtube\.com/user/([^/?#]+)",
            r"youtu\.be/([^/?#]+)",
        )
        for pattern in patterns:
            match = re.search(pattern, lowered)
            if match:
                return normalize_identifier(match.group(1))

    return normalize_identifier(text)


def build_canonical_profile_url(platform, identifier):
    normalized = normalize_identifier(identifier)
    if not normalized:
        return ""
    if platform == "instagram":
        return f"https://www.instagram.com/{normalized}/"
    if platform == "tiktok":
        return f"https://www.tiktok.com/@{normalized}"
    if platform == "youtube":
        return f"https://www.youtube.com/@{normalized}"
    return normalized


def parse_iso_datetime(value):
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None


def sort_items_by_latest(items, time_field):
    fallback = datetime.min.replace(tzinfo=timezone.utc)
    return sorted(
        list(items or []),
        key=lambda item: parse_iso_datetime((item or {}).get(time_field)) or fallback,
        reverse=True,
    )


def get_runtime_rules(active_rulespec, platform):
    resolved = {
        **DEFAULT_RULES.get("shared", {}),
        **DEFAULT_RULES.get(platform, {}),
    }
    resolved["_visual_review_cover_limit_explicit"] = False
    visual_contract = resolve_visual_runtime_contract(active_rulespec, platform)
    resolved["visual_runtime_contract"] = visual_contract
    resolved["visual_contract_source"] = str((visual_contract or {}).get("source") or "").strip()

    if not isinstance(active_rulespec, dict):
        return resolved

    overrides = active_rulespec.get("platform_overrides") or {}
    if not isinstance(overrides, dict):
        return resolved

    shared_override = overrides.get("shared")
    if isinstance(shared_override, dict):
        if shared_override.get("visual_review_cover_limit") not in (None, ""):
            resolved["_visual_review_cover_limit_explicit"] = True
        resolved.update({key: value for key, value in shared_override.items() if value not in (None, "")})

    platform_override = overrides.get(platform)
    if isinstance(platform_override, dict):
        if platform_override.get("visual_review_cover_limit") not in (None, ""):
            resolved["_visual_review_cover_limit_explicit"] = True
        resolved.update({key: value for key, value in platform_override.items() if value not in (None, "")})

    return resolved


def resolve_visual_review_request_cover_limit(runtime_rules=None):
    rules = runtime_rules or {}
    default_limit = DEFAULT_RULES["shared"]["visual_review_cover_limit"]

    if rules.get("_visual_review_cover_limit_explicit"):
        explicit_limit = coerce_positive_int(rules.get("visual_review_cover_limit"))
        if explicit_limit is not None:
            return explicit_limit

    contract_limit = coerce_positive_int(((rules.get("visual_runtime_contract") or {}).get("cover_count")))
    if contract_limit is not None:
        return contract_limit

    requested_limit = coerce_positive_int(rules.get("visual_review_cover_limit"))
    return requested_limit if requested_limit is not None else default_limit


def resolve_visual_review_candidate_cover_limit(runtime_rules=None):
    requested_limit = resolve_visual_review_request_cover_limit(runtime_rules)
    return max(1, requested_limit, VISUAL_REVIEW_CANDIDATE_COVER_LIMIT_FLOOR)


def extract_tiktok_cover_urls(items, cover_limit):
    covers = []
    for item in list(items or [])[:max(1, int(cover_limit or 1))]:
        if item.get("isSlideshow") and isinstance(item.get("slideshowImageLinks"), list):
            slideshow_links = item.get("slideshowImageLinks") or []
            if slideshow_links and isinstance(slideshow_links[0], dict):
                cover = slideshow_links[0].get("tiktokLink")
                if cover:
                    covers.append(cover)
                    continue
        video_meta = item.get("videoMeta") or {}
        cover = video_meta.get("originalCoverUrl") or video_meta.get("coverUrl")
        if cover:
            covers.append(cover)
    return covers


def extract_instagram_cover_urls(posts, cover_limit):
    return [
        post.get("displayUrl")
        for post in list(posts or [])[:max(1, int(cover_limit or 1))]
        if str(post.get("displayUrl") or "").strip()
    ]


def _append_unique_cover_url(candidate, covers, seen, cover_limit):
    url = str(candidate or "").strip()
    if not url or url in seen or len(covers) >= cover_limit:
        return
    seen.add(url)
    covers.append(url)


def _collect_youtube_thumbnail_urls(node, covers, seen, cover_limit, *, thumbnail_context=False):
    if len(covers) >= cover_limit:
        return
    if isinstance(node, str):
        if thumbnail_context:
            _append_unique_cover_url(node, covers, seen, cover_limit)
        return
    if isinstance(node, list):
        for item in node:
            _collect_youtube_thumbnail_urls(
                item,
                covers,
                seen,
                cover_limit,
                thumbnail_context=thumbnail_context,
            )
            if len(covers) >= cover_limit:
                break
        return
    if not isinstance(node, dict):
        return

    thumbnail_url = node.get("thumbnailUrl")
    if str(thumbnail_url or "").strip():
        _append_unique_cover_url(thumbnail_url, covers, seen, cover_limit)

    for key, value in node.items():
        normalized_key = str(key or "").strip().lower()
        if normalized_key == "thumbnailurl":
            continue
        child_thumbnail_context = (
            thumbnail_context
            or "thumb" in normalized_key
            or normalized_key in {"image", "images", "preview", "previews", "default", "medium", "high", "standard", "maxres"}
        )
        if normalized_key in {"url", "src"}:
            if child_thumbnail_context:
                _append_unique_cover_url(value, covers, seen, cover_limit)
            continue
        _collect_youtube_thumbnail_urls(
            value,
            covers,
            seen,
            cover_limit,
            thumbnail_context=child_thumbnail_context,
        )
        if len(covers) >= cover_limit:
            break


def extract_youtube_cover_urls(items, cover_limit):
    resolved_cover_limit = max(1, int(cover_limit or 1))
    covers = []
    seen = set()
    for item in list(items or []):
        _collect_youtube_thumbnail_urls(item, covers, seen, resolved_cover_limit)
        if len(covers) >= resolved_cover_limit:
            break
    return covers


def build_profile_review_record(
    platform,
    username="",
    profile_url="",
    status="Reject",
    reason="",
    covers=None,
    latest_post_time=None,
    soft_flags=None,
    stats=None,
    upload_metadata=None,
    resolved_cover_limit=None,
    visual_contract_source="",
):
    normalized_status = status if status in PROFILE_REVIEW_ALLOWED_STATUSES else "Reject"
    normalized_covers = []
    if isinstance(covers, list):
        normalized_covers = [str(item).strip() for item in covers if str(item or "").strip()]

    return {
        "platform": str(platform or "").strip(),
        "username": str(username or "").strip(),
        "profile_url": str(profile_url or "").strip(),
        "status": normalized_status,
        "reason": str(reason or "").strip(),
        "covers": normalized_covers,
        "latest_post_time": latest_post_time or None,
        "soft_flags": list(soft_flags or []),
        "stats": dict(stats or {}),
        "upload_metadata": dict(upload_metadata or {}),
        "resolved_cover_limit": coerce_positive_int(resolved_cover_limit),
        "visual_contract_source": str(visual_contract_source or "").strip(),
    }


def _normalize_string_list(values, *, limit=None):
    normalized = []
    seen = set()
    for value in values or []:
        cleaned = str(value or "").strip()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        normalized.append(cleaned)
        if limit is not None and len(normalized) >= int(limit):
            break
    return normalized


def normalize_fit_recommendation(value):
    cleaned = str(value or "").strip().lower()
    mapping = {
        "high": "High Fit",
        "high fit": "High Fit",
        "strong": "High Fit",
        "strong fit": "High Fit",
        "pass": "High Fit",
        "medium": "Medium Fit",
        "medium fit": "Medium Fit",
        "mid": "Medium Fit",
        "moderate": "Medium Fit",
        "moderate fit": "Medium Fit",
        "partial": "Medium Fit",
        "low": "Low Fit",
        "low fit": "Low Fit",
        "weak": "Low Fit",
        "weak fit": "Low Fit",
        "reject": "Low Fit",
        "unclear": "Unclear",
        "unknown": "Unclear",
        "not sure": "Unclear",
    }
    if cleaned in mapping:
        return mapping[cleaned]
    if "high" in cleaned:
        return "High Fit"
    if "medium" in cleaned or "moderate" in cleaned:
        return "Medium Fit"
    if "low" in cleaned or "weak" in cleaned:
        return "Low Fit"
    return "Unclear"


def build_positioning_card_record(
    platform,
    username="",
    profile_url="",
    positioning_labels=None,
    fit_recommendation="",
    fit_summary="",
    evidence_signals=None,
    provider="",
    model="",
    configured_model="",
    requested_model="",
    response_model="",
    effective_model="",
    prompt_source="",
    prompt_selection=None,
    reviewed_at=None,
    visual_status="",
    visual_reason="",
    visual_reviewed_at=None,
    visual_contract_source="",
    usage=None,
    cover_count=None,
    candidate_cover_count=None,
    skipped_cover_count=None,
):
    return {
        "platform": str(platform or "").strip(),
        "username": str(username or "").strip(),
        "profile_url": str(profile_url or "").strip(),
        "positioning_labels": _normalize_string_list(positioning_labels, limit=6),
        "fit_recommendation": normalize_fit_recommendation(fit_recommendation),
        "fit_summary": str(fit_summary or "").strip(),
        "evidence_signals": _normalize_string_list(evidence_signals, limit=5),
        "provider": str(provider or "").strip(),
        "model": str(model or "").strip(),
        "configured_model": str(configured_model or "").strip(),
        "requested_model": str(requested_model or "").strip(),
        "response_model": str(response_model or "").strip(),
        "effective_model": str(effective_model or "").strip(),
        "prompt_source": str(prompt_source or "").strip(),
        "prompt_selection": dict(prompt_selection or {}),
        "reviewed_at": reviewed_at or None,
        "visual_status": str(visual_status or "").strip(),
        "visual_reason": str(visual_reason or "").strip(),
        "visual_reviewed_at": visual_reviewed_at or None,
        "visual_contract_source": str(visual_contract_source or "").strip(),
        "usage": dict(usage or {}),
        "cover_count": coerce_positive_int(cover_count),
        "candidate_cover_count": coerce_positive_int(candidate_cover_count),
        "skipped_cover_count": max(0, int(skipped_cover_count or 0)),
    }


def resolve_upload_metadata(metadata_lookup, *candidates):
    if not isinstance(metadata_lookup, dict):
        return {}
    for candidate in candidates:
        identifier = normalize_identifier(candidate)
        if identifier and isinstance(metadata_lookup.get(identifier), dict):
            return dict(metadata_lookup[identifier])
    return {}


def _collect_instagram_region_field_text(value, parts):
    if isinstance(value, str) and value.strip():
        parts.append(value.strip())
        return
    if isinstance(value, dict):
        for nested in value.values():
            _collect_instagram_region_field_text(nested, parts)
        return
    if isinstance(value, list):
        for nested in value:
            _collect_instagram_region_field_text(nested, parts)


def normalize_region_text(value):
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", str(value or "").lower())).strip()


def build_instagram_region_text(profile):
    parts = []
    for key in (
        "biography",
        "fullName",
        "full_name",
        "addressStreet",
        "address_street",
        "cityName",
        "city_name",
        "location",
        "externalUrl",
        "external_url",
        "externalUrls",
        "external_urls",
        "businessAddressJson",
        "business_address_json",
        "businessAddressCity",
        "business_address_city",
        "businessAddressCountryCode",
        "business_address_country_code",
        "businessCategoryName",
        "business_category_name",
    ):
        _collect_instagram_region_field_text(profile.get(key), parts)
    return normalize_region_text(" ".join(parts))


def has_instagram_allowed_region(profile, upload_metadata, allowed_regions):
    allowed_regions = [str(item or "").strip().upper() for item in (allowed_regions or []) if str(item or "").strip()]
    if not allowed_regions:
        return True

    upload_region = str((upload_metadata or {}).get("region") or "").strip().upper()
    if upload_region:
        return upload_region in allowed_regions

    region_text = build_instagram_region_text(profile)
    if not region_text:
        return False

    region_aliases = {
        "US": (
            r"\busa\b",
            r"\bunited states\b",
            r"\bamerica\b",
            r"\bamerican\b",
            r"\bnew york\b",
            r"\bnyc\b",
            r"\bcalifornia\b",
            r"\bflorida\b",
            r"\bsouth florida\b",
            r"\btexas\b",
            r"\barizona\b",
            r"\bchicago\b",
            r"\bmiami\b",
            r"\blos angeles\b",
            r"\bbay area\b",
            r"\bsedona\b",
            r"\bla\s+ca\b",
            r"\bmiami\s+fl\b",
            r"\bchicago\s+il\b",
            r"\bnew york\s+ny\b",
        ),
    }
    for region in allowed_regions:
        aliases = region_aliases.get(region, (rf"\b{re.escape(region.lower())}\b",))
        if any(re.search(alias, region_text) for alias in aliases):
            return True
    return False


def check_tiktok_profile(items, runtime_rules=None):
    rules = runtime_rules or {}
    if not items:
        return {"status": "Reject", "reason": REASON_NO_DATA}

    sorted_items = sort_items_by_latest(items, "createTimeISO")
    first_item = sorted_items[0]
    latest_post_time = first_item.get("createTimeISO")
    active_days_max = int(rules.get("active_days_max") or DEFAULT_RULES["shared"]["active_days_max"])

    if latest_post_time:
        latest_dt = parse_iso_datetime(latest_post_time)
        if latest_dt and (datetime.now(timezone.utc) - latest_dt).days > active_days_max:
            return {"status": "Reject", "reason": f"最近 {active_days_max} 天无更新", "latest_post_time": latest_post_time}

    top_items = sorted_items[:50]
    play_counts = [int(item.get("playCount") or 0) for item in top_items]
    avg_views = statistics.mean(play_counts) if play_counts else 0
    median_views = statistics.median(play_counts) if play_counts else 0
    min_avg_views = float(rules.get("min_avg_views") or DEFAULT_RULES["tiktok"]["min_avg_views"])
    min_median_views = float(rules.get("min_median_views") or DEFAULT_RULES["tiktok"]["min_median_views"])

    if avg_views < min_avg_views or median_views < min_median_views:
        return {
            "status": "Reject",
            "reason": (
                f"播放量不达标（均值 {avg_views:.0f}/{min_avg_views:.0f}，"
                f"中位数 {median_views:.0f}/{min_median_views:.0f}）"
            ),
            "latest_post_time": latest_post_time,
            "stats": {
                "avg_views": round(avg_views, 1),
                "median_views": round(median_views, 1),
                "video_count": len(play_counts),
            },
        }

    resolved_cover_limit = resolve_visual_review_request_cover_limit(rules)
    candidate_cover_limit = resolve_visual_review_candidate_cover_limit(rules)
    covers = extract_tiktok_cover_urls(sorted_items, candidate_cover_limit)

    return {
        "status": "Pass",
        "reason": (
            f"近 {active_days_max} 天有更新；播放量达标（均值 {avg_views:.0f}，中位数 {median_views:.0f}）；"
            f"已提取 {len(covers)} 张封面"
        ),
        "latest_post_time": latest_post_time,
        "covers": covers,
        "resolved_cover_limit": resolved_cover_limit,
        "visual_contract_source": rules.get("visual_contract_source") or "",
        "stats": {
            "avg_views": round(avg_views, 1),
            "median_views": round(median_views, 1),
            "video_count": len(play_counts),
        },
    }


def check_instagram_profile(profile, upload_metadata=None, runtime_rules=None):
    rules = runtime_rules or {}
    if not profile:
        return {"status": "Reject", "reason": REASON_NO_DATA}

    allowed_regions = rules.get("allowed_regions") or DEFAULT_RULES["instagram"]["allowed_regions"]
    if not has_instagram_allowed_region(profile, upload_metadata, allowed_regions):
        upload_region = str((upload_metadata or {}).get("region") or "").strip()
        if upload_region:
            return {"status": "Reject", "reason": f"上传表 Region=`{upload_region}` 未命中允许地区"}
        return {"status": "Reject", "reason": "简介或资料字段未识别到允许地区线索"}

    posts = profile.get("latestPosts") or []
    if not posts:
        return {"status": "Reject", "reason": REASON_NO_POSTS}

    sorted_posts = sort_items_by_latest(posts, "timestamp")
    latest_post_time = sorted_posts[0].get("timestamp")
    active_days_max = int(rules.get("active_days_max") or DEFAULT_RULES["shared"]["active_days_max"])
    latest_dt = parse_iso_datetime(latest_post_time)
    if latest_dt and (datetime.now(timezone.utc) - latest_dt).days > active_days_max:
        return {"status": "Reject", "reason": f"最近 {active_days_max} 天无更新", "latest_post_time": latest_post_time}

    resolved_cover_limit = resolve_visual_review_request_cover_limit(rules)
    candidate_cover_limit = resolve_visual_review_candidate_cover_limit(rules)
    covers = extract_instagram_cover_urls(sorted_posts, candidate_cover_limit)
    return {
        "status": "Pass",
        "reason": f"地区符合；近 {active_days_max} 天有更新；已提取 {len(covers)} 张封面",
        "latest_post_time": latest_post_time,
        "covers": covers,
        "resolved_cover_limit": resolved_cover_limit,
        "visual_contract_source": rules.get("visual_contract_source") or "",
    }


def check_youtube_profile(items, runtime_rules=None):
    rules = runtime_rules or {}
    if not items:
        return {"status": "Reject", "reason": REASON_NO_DATA}

    sorted_items = sort_items_by_latest(items, "date")
    first_item = sorted_items[0]
    latest_post_time = first_item.get("date")
    active_days_max = int(rules.get("active_days_max") or DEFAULT_RULES["shared"]["active_days_max"])
    latest_dt = parse_iso_datetime(latest_post_time)
    if latest_dt and (datetime.now(timezone.utc) - latest_dt).days > active_days_max:
        return {"status": "Reject", "reason": f"最近 {active_days_max} 天无更新", "latest_post_time": latest_post_time}

    paid_window = int(rules.get("paid_content_window") or DEFAULT_RULES["youtube"]["paid_content_window"])
    max_paid_hits = int(rules.get("max_paid_content_hits") or DEFAULT_RULES["youtube"]["max_paid_content_hits"])
    paid_hits = 0
    for item in sorted_items[:paid_window]:
        if item.get("isPaidContent") is True:
            paid_hits += 1
    if len(sorted_items) >= paid_window and paid_hits >= max_paid_hits:
        return {
            "status": "Reject",
            "reason": f"近期商业合作内容过多（最近 {paid_window} 条中 {paid_hits} 条标记为 paid content）",
            "latest_post_time": latest_post_time,
        }

    resolved_cover_limit = resolve_visual_review_request_cover_limit(rules)
    candidate_cover_limit = resolve_visual_review_candidate_cover_limit(rules)
    covers = extract_youtube_cover_urls(sorted_items, candidate_cover_limit)
    return {
        "status": "Pass",
        "reason": f"近 {active_days_max} 天有更新；已提取 {len(covers)} 张封面",
        "latest_post_time": latest_post_time,
        "covers": covers,
        "resolved_cover_limit": resolved_cover_limit,
        "visual_contract_source": rules.get("visual_contract_source") or "",
    }


def resolve_profile_review_identifier(platform, item):
    if not isinstance(item, dict):
        return ""
    upload_metadata = item.get("upload_metadata") or {}
    for candidate in (
        item.get("profile_url"),
        item.get("username"),
        upload_metadata.get("url"),
        upload_metadata.get("handle"),
    ):
        identifier = extract_platform_identifier(platform, candidate)
        if identifier:
            return identifier
    return ""


def build_missing_review(platform, candidate, metadata_lookup):
    identifier = extract_platform_identifier(platform, candidate)
    profile_url = build_canonical_profile_url(platform, identifier)
    upload_metadata = resolve_upload_metadata(metadata_lookup, profile_url, identifier, candidate)
    return build_profile_review_record(
        platform,
        username=identifier or normalize_identifier(candidate),
        profile_url=profile_url,
        status="Missing",
        reason=REASON_MISSING_PROFILE,
        upload_metadata=upload_metadata,
    )


def build_scrape_error_reason(error_message):
    message = str(error_message or "").strip()
    if not message:
        return REASON_NO_DATA
    lowered = message.lower()
    if "does not exist" in lowered or "not exist" in lowered:
        return REASON_PROFILE_UNAVAILABLE
    return f"抓取返回错误：{message}"


def filter_scraped_items(platform, items, expected_profiles=None, upload_metadata_lookup=None, active_rulespec=None):
    expected_profiles = [item for item in (expected_profiles or []) if str(item or "").strip()]
    upload_metadata_lookup = upload_metadata_lookup or {}
    runtime_rules = get_runtime_rules(active_rulespec, platform)
    raw_items = list(items or [])
    profile_reviews = []
    passed_items = []
    returned_identifiers = set()

    if platform == "tiktok":
        grouped = {}
        for item in raw_items:
            author_meta = item.get("authorMeta") or {}
            identifier = (
                extract_platform_identifier(platform, author_meta.get("profileUrl"))
                or extract_platform_identifier(platform, author_meta.get("name"))
                or extract_platform_identifier(platform, item.get("url"))
                or extract_platform_identifier(platform, item.get("input"))
                or extract_platform_identifier(platform, item.get("webVideoUrl"))
            )
            if not identifier:
                continue
            grouped.setdefault(identifier, []).append(item)

        for identifier, grouped_items in grouped.items():
            content_items = [
                item
                for item in grouped_items
                if ((item.get("authorMeta") or {}).get("profileUrl")) or ((item.get("authorMeta") or {}).get("name"))
            ]
            first_item = content_items[0] if content_items else grouped_items[0]
            profile_url = (
                ((first_item.get("authorMeta") or {}).get("profileUrl"))
                or first_item.get("url")
                or build_canonical_profile_url(platform, identifier)
            )
            upload_metadata = resolve_upload_metadata(
                upload_metadata_lookup,
                profile_url,
                identifier,
                first_item.get("input"),
            )
            if content_items:
                review = check_tiktok_profile(content_items, runtime_rules=runtime_rules)
            else:
                error_message = next(
                    (str(item.get("error") or "").strip() for item in grouped_items if str(item.get("error") or "").strip()),
                    "",
                )
                review = {
                    "status": "Reject",
                    "reason": build_scrape_error_reason(error_message),
                }
            profile_reviews.append(
                build_profile_review_record(
                    platform,
                    username=identifier,
                    profile_url=profile_url,
                    status=review.get("status"),
                    reason=review.get("reason"),
                    covers=review.get("covers"),
                    latest_post_time=review.get("latest_post_time"),
                    stats=review.get("stats"),
                    upload_metadata=upload_metadata,
                    resolved_cover_limit=review.get("resolved_cover_limit"),
                    visual_contract_source=review.get("visual_contract_source"),
                )
            )
            returned_identifiers.add(identifier)
            if review.get("status") == "Pass":
                passed_items.extend(content_items)

    elif platform == "instagram":
        for item in raw_items:
            identifier = (
                extract_platform_identifier(platform, item.get("url"))
                or extract_platform_identifier(platform, item.get("username"))
            )
            if not identifier:
                continue
            profile_url = item.get("url") or build_canonical_profile_url(platform, identifier)
            upload_metadata = resolve_upload_metadata(upload_metadata_lookup, profile_url, identifier)
            review = check_instagram_profile(item, upload_metadata=upload_metadata, runtime_rules=runtime_rules)
            profile_reviews.append(
                build_profile_review_record(
                    platform,
                    username=identifier,
                    profile_url=profile_url,
                    status=review.get("status"),
                    reason=review.get("reason"),
                    covers=review.get("covers"),
                    latest_post_time=review.get("latest_post_time"),
                    soft_flags=review.get("soft_flags"),
                    stats=review.get("stats"),
                    upload_metadata=upload_metadata,
                    resolved_cover_limit=review.get("resolved_cover_limit"),
                    visual_contract_source=review.get("visual_contract_source"),
                )
            )
            returned_identifiers.add(identifier)
            if review.get("status") == "Pass":
                passed_items.append(item)

    elif platform == "youtube":
        grouped = {}
        for item in raw_items:
            identifier = (
                extract_platform_identifier(platform, item.get("inputChannelUrl"))
                or extract_platform_identifier(platform, item.get("input"))
                or extract_platform_identifier(platform, item.get("channelUsername"))
                or extract_platform_identifier(platform, item.get("channelUrl"))
                or extract_platform_identifier(platform, item.get("channelName"))
                or extract_platform_identifier(platform, ((item.get("aboutChannelInfo") or {}).get("channelUrl")))
                or extract_platform_identifier(platform, ((item.get("aboutChannelInfo") or {}).get("inputChannelUrl")))
                or extract_platform_identifier(platform, ((item.get("aboutChannelInfo") or {}).get("channelUsername")))
            )
            if not identifier:
                continue
            grouped.setdefault(identifier, []).append(item)

        for identifier, grouped_items in grouped.items():
            first_item = grouped_items[0]
            profile_url = (
                first_item.get("inputChannelUrl")
                or first_item.get("input")
                or (
                    build_canonical_profile_url(platform, first_item.get("channelUsername"))
                    if first_item.get("channelUsername")
                    else ""
                )
                or first_item.get("channelUrl")
                or first_item.get("channelLink")
                or ((first_item.get("aboutChannelInfo") or {}).get("channelUrl"))
                or ((first_item.get("aboutChannelInfo") or {}).get("inputChannelUrl"))
                or build_canonical_profile_url(platform, identifier)
            )
            upload_metadata = resolve_upload_metadata(upload_metadata_lookup, profile_url, identifier)
            review = check_youtube_profile(grouped_items, runtime_rules=runtime_rules)
            profile_reviews.append(
                build_profile_review_record(
                    platform,
                    username=identifier,
                    profile_url=profile_url,
                    status=review.get("status"),
                    reason=review.get("reason"),
                    covers=review.get("covers"),
                    latest_post_time=review.get("latest_post_time"),
                    soft_flags=review.get("soft_flags"),
                    stats=review.get("stats"),
                    upload_metadata=upload_metadata,
                    resolved_cover_limit=review.get("resolved_cover_limit"),
                    visual_contract_source=review.get("visual_contract_source"),
                )
            )
            returned_identifiers.add(identifier)
            if review.get("status") == "Pass":
                passed_items.extend(grouped_items)
    else:
        raise ValueError(f"Unsupported platform: {platform}")

    expected_identifiers = [extract_platform_identifier(platform, item) for item in expected_profiles]
    expected_identifiers = [item for item in expected_identifiers if item]
    for identifier in expected_identifiers:
        if identifier not in returned_identifiers:
            profile_reviews.append(build_missing_review(platform, identifier, upload_metadata_lookup))

    profile_reviews = sorted(
        profile_reviews,
        key=lambda item: (
            {"Pass": 0, "Reject": 1, "Missing": 2}.get(item.get("status"), 3),
            item.get("username") or "",
        ),
    )

    return {
        "success": True,
        "raw_items": raw_items,
        "passed_items": passed_items,
        "profile_reviews": profile_reviews,
        "original_profiles": len(returned_identifiers),
        "passed_profiles": len([item for item in profile_reviews if item.get("status") == "Pass"]),
        "missing_profiles": [item for item in profile_reviews if item.get("status") == "Missing"],
        "rejected_profiles": [
            {
                "username": item.get("username"),
                "profile_url": item.get("profile_url"),
                "reason": item.get("reason"),
            }
            for item in profile_reviews
            if item.get("status") in {"Reject", "Missing"}
        ],
        "successful_identifiers": sorted(returned_identifiers),
    }
