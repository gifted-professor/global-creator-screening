import os
import math
import re
import statistics
from datetime import datetime, timezone
from urllib.parse import urlparse


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

DEFAULT_RULES = {
    "shared": {
        "active_days_max": 30,
        "visual_review_cover_limit": 9,
    },
    "instagram": {
        "allowed_regions": ["US"],
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

    if not isinstance(active_rulespec, dict):
        return resolved

    overrides = active_rulespec.get("platform_overrides") or {}
    if not isinstance(overrides, dict):
        return resolved

    shared_override = overrides.get("shared")
    if isinstance(shared_override, dict):
        resolved.update({key: value for key, value in shared_override.items() if value not in (None, "")})

    platform_override = overrides.get(platform)
    if isinstance(platform_override, dict):
        resolved.update({key: value for key, value in platform_override.items() if value not in (None, "")})

    return resolved


def resolve_visual_review_candidate_cover_limit(runtime_rules=None):
    rules = runtime_rules or {}
    requested_limit = int(rules.get("visual_review_cover_limit") or DEFAULT_RULES["shared"]["visual_review_cover_limit"])
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


def extract_youtube_cover_urls(items, cover_limit):
    return [
        item.get("thumbnailUrl")
        for item in list(items or [])[:max(1, int(cover_limit or 1))]
        if str(item.get("thumbnailUrl") or "").strip()
    ]


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

    cover_limit = resolve_visual_review_candidate_cover_limit(rules)
    covers = extract_tiktok_cover_urls(sorted_items, cover_limit)

    return {
        "status": "Pass",
        "reason": (
            f"近 {active_days_max} 天有更新；播放量达标（均值 {avg_views:.0f}，中位数 {median_views:.0f}）；"
            f"已提取 {len(covers)} 张封面"
        ),
        "latest_post_time": latest_post_time,
        "covers": covers,
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

    cover_limit = resolve_visual_review_candidate_cover_limit(rules)
    covers = extract_instagram_cover_urls(sorted_posts, cover_limit)
    return {
        "status": "Pass",
        "reason": f"地区符合；近 {active_days_max} 天有更新；已提取 {len(covers)} 张封面",
        "latest_post_time": latest_post_time,
        "covers": covers,
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

    cover_limit = resolve_visual_review_candidate_cover_limit(rules)
    covers = extract_youtube_cover_urls(sorted_items, cover_limit)
    return {
        "status": "Pass",
        "reason": f"近 {active_days_max} 天有更新；已提取 {len(covers)} 张封面",
        "latest_post_time": latest_post_time,
        "covers": covers,
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
            author_name = ((item.get("authorMeta") or {}).get("name")) or ""
            identifier = extract_platform_identifier(platform, ((item.get("authorMeta") or {}).get("profileUrl")) or author_name)
            if not identifier:
                continue
            grouped.setdefault(identifier, []).append(item)

        for identifier, grouped_items in grouped.items():
            first_item = grouped_items[0]
            profile_url = ((first_item.get("authorMeta") or {}).get("profileUrl")) or build_canonical_profile_url(platform, identifier)
            upload_metadata = resolve_upload_metadata(upload_metadata_lookup, profile_url, identifier)
            review = check_tiktok_profile(grouped_items, runtime_rules=runtime_rules)
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
                )
            )
            returned_identifiers.add(identifier)
            if review.get("status") == "Pass":
                passed_items.extend(grouped_items)

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
