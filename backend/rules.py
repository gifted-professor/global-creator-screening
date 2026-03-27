import json
import re
from datetime import datetime


UNSUPPORTED_HINTS = {
    "comment_text": ("评论区", "评论文本", "评论质量"),
    "full_video_content": ("完整视频", "整条视频", "视频内容"),
    "audience_demographics": ("粉丝画像", "受众画像", "年龄", "性别", "受众地区"),
}


def _extract_int(text, patterns):
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if not match:
            continue
        raw_value = re.sub(r"[,_\s]", "", match.group(1))
        try:
            return int(raw_value)
        except ValueError:
            continue
    return None


def _collect_unsupported_mentions(text):
    lowered = str(text or "").lower()
    items = []
    for capability, hints in UNSUPPORTED_HINTS.items():
        for hint in hints:
            if hint.lower() in lowered:
                items.append({
                    "capability": capability,
                    "matched_hint": hint,
                    "status": "missing_capabilities",
                })
                break
    return items


def compile_rulespec_from_text(text):
    raw_text = str(text or "").strip()
    if not raw_text:
        raise ValueError("sop_text 不能为空")

    platform_overrides = {
        "shared": {},
        "instagram": {},
        "tiktok": {},
        "youtube": {},
    }
    matched_rules = []

    if re.search(r"(美国|us|united states|america)", raw_text, re.IGNORECASE):
        platform_overrides["instagram"]["allowed_regions"] = ["US"]
        matched_rules.append({
            "platform": "instagram",
            "field": "allowed_regions",
            "value": ["US"],
            "status": "supported",
        })

    active_days = _extract_int(
        raw_text,
        (
            r"近\s*(\d+)\s*天(?:内)?(?:有更新|更新|活跃)",
            r"最近\s*(\d+)\s*天(?:内)?(?:有更新|更新|活跃)",
            r"(\d+)\s*天(?:内)?(?:有更新|更新|活跃)",
        ),
    )
    if active_days is not None:
        platform_overrides["shared"]["active_days_max"] = active_days
        matched_rules.append({
            "platform": "shared",
            "field": "active_days_max",
            "value": active_days,
            "status": "supported",
        })

    min_avg_views = _extract_int(
        raw_text,
        (
            r"平均播放(?:量)?[^\d]{0,8}(\d[\d,\s_]*)",
            r"均值[^\d]{0,8}(\d[\d,\s_]*)",
        ),
    )
    if min_avg_views is not None:
        platform_overrides["tiktok"]["min_avg_views"] = min_avg_views
        matched_rules.append({
            "platform": "tiktok",
            "field": "min_avg_views",
            "value": min_avg_views,
            "status": "supported",
        })

    min_median_views = _extract_int(
        raw_text,
        (
            r"中位数[^\d]{0,8}(\d[\d,\s_]*)",
            r"中位播放(?:量)?[^\d]{0,8}(\d[\d,\s_]*)",
        ),
    )
    if min_median_views is not None:
        platform_overrides["tiktok"]["min_median_views"] = min_median_views
        matched_rules.append({
            "platform": "tiktok",
            "field": "min_median_views",
            "value": min_median_views,
            "status": "supported",
        })

    visual_cover_limit = _extract_int(
        raw_text,
        (
            r"(?:封面|图片|视觉).{0,10}?(\d+)\s*张",
        ),
    )
    if visual_cover_limit is not None:
        visual_cover_limit = max(1, min(18, visual_cover_limit))
        platform_overrides["shared"]["visual_review_cover_limit"] = visual_cover_limit
        matched_rules.append({
            "platform": "shared",
            "field": "visual_review_cover_limit",
            "value": visual_cover_limit,
            "status": "supported",
        })

    unsupported = _collect_unsupported_mentions(raw_text)
    field_match_report = {
        "platforms": {
            "shared": {"items": [item for item in matched_rules if item["platform"] == "shared"]},
            "instagram": {"items": [item for item in matched_rules if item["platform"] == "instagram"]},
            "tiktok": {"items": [item for item in matched_rules if item["platform"] == "tiktok"]},
            "youtube": {"items": [item for item in matched_rules if item["platform"] == "youtube"]},
        }
    }
    missing_capabilities = {
        "items": unsupported,
    }
    rule_spec = {
        "version": "rulespec-lite-v1",
        "compiled_at": datetime.utcnow().isoformat() + "Z",
        "source_text": raw_text,
        "platform_overrides": platform_overrides,
    }
    review_notes_markdown = "\n".join(
        [
            "# RuleSpec Review Notes",
            "",
            f"- matched rules: {len(matched_rules)}",
            f"- missing capabilities: {len(unsupported)}",
        ]
    )
    return {
        "rule_spec": rule_spec,
        "field_match_report": field_match_report,
        "missing_capabilities": missing_capabilities,
        "review_notes_markdown": review_notes_markdown,
        "compiled_at": rule_spec["compiled_at"],
    }


def write_json(path, payload):
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
