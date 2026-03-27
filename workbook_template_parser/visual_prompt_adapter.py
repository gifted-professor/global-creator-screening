#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


PLATFORM_LABELS = {
    "tiktok": "TikTok",
    "instagram": "Instagram",
    "youtube": "YouTube",
}

SENSITIVE_TERMS = (
    "黑人",
    "白人",
    "黄种人",
    "亚裔",
    "拉丁裔",
    "中老年",
    "老年",
    "老人",
    "种族",
    "民族",
    "年龄",
    "race",
    "ethnic",
    "black",
    "white",
)


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def format_percent(value: Any) -> str:
    raw = normalize_text(value)
    if not raw:
        return ""
    try:
        number = float(raw)
    except ValueError:
        return raw
    if number.is_integer():
        return str(int(number))
    return str(number).rstrip("0").rstrip(".")


def format_operator(value: Any) -> str:
    normalized = normalize_text(value).lower()
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


def render_positive_feature(feature: dict[str, Any]) -> str:
    label = normalize_text(feature.get("label") or feature.get("source_label"))
    note = normalize_text(feature.get("note"))
    if label and note and note != label:
        return f"{label}：{note}"
    return label


def render_negative_feature(feature: dict[str, Any]) -> str:
    label = normalize_text(feature.get("label") or feature.get("source_label"))
    key = normalize_text(feature.get("key"))
    operator = format_operator(feature.get("operator"))
    threshold = format_percent(feature.get("threshold_percent"))
    note = normalize_text(feature.get("note"))

    if key == "green_screen":
        return '若明显出现绿幕、抠图感虚拟背景或大面积纯色虚拟背景，输出 Reject，reason 写"出现绿幕背景"。'

    if operator == "must_not_appear":
        if note:
            return f'若明显出现{label}（{note}），输出 Reject，reason 写"出现{label}"。'
        return f'若明显出现{label}，输出 Reject，reason 写"出现{label}"。'

    if threshold:
        reason = f"{label}过高" if label else "视觉排除项占比过高"
        return f'若{label} {operator} {threshold}%，输出 Reject，reason 写"{reason}"。'

    if note:
        return f"- {label}：{note}"
    return label


def render_manual_item(item: dict[str, Any]) -> str:
    label = normalize_text(item.get("label"))
    value = normalize_text(item.get("value"))
    note = normalize_text(item.get("note"))
    if label and value and value != label:
        return f"{label}：{value}"
    if label and note and note != label:
        return f"{label}：{note}"
    return label or value or note


def render_compliance_note(item: dict[str, Any]) -> str:
    label = normalize_text(item.get("label"))
    value = normalize_text(item.get("value"))
    note = normalize_text(item.get("note"))
    if label and value and value != label:
        return f"{label}：{value}"
    if value:
        return value
    if label and note and note != label:
        return f"{label}：{note}"
    return label or note


def has_protected_attribute_notice(compliance_notes: list[dict[str, Any]]) -> bool:
    for item in compliance_notes:
        combined = " ".join(
            normalize_text(item.get(field))
            for field in ("key", "label", "value", "note", "policy")
        ).lower()
        if "protected_attribute" in combined or "受保护属性" in combined:
            return True
    return False


def is_sensitive_compliance_item(item: dict[str, Any]) -> bool:
    combined = " ".join(
        normalize_text(item.get(field))
        for field in ("key", "label", "value", "note", "policy")
    ).lower()
    return any(term.lower() in combined for term in SENSITIVE_TERMS)


def collect_platforms(spec: dict[str, Any], explicit_platform: str | None = None) -> list[str]:
    if explicit_platform:
        return [normalize_text(explicit_platform).lower()]
    requested = [
        normalize_text(item).lower()
        for item in (spec.get("requested_platforms") or [])
        if normalize_text(item)
    ]
    if requested:
        return requested
    return ["tiktok"]


def build_visual_prompt_bundle(spec: dict[str, Any], platform: str) -> dict[str, Any]:
    visual_scope = spec.get("visual_scope") or {}
    positive_features_raw = list(visual_scope.get("positive_features") or [])
    negative_features_raw = list(visual_scope.get("negative_features") or [])
    manual_items_raw = list(spec.get("manual_review_items") or [])
    compliance_notes_raw = list(spec.get("compliance_notes") or [])

    positive_features = [
        item
        for item in (render_positive_feature(feature) for feature in positive_features_raw)
        if item
    ]
    positive_labels = [
        normalize_text(feature.get("label") or feature.get("source_label"))
        for feature in positive_features_raw
        if normalize_text(feature.get("label") or feature.get("source_label"))
    ]
    negative_features = [
        item
        for item in (render_negative_feature(feature) for feature in negative_features_raw)
        if item
    ]
    manual_items = [
        item
        for item in (render_manual_item(feature) for feature in manual_items_raw)
        if item
    ]
    compliance_note_pairs = [
        (feature, render_compliance_note(feature))
        for feature in compliance_notes_raw
        if render_compliance_note(feature)
    ]

    cover_count = visual_scope.get("cover_count") or 18
    min_hit_features = visual_scope.get("min_hit_features") or 1
    goal = normalize_text(spec.get("goal"))
    platform_name = PLATFORM_LABELS.get(platform, platform or "达人")
    include_protected_notice = has_protected_attribute_notice(compliance_notes_raw)

    lines = [
        f"你是 {platform_name} 达人初筛流程中的视觉复核员。输入图片是一位博主最近最多 {cover_count} 张封面，按时间顺序拆成最多 2 张 3x3 九宫格。请综合全部输入图片一起判断。",
        "",
        "只根据图片画面做初步判断，不要假设看不到的内容。",
    ]

    if goal:
        lines.extend(["", f"审核目标：{goal}"])

    if positive_features:
        lines.extend(["", f"步骤3 — 内容 / 视觉审核：检查是否命中以下至少 {min_hit_features} 类特征："])
        for item in positive_features:
            lines.append(f"- {item}")
        if positive_labels:
            lines.append(
                f'如果以上特征全部未命中，输出 Reject，reason 写"未命中{"/".join(positive_labels)}"。'
            )

    if negative_features:
        lines.extend(["", "步骤4 — 排除项审核："])
        for index, item in enumerate(negative_features, start=1):
            if item.startswith("- "):
                lines.append(item)
                continue
            lines.append(f"{index}. {item}")

    if manual_items:
        lines.extend(["", "人工判断提醒：不要把以下事项当作自动通过或自动拒绝条件："])
        for item in manual_items:
            lines.append(f"- {item}")

    if include_protected_notice or compliance_note_pairs:
        lines.extend(["", "合规提醒："])
        if include_protected_notice:
            lines.append("- 不要根据年龄、种族、民族、肤色、宗教等受保护属性做判断。")
        for raw_note, rendered_note in compliance_note_pairs:
            if include_protected_notice and "受保护属性" in rendered_note:
                continue
            if include_protected_notice and is_sensitive_compliance_item(raw_note):
                continue
            lines.append(f"- {rendered_note}")

    lines.extend(
        [
            "",
            "如果通过以上检查，输出 Pass。",
            "",
            "如果同时命中多项风险，不要只写一项：",
            "- `reason` 写一句主结论。",
            "- `signals` 尽量列出所有已识别到的命中项，最多 3 个，宁可短一点，也不要漏掉明显命中项。",
            "- `signals` 只写风险点，不要重复空话。",
            "",
            "请只返回 JSON，不要加 markdown，不要加额外说明，格式固定为：",
            '{"decision":"Pass 或 Reject","reason":"一句中文原因","signals":["最多 3 个简短中文信号"]}',
        ]
    )

    return {
        "platform": platform,
        "source": "visual_reuse_spec",
        "cover_count": cover_count,
        "min_hit_features": min_hit_features,
        "positive_feature_count": len(positive_features),
        "negative_feature_count": len(negative_features),
        "manual_review_item_count": len(manual_items),
        "prompt": "\n".join(lines),
    }


def build_visual_prompt_artifacts(spec: dict[str, Any], explicit_platform: str | None = None) -> dict[str, Any]:
    bundles: dict[str, Any] = {}
    for platform in collect_platforms(spec, explicit_platform):
        bundles[platform] = build_visual_prompt_bundle(spec, platform)
    return bundles


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Render reusable vision prompts from visual_reuse_spec.json.")
    parser.add_argument("--input", required=True, help="visual_reuse_spec.json 路径")
    parser.add_argument("--platform", help="指定单个平台输出")
    parser.add_argument("--output", help="输出 json 路径，不填则直接打印")
    args = parser.parse_args()

    input_path = Path(args.input).resolve()
    spec = load_json(input_path)
    payload = build_visual_prompt_artifacts(spec, explicit_platform=args.platform)

    if args.output:
        write_json(Path(args.output).resolve(), payload)
        return

    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
