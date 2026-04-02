#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from resolve_run_handoff import WorkflowResolutionError, resolve_run_handoff


TERMINAL_OUTCOMES = {"completed", "failed", "blocked", "stopped"}
RUNNING_OUTCOME = "running"
SCOPE_GOALS = {
    "task-upload-to-final-export": "执行从 task upload 到 final export 的单次链路",
    "task-upload-to-keep-list": "执行从 task upload 到 keep-list 的单次链路",
    "keep-list-screening": "执行 keep-list screening 边界 run",
}
RECOMMENDED_ACTION_ALIASES = {
    "fix_input": "repair_inputs",
}


def _first_non_empty(*values: Any) -> str:
    for value in values:
        normalized = str(value or "").strip()
        if normalized:
            return normalized
    return ""


def _format_code(value: Any) -> str:
    return f"`{value}`"


def _format_bullets(items: list[str]) -> str:
    return "\n".join(f"- {item}" for item in items)


def _format_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, list):
        return ", ".join(str(item) for item in value)
    return str(value)


def _compact_pairs(payload: dict[str, Any], preferred_keys: list[str]) -> list[str]:
    pairs: list[str] = []
    for key in preferred_keys:
        value = payload.get(key)
        if value in (None, "", [], False):
            continue
        pairs.append(f"{key}={_format_value(value)}")
    return pairs


def _build_goal(resolved: dict[str, Any]) -> str:
    handoff = dict(resolved["handoff"] or {})
    task_spec = dict(resolved.get("task_spec") or {})
    intent = dict((task_spec.get("intent") or {}) or handoff.get("intent_summary", {}).get("intent", {}) or {})

    scope = str(handoff.get("scope") or resolved.get("scope") or "").strip()
    boundary = str(handoff.get("canonical_boundary") or resolved.get("canonical_boundary") or "").strip()
    task_name = _first_non_empty(intent.get("task_name"))
    platforms = intent.get("requested_platforms") or []

    scope_phrase = SCOPE_GOALS.get(scope, f"执行 {scope or 'unknown'} run")
    if task_name:
        sentence = f"围绕 {task_name} {scope_phrase}"
    else:
        sentence = scope_phrase
    if boundary:
        sentence += f"，canonical boundary={boundary}"
    if isinstance(platforms, list) and platforms:
        sentence += f"，目标平台={', '.join(str(item) for item in platforms)}"
    return sentence + "。"


def _build_execution_inputs(resolved: dict[str, Any]) -> str:
    handoff = dict(resolved["handoff"] or {})
    intent_summary = dict(handoff.get("intent_summary") or {})
    intent = dict(intent_summary.get("intent") or {})
    controls = dict(intent_summary.get("controls") or {})

    lines = [
        f"显式目标：{_format_code(resolved['input_path'])}",
        f"workflow_handoff：{_format_code(resolved['workflow_handoff_json'])}",
    ]

    input_pairs = _compact_pairs(
        intent,
        [
            "task_name",
            "keep_workbook",
            "template_workbook",
            "task_upload_url",
            "employee_info_url",
            "requested_platforms",
            "matching_strategy",
            "brand_keyword",
            "stop_after",
        ],
    )
    if input_pairs:
        lines.append(f"关键输入：{'; '.join(input_pairs)}")

    control_pairs = _compact_pairs(
        controls,
        [
            "skip_scrape",
            "skip_visual",
            "skip_positioning_card_analysis",
            "vision_provider",
            "probe_vision_provider_only",
            "reuse_existing",
        ],
    )
    if control_pairs:
        lines.append(f"关键控制：{'; '.join(control_pairs)}")

    return _format_bullets(lines)


def _build_status_section(resolved: dict[str, Any]) -> str:
    handoff = dict(resolved["handoff"] or {})
    verdict = dict(handoff.get("verdict") or {})
    outcome = _first_non_empty(verdict.get("outcome"), handoff.get("status"), "unknown")
    run_status = _first_non_empty(handoff.get("status"))
    current_stage = _first_non_empty(handoff.get("current_stage"), "unknown")
    recommended_action = _first_non_empty(handoff.get("recommended_action"), "inspect_summary")
    next_report_triggers = handoff.get("next_report_triggers") or []

    lines = [f"状态：{outcome}"]
    if run_status and run_status != outcome:
        lines.append(f"run status：{run_status}")
    lines.append(f"当前阶段：{current_stage}")
    lines.append(f"建议动作：{recommended_action}")
    if outcome == RUNNING_OUTCOME:
        trigger_text = ", ".join(str(item) for item in next_report_triggers) if next_report_triggers else "当前阶段切换, 结构化 failure, run 进入终态"
        lines.append(f"下一次汇报触发条件：{trigger_text}")
    return _format_bullets(lines)


def _build_confirmed_facts(resolved: dict[str, Any]) -> str:
    handoff = dict(resolved["handoff"] or {})
    failure = dict(handoff.get("failure") or {})
    failure_decision = dict(handoff.get("failure_decision") or {})
    resume = dict(handoff.get("resume") or {})

    lines = [
        f"scope={handoff.get('scope') or ''}，canonical_boundary={handoff.get('canonical_boundary') or ''}",
        f"run_root={_format_code(resolved['run_root'])}",
    ]

    if bool(handoff.get("task_spec_available")):
        lines.append(f"task_spec 已物化：{_format_code(resolved['task_spec_json'])}")
    else:
        lines.append("task_spec 当前未物化，当前汇报按 handoff-only 继续。")

    if failure:
        lines.append(
            "结构化 failure 已形成："
            f"error_code={failure.get('error_code') or ''}，"
            f"failure_layer={failure.get('failure_layer') or ''}，"
            f"stage={failure.get('stage') or ''}"
        )
    if failure_decision:
        lines.append(
            "failure_decision 已形成："
            f"category={failure_decision.get('category') or ''} / "
            f"{failure_decision.get('resolution_mode') or ''}，"
            f"recommended_action={failure_decision.get('recommended_action') or ''}"
        )
    if bool(resume.get("available")):
        lines.append(
            "稳定 resume 摘要已暴露："
            f"canonical_resume_point={resume.get('canonical_resume_point') or ''}，"
            f"resume_point_keys={', '.join(str(item) for item in (resume.get('resume_point_keys') or [])) or '<none>'}"
        )
    return _format_bullets(lines[:5])


def _build_unconfirmed(resolved: dict[str, Any]) -> str:
    handoff = dict(resolved["handoff"] or {})
    verdict = dict(handoff.get("verdict") or {})
    failure_decision = dict(handoff.get("failure_decision") or {})
    outcome = _first_non_empty(verdict.get("outcome"), handoff.get("status"), "unknown")

    items: list[str] = []
    if not bool(handoff.get("task_spec_available")):
        items.append("规范化意图文件未物化，通常意味着 run 在 preflight/setup 前失败。")
    elif not bool(resolved.get("task_spec_loaded")):
        items.append("handoff 标记 task_spec_available=true，但当前没有成功读取 task_spec.json。")

    if outcome == RUNNING_OUTCOME:
        items.append("当前仍在执行，终态结论尚未形成。")
    elif outcome == "unknown":
        items.append("当前顶层 verdict 仍是 unknown；如需继续判断，需要下钻 summary.json。")

    if str(failure_decision.get("resolution_mode") or "") == "manual_investigation":
        items.append("具体根因仍需结合 summary.json 和关键 step 输出继续下钻。")

    if not items:
        items.append("当前没有关键未确认项。")
    return _format_bullets(items)


def _build_conclusion(resolved: dict[str, Any]) -> str | None:
    handoff = dict(resolved["handoff"] or {})
    verdict = dict(handoff.get("verdict") or {})
    outcome = _first_non_empty(verdict.get("outcome"), handoff.get("status"), "unknown")
    if outcome not in TERMINAL_OUTCOMES:
        return None
    conclusion = _first_non_empty(verdict.get("conclusion"))
    if conclusion:
        return conclusion
    if outcome == "stopped":
        return "本次 run 按 stop_after 主动停止，可基于 resume point 继续。"
    if outcome == "completed":
        return "本次 run 已完成，可以直接查看产物和 summary。"
    return "当前 run 已进入终态，请按结构化 verdict 和 failure_decision 处理。"


def _normalize_recommended_action(raw_action: Any) -> str:
    normalized = _first_non_empty(raw_action, "inspect_summary")
    return RECOMMENDED_ACTION_ALIASES.get(normalized, normalized)


def _suggest_resume_run(resolved: dict[str, Any]) -> list[str]:
    handoff = dict(resolved["handoff"] or {})
    resume = dict(handoff.get("resume") or {})
    point = _first_non_empty(resume.get("canonical_resume_point"), "<unknown>")
    return [
        f"基于当前 canonical resume point {point} 继续本轮 run。",
        "如需确认恢复边界，优先查看 task_spec.json 和 summary.json。",
    ]


def _suggest_consume_outputs(resolved: dict[str, Any]) -> list[str]:
    handoff = dict(resolved["handoff"] or {})
    suggestions = ["直接查看 run_root 下已落盘产物和 summary.json。"]
    if bool(handoff.get("task_spec_available")):
        suggestions.append("如需复核本轮意图边界，再查看 task_spec.json。")
    return suggestions


def _suggest_fix_configuration(_: dict[str, Any]) -> list[str]:
    return [
        "先补齐或修正当前配置，再重跑。",
        "如需定位缺口，优先核对 handoff.failure/error_code 和相关配置来源。",
    ]


def _suggest_repair_inputs(_: dict[str, Any]) -> list[str]:
    return [
        "先补齐或修正当前输入、模板或上游产物，再重跑。",
        "如需确认输入边界，优先查看 task_spec.json 与 handoff.intent_summary。",
    ]


def _suggest_repair_output_path(_: dict[str, Any]) -> list[str]:
    return [
        "先修复输出路径、权限或磁盘状态，再重跑。",
        "如需定位具体写入失败点，优先核对 handoff.failure/error_code 与目标输出目录。",
    ]


def _suggest_repair_environment(_: dict[str, Any]) -> list[str]:
    return [
        "先补齐本地依赖或运行环境，再重跑。",
        "如需定位环境缺口，优先查看失败 step 的 runtime/import 错误。",
    ]


def _suggest_inspect_child_run(_: dict[str, Any]) -> list[str]:
    return [
        "先查看 handoff.pointers 暴露的 child run handoff 或 summary，确认失败落在 upstream 还是 downstream。",
        "定位到具体 child run 后，再决定是修输入、修配置还是重试。",
    ]


def _suggest_inspect_runtime(_: dict[str, Any]) -> list[str]:
    return [
        "先查看 summary.json 和失败 step 的 runtime 输出，确认根因后再继续处理。",
        "如需还原失败上下文，优先下钻关键 stage/step 产物而不是泛化重试。",
    ]


def _suggest_retry_run(_: dict[str, Any]) -> list[str]:
    return [
        "按当前结构化 failure 允许范围先重试一次。",
        "若再次失败，再下钻 summary.json 和关键 step 输出。",
    ]


def _suggest_wait_for_completion(_: dict[str, Any]) -> list[str]:
    return [
        "继续观察当前阶段，等待下一次汇报触发条件出现。",
        "如果出现结构化 failure，再转入终态判断。",
    ]


def _suggest_inspect_summary(_: dict[str, Any]) -> list[str]:
    return [
        "先查看 summary.json 和关键 stage/step 输出。",
        "如需还原本轮规范化意图，再查看 task_spec.json。",
    ]


RECOMMENDED_ACTION_SUGGESTIONS = {
    "consume_outputs": _suggest_consume_outputs,
    "resume_run": _suggest_resume_run,
    "fix_configuration": _suggest_fix_configuration,
    "repair_inputs": _suggest_repair_inputs,
    "repair_output_path": _suggest_repair_output_path,
    "repair_environment": _suggest_repair_environment,
    "inspect_child_run": _suggest_inspect_child_run,
    "inspect_runtime": _suggest_inspect_runtime,
    "retry_run": _suggest_retry_run,
    "wait_for_completion": _suggest_wait_for_completion,
    "inspect_summary": _suggest_inspect_summary,
}


def _action_from_recommended_action(resolved: dict[str, Any]) -> list[str]:
    handoff = dict(resolved["handoff"] or {})
    verdict = dict(handoff.get("verdict") or {})
    recommended_action = _normalize_recommended_action(
        _first_non_empty(handoff.get("recommended_action"), verdict.get("recommended_action"), "inspect_summary")
    )
    suggestion_builder = RECOMMENDED_ACTION_SUGGESTIONS.get(recommended_action, _suggest_inspect_summary)
    return suggestion_builder(resolved)


def _build_next_steps(resolved: dict[str, Any]) -> str:
    return _format_bullets(_action_from_recommended_action(resolved)[:2])


def _build_decision_section(resolved: dict[str, Any]) -> str:
    return "不需要"


def build_workflow_report(target: str | Path) -> dict[str, Any]:
    resolved = resolve_run_handoff(target, load_task_spec=True, load_summary=False)
    handoff = dict(resolved["handoff"] or {})
    verdict = dict(handoff.get("verdict") or {})
    outcome = _first_non_empty(verdict.get("outcome"), handoff.get("status"), "unknown")

    sections: list[tuple[str, str]] = [
        ("1. 本轮目标", _build_goal(resolved)),
        ("2. 执行命令/输入", _build_execution_inputs(resolved)),
        ("3. 当前状态", _build_status_section(resolved)),
        ("4. 已确认事实", _build_confirmed_facts(resolved)),
        ("5. 未确认部分", _build_unconfirmed(resolved)),
    ]
    conclusion = _build_conclusion(resolved)
    if conclusion is not None:
        sections.append(("6. 结论", conclusion))
    sections.extend(
        [
            ("7. 下一步建议", _build_next_steps(resolved)),
            ("8. 是否需要我决策", _build_decision_section(resolved)),
        ]
    )

    return {
        "status": outcome,
        "terminal": outcome in TERMINAL_OUTCOMES,
        "workflow_handoff_json": resolved["workflow_handoff_json"],
        "sections": sections,
    }


def render_markdown(report: dict[str, Any]) -> str:
    blocks: list[str] = []
    for title, body in report.get("sections", []):
        blocks.append(title)
        blocks.append(body)
        blocks.append("")
    return "\n".join(blocks).rstrip() + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Render a verdict-first workflow report from a canonical run target.",
    )
    parser.add_argument("target", help="run_root or one of workflow_handoff.json/summary.json/task_spec.json")
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit the structured report payload as JSON instead of Markdown.",
    )
    args = parser.parse_args()

    try:
        report = build_workflow_report(args.target)
    except WorkflowResolutionError as exc:
        print(
            json.dumps(
                {
                    "supported": False,
                    "error_code": exc.code,
                    "message": exc.message,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 1

    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print(render_markdown(report), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
