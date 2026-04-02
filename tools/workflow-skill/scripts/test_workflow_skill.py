#!/usr/bin/env python3
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from render_workflow_report import build_workflow_report, render_markdown
from resolve_run_handoff import WorkflowResolutionError, resolve_run_handoff


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _make_handoff(
    run_root: Path,
    *,
    scope: str = "keep-list-screening",
    canonical_boundary: str = "screening",
    status: str = "completed",
    outcome: str = "completed",
    recommended_action: str = "consume_outputs",
    conclusion: str = "本次 run 已完成，可以直接查看 summary 和产物。",
    task_spec_available: bool = True,
    current_stage: str = "instagram:completed",
    next_report_triggers: list[str] | None = None,
    resume: dict | None = None,
    failure: dict | None = None,
    failure_decision: dict | None = None,
) -> dict:
    summary_path = run_root / "summary.json"
    task_spec_path = run_root / "task_spec.json"
    return {
        "workflow_handoff_version": "harness.workflow-handoff.v1-draft",
        "source_contract_version": "harness.run-summary.v1-draft",
        "source_failure_schema_version": "harness.failure.v1-draft",
        "scope": scope,
        "canonical_boundary": canonical_boundary,
        "run_id": "run-001",
        "run_root": str(run_root),
        "status": status,
        "summary_json": str(summary_path),
        "task_spec_json": str(task_spec_path),
        "task_spec_available": task_spec_available,
        "current_stage": current_stage,
        "next_report_triggers": list(next_report_triggers or []),
        "verdict": {
            "outcome": outcome,
            "status": status,
            "failure_layer": "",
            "category": (failure_decision or {}).get("category", ""),
            "resolution_mode": (failure_decision or {}).get("resolution_mode", ""),
            "retryable": bool((failure_decision or {}).get("retryable", False)),
            "requires_manual_intervention": bool((failure_decision or {}).get("requires_manual_intervention", False)),
            "recommended_action": recommended_action,
            "conclusion": conclusion,
        },
        "recommended_action": recommended_action,
        "retryable": bool((failure_decision or {}).get("retryable", False)),
        "requires_manual_intervention": bool((failure_decision or {}).get("requires_manual_intervention", False)),
        "resume": resume
        or {
            "available": False,
            "canonical_resume_point": "",
            "resume_point_keys": [],
        },
        "intent_summary": {
            "intent": {
                "task_name": "MINISO",
                "requested_platforms": ["instagram"],
                "stop_after": "",
            },
            "controls": {
                "skip_scrape": False,
                "skip_visual": False,
            },
        },
        "pointers": {
            "run_root": str(run_root),
            "summary_json": str(summary_path),
            "task_spec_json": str(task_spec_path),
        },
        **({"failure": failure} if failure else {}),
        **({"failure_decision": failure_decision} if failure_decision else {}),
    }


def _make_task_spec(run_root: Path, *, scope: str = "keep-list-screening") -> dict:
    return {
        "scope": scope,
        "canonical_boundary": "screening",
        "run": {
            "run_id": "run-001",
            "run_root": str(run_root),
            "summary_json": str(run_root / "summary.json"),
            "task_spec_json": str(run_root / "task_spec.json"),
            "workflow_handoff_json": str(run_root / "workflow_handoff.json"),
        },
        "intent": {
            "task_name": "MINISO",
            "requested_platforms": ["instagram"],
        },
        "controls": {
            "skip_scrape": False,
        },
    }


def _make_failure_decision(
    *,
    category: str,
    resolution_mode: str,
    recommended_action: str,
    conclusion: str,
    retryable: bool = False,
    requires_manual_intervention: bool = True,
) -> dict:
    return {
        "category": category,
        "resolution_mode": resolution_mode,
        "retryable": retryable,
        "requires_manual_intervention": requires_manual_intervention,
        "recommended_action": recommended_action,
        "conclusion": conclusion,
    }


class WorkflowSkillTests(unittest.TestCase):
    def test_resolve_run_root_finds_handoff(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            run_root = Path(tmp_dir) / "run"
            handoff = _make_handoff(run_root)
            _write_json(run_root / "workflow_handoff.json", handoff)
            _write_json(run_root / "task_spec.json", _make_task_spec(run_root))

            resolved = resolve_run_handoff(run_root)

        self.assertTrue(resolved["supported"])
        self.assertEqual(resolved["input_kind"], "run_root")
        self.assertEqual(resolved["scope"], "keep-list-screening")
        self.assertTrue(resolved["task_spec_loaded"])

    def test_resolve_summary_path_backtracks_to_handoff(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            run_root = Path(tmp_dir) / "run"
            handoff = _make_handoff(run_root)
            _write_json(run_root / "workflow_handoff.json", handoff)
            _write_json(run_root / "task_spec.json", _make_task_spec(run_root))
            _write_json(run_root / "summary.json", {"status": "completed"})

            resolved = resolve_run_handoff(run_root / "summary.json")

        self.assertEqual(resolved["input_kind"], "summary_json")
        self.assertEqual(Path(resolved["workflow_handoff_json"]).name, "workflow_handoff.json")

    def test_rejects_unsupported_scope(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            run_root = Path(tmp_dir) / "run"
            handoff = _make_handoff(run_root, scope="operator-bootstrap")
            _write_json(run_root / "workflow_handoff.json", handoff)

            with self.assertRaises(WorkflowResolutionError) as ctx:
                resolve_run_handoff(run_root)

        self.assertEqual(ctx.exception.code, "unsupported_scope")

    def test_rejects_invalid_or_unsupported_handoff_payloads(self) -> None:
        cases = [
            (
                "missing_version",
                lambda handoff: handoff.pop("workflow_handoff_version"),
                "unsupported_workflow_handoff_version",
            ),
            (
                "unsupported_version",
                lambda handoff: handoff.__setitem__("workflow_handoff_version", "harness.workflow-handoff.v0"),
                "unsupported_workflow_handoff_version",
            ),
            (
                "missing_required_top_level_field",
                lambda handoff: handoff.pop("resume"),
                "invalid_workflow_handoff",
            ),
            (
                "missing_required_verdict_field",
                lambda handoff: handoff["verdict"].pop("conclusion"),
                "invalid_workflow_handoff",
            ),
            (
                "missing_required_resume_field",
                lambda handoff: handoff["resume"].pop("resume_point_keys"),
                "invalid_workflow_handoff",
            ),
        ]

        for name, mutate, expected_code in cases:
            with self.subTest(case=name):
                with tempfile.TemporaryDirectory() as tmp_dir:
                    run_root = Path(tmp_dir) / "run"
                    handoff = _make_handoff(run_root)
                    mutate(handoff)
                    _write_json(run_root / "workflow_handoff.json", handoff)

                    with self.assertRaises(WorkflowResolutionError) as ctx:
                        resolve_run_handoff(run_root)

                self.assertEqual(ctx.exception.code, expected_code)

    def test_handoff_only_report_supports_task_spec_unavailable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            run_root = Path(tmp_dir) / "run"
            failure = {
                "error_code": "FEISHU_APP_ID_MISSING",
                "message": "缺少 FEISHU_APP_ID。",
                "failure_layer": "preflight",
                "stage": "preflight",
            }
            failure_decision = {
                "category": "configuration",
                "resolution_mode": "manual_fix",
                "retryable": False,
                "requires_manual_intervention": True,
                "recommended_action": "fix_configuration",
                "conclusion": "需要人工补齐或修正配置后重试。",
            }
            handoff = _make_handoff(
                run_root,
                scope="task-upload-to-keep-list",
                canonical_boundary="keep-list",
                status="failed",
                outcome="failed",
                recommended_action="fix_configuration",
                conclusion="需要人工补齐或修正配置后重试。",
                task_spec_available=False,
                current_stage="preflight",
                failure=failure,
                failure_decision=failure_decision,
            )
            _write_json(run_root / "workflow_handoff.json", handoff)

            report = build_workflow_report(run_root)
            markdown = render_markdown(report)

        self.assertTrue(report["terminal"])
        self.assertIn("task_spec 当前未物化", markdown)
        self.assertIn("规范化意图文件未物化", markdown)
        self.assertIn("6. 结论", markdown)

    def test_running_report_omits_conclusion_and_uses_triggers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            run_root = Path(tmp_dir) / "run"
            handoff = _make_handoff(
                run_root,
                status="running",
                outcome="running",
                recommended_action="wait_for_completion",
                conclusion="本次 run 正在执行。",
                current_stage="instagram:scrape_running",
                next_report_triggers=["stage_changed", "structured_failure", "run_terminal"],
            )
            _write_json(run_root / "workflow_handoff.json", handoff)
            _write_json(run_root / "task_spec.json", _make_task_spec(run_root))

            markdown = render_markdown(build_workflow_report(run_root))

        self.assertIn("状态：running", markdown)
        self.assertIn("当前阶段：instagram:scrape_running", markdown)
        self.assertIn("下一次汇报触发条件：stage_changed, structured_failure, run_terminal", markdown)
        self.assertNotIn("\n6. 结论\n", markdown)

    def test_stopped_report_prefers_resume_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            run_root = Path(tmp_dir) / "run"
            handoff = _make_handoff(
                run_root,
                scope="task-upload-to-keep-list",
                canonical_boundary="keep-list",
                status="stopped_after_task-assets",
                outcome="stopped",
                recommended_action="resume_run",
                conclusion="本次 run 按 stop_after 主动停止，可基于 resume point 继续。",
                current_stage="task_assets",
                resume={
                    "available": True,
                    "canonical_resume_point": "keep_list",
                    "resume_point_keys": ["task_assets", "keep_list"],
                },
            )
            _write_json(run_root / "workflow_handoff.json", handoff)
            _write_json(run_root / "task_spec.json", _make_task_spec(run_root, scope="task-upload-to-keep-list"))

            markdown = render_markdown(build_workflow_report(run_root))

        self.assertIn("状态：stopped", markdown)
        self.assertIn("本次 run 按 stop_after 主动停止", markdown)
        self.assertIn("canonical resume point keep_list", markdown)
        self.assertNotIn("resume_points", markdown)

    def test_action_mapping_covers_canonical_actions_and_aliases(self) -> None:
        cases = [
            (
                "repair_inputs",
                _make_failure_decision(
                    category="input",
                    resolution_mode="manual_fix",
                    recommended_action="repair_inputs",
                    conclusion="需要人工修复输入、模板或上游产物后重试。",
                ),
                [
                    "先补齐或修正当前输入、模板或上游产物，再重跑。",
                    "如需确认输入边界，优先查看 task_spec.json 与 handoff.intent_summary。",
                ],
            ),
            (
                "fix_input",
                _make_failure_decision(
                    category="input",
                    resolution_mode="manual_fix",
                    recommended_action="fix_input",
                    conclusion="需要人工修复输入、模板或上游产物后重试。",
                ),
                [
                    "先补齐或修正当前输入、模板或上游产物，再重跑。",
                    "如需确认输入边界，优先查看 task_spec.json 与 handoff.intent_summary。",
                ],
            ),
            (
                "repair_output_path",
                _make_failure_decision(
                    category="filesystem",
                    resolution_mode="manual_fix",
                    recommended_action="repair_output_path",
                    conclusion="需要人工修复输出路径、权限或磁盘状态后重试。",
                ),
                [
                    "先修复输出路径、权限或磁盘状态，再重跑。",
                    "如需定位具体写入失败点，优先核对 handoff.failure/error_code 与目标输出目录。",
                ],
            ),
            (
                "repair_environment",
                _make_failure_decision(
                    category="dependency",
                    resolution_mode="manual_fix",
                    recommended_action="repair_environment",
                    conclusion="需要人工补齐本地依赖或运行环境后重试。",
                ),
                [
                    "先补齐本地依赖或运行环境，再重跑。",
                    "如需定位环境缺口，优先查看失败 step 的 runtime/import 错误。",
                ],
            ),
            (
                "inspect_child_run",
                _make_failure_decision(
                    category="orchestration",
                    resolution_mode="manual_investigation",
                    recommended_action="inspect_child_run",
                    conclusion="需要先检查子 run summary 和运行态，再决定是否重试。",
                ),
                [
                    "先查看 handoff.pointers 暴露的 child run handoff 或 summary，确认失败落在 upstream 还是 downstream。",
                    "定位到具体 child run 后，再决定是修输入、修配置还是重试。",
                ],
            ),
            (
                "inspect_runtime",
                _make_failure_decision(
                    category="runtime",
                    resolution_mode="manual_investigation",
                    recommended_action="inspect_runtime",
                    conclusion="需要先检查运行时错误和相关 step 产物，再决定是否重试。",
                ),
                [
                    "先查看 summary.json 和失败 step 的 runtime 输出，确认根因后再继续处理。",
                    "如需还原失败上下文，优先下钻关键 stage/step 产物而不是泛化重试。",
                ],
            ),
            (
                "retry_run",
                _make_failure_decision(
                    category="external_runtime",
                    resolution_mode="auto_retry",
                    recommended_action="retry_run",
                    conclusion="更像临时运行失败，可以先直接重试当前 run；若连续失败再转人工排查。",
                    retryable=True,
                    requires_manual_intervention=False,
                ),
                [
                    "按当前结构化 failure 允许范围先重试一次。",
                    "若再次失败，再下钻 summary.json 和关键 step 输出。",
                ],
            ),
            (
                "unknown_action",
                _make_failure_decision(
                    category="runtime",
                    resolution_mode="manual_investigation",
                    recommended_action="unknown_action",
                    conclusion="需要继续检查。",
                ),
                [
                    "先查看 summary.json 和关键 stage/step 输出。",
                    "如需还原本轮规范化意图，再查看 task_spec.json。",
                ],
            ),
        ]

        for action, failure_decision, expected_lines in cases:
            with self.subTest(action=action):
                with tempfile.TemporaryDirectory() as tmp_dir:
                    run_root = Path(tmp_dir) / "run"
                    handoff = _make_handoff(
                        run_root,
                        status="failed",
                        outcome="failed",
                        recommended_action=action,
                        conclusion=failure_decision["conclusion"],
                        failure_decision=failure_decision,
                    )
                    _write_json(run_root / "workflow_handoff.json", handoff)
                    _write_json(run_root / "task_spec.json", _make_task_spec(run_root))

                    markdown = render_markdown(build_workflow_report(run_root))

                for expected_line in expected_lines:
                    self.assertIn(expected_line, markdown)

    def test_section_8_is_always_no_decision_in_v1(self) -> None:
        cases = [
            (
                "running",
                {
                    "status": "running",
                    "outcome": "running",
                    "recommended_action": "wait_for_completion",
                    "conclusion": "本次 run 正在执行。",
                    "failure_decision": None,
                },
            ),
            (
                "completed",
                {
                    "status": "completed",
                    "outcome": "completed",
                    "recommended_action": "consume_outputs",
                    "conclusion": "本次 run 已完成，可以直接查看 summary 和产物。",
                    "failure_decision": None,
                },
            ),
            (
                "manual_fix",
                {
                    "status": "failed",
                    "outcome": "failed",
                    "recommended_action": "fix_configuration",
                    "conclusion": "需要人工补齐或修正配置后重试。",
                    "failure_decision": _make_failure_decision(
                        category="configuration",
                        resolution_mode="manual_fix",
                        recommended_action="fix_configuration",
                        conclusion="需要人工补齐或修正配置后重试。",
                    ),
                },
            ),
            (
                "manual_investigation",
                {
                    "status": "failed",
                    "outcome": "failed",
                    "recommended_action": "inspect_runtime",
                    "conclusion": "需要先检查运行时错误和相关 step 产物，再决定是否重试。",
                    "failure_decision": _make_failure_decision(
                        category="runtime",
                        resolution_mode="manual_investigation",
                        recommended_action="inspect_runtime",
                        conclusion="需要先检查运行时错误和相关 step 产物，再决定是否重试。",
                    ),
                },
            ),
            (
                "retry_run",
                {
                    "status": "failed",
                    "outcome": "failed",
                    "recommended_action": "retry_run",
                    "conclusion": "更像临时运行失败，可以先直接重试当前 run；若连续失败再转人工排查。",
                    "failure_decision": _make_failure_decision(
                        category="external_runtime",
                        resolution_mode="auto_retry",
                        recommended_action="retry_run",
                        conclusion="更像临时运行失败，可以先直接重试当前 run；若连续失败再转人工排查。",
                        retryable=True,
                        requires_manual_intervention=False,
                    ),
                },
            ),
        ]

        for name, case in cases:
            with self.subTest(case=name):
                with tempfile.TemporaryDirectory() as tmp_dir:
                    run_root = Path(tmp_dir) / "run"
                    handoff = _make_handoff(
                        run_root,
                        status=case["status"],
                        outcome=case["outcome"],
                        recommended_action=case["recommended_action"],
                        conclusion=case["conclusion"],
                        failure_decision=case["failure_decision"],
                    )
                    _write_json(run_root / "workflow_handoff.json", handoff)
                    _write_json(run_root / "task_spec.json", _make_task_spec(run_root))

                    markdown = render_markdown(build_workflow_report(run_root))

                self.assertIn("8. 是否需要我决策", markdown)
                self.assertTrue(markdown.rstrip().endswith("不需要"))


if __name__ == "__main__":
    unittest.main()
