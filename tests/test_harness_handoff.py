from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from harness.contract import FAILURE_SCHEMA_VERSION, RUN_CONTRACT_VERSION, attach_run_contract
from harness.failures import attach_failure_to_summary, build_failure_payload
from harness.handoff import WORKFLOW_HANDOFF_VERSION, build_workflow_handoff, write_workflow_handoff


REPO_ROOT = Path(__file__).resolve().parents[1]


def _build_task_spec(scope: str, canonical_boundary: str, run_root: Path) -> dict[str, object]:
    return {
        "scope": scope,
        "canonical_boundary": canonical_boundary,
        "run": {
            "run_id": "20260401_MINISO_deadbeef",
            "run_root": str(run_root),
            "summary_json": str(run_root / "summary.json"),
            "task_spec_json": str(run_root / "task_spec.json"),
            "workflow_handoff_json": str(run_root / "workflow_handoff.json"),
        },
        "intent": {
            "task_name": "MINISO",
            "task_upload_url": "https://example.com/task",
            "requested_platforms": ["instagram"],
            "stop_after": "keep-list",
        },
        "controls": {
            "requested_platforms": ["instagram"],
            "skip_scrape": False,
            "vision_provider": "openai",
            "reuse_existing": False,
        },
        "paths": {
            "upstream_workflow_handoff_json": str(run_root / "upstream" / "workflow_handoff.json"),
            "upstream_summary_json": str(run_root / "upstream" / "summary.json"),
            "upstream_task_spec_json": str(run_root / "upstream" / "task_spec.json"),
            "downstream_workflow_handoff_json": str(run_root / "downstream" / "workflow_handoff.json"),
            "downstream_summary_json": str(run_root / "downstream" / "summary.json"),
            "downstream_task_spec_json": str(run_root / "downstream" / "task_spec.json"),
        },
    }


def _build_summary(scope: str, run_root: Path, *, status: str) -> dict[str, object]:
    summary = {
        "status": status,
        "run_id": "20260401_MINISO_deadbeef",
        "run_root": str(run_root),
        "summary_json": str(run_root / "summary.json"),
        "task_spec_json": str(run_root / "task_spec.json"),
        "contract": {"scope": scope, "canonical_boundary": "keep-list"},
        "resume_points": {},
        "setup": {"completed": True, "skipped": False},
    }
    return attach_run_contract(summary)


class HarnessWorkflowHandoffTests(unittest.TestCase):
    def test_build_workflow_handoff_marks_completed_run(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            run_root = Path(temp_dir) / "run"
            task_spec = _build_task_spec("task-upload-to-final-export", "final-export", run_root)
            summary = _build_summary("task-upload-to-final-export", run_root, status="completed")
            handoff = build_workflow_handoff(
                summary=summary,
                task_spec=task_spec,
                task_spec_available=True,
            )

        self.assertEqual(handoff["workflow_handoff_version"], WORKFLOW_HANDOFF_VERSION)
        self.assertEqual(handoff["source_contract_version"], RUN_CONTRACT_VERSION)
        self.assertEqual(handoff["source_failure_schema_version"], FAILURE_SCHEMA_VERSION)
        self.assertEqual(handoff["scope"], "task-upload-to-final-export")
        self.assertEqual(handoff["canonical_boundary"], "final-export")
        self.assertEqual(handoff["verdict"]["outcome"], "completed")
        self.assertEqual(handoff["recommended_action"], "consume_outputs")
        self.assertTrue(handoff["task_spec_available"])
        self.assertEqual(handoff["current_stage"], "completed")
        self.assertEqual(handoff["next_report_triggers"], [])
        self.assertEqual(handoff["intent_summary"]["intent"]["task_name"], "MINISO")
        self.assertFalse(handoff["resume"]["available"])
        self.assertEqual(
            handoff["pointers"]["downstream_workflow_handoff_json"],
            str(run_root / "downstream" / "workflow_handoff.json"),
        )

    def test_build_workflow_handoff_marks_stopped_run_with_resume_points(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            run_root = Path(temp_dir) / "run"
            task_spec = _build_task_spec("task-upload-to-keep-list", "keep-list", run_root)
            summary = _build_summary("task-upload-to-keep-list", run_root, status="stopped_after_keep-list")
            summary["resume_points"] = {"keep_list": {"keep_workbook": "/tmp/keep.xlsx"}}
            summary["contract"]["canonical_resume_point"] = "keep_list"
            handoff = build_workflow_handoff(
                summary=summary,
                task_spec=task_spec,
                task_spec_available=True,
            )

        self.assertEqual(handoff["verdict"]["outcome"], "stopped")
        self.assertEqual(handoff["recommended_action"], "resume_run")
        self.assertTrue(handoff["resume"]["available"])
        self.assertEqual(handoff["resume"]["canonical_resume_point"], "keep_list")
        self.assertEqual(handoff["resume"]["resume_point_keys"], ["keep_list"])
        self.assertNotIn("resume_points", handoff["pointers"])

    def test_build_workflow_handoff_preserves_failure_decision(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            run_root = Path(temp_dir) / "run"
            task_spec = _build_task_spec("keep-list-screening", "screening", run_root)
            summary = _build_summary("keep-list-screening", run_root, status="failed")
            attach_failure_to_summary(
                summary,
                build_failure_payload(
                    stage="platform_scrape",
                    error_code="SCRAPE_FAILED",
                    message="至少一个平台 scrape 未完成。",
                    remediation="先重试当前 run。",
                ),
            )
            attach_run_contract(summary)
            handoff = build_workflow_handoff(
                summary=summary,
                task_spec=task_spec,
                task_spec_available=True,
            )

        self.assertEqual(handoff["verdict"]["outcome"], "failed")
        self.assertEqual(handoff["failure"]["error_code"], "SCRAPE_FAILED")
        self.assertEqual(handoff["failure"]["stage"], "platform_scrape")
        self.assertEqual(handoff["failure_decision"]["category"], "external_runtime")
        self.assertEqual(handoff["failure_decision"]["resolution_mode"], "auto_retry")
        self.assertTrue(handoff["retryable"])
        self.assertFalse(handoff["requires_manual_intervention"])

    def test_build_workflow_handoff_exposes_running_stage_and_report_triggers(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            run_root = Path(temp_dir) / "run"
            task_spec = _build_task_spec("keep-list-screening", "screening", run_root)
            summary = _build_summary("keep-list-screening", run_root, status="running")
            summary["platforms"] = {
                "instagram": {
                    "status": "running",
                    "current_stage": "scrape_running",
                }
            }
            summary["setup"] = {"completed": True, "skipped": False}
            attach_run_contract(summary)
            handoff = build_workflow_handoff(
                summary=summary,
                task_spec=task_spec,
                task_spec_available=True,
            )

        self.assertEqual(handoff["current_stage"], "instagram:scrape_running")
        self.assertEqual(
            handoff["next_report_triggers"],
            ["stage_changed", "structured_failure", "run_terminal"],
        )
        self.assertEqual(handoff["verdict"]["outcome"], "running")

    def test_write_workflow_handoff_marks_preflight_failure_without_task_spec(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            run_root = Path(temp_dir) / "run"
            task_spec = _build_task_spec("task-upload-to-final-export", "final-export", run_root)
            summary = _build_summary("task-upload-to-final-export", run_root, status="failed")
            summary["setup"] = {"completed": False, "skipped": True}
            attach_failure_to_summary(
                summary,
                build_failure_payload(
                    stage="preflight",
                    error_code="EMPLOYEE_INFO_URL_MISSING",
                    message="缺少 EMPLOYEE_INFO_URL。",
                    remediation="补齐配置后重试。",
                    failure_layer="preflight",
                ),
            )
            attach_run_contract(summary)
            handoff_path = run_root / "workflow_handoff.json"
            payload = write_workflow_handoff(
                handoff_path,
                summary=summary,
                task_spec=task_spec,
                task_spec_available=False,
            )
            persisted = json.loads(handoff_path.read_text(encoding="utf-8"))

        self.assertFalse(payload["task_spec_available"])
        self.assertEqual(payload["failure"]["failure_layer"], "preflight")
        self.assertEqual(payload["failure_decision"]["category"], "configuration")
        self.assertEqual(persisted["task_spec_json"], str(run_root / "task_spec.json"))
        self.assertEqual(persisted["current_stage"], "preflight")

    def test_schema_draft_matches_built_handoff_shape(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            run_root = Path(temp_dir) / "run"
            task_spec = _build_task_spec("task-upload-to-final-export", "final-export", run_root)
            summary = _build_summary("task-upload-to-final-export", run_root, status="completed")
            handoff = build_workflow_handoff(
                summary=summary,
                task_spec=task_spec,
                task_spec_available=True,
            )
        schema = json.loads(
            (REPO_ROOT / "harness" / "schemas" / "workflow-handoff.schema.json").read_text(encoding="utf-8")
        )

        self.assertEqual(schema["properties"]["workflow_handoff_version"]["const"], WORKFLOW_HANDOFF_VERSION)
        for required_key in schema["required"]:
            self.assertIn(required_key, handoff)


if __name__ == "__main__":
    unittest.main()
