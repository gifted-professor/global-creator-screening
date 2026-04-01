from __future__ import annotations

import json
import unittest
from pathlib import Path

from harness.contract import (
    FAILURE_SCHEMA_VERSION,
    RUN_CONTRACT_VERSION,
    attach_run_contract,
    build_run_verdict,
)


REPO_ROOT = Path(__file__).resolve().parents[1]


class HarnessContractTests(unittest.TestCase):
    def test_build_run_verdict_marks_completed_status(self) -> None:
        verdict = build_run_verdict(status="completed")

        self.assertEqual(verdict["outcome"], "completed")
        self.assertEqual(verdict["recommended_action"], "consume_outputs")
        self.assertFalse(verdict["retryable"])
        self.assertFalse(verdict["requires_manual_intervention"])

    def test_build_run_verdict_uses_structured_failure(self) -> None:
        verdict = build_run_verdict(
            status="failed",
            failure={
                "failure_layer": "runtime",
                "category": "external_runtime",
                "resolution_mode": "auto_retry",
                "retryable": True,
                "requires_manual_intervention": False,
                "recommended_action": "retry_run",
                "conclusion": "可以先直接重试。",
            },
        )

        self.assertEqual(verdict["outcome"], "failed")
        self.assertEqual(verdict["category"], "external_runtime")
        self.assertEqual(verdict["resolution_mode"], "auto_retry")
        self.assertTrue(verdict["retryable"])

    def test_attach_run_contract_adds_versions_and_verdict(self) -> None:
        summary = attach_run_contract({"status": "running"})

        self.assertEqual(summary["contract_version"], RUN_CONTRACT_VERSION)
        self.assertEqual(summary["failure_schema_version"], FAILURE_SCHEMA_VERSION)
        self.assertEqual(summary["verdict"]["outcome"], "running")

    def test_schema_drafts_define_expected_contract_shape(self) -> None:
        run_schema = json.loads((REPO_ROOT / "harness" / "schemas" / "run-summary.schema.json").read_text(encoding="utf-8"))
        failure_schema = json.loads((REPO_ROOT / "harness" / "schemas" / "failure.schema.json").read_text(encoding="utf-8"))

        self.assertEqual(run_schema["properties"]["contract_version"]["const"], RUN_CONTRACT_VERSION)
        self.assertEqual(run_schema["properties"]["failure_schema_version"]["const"], FAILURE_SCHEMA_VERSION)
        self.assertIn("verdict", run_schema["required"])
        self.assertIn("category", failure_schema["properties"])
        self.assertIn("resolution_mode", failure_schema["required"])
