from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import scripts.run_task_upload_to_final_export_pipeline as final_runner


class TaskUploadToFinalExportRunnerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.original_loader = final_runner._load_runtime_dependencies

    def tearDown(self) -> None:
        final_runner._load_runtime_dependencies = self.original_loader

    def test_runner_links_upstream_and_downstream_with_keep_list_resume_point(self) -> None:
        observed: dict[str, object] = {}

        def fake_upstream(**kwargs):
            observed["upstream_kwargs"] = kwargs
            keep_path = Path(kwargs["output_root"]) / "exports" / "MINISO_final_keep.xlsx"
            template_path = Path(kwargs["output_root"]) / "downloads" / "template.xlsx"
            prompt_summary_path = Path(kwargs["output_root"]) / "task_assets_prompt_artifacts" / "summary.json"
            prompt_artifacts_path = Path(kwargs["output_root"]) / "task_assets_prompt_artifacts" / "runtime_prompt_artifacts.json"
            keep_path.parent.mkdir(parents=True, exist_ok=True)
            keep_path.touch()
            template_path.parent.mkdir(parents=True, exist_ok=True)
            template_path.touch()
            prompt_summary_path.parent.mkdir(parents=True, exist_ok=True)
            prompt_summary_path.touch()
            prompt_artifacts_path.touch()
            return {
                "status": "stopped_after_keep-list",
                "contract": {"canonical_boundary": "keep-list"},
                "resume_points": {
                    "keep_list": {
                        "keep_workbook": str(keep_path),
                        "template_workbook": str(template_path),
                    }
                },
                "artifacts": {
                    "keep_workbook": str(keep_path),
                    "template_workbook": str(template_path),
                    "template_prepare_summary_json": str(prompt_summary_path),
                    "template_runtime_prompt_artifacts_json": str(prompt_artifacts_path),
                },
                "downstream_handoff": {"runner_script": "scripts/run_keep_list_screening_pipeline.py"},
            }

        def fake_downstream(**kwargs):
            observed["downstream_kwargs"] = kwargs
            export_path = Path(kwargs["output_root"]) / "exports" / "instagram" / "instagram_final_review.xlsx"
            combined_path = Path(kwargs["output_root"]) / "exports" / "all_platforms_final_review.xlsx"
            payload_path = Path(kwargs["output_root"]) / "exports" / "all_platforms_final_review_payload.json"
            export_path.parent.mkdir(parents=True, exist_ok=True)
            export_path.touch()
            combined_path.touch()
            payload_path.touch()
            return {
                "status": "completed",
                "artifacts": {
                    "all_platforms_final_review": str(combined_path),
                    "all_platforms_upload_payload_json": str(payload_path),
                },
                "platforms": {
                    "instagram": {
                        "status": "completed",
                        "exports": {"final_review": str(export_path)},
                    }
                },
                "vision_probe": {"success": True, "provider": "openai"},
            }

        final_runner._load_runtime_dependencies = lambda: {
            "run_task_upload_to_keep_list_pipeline": fake_upstream,
            "run_keep_list_screening_pipeline": fake_downstream,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            summary_path = temp_root / "run" / "summary.json"
            summary = final_runner.run_task_upload_to_final_export_pipeline(
                task_name="MINISO",
                env_file=".env",
                output_root=temp_root / "run",
                summary_json=summary_path,
                owner_email_overrides={"MINISO": "eden@amagency.biz"},
                matching_strategy="brand-keyword-fast-path",
                brand_keyword="MINISO",
                brand_match_include_from=True,
                platform_filters=["instagram"],
                vision_provider="openai",
                max_identifiers_per_platform=1,
            )
            persisted_summary = json.loads(summary_path.read_text(encoding="utf-8"))

        self.assertEqual(summary["status"], "completed")
        self.assertEqual(summary["steps"]["upstream"]["status"], "stopped_after_keep-list")
        self.assertEqual(summary["steps"]["downstream"]["status"], "completed")
        self.assertEqual(summary["contract"]["canonical_internal_boundary"], "keep-list")
        self.assertTrue(summary["artifacts"]["keep_workbook"].endswith("MINISO_final_keep.xlsx"))
        self.assertIn("instagram", summary["artifacts"]["final_exports"])
        self.assertTrue(summary["artifacts"]["all_platforms_final_review"].endswith("all_platforms_final_review.xlsx"))
        self.assertTrue(summary["artifacts"]["template_prepare_summary_json"].endswith("summary.json"))
        self.assertTrue(summary["artifacts"]["template_runtime_prompt_artifacts_json"].endswith("runtime_prompt_artifacts.json"))
        self.assertIn("--keep-workbook", summary["resume_points"]["keep_list"]["recommended_command"])
        self.assertIn("--platform instagram", summary["resume_points"]["keep_list"]["recommended_command"])
        self.assertIn("--vision-provider openai", summary["resume_points"]["keep_list"]["recommended_command"])
        self.assertEqual(observed["upstream_kwargs"]["stop_after"], "keep-list")
        self.assertEqual(observed["upstream_kwargs"]["owner_email_overrides"], {"MINISO": "eden@amagency.biz"})
        self.assertEqual(observed["upstream_kwargs"]["matching_strategy"], "brand-keyword-fast-path")
        self.assertEqual(observed["downstream_kwargs"]["vision_provider"], "openai")
        self.assertEqual(observed["downstream_kwargs"]["platform_filters"], ["instagram"])
        self.assertEqual(observed["downstream_kwargs"]["max_identifiers_per_platform"], 1)
        self.assertEqual(observed["downstream_kwargs"]["task_owner_name"], "")
        self.assertEqual(
            persisted_summary["steps"]["downstream"]["final_exports"]["instagram"]["final_review"],
            summary["artifacts"]["final_exports"]["instagram"]["final_review"],
        )
        self.assertEqual(
            persisted_summary["artifacts"]["all_platforms_final_review"],
            summary["artifacts"]["all_platforms_final_review"],
        )

    def test_runner_fails_when_upstream_keep_workbook_is_missing(self) -> None:
        def fake_upstream(**kwargs):
            return {
                "status": "stopped_after_keep-list",
                "contract": {"canonical_boundary": "keep-list"},
                "resume_points": {
                    "keep_list": {
                        "keep_workbook": str(Path(kwargs["output_root"]) / "exports" / "missing_keep.xlsx"),
                        "template_workbook": "",
                    }
                },
                "artifacts": {},
            }

        def fake_downstream(**kwargs):
            raise AssertionError("downstream should not run when keep workbook is missing")

        final_runner._load_runtime_dependencies = lambda: {
            "run_task_upload_to_keep_list_pipeline": fake_upstream,
            "run_keep_list_screening_pipeline": fake_downstream,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            summary = final_runner.run_task_upload_to_final_export_pipeline(
                task_name="MINISO",
                output_root=temp_root / "run",
            )

        self.assertEqual(summary["status"], "failed")
        self.assertEqual(summary["error_code"], "KEEP_LIST_ARTIFACT_MISSING")
        self.assertEqual(summary["failure"]["stage"], "upstream")

    def test_runner_fails_when_downstream_status_is_not_completed(self) -> None:
        def fake_upstream(**kwargs):
            keep_path = Path(kwargs["output_root"]) / "exports" / "MINISO_final_keep.xlsx"
            template_path = Path(kwargs["output_root"]) / "downloads" / "template.xlsx"
            keep_path.parent.mkdir(parents=True, exist_ok=True)
            keep_path.touch()
            template_path.parent.mkdir(parents=True, exist_ok=True)
            template_path.touch()
            return {
                "status": "stopped_after_keep-list",
                "contract": {"canonical_boundary": "keep-list"},
                "resume_points": {
                    "keep_list": {
                        "keep_workbook": str(keep_path),
                        "template_workbook": str(template_path),
                    }
                },
                "artifacts": {
                    "keep_workbook": str(keep_path),
                    "template_workbook": str(template_path),
                },
            }

        def fake_downstream(**kwargs):
            return {
                "status": "scrape_failed",
                "platforms": {
                    "instagram": {
                        "status": "scrape_failed",
                    }
                },
            }

        final_runner._load_runtime_dependencies = lambda: {
            "run_task_upload_to_keep_list_pipeline": fake_upstream,
            "run_keep_list_screening_pipeline": fake_downstream,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            summary = final_runner.run_task_upload_to_final_export_pipeline(
                task_name="MINISO",
                output_root=temp_root / "run",
                platform_filters=["instagram"],
            )

        self.assertEqual(summary["status"], "failed")
        self.assertEqual(summary["failure"]["stage"], "downstream")
        self.assertEqual(summary["error_code"], "DOWNSTREAM_SCRAPE_FAILED")

    def test_runner_accepts_completed_with_partial_scrape_and_preserves_platform_statuses(self) -> None:
        def fake_upstream(**kwargs):
            keep_path = Path(kwargs["output_root"]) / "exports" / "MINISO_final_keep.xlsx"
            template_path = Path(kwargs["output_root"]) / "downloads" / "template.xlsx"
            keep_path.parent.mkdir(parents=True, exist_ok=True)
            keep_path.touch()
            template_path.parent.mkdir(parents=True, exist_ok=True)
            template_path.touch()
            return {
                "status": "stopped_after_keep-list",
                "contract": {"canonical_boundary": "keep-list"},
                "resume_points": {
                    "keep_list": {
                        "keep_workbook": str(keep_path),
                        "template_workbook": str(template_path),
                    }
                },
                "artifacts": {
                    "keep_workbook": str(keep_path),
                    "template_workbook": str(template_path),
                },
            }

        def fake_downstream(**kwargs):
            export_path = Path(kwargs["output_root"]) / "exports" / "instagram" / "instagram_final_review.xlsx"
            export_path.parent.mkdir(parents=True, exist_ok=True)
            export_path.touch()
            return {
                "status": "completed_with_partial_scrape",
                "platforms": {
                    "instagram": {
                        "status": "completed_with_partial_scrape",
                        "exports": {"final_review": str(export_path)},
                    },
                    "tiktok": {
                        "status": "scrape_failed",
                    },
                },
            }

        final_runner._load_runtime_dependencies = lambda: {
            "run_task_upload_to_keep_list_pipeline": fake_upstream,
            "run_keep_list_screening_pipeline": fake_downstream,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            summary = final_runner.run_task_upload_to_final_export_pipeline(
                task_name="MINISO",
                output_root=temp_root / "run",
                platform_filters=["instagram", "tiktok"],
            )

        self.assertEqual(summary["status"], "completed_with_partial_scrape")
        self.assertEqual(summary["delivery_status"], "completed_with_partial_scrape")
        self.assertEqual(
            summary["steps"]["downstream"]["platform_statuses"],
            {"instagram": "completed_with_partial_scrape", "tiktok": "scrape_failed"},
        )

    def test_runner_surfaces_positioning_artifacts_and_stage_summaries_without_blocking_delivery(self) -> None:
        def fake_upstream(**kwargs):
            keep_path = Path(kwargs["output_root"]) / "exports" / "MINISO_final_keep.xlsx"
            template_path = Path(kwargs["output_root"]) / "downloads" / "template.xlsx"
            keep_path.parent.mkdir(parents=True, exist_ok=True)
            keep_path.touch()
            template_path.parent.mkdir(parents=True, exist_ok=True)
            template_path.touch()
            return {
                "status": "stopped_after_keep-list",
                "contract": {"canonical_boundary": "keep-list"},
                "resume_points": {
                    "keep_list": {
                        "keep_workbook": str(keep_path),
                        "template_workbook": str(template_path),
                    }
                },
                "artifacts": {
                    "keep_workbook": str(keep_path),
                    "template_workbook": str(template_path),
                },
            }

        def fake_downstream(**kwargs):
            final_export = Path(kwargs["output_root"]) / "exports" / "instagram" / "instagram_final_review.xlsx"
            positioning_export = Path(kwargs["output_root"]) / "exports" / "instagram" / "instagram_positioning_card_review.xlsx"
            positioning_json = Path(kwargs["output_root"]) / "exports" / "instagram" / "instagram_positioning_card_results.json"
            combined_path = Path(kwargs["output_root"]) / "exports" / "all_platforms_final_review.xlsx"
            payload_path = Path(kwargs["output_root"]) / "exports" / "all_platforms_final_review_payload.json"
            for path in (final_export, positioning_export, positioning_json, combined_path, payload_path):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.touch()
            return {
                "status": "completed",
                "artifacts": {
                    "all_platforms_final_review": str(combined_path),
                    "all_platforms_upload_payload_json": str(payload_path),
                },
                "platforms": {
                    "instagram": {
                        "status": "completed",
                        "exports": {
                            "final_review": str(final_export),
                            "positioning_card_review": str(positioning_export),
                            "positioning_card_json": str(positioning_json),
                        },
                        "positioning_card_analysis": {
                            "status": "failed",
                            "reason": "provider timeout",
                            "non_blocking": True,
                        },
                    }
                },
            }

        final_runner._load_runtime_dependencies = lambda: {
            "run_task_upload_to_keep_list_pipeline": fake_upstream,
            "run_keep_list_screening_pipeline": fake_downstream,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            summary = final_runner.run_task_upload_to_final_export_pipeline(
                task_name="MINISO",
                output_root=temp_root / "run",
                platform_filters=["instagram"],
            )

        self.assertEqual(summary["status"], "completed")
        self.assertIn("instagram", summary["steps"]["downstream"]["positioning_artifacts"])
        self.assertEqual(
            summary["steps"]["downstream"]["positioning_card_analysis"]["instagram"]["status"],
            "failed",
        )
        self.assertTrue(
            summary["steps"]["downstream"]["positioning_card_analysis"]["instagram"]["non_blocking"]
        )
        self.assertIn("positioning_card_review", summary["artifacts"]["positioning_artifacts"]["instagram"])
        self.assertTrue(summary["artifacts"]["all_platforms_upload_payload_json"].endswith(".json"))


if __name__ == "__main__":
    unittest.main()
