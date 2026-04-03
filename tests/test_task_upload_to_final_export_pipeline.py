from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from harness.contract import FAILURE_SCHEMA_VERSION, RUN_CONTRACT_VERSION
import harness.paths as harness_paths
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
            env_path = temp_root / ".env"
            env_path.write_text("", encoding="utf-8")
            summary_path = temp_root / "run" / "summary.json"
            summary = final_runner.run_task_upload_to_final_export_pipeline(
                task_name="MINISO",
                env_file=str(env_path),
                output_root=temp_root / "run",
                summary_json=summary_path,
                task_upload_url="https://example.com/task",
                employee_info_url="https://example.com/employee",
                existing_mail_db_path="/tmp/shared/email_sync.db",
                existing_mail_raw_dir="/tmp/shared/raw",
                existing_mail_data_dir="/tmp/shared",
                feishu_app_id="app-id",
                feishu_app_secret="app-secret",
                owner_email_overrides={"MINISO": "eden@amagency.biz"},
                matching_strategy="brand-keyword-fast-path",
                brand_keyword="MINISO",
                brand_match_include_from=True,
                platform_filters=["instagram"],
                vision_provider="openai",
                max_identifiers_per_platform=1,
                creator_cache_db_path="/tmp/creator-cache.db",
                force_refresh_creator_cache=True,
            )
            persisted_summary = json.loads(summary_path.read_text(encoding="utf-8"))
            task_spec = json.loads(Path(summary["task_spec_json"]).read_text(encoding="utf-8"))
            workflow_handoff = json.loads(Path(summary["workflow_handoff_json"]).read_text(encoding="utf-8"))

        self.assertEqual(summary["status"], "completed")
        self.assertEqual(summary["contract_version"], RUN_CONTRACT_VERSION)
        self.assertEqual(summary["failure_schema_version"], FAILURE_SCHEMA_VERSION)
        self.assertEqual(summary["verdict"]["outcome"], "completed")
        self.assertEqual(summary["verdict"]["recommended_action"], "consume_outputs")
        self.assertTrue(summary["run_id"])
        self.assertEqual(summary["run_root"], str((temp_root / "run").resolve()))
        self.assertEqual(summary["env_file_raw"], str(env_path))
        self.assertEqual(summary["env_file"], str(env_path.resolve()))
        self.assertEqual(summary["env_file"], persisted_summary["resolved_inputs"]["env_file"]["path"])
        self.assertTrue(summary["workflow_handoff_json"].endswith("/workflow_handoff.json"))
        self.assertIn("resolved_config_sources", summary)
        self.assertEqual(summary["resolved_config_sources"]["matching_strategy"], "cli")
        self.assertEqual(summary["resolved_config_sources"]["brand_keyword"], "cli")
        self.assertEqual(summary["resolved_config_sources"]["vision_provider"], "cli")
        self.assertEqual(task_spec["scope"], "task-upload-to-final-export")
        self.assertEqual(task_spec["canonical_boundary"], "final-export")
        self.assertEqual(task_spec["intent"]["task_name"], "MINISO")
        self.assertEqual(task_spec["intent"]["task_upload_url"], "https://example.com/task")
        self.assertEqual(task_spec["controls"]["requested_platforms"], ["instagram"])
        self.assertTrue(task_spec["controls"]["force_refresh_creator_cache"])
        self.assertEqual(task_spec["paths"]["existing_mail_db_path"], "/tmp/shared/email_sync.db")
        self.assertEqual(task_spec["paths"]["existing_mail_raw_dir"], "/tmp/shared/raw")
        self.assertEqual(task_spec["paths"]["existing_mail_data_dir"], "/tmp/shared")
        self.assertEqual(task_spec["paths"]["creator_cache_db_path"], "/tmp/creator-cache.db")
        self.assertEqual(task_spec["run"]["workflow_handoff_json"], summary["workflow_handoff_json"])
        self.assertTrue(task_spec["paths"]["upstream_task_spec_json"].endswith("/upstream/task_spec.json"))
        self.assertTrue(task_spec["paths"]["upstream_workflow_handoff_json"].endswith("/upstream/workflow_handoff.json"))
        self.assertTrue(task_spec["paths"]["downstream_task_spec_json"].endswith("/downstream/task_spec.json"))
        self.assertTrue(task_spec["paths"]["downstream_workflow_handoff_json"].endswith("/downstream/workflow_handoff.json"))
        self.assertEqual(workflow_handoff["verdict"]["outcome"], "completed")
        self.assertEqual(workflow_handoff["recommended_action"], "consume_outputs")
        self.assertTrue(workflow_handoff["task_spec_available"])
        self.assertEqual(
            workflow_handoff["pointers"]["upstream_workflow_handoff_json"],
            summary["resolved_paths"]["upstream_workflow_handoff_json"],
        )
        self.assertEqual(
            workflow_handoff["pointers"]["downstream_workflow_handoff_json"],
            summary["resolved_paths"]["downstream_workflow_handoff_json"],
        )
        self.assertEqual(summary["steps"]["upstream"]["status"], "stopped_after_keep-list")
        self.assertEqual(summary["steps"]["downstream"]["status"], "completed")
        self.assertEqual(summary["contract"]["canonical_internal_boundary"], "keep-list")
        self.assertEqual(summary["inputs"]["existing_mail_db_path"], "/tmp/shared/email_sync.db")
        self.assertEqual(summary["inputs"]["existing_mail_raw_dir"], "/tmp/shared/raw")
        self.assertEqual(summary["inputs"]["existing_mail_data_dir"], "/tmp/shared")
        self.assertEqual(summary["inputs"]["creator_cache_db_path"], "/tmp/creator-cache.db")
        self.assertTrue(summary["inputs"]["force_refresh_creator_cache"])
        self.assertTrue(summary["artifacts"]["keep_workbook"].endswith("MINISO_final_keep.xlsx"))
        self.assertIn("instagram", summary["artifacts"]["final_exports"])
        self.assertTrue(summary["artifacts"]["all_platforms_final_review"].endswith("all_platforms_final_review.xlsx"))
        self.assertTrue(summary["artifacts"]["template_prepare_summary_json"].endswith("summary.json"))
        self.assertTrue(summary["artifacts"]["template_runtime_prompt_artifacts_json"].endswith("runtime_prompt_artifacts.json"))
        self.assertIn("--keep-workbook", summary["resume_points"]["keep_list"]["recommended_command"])
        self.assertIn("--platform instagram", summary["resume_points"]["keep_list"]["recommended_command"])
        self.assertIn("--vision-provider openai", summary["resume_points"]["keep_list"]["recommended_command"])
        self.assertEqual(observed["upstream_kwargs"]["stop_after"], "keep-list")
        self.assertEqual(observed["upstream_kwargs"]["existing_mail_db_path"], "/tmp/shared/email_sync.db")
        self.assertEqual(observed["upstream_kwargs"]["existing_mail_raw_dir"], "/tmp/shared/raw")
        self.assertEqual(observed["upstream_kwargs"]["existing_mail_data_dir"], "/tmp/shared")
        self.assertEqual(observed["upstream_kwargs"]["owner_email_overrides"], {"MINISO": "eden@amagency.biz"})
        self.assertEqual(observed["upstream_kwargs"]["matching_strategy"], "brand-keyword-fast-path")
        self.assertEqual(observed["downstream_kwargs"]["vision_provider"], "openai")
        self.assertEqual(observed["downstream_kwargs"]["platform_filters"], ["instagram"])
        self.assertEqual(observed["downstream_kwargs"]["max_identifiers_per_platform"], 1)
        self.assertEqual(observed["downstream_kwargs"]["creator_cache_db_path"], "/tmp/creator-cache.db")
        self.assertTrue(observed["downstream_kwargs"]["force_refresh_creator_cache"])
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
            env_path = temp_root / ".env"
            env_path.write_text("", encoding="utf-8")
            summary = final_runner.run_task_upload_to_final_export_pipeline(
                task_name="MINISO",
                env_file=str(env_path),
                output_root=temp_root / "run",
                task_upload_url="https://example.com/task",
                employee_info_url="https://example.com/employee",
                feishu_app_id="app-id",
                feishu_app_secret="app-secret",
            )

        self.assertEqual(summary["status"], "failed")
        self.assertEqual(summary["contract_version"], RUN_CONTRACT_VERSION)
        self.assertEqual(summary["verdict"]["outcome"], "failed")
        self.assertEqual(summary["error_code"], "KEEP_LIST_ARTIFACT_MISSING")
        self.assertEqual(summary["failure"]["stage"], "upstream")
        self.assertEqual(summary["failure_decision"]["category"], "input")
        self.assertTrue(summary["failure_decision"]["requires_manual_intervention"])
        self.assertFalse(summary["failure_decision"]["retryable"])

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
            env_path = temp_root / ".env"
            env_path.write_text("", encoding="utf-8")
            summary = final_runner.run_task_upload_to_final_export_pipeline(
                task_name="MINISO",
                env_file=str(env_path),
                output_root=temp_root / "run",
                task_upload_url="https://example.com/task",
                employee_info_url="https://example.com/employee",
                feishu_app_id="app-id",
                feishu_app_secret="app-secret",
                platform_filters=["instagram"],
            )

        self.assertEqual(summary["status"], "failed")
        self.assertEqual(summary["verdict"]["outcome"], "failed")
        self.assertEqual(summary["verdict"]["resolution_mode"], "auto_retry")
        self.assertEqual(summary["failure"]["stage"], "downstream")
        self.assertEqual(summary["error_code"], "DOWNSTREAM_SCRAPE_FAILED")
        self.assertEqual(summary["failure_decision"]["category"], "external_runtime")
        self.assertEqual(summary["failure_decision"]["resolution_mode"], "auto_retry")
        self.assertTrue(summary["failure_decision"]["retryable"])

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
            env_path = temp_root / ".env"
            env_path.write_text("", encoding="utf-8")
            summary = final_runner.run_task_upload_to_final_export_pipeline(
                task_name="MINISO",
                env_file=str(env_path),
                output_root=temp_root / "run",
                task_upload_url="https://example.com/task",
                employee_info_url="https://example.com/employee",
                feishu_app_id="app-id",
                feishu_app_secret="app-secret",
                platform_filters=["instagram", "tiktok"],
            )

        self.assertEqual(summary["status"], "completed_with_partial_scrape")
        self.assertEqual(summary["delivery_status"], "completed_with_partial_scrape")
        self.assertEqual(
            summary["steps"]["downstream"]["platform_statuses"],
            {"instagram": "completed_with_partial_scrape", "tiktok": "scrape_failed"},
        )

    def test_runner_accepts_staged_only_as_successful_terminal_status(self) -> None:
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
                "status": "staged_only",
                "platforms": {
                    "instagram": {
                        "status": "staged_only",
                    }
                },
                "artifacts": {},
            }

        final_runner._load_runtime_dependencies = lambda: {
            "run_task_upload_to_keep_list_pipeline": fake_upstream,
            "run_keep_list_screening_pipeline": fake_downstream,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            env_path = temp_root / ".env"
            env_path.write_text("", encoding="utf-8")
            summary = final_runner.run_task_upload_to_final_export_pipeline(
                task_name="MINISO",
                env_file=str(env_path),
                output_root=temp_root / "run",
                task_upload_url="https://example.com/task",
                employee_info_url="https://example.com/employee",
                feishu_app_id="app-id",
                feishu_app_secret="app-secret",
                platform_filters=["instagram"],
                skip_scrape=True,
            )

        self.assertEqual(summary["status"], "staged_only")
        self.assertEqual(summary["delivery_status"], "staged_only")
        self.assertEqual(summary["verdict"]["outcome"], "completed")
        self.assertNotIn("failure", summary)

    def test_runner_accepts_vision_probe_only_as_successful_terminal_status(self) -> None:
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
                "status": "vision_probe_only",
                "platforms": {},
                "artifacts": {},
                "vision_probe": {"success": True, "provider": "openai"},
            }

        final_runner._load_runtime_dependencies = lambda: {
            "run_task_upload_to_keep_list_pipeline": fake_upstream,
            "run_keep_list_screening_pipeline": fake_downstream,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            env_path = temp_root / ".env"
            env_path.write_text("", encoding="utf-8")
            summary = final_runner.run_task_upload_to_final_export_pipeline(
                task_name="MINISO",
                env_file=str(env_path),
                output_root=temp_root / "run",
                task_upload_url="https://example.com/task",
                employee_info_url="https://example.com/employee",
                feishu_app_id="app-id",
                feishu_app_secret="app-secret",
                probe_vision_provider_only=True,
            )

        self.assertEqual(summary["status"], "vision_probe_only")
        self.assertEqual(summary["delivery_status"], "vision_probe_only")
        self.assertEqual(summary["verdict"]["outcome"], "completed")
        self.assertEqual(summary["steps"]["downstream"]["vision_probe"]["provider"], "openai")
        self.assertNotIn("failure", summary)

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
            env_path = temp_root / ".env"
            env_path.write_text("", encoding="utf-8")
            summary = final_runner.run_task_upload_to_final_export_pipeline(
                task_name="MINISO",
                env_file=str(env_path),
                output_root=temp_root / "run",
                task_upload_url="https://example.com/task",
                employee_info_url="https://example.com/employee",
                feishu_app_id="app-id",
                feishu_app_secret="app-secret",
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

    def test_runner_uses_run_scoped_default_root_and_redacts_sensitive_config_sources(self) -> None:
        observed: dict[str, object] = {}

        def fake_upstream(**kwargs):
            observed["upstream_output_root"] = kwargs["output_root"]
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
            observed["downstream_output_root"] = kwargs["output_root"]
            return {"status": "completed", "platforms": {}, "artifacts": {}}

        final_runner._load_runtime_dependencies = lambda: {
            "run_task_upload_to_keep_list_pipeline": fake_upstream,
            "run_keep_list_screening_pipeline": fake_downstream,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            workspace_root = Path(temp_dir)
            env_path = workspace_root / ".env"
            env_path.write_text(
                "\n".join(
                    [
                        "TASK_UPLOAD_URL=https://env.example/task",
                        "EMPLOYEE_INFO_URL=https://env.example/employee",
                        "FEISHU_APP_ID=app-id-secret",
                        "FEISHU_APP_SECRET=app-secret-secret",
                        "",
                    ]
                ),
                encoding="utf-8",
            )
            original_repo_root = harness_paths.REPO_ROOT
            try:
                harness_paths.REPO_ROOT = workspace_root
                summary = final_runner.run_task_upload_to_final_export_pipeline(
                    task_name="MINISO",
                    env_file=str(env_path),
                )
            finally:
                harness_paths.REPO_ROOT = original_repo_root

        self.assertEqual(summary["status"], "completed")
        self.assertIn("/temp/runs/task_upload_to_final_export/", summary["run_root"])
        self.assertEqual(summary["output_root"], summary["run_root"])
        self.assertTrue(str(observed["upstream_output_root"]).startswith(summary["run_root"]))
        self.assertTrue(str(observed["downstream_output_root"]).startswith(summary["run_root"]))
        self.assertTrue(summary["task_spec_json"].startswith(summary["run_root"]))
        self.assertEqual(summary["env_file_raw"], str(env_path))
        self.assertEqual(summary["env_file"], str(env_path.resolve()))
        self.assertEqual(summary["env_file"], summary["resolved_inputs"]["env_file"]["path"])
        self.assertEqual(summary["resolved_config_sources"]["task_upload_url"], "env_file:TASK_UPLOAD_URL")
        self.assertEqual(summary["resolved_config_sources"]["employee_info_url"], "env_file:EMPLOYEE_INFO_URL")
        self.assertEqual(
            summary["resolved_config_sources"]["feishu_app_id"],
            {"present": True, "source": "env_file:FEISHU_APP_ID"},
        )
        self.assertEqual(
            summary["resolved_config_sources"]["feishu_app_secret"],
            {"present": True, "source": "env_file:FEISHU_APP_SECRET"},
        )
        self.assertNotIn("app-id-secret", json.dumps(summary["resolved_config_sources"], ensure_ascii=False))
        self.assertNotIn("app-secret-secret", json.dumps(summary["resolved_config_sources"], ensure_ascii=False))
        self.assertTrue(summary["resolved_paths"]["upstream_task_spec_json"].endswith("/upstream/task_spec.json"))
        self.assertTrue(summary["resolved_paths"]["downstream_task_spec_json"].endswith("/downstream/task_spec.json"))

    def test_runner_allows_missing_env_file_when_required_config_is_provided_explicitly(self) -> None:
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
            return {"status": "completed", "platforms": {}, "artifacts": {}}

        final_runner._load_runtime_dependencies = lambda: {
            "run_task_upload_to_keep_list_pipeline": fake_upstream,
            "run_keep_list_screening_pipeline": fake_downstream,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            missing_env = temp_root / "missing.env"
            summary = final_runner.run_task_upload_to_final_export_pipeline(
                task_name="MINISO",
                env_file=str(missing_env),
                output_root=temp_root / "run",
                task_upload_url="https://example.com/task",
                employee_info_url="https://example.com/employee",
                feishu_app_id="app-id",
                feishu_app_secret="app-secret",
            )

        self.assertEqual(summary["status"], "completed")
        self.assertEqual(summary["contract_version"], RUN_CONTRACT_VERSION)
        self.assertEqual(summary["verdict"]["outcome"], "completed")
        self.assertTrue(summary["preflight"]["ready"])
        self.assertFalse(summary["preflight"]["env_file_exists"])
        self.assertEqual(summary["env_file"], str(missing_env.resolve()))
        self.assertEqual(summary["env_file_raw"], str(missing_env))

    def test_runner_fails_preflight_when_required_config_is_missing(self) -> None:
        final_runner._load_runtime_dependencies = lambda: (_ for _ in ()).throw(
            AssertionError("runtime should not load when preflight fails")
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            env_path = temp_root / ".env"
            env_path.write_text("TASK_UPLOAD_URL=https://env.example/task\n", encoding="utf-8")
            summary = final_runner.run_task_upload_to_final_export_pipeline(
                task_name="MINISO",
                env_file=str(env_path),
                output_root=temp_root / "run",
            )
            self.assertTrue(Path(summary["workflow_handoff_json"]).exists())
            workflow_handoff = json.loads(Path(summary["workflow_handoff_json"]).read_text(encoding="utf-8"))

        self.assertEqual(summary["status"], "failed")
        self.assertEqual(summary["contract_version"], RUN_CONTRACT_VERSION)
        self.assertEqual(summary["verdict"]["outcome"], "failed")
        self.assertEqual(summary["failure"]["stage"], "preflight")
        self.assertEqual(summary["failure_layer"], "preflight")
        self.assertEqual(summary["failure_decision"]["category"], "configuration")
        self.assertEqual(summary["failure_decision"]["resolution_mode"], "manual_fix")
        self.assertTrue(summary["failure_decision"]["requires_manual_intervention"])
        self.assertFalse(summary["failure_decision"]["retryable"])
        self.assertFalse(summary["preflight"]["ready"])
        self.assertTrue(summary["setup"]["skipped"])
        self.assertFalse(summary["setup"]["completed"])
        self.assertEqual(summary["preflight"]["env_file_exists"], True)
        self.assertEqual(summary["preflight"]["task_upload_url_present"], True)
        self.assertEqual(summary["preflight"]["employee_info_url_present"], False)
        self.assertEqual(summary["preflight"]["feishu_app_id_present"], False)
        self.assertEqual(summary["preflight"]["feishu_app_secret_present"], False)
        self.assertEqual(summary["preflight"]["errors"][0]["error_code"], "EMPLOYEE_INFO_URL_MISSING")
        self.assertFalse(Path(summary["task_spec_json"]).exists())
        self.assertFalse(workflow_handoff["task_spec_available"])
        self.assertEqual(workflow_handoff["failure"]["error_code"], "EMPLOYEE_INFO_URL_MISSING")
        self.assertEqual(workflow_handoff["failure"]["failure_layer"], "preflight")
        self.assertEqual(workflow_handoff["failure_decision"]["category"], "configuration")

    def test_runner_serializes_task_group_alias_into_child_runs(self) -> None:
        observed_upstream_tasks: list[str] = []
        observed_downstream_tasks: list[str] = []

        class FakeFeishuClient:
            def __init__(self, **_: object) -> None:
                pass

        def fake_resolve_task_upload_entries(**kwargs):
            self.assertEqual(kwargs["task_name"], "duet")
            return [
                SimpleNamespace(task_name="Duet1"),
                SimpleNamespace(task_name="Duet2"),
            ]

        def fake_upstream(**kwargs):
            task_name = kwargs["task_name"]
            observed_upstream_tasks.append(task_name)
            keep_path = Path(kwargs["output_root"]) / "exports" / f"{task_name}_keep.xlsx"
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
            task_name = kwargs["task_name"]
            observed_downstream_tasks.append(task_name)
            export_path = Path(kwargs["output_root"]) / "exports" / "instagram" / f"{task_name}_final_review.xlsx"
            export_path.parent.mkdir(parents=True, exist_ok=True)
            export_path.touch()
            return {
                "status": "completed",
                "platforms": {
                    "instagram": {
                        "status": "completed",
                        "exports": {"final_review": str(export_path)},
                    }
                },
                "artifacts": {},
            }

        final_runner._load_runtime_dependencies = lambda: {
            "DEFAULT_FEISHU_BASE_URL": "https://open.feishu.cn/open-apis",
            "FeishuOpenClient": FakeFeishuClient,
            "resolve_task_upload_entries": fake_resolve_task_upload_entries,
            "run_task_upload_to_keep_list_pipeline": fake_upstream,
            "run_keep_list_screening_pipeline": fake_downstream,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            env_path = temp_root / ".env"
            env_path.write_text("", encoding="utf-8")
            summary = final_runner.run_task_upload_to_final_export_pipeline(
                task_name="duet",
                env_file=str(env_path),
                output_root=temp_root / "run",
                task_upload_url="https://example.com/task",
                employee_info_url="https://example.com/employee",
                feishu_app_id="app-id",
                feishu_app_secret="app-secret",
                platform_filters=["instagram"],
            )

        self.assertEqual(summary["status"], "completed")
        self.assertEqual(summary["delivery_status"], "completed")
        self.assertEqual(observed_upstream_tasks, ["Duet1", "Duet2"])
        self.assertEqual(observed_downstream_tasks, ["Duet1", "Duet2"])
        self.assertEqual(summary["steps"]["fan_out"]["mode"], "serial")
        self.assertEqual(summary["steps"]["fan_out"]["requested_task_name"], "duet")
        self.assertEqual(summary["steps"]["fan_out"]["resolved_task_names"], ["Duet1", "Duet2"])
        self.assertEqual(len(summary["steps"]["fan_out"]["children"]), 2)
        self.assertEqual(
            sorted(summary["artifacts"]["final_exports_by_task"].keys()),
            ["Duet1", "Duet2"],
        )
        self.assertTrue(
            summary["artifacts"]["final_exports_by_task"]["Duet1"]["instagram"]["final_review"].endswith(
                "Duet1_final_review.xlsx"
            )
        )
        self.assertTrue(
            summary["artifacts"]["final_exports_by_task"]["Duet2"]["instagram"]["final_review"].endswith(
                "Duet2_final_review.xlsx"
            )
        )

    def test_parser_accepts_existing_mail_sidecar_args(self) -> None:
        parser = final_runner.build_parser()
        args = parser.parse_args(
            [
                "--task-name",
                "MINISO",
                "--existing-mail-db-path",
                "/tmp/shared/email_sync.db",
                "--existing-mail-raw-dir",
                "/tmp/shared/raw",
                "--existing-mail-data-dir",
                "/tmp/shared",
            ]
        )

        self.assertEqual(args.existing_mail_db_path, "/tmp/shared/email_sync.db")
        self.assertEqual(args.existing_mail_raw_dir, "/tmp/shared/raw")
        self.assertEqual(args.existing_mail_data_dir, "/tmp/shared")


if __name__ == "__main__":
    unittest.main()
