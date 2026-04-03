from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from harness.contract import RUN_CONTRACT_VERSION
import scripts.run_task_upload_to_keep_list_pipeline as task_runner


REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_TEMPLATE = REPO_ROOT / "tests" / "fixtures" / "template_parser" / "11.xlsx"


class TaskUploadToKeepListPipelineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.original_loader = task_runner._load_runtime_dependencies

    def tearDown(self) -> None:
        task_runner._load_runtime_dependencies = self.original_loader

    def _write_env_file(self, root: Path, **overrides: str) -> Path:
        values = {
            "FEISHU_APP_ID": "app-id",
            "FEISHU_APP_SECRET": "app-secret",
            "TASK_UPLOAD_URL": "https://env.example/task",
            "EMPLOYEE_INFO_URL": "https://env.example/employee",
            "TIMEOUT_SECONDS": "30",
        }
        values.update({key: value for key, value in overrides.items() if value is not None})
        env_path = root / ".env"
        env_path.write_text(
            "\n".join(f"{key}={value}" for key, value in values.items()) + "\n",
            encoding="utf-8",
        )
        return env_path

    def test_parser_keeps_single_entry_options(self) -> None:
        parser = task_runner.build_parser()
        args = parser.parse_args(
            [
                "--task-name",
                "MINISO",
                "--task-upload-url",
                "https://example.com/task",
                "--employee-info-url",
                "https://example.com/employee",
                "--output-root",
                "temp/e2e",
                "--owner-email-override",
                "MINISO:eden@amagency.biz",
                "--mail-limit",
                "5",
                "--mail-workers",
                "2",
                "--stop-after",
                "keep-list",
                "--no-reuse-existing",
            ]
        )
        self.assertEqual(args.task_name, "MINISO")
        self.assertEqual(args.task_upload_url, "https://example.com/task")
        self.assertEqual(args.employee_info_url, "https://example.com/employee")
        self.assertEqual(args.output_root, "temp/e2e")
        self.assertEqual(args.owner_email_override, ["MINISO:eden@amagency.biz"])
        self.assertEqual(args.mail_limit, 5)
        self.assertEqual(args.mail_workers, 2)
        self.assertEqual(args.stop_after, "keep-list")
        self.assertTrue(args.no_reuse_existing)

    def test_parser_accepts_fast_path_options(self) -> None:
        parser = task_runner.build_parser()
        args = parser.parse_args(
            [
                "--task-name",
                "MINISO",
                "--matching-strategy",
                "brand-keyword-fast-path",
                "--brand-keyword",
                "MINISO",
                "--brand-match-include-from",
                "--stop-after",
                "shared-resolution",
            ]
        )
        self.assertEqual(args.matching_strategy, "brand-keyword-fast-path")
        self.assertEqual(args.brand_keyword, "MINISO")
        self.assertTrue(args.brand_match_include_from)
        self.assertEqual(args.stop_after, "shared-resolution")

    def test_parser_defaults_to_brand_keyword_fast_path(self) -> None:
        parser = task_runner.build_parser()
        args = parser.parse_args(["--task-name", "MINISO"])
        self.assertEqual(args.matching_strategy, "brand-keyword-fast-path")

    def test_parser_accepts_existing_mail_db_options(self) -> None:
        parser = task_runner.build_parser()
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

    def test_runner_records_structured_preflight_failure_when_feishu_credentials_missing(self) -> None:
        task_runner._load_runtime_dependencies = lambda: {
            "Settings": object,
            "Database": object,
            "FeishuOpenClient": lambda **kwargs: object(),
            "DEFAULT_FEISHU_BASE_URL": "https://open.feishu.cn",
            "download_task_upload_screening_assets": lambda **kwargs: {},
            "sync_task_upload_mailboxes": lambda **kwargs: {},
            "match_brand_keyword": lambda **kwargs: {},
            "resolve_shared_email_candidates": lambda **kwargs: {},
            "run_shared_email_final_review": lambda **kwargs: {},
            "enrich_creator_workbook": lambda **kwargs: {},
            "prepare_llm_review_candidates": lambda **kwargs: {},
            "run_and_apply_llm_review": lambda **kwargs: {},
            "resolve_sync_sent_since": lambda value: __import__("datetime").date(2025, 12, 27),
            "load_local_env": lambda env_file: {},
            "get_preferred_value": lambda cli_value, env_values, env_key, default="": str(cli_value or "").strip() or str(env_values.get(env_key, default) or "").strip(),
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            summary_path = temp_root / "run" / "summary.json"
            env_path = self._write_env_file(
                temp_root,
                FEISHU_APP_ID="",
                FEISHU_APP_SECRET="",
            )
            summary = task_runner.run_task_upload_to_keep_list_pipeline(
                task_name="MINISO",
                env_file=env_path,
                output_root=temp_root / "run",
                summary_json=summary_path,
            )
            persisted = json.loads(summary_path.read_text(encoding="utf-8"))
            self.assertTrue(Path(summary["workflow_handoff_json"]).exists())
            workflow_handoff = json.loads(Path(summary["workflow_handoff_json"]).read_text(encoding="utf-8"))

        self.assertEqual(summary["status"], "failed")
        self.assertEqual(summary["contract_version"], RUN_CONTRACT_VERSION)
        self.assertEqual(summary["verdict"]["outcome"], "failed")
        self.assertEqual(summary["error_code"], "FEISHU_APP_ID_MISSING")
        self.assertEqual(summary["failure"]["stage"], "preflight")
        self.assertEqual(summary["failure_layer"], "preflight")
        self.assertEqual(summary["failure_decision"]["category"], "configuration")
        self.assertEqual(summary["failure_decision"]["resolution_mode"], "manual_fix")
        self.assertTrue(summary["failure_decision"]["requires_manual_intervention"])
        self.assertFalse(summary["failure_decision"]["retryable"])
        self.assertFalse(summary["preflight"]["ready"])
        self.assertTrue(summary["setup"]["skipped"])
        self.assertFalse(summary["setup"]["completed"])
        self.assertEqual(summary["preflight"]["errors"][0]["error_code"], "FEISHU_APP_ID_MISSING")
        self.assertFalse(Path(summary["task_spec_json"]).exists())
        self.assertFalse(workflow_handoff["task_spec_available"])
        self.assertEqual(workflow_handoff["failure"]["failure_layer"], "preflight")
        self.assertEqual(workflow_handoff["failure_decision"]["category"], "configuration")
        self.assertEqual(persisted["failure"]["error_code"], "FEISHU_APP_ID_MISSING")

    def test_runner_defaults_to_run_local_paths_when_output_root_is_omitted(self) -> None:
        task_runner._load_runtime_dependencies = lambda: (_ for _ in ()).throw(AssertionError("runtime should not load"))

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            env_path = self._write_env_file(
                temp_root,
                FEISHU_APP_ID="",
                FEISHU_APP_SECRET="",
            )
            summary = task_runner.run_task_upload_to_keep_list_pipeline(
                task_name="MINISO",
                env_file=env_path,
            )

        self.assertEqual(summary["status"], "failed")
        self.assertIn("/temp/runs/task_upload_to_keep_list/", summary["run_root"])
        self.assertEqual(summary["output_root"], summary["run_root"])
        self.assertTrue(summary["resolved_inputs"]["paths"]["downloads_dir"]["path"].startswith(summary["run_root"]))
        self.assertTrue(summary["resolved_inputs"]["paths"]["mail_root"]["path"].startswith(summary["run_root"]))
        self.assertEqual(summary["resolved_config_sources"]["env_file"], "cli")

    def test_runner_allows_missing_env_file_when_required_config_is_provided_explicitly(self) -> None:
        def fake_download_task_upload_screening_assets(**kwargs):
            download_dir = Path(kwargs["download_dir"])
            template_path = download_dir / "miniso_template.xlsx"
            sending_list_path = download_dir / "miniso_sending_list.xlsx"
            template_path.parent.mkdir(parents=True, exist_ok=True)
            template_path.touch()
            sending_list_path.touch()
            return {
                "recordId": "rec123",
                "taskName": "MINISO",
                "linkedBitableUrl": "https://bitable.example/miniso",
                "templateDownloadedPath": str(template_path),
                "sendingListDownloadedPath": str(sending_list_path),
            }

        task_runner._load_runtime_dependencies = lambda: {
            "Settings": object,
            "Database": object,
            "FeishuOpenClient": lambda **kwargs: object(),
            "DEFAULT_FEISHU_BASE_URL": "https://open.feishu.cn",
            "download_task_upload_screening_assets": fake_download_task_upload_screening_assets,
            "sync_task_upload_mailboxes": lambda **kwargs: {},
            "match_brand_keyword": lambda **kwargs: {},
            "resolve_shared_email_candidates": lambda **kwargs: {},
            "run_shared_email_final_review": lambda **kwargs: {},
            "enrich_creator_workbook": lambda **kwargs: {},
            "prepare_llm_review_candidates": lambda **kwargs: {},
            "run_and_apply_llm_review": lambda **kwargs: {},
            "resolve_sync_sent_since": lambda value: __import__("datetime").date(2025, 12, 27),
            "load_local_env": lambda env_file: {},
            "get_preferred_value": lambda cli_value, env_values, env_key, default="": str(cli_value or "").strip() or str(env_values.get(env_key, default) or "").strip(),
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            missing_env = temp_root / "missing.env"
            summary = task_runner.run_task_upload_to_keep_list_pipeline(
                task_name="MINISO",
                env_file=missing_env,
                output_root=temp_root / "run",
                stop_after="task-assets",
                task_upload_url="https://example.com/task",
                employee_info_url="https://example.com/employee",
                feishu_app_id="app-id",
                feishu_app_secret="app-secret",
            )

        self.assertEqual(summary["status"], "stopped_after_task-assets")
        self.assertEqual(summary["contract_version"], RUN_CONTRACT_VERSION)
        self.assertEqual(summary["verdict"]["outcome"], "stopped")
        self.assertEqual(summary["verdict"]["recommended_action"], "resume_run")
        self.assertTrue(summary["preflight"]["ready"])
        self.assertFalse(summary["preflight"]["env_file_exists"])
        self.assertEqual(summary["env_file"], str(missing_env.resolve()))
        self.assertEqual(summary["resolved_config_sources"]["task_upload_url"], "cli")

    def test_runner_records_single_entry_summary_and_handoff(self) -> None:
        class FakeClient:
            pass

        class FakeDb:
            def __init__(self, db_path):
                self.db_path = Path(db_path)

            def close(self):
                return None

        def fake_load_local_env(env_file):
            return {
                "FEISHU_APP_ID": "app-id",
                "FEISHU_APP_SECRET": "app-secret",
                "TASK_UPLOAD_URL": "https://env.example/task",
                "EMPLOYEE_INFO_URL": "https://env.example/employee",
                "TIMEOUT_SECONDS": "30",
            }

        def fake_get_preferred_value(cli_value, env_values, env_key, default=""):
            return str(cli_value or "").strip() or str(env_values.get(env_key, default) or "").strip()

        def fake_download_task_upload_screening_assets(**kwargs):
            download_dir = Path(kwargs["download_dir"])
            template_path = download_dir / "miniso_template.xlsx"
            sending_list_path = download_dir / "miniso_sending_list.xlsx"
            template_path.parent.mkdir(parents=True, exist_ok=True)
            template_path.touch()
            sending_list_path.touch()
            return {
                "recordId": "rec123",
                "taskName": "MINISO",
                "linkedBitableUrl": "https://bitable.example/miniso",
                "templateDownloadedPath": str(template_path),
                "sendingListDownloadedPath": str(sending_list_path),
            }

        def fake_sync_task_upload_mailboxes(**kwargs):
            mail_root = Path(kwargs["mail_data_dir"])
            db_path = mail_root / "MINISO" / "email_sync.db"
            raw_dir = mail_root / "MINISO" / "raw"
            db_path.parent.mkdir(parents=True, exist_ok=True)
            db_path.touch()
            raw_dir.mkdir(parents=True, exist_ok=True)
            return {
                "selectedCount": 1,
                "syncedCount": 1,
                "failedCount": 0,
                "items": [
                    {
                        "taskName": "MINISO",
                        "employeeName": "Alice",
                        "resolvedFolder": "其他文件夹/MINISO",
                        "mailFetchedCount": 3,
                        "mailSyncOk": True,
                        "mailSyncError": "",
                        "mailDbPath": str(db_path),
                        "mailRawDir": str(raw_dir),
                        "mailDataDir": str(db_path.parent),
                    }
                ],
            }

        def fake_enrich_creator_workbook(*, db, input_path, output_prefix):
            all_xlsx = output_prefix.with_suffix(".xlsx")
            high_xlsx = output_prefix.with_name(f"{output_prefix.name}_高置信").with_suffix(".xlsx")
            all_csv = output_prefix.with_suffix(".csv")
            high_csv = output_prefix.with_name(f"{output_prefix.name}_高置信").with_suffix(".csv")
            for path in (all_xlsx, high_xlsx, all_csv, high_csv):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.touch()
            return {
                "source_kind": "sending_list",
                "rows": 10,
                "matched_rows": 8,
                "high_confidence_rows": 4,
                "csv_path": str(all_csv),
                "xlsx_path": str(all_xlsx),
                "high_csv_path": str(high_csv),
                "high_xlsx_path": str(high_xlsx),
            }

        def fake_prepare_llm_review_candidates(*, db, input_path, output_prefix):
            prep_xlsx = output_prefix.with_suffix(".xlsx")
            deduped_xlsx = output_prefix.with_name(f"{output_prefix.name}_去重").with_suffix(".xlsx")
            jsonl_path = output_prefix.with_name(f"{output_prefix.name}_llm_candidates").with_suffix(".jsonl")
            for path in (prep_xlsx, deduped_xlsx, jsonl_path):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.touch()
            return {
                "source_row_count": 4,
                "prep_row_count": 4,
                "deduped_row_count": 3,
                "llm_candidate_group_count": 2,
                "prep_xlsx_path": str(prep_xlsx),
                "deduped_xlsx_path": str(deduped_xlsx),
                "llm_candidates_jsonl_path": str(jsonl_path),
            }

        def fake_run_and_apply_llm_review(*, input_prefix, env_path, base_url, api_key, model, wire_api):
            review_jsonl = input_prefix.with_name(f"{input_prefix.name}_llm_review").with_suffix(".jsonl")
            reviewed_xlsx = input_prefix.with_name(f"{input_prefix.name}_llm_reviewed").with_suffix(".xlsx")
            keep_xlsx = input_prefix.with_name(f"{input_prefix.name}_llm_reviewed_keep").with_suffix(".xlsx")
            for path in (review_jsonl, reviewed_xlsx, keep_xlsx):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.touch()
            return {
                "review_group_count": 2,
                "reviewed_row_count": 3,
                "keep_row_count": 2,
                "llm_review_jsonl_path": str(review_jsonl),
                "llm_reviewed_xlsx_path": str(reviewed_xlsx),
                "llm_reviewed_keep_xlsx_path": str(keep_xlsx),
            }

        task_runner._load_runtime_dependencies = lambda: {
            "Settings": object,
            "Database": FakeDb,
            "FeishuOpenClient": lambda **kwargs: FakeClient(),
            "DEFAULT_FEISHU_BASE_URL": "https://open.feishu.cn",
            "download_task_upload_screening_assets": fake_download_task_upload_screening_assets,
            "sync_task_upload_mailboxes": fake_sync_task_upload_mailboxes,
            "match_brand_keyword": lambda **kwargs: (_ for _ in ()).throw(AssertionError("fast path should not run")),
            "resolve_shared_email_candidates": lambda **kwargs: (_ for _ in ()).throw(AssertionError("fast path should not run")),
            "run_shared_email_final_review": lambda **kwargs: (_ for _ in ()).throw(AssertionError("fast path should not run")),
            "enrich_creator_workbook": fake_enrich_creator_workbook,
            "prepare_llm_review_candidates": fake_prepare_llm_review_candidates,
            "run_and_apply_llm_review": fake_run_and_apply_llm_review,
            "resolve_sync_sent_since": lambda value: __import__("datetime").date(2025, 12, 27),
            "load_local_env": fake_load_local_env,
            "get_preferred_value": fake_get_preferred_value,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            summary_path = temp_root / "run" / "summary.json"
            env_path = self._write_env_file(temp_root)
            summary = task_runner.run_task_upload_to_keep_list_pipeline(
                task_name="MINISO",
                env_file=env_path,
                output_root=temp_root / "run",
                summary_json=summary_path,
                stop_after="keep-list",
                matching_strategy="legacy-enrichment",
            )
            persisted = json.loads(summary_path.read_text(encoding="utf-8"))
            task_spec = json.loads(Path(summary["task_spec_json"]).read_text(encoding="utf-8"))

        self.assertEqual(summary["status"], "stopped_after_keep-list")
        self.assertEqual(summary["contract_version"], RUN_CONTRACT_VERSION)
        self.assertEqual(summary["verdict"]["outcome"], "stopped")
        self.assertEqual(summary["steps"]["task_assets"]["status"], "completed")
        self.assertEqual(summary["steps"]["mail_sync"]["status"], "completed")
        self.assertEqual(summary["steps"]["enrichment"]["status"], "completed")
        self.assertEqual(summary["steps"]["llm_candidates"]["status"], "completed")
        self.assertEqual(summary["steps"]["llm_review"]["status"], "completed")
        self.assertEqual(summary["contract"]["contract_version"], "phase16.keep-list.v2")
        self.assertEqual(summary["contract"]["canonical_boundary"], "keep-list")
        self.assertTrue(summary["run_id"])
        self.assertEqual(summary["run_root"], str((temp_root / "run").resolve()))
        self.assertEqual(summary["env_file_raw"], str(env_path))
        self.assertEqual(summary["env_file"], str(env_path.resolve()))
        self.assertEqual(summary["resolved_inputs"]["env_file"]["path"], str(env_path.resolve()))
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
        self.assertNotIn("app-secret", json.dumps(summary["resolved_config_sources"], ensure_ascii=False))
        self.assertFalse(summary["resume_context"]["existing_summary_accepted"])
        self.assertEqual(summary["resume_context"]["reset_reason"], "no_existing_summary")
        self.assertIn("exists", summary["resolved_inputs"]["env_file"])
        self.assertEqual(summary["resolved_inputs"]["env_file"]["path"], str(env_path.resolve()))
        self.assertEqual(summary["resolved_inputs"]["feishu"]["task_upload_url_source"], "env_file")
        self.assertEqual(summary["resolved_inputs"]["mail_sync"]["sent_since_source"], "default_today_only")
        self.assertEqual(task_spec["scope"], "task-upload-to-keep-list")
        self.assertEqual(task_spec["canonical_boundary"], "keep-list")
        self.assertEqual(task_spec["intent"]["task_name"], "MINISO")
        self.assertEqual(task_spec["intent"]["task_upload_url"], "https://env.example/task")
        self.assertEqual(task_spec["intent"]["stop_after"], "keep-list")
        self.assertEqual(task_spec["controls"]["owner_email_overrides"], {})
        self.assertEqual(summary["resolved_inputs"]["paths"]["downloads_dir"]["source"], "output_root_default")
        self.assertTrue(summary["setup"]["completed"])
        self.assertTrue(summary["setup"]["downloads_dir_ready"])
        self.assertEqual(summary["steps"]["mail_sync"]["resume_policy"]["stage_policy"], "always_rerun_incremental")
        self.assertEqual(summary["steps"]["llm_review"]["resume_policy"]["resume_point_key"], "keep_list")
        self.assertTrue(summary["artifacts"]["keep_workbook"].endswith("_llm_reviewed_keep.xlsx"))
        self.assertEqual(
            summary["downstream_handoff"]["runner_script"],
            "scripts/run_keep_list_screening_pipeline.py",
        )
        self.assertEqual(summary["downstream_handoff"]["boundary_step"], "keep-list")
        self.assertEqual(
            summary["canonical_artifacts"]["keep_list"]["keep_workbook"],
            summary["artifacts"]["keep_workbook"],
        )
        self.assertIn("--keep-workbook", summary["downstream_handoff"]["recommended_command"])
        self.assertEqual(
            persisted["resume_points"]["llm_candidates"]["llm_review_input_prefix"],
            summary["artifacts"]["llm_review_input_prefix"],
        )

    def test_runner_keeps_task_assets_boundary_pure_when_stopped_early(self) -> None:
        class FakeClient:
            pass

        def fake_load_local_env(env_file):
            return {
                "FEISHU_APP_ID": "app-id",
                "FEISHU_APP_SECRET": "app-secret",
                "TASK_UPLOAD_URL": "https://env.example/task",
                "EMPLOYEE_INFO_URL": "https://env.example/employee",
            }

        def fake_get_preferred_value(cli_value, env_values, env_key, default=""):
            return str(cli_value or "").strip() or str(env_values.get(env_key, default) or "").strip()

        def fake_download_task_upload_screening_assets(**kwargs):
            download_dir = Path(kwargs["download_dir"])
            sending_list_path = download_dir / "miniso_sending_list.xlsx"
            download_dir.mkdir(parents=True, exist_ok=True)
            sending_list_path.touch()
            return {
                "recordId": "rec123",
                "taskName": "MINISO",
                "linkedBitableUrl": "https://bitable.example/miniso",
                "templateDownloadedPath": str(FIXTURE_TEMPLATE),
                "sendingListDownloadedPath": str(sending_list_path),
            }

        task_runner._load_runtime_dependencies = lambda: {
            "Settings": object,
            "Database": object,
            "FeishuOpenClient": lambda **kwargs: FakeClient(),
            "DEFAULT_FEISHU_BASE_URL": "https://open.feishu.cn",
            "download_task_upload_screening_assets": fake_download_task_upload_screening_assets,
            "sync_task_upload_mailboxes": lambda **kwargs: (_ for _ in ()).throw(AssertionError("mail sync should not run")),
            "match_brand_keyword": lambda **kwargs: (_ for _ in ()).throw(AssertionError("fast path should not run")),
            "resolve_shared_email_candidates": lambda **kwargs: (_ for _ in ()).throw(AssertionError("shared resolution should not run")),
            "run_shared_email_final_review": lambda **kwargs: (_ for _ in ()).throw(AssertionError("shared final review should not run")),
            "enrich_creator_workbook": lambda **kwargs: (_ for _ in ()).throw(AssertionError("enrichment should not run")),
            "prepare_llm_review_candidates": lambda **kwargs: (_ for _ in ()).throw(AssertionError("llm candidates should not run")),
            "run_and_apply_llm_review": lambda **kwargs: (_ for _ in ()).throw(AssertionError("llm review should not run")),
            "resolve_sync_sent_since": lambda value: __import__("datetime").date(2025, 12, 27),
            "load_local_env": fake_load_local_env,
            "get_preferred_value": fake_get_preferred_value,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            summary = task_runner.run_task_upload_to_keep_list_pipeline(
                task_name="MINISO",
                env_file=".env",
                output_root=temp_root / "run",
                task_upload_url="https://cli.example/task",
                employee_info_url="https://cli.example/employee",
                feishu_app_id="app-id",
                feishu_app_secret="app-secret",
                stop_after="task-assets",
            )
            workflow_handoff = json.loads(Path(summary["workflow_handoff_json"]).read_text(encoding="utf-8"))

        self.assertEqual(summary["status"], "stopped_after_task-assets")
        self.assertTrue(summary["workflow_handoff_json"].endswith("/workflow_handoff.json"))
        self.assertEqual(summary["steps"]["task_assets"]["status"], "completed")
        self.assertEqual(summary["artifacts"].get("template_prepare_summary_json", ""), "")
        self.assertEqual(summary["artifacts"].get("template_runtime_prompt_artifacts_json", ""), "")
        self.assertNotIn("template_prompt_artifacts", summary["steps"]["task_assets"])
        self.assertEqual(workflow_handoff["verdict"]["outcome"], "stopped")
        self.assertEqual(workflow_handoff["recommended_action"], "resume_run")
        self.assertTrue(workflow_handoff["task_spec_available"])
        self.assertTrue(workflow_handoff["resume"]["available"])
        self.assertEqual(workflow_handoff["resume"]["canonical_resume_point"], "keep_list")
        self.assertIn("task_assets", workflow_handoff["resume"]["resume_point_keys"])
        self.assertNotIn("resume_points", workflow_handoff["pointers"])
        self.assertEqual(workflow_handoff["intent_summary"]["intent"]["stop_after"], "task-assets")

    def test_runner_passes_owner_email_overrides_to_mail_sync(self) -> None:
        observed: dict[str, object] = {}

        class FakeClient:
            pass

        class FakeDb:
            def __init__(self, db_path):
                self.db_path = Path(db_path)

            def close(self):
                return None

        def fake_load_local_env(env_file):
            return {
                "FEISHU_APP_ID": "app-id",
                "FEISHU_APP_SECRET": "app-secret",
                "TASK_UPLOAD_URL": "https://env.example/task",
                "EMPLOYEE_INFO_URL": "https://env.example/employee",
                "TIMEOUT_SECONDS": "30",
            }

        def fake_get_preferred_value(cli_value, env_values, env_key, default=""):
            return str(cli_value or "").strip() or str(env_values.get(env_key, default) or "").strip()

        def fake_download_task_upload_screening_assets(**kwargs):
            download_dir = Path(kwargs["download_dir"])
            template_path = download_dir / "miniso_template.xlsx"
            sending_list_path = download_dir / "miniso_sending_list.xlsx"
            template_path.parent.mkdir(parents=True, exist_ok=True)
            template_path.touch()
            sending_list_path.touch()
            return {
                "recordId": "rec123",
                "taskName": "MINISO",
                "linkedBitableUrl": "https://bitable.example/miniso",
                "templateDownloadedPath": str(template_path),
                "sendingListDownloadedPath": str(sending_list_path),
            }

        def fake_sync_task_upload_mailboxes(**kwargs):
            observed["owner_email_overrides"] = kwargs["owner_email_overrides"]
            mail_root = Path(kwargs["mail_data_dir"])
            db_path = mail_root / "MINISO" / "email_sync.db"
            raw_dir = mail_root / "MINISO" / "raw"
            db_path.parent.mkdir(parents=True, exist_ok=True)
            db_path.touch()
            raw_dir.mkdir(parents=True, exist_ok=True)
            return {
                "selectedCount": 1,
                "syncedCount": 1,
                "failedCount": 0,
                "items": [
                    {
                        "taskName": "MINISO",
                        "employeeName": "Eden",
                        "resolvedFolder": "其他文件夹/MINISO",
                        "mailFetchedCount": 1,
                        "mailSyncOk": True,
                        "mailSyncError": "",
                        "mailDbPath": str(db_path),
                        "mailRawDir": str(raw_dir),
                        "mailDataDir": str(db_path.parent),
                    }
                ],
            }

        def fake_enrich_creator_workbook(*, db, input_path, output_prefix):
            all_xlsx = output_prefix.with_suffix(".xlsx")
            high_xlsx = output_prefix.with_name(f"{output_prefix.name}_高置信").with_suffix(".xlsx")
            all_csv = output_prefix.with_suffix(".csv")
            high_csv = output_prefix.with_name(f"{output_prefix.name}_高置信").with_suffix(".csv")
            for path in (all_xlsx, high_xlsx, all_csv, high_csv):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.touch()
            return {
                "source_kind": "sending_list",
                "rows": 1,
                "matched_rows": 1,
                "high_confidence_rows": 1,
                "csv_path": str(all_csv),
                "xlsx_path": str(all_xlsx),
                "high_csv_path": str(high_csv),
                "high_xlsx_path": str(high_xlsx),
            }

        def fake_prepare_llm_review_candidates(*, db, input_path, output_prefix):
            prep_xlsx = output_prefix.with_suffix(".xlsx")
            deduped_xlsx = output_prefix.with_name(f"{output_prefix.name}_去重").with_suffix(".xlsx")
            jsonl_path = output_prefix.with_name(f"{output_prefix.name}_llm_candidates").with_suffix(".jsonl")
            for path in (prep_xlsx, deduped_xlsx, jsonl_path):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.touch()
            return {
                "source_row_count": 1,
                "prep_row_count": 1,
                "deduped_row_count": 1,
                "llm_candidate_group_count": 0,
                "prep_xlsx_path": str(prep_xlsx),
                "deduped_xlsx_path": str(deduped_xlsx),
                "llm_candidates_jsonl_path": str(jsonl_path),
            }

        def fake_run_and_apply_llm_review(*, input_prefix, env_path, base_url, api_key, model, wire_api):
            review_jsonl = input_prefix.with_name(f"{input_prefix.name}_llm_review").with_suffix(".jsonl")
            reviewed_xlsx = input_prefix.with_name(f"{input_prefix.name}_llm_reviewed").with_suffix(".xlsx")
            keep_xlsx = input_prefix.with_name(f"{input_prefix.name}_llm_reviewed_keep").with_suffix(".xlsx")
            for path in (review_jsonl, reviewed_xlsx, keep_xlsx):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.touch()
            return {
                "review_group_count": 0,
                "reviewed_row_count": 1,
                "keep_row_count": 1,
                "llm_review_jsonl_path": str(review_jsonl),
                "llm_reviewed_xlsx_path": str(reviewed_xlsx),
                "llm_reviewed_keep_xlsx_path": str(keep_xlsx),
            }

        task_runner._load_runtime_dependencies = lambda: {
            "Settings": object,
            "Database": FakeDb,
            "FeishuOpenClient": lambda **kwargs: FakeClient(),
            "DEFAULT_FEISHU_BASE_URL": "https://open.feishu.cn",
            "download_task_upload_screening_assets": fake_download_task_upload_screening_assets,
            "sync_task_upload_mailboxes": fake_sync_task_upload_mailboxes,
            "match_brand_keyword": lambda **kwargs: (_ for _ in ()).throw(AssertionError("fast path should not run")),
            "resolve_shared_email_candidates": lambda **kwargs: (_ for _ in ()).throw(AssertionError("fast path should not run")),
            "run_shared_email_final_review": lambda **kwargs: (_ for _ in ()).throw(AssertionError("fast path should not run")),
            "enrich_creator_workbook": fake_enrich_creator_workbook,
            "prepare_llm_review_candidates": fake_prepare_llm_review_candidates,
            "run_and_apply_llm_review": fake_run_and_apply_llm_review,
            "resolve_sync_sent_since": lambda value: __import__("datetime").date(2025, 12, 27),
            "load_local_env": fake_load_local_env,
            "get_preferred_value": fake_get_preferred_value,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            env_path = self._write_env_file(temp_root)
            summary = task_runner.run_task_upload_to_keep_list_pipeline(
                task_name="MINISO",
                env_file=env_path,
                output_root=temp_root / "run",
                owner_email_overrides={"MINISO": "eden@amagency.biz"},
                stop_after="keep-list",
            )

        self.assertEqual(observed["owner_email_overrides"], {"MINISO": "eden@amagency.biz"})
        self.assertEqual(summary["resolved_inputs"]["mail_sync"]["owner_email_overrides"], {"MINISO": "eden@amagency.biz"})

    def test_runner_can_reuse_existing_shared_mail_db_without_running_imap_sync(self) -> None:
        class FakeClient:
            pass

        class FakeDb:
            def __init__(self, db_path):
                self.db_path = Path(db_path)

            def close(self):
                return None

        def fake_load_local_env(env_file):
            return {
                "FEISHU_APP_ID": "app-id",
                "FEISHU_APP_SECRET": "app-secret",
                "TASK_UPLOAD_URL": "https://env.example/task",
                "EMPLOYEE_INFO_URL": "https://env.example/employee",
                "TIMEOUT_SECONDS": "30",
                "EMAIL_ACCOUNT": "partnerships@amagency.biz",
                "EMAIL_AUTH_CODE": "secret",
            }

        def fake_get_preferred_value(cli_value, env_values, env_key, default=""):
            return str(cli_value or "").strip() or str(env_values.get(env_key, default) or "").strip()

        def fake_download_task_upload_screening_assets(**kwargs):
            download_dir = Path(kwargs["download_dir"])
            template_path = download_dir / "miniso_template.xlsx"
            sending_list_path = download_dir / "miniso_sending_list.xlsx"
            template_path.parent.mkdir(parents=True, exist_ok=True)
            template_path.touch()
            sending_list_path.touch()
            return {
                "recordId": "rec123",
                "taskName": "MINISO",
                "linkedBitableUrl": "https://bitable.example/miniso",
                "templateDownloadedPath": str(template_path),
                "sendingListDownloadedPath": str(sending_list_path),
            }

        def fake_inspect_task_upload_assignments(**kwargs):
            return {
                "items": [
                    {
                        "recordId": "rec123",
                        "taskName": "MINISO",
                        "employeeId": "ou_alpha",
                        "employeeRecordId": "rec_emp",
                        "employeeName": "陈俊仁",
                        "employeeEmail": "chenjunren@amagency.biz",
                        "responsibleName": "陈俊仁",
                        "ownerName": "陈俊仁",
                        "linkedBitableUrl": "https://bitable.example/miniso",
                    }
                ]
            }

        def fail_sync_task_upload_mailboxes(**kwargs):
            raise AssertionError("should not call IMAP sync when existing_mail_db_path is provided")

        observed: dict[str, object] = {}

        def fake_match_brand_keyword(*, db, input_path, output_prefix, keyword, sent_since, include_from):
            observed["sent_since"] = sent_since
            all_xlsx = output_prefix.with_suffix(".xlsx")
            deduped_xlsx = output_prefix.with_name(f"{output_prefix.name}_去重").with_suffix(".xlsx")
            unique_xlsx = output_prefix.with_name(f"{output_prefix.name}_唯一邮箱").with_suffix(".xlsx")
            shared_xlsx = output_prefix.with_name(f"{output_prefix.name}_共享邮箱").with_suffix(".xlsx")
            for path in (all_xlsx, deduped_xlsx, unique_xlsx, shared_xlsx):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.touch()
            return {
                "source_kind": "sending_list",
                "message_hit_count": 1,
                "matched_email_count": 1,
                "email_direct_match_row_count": 1,
                "profile_deduped_row_count": 1,
                "unique_email_row_count": 1,
                "shared_email_row_count": 0,
                "shared_email_group_count": 0,
                "xlsx_path": str(all_xlsx),
                "deduped_xlsx_path": str(deduped_xlsx),
                "unique_xlsx_path": str(unique_xlsx),
                "shared_xlsx_path": str(shared_xlsx),
            }

        def fake_resolve_shared_email_candidates(*, db, input_path, output_prefix):
            resolved_xlsx = output_prefix.with_suffix(".xlsx")
            unresolved_xlsx = output_prefix.with_name(f"{output_prefix.name}_待复核").with_suffix(".xlsx")
            jsonl_path = output_prefix.with_name(f"{output_prefix.name}_llm_candidates").with_suffix(".jsonl")
            for path in (resolved_xlsx, unresolved_xlsx, jsonl_path):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.touch()
            return {
                "resolved_group_count": 0,
                "resolved_row_count": 0,
                "unresolved_group_count": 0,
                "unresolved_row_count": 0,
                "llm_candidate_group_count": 0,
                "resolved_xlsx_path": str(resolved_xlsx),
                "unresolved_xlsx_path": str(unresolved_xlsx),
                "llm_candidates_jsonl_path": str(jsonl_path),
            }

        def fake_run_shared_email_final_review(*, input_prefix, env_path, auto_keep_paths, base_url, api_key, model, wire_api):
            review_jsonl = input_prefix.with_name(f"{input_prefix.name}_llm_review").with_suffix(".jsonl")
            resolved_xlsx = input_prefix.with_name(f"{input_prefix.name}_llm_resolved").with_suffix(".xlsx")
            manual_tail_xlsx = input_prefix.with_name(f"{input_prefix.name}_manual_tail").with_suffix(".xlsx")
            keep_xlsx = input_prefix.with_name(f"{input_prefix.name}_final_keep").with_suffix(".xlsx")
            for path in (review_jsonl, resolved_xlsx, manual_tail_xlsx, keep_xlsx):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.touch()
            return {
                "review_group_count": 0,
                "llm_resolved_row_count": 0,
                "manual_row_count": 0,
                "final_keep_row_count": 0,
                "retryable_failure_count": 0,
                "llm_review_jsonl_path": str(review_jsonl),
                "llm_resolved_xlsx_path": str(resolved_xlsx),
                "manual_tail_xlsx_path": str(manual_tail_xlsx),
                "final_keep_xlsx_path": str(keep_xlsx),
            }

        task_runner._load_runtime_dependencies = lambda: {
            "Settings": object,
            "Database": FakeDb,
            "FeishuOpenClient": lambda **kwargs: FakeClient(),
            "DEFAULT_FEISHU_BASE_URL": "https://open.feishu.cn",
            "download_task_upload_screening_assets": fake_download_task_upload_screening_assets,
            "inspect_task_upload_assignments": fake_inspect_task_upload_assignments,
            "sync_task_upload_mailboxes": fail_sync_task_upload_mailboxes,
            "match_brand_keyword": fake_match_brand_keyword,
            "resolve_shared_email_candidates": fake_resolve_shared_email_candidates,
            "run_shared_email_final_review": fake_run_shared_email_final_review,
            "enrich_creator_workbook": lambda **kwargs: (_ for _ in ()).throw(AssertionError("legacy enrichment should not run")),
            "prepare_llm_review_candidates": lambda **kwargs: (_ for _ in ()).throw(AssertionError("llm candidates should not run")),
            "run_and_apply_llm_review": lambda **kwargs: (_ for _ in ()).throw(AssertionError("llm review should not run")),
            "resolve_sync_sent_since": lambda value: __import__("datetime").date(2026, 3, 31),
            "load_local_env": fake_load_local_env,
            "get_preferred_value": fake_get_preferred_value,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            existing_db = root / "shared" / "email_sync.db"
            existing_raw = root / "shared" / "raw"
            existing_db.parent.mkdir(parents=True, exist_ok=True)
            existing_db.touch()
            existing_raw.mkdir(parents=True, exist_ok=True)
            summary = task_runner.run_task_upload_to_keep_list_pipeline(
                task_name="MINISO",
                env_file=".env",
                output_root=root / "run",
                summary_json=root / "run" / "summary.json",
                matching_strategy="brand-keyword-fast-path",
                stop_after="keep-list",
                task_upload_url="https://cli.example/task",
                employee_info_url="https://cli.example/employee",
                feishu_app_id="app-id",
                feishu_app_secret="app-secret",
                existing_mail_db_path=existing_db,
                existing_mail_raw_dir=existing_raw,
                existing_mail_data_dir=existing_db.parent,
            )

        self.assertEqual(summary["status"], "stopped_after_keep-list")
        self.assertEqual(summary["resolved_inputs"]["mail_sync"]["source_mode"], "pre_synced_mail_db")
        self.assertEqual(summary["steps"]["mail_sync"]["task"]["credential_source"], "external_shared_mailbox_cache")
        self.assertEqual(summary["steps"]["mail_sync"]["artifacts"]["mail_db_path"], str(existing_db.resolve()))
        self.assertEqual(summary["steps"]["mail_sync"]["artifacts"]["mail_raw_dir"], str(existing_raw.resolve()))
        self.assertEqual(summary["steps"]["mail_sync"]["resume_policy"]["stage_policy"], "reuse_external_shared_mail_db_reference")
        self.assertEqual(str(observed["sent_since"]), "2026-03-31")

    def test_runner_passes_default_mail_credentials_to_mail_sync(self) -> None:
        observed: dict[str, object] = {}

        class FakeClient:
            pass

        class FakeDb:
            def __init__(self, db_path):
                self.db_path = Path(db_path)

            def close(self):
                return None

        def fake_load_local_env(env_file):
            return {
                "FEISHU_APP_ID": "app-id",
                "FEISHU_APP_SECRET": "app-secret",
                "TASK_UPLOAD_URL": "https://env.example/task",
                "EMPLOYEE_INFO_URL": "https://env.example/employee",
                "TIMEOUT_SECONDS": "30",
                "EMAIL_ACCOUNT": "partnerships@amagency.biz",
                "EMAIL_AUTH_CODE": "xYeGKyNmK5jFN7Y2",
            }

        def fake_get_preferred_value(cli_value, env_values, env_key, default=""):
            return str(cli_value or "").strip() or str(env_values.get(env_key, default) or "").strip()

        def fake_download_task_upload_screening_assets(**kwargs):
            download_dir = Path(kwargs["download_dir"])
            template_path = download_dir / "miniso_template.xlsx"
            sending_list_path = download_dir / "miniso_sending_list.xlsx"
            template_path.parent.mkdir(parents=True, exist_ok=True)
            template_path.touch()
            sending_list_path.touch()
            return {
                "recordId": "rec123",
                "taskName": "MINISO",
                "linkedBitableUrl": "https://bitable.example/miniso",
                "templateDownloadedPath": str(template_path),
                "sendingListDownloadedPath": str(sending_list_path),
            }

        def fake_sync_task_upload_mailboxes(**kwargs):
            observed["default_account_email"] = kwargs["default_account_email"]
            observed["default_auth_code"] = kwargs["default_auth_code"]
            mail_root = Path(kwargs["mail_data_dir"])
            db_path = mail_root / "MINISO" / "email_sync.db"
            raw_dir = mail_root / "MINISO" / "raw"
            db_path.parent.mkdir(parents=True, exist_ok=True)
            db_path.touch()
            raw_dir.mkdir(parents=True, exist_ok=True)
            return {
                "selectedCount": 1,
                "syncedCount": 1,
                "failedCount": 0,
                "items": [
                    {
                        "taskName": "MINISO",
                        "employeeName": "",
                        "resolvedFolder": "其他文件夹/MINISO",
                        "mailFetchedCount": 1,
                        "mailSyncOk": True,
                        "mailSyncError": "",
                        "mailDbPath": str(db_path),
                        "mailRawDir": str(raw_dir),
                        "mailDataDir": str(db_path.parent),
                        "mailCredentialSource": "default_account",
                        "mailLoginEmail": "partnerships@amagency.biz",
                    }
                ],
            }

        def fake_enrich_creator_workbook(*, db, input_path, output_prefix):
            all_xlsx = output_prefix.with_suffix(".xlsx")
            high_xlsx = output_prefix.with_name(f"{output_prefix.name}_高置信").with_suffix(".xlsx")
            all_csv = output_prefix.with_suffix(".csv")
            high_csv = output_prefix.with_name(f"{output_prefix.name}_高置信").with_suffix(".csv")
            for path in (all_xlsx, high_xlsx, all_csv, high_csv):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.touch()
            return {
                "source_kind": "sending_list",
                "rows": 1,
                "matched_rows": 1,
                "high_confidence_rows": 1,
                "csv_path": str(all_csv),
                "xlsx_path": str(all_xlsx),
                "high_csv_path": str(high_csv),
                "high_xlsx_path": str(high_xlsx),
            }

        def fake_prepare_llm_review_candidates(*, db, input_path, output_prefix):
            prep_xlsx = output_prefix.with_suffix(".xlsx")
            deduped_xlsx = output_prefix.with_name(f"{output_prefix.name}_去重").with_suffix(".xlsx")
            jsonl_path = output_prefix.with_name(f"{output_prefix.name}_llm_candidates").with_suffix(".jsonl")
            for path in (prep_xlsx, deduped_xlsx, jsonl_path):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.touch()
            return {
                "source_row_count": 1,
                "prep_row_count": 1,
                "deduped_row_count": 1,
                "llm_candidate_group_count": 0,
                "prep_xlsx_path": str(prep_xlsx),
                "deduped_xlsx_path": str(deduped_xlsx),
                "llm_candidates_jsonl_path": str(jsonl_path),
            }

        def fake_run_and_apply_llm_review(*, input_prefix, env_path, base_url, api_key, model, wire_api):
            review_jsonl = input_prefix.with_name(f"{input_prefix.name}_llm_review").with_suffix(".jsonl")
            reviewed_xlsx = input_prefix.with_name(f"{input_prefix.name}_llm_reviewed").with_suffix(".xlsx")
            keep_xlsx = input_prefix.with_name(f"{input_prefix.name}_llm_reviewed_keep").with_suffix(".xlsx")
            for path in (review_jsonl, reviewed_xlsx, keep_xlsx):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.touch()
            return {
                "review_group_count": 0,
                "reviewed_row_count": 1,
                "keep_row_count": 1,
                "llm_review_jsonl_path": str(review_jsonl),
                "llm_reviewed_xlsx_path": str(reviewed_xlsx),
                "llm_reviewed_keep_xlsx_path": str(keep_xlsx),
            }

        task_runner._load_runtime_dependencies = lambda: {
            "Settings": object,
            "Database": FakeDb,
            "FeishuOpenClient": lambda **kwargs: FakeClient(),
            "DEFAULT_FEISHU_BASE_URL": "https://open.feishu.cn",
            "download_task_upload_screening_assets": fake_download_task_upload_screening_assets,
            "sync_task_upload_mailboxes": fake_sync_task_upload_mailboxes,
            "match_brand_keyword": lambda **kwargs: (_ for _ in ()).throw(AssertionError("fast path should not run")),
            "resolve_shared_email_candidates": lambda **kwargs: (_ for _ in ()).throw(AssertionError("fast path should not run")),
            "run_shared_email_final_review": lambda **kwargs: (_ for _ in ()).throw(AssertionError("fast path should not run")),
            "enrich_creator_workbook": fake_enrich_creator_workbook,
            "prepare_llm_review_candidates": fake_prepare_llm_review_candidates,
            "run_and_apply_llm_review": fake_run_and_apply_llm_review,
            "resolve_sync_sent_since": lambda value: __import__("datetime").date(2025, 12, 27),
            "load_local_env": fake_load_local_env,
            "get_preferred_value": fake_get_preferred_value,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            env_path = self._write_env_file(
                temp_root,
                EMAIL_ACCOUNT="partnerships@amagency.biz",
                EMAIL_AUTH_CODE="xYeGKyNmK5jFN7Y2",
            )
            summary = task_runner.run_task_upload_to_keep_list_pipeline(
                task_name="MINISO",
                env_file=env_path,
                output_root=temp_root / "run",
                stop_after="keep-list",
            )

        self.assertEqual(observed["default_account_email"], "partnerships@amagency.biz")
        self.assertEqual(observed["default_auth_code"], "xYeGKyNmK5jFN7Y2")
        self.assertEqual(summary["resolved_inputs"]["mail_sync"]["default_account_email"], "partnerships@amagency.biz")
        self.assertEqual(summary["resolved_inputs"]["mail_sync"]["default_account_email_source"], "env_file:EMAIL_ACCOUNT")
        self.assertTrue(summary["resolved_inputs"]["mail_sync"]["default_auth_code_present"])
        self.assertEqual(summary["resolved_inputs"]["mail_sync"]["default_auth_code_source"], "env_file:EMAIL_AUTH_CODE")
        self.assertEqual(
            summary["resolved_inputs"]["mail_sync"]["credential_mode"],
            "default_account_preferred_with_employee_fallback",
        )

    def test_runner_defaults_sent_since_to_task_upload_start_date(self) -> None:
        observed: dict[str, object] = {}

        class FakeDb:
            def __init__(self, db_path):
                self.db_path = Path(db_path)

            def close(self):
                return None

        def fake_download_task_upload_screening_assets(**kwargs):
            download_dir = Path(kwargs["download_dir"])
            template_path = download_dir / "template.xlsx"
            sending_list_path = download_dir / "sending_list.xlsx"
            template_path.parent.mkdir(parents=True, exist_ok=True)
            template_path.touch()
            sending_list_path.touch()
            return {
                "recordId": "rec123",
                "taskName": "MINISO",
                "taskStartDate": "2026-04-01",
                "linkedBitableUrl": "https://bitable.example/miniso",
                "templateDownloadedPath": str(template_path),
                "sendingListDownloadedPath": str(sending_list_path),
            }

        def fake_sync_task_upload_mailboxes(**kwargs):
            observed["sent_since"] = kwargs["sent_since"]
            mail_root = Path(kwargs["mail_data_dir"])
            db_path = mail_root / "MINISO" / "email_sync.db"
            raw_dir = mail_root / "MINISO" / "raw"
            db_path.parent.mkdir(parents=True, exist_ok=True)
            db_path.touch()
            raw_dir.mkdir(parents=True, exist_ok=True)
            return {
                "selectedCount": 1,
                "syncedCount": 1,
                "failedCount": 0,
                "items": [
                    {
                        "taskName": "MINISO",
                        "employeeName": "Alice",
                        "resolvedFolder": "其他文件夹/MINISO",
                        "mailFetchedCount": 3,
                        "mailSyncOk": True,
                        "mailSyncError": "",
                        "mailDbPath": str(db_path),
                        "mailRawDir": str(raw_dir),
                        "mailDataDir": str(db_path.parent),
                    }
                ],
            }

        task_runner._load_runtime_dependencies = lambda: {
            "Settings": object,
            "Database": FakeDb,
            "FeishuOpenClient": lambda **kwargs: object(),
            "DEFAULT_FEISHU_BASE_URL": "https://open.feishu.cn",
            "download_task_upload_screening_assets": fake_download_task_upload_screening_assets,
            "sync_task_upload_mailboxes": fake_sync_task_upload_mailboxes,
            "match_brand_keyword": lambda **kwargs: {},
            "resolve_shared_email_candidates": lambda **kwargs: {},
            "run_shared_email_final_review": lambda **kwargs: {},
            "enrich_creator_workbook": lambda **kwargs: {},
            "prepare_llm_review_candidates": lambda **kwargs: {},
            "run_and_apply_llm_review": lambda **kwargs: {},
            "resolve_sync_sent_since": lambda value: __import__("datetime").date.fromisoformat(value) if value else __import__("datetime").date(2025, 12, 27),
            "load_local_env": lambda env_file: {},
            "get_preferred_value": lambda cli_value, env_values, env_key, default="": str(cli_value or "").strip() or str(env_values.get(env_key, default) or "").strip(),
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            env_path = self._write_env_file(temp_root)
            summary = task_runner.run_task_upload_to_keep_list_pipeline(
                task_name="MINISO",
                env_file=env_path,
                output_root=temp_root / "run",
                summary_json=temp_root / "run" / "summary.json",
                stop_after="mail-sync",
                task_upload_url="https://example.com/task",
                employee_info_url="https://example.com/employee",
                feishu_app_id="app-id",
                feishu_app_secret="app-secret",
            )

        self.assertEqual(summary["status"], "stopped_after_mail-sync")
        self.assertEqual(summary["resolved_inputs"]["mail_sync"]["sent_since"], "2026-04-01")
        self.assertEqual(summary["resolved_inputs"]["mail_sync"]["sent_since_source"], "task_upload_start_time")
        self.assertEqual(summary["resolved_inputs"]["mail_sync"]["task_start_date"], "2026-04-01")
        self.assertEqual(observed["sent_since"], "2026-04-01")

    def test_runner_prefers_cli_sent_since_over_task_upload_start_date(self) -> None:
        observed: dict[str, object] = {}

        class FakeDb:
            def __init__(self, db_path):
                self.db_path = Path(db_path)

            def close(self):
                return None

        def fake_download_task_upload_screening_assets(**kwargs):
            download_dir = Path(kwargs["download_dir"])
            template_path = download_dir / "template.xlsx"
            sending_list_path = download_dir / "sending_list.xlsx"
            template_path.parent.mkdir(parents=True, exist_ok=True)
            template_path.touch()
            sending_list_path.touch()
            return {
                "recordId": "rec123",
                "taskName": "MINISO",
                "taskStartDate": "2026-04-01",
                "linkedBitableUrl": "https://bitable.example/miniso",
                "templateDownloadedPath": str(template_path),
                "sendingListDownloadedPath": str(sending_list_path),
            }

        def fake_sync_task_upload_mailboxes(**kwargs):
            observed["sent_since"] = kwargs["sent_since"]
            mail_root = Path(kwargs["mail_data_dir"])
            db_path = mail_root / "MINISO" / "email_sync.db"
            raw_dir = mail_root / "MINISO" / "raw"
            db_path.parent.mkdir(parents=True, exist_ok=True)
            db_path.touch()
            raw_dir.mkdir(parents=True, exist_ok=True)
            return {
                "selectedCount": 1,
                "syncedCount": 1,
                "failedCount": 0,
                "items": [
                    {
                        "taskName": "MINISO",
                        "employeeName": "Alice",
                        "resolvedFolder": "其他文件夹/MINISO",
                        "mailFetchedCount": 3,
                        "mailSyncOk": True,
                        "mailSyncError": "",
                        "mailDbPath": str(db_path),
                        "mailRawDir": str(raw_dir),
                        "mailDataDir": str(db_path.parent),
                    }
                ],
            }

        task_runner._load_runtime_dependencies = lambda: {
            "Settings": object,
            "Database": FakeDb,
            "FeishuOpenClient": lambda **kwargs: object(),
            "DEFAULT_FEISHU_BASE_URL": "https://open.feishu.cn",
            "download_task_upload_screening_assets": fake_download_task_upload_screening_assets,
            "sync_task_upload_mailboxes": fake_sync_task_upload_mailboxes,
            "match_brand_keyword": lambda **kwargs: {},
            "resolve_shared_email_candidates": lambda **kwargs: {},
            "run_shared_email_final_review": lambda **kwargs: {},
            "enrich_creator_workbook": lambda **kwargs: {},
            "prepare_llm_review_candidates": lambda **kwargs: {},
            "run_and_apply_llm_review": lambda **kwargs: {},
            "resolve_sync_sent_since": lambda value: __import__("datetime").date.fromisoformat(value) if value else __import__("datetime").date(2025, 12, 27),
            "load_local_env": lambda env_file: {},
            "get_preferred_value": lambda cli_value, env_values, env_key, default="": str(cli_value or "").strip() or str(env_values.get(env_key, default) or "").strip(),
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            env_path = self._write_env_file(temp_root)
            summary = task_runner.run_task_upload_to_keep_list_pipeline(
                task_name="MINISO",
                env_file=env_path,
                output_root=temp_root / "run",
                summary_json=temp_root / "run" / "summary.json",
                stop_after="mail-sync",
                sent_since="2026-04-02",
                task_upload_url="https://example.com/task",
                employee_info_url="https://example.com/employee",
                feishu_app_id="app-id",
                feishu_app_secret="app-secret",
            )

        self.assertEqual(summary["status"], "stopped_after_mail-sync")
        self.assertEqual(summary["resolved_inputs"]["mail_sync"]["sent_since"], "2026-04-02")
        self.assertEqual(summary["resolved_inputs"]["mail_sync"]["sent_since_source"], "cli")
        self.assertEqual(summary["resolved_inputs"]["mail_sync"]["task_start_date"], "2026-04-01")
        self.assertEqual(observed["sent_since"], "2026-04-02")

    def test_runner_can_reuse_existing_artifacts_from_prior_summary(self) -> None:
        class FakeClient:
            pass

        class FakeDb:
            def __init__(self, db_path):
                self.db_path = Path(db_path)

            def close(self):
                return None

        enrich_calls = {"count": 0}
        candidate_calls = {"count": 0}
        review_calls = {"count": 0}

        def fake_load_local_env(env_file):
            return {
                "FEISHU_APP_ID": "app-id",
                "FEISHU_APP_SECRET": "app-secret",
                "TASK_UPLOAD_URL": "https://env.example/task",
                "EMPLOYEE_INFO_URL": "https://env.example/employee",
            }

        def fake_get_preferred_value(cli_value, env_values, env_key, default=""):
            return str(cli_value or "").strip() or str(env_values.get(env_key, default) or "").strip()

        def fake_download_task_upload_screening_assets(**kwargs):
            raise AssertionError("task assets should be reused")

        def fake_sync_task_upload_mailboxes(**kwargs):
            mail_root = Path(kwargs["mail_data_dir"])
            db_path = mail_root / "MINISO" / "email_sync.db"
            raw_dir = mail_root / "MINISO" / "raw"
            db_path.parent.mkdir(parents=True, exist_ok=True)
            db_path.touch()
            raw_dir.mkdir(parents=True, exist_ok=True)
            return {
                "selectedCount": 1,
                "syncedCount": 1,
                "failedCount": 0,
                "items": [
                    {
                        "taskName": "MINISO",
                        "employeeName": "Alice",
                        "resolvedFolder": "其他文件夹/MINISO",
                        "mailFetchedCount": 0,
                        "mailSyncOk": True,
                        "mailSyncError": "",
                        "mailDbPath": str(db_path),
                        "mailRawDir": str(raw_dir),
                        "mailDataDir": str(db_path.parent),
                    }
                ],
            }

        def fake_enrich_creator_workbook(*, db, input_path, output_prefix):
            enrich_calls["count"] += 1
            raise AssertionError("enrichment should be reused")

        def fake_prepare_llm_review_candidates(*, db, input_path, output_prefix):
            candidate_calls["count"] += 1
            raise AssertionError("llm candidates should be reused")

        def fake_run_and_apply_llm_review(*, input_prefix, env_path, base_url, api_key, model, wire_api):
            review_calls["count"] += 1
            raise AssertionError("llm review should be reused")

        task_runner._load_runtime_dependencies = lambda: {
            "Settings": object,
            "Database": FakeDb,
            "FeishuOpenClient": lambda **kwargs: FakeClient(),
            "DEFAULT_FEISHU_BASE_URL": "https://open.feishu.cn",
            "download_task_upload_screening_assets": fake_download_task_upload_screening_assets,
            "sync_task_upload_mailboxes": fake_sync_task_upload_mailboxes,
            "match_brand_keyword": lambda **kwargs: (_ for _ in ()).throw(AssertionError("fast path should not run")),
            "resolve_shared_email_candidates": lambda **kwargs: (_ for _ in ()).throw(AssertionError("fast path should not run")),
            "run_shared_email_final_review": lambda **kwargs: (_ for _ in ()).throw(AssertionError("fast path should not run")),
            "enrich_creator_workbook": fake_enrich_creator_workbook,
            "prepare_llm_review_candidates": fake_prepare_llm_review_candidates,
            "run_and_apply_llm_review": fake_run_and_apply_llm_review,
            "resolve_sync_sent_since": lambda value: __import__("datetime").date(2025, 12, 27),
            "load_local_env": fake_load_local_env,
            "get_preferred_value": fake_get_preferred_value,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            env_path = self._write_env_file(temp_root)
            downloads = temp_root / "run" / "downloads"
            exports = temp_root / "run" / "exports"
            downloads.mkdir(parents=True, exist_ok=True)
            exports.mkdir(parents=True, exist_ok=True)

            template = downloads / "template.xlsx"
            sending = downloads / "sending.xlsx"
            all_xlsx = exports / "miniso_all.xlsx"
            high_xlsx = exports / "miniso_high.xlsx"
            prep_xlsx = exports / "miniso_prep.xlsx"
            deduped_xlsx = exports / "miniso_deduped.xlsx"
            candidates_jsonl = exports / "miniso_candidates.jsonl"
            review_jsonl = exports / "miniso_review.jsonl"
            reviewed_xlsx = exports / "miniso_reviewed.xlsx"
            keep_xlsx = exports / "miniso_keep.xlsx"
            for path in (
                template,
                sending,
                all_xlsx,
                high_xlsx,
                prep_xlsx,
                deduped_xlsx,
                candidates_jsonl,
                review_jsonl,
                reviewed_xlsx,
                keep_xlsx,
            ):
                path.touch()

            summary_path = temp_root / "run" / "summary.json"
            summary_path.write_text(
                json.dumps(
                    {
                        "matching_strategy": "legacy-enrichment",
                        "steps": {
                            "task_assets": {
                                "artifacts": {
                                    "template_workbook": str(template),
                                    "sending_list_workbook": str(sending),
                                }
                            },
                            "enrichment": {
                                "artifacts": {
                                    "all_xlsx": str(all_xlsx),
                                    "high_xlsx": str(high_xlsx),
                                }
                            },
                            "llm_candidates": {
                                "artifacts": {
                                    "prep_xlsx": str(prep_xlsx),
                                    "deduped_xlsx": str(deduped_xlsx),
                                    "llm_candidates_jsonl": str(candidates_jsonl),
                                }
                            },
                            "llm_review": {
                                "artifacts": {
                                    "review_jsonl": str(review_jsonl),
                                    "reviewed_xlsx": str(reviewed_xlsx),
                                    "keep_xlsx": str(keep_xlsx),
                                }
                            },
                        }
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            summary = task_runner.run_task_upload_to_keep_list_pipeline(
                task_name="MINISO",
                env_file=env_path,
                output_root=temp_root / "run",
                summary_json=summary_path,
                stop_after="keep-list",
                reuse_existing=True,
                matching_strategy="legacy-enrichment",
            )

        self.assertEqual(summary["steps"]["task_assets"]["status"], "reused")
        self.assertEqual(summary["steps"]["task_assets"]["execution_mode"], "reused")
        self.assertEqual(summary["steps"]["mail_sync"]["execution_mode"], "rerun")
        self.assertEqual(summary["steps"]["enrichment"]["status"], "reused")
        self.assertEqual(summary["steps"]["enrichment"]["execution_mode"], "reused")
        self.assertEqual(summary["steps"]["llm_candidates"]["status"], "reused")
        self.assertEqual(summary["steps"]["llm_review"]["status"], "reused")
        self.assertTrue(summary["resume_context"]["existing_summary_accepted"])
        self.assertTrue(summary["resume_context"]["downstream_reuse_allowed"])
        self.assertEqual(summary["resume_context"]["downstream_reuse_reason"], "upstream_inputs_unchanged")
        self.assertEqual(summary["resolved_inputs"]["mail_sync"]["imap_host_source"], "default")
        self.assertEqual(enrich_calls["count"], 0)
        self.assertEqual(candidate_calls["count"], 0)
        self.assertEqual(review_calls["count"], 0)

    def test_runner_reruns_downstream_when_mail_sync_fetches_new_mail(self) -> None:
        class FakeClient:
            pass

        class FakeDb:
            def __init__(self, db_path):
                self.db_path = Path(db_path)

            def close(self):
                return None

        enrich_calls = {"count": 0}
        candidate_calls = {"count": 0}
        review_calls = {"count": 0}

        def fake_load_local_env(env_file):
            return {
                "FEISHU_APP_ID": "app-id",
                "FEISHU_APP_SECRET": "app-secret",
                "TASK_UPLOAD_URL": "https://env.example/task",
                "EMPLOYEE_INFO_URL": "https://env.example/employee",
            }

        def fake_get_preferred_value(cli_value, env_values, env_key, default=""):
            return str(cli_value or "").strip() or str(env_values.get(env_key, default) or "").strip()

        def fake_download_task_upload_screening_assets(**kwargs):
            raise AssertionError("task assets should still be reused")

        def fake_sync_task_upload_mailboxes(**kwargs):
            mail_root = Path(kwargs["mail_data_dir"])
            db_path = mail_root / "MINISO" / "email_sync.db"
            raw_dir = mail_root / "MINISO" / "raw"
            db_path.parent.mkdir(parents=True, exist_ok=True)
            db_path.touch()
            raw_dir.mkdir(parents=True, exist_ok=True)
            return {
                "selectedCount": 1,
                "syncedCount": 1,
                "failedCount": 0,
                "items": [
                    {
                        "taskName": "MINISO",
                        "employeeName": "Alice",
                        "resolvedFolder": "其他文件夹/MINISO",
                        "mailFetchedCount": 2,
                        "mailSyncOk": True,
                        "mailSyncError": "",
                        "mailDbPath": str(db_path),
                        "mailRawDir": str(raw_dir),
                        "mailDataDir": str(db_path.parent),
                    }
                ],
            }

        def fake_enrich_creator_workbook(*, db, input_path, output_prefix):
            enrich_calls["count"] += 1
            all_xlsx = output_prefix.with_suffix(".xlsx")
            high_xlsx = output_prefix.with_name(f"{output_prefix.name}_高置信").with_suffix(".xlsx")
            all_csv = output_prefix.with_suffix(".csv")
            high_csv = output_prefix.with_name(f"{output_prefix.name}_高置信").with_suffix(".csv")
            for path in (all_xlsx, high_xlsx, all_csv, high_csv):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.touch()
            return {
                "source_kind": "sending_list",
                "rows": 10,
                "matched_rows": 8,
                "high_confidence_rows": 4,
                "csv_path": str(all_csv),
                "xlsx_path": str(all_xlsx),
                "high_csv_path": str(high_csv),
                "high_xlsx_path": str(high_xlsx),
            }

        def fake_prepare_llm_review_candidates(*, db, input_path, output_prefix):
            candidate_calls["count"] += 1
            prep_xlsx = output_prefix.with_suffix(".xlsx")
            deduped_xlsx = output_prefix.with_name(f"{output_prefix.name}_去重").with_suffix(".xlsx")
            jsonl_path = output_prefix.with_name(f"{output_prefix.name}_llm_candidates").with_suffix(".jsonl")
            for path in (prep_xlsx, deduped_xlsx, jsonl_path):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.touch()
            return {
                "source_row_count": 4,
                "prep_row_count": 4,
                "deduped_row_count": 3,
                "llm_candidate_group_count": 2,
                "prep_xlsx_path": str(prep_xlsx),
                "deduped_xlsx_path": str(deduped_xlsx),
                "llm_candidates_jsonl_path": str(jsonl_path),
            }

        def fake_run_and_apply_llm_review(*, input_prefix, env_path, base_url, api_key, model, wire_api):
            review_calls["count"] += 1
            review_jsonl = input_prefix.with_name(f"{input_prefix.name}_llm_review").with_suffix(".jsonl")
            reviewed_xlsx = input_prefix.with_name(f"{input_prefix.name}_llm_reviewed").with_suffix(".xlsx")
            keep_xlsx = input_prefix.with_name(f"{input_prefix.name}_llm_reviewed_keep").with_suffix(".xlsx")
            for path in (review_jsonl, reviewed_xlsx, keep_xlsx):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.touch()
            return {
                "review_group_count": 2,
                "reviewed_row_count": 3,
                "keep_row_count": 2,
                "llm_review_jsonl_path": str(review_jsonl),
                "llm_reviewed_xlsx_path": str(reviewed_xlsx),
                "llm_reviewed_keep_xlsx_path": str(keep_xlsx),
            }

        task_runner._load_runtime_dependencies = lambda: {
            "Settings": object,
            "Database": FakeDb,
            "FeishuOpenClient": lambda **kwargs: FakeClient(),
            "DEFAULT_FEISHU_BASE_URL": "https://open.feishu.cn",
            "download_task_upload_screening_assets": fake_download_task_upload_screening_assets,
            "sync_task_upload_mailboxes": fake_sync_task_upload_mailboxes,
            "match_brand_keyword": lambda **kwargs: (_ for _ in ()).throw(AssertionError("fast path should not run")),
            "resolve_shared_email_candidates": lambda **kwargs: (_ for _ in ()).throw(AssertionError("fast path should not run")),
            "run_shared_email_final_review": lambda **kwargs: (_ for _ in ()).throw(AssertionError("fast path should not run")),
            "enrich_creator_workbook": fake_enrich_creator_workbook,
            "prepare_llm_review_candidates": fake_prepare_llm_review_candidates,
            "run_and_apply_llm_review": fake_run_and_apply_llm_review,
            "resolve_sync_sent_since": lambda value: __import__("datetime").date(2025, 12, 27),
            "load_local_env": fake_load_local_env,
            "get_preferred_value": fake_get_preferred_value,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            env_path = self._write_env_file(temp_root)
            downloads = temp_root / "run" / "downloads"
            exports = temp_root / "run" / "exports"
            downloads.mkdir(parents=True, exist_ok=True)
            exports.mkdir(parents=True, exist_ok=True)

            template = downloads / "template.xlsx"
            sending = downloads / "sending.xlsx"
            all_xlsx = exports / "miniso_all.xlsx"
            high_xlsx = exports / "miniso_high.xlsx"
            prep_xlsx = exports / "miniso_prep.xlsx"
            deduped_xlsx = exports / "miniso_deduped.xlsx"
            candidates_jsonl = exports / "miniso_candidates.jsonl"
            review_jsonl = exports / "miniso_review.jsonl"
            reviewed_xlsx = exports / "miniso_reviewed.xlsx"
            keep_xlsx = exports / "miniso_keep.xlsx"
            for path in (
                template,
                sending,
                all_xlsx,
                high_xlsx,
                prep_xlsx,
                deduped_xlsx,
                candidates_jsonl,
                review_jsonl,
                reviewed_xlsx,
                keep_xlsx,
            ):
                path.touch()

            summary_path = temp_root / "run" / "summary.json"
            summary_path.write_text(
                json.dumps(
                    {
                        "matching_strategy": "legacy-enrichment",
                        "steps": {
                            "task_assets": {
                                "artifacts": {
                                    "template_workbook": str(template),
                                    "sending_list_workbook": str(sending),
                                }
                            },
                            "enrichment": {
                                "artifacts": {
                                    "all_xlsx": str(all_xlsx),
                                    "high_xlsx": str(high_xlsx),
                                }
                            },
                            "llm_candidates": {
                                "artifacts": {
                                    "prep_xlsx": str(prep_xlsx),
                                    "deduped_xlsx": str(deduped_xlsx),
                                    "llm_candidates_jsonl": str(candidates_jsonl),
                                }
                            },
                            "llm_review": {
                                "artifacts": {
                                    "review_jsonl": str(review_jsonl),
                                    "reviewed_xlsx": str(reviewed_xlsx),
                                    "keep_xlsx": str(keep_xlsx),
                                }
                            },
                        }
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            summary = task_runner.run_task_upload_to_keep_list_pipeline(
                task_name="MINISO",
                env_file=env_path,
                output_root=temp_root / "run",
                summary_json=summary_path,
                stop_after="keep-list",
                reuse_existing=True,
                matching_strategy="legacy-enrichment",
            )

        self.assertEqual(summary["steps"]["task_assets"]["status"], "reused")
        self.assertEqual(summary["steps"]["enrichment"]["status"], "completed")
        self.assertEqual(summary["steps"]["enrichment"]["execution_mode"], "rerun")
        self.assertEqual(summary["steps"]["llm_candidates"]["execution_mode"], "rerun")
        self.assertEqual(summary["steps"]["llm_review"]["execution_mode"], "rerun")
        self.assertFalse(summary["resume_context"]["downstream_reuse_allowed"])
        self.assertEqual(summary["resume_context"]["downstream_reuse_reason"], "mail_sync_fetched_new_mail")
        self.assertEqual(enrich_calls["count"], 1)
        self.assertEqual(candidate_calls["count"], 1)
        self.assertEqual(review_calls["count"], 1)

    def test_runner_can_use_brand_keyword_fast_path(self) -> None:
        class FakeClient:
            pass

        class FakeDb:
            def __init__(self, db_path):
                self.db_path = Path(db_path)

            def close(self):
                return None

        def fake_load_local_env(env_file):
            return {
                "FEISHU_APP_ID": "app-id",
                "FEISHU_APP_SECRET": "app-secret",
                "TASK_UPLOAD_URL": "https://env.example/task",
                "EMPLOYEE_INFO_URL": "https://env.example/employee",
                "TIMEOUT_SECONDS": "30",
            }

        def fake_get_preferred_value(cli_value, env_values, env_key, default=""):
            return str(cli_value or "").strip() or str(env_values.get(env_key, default) or "").strip()

        def fake_download_task_upload_screening_assets(**kwargs):
            download_dir = Path(kwargs["download_dir"])
            template_path = download_dir / "miniso_template.xlsx"
            sending_list_path = download_dir / "miniso_sending_list.xlsx"
            template_path.parent.mkdir(parents=True, exist_ok=True)
            template_path.touch()
            sending_list_path.touch()
            return {
                "recordId": "rec123",
                "taskName": "MINISO",
                "linkedBitableUrl": "https://bitable.example/miniso",
                "templateDownloadedPath": str(template_path),
                "sendingListDownloadedPath": str(sending_list_path),
            }

        def fake_sync_task_upload_mailboxes(**kwargs):
            mail_root = Path(kwargs["mail_data_dir"])
            db_path = mail_root / "MINISO" / "email_sync.db"
            raw_dir = mail_root / "MINISO" / "raw"
            db_path.parent.mkdir(parents=True, exist_ok=True)
            db_path.touch()
            raw_dir.mkdir(parents=True, exist_ok=True)
            return {
                "selectedCount": 1,
                "syncedCount": 1,
                "failedCount": 0,
                "items": [
                    {
                        "taskName": "MINISO",
                        "employeeName": "Alice",
                        "resolvedFolder": "其他文件夹/MINISO",
                        "mailFetchedCount": 3,
                        "mailSyncOk": True,
                        "mailSyncError": "",
                        "mailDbPath": str(db_path),
                        "mailRawDir": str(raw_dir),
                        "mailDataDir": str(db_path.parent),
                    }
                ],
            }

        def fake_match_brand_keyword(*, db, input_path, output_prefix, keyword, sent_since, include_from):
            all_xlsx = output_prefix.with_suffix(".xlsx")
            deduped_xlsx = output_prefix.with_name(f"{output_prefix.name}_deduped").with_suffix(".xlsx")
            unique_xlsx = output_prefix.with_name(f"{output_prefix.name}_unique_email").with_suffix(".xlsx")
            shared_xlsx = output_prefix.with_name(f"{output_prefix.name}_shared_email").with_suffix(".xlsx")
            for path in (all_xlsx, deduped_xlsx, unique_xlsx, shared_xlsx):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.touch()
            return {
                "source_kind": "sending_list",
                "message_hit_count": 20,
                "matched_email_count": 10,
                "email_direct_match_row_count": 8,
                "profile_deduped_row_count": 6,
                "unique_email_row_count": 4,
                "shared_email_row_count": 2,
                "shared_email_group_count": 1,
                "xlsx_path": str(all_xlsx),
                "deduped_xlsx_path": str(deduped_xlsx),
                "unique_xlsx_path": str(unique_xlsx),
                "shared_xlsx_path": str(shared_xlsx),
            }

        def fake_resolve_shared_email_candidates(*, db, input_path, output_prefix):
            resolved_xlsx = output_prefix.with_name(f"{output_prefix.name}_resolved").with_suffix(".xlsx")
            unresolved_xlsx = output_prefix.with_name(f"{output_prefix.name}_unresolved").with_suffix(".xlsx")
            llm_candidates = output_prefix.with_name(f"{output_prefix.name}_llm_candidates").with_suffix(".jsonl")
            for path in (resolved_xlsx, unresolved_xlsx, llm_candidates):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.touch()
            return {
                "resolved_xlsx_path": str(resolved_xlsx),
                "unresolved_xlsx_path": str(unresolved_xlsx),
                "llm_candidates_jsonl_path": str(llm_candidates),
                "resolved_group_count": 1,
                "resolved_row_count": 1,
                "unresolved_group_count": 1,
                "unresolved_row_count": 2,
                "llm_candidate_group_count": 1,
            }

        def fake_run_shared_email_final_review(*, input_prefix, env_path, auto_keep_paths, base_url, api_key, model, wire_api):
            llm_review = input_prefix.with_name(f"{input_prefix.name}_llm_review").with_suffix(".jsonl")
            llm_resolved = input_prefix.with_name(f"{input_prefix.name}_llm_resolved").with_suffix(".xlsx")
            manual_tail = input_prefix.with_name(f"{input_prefix.name}_manual_tail").with_suffix(".xlsx")
            final_keep = input_prefix.with_name(f"{input_prefix.name}_final_keep").with_suffix(".xlsx")
            for path in (llm_review, llm_resolved, manual_tail, final_keep):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.touch()
            return {
                "llm_review_jsonl_path": str(llm_review),
                "llm_resolved_xlsx_path": str(llm_resolved),
                "manual_tail_xlsx_path": str(manual_tail),
                "final_keep_xlsx_path": str(final_keep),
                "review_group_count": 1,
                "llm_resolved_row_count": 1,
                "manual_row_count": 1,
                "final_keep_row_count": 6,
                "retryable_failure_count": 1,
                "selected_provider": "Secondary",
                "selected_model": "qwen-max",
                "selected_wire_api": "responses",
                "provider_attempts": [
                    {
                        "candidate_stage": "primary",
                        "provider": "Primary",
                        "model": "gpt-5.4",
                        "wire_api": "responses",
                        "attempt_count": 1,
                        "success_count": 0,
                        "failure_count": 1,
                        "retryable_failure_count": 1,
                        "last_error": "SSLEOFError",
                    },
                    {
                        "candidate_stage": "secondary",
                        "provider": "Secondary",
                        "model": "qwen-max",
                        "wire_api": "responses",
                        "attempt_count": 1,
                        "success_count": 1,
                        "failure_count": 0,
                        "retryable_failure_count": 0,
                        "last_error": "",
                    },
                ],
                "absorbed_failures": [
                    {
                        "candidate_stage": "primary",
                        "provider": "Primary",
                        "model": "gpt-5.4",
                        "wire_api": "responses",
                        "error": "SSLEOFError",
                        "retryable": True,
                        "recovered_by_provider": "Secondary",
                        "recovered_by_model": "qwen-max",
                    }
                ],
            }

        def fail_legacy(*args, **kwargs):
            raise AssertionError("legacy path should not run under fast path strategy")

        task_runner._load_runtime_dependencies = lambda: {
            "Settings": object,
            "Database": FakeDb,
            "FeishuOpenClient": lambda **kwargs: FakeClient(),
            "DEFAULT_FEISHU_BASE_URL": "https://open.feishu.cn",
            "download_task_upload_screening_assets": fake_download_task_upload_screening_assets,
            "sync_task_upload_mailboxes": fake_sync_task_upload_mailboxes,
            "match_brand_keyword": fake_match_brand_keyword,
            "resolve_shared_email_candidates": fake_resolve_shared_email_candidates,
            "run_shared_email_final_review": fake_run_shared_email_final_review,
            "enrich_creator_workbook": fail_legacy,
            "prepare_llm_review_candidates": fail_legacy,
            "run_and_apply_llm_review": fail_legacy,
            "resolve_sync_sent_since": lambda value: __import__("datetime").date(2025, 12, 27),
            "load_local_env": fake_load_local_env,
            "get_preferred_value": fake_get_preferred_value,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            summary_path = temp_root / "run" / "summary.json"
            env_path = self._write_env_file(temp_root)
            summary = task_runner.run_task_upload_to_keep_list_pipeline(
                task_name="MINISO",
                env_file=env_path,
                output_root=temp_root / "run",
                summary_json=summary_path,
                matching_strategy="brand-keyword-fast-path",
                brand_keyword="MINISO",
                brand_match_include_from=True,
                stop_after="keep-list",
            )

        self.assertEqual(summary["status"], "stopped_after_keep-list")
        self.assertEqual(summary["matching_strategy"], "brand-keyword-fast-path")
        self.assertEqual(summary["steps"]["brand_match"]["status"], "completed")
        self.assertEqual(summary["steps"]["shared_resolution"]["status"], "completed")
        self.assertEqual(summary["steps"]["final_review"]["status"], "completed")
        self.assertEqual(summary["steps"]["final_review"]["selected_provider"], "Secondary")
        self.assertEqual(summary["steps"]["final_review"]["selected_model"], "qwen-max")
        self.assertEqual(summary["steps"]["final_review"]["stats"]["retryable_failure_count"], 1)
        self.assertEqual(len(summary["steps"]["final_review"]["provider_attempts"]), 2)
        self.assertEqual(len(summary["steps"]["final_review"]["absorbed_failures"]), 1)
        self.assertNotIn("enrichment", summary["steps"])
        self.assertTrue(summary["artifacts"]["keep_workbook"].endswith("_final_keep.xlsx"))
        self.assertTrue(summary["artifacts"]["manual_tail_xlsx"].endswith("_manual_tail.xlsx"))
        self.assertEqual(summary["resume_points"]["brand_match"]["shared_email_workbook"], summary["artifacts"]["brand_match_shared_xlsx"])


if __name__ == "__main__":
    unittest.main()
