from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from typing import Any

from harness.contract import RUN_CONTRACT_VERSION
import scripts.run_keep_list_screening_pipeline as keep_list_runner


class FakeResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code

    def get_json(self, silent=False):
        return self._payload


class FakeClient:
    def __init__(self, preflight=None, artifact_status_by_platform=None):
        self.probe_calls = []
        self.scrape_start_calls = []
        self.visual_start_calls = []
        self.positioning_start_calls = []
        self.preflight = json.loads(json.dumps(preflight or {}))
        self.artifact_status_by_platform = json.loads(json.dumps(artifact_status_by_platform or {}))

    def post(self, url, json=None):
        if url == "/api/vision/providers/probe":
            self.probe_calls.append(json or {})
            if str(self.preflight.get("status") or "") != "configured":
                return FakeResponse({
                    "success": False,
                    "error_code": self.preflight.get("error_code") or "VISION_PROVIDER_PREFLIGHT_FAILED",
                    "error": self.preflight.get("message") or "视觉模型预检未通过",
                    "vision_preflight": self.preflight,
                }, status_code=400)
            return FakeResponse({
                "success": True,
                "provider": self.preflight.get("preferred_provider") or "openai",
                "probe": {"success": True, "provider": self.preflight.get("preferred_provider") or "openai"},
                "vision_preflight": self.preflight,
            })
        if url == "/api/jobs/scrape":
            self.scrape_start_calls.append(json or {})
            return FakeResponse({"success": True, "job": {"id": "scrape-job-1"}})
        if url == "/api/jobs/visual-review":
            self.visual_start_calls.append(json or {})
            return FakeResponse({"success": True, "job": {"id": f"visual-job-{len(self.visual_start_calls)}"}})
        if url == "/api/jobs/positioning-card-analysis":
            self.positioning_start_calls.append(json or {})
            return FakeResponse({"success": True, "job": {"id": "positioning-job-1"}})
        raise AssertionError(f"unexpected POST {url}")

    def get(self, url):
        if url.endswith("/status"):
            platform = str(url).split("/api/artifacts/", 1)[-1].split("/", 1)[0]
            payload = {
                "success": True,
                "available": {"final_review": False},
                "saved_positioning_card_artifacts_available": False,
            }
            payload.update(self.artifact_status_by_platform.get(platform, {}))
            return FakeResponse(payload)
        raise AssertionError(f"unexpected GET {url}")


class FakeFlaskApp:
    def __init__(self, client):
        self._client = client
        self.config = {}

    def test_client(self):
        return self._client


class FakeBackendApp:
    PLATFORM_ACTORS = {"instagram": "actor", "tiktok": "actor", "youtube": "actor"}

    def __init__(self, preflight, routing_strategy="", channel_race=None, metadata=None, artifact_status_by_platform=None):
        self._preflight = json.loads(json.dumps(preflight))
        self._metadata = json.loads(json.dumps(metadata or {"instagram": {"alpha": {"handle": "alpha"}}}))
        self._client = FakeClient(preflight=preflight, artifact_status_by_platform=artifact_status_by_platform)
        self.app = FakeFlaskApp(self._client)
        self.DATA_DIR = "/original/data"
        self.CONFIG_DIR = "/original/config"
        self.TEMP_DIR = "/original/temp"
        self.UPLOAD_FOLDER = "/original/data/uploads"
        self.ACTIVE_RULESPEC_PATH = "/original/config/active_rulespec.json"
        self.ACTIVE_VISUAL_PROMPTS_PATH = "/original/config/active_visual_prompts.json"
        self.FIELD_MATCH_REPORT_PATH = "/original/config/field_match_report.json"
        self.MISSING_CAPABILITIES_PATH = "/original/config/missing_capabilities.json"
        self.REVIEW_NOTES_PATH = "/original/config/review_notes.md"
        self.APIFY_TOKEN_POOL_STATE_FILE = "/original/data/apify_token_pool_state.json"
        self.APIFY_BALANCE_CACHE_FILE = "/original/data/apify_balance_cache.json"
        self.APIFY_RUN_GUARDS_FILE = "/original/data/apify_run_guards.json"
        self.app.config["UPLOAD_FOLDER"] = self.UPLOAD_FOLDER
        self._routing_strategy = str(routing_strategy or "")
        self._channel_race = json.loads(json.dumps(channel_race or {}))

    def iso_now(self):
        return "2026-03-28T01:02:03Z"

    def load_upload_metadata(self, platform):
        return self._metadata.get(platform, {})

    def save_upload_metadata(self, platform, payload, replace=False):
        normalized_platform = str(platform or "").strip().lower()
        existing = {} if replace else json.loads(json.dumps(self._metadata.get(normalized_platform, {})))
        for key, value in dict(payload or {}).items():
            existing[str(key or "").strip()] = json.loads(json.dumps(value))
        self._metadata[normalized_platform] = existing

    def get_available_vision_provider_names(self, provider_name=None):
        names = list(self._preflight.get("runnable_provider_names") or [])
        requested = str(provider_name or "").strip().lower()
        if requested:
            return [name for name in names if name == requested]
        return names

    def build_vision_preflight(self, provider_name=None):
        payload = json.loads(json.dumps(self._preflight))
        requested = str(provider_name or "").strip().lower()
        if requested:
            payload["requested_provider"] = requested
            payload["preferred_provider"] = requested
        return payload

    def resolve_visual_review_routing_strategy(self, payload=None):
        return self._routing_strategy

    def run_probe_ranked_visual_provider_race(self, platform="instagram", cover_urls=None):
        return json.loads(json.dumps(self._channel_race))

    def write_json_file(self, path, payload):
        output_path = Path(path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    def resolve_positioning_card_analysis_targets(self, platform, payload):
        identifiers = payload.get("identifiers") or payload.get("usernames") or payload.get("profiles") or payload.get("urls") or []
        return [{"username": identifier} for identifier in identifiers]


class KeepListRunnerSummaryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.original_loader = keep_list_runner._load_runtime_dependencies

    def tearDown(self) -> None:
        keep_list_runner._load_runtime_dependencies = self.original_loader

    def _write_env_file(self, root: Path) -> Path:
        env_path = root / ".env"
        env_path.write_text("TEST_ONLY=1\n", encoding="utf-8")
        return env_path

    def test_runner_fails_early_when_keep_workbook_is_missing(self) -> None:
        keep_list_runner._load_runtime_dependencies = lambda: (_ for _ in ()).throw(AssertionError("runtime should not load"))

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            summary_path = temp_root / "run" / "summary.json"
            env_path = self._write_env_file(temp_root)
            summary = keep_list_runner.run_keep_list_screening_pipeline(
                keep_workbook=temp_root / "missing_keep.xlsx",
                env_file=env_path,
                output_root=temp_root / "run",
                summary_json=summary_path,
                platform_filters=["instagram"],
                skip_scrape=True,
            )
            persisted_summary = json.loads(summary_path.read_text(encoding="utf-8"))
            self.assertTrue(Path(summary["workflow_handoff_json"]).exists())
            workflow_handoff = json.loads(Path(summary["workflow_handoff_json"]).read_text(encoding="utf-8"))

        self.assertEqual(summary["status"], "failed")
        self.assertEqual(summary["contract_version"], RUN_CONTRACT_VERSION)
        self.assertEqual(summary["verdict"]["outcome"], "failed")
        self.assertEqual(summary["error_code"], "KEEP_WORKBOOK_MISSING")
        self.assertEqual(summary["failure"]["stage"], "preflight")
        self.assertEqual(summary["failure_layer"], "preflight")
        self.assertEqual(summary["failure_decision"]["category"], "input")
        self.assertEqual(summary["failure_decision"]["resolution_mode"], "manual_fix")
        self.assertTrue(summary["failure_decision"]["requires_manual_intervention"])
        self.assertFalse(summary["failure_decision"]["retryable"])
        self.assertFalse(summary["preflight"]["ready"])
        self.assertTrue(summary["setup"]["skipped"])
        self.assertFalse(summary["setup"]["completed"])
        self.assertEqual(summary["preflight"]["errors"][0]["error_code"], "KEEP_WORKBOOK_MISSING")
        self.assertFalse(Path(summary["task_spec_json"]).exists())
        self.assertFalse(workflow_handoff["task_spec_available"])
        self.assertEqual(workflow_handoff["failure"]["failure_layer"], "preflight")
        self.assertEqual(workflow_handoff["failure_decision"]["category"], "input")
        self.assertEqual(persisted_summary["failure"]["error_code"], "KEEP_WORKBOOK_MISSING")

    def test_runner_allows_missing_env_file_for_local_keep_and_template_inputs(self) -> None:
        backend_app = FakeBackendApp(
            {
                "status": "configured",
                "error_code": "",
                "message": "视觉模型已就绪：openai",
                "configured_provider_names": ["openai"],
                "runnable_provider_names": ["openai"],
                "providers": [{"name": "openai", "runnable": True}],
            }
        )

        def fake_prepare_screening_inputs(**kwargs):
            return {
                "prepared_at": "2026-03-28T01:02:03Z",
                "upload": {"stats": {"Instagram": 1}},
            }

        keep_list_runner._load_runtime_dependencies = lambda: {
            "backend_app": backend_app,
            "prepare_screening_inputs": fake_prepare_screening_inputs,
            "count_passed_profiles": lambda scrape_job: 0,
            "export_platform_artifacts": lambda client, platform, export_dir: {},
            "poll_job": lambda client, job_id, label, interval: {},
            "require_success": lambda response, label: response.get_json(),
            "reset_backend_runtime_state": lambda: None,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            missing_env = temp_root / "missing.env"
            keep_path = temp_root / "keep.xlsx"
            template_path = temp_root / "template.xlsx"
            keep_path.touch()
            template_path.touch()
            summary = keep_list_runner.run_keep_list_screening_pipeline(
                keep_workbook=keep_path,
                template_workbook=template_path,
                env_file=missing_env,
                output_root=temp_root / "run",
                platform_filters=["instagram"],
                skip_scrape=True,
            )

        self.assertEqual(summary["status"], "staged_only")
        self.assertEqual(summary["contract_version"], RUN_CONTRACT_VERSION)
        self.assertEqual(summary["verdict"]["outcome"], "completed")
        self.assertTrue(summary["preflight"]["ready"])
        self.assertTrue(summary["setup"]["completed"])
        self.assertFalse(summary["preflight"]["env_file_exists"])
        self.assertEqual(summary["env_file"], str(missing_env.resolve()))

    def test_runner_records_runtime_import_failure_before_staging(self) -> None:
        keep_list_runner._load_runtime_dependencies = lambda: (_ for _ in ()).throw(ModuleNotFoundError("backend.app"))

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            keep_path = temp_root / "keep.xlsx"
            keep_path.touch()
            env_path = self._write_env_file(temp_root)
            summary = keep_list_runner.run_keep_list_screening_pipeline(
                keep_workbook=keep_path,
                env_file=env_path,
                output_root=temp_root / "run",
                platform_filters=["instagram"],
                skip_scrape=True,
            )

        self.assertEqual(summary["status"], "failed")
        self.assertEqual(summary["verdict"]["outcome"], "failed")
        self.assertEqual(summary["error_code"], "SCREENING_RUNTIME_IMPORT_FAILED")
        self.assertEqual(summary["failure"]["stage"], "runtime_import")
        self.assertEqual(summary["failure_layer"], "runtime")
        self.assertEqual(summary["failure_decision"]["category"], "dependency")
        self.assertEqual(summary["failure_decision"]["resolution_mode"], "manual_fix")
        self.assertTrue(summary["failure_decision"]["requires_manual_intervention"])
        self.assertFalse(summary["failure_decision"]["retryable"])
        self.assertTrue(summary["preflight"]["ready"])
        self.assertEqual(summary["preflight"]["errors"], [])
        self.assertTrue(summary["setup"]["completed"])

    def test_summary_includes_vision_preflight_for_staging_only_run(self) -> None:
        preflight = {
            "status": "configured",
            "error_code": "",
            "message": "视觉模型已就绪：openai",
            "configured_provider_names": ["openai"],
            "runnable_provider_names": ["openai"],
            "providers": [{"name": "openai", "runnable": True}],
        }
        backend_app = FakeBackendApp(
            preflight,
            metadata={"tiktok": {"alpha": {"handle": "alpha", "profile_url": "https://www.tiktok.com/@alpha"}}},
        )

        def fake_prepare_screening_inputs(**kwargs):
            return {
                "prepared_at": "2026-03-28T01:02:03Z",
                "upload": {"stats": {"Instagram": 1}},
            }

        keep_list_runner._load_runtime_dependencies = lambda: {
            "backend_app": backend_app,
            "prepare_screening_inputs": fake_prepare_screening_inputs,
            "count_passed_profiles": lambda scrape_job: 0,
            "export_platform_artifacts": lambda client, platform, export_dir: {},
            "poll_job": lambda client, job_id, label, interval: {},
            "require_success": lambda response, label: response.get_json(),
            "reset_backend_runtime_state": lambda: None,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            keep_path = temp_root / "keep.xlsx"
            template_path = temp_root / "template.xlsx"
            keep_path.touch()
            template_path.touch()
            env_path = self._write_env_file(temp_root)

            summary = keep_list_runner.run_keep_list_screening_pipeline(
                keep_workbook=keep_path,
                template_workbook=template_path,
                env_file=env_path,
                output_root=temp_root / "run",
                platform_filters=["instagram"],
                skip_scrape=True,
            )
            task_spec = json.loads(Path(summary["task_spec_json"]).read_text(encoding="utf-8"))
            workflow_handoff = json.loads(Path(summary["workflow_handoff_json"]).read_text(encoding="utf-8"))

        self.assertEqual(summary["vision_preflight"]["status"], "configured")
        self.assertTrue(summary["run_id"])
        self.assertEqual(summary["run_root"], str((temp_root / "run").resolve()))
        self.assertEqual(summary["env_file_raw"], str(env_path))
        self.assertEqual(summary["env_file"], str(env_path.resolve()))
        self.assertEqual(summary["resolved_inputs"]["env_file"]["path"], str(env_path.resolve()))
        self.assertEqual(summary["resolved_config_sources"]["env_file"], "cli")
        self.assertEqual(summary["workflow_handoff_json"], str((temp_root / "run" / "workflow_handoff.json").resolve()))
        self.assertTrue(summary["setup"]["completed"])
        self.assertEqual(task_spec["scope"], "keep-list-screening")
        self.assertEqual(task_spec["canonical_boundary"], "screening")
        self.assertEqual(task_spec["intent"]["keep_workbook"], str(keep_path.resolve()))
        self.assertEqual(task_spec["intent"]["requested_platforms"], ["instagram"])
        self.assertEqual(task_spec["run"]["workflow_handoff_json"], summary["workflow_handoff_json"])
        self.assertTrue(task_spec["paths"]["staging_summary_json"].endswith("/staging_summary.json"))
        self.assertEqual(workflow_handoff["verdict"]["outcome"], "completed")
        self.assertEqual(workflow_handoff["recommended_action"], "consume_outputs")
        self.assertTrue(workflow_handoff["task_spec_available"])
        self.assertEqual(workflow_handoff["current_stage"], "instagram:platform_skipped")
        self.assertEqual(workflow_handoff["next_report_triggers"], [])
        self.assertFalse(workflow_handoff["resume"]["available"])
        self.assertEqual(workflow_handoff["intent_summary"]["controls"]["skip_scrape"], True)
        self.assertTrue(summary["resolved_inputs"]["keep_workbook"]["exists"])
        self.assertEqual(summary["resolved_inputs"]["keep_workbook"]["source"], "cli_or_default")
        self.assertEqual(summary["preflight"]["template_input_mode"], "template_workbook")
        self.assertEqual(summary["platforms"]["instagram"]["vision_preflight"]["status"], "configured")
        self.assertEqual(summary["platforms"]["instagram"]["status"], "skipped")
        self.assertEqual(summary["vision_probe"]["status"], "skipped")
        self.assertEqual(summary["vision_probe"]["reason"], "skip_scrape flag set")
        self.assertEqual(backend_app._client.probe_calls, [])

    def test_skip_scrape_does_not_fail_when_vision_preflight_is_unconfigured(self) -> None:
        preflight = {
            "status": "unconfigured",
            "error_code": "MISSING_VISION_CONFIG",
            "message": "缺少视觉模型配置。",
            "configured_provider_names": [],
            "runnable_provider_names": [],
            "providers": [],
        }
        backend_app = FakeBackendApp(
            preflight,
            metadata={"instagram": {"alpha": {"handle": "alpha"}}},
        )

        def fake_prepare_screening_inputs(**kwargs):
            return {
                "prepared_at": "2026-03-28T01:02:03Z",
                "upload": {"stats": {"Instagram": 1}},
            }

        keep_list_runner._load_runtime_dependencies = lambda: {
            "backend_app": backend_app,
            "prepare_screening_inputs": fake_prepare_screening_inputs,
            "count_passed_profiles": lambda scrape_job: 0,
            "export_platform_artifacts": lambda client, platform, export_dir: {},
            "poll_job": lambda client, job_id, label, interval: {},
            "require_success": lambda response, label: response.get_json(),
            "reset_backend_runtime_state": lambda: None,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            keep_path = temp_root / "keep.xlsx"
            template_path = temp_root / "template.xlsx"
            keep_path.touch()
            template_path.touch()
            env_path = self._write_env_file(temp_root)

            summary = keep_list_runner.run_keep_list_screening_pipeline(
                keep_workbook=keep_path,
                template_workbook=template_path,
                env_file=env_path,
                output_root=temp_root / "run",
                platform_filters=["instagram"],
                skip_scrape=True,
            )

        self.assertEqual(summary["status"], "staged_only")
        self.assertEqual(summary["verdict"]["outcome"], "completed")
        self.assertEqual(summary["vision_preflight"]["status"], "unconfigured")
        self.assertEqual(summary["vision_probe"]["status"], "skipped")
        self.assertEqual(summary["vision_probe"]["reason"], "skip_scrape flag set")
        self.assertNotIn("failure", summary)
        self.assertNotIn("failure_decision", summary)
        self.assertEqual(summary["platforms"]["instagram"]["status"], "staged_only")
        self.assertEqual(summary["platforms"]["instagram"]["visual_gate"]["preflight_status"], "unconfigured")
        self.assertEqual(backend_app._client.probe_calls, [])

    def test_runner_defaults_to_run_local_download_and_template_output_dirs(self) -> None:
        backend_app = FakeBackendApp(
            {
                "status": "configured",
                "error_code": "",
                "message": "视觉模型已就绪：openai",
                "configured_provider_names": ["openai"],
                "runnable_provider_names": ["openai"],
                "providers": [{"name": "openai", "runnable": True}],
            }
        )
        observed: dict[str, Any] = {}

        def fake_prepare_screening_inputs(**kwargs):
            observed.update(kwargs)
            return {
                "prepared_at": "2026-03-28T01:02:03Z",
                "upload": {"stats": {"Instagram": 1}},
            }

        keep_list_runner._load_runtime_dependencies = lambda: {
            "backend_app": backend_app,
            "prepare_screening_inputs": fake_prepare_screening_inputs,
            "count_passed_profiles": lambda scrape_job: 0,
            "export_platform_artifacts": lambda client, platform, export_dir: {},
            "poll_job": lambda client, job_id, label, interval: {},
            "require_success": lambda response, label: response.get_json(),
            "reset_backend_runtime_state": lambda: None,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            keep_path = temp_root / "keep.xlsx"
            template_path = temp_root / "template.xlsx"
            keep_path.touch()
            template_path.touch()
            env_path = self._write_env_file(temp_root)

            summary = keep_list_runner.run_keep_list_screening_pipeline(
                keep_workbook=keep_path,
                template_workbook=template_path,
                env_file=env_path,
                task_name="MINISO",
                platform_filters=["instagram"],
                skip_scrape=True,
            )

        self.assertIn("/temp/runs/keep_list_screening/", summary["run_root"])
        self.assertEqual(summary["output_root"], summary["run_root"])
        self.assertTrue(summary["setup"]["completed"])
        self.assertTrue(str(observed["task_download_dir"]).startswith(summary["run_root"]))
        self.assertTrue(str(observed["template_output_dir"]).startswith(summary["run_root"]))
        self.assertNotEqual(
            str(observed["template_output_dir"]),
            str(keep_list_runner.REPO_ROOT / "downloads" / "task_upload_attachments" / "parsed_outputs"),
        )

    def test_runner_restores_backend_runtime_dirs_between_runs(self) -> None:
        backend_app = FakeBackendApp(
            {
                "status": "configured",
                "error_code": "",
                "message": "视觉模型已就绪：openai",
                "configured_provider_names": ["openai"],
                "runnable_provider_names": ["openai"],
                "providers": [{"name": "openai", "runnable": True}],
            }
        )
        observed_snapshots: list[tuple[str, str, str]] = []

        def fake_snapshot_backend_runtime_state():
            observed_snapshots.append((backend_app.DATA_DIR, backend_app.CONFIG_DIR, backend_app.TEMP_DIR))
            return {
                "DATA_DIR": backend_app.DATA_DIR,
                "CONFIG_DIR": backend_app.CONFIG_DIR,
                "TEMP_DIR": backend_app.TEMP_DIR,
                "UPLOAD_FOLDER": backend_app.UPLOAD_FOLDER,
                "ACTIVE_RULESPEC_PATH": backend_app.ACTIVE_RULESPEC_PATH,
                "ACTIVE_VISUAL_PROMPTS_PATH": backend_app.ACTIVE_VISUAL_PROMPTS_PATH,
                "FIELD_MATCH_REPORT_PATH": backend_app.FIELD_MATCH_REPORT_PATH,
                "MISSING_CAPABILITIES_PATH": backend_app.MISSING_CAPABILITIES_PATH,
                "REVIEW_NOTES_PATH": backend_app.REVIEW_NOTES_PATH,
                "APIFY_TOKEN_POOL_STATE_FILE": backend_app.APIFY_TOKEN_POOL_STATE_FILE,
                "APIFY_BALANCE_CACHE_FILE": backend_app.APIFY_BALANCE_CACHE_FILE,
                "APIFY_RUN_GUARDS_FILE": backend_app.APIFY_RUN_GUARDS_FILE,
                "app_upload_folder": backend_app.app.config["UPLOAD_FOLDER"],
            }

        def fake_restore_backend_runtime_state(snapshot):
            backend_app.DATA_DIR = snapshot["DATA_DIR"]
            backend_app.CONFIG_DIR = snapshot["CONFIG_DIR"]
            backend_app.TEMP_DIR = snapshot["TEMP_DIR"]
            backend_app.UPLOAD_FOLDER = snapshot["UPLOAD_FOLDER"]
            backend_app.ACTIVE_RULESPEC_PATH = snapshot["ACTIVE_RULESPEC_PATH"]
            backend_app.ACTIVE_VISUAL_PROMPTS_PATH = snapshot["ACTIVE_VISUAL_PROMPTS_PATH"]
            backend_app.FIELD_MATCH_REPORT_PATH = snapshot["FIELD_MATCH_REPORT_PATH"]
            backend_app.MISSING_CAPABILITIES_PATH = snapshot["MISSING_CAPABILITIES_PATH"]
            backend_app.REVIEW_NOTES_PATH = snapshot["REVIEW_NOTES_PATH"]
            backend_app.APIFY_TOKEN_POOL_STATE_FILE = snapshot["APIFY_TOKEN_POOL_STATE_FILE"]
            backend_app.APIFY_BALANCE_CACHE_FILE = snapshot["APIFY_BALANCE_CACHE_FILE"]
            backend_app.APIFY_RUN_GUARDS_FILE = snapshot["APIFY_RUN_GUARDS_FILE"]
            backend_app.app.config["UPLOAD_FOLDER"] = snapshot["app_upload_folder"]

        def fake_prepare_screening_inputs(**kwargs):
            backend_app.DATA_DIR = str(kwargs["screening_data_dir"])
            backend_app.CONFIG_DIR = str(kwargs["config_dir"])
            backend_app.TEMP_DIR = str(kwargs["temp_dir"])
            backend_app.UPLOAD_FOLDER = str(Path(backend_app.DATA_DIR) / "uploads")
            backend_app.app.config["UPLOAD_FOLDER"] = backend_app.UPLOAD_FOLDER
            return {
                "prepared_at": "2026-03-28T01:02:03Z",
                "upload": {"stats": {"Instagram": 1}},
            }

        keep_list_runner._load_runtime_dependencies = lambda: {
            "backend_app": backend_app,
            "prepare_screening_inputs": fake_prepare_screening_inputs,
            "snapshot_backend_runtime_state": fake_snapshot_backend_runtime_state,
            "restore_backend_runtime_state": fake_restore_backend_runtime_state,
            "count_passed_profiles": lambda scrape_job: 0,
            "export_platform_artifacts": lambda client, platform, export_dir: {},
            "poll_job": lambda client, job_id, label, interval: {},
            "require_success": lambda response, label: response.get_json(),
            "reset_backend_runtime_state": lambda: None,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            env_path = self._write_env_file(temp_root)
            keep_one = temp_root / "keep_one.xlsx"
            template_one = temp_root / "template_one.xlsx"
            keep_two = temp_root / "keep_two.xlsx"
            template_two = temp_root / "template_two.xlsx"
            for path in (keep_one, template_one, keep_two, template_two):
                path.touch()

            first = keep_list_runner.run_keep_list_screening_pipeline(
                keep_workbook=keep_one,
                template_workbook=template_one,
                env_file=env_path,
                output_root=temp_root / "run_one",
                platform_filters=["instagram"],
                skip_scrape=True,
            )
            second = keep_list_runner.run_keep_list_screening_pipeline(
                keep_workbook=keep_two,
                template_workbook=template_two,
                env_file=env_path,
                output_root=temp_root / "run_two",
                platform_filters=["instagram"],
                skip_scrape=True,
            )

        self.assertEqual(first["status"], "staged_only")
        self.assertEqual(second["status"], "staged_only")
        self.assertEqual(observed_snapshots, [
            ("/original/data", "/original/config", "/original/temp"),
            ("/original/data", "/original/config", "/original/temp"),
        ])
        self.assertEqual(backend_app.DATA_DIR, "/original/data")
        self.assertEqual(backend_app.CONFIG_DIR, "/original/config")
        self.assertEqual(backend_app.TEMP_DIR, "/original/temp")
        self.assertEqual(backend_app.app.config["UPLOAD_FOLDER"], "/original/data/uploads")

    def test_runner_records_task_upload_url_source_from_env_file(self) -> None:
        backend_app = FakeBackendApp(
            {
                "status": "configured",
                "error_code": "",
                "message": "视觉模型已就绪：openai",
                "configured_provider_names": ["openai"],
                "runnable_provider_names": ["openai"],
                "providers": [{"name": "openai", "runnable": True}],
            }
        )

        def fake_prepare_screening_inputs(**kwargs):
            return {
                "prepared_at": "2026-03-28T01:02:03Z",
                "upload": {"stats": {"Instagram": 1}},
            }

        keep_list_runner._load_runtime_dependencies = lambda: {
            "backend_app": backend_app,
            "prepare_screening_inputs": fake_prepare_screening_inputs,
            "count_passed_profiles": lambda scrape_job: 0,
            "export_platform_artifacts": lambda client, platform, export_dir: {},
            "poll_job": lambda client, job_id, label, interval: {},
            "require_success": lambda response, label: response.get_json(),
            "reset_backend_runtime_state": lambda: None,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            env_path = temp_root / ".env"
            env_path.write_text("TASK_UPLOAD_URL=https://env.example/task\n", encoding="utf-8")
            keep_path = temp_root / "keep.xlsx"
            template_path = temp_root / "template.xlsx"
            keep_path.touch()
            template_path.touch()

            summary = keep_list_runner.run_keep_list_screening_pipeline(
                keep_workbook=keep_path,
                template_workbook=template_path,
                env_file=env_path,
                output_root=temp_root / "run",
                platform_filters=["instagram"],
                skip_scrape=True,
            )

        self.assertEqual(summary["resolved_config_sources"]["task_upload_url"], "env_file:TASK_UPLOAD_URL")

    def test_runner_uploads_combined_payload_to_feishu_when_rows_are_available(self) -> None:
        preflight = {
            "status": "configured",
            "error_code": "",
            "message": "视觉模型已就绪：openai",
            "configured_provider_names": ["openai"],
            "runnable_provider_names": ["openai"],
            "providers": [{"name": "openai", "runnable": True}],
        }
        backend_app = FakeBackendApp(preflight)
        observed: dict[str, Any] = {}

        def fake_prepare_screening_inputs(**kwargs):
            return {
                "prepared_at": "2026-03-28T01:02:03Z",
                "upload": {"stats": {"Instagram": 1}},
            }

        def fake_poll_job(client, job_id, label, interval):
            return {"status": "completed"}

        def fake_export_platform_artifacts(client, platform, export_dir):
            export_dir.mkdir(parents=True, exist_ok=True)
            final_review_path = export_dir / f"{platform}_final_review.xlsx"
            final_review_path.write_text("placeholder", encoding="utf-8")
            return {"final_review": str(final_review_path)}

        def fake_build_all_platforms_final_review_artifacts(**kwargs):
            payload_json_path = Path(kwargs["payload_json_path"])
            archive_dir = payload_json_path.parent / "feishu_upload_local_archive"
            archive_dir.mkdir(parents=True, exist_ok=True)
            payload_json_path.write_text(
                json.dumps(
                    {
                        "rows": [
                            {
                                "达人ID": "alpha",
                            "平台": "instagram",
                                "达人对接人": "陈俊仁",
                                "达人对接人_employee_id": "ou_test",
                            }
                        ]
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            skipped_json = archive_dir / "skipped_from_feishu_upload.json"
            skipped_xlsx = archive_dir / "skipped_from_feishu_upload.xlsx"
            skipped_json.write_text("{}", encoding="utf-8")
            skipped_xlsx.write_text("placeholder", encoding="utf-8")
            workbook_path = Path(kwargs["output_path"])
            workbook_path.write_text("placeholder", encoding="utf-8")
            return {
                "all_platforms_final_review": str(workbook_path),
                "all_platforms_upload_payload_json": str(payload_json_path),
                "all_platforms_upload_local_archive_dir": str(archive_dir),
                "all_platforms_upload_skipped_archive_json": str(skipped_json),
                "all_platforms_upload_skipped_archive_xlsx": str(skipped_xlsx),
                "row_count": 1,
                "source_row_count": 1,
                "skipped_row_count": 0,
            }

        def fake_upload_final_review_payload_to_bitable(client, **kwargs):
            observed.update(kwargs)
            archive_dir = Path(kwargs["payload_json_path"]).parent / "feishu_upload_local_archive"
            result_json = archive_dir / "feishu_bitable_upload_result.json"
            result_xlsx = archive_dir / "feishu_bitable_upload_result.xlsx"
            result_json.write_text(json.dumps({"ok": True}, ensure_ascii=False), encoding="utf-8")
            result_xlsx.write_text("placeholder", encoding="utf-8")
            return {
                "ok": True,
                "result_json_path": str(result_json),
                "result_xlsx_path": str(result_xlsx),
                "target_url": "https://example.com/base",
                "target_table_id": "tbl123",
                "target_table_name": "达人管理",
                "created_count": 1,
                "updated_count": 0,
                "failed_count": 0,
                "skipped_existing_count": 0,
            }

        keep_list_runner._load_runtime_dependencies = lambda: {
            "backend_app": backend_app,
            "DEFAULT_FEISHU_BASE_URL": "https://open.feishu.cn/open-apis",
            "FeishuOpenClient": lambda **kwargs: object(),
            "get_preferred_value": lambda cli_value, env_values, env_key, default="": str(env_values.get(env_key, default) or ""),
            "load_local_env": lambda env_file: {
                "FEISHU_APP_ID": "cli_app_id",
                "FEISHU_APP_SECRET": "cli_app_secret",
                "TIMEOUT_SECONDS": "30",
            },
            "build_all_platforms_final_review_artifacts": fake_build_all_platforms_final_review_artifacts,
            "collect_final_exports": lambda platforms: {"instagram": {"final_review": "/tmp/instagram_final_review.xlsx"}},
            "prepare_screening_inputs": fake_prepare_screening_inputs,
            "count_passed_profiles": lambda scrape_job: 1,
            "export_platform_artifacts": fake_export_platform_artifacts,
            "poll_job": fake_poll_job,
            "require_success": lambda response, label: response.get_json(),
            "reset_backend_runtime_state": lambda: None,
            "upload_final_review_payload_to_bitable": fake_upload_final_review_payload_to_bitable,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            env_path = self._write_env_file(temp_root)
            keep_path = temp_root / "keep.xlsx"
            template_path = temp_root / "template.xlsx"
            keep_path.touch()
            template_path.touch()
            summary = keep_list_runner.run_keep_list_screening_pipeline(
                keep_workbook=keep_path,
                template_workbook=template_path,
                env_file=env_path,
                output_root=temp_root / "run",
                platform_filters=["instagram"],
                task_name="MINISO",
                task_upload_url="https://example.com/task",
                task_owner_name="陈俊仁",
                task_owner_employee_id="ou_test",
                linked_bitable_url="https://example.com/base",
            )

        self.assertEqual(summary["status"], "completed")
        self.assertEqual(observed["task_name"], "MINISO")
        self.assertEqual(observed["task_upload_url"], "https://example.com/task")
        self.assertEqual(observed["linked_bitable_url"], "https://example.com/base")
        self.assertTrue(summary["artifacts"]["feishu_upload_result_json"].endswith("feishu_bitable_upload_result.json"))
        self.assertEqual(summary["artifacts"]["feishu_upload_created_count"], 1)
        self.assertEqual(summary["artifacts"]["feishu_upload_failed_count"], 0)

    def test_runner_keeps_completed_status_when_feishu_upload_has_partial_failures(self) -> None:
        preflight = {
            "status": "configured",
            "error_code": "",
            "message": "视觉模型已就绪：openai",
            "configured_provider_names": ["openai"],
            "runnable_provider_names": ["openai"],
            "providers": [{"name": "openai", "runnable": True}],
        }
        backend_app = FakeBackendApp(preflight)
        observed: dict[str, Any] = {}

        def fake_prepare_screening_inputs(**kwargs):
            return {
                "prepared_at": "2026-03-28T01:02:03Z",
                "upload": {"stats": {"Instagram": 1}},
            }

        def fake_poll_job(client, job_id, label, interval):
            return {"status": "completed"}

        def fake_export_platform_artifacts(client, platform, export_dir):
            export_dir.mkdir(parents=True, exist_ok=True)
            final_review_path = export_dir / f"{platform}_final_review.xlsx"
            final_review_path.write_text("placeholder", encoding="utf-8")
            return {"final_review": str(final_review_path)}

        def fake_build_all_platforms_final_review_artifacts(**kwargs):
            payload_json_path = Path(kwargs["payload_json_path"])
            archive_dir = payload_json_path.parent / "feishu_upload_local_archive"
            archive_dir.mkdir(parents=True, exist_ok=True)
            payload_json_path.write_text(
                json.dumps(
                    {
                        "rows": [
                            {
                                "达人ID": "alpha",
                                "平台": "instagram",
                                "__feishu_update_mode": "create_or_mail_only_update",
                            }
                        ]
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            skipped_json = archive_dir / "skipped_from_feishu_upload.json"
            skipped_xlsx = archive_dir / "skipped_from_feishu_upload.xlsx"
            skipped_json.write_text("{}", encoding="utf-8")
            skipped_xlsx.write_text("placeholder", encoding="utf-8")
            workbook_path = Path(kwargs["output_path"])
            workbook_path.write_text("placeholder", encoding="utf-8")
            return {
                "all_platforms_final_review": str(workbook_path),
                "all_platforms_upload_payload_json": str(payload_json_path),
                "all_platforms_upload_local_archive_dir": str(archive_dir),
                "all_platforms_upload_skipped_archive_json": str(skipped_json),
                "all_platforms_upload_skipped_archive_xlsx": str(skipped_xlsx),
                "row_count": 1,
                "source_row_count": 1,
                "skipped_row_count": 0,
            }

        def fake_upload_final_review_payload_to_bitable(client, **kwargs):
            observed.update(kwargs)
            observed["payload"] = json.loads(Path(kwargs["payload_json_path"]).read_text(encoding="utf-8"))
            archive_dir = Path(kwargs["payload_json_path"]).parent / "feishu_upload_local_archive"
            result_json = archive_dir / "feishu_bitable_upload_result.json"
            result_xlsx = archive_dir / "feishu_bitable_upload_result.xlsx"
            result_json.write_text(json.dumps({"ok": True}, ensure_ascii=False), encoding="utf-8")
            result_xlsx.write_text("placeholder", encoding="utf-8")
            return {
                "ok": True,
                "result_json_path": str(result_json),
                "result_xlsx_path": str(result_xlsx),
                "target_url": "https://example.com/base",
                "target_table_id": "tbl123",
                "target_table_name": "达人管理",
                "created_count": 1,
                "updated_count": 0,
                "failed_count": 1,
                "skipped_existing_count": 0,
                "created_rows": [
                    {
                        "status": "created",
                        "record_id": "rec_new",
                        "row": {"达人ID": "alpha", "平台": "instagram", "主页链接": "https://instagram.com/alpha"},
                    }
                ],
                "failed_rows": [
                    {
                        "status": "failed",
                        "record_id": "",
                        "row": {"达人ID": "beta", "平台": "instagram"},
                        "error": "URLFieldConvFail",
                    }
                ],
                "deduplicated_rows": [
                    {
                        "status": "deduplicated_in_payload",
                        "row": {"达人ID": "dup", "平台": "instagram"},
                        "record_key": "dup::instagram",
                        "error": "Payload 内部重复，已保留最后一条。",
                    }
                ],
                "duplicate_existing_groups": [
                    {
                        "record_key": "alpha::instagram",
                        "creator_id": "alpha",
                        "platform": "instagram",
                        "keep_record": {"record_id": "rec_keep", "fields": {}},
                        "duplicate_records": [{"record_id": "rec_dup", "fields": {}}],
                    }
                ],
            }

        keep_list_runner._load_runtime_dependencies = lambda: {
            "backend_app": backend_app,
            "DEFAULT_FEISHU_BASE_URL": "https://open.feishu.cn/open-apis",
            "FeishuOpenClient": lambda **kwargs: object(),
            "get_preferred_value": lambda cli_value, env_values, env_key, default="": str(env_values.get(env_key, default) or ""),
            "load_local_env": lambda env_file: {
                "FEISHU_APP_ID": "cli_app_id",
                "FEISHU_APP_SECRET": "cli_app_secret",
                "TIMEOUT_SECONDS": "30",
            },
            "build_all_platforms_final_review_artifacts": fake_build_all_platforms_final_review_artifacts,
            "collect_final_exports": lambda platforms: {"instagram": {"final_review": "/tmp/instagram_final_review.xlsx"}},
            "prepare_screening_inputs": fake_prepare_screening_inputs,
            "count_passed_profiles": lambda scrape_job: 1,
            "export_platform_artifacts": fake_export_platform_artifacts,
            "poll_job": fake_poll_job,
            "require_success": lambda response, label: response.get_json(),
            "reset_backend_runtime_state": lambda: None,
            "upload_final_review_payload_to_bitable": fake_upload_final_review_payload_to_bitable,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            env_path = self._write_env_file(temp_root)
            keep_path = temp_root / "keep.xlsx"
            template_path = temp_root / "template.xlsx"
            keep_path.touch()
            template_path.touch()
            summary = keep_list_runner.run_keep_list_screening_pipeline(
                keep_workbook=keep_path,
                template_workbook=template_path,
                env_file=env_path,
                output_root=temp_root / "run",
                platform_filters=["instagram"],
                task_name="MINISO",
                task_upload_url="https://example.com/task",
                task_owner_name="陈俊仁",
                task_owner_employee_id="ou_test",
                linked_bitable_url="https://example.com/base",
            )

        self.assertEqual(summary["status"], "completed")
        self.assertEqual(summary["verdict"]["outcome"], "completed")
        self.assertEqual(summary["artifacts"]["feishu_upload_created_count"], 1)
        self.assertEqual(summary["artifacts"]["feishu_upload_failed_count"], 1)
        self.assertEqual(summary["warnings"]["feishu_upload_partial_failure"]["failed_count"], 1)
        self.assertEqual(observed["payload"]["rows"][0]["__feishu_update_mode"], "create_or_update")
        self.assertTrue(summary["artifacts"]["success_report_xlsx"].endswith("success_report.xlsx"))
        self.assertTrue(summary["artifacts"]["error_report_xlsx"].endswith("error_report.xlsx"))

    def test_runner_fails_when_all_rows_are_locally_archived_before_upload(self) -> None:
        preflight = {
            "status": "configured",
            "error_code": "",
            "message": "视觉模型已就绪：openai",
            "configured_provider_names": ["openai"],
            "runnable_provider_names": ["openai"],
            "providers": [{"name": "openai", "runnable": True}],
        }
        backend_app = FakeBackendApp(
            preflight,
            metadata={"instagram": {"alpha": {"handle": "alpha"}}},
        )

        def fake_prepare_screening_inputs(**kwargs):
            return {
                "prepared_at": "2026-03-28T01:02:03Z",
                "upload": {"stats": {"Instagram": 1}},
            }

        def fake_poll_job(client, job_id, label, interval):
            return {"status": "completed"}

        def fake_export_platform_artifacts(client, platform, export_dir):
            export_dir.mkdir(parents=True, exist_ok=True)
            final_review_path = export_dir / f"{platform}_final_review.xlsx"
            final_review_path.write_text("placeholder", encoding="utf-8")
            return {"final_review": str(final_review_path)}

        def fake_build_all_platforms_final_review_artifacts(**kwargs):
            payload_json_path = Path(kwargs["payload_json_path"])
            archive_dir = payload_json_path.parent / "feishu_upload_local_archive"
            archive_dir.mkdir(parents=True, exist_ok=True)
            payload_json_path.write_text(json.dumps({"rows": []}, ensure_ascii=False), encoding="utf-8")
            skipped_json = archive_dir / "skipped_from_feishu_upload.json"
            skipped_xlsx = archive_dir / "skipped_from_feishu_upload.xlsx"
            skipped_json.write_text("{}", encoding="utf-8")
            skipped_xlsx.write_text("placeholder", encoding="utf-8")
            workbook_path = Path(kwargs["output_path"])
            workbook_path.write_text("placeholder", encoding="utf-8")
            return {
                "all_platforms_final_review": str(workbook_path),
                "all_platforms_upload_payload_json": str(payload_json_path),
                "all_platforms_upload_local_archive_dir": str(archive_dir),
                "all_platforms_upload_skipped_archive_json": str(skipped_json),
                "all_platforms_upload_skipped_archive_xlsx": str(skipped_xlsx),
                "row_count": 0,
                "source_row_count": 1,
                "skipped_row_count": 1,
            }

        keep_list_runner._load_runtime_dependencies = lambda: {
            "backend_app": backend_app,
            "build_all_platforms_final_review_artifacts": fake_build_all_platforms_final_review_artifacts,
            "collect_final_exports": lambda platforms: {"instagram": {"final_review": "/tmp/instagram_final_review.xlsx"}},
            "prepare_screening_inputs": fake_prepare_screening_inputs,
            "count_passed_profiles": lambda scrape_job: 1,
            "export_platform_artifacts": fake_export_platform_artifacts,
            "poll_job": fake_poll_job,
            "require_success": lambda response, label: response.get_json(),
            "reset_backend_runtime_state": lambda: None,
            "upload_final_review_payload_to_bitable": lambda client, **kwargs: (_ for _ in ()).throw(
                AssertionError("upload should not be called when payload is empty")
            ),
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            env_path = self._write_env_file(temp_root)
            keep_path = temp_root / "keep.xlsx"
            template_path = temp_root / "template.xlsx"
            keep_path.touch()
            template_path.touch()
            summary = keep_list_runner.run_keep_list_screening_pipeline(
                keep_workbook=keep_path,
                template_workbook=template_path,
                env_file=env_path,
                output_root=temp_root / "run",
                platform_filters=["instagram"],
                task_name="MINISO",
            )

        self.assertEqual(summary["status"], "failed")
        self.assertEqual(summary["error_code"], "FEISHU_UPLOAD_PAYLOAD_EMPTY")
        self.assertEqual(summary["failure"]["stage"], "feishu_upload")

    def test_runner_merges_adjacent_task_spec_and_upstream_summary_context_when_rerunning_downstream(self) -> None:
        preflight = {
            "status": "configured",
            "error_code": "",
            "message": "视觉模型已就绪：openai",
            "configured_provider_names": ["openai"],
            "runnable_provider_names": ["openai"],
            "providers": [{"name": "openai", "runnable": True}],
        }
        backend_app = FakeBackendApp(
            preflight,
            metadata={"instagram": {"alpha": {"handle": "alpha"}}},
        )
        observed: dict[str, Any] = {}

        def fake_prepare_screening_inputs(**kwargs):
            return {
                "prepared_at": "2026-03-28T01:02:03Z",
                "upload": {"stats": {"Instagram": 1}},
            }

        def fake_poll_job(client, job_id, label, interval):
            return {"status": "completed"}

        def fake_export_platform_artifacts(client, platform, export_dir):
            export_dir.mkdir(parents=True, exist_ok=True)
            final_review_path = export_dir / f"{platform}_final_review.xlsx"
            final_review_path.write_text("placeholder", encoding="utf-8")
            return {"final_review": str(final_review_path)}

        def fake_build_all_platforms_final_review_artifacts(**kwargs):
            observed["task_owner"] = kwargs["task_owner"]
            payload_json_path = Path(kwargs["payload_json_path"])
            archive_dir = payload_json_path.parent / "feishu_upload_local_archive"
            archive_dir.mkdir(parents=True, exist_ok=True)
            payload_json_path.write_text(
                json.dumps(
                    {
                        "task_owner": kwargs["task_owner"],
                        "rows": [{"达人ID": "alpha", "达人对接人": kwargs["task_owner"]["responsible_name"]}],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            workbook_path = Path(kwargs["output_path"])
            workbook_path.write_text("placeholder", encoding="utf-8")
            return {
                "all_platforms_final_review": str(workbook_path),
                "all_platforms_upload_payload_json": str(payload_json_path),
                "all_platforms_upload_local_archive_dir": str(archive_dir),
                "all_platforms_upload_skipped_archive_json": str(archive_dir / "skipped_from_feishu_upload.json"),
                "all_platforms_upload_skipped_archive_xlsx": str(archive_dir / "skipped_from_feishu_upload.xlsx"),
                "row_count": 1,
                "source_row_count": 1,
                "skipped_row_count": 0,
            }

        def fake_upload_final_review_payload_to_bitable(client, **kwargs):
            observed["upload"] = dict(kwargs)
            result_json = Path(kwargs["payload_json_path"]).parent / "feishu_upload_local_archive" / "feishu_bitable_upload_result.json"
            result_xlsx = result_json.with_suffix(".xlsx")
            result_json.parent.mkdir(parents=True, exist_ok=True)
            result_json.write_text("{}", encoding="utf-8")
            result_xlsx.write_text("placeholder", encoding="utf-8")
            return {
                "ok": True,
                "result_json_path": str(result_json),
                "result_xlsx_path": str(result_xlsx),
                "target_url": kwargs["linked_bitable_url"],
                "target_table_id": "tbl123",
                "target_table_name": "AI回信管理",
                "created_count": 1,
                "updated_count": 0,
                "failed_count": 0,
                "skipped_existing_count": 0,
            }

        keep_list_runner._load_runtime_dependencies = lambda: {
            "backend_app": backend_app,
            "DEFAULT_FEISHU_BASE_URL": "https://open.feishu.cn/open-apis",
            "FeishuOpenClient": lambda **kwargs: object(),
            "get_preferred_value": lambda cli_value, env_values, env_key, default="": str(env_values.get(env_key, default) or ""),
            "load_local_env": lambda env_file: {
                "FEISHU_APP_ID": "cli_app_id",
                "FEISHU_APP_SECRET": "cli_app_secret",
                "TIMEOUT_SECONDS": "30",
            },
            "build_all_platforms_final_review_artifacts": fake_build_all_platforms_final_review_artifacts,
            "collect_final_exports": lambda platforms: {"instagram": {"final_review": "/tmp/instagram_final_review.xlsx"}},
            "prepare_screening_inputs": fake_prepare_screening_inputs,
            "count_passed_profiles": lambda scrape_job: 1,
            "export_platform_artifacts": fake_export_platform_artifacts,
            "poll_job": fake_poll_job,
            "require_success": lambda response, label: response.get_json(),
            "reset_backend_runtime_state": lambda: None,
            "upload_final_review_payload_to_bitable": fake_upload_final_review_payload_to_bitable,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            env_path = self._write_env_file(temp_root)
            keep_path = temp_root / "task" / "upstream" / "exports" / "keep.xlsx"
            template_path = temp_root / "task" / "upstream" / "downloads" / "template.xlsx"
            keep_path.parent.mkdir(parents=True, exist_ok=True)
            template_path.parent.mkdir(parents=True, exist_ok=True)
            keep_path.touch()
            template_path.touch()
            inferred_spec = temp_root / "task" / "downstream" / "task_spec.json"
            inferred_spec.parent.mkdir(parents=True, exist_ok=True)
            inferred_spec.write_text(
                json.dumps(
                    {
                        "intent": {
                            "task_name": "Duet1",
                            "task_upload_url": "https://example.com/task-upload",
                        },
                        "task_owner": {
                            "task_owner_name": "Yvette",
                            "task_owner_employee_id": "ou_owner",
                            "task_owner_employee_record_id": "rec_owner",
                            "task_owner_employee_email": "yvette@amagency.biz",
                            "task_owner_owner_name": "yvette@amagency.biz",
                            "linked_bitable_url": "",
                        },
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            upstream_summary = temp_root / "task" / "upstream" / "summary.json"
            upstream_summary.parent.mkdir(parents=True, exist_ok=True)
            upstream_summary.write_text(
                json.dumps(
                    {
                        "steps": {
                            "task_assets": {
                                "linked_bitable_url": "https://example.com/base",
                            }
                        },
                        "downstream_handoff": {
                            "task_owner": {
                                "task_name": "Duet1",
                                "task_upload_url": "https://example.com/task-upload",
                                "linked_bitable_url": "https://example.com/base",
                            }
                        },
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            summary = keep_list_runner.run_keep_list_screening_pipeline(
                keep_workbook=keep_path,
                template_workbook=template_path,
                env_file=env_path,
                output_root=temp_root / "run",
                platform_filters=["instagram"],
            )

        self.assertEqual(summary["status"], "completed")
        self.assertEqual(observed["task_owner"]["responsible_name"], "Yvette")
        self.assertEqual(observed["task_owner"]["employee_id"], "ou_owner")
        self.assertEqual(observed["task_owner"]["linked_bitable_url"], "https://example.com/base")
        self.assertEqual(observed["task_owner"]["task_name"], "Duet1")
        self.assertEqual(observed["upload"]["linked_bitable_url"], "https://example.com/base")
        self.assertEqual(observed["upload"]["task_name"], "Duet1")
        self.assertEqual(observed["upload"]["task_upload_url"], "https://example.com/task-upload")
        self.assertEqual(summary["resolved_task_owner"]["task_owner_name"], "Yvette")

    def test_probe_ranked_summary_uses_channel_race_when_no_explicit_provider_is_requested(self) -> None:
        preflight = {
            "status": "configured",
            "error_code": "",
            "message": "视觉模型已就绪：openai",
            "preferred_provider": "openai",
            "configured_provider_names": ["openai", "reelx"],
            "runnable_provider_names": ["openai", "reelx"],
            "providers": [
                {"name": "openai", "runnable": True},
                {"name": "reelx", "runnable": True},
            ],
        }
        channel_race = {
            "strategy": "probe_ranked",
            "checked_at": "2026-03-29T00:00:00Z",
            "success": True,
            "selected_stage": "preferred_parallel",
            "selected_provider": "reelx",
            "selected_model": "qwen-vl-max",
            "candidates": [
                {"stage": "preferred", "group": "preferred", "provider": "openai", "model": "gpt-5.4", "ok": False},
                {"stage": "preferred_parallel", "group": "fallback", "provider": "reelx", "model": "qwen-vl-max", "ok": True},
                {"stage": "secondary", "group": "fallback", "provider": "reelx", "model": "gemini-3-flash-preview", "ok": True},
            ],
        }
        backend_app = FakeBackendApp(preflight, routing_strategy="probe_ranked", channel_race=channel_race)

        def fake_prepare_screening_inputs(**kwargs):
            return {
                "prepared_at": "2026-03-28T01:02:03Z",
                "upload": {"stats": {"Instagram": 1}},
            }

        keep_list_runner._load_runtime_dependencies = lambda: {
            "backend_app": backend_app,
            "prepare_screening_inputs": fake_prepare_screening_inputs,
            "count_passed_profiles": lambda scrape_job: 0,
            "export_platform_artifacts": lambda client, platform, export_dir: {},
            "poll_job": lambda client, job_id, label, interval: {},
            "require_success": lambda response, label: response.get_json(),
            "reset_backend_runtime_state": lambda: None,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            keep_path = temp_root / "keep.xlsx"
            template_path = temp_root / "template.xlsx"
            keep_path.touch()
            template_path.touch()
            env_path = self._write_env_file(temp_root)

            summary = keep_list_runner.run_keep_list_screening_pipeline(
                keep_workbook=keep_path,
                template_workbook=template_path,
                env_file=env_path,
                output_root=temp_root / "run",
                platform_filters=["instagram"],
                probe_vision_provider_only=True,
            )

        self.assertEqual(summary["status"], "vision_probe_only")
        self.assertEqual(summary["vision_probe"]["success"], True)
        self.assertEqual(summary["vision_probe"]["provider"], "reelx")
        self.assertEqual(summary["vision_probe"]["probe"]["model"], "qwen-vl-max")
        self.assertEqual(summary["vision_probe"]["channel_race"]["selected_provider"], "reelx")
        self.assertEqual(backend_app._client.probe_calls, [])

    def test_summary_records_preflight_reason_when_visual_is_not_runnable(self) -> None:
        preflight = {
            "status": "degraded",
            "error_code": "VISION_PROVIDER_PREFLIGHT_FAILED",
            "message": "视觉模型预检未通过：已检测到 provider key，但当前没有可运行 provider。请检查 base_url、api_style 和 model。",
            "configured_provider_names": ["openai"],
            "runnable_provider_names": [],
            "providers": [{"name": "openai", "runnable": False, "issues": ["invalid_base_url"]}],
        }
        backend_app = FakeBackendApp(preflight)

        def fake_prepare_screening_inputs(**kwargs):
            return {
                "prepared_at": "2026-03-28T01:02:03Z",
                "upload": {"stats": {"Instagram": 1}},
            }

        def fake_poll_job(client, job_id, label, interval):
            return {
                "id": job_id,
                "status": "completed",
                "result": {
                    "profile_reviews": [{"status": "Pass", "username": "alpha"}],
                },
            }

        def fake_export_platform_artifacts(client, platform, export_dir):
            return {"final_review": str(export_dir / f"{platform}_final_review.xlsx")}

        keep_list_runner._load_runtime_dependencies = lambda: {
            "backend_app": backend_app,
            "prepare_screening_inputs": fake_prepare_screening_inputs,
            "count_passed_profiles": lambda scrape_job: 1,
            "export_platform_artifacts": fake_export_platform_artifacts,
            "poll_job": fake_poll_job,
            "require_success": lambda response, label: response.get_json(),
            "reset_backend_runtime_state": lambda: None,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            keep_path = temp_root / "keep.xlsx"
            template_path = temp_root / "template.xlsx"
            keep_path.touch()
            template_path.touch()
            summary_path = temp_root / "run" / "summary.json"
            env_path = self._write_env_file(temp_root)

            summary = keep_list_runner.run_keep_list_screening_pipeline(
                keep_workbook=keep_path,
                template_workbook=template_path,
                env_file=env_path,
                output_root=temp_root / "run",
                summary_json=summary_path,
                platform_filters=["instagram"],
                skip_scrape=False,
                skip_visual=False,
            )
            persisted_summary = json.loads(summary_path.read_text(encoding="utf-8"))

        self.assertEqual(summary["status"], "vision_probe_failed")
        self.assertEqual(summary["verdict"]["outcome"], "failed")
        self.assertEqual(summary["vision_probe"]["error_code"], "VISION_PROVIDER_PREFLIGHT_FAILED")
        self.assertEqual(summary["vision_probe"]["vision_preflight"]["status"], "degraded")
        self.assertEqual(summary["failure"]["error_code"], "VISION_PROVIDER_PREFLIGHT_FAILED")
        self.assertEqual(summary["failure_decision"]["category"], "configuration")
        self.assertEqual(summary["failure_decision"]["resolution_mode"], "manual_fix")
        self.assertTrue(summary["failure_decision"]["requires_manual_intervention"])
        self.assertEqual(persisted_summary["vision_preflight"]["status"], "degraded")

    def test_runner_can_probe_and_target_specific_provider(self) -> None:
        preflight = {
            "status": "configured",
            "error_code": "",
            "message": "视觉模型已就绪：openai",
            "configured_provider_names": ["openai"],
            "runnable_provider_names": ["openai"],
            "preferred_provider": "openai",
            "providers": [{"name": "openai", "runnable": True}],
        }
        backend_app = FakeBackendApp(preflight)

        def fake_prepare_screening_inputs(**kwargs):
            return {
                "prepared_at": "2026-03-28T01:02:03Z",
                "upload": {"stats": {"Instagram": 1}},
            }

        def fake_poll_job(client, job_id, label, interval):
            if job_id == "scrape-job-1":
                return {
                    "id": job_id,
                    "status": "completed",
                    "result": {
                        "profile_reviews": [{"status": "Pass", "username": "alpha"}],
                    },
                }
            return {
                "id": job_id,
                "status": "completed",
                "result": {"success": True},
            }

        keep_list_runner._load_runtime_dependencies = lambda: {
            "backend_app": backend_app,
            "prepare_screening_inputs": fake_prepare_screening_inputs,
            "count_passed_profiles": lambda scrape_job: 1,
            "export_platform_artifacts": lambda client, platform, export_dir: {},
            "poll_job": fake_poll_job,
            "require_success": lambda response, label: response.get_json(),
            "reset_backend_runtime_state": lambda: None,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            keep_path = temp_root / "keep.xlsx"
            template_path = temp_root / "template.xlsx"
            keep_path.touch()
            template_path.touch()
            env_path = self._write_env_file(temp_root)
            summary = keep_list_runner.run_keep_list_screening_pipeline(
                keep_workbook=keep_path,
                template_workbook=template_path,
                env_file=env_path,
                output_root=temp_root / "run",
                platform_filters=["instagram"],
                vision_provider="openai",
            )

        self.assertEqual(summary["requested_vision_provider"], "openai")
        self.assertEqual(summary["vision_probe"]["success"], True)
        self.assertTrue(summary["resolved_inputs"]["output_dirs"]["output_root"]["exists"])
        self.assertTrue(summary["setup"]["completed"])
        self.assertEqual(summary["preflight"]["requested_platforms"], ["instagram"])
        self.assertEqual(summary["platforms"]["instagram"]["requested_vision_provider"], "openai")
        self.assertEqual(
            backend_app.app.test_client().probe_calls[0]["provider"],
            "openai",
        )
        self.assertEqual(
            backend_app.app.test_client().visual_start_calls[0]["payload"]["provider"],
            "openai",
        )

    def test_runner_prefers_staged_urls_for_tiktok_scrape_payload(self) -> None:
        backend_app = FakeBackendApp(
            {"status": "configured", "runnable_provider_names": [], "configured_provider_names": []},
            metadata={
                "tiktok": {
                    "alpha": {
                        "handle": "alpha",
                        "url": "https://tiktok.com/@alpha",
                    }
                }
            },
        )

        def fake_prepare_screening_inputs(**kwargs):
            return {
                "prepared_at": "2026-03-28T01:02:03Z",
                "upload": {"stats": {"TikTok": 1}},
            }

        def fake_poll_job(client, job_id, label, interval):
            return {
                "id": job_id,
                "status": "completed",
                "result": {
                    "profile_reviews": [{"status": "Pass", "username": "alpha"}],
                },
            }

        keep_list_runner._load_runtime_dependencies = lambda: {
            "backend_app": backend_app,
            "prepare_screening_inputs": fake_prepare_screening_inputs,
            "count_passed_profiles": lambda scrape_job: 1,
            "export_platform_artifacts": lambda client, platform, export_dir: {},
            "poll_job": fake_poll_job,
            "require_success": lambda response, label: response.get_json(),
            "reset_backend_runtime_state": lambda: None,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            keep_path = temp_root / "keep.xlsx"
            template_path = temp_root / "template.xlsx"
            keep_path.touch()
            template_path.touch()
            env_path = self._write_env_file(temp_root)

            summary = keep_list_runner.run_keep_list_screening_pipeline(
                keep_workbook=keep_path,
                template_workbook=template_path,
                env_file=env_path,
                output_root=temp_root / "run",
                platform_filters=["tiktok"],
                skip_visual=True,
            )

        self.assertEqual(
            summary["platforms"]["tiktok"]["requested_identifier_preview"],
            ["https://tiktok.com/@alpha"],
        )
        self.assertEqual(
            backend_app.app.test_client().scrape_start_calls[0]["payload"]["profiles"],
            ["https://tiktok.com/@alpha"],
        )
        self.assertTrue(
            backend_app.app.test_client().scrape_start_calls[0]["payload"]["excludePinnedPosts"]
        )

    def test_runner_stages_missing_tiktok_profiles_to_instagram_fallback(self) -> None:
        backend_app = FakeBackendApp(
            {"status": "configured", "runnable_provider_names": [], "configured_provider_names": []},
            metadata={
                "tiktok": {
                    "alpha": {
                        "handle": "alpha",
                        "url": "https://www.tiktok.com/@alpha",
                        "instagram_url": "https://www.instagram.com/alpha/",
                        "youtube_url": "https://www.youtube.com/@alpha",
                        "platform_attempt_order": "tiktok,instagram,youtube",
                    }
                },
                "instagram": {},
                "youtube": {},
            },
        )
        backend_app.screening = type(
            "ScreeningStub",
            (),
            {
                "build_canonical_profile_url": staticmethod(
                    lambda platform, handle: (
                        f"https://www.instagram.com/{handle}/"
                        if str(platform or "").strip().lower() == "instagram"
                        else f"https://www.youtube.com/@{handle}"
                        if str(platform or "").strip().lower() == "youtube"
                        else f"https://www.tiktok.com/@{handle}"
                    )
                )
            },
        )()

        def fake_prepare_screening_inputs(**kwargs):
            return {
                "prepared_at": "2026-03-28T01:02:03Z",
                "upload": {"stats": {"TikTok": 1, "Instagram": 0, "YouTube": 0}},
            }

        def fake_poll_job(client, job_id, label, interval):
            if label.startswith("tiktok scrape"):
                return {
                    "id": job_id,
                    "status": "completed",
                    "result": {
                        "profile_reviews": [{"status": "Missing", "username": "alpha", "reason": "not found"}],
                    },
                }
            if label.startswith("instagram scrape"):
                return {
                    "id": job_id,
                    "status": "completed",
                    "result": {
                        "profile_reviews": [{"status": "Pass", "username": "alpha"}],
                    },
                }
            return {"id": job_id, "status": "completed", "result": {}}

        keep_list_runner._load_runtime_dependencies = lambda: {
            "backend_app": backend_app,
            "prepare_screening_inputs": fake_prepare_screening_inputs,
            "count_passed_profiles": lambda scrape_job: 1 if "instagram" in json.dumps(scrape_job) else 0,
            "export_platform_artifacts": lambda client, platform, export_dir: {},
            "poll_job": fake_poll_job,
            "require_success": lambda response, label: response.get_json(),
            "reset_backend_runtime_state": lambda: None,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            keep_path = temp_root / "keep.xlsx"
            template_path = temp_root / "template.xlsx"
            keep_path.touch()
            template_path.touch()
            env_path = self._write_env_file(temp_root)

            summary = keep_list_runner.run_keep_list_screening_pipeline(
                keep_workbook=keep_path,
                template_workbook=template_path,
                env_file=env_path,
                output_root=temp_root / "run",
                platform_filters=["tiktok", "instagram", "youtube"],
                skip_visual=True,
                skip_positioning_card_analysis=True,
            )

        self.assertEqual(summary["platforms"]["tiktok"]["fallback"]["staged_count"], 1)
        self.assertEqual(summary["platforms"]["instagram"]["requested_identifier_preview"], ["alpha"])
        self.assertEqual(
            backend_app.app.test_client().scrape_start_calls[1]["platform"],
            "instagram",
        )

    def test_runner_stages_missing_tiktok_profiles_to_instagram_fallback_with_handle_only(self) -> None:
        backend_app = FakeBackendApp(
            {"status": "configured", "runnable_provider_names": [], "configured_provider_names": []},
            metadata={
                "tiktok": {
                    "alpha": {
                        "handle": "alpha",
                        "url": "https://www.tiktok.com/@alpha",
                    }
                },
                "instagram": {},
                "youtube": {},
            },
        )
        backend_app.screening = type(
            "ScreeningStub",
            (),
            {
                "build_canonical_profile_url": staticmethod(
                    lambda platform, handle: (
                        f"https://www.instagram.com/{handle}/"
                        if str(platform or "").strip().lower() == "instagram"
                        else f"https://www.youtube.com/@{handle}"
                        if str(platform or "").strip().lower() == "youtube"
                        else f"https://www.tiktok.com/@{handle}"
                    )
                )
            },
        )()

        def fake_prepare_screening_inputs(**kwargs):
            return {
                "prepared_at": "2026-03-28T01:02:03Z",
                "upload": {"stats": {"TikTok": 1, "Instagram": 0, "YouTube": 0}},
            }

        def fake_poll_job(client, job_id, label, interval):
            if label.startswith("tiktok scrape"):
                return {
                    "id": job_id,
                    "status": "completed",
                    "result": {
                        "profile_reviews": [{"status": "Missing", "username": "alpha", "reason": "not found"}],
                    },
                }
            if label.startswith("instagram scrape"):
                return {
                    "id": job_id,
                    "status": "completed",
                    "result": {
                        "profile_reviews": [{"status": "Pass", "username": "alpha"}],
                    },
                }
            return {"id": job_id, "status": "completed", "result": {}}

        keep_list_runner._load_runtime_dependencies = lambda: {
            "backend_app": backend_app,
            "prepare_screening_inputs": fake_prepare_screening_inputs,
            "count_passed_profiles": lambda scrape_job: 1 if "instagram" in json.dumps(scrape_job) else 0,
            "export_platform_artifacts": lambda client, platform, export_dir: {},
            "poll_job": fake_poll_job,
            "require_success": lambda response, label: response.get_json(),
            "reset_backend_runtime_state": lambda: None,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            keep_path = temp_root / "keep.xlsx"
            template_path = temp_root / "template.xlsx"
            keep_path.touch()
            template_path.touch()
            env_path = self._write_env_file(temp_root)

            summary = keep_list_runner.run_keep_list_screening_pipeline(
                keep_workbook=keep_path,
                template_workbook=template_path,
                env_file=env_path,
                output_root=temp_root / "run",
                platform_filters=["tiktok", "instagram", "youtube"],
                skip_visual=True,
                skip_positioning_card_analysis=True,
            )

        self.assertEqual(summary["platforms"]["tiktok"]["fallback"]["staged_count"], 1)
        self.assertEqual(summary["platforms"]["instagram"]["requested_identifier_preview"], ["alpha"])
        self.assertEqual(
            backend_app.app.test_client().scrape_start_calls[1]["payload"]["usernames"],
            ["alpha"],
        )

    def test_runner_stages_rejected_tiktok_profiles_to_instagram_fallback(self) -> None:
        backend_app = FakeBackendApp(
            {"status": "configured", "runnable_provider_names": [], "configured_provider_names": []},
            metadata={
                "tiktok": {
                    "alpha": {
                        "handle": "alpha",
                        "url": "https://www.tiktok.com/@alpha",
                    }
                },
                "instagram": {},
                "youtube": {},
            },
        )
        backend_app.screening = type(
            "ScreeningStub",
            (),
            {
                "build_canonical_profile_url": staticmethod(
                    lambda platform, handle: (
                        f"https://www.instagram.com/{handle}/"
                        if str(platform or "").strip().lower() == "instagram"
                        else f"https://www.youtube.com/@{handle}"
                        if str(platform or "").strip().lower() == "youtube"
                        else f"https://www.tiktok.com/@{handle}"
                    )
                )
            },
        )()

        def fake_prepare_screening_inputs(**kwargs):
            return {
                "prepared_at": "2026-03-28T01:02:03Z",
                "upload": {"stats": {"TikTok": 1, "Instagram": 0, "YouTube": 0}},
            }

        def fake_poll_job(client, job_id, label, interval):
            if label.startswith("tiktok scrape"):
                return {
                    "id": job_id,
                    "status": "completed",
                    "result": {
                        "profile_reviews": [{"status": "Reject", "username": "alpha", "reason": "播放量不达标"}],
                    },
                }
            if label.startswith("instagram scrape"):
                return {
                    "id": job_id,
                    "status": "completed",
                    "result": {
                        "profile_reviews": [{"status": "Pass", "username": "alpha"}],
                    },
                }
            return {"id": job_id, "status": "completed", "result": {}}

        keep_list_runner._load_runtime_dependencies = lambda: {
            "backend_app": backend_app,
            "prepare_screening_inputs": fake_prepare_screening_inputs,
            "count_passed_profiles": lambda scrape_job: 1 if "instagram" in json.dumps(scrape_job) else 0,
            "export_platform_artifacts": lambda client, platform, export_dir: {},
            "poll_job": fake_poll_job,
            "require_success": lambda response, label: response.get_json(),
            "reset_backend_runtime_state": lambda: None,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            keep_path = temp_root / "keep.xlsx"
            template_path = temp_root / "template.xlsx"
            keep_path.touch()
            template_path.touch()
            env_path = self._write_env_file(temp_root)

            summary = keep_list_runner.run_keep_list_screening_pipeline(
                keep_workbook=keep_path,
                template_workbook=template_path,
                env_file=env_path,
                output_root=temp_root / "run",
                platform_filters=["tiktok"],
                skip_visual=True,
                skip_positioning_card_analysis=True,
            )

        self.assertEqual(summary["platforms"]["tiktok"]["fallback"]["staged_count"], 1)
        self.assertEqual(summary["platforms"]["tiktok"]["fallback_candidate_count"], 1)
        self.assertEqual(summary["platforms"]["instagram"]["requested_identifier_preview"], ["alpha"])
        self.assertEqual(
            backend_app.app.test_client().scrape_start_calls[1]["platform"],
            "instagram",
        )

    def test_runner_expands_tiktok_request_to_fallback_platforms(self) -> None:
        backend_app = FakeBackendApp(
            {"status": "configured", "runnable_provider_names": [], "configured_provider_names": []},
            metadata={
                "tiktok": {
                    "alpha": {
                        "handle": "alpha",
                        "url": "https://www.tiktok.com/@alpha",
                    }
                },
                "instagram": {},
                "youtube": {},
            },
        )
        backend_app.screening = type(
            "ScreeningStub",
            (),
            {
                "build_canonical_profile_url": staticmethod(
                    lambda platform, handle: (
                        f"https://www.instagram.com/{handle}/"
                        if str(platform or "").strip().lower() == "instagram"
                        else f"https://www.youtube.com/@{handle}"
                        if str(platform or "").strip().lower() == "youtube"
                        else f"https://www.tiktok.com/@{handle}"
                    )
                )
            },
        )()

        def fake_prepare_screening_inputs(**kwargs):
            return {
                "prepared_at": "2026-03-28T01:02:03Z",
                "upload": {"stats": {"TikTok": 1, "Instagram": 0, "YouTube": 0}},
            }

        def fake_poll_job(client, job_id, label, interval):
            if label.startswith("tiktok scrape"):
                return {
                    "id": job_id,
                    "status": "completed",
                    "result": {
                        "profile_reviews": [{"status": "Missing", "username": "alpha", "reason": "not found"}],
                    },
                }
            if label.startswith("instagram scrape"):
                return {
                    "id": job_id,
                    "status": "completed",
                    "result": {
                        "profile_reviews": [{"status": "Pass", "username": "alpha"}],
                    },
                }
            return {"id": job_id, "status": "completed", "result": {}}

        keep_list_runner._load_runtime_dependencies = lambda: {
            "backend_app": backend_app,
            "prepare_screening_inputs": fake_prepare_screening_inputs,
            "count_passed_profiles": lambda scrape_job: 1 if "instagram" in json.dumps(scrape_job) else 0,
            "export_platform_artifacts": lambda client, platform, export_dir: {},
            "poll_job": fake_poll_job,
            "require_success": lambda response, label: response.get_json(),
            "reset_backend_runtime_state": lambda: None,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            keep_path = temp_root / "keep.xlsx"
            template_path = temp_root / "template.xlsx"
            keep_path.touch()
            template_path.touch()
            env_path = self._write_env_file(temp_root)

            summary = keep_list_runner.run_keep_list_screening_pipeline(
                keep_workbook=keep_path,
                template_workbook=template_path,
                env_file=env_path,
                output_root=temp_root / "run",
                platform_filters=["tiktok"],
                skip_visual=True,
                skip_positioning_card_analysis=True,
            )

        self.assertIn("instagram", summary["platforms"])
        self.assertEqual(summary["platforms"]["tiktok"]["fallback"]["staged_count"], 1)
        self.assertEqual(summary["platforms"]["instagram"]["requested_identifier_preview"], ["alpha"])
        self.assertEqual(
            backend_app.app.test_client().scrape_start_calls[1]["platform"],
            "instagram",
        )

    def test_runner_defers_blocked_tiktok_final_export_until_instagram_fallback_finishes(self) -> None:
        backend_app = FakeBackendApp(
            {"status": "configured", "runnable_provider_names": [], "configured_provider_names": []},
            metadata={
                "tiktok": {
                    "alpha": {
                        "handle": "alpha",
                        "url": "https://www.tiktok.com/@alpha",
                    },
                    "beta": {
                        "handle": "beta",
                        "url": "https://www.tiktok.com/@beta",
                    },
                },
                "instagram": {},
                "youtube": {},
            },
            artifact_status_by_platform={
                "tiktok": {"final_review_export_blocked": True},
                "instagram": {"final_review_export_blocked": False},
            },
        )
        backend_app.screening = type(
            "ScreeningStub",
            (),
            {
                "build_canonical_profile_url": staticmethod(
                    lambda platform, handle: (
                        f"https://www.instagram.com/{handle}/"
                        if str(platform or "").strip().lower() == "instagram"
                        else f"https://www.youtube.com/@{handle}"
                        if str(platform or "").strip().lower() == "youtube"
                        else f"https://www.tiktok.com/@{handle}"
                    )
                )
            },
        )()

        def fake_prepare_screening_inputs(**kwargs):
            return {
                "prepared_at": "2026-03-28T01:02:03Z",
                "upload": {"stats": {"TikTok": 2, "Instagram": 0, "YouTube": 0}},
            }

        def fake_poll_job(client, job_id, label, interval):
            if label.startswith("tiktok scrape"):
                return {
                    "id": job_id,
                    "status": "completed",
                    "result": {
                        "profile_reviews": [
                            {"status": "Missing", "username": "alpha", "reason": "not found"},
                            {"status": "Pass", "username": "beta"},
                        ],
                    },
                }
            if label.startswith("instagram scrape"):
                return {
                    "id": job_id,
                    "status": "completed",
                    "result": {
                        "profile_reviews": [{"status": "Pass", "username": "alpha"}],
                    },
                }
            return {"id": job_id, "status": "completed", "result": {}}

        export_calls: list[str] = []

        def fake_export_platform_artifacts(client, platform, export_dir):
            export_calls.append(str(platform))
            if str(platform) == "tiktok":
                raise AssertionError("tiktok final export should be deferred when fallback staging succeeded")
            return {}

        keep_list_runner._load_runtime_dependencies = lambda: {
            "backend_app": backend_app,
            "prepare_screening_inputs": fake_prepare_screening_inputs,
            "count_passed_profiles": lambda scrape_job: 1 if "Pass" in json.dumps(scrape_job) else 0,
            "export_platform_artifacts": fake_export_platform_artifacts,
            "poll_job": fake_poll_job,
            "require_success": lambda response, label: response.get_json(),
            "reset_backend_runtime_state": lambda: None,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            keep_path = temp_root / "keep.xlsx"
            template_path = temp_root / "template.xlsx"
            keep_path.touch()
            template_path.touch()
            env_path = self._write_env_file(temp_root)

            summary = keep_list_runner.run_keep_list_screening_pipeline(
                keep_workbook=keep_path,
                template_workbook=template_path,
                env_file=env_path,
                output_root=temp_root / "run",
                platform_filters=["tiktok"],
                skip_visual=True,
                skip_positioning_card_analysis=True,
            )

        self.assertEqual(summary["platforms"]["tiktok"]["fallback"]["staged_count"], 1)
        self.assertEqual(summary["platforms"]["tiktok"]["final_review_export"]["status"], "deferred")
        self.assertIn("instagram", summary["platforms"])
        self.assertEqual(summary["platforms"]["instagram"]["requested_identifier_preview"], ["alpha"])
        self.assertEqual(export_calls, ["instagram"])

    def test_runner_forwards_creator_cache_controls_to_scrape_and_visual_payloads(self) -> None:
        preflight = {
            "status": "configured",
            "error_code": "",
            "message": "视觉模型已就绪：openai",
            "configured_provider_names": ["openai"],
            "runnable_provider_names": ["openai"],
            "preferred_provider": "openai",
            "providers": [{"name": "openai", "runnable": True}],
        }
        backend_app = FakeBackendApp(preflight, metadata={"instagram": {"alpha": {"handle": "alpha"}}})

        def fake_prepare_screening_inputs(**kwargs):
            return {
                "prepared_at": "2026-03-28T01:02:03Z",
                "upload": {"stats": {"Instagram": 1}},
            }

        def fake_poll_job(client, job_id, label, interval):
            if job_id == "scrape-job-1":
                return {
                    "id": job_id,
                    "status": "completed",
                    "result": {
                        "profile_reviews": [{"status": "Pass", "username": "alpha"}],
                        "successful_identifiers": ["alpha"],
                    },
                }
            return {
                "id": job_id,
                "status": "completed",
                "result": {
                    "visual_results": {
                        "alpha": {"decision": "Pass", "reviewed_at": "2026-03-28T01:02:03Z"}
                    }
                },
            }

        keep_list_runner._load_runtime_dependencies = lambda: {
            "backend_app": backend_app,
            "prepare_screening_inputs": fake_prepare_screening_inputs,
            "count_passed_profiles": lambda scrape_job: 1,
            "export_platform_artifacts": lambda client, platform, export_dir: {},
            "poll_job": fake_poll_job,
            "require_success": lambda response, label: response.get_json(),
            "reset_backend_runtime_state": lambda: None,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            keep_path = temp_root / "keep.xlsx"
            template_path = temp_root / "template.xlsx"
            keep_path.touch()
            template_path.touch()
            env_path = self._write_env_file(temp_root)

            summary = keep_list_runner.run_keep_list_screening_pipeline(
                keep_workbook=keep_path,
                template_workbook=template_path,
                env_file=env_path,
                output_root=temp_root / "run",
                platform_filters=["instagram"],
                creator_cache_db_path="/tmp/creator-cache.db",
                force_refresh_creator_cache=True,
            )

        self.assertEqual(summary["status"], "completed")
        self.assertEqual(
            backend_app.app.test_client().scrape_start_calls[0]["payload"]["creator_cache_db_path"],
            "/tmp/creator-cache.db",
        )
        self.assertTrue(
            backend_app.app.test_client().scrape_start_calls[0]["payload"]["force_refresh_creator_cache"]
        )
        self.assertEqual(
            backend_app.app.test_client().visual_start_calls[0]["payload"]["creator_cache_db_path"],
            "/tmp/creator-cache.db",
        )
        self.assertTrue(
            backend_app.app.test_client().visual_start_calls[0]["payload"]["force_refresh_creator_cache"]
        )

    def test_runner_blocks_visual_and_export_when_scrape_contains_missing_profiles(self) -> None:
        preflight = {
            "status": "configured",
            "error_code": "",
            "message": "视觉模型已就绪：openai",
            "configured_provider_names": ["openai"],
            "runnable_provider_names": ["openai"],
            "preferred_provider": "openai",
            "providers": [{"name": "openai", "runnable": True}],
        }
        backend_app = FakeBackendApp(preflight)
        export_calls = []

        def fake_prepare_screening_inputs(**kwargs):
            return {
                "prepared_at": "2026-03-28T01:02:03Z",
                "upload": {"stats": {"Instagram": 1}},
            }

        def fake_poll_job(client, job_id, label, interval):
            return {
                "id": job_id,
                "status": "completed",
                "result": {
                    "profile_reviews": [
                        {"status": "Missing", "username": "ghost", "reason": "名单账号未在本次抓取结果中返回"},
                    ],
                },
            }

        def fake_export_platform_artifacts(client, platform, export_dir):
            export_calls.append(platform)
            return {}

        keep_list_runner._load_runtime_dependencies = lambda: {
            "backend_app": backend_app,
            "prepare_screening_inputs": fake_prepare_screening_inputs,
            "count_passed_profiles": lambda scrape_job: 0,
            "export_platform_artifacts": fake_export_platform_artifacts,
            "poll_job": fake_poll_job,
            "require_success": lambda response, label: response.get_json(),
            "reset_backend_runtime_state": lambda: None,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            keep_path = temp_root / "keep.xlsx"
            template_path = temp_root / "template.xlsx"
            keep_path.touch()
            template_path.touch()
            env_path = self._write_env_file(temp_root)

            summary = keep_list_runner.run_keep_list_screening_pipeline(
                keep_workbook=keep_path,
                template_workbook=template_path,
                env_file=env_path,
                output_root=temp_root / "run",
                platform_filters=["instagram"],
                vision_provider="openai",
            )

        platform_summary = summary["platforms"]["instagram"]
        self.assertEqual(summary["status"], "missing_profiles_blocked")
        self.assertEqual(summary["verdict"]["outcome"], "blocked")
        self.assertEqual(summary["failure"]["error_code"], "MISSING_PROFILES_BLOCKED")
        self.assertEqual(summary["failure_decision"]["category"], "input")
        self.assertTrue(summary["failure_decision"]["requires_manual_intervention"])
        self.assertEqual(platform_summary["status"], "missing_profiles_blocked")
        self.assertEqual(platform_summary["missing_profile_count"], 1)
        self.assertEqual(platform_summary["missing_profiles"][0]["identifier"], "ghost")
        self.assertTrue(platform_summary["visual_gate"]["blocked"])
        self.assertEqual(backend_app.app.test_client().visual_start_calls, [])
        self.assertEqual(export_calls, [])

    def test_runner_keeps_successful_rows_when_missing_profiles_have_no_fallback_contract(self) -> None:
        preflight = {
            "status": "configured",
            "error_code": "",
            "message": "视觉模型已就绪：openai",
            "configured_provider_names": ["openai"],
            "runnable_provider_names": ["openai"],
            "preferred_provider": "openai",
            "providers": [{"name": "openai", "runnable": True}],
        }
        backend_app = FakeBackendApp(preflight)
        export_calls = []

        def fake_prepare_screening_inputs(**kwargs):
            return {
                "prepared_at": "2026-03-28T01:02:03Z",
                "upload": {"stats": {"Instagram": 2}},
            }

        def fake_poll_job(client, job_id, label, interval):
            return {
                "id": job_id,
                "status": "completed",
                "result": {
                    "profile_reviews": [
                        {"status": "Pass", "username": "alpha"},
                        {"status": "Missing", "username": "ghost", "reason": "名单账号未在本次抓取结果中返回"},
                    ],
                },
            }

        def fake_export_platform_artifacts(client, platform, export_dir):
            export_calls.append(platform)
            return {"final_review": str(export_dir / f"{platform}_final_review.xlsx")}

        def fake_build_all_platforms_final_review_artifacts(**kwargs):
            output_path = Path(kwargs["output_path"])
            payload_path = Path(kwargs["payload_json_path"])
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.touch()
            payload_path.write_text(json.dumps({"rows": []}, ensure_ascii=False), encoding="utf-8")
            skipped_json = output_path.with_name("skipped_from_feishu_upload.json")
            skipped_xlsx = output_path.with_name("skipped_from_feishu_upload.xlsx")
            skipped_json.write_text("[]", encoding="utf-8")
            skipped_xlsx.touch()
            return {
                "all_platforms_final_review": str(output_path),
                "all_platforms_upload_payload_json": str(payload_path),
                "all_platforms_upload_local_archive_dir": str(output_path.parent / "archives"),
                "all_platforms_upload_skipped_archive_json": str(skipped_json),
                "all_platforms_upload_skipped_archive_xlsx": str(skipped_xlsx),
                "row_count": 0,
                "source_row_count": 0,
                "skipped_row_count": 0,
            }

        keep_list_runner._load_runtime_dependencies = lambda: {
            "backend_app": backend_app,
            "prepare_screening_inputs": fake_prepare_screening_inputs,
            "count_passed_profiles": lambda scrape_job: 1,
            "export_platform_artifacts": fake_export_platform_artifacts,
            "poll_job": fake_poll_job,
            "require_success": lambda response, label: response.get_json(),
            "reset_backend_runtime_state": lambda: None,
            "collect_final_exports": lambda platforms: dict(platforms or {}),
            "build_all_platforms_final_review_artifacts": fake_build_all_platforms_final_review_artifacts,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            keep_path = temp_root / "keep.xlsx"
            template_path = temp_root / "template.xlsx"
            keep_path.touch()
            template_path.touch()
            env_path = self._write_env_file(temp_root)

            summary = keep_list_runner.run_keep_list_screening_pipeline(
                keep_workbook=keep_path,
                template_workbook=template_path,
                env_file=env_path,
                output_root=temp_root / "run",
                platform_filters=["instagram"],
                vision_provider="openai",
                skip_visual=True,
                skip_positioning_card_analysis=True,
            )

        platform_summary = summary["platforms"]["instagram"]
        self.assertEqual(summary["status"], "completed_with_quality_warnings")
        self.assertEqual(summary["verdict"]["outcome"], "completed")
        self.assertEqual(platform_summary["status"], "completed")
        self.assertEqual(platform_summary["missing_profile_count"], 1)
        self.assertEqual(platform_summary["visual_job"]["status"], "skipped")
        self.assertEqual(summary["manual_review_rows"][0]["identifier"], "ghost")
        self.assertEqual(summary["manual_review_rows"][0]["platform"], "instagram")
        self.assertEqual(export_calls, ["instagram"])

    def test_runner_persists_live_platform_stage_before_scrape_poll_returns(self) -> None:
        preflight = {
            "status": "configured",
            "error_code": "",
            "message": "视觉模型已就绪：openai",
            "configured_provider_names": ["openai"],
            "runnable_provider_names": ["openai"],
            "preferred_provider": "openai",
            "providers": [{"name": "openai", "runnable": True}],
        }
        backend_app = FakeBackendApp(preflight)

        def fake_prepare_screening_inputs(**kwargs):
            return {
                "prepared_at": "2026-03-28T01:02:03Z",
                "upload": {"stats": {"Instagram": 1}},
            }

        observed_stages = []
        summary_path = None

        def fake_poll_job(client, job_id, label, interval):
            persisted_summary = json.loads(Path(summary_path).read_text(encoding="utf-8"))
            observed_stages.append(persisted_summary["platforms"]["instagram"]["current_stage"])
            return {
                "id": job_id,
                "status": "completed",
                "result": {
                    "profile_reviews": [{"status": "Reject", "username": "alpha"}],
                },
            }

        keep_list_runner._load_runtime_dependencies = lambda: {
            "backend_app": backend_app,
            "prepare_screening_inputs": fake_prepare_screening_inputs,
            "count_passed_profiles": lambda scrape_job: 0,
            "export_platform_artifacts": lambda client, platform, export_dir: {},
            "poll_job": fake_poll_job,
            "require_success": lambda response, label: response.get_json(),
            "reset_backend_runtime_state": lambda: None,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            keep_path = temp_root / "keep.xlsx"
            template_path = temp_root / "template.xlsx"
            keep_path.touch()
            template_path.touch()
            summary_path = temp_root / "run" / "summary.json"
            env_path = self._write_env_file(temp_root)

            summary = keep_list_runner.run_keep_list_screening_pipeline(
                keep_workbook=keep_path,
                template_workbook=template_path,
                env_file=env_path,
                output_root=temp_root / "run",
                summary_json=summary_path,
                platform_filters=["instagram"],
                skip_visual=True,
            )

        self.assertEqual(summary["platforms"]["instagram"]["status"], "completed")
        self.assertIn("scrape_running", observed_stages)

    def test_runner_salvages_partial_scrape_failure_and_marks_partial_completion(self) -> None:
        preflight = {
            "status": "configured",
            "error_code": "",
            "message": "视觉模型已就绪：openai",
            "configured_provider_names": ["openai"],
            "runnable_provider_names": ["openai"],
            "preferred_provider": "openai",
            "providers": [{"name": "openai", "runnable": True}],
        }
        backend_app = FakeBackendApp(preflight)

        def fake_prepare_screening_inputs(**kwargs):
            return {
                "prepared_at": "2026-03-28T01:02:03Z",
                "upload": {"stats": {"Instagram": 1}},
            }

        partial_result = {
            "platform": "instagram",
            "raw_count": 1,
            "profile_reviews": [{"status": "Pass", "username": "alpha"}],
            "successful_identifiers": ["alpha"],
        }

        def fake_poll_job(client, job_id, label, interval):
            if job_id == "scrape-job-1":
                return {
                    "id": job_id,
                    "status": "failed",
                    "stage": "poll",
                    "partial_result": partial_result,
                    "result": {
                        "success": False,
                        "error": "查询 Apify run 失败：HTTP 502 Bad Gateway",
                        "failure_stage": "poll",
                        "partial_result": partial_result,
                        "apify": {
                            "apify_run_id": "apify-run-1",
                            "apify_dataset_id": "dataset-1",
                            "guard_key": "guard-1",
                            "reused_guard": False,
                        },
                    },
                }
            return {
                "id": job_id,
                "status": "completed",
                "result": {"success": True},
            }

        keep_list_runner._load_runtime_dependencies = lambda: {
            "backend_app": backend_app,
            "prepare_screening_inputs": fake_prepare_screening_inputs,
            "count_passed_profiles": lambda scrape_job: 0,
            "export_platform_artifacts": lambda client, platform, export_dir: {"final_review": str(export_dir / f"{platform}_final_review.xlsx")},
            "poll_job": fake_poll_job,
            "require_success": lambda response, label: response.get_json(),
            "reset_backend_runtime_state": lambda: None,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            keep_path = temp_root / "keep.xlsx"
            template_path = temp_root / "template.xlsx"
            keep_path.touch()
            template_path.touch()
            env_path = self._write_env_file(temp_root)

            summary = keep_list_runner.run_keep_list_screening_pipeline(
                keep_workbook=keep_path,
                template_workbook=template_path,
                env_file=env_path,
                output_root=temp_root / "run",
                summary_json=temp_root / "run" / "summary.json",
                platform_filters=["instagram"],
                vision_provider="openai",
            )

        platform_summary = summary["platforms"]["instagram"]
        self.assertEqual(summary["status"], "completed_with_partial_scrape")
        self.assertEqual(platform_summary["status"], "completed_with_partial_scrape")
        self.assertEqual(platform_summary["prescreen_pass_count"], 1)
        self.assertTrue(platform_summary["scrape_job"]["salvaged"])
        self.assertEqual(platform_summary["scrape_job"]["failure_stage"], "poll")
        self.assertEqual(platform_summary["scrape_job"]["apify_run_id"], "apify-run-1")
        self.assertEqual(platform_summary["scrape_job"]["apify_dataset_id"], "dataset-1")
        self.assertEqual(
            backend_app.app.test_client().visual_start_calls[0]["payload"]["provider"],
            "openai",
        )

    def test_runner_executes_positioning_card_analysis_after_visual_pass(self) -> None:
        preflight = {
            "status": "configured",
            "error_code": "",
            "message": "视觉模型已就绪：openai",
            "configured_provider_names": ["openai"],
            "runnable_provider_names": ["openai"],
            "preferred_provider": "openai",
            "providers": [{"name": "openai", "runnable": True}],
        }
        backend_app = FakeBackendApp(preflight)

        def fake_prepare_screening_inputs(**kwargs):
            return {"prepared_at": "2026-03-28T01:02:03Z", "upload": {"stats": {"Instagram": 1}}}

        def fake_poll_job(client, job_id, label, interval):
            if job_id == "scrape-job-1":
                return {
                    "id": job_id,
                    "status": "completed",
                    "result": {"profile_reviews": [{"status": "Pass", "username": "alpha"}]},
                }
            if job_id == "visual-job-1":
                return {"id": job_id, "status": "completed", "result": {"success": True}}
            if job_id == "positioning-job-1":
                return {"id": job_id, "status": "completed", "result": {"success": True}}
            raise AssertionError(f"unexpected job id: {job_id}")

        keep_list_runner._load_runtime_dependencies = lambda: {
            "backend_app": backend_app,
            "prepare_screening_inputs": fake_prepare_screening_inputs,
            "count_passed_profiles": lambda scrape_job: 1,
            "export_platform_artifacts": lambda client, platform, export_dir: {},
            "poll_job": fake_poll_job,
            "require_success": lambda response, label: response.get_json(),
            "reset_backend_runtime_state": lambda: None,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            keep_path = temp_root / "keep.xlsx"
            template_path = temp_root / "template.xlsx"
            keep_path.touch()
            template_path.touch()
            env_path = self._write_env_file(temp_root)

            summary = keep_list_runner.run_keep_list_screening_pipeline(
                keep_workbook=keep_path,
                template_workbook=template_path,
                env_file=env_path,
                output_root=temp_root / "run",
                platform_filters=["instagram"],
                vision_provider="openai",
            )

        platform_summary = summary["platforms"]["instagram"]
        self.assertEqual(platform_summary["positioning_card_analysis"]["status"], "completed")
        self.assertEqual(
            backend_app.app.test_client().positioning_start_calls[0]["payload"]["provider"],
            "openai",
        )

    def test_runner_reruns_failed_visual_rows_before_export(self) -> None:
        preflight = {
            "status": "configured",
            "error_code": "",
            "message": "视觉模型已就绪：openai",
            "configured_provider_names": ["openai", "reelx"],
            "runnable_provider_names": ["openai", "reelx"],
            "preferred_provider": "openai",
            "providers": [{"name": "openai", "runnable": True}, {"name": "reelx", "runnable": True}],
        }
        backend_app = FakeBackendApp(preflight, metadata={"instagram": {"alpha": {"handle": "alpha"}, "beta": {"handle": "beta"}}})

        def fake_prepare_screening_inputs(**kwargs):
            return {"prepared_at": "2026-03-28T01:02:03Z", "upload": {"stats": {"Instagram": 2}}}

        def fake_poll_job(client, job_id, label, interval):
            if job_id == "scrape-job-1":
                return {
                    "id": job_id,
                    "status": "completed",
                    "result": {"profile_reviews": [{"status": "Pass", "username": "alpha"}, {"status": "Pass", "username": "beta"}]},
                }
            if job_id == "visual-job-1":
                return {
                    "id": job_id,
                    "status": "completed",
                    "result": {
                        "success": True,
                        "visual_results": {
                            "alpha": {"username": "alpha", "decision": "Pass", "reason": "ok"},
                            "beta": {"username": "beta", "success": False, "error": "openai: Read timed out"},
                        },
                    },
                }
            if job_id == "visual-job-2":
                return {
                    "id": job_id,
                    "status": "completed",
                    "result": {
                        "success": True,
                        "visual_results": {
                            "beta": {"username": "beta", "decision": "Reject", "reason": "resolved"},
                        },
                    },
                }
            if job_id == "positioning-job-1":
                return {"id": job_id, "status": "completed", "result": {"success": True}}
            raise AssertionError(f"unexpected job id: {job_id}")

        keep_list_runner._load_runtime_dependencies = lambda: {
            "backend_app": backend_app,
            "prepare_screening_inputs": fake_prepare_screening_inputs,
            "count_passed_profiles": lambda scrape_job: 2,
            "export_platform_artifacts": lambda client, platform, export_dir: {},
            "poll_job": fake_poll_job,
            "require_success": lambda response, label: response.get_json(),
            "reset_backend_runtime_state": lambda: None,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            keep_path = temp_root / "keep.xlsx"
            template_path = temp_root / "template.xlsx"
            keep_path.touch()
            template_path.touch()

            summary = keep_list_runner.run_keep_list_screening_pipeline(
                keep_workbook=keep_path,
                template_workbook=template_path,
                output_root=temp_root / "run",
                platform_filters=["instagram"],
                vision_provider="openai",
                visual_postcheck_max_rounds=2,
            )

        platform_summary = summary["platforms"]["instagram"]
        self.assertEqual(platform_summary["visual_retry"]["status"], "completed")
        self.assertEqual(platform_summary["visual_retry"]["initial_error_count"], 1)
        self.assertEqual(platform_summary["visual_retry"]["final_error_count"], 0)
        self.assertEqual(len(backend_app.app.test_client().visual_start_calls), 2)
        self.assertEqual(
            backend_app.app.test_client().visual_start_calls[1]["payload"]["identifiers"],
            ["beta"],
        )

    def test_runner_can_skip_positioning_card_analysis_explicitly(self) -> None:
        preflight = {
            "status": "configured",
            "error_code": "",
            "message": "视觉模型已就绪：openai",
            "configured_provider_names": ["openai"],
            "runnable_provider_names": ["openai"],
            "preferred_provider": "openai",
            "providers": [{"name": "openai", "runnable": True}],
        }
        backend_app = FakeBackendApp(preflight)

        def fake_prepare_screening_inputs(**kwargs):
            return {"prepared_at": "2026-03-28T01:02:03Z", "upload": {"stats": {"Instagram": 1}}}

        def fake_poll_job(client, job_id, label, interval):
            if job_id == "scrape-job-1":
                return {
                    "id": job_id,
                    "status": "completed",
                    "result": {"profile_reviews": [{"status": "Pass", "username": "alpha"}]},
                }
            return {"id": job_id, "status": "completed", "result": {"success": True}}

        keep_list_runner._load_runtime_dependencies = lambda: {
            "backend_app": backend_app,
            "prepare_screening_inputs": fake_prepare_screening_inputs,
            "count_passed_profiles": lambda scrape_job: 1,
            "export_platform_artifacts": lambda client, platform, export_dir: {},
            "poll_job": fake_poll_job,
            "require_success": lambda response, label: response.get_json(),
            "reset_backend_runtime_state": lambda: None,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            keep_path = temp_root / "keep.xlsx"
            template_path = temp_root / "template.xlsx"
            keep_path.touch()
            template_path.touch()
            env_path = self._write_env_file(temp_root)

            summary = keep_list_runner.run_keep_list_screening_pipeline(
                keep_workbook=keep_path,
                template_workbook=template_path,
                env_file=env_path,
                output_root=temp_root / "run",
                platform_filters=["instagram"],
                skip_positioning_card_analysis=True,
            )

        platform_summary = summary["platforms"]["instagram"]
        self.assertEqual(platform_summary["positioning_card_analysis"]["status"], "skipped")
        self.assertEqual(platform_summary["positioning_card_analysis"]["reason"], "skip_positioning_card_analysis flag set")
        self.assertEqual(backend_app.app.test_client().positioning_start_calls, [])

    def test_runner_keeps_platform_completed_when_positioning_card_analysis_fails_non_blockingly(self) -> None:
        preflight = {
            "status": "configured",
            "error_code": "",
            "message": "视觉模型已就绪：openai",
            "configured_provider_names": ["openai"],
            "runnable_provider_names": ["openai"],
            "preferred_provider": "openai",
            "providers": [{"name": "openai", "runnable": True}],
        }
        backend_app = FakeBackendApp(preflight)

        def fake_prepare_screening_inputs(**kwargs):
            return {"prepared_at": "2026-03-28T01:02:03Z", "upload": {"stats": {"Instagram": 1}}}

        def fake_poll_job(client, job_id, label, interval):
            if job_id == "scrape-job-1":
                return {
                    "id": job_id,
                    "status": "completed",
                    "result": {"profile_reviews": [{"status": "Pass", "username": "alpha"}]},
                }
            if job_id == "visual-job-1":
                return {"id": job_id, "status": "completed", "result": {"success": True}}
            if job_id == "positioning-job-1":
                raise RuntimeError("positioning crash")
            raise AssertionError(f"unexpected job id: {job_id}")

        keep_list_runner._load_runtime_dependencies = lambda: {
            "backend_app": backend_app,
            "prepare_screening_inputs": fake_prepare_screening_inputs,
            "count_passed_profiles": lambda scrape_job: 1,
            "export_platform_artifacts": lambda client, platform, export_dir: {"final_review": str(export_dir / "instagram_final_review.xlsx")},
            "poll_job": fake_poll_job,
            "require_success": lambda response, label: response.get_json(),
            "reset_backend_runtime_state": lambda: None,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            keep_path = temp_root / "keep.xlsx"
            template_path = temp_root / "template.xlsx"
            keep_path.touch()
            template_path.touch()
            env_path = self._write_env_file(temp_root)

            summary = keep_list_runner.run_keep_list_screening_pipeline(
                keep_workbook=keep_path,
                template_workbook=template_path,
                env_file=env_path,
                output_root=temp_root / "run",
                platform_filters=["instagram"],
            )

        platform_summary = summary["platforms"]["instagram"]
        self.assertEqual(summary["status"], "completed")
        self.assertEqual(platform_summary["status"], "completed")
        self.assertEqual(platform_summary["positioning_card_analysis"]["status"], "failed")
        self.assertTrue(platform_summary["positioning_card_analysis"]["non_blocking"])

    def test_runner_marks_completed_with_quality_warnings_when_visual_coverage_is_missing(self) -> None:
        preflight = {
            "status": "configured",
            "error_code": "",
            "message": "视觉模型已就绪：openai",
            "configured_provider_names": ["openai"],
            "runnable_provider_names": ["openai"],
            "preferred_provider": "openai",
            "providers": [{"name": "openai", "runnable": True}],
        }
        backend_app = FakeBackendApp(
            preflight,
            artifact_status_by_platform={
                "instagram": {
                    "profile_review_count": 2,
                    "visual_review_count": 0,
                    "missing_profile_count": 0,
                }
            },
        )

        def fake_prepare_screening_inputs(**kwargs):
            return {"prepared_at": "2026-03-28T01:02:03Z", "upload": {"stats": {"Instagram": 2}}}

        def fake_poll_job(client, job_id, label, interval):
            if job_id == "scrape-job-1":
                return {
                    "id": job_id,
                    "status": "completed",
                    "result": {"profile_reviews": [{"status": "Pass", "username": "alpha"}]},
                }
            if job_id == "visual-job-1":
                return {"id": job_id, "status": "failed", "error": "openai timeout"}
            raise AssertionError(f"unexpected job id: {job_id}")

        keep_list_runner._load_runtime_dependencies = lambda: {
            "backend_app": backend_app,
            "prepare_screening_inputs": fake_prepare_screening_inputs,
            "count_passed_profiles": lambda scrape_job: 1,
            "export_platform_artifacts": lambda client, platform, export_dir: {},
            "poll_job": fake_poll_job,
            "require_success": lambda response, label: response.get_json(),
            "reset_backend_runtime_state": lambda: None,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            keep_path = temp_root / "keep.xlsx"
            template_path = temp_root / "template.xlsx"
            keep_path.touch()
            template_path.touch()
            env_path = self._write_env_file(temp_root)

            summary = keep_list_runner.run_keep_list_screening_pipeline(
                keep_workbook=keep_path,
                template_workbook=template_path,
                env_file=env_path,
                output_root=temp_root / "run",
                platform_filters=["instagram"],
                vision_provider="openai",
            )

        self.assertEqual(summary["status"], "completed_with_quality_warnings")
        self.assertEqual(summary["verdict"]["outcome"], "completed")
        self.assertEqual(summary["verdict"]["recommended_action"], "inspect_summary")
        self.assertEqual(summary["quality_report"]["status"], "warning")
        self.assertEqual(summary["quality_report"]["warning_count"], 1)
        self.assertEqual(summary["quality_report"]["warnings"][0]["code"], "visual_coverage_gap")
        self.assertEqual(summary["quality_report"]["warnings"][0]["platform"], "instagram")
        self.assertEqual(summary["quality_report"]["warnings"][0]["count"], 1)

    def test_runner_does_not_emit_quality_warning_when_visual_is_explicitly_skipped(self) -> None:
        preflight = {
            "status": "configured",
            "error_code": "",
            "message": "视觉模型已就绪：openai",
            "configured_provider_names": ["openai"],
            "runnable_provider_names": ["openai"],
            "preferred_provider": "openai",
            "providers": [{"name": "openai", "runnable": True}],
        }
        backend_app = FakeBackendApp(
            preflight,
            artifact_status_by_platform={
                "instagram": {
                    "profile_review_count": 1,
                    "visual_review_count": 0,
                    "missing_profile_count": 0,
                }
            },
        )

        def fake_prepare_screening_inputs(**kwargs):
            return {"prepared_at": "2026-03-28T01:02:03Z", "upload": {"stats": {"Instagram": 1}}}

        def fake_poll_job(client, job_id, label, interval):
            if job_id == "scrape-job-1":
                return {
                    "id": job_id,
                    "status": "completed",
                    "result": {"profile_reviews": [{"status": "Pass", "username": "alpha"}]},
                }
            raise AssertionError(f"unexpected job id: {job_id}")

        keep_list_runner._load_runtime_dependencies = lambda: {
            "backend_app": backend_app,
            "prepare_screening_inputs": fake_prepare_screening_inputs,
            "count_passed_profiles": lambda scrape_job: 1,
            "export_platform_artifacts": lambda client, platform, export_dir: {},
            "poll_job": fake_poll_job,
            "require_success": lambda response, label: response.get_json(),
            "reset_backend_runtime_state": lambda: None,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            keep_path = temp_root / "keep.xlsx"
            template_path = temp_root / "template.xlsx"
            keep_path.touch()
            template_path.touch()
            env_path = self._write_env_file(temp_root)

            summary = keep_list_runner.run_keep_list_screening_pipeline(
                keep_workbook=keep_path,
                template_workbook=template_path,
                env_file=env_path,
                output_root=temp_root / "run",
                platform_filters=["instagram"],
                skip_visual=True,
            )

        self.assertEqual(summary["status"], "completed")
        self.assertEqual(summary["quality_report"]["status"], "ok")
        self.assertEqual(summary["quality_report"]["warning_count"], 0)

    def test_runner_marks_top_level_scrape_failure_when_platform_scrape_fails(self) -> None:
        preflight = {
            "status": "configured",
            "error_code": "",
            "message": "视觉模型已就绪：openai",
            "configured_provider_names": ["openai"],
            "runnable_provider_names": ["openai"],
            "preferred_provider": "openai",
            "providers": [{"name": "openai", "runnable": True}],
        }
        backend_app = FakeBackendApp(preflight)

        def fake_prepare_screening_inputs(**kwargs):
            return {
                "prepared_at": "2026-03-28T01:02:03Z",
                "upload": {"stats": {"Instagram": 1}},
            }

        def fake_poll_job(client, job_id, label, interval):
            return {
                "id": job_id,
                "status": "failed",
                "result": None,
            }

        keep_list_runner._load_runtime_dependencies = lambda: {
            "backend_app": backend_app,
            "prepare_screening_inputs": fake_prepare_screening_inputs,
            "count_passed_profiles": lambda scrape_job: 0,
            "export_platform_artifacts": lambda client, platform, export_dir: {},
            "poll_job": fake_poll_job,
            "require_success": lambda response, label: response.get_json(),
            "reset_backend_runtime_state": lambda: None,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            keep_path = temp_root / "keep.xlsx"
            template_path = temp_root / "template.xlsx"
            keep_path.touch()
            template_path.touch()
            env_path = self._write_env_file(temp_root)
            summary = keep_list_runner.run_keep_list_screening_pipeline(
                keep_workbook=keep_path,
                template_workbook=template_path,
                env_file=env_path,
                output_root=temp_root / "run",
                platform_filters=["instagram"],
                vision_provider="openai",
            )

        self.assertEqual(summary["status"], "scrape_failed")
        self.assertEqual(summary["verdict"]["outcome"], "failed")
        self.assertEqual(summary["verdict"]["resolution_mode"], "auto_retry")
        self.assertEqual(summary["platforms"]["instagram"]["status"], "scrape_failed")
        self.assertEqual(summary["failure"]["error_code"], "SCRAPE_FAILED")
        self.assertEqual(summary["failure_decision"]["category"], "external_runtime")
        self.assertEqual(summary["failure_decision"]["resolution_mode"], "auto_retry")
        self.assertTrue(summary["failure_decision"]["retryable"])

    def test_runner_continues_to_next_platform_after_platform_runtime_failure(self) -> None:
        preflight = {
            "status": "configured",
            "error_code": "",
            "message": "视觉模型已就绪：openai",
            "configured_provider_names": ["openai"],
            "runnable_provider_names": ["openai"],
            "preferred_provider": "openai",
            "providers": [{"name": "openai", "runnable": True}],
        }
        backend_app = FakeBackendApp(
            preflight,
            metadata={
                "tiktok": {
                    "alpha": {
                        "url": "https://www.tiktok.com/@alpha",
                    }
                },
                "instagram": {
                    "beta": {
                        "handle": "beta",
                    }
                },
            },
        )

        def fake_prepare_screening_inputs(**kwargs):
            return {
                "prepared_at": "2026-03-28T01:02:03Z",
                "upload": {"stats": {"TikTok": 1, "Instagram": 1}},
            }

        def fake_poll_job(client, job_id, label, interval):
            return {
                "id": job_id,
                "status": "completed",
                "result": {
                    "profile_reviews": [{"status": "Pass", "username": "beta"}],
                },
            }

        export_calls: list[str] = []

        def fake_export_platform_artifacts(client, platform, export_dir):
            export_calls.append(str(platform))
            return {"final_review": str(export_dir / f"{platform}_final_review.xlsx")}

        def fake_require_success(response, label):
            if label == "tiktok scrape start":
                raise RuntimeError("tiktok scrape start timeout")
            return response.get_json()

        keep_list_runner._load_runtime_dependencies = lambda: {
            "backend_app": backend_app,
            "prepare_screening_inputs": fake_prepare_screening_inputs,
            "count_passed_profiles": lambda scrape_job: 1 if "Pass" in json.dumps(scrape_job) else 0,
            "export_platform_artifacts": fake_export_platform_artifacts,
            "poll_job": fake_poll_job,
            "require_success": fake_require_success,
            "reset_backend_runtime_state": lambda: None,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            keep_path = temp_root / "keep.xlsx"
            template_path = temp_root / "template.xlsx"
            keep_path.touch()
            template_path.touch()
            env_path = self._write_env_file(temp_root)
            summary = keep_list_runner.run_keep_list_screening_pipeline(
                keep_workbook=keep_path,
                template_workbook=template_path,
                env_file=env_path,
                output_root=temp_root / "run",
                platform_filters=["tiktok", "instagram"],
                skip_visual=True,
                skip_positioning_card_analysis=True,
            )

        self.assertEqual(summary["status"], "completed_with_platform_failures")
        self.assertEqual(summary["verdict"]["outcome"], "completed")
        self.assertEqual(summary["verdict"]["recommended_action"], "inspect_summary")
        self.assertEqual(summary["platforms"]["tiktok"]["status"], "failed")
        self.assertEqual(summary["platforms"]["tiktok"]["error_code"], "PLATFORM_RUNTIME_FAILED")
        self.assertIn("timeout", summary["platforms"]["tiktok"]["error"])
        self.assertEqual(summary["platforms"]["instagram"]["status"], "completed")
        self.assertEqual(export_calls, ["instagram"])

    def test_runner_attaches_structured_failure_when_no_platform_succeeds(self) -> None:
        preflight = {
            "status": "configured",
            "error_code": "",
            "message": "视觉模型已就绪：openai",
            "configured_provider_names": ["openai"],
            "runnable_provider_names": ["openai"],
            "preferred_provider": "openai",
            "providers": [{"name": "openai", "runnable": True}],
        }
        backend_app = FakeBackendApp(
            preflight,
            metadata={
                "tiktok": {"alpha": {"handle": "alpha"}},
                "instagram": {"beta": {"handle": "beta"}},
                "youtube": {"gamma": {"handle": "gamma"}},
            },
        )

        def fake_prepare_screening_inputs(**kwargs):
            return {
                "prepared_at": "2026-03-28T01:02:03Z",
                "upload": {"stats": {"TikTok": 1, "Instagram": 1, "YouTube": 1}},
            }

        def fake_build_all_platforms_final_review_artifacts(**kwargs):
            payload_json_path = Path(kwargs["payload_json_path"])
            payload_json_path.parent.mkdir(parents=True, exist_ok=True)
            payload_json_path.write_text(json.dumps({"rows": []}, ensure_ascii=False), encoding="utf-8")
            workbook_path = Path(kwargs["output_path"])
            workbook_path.write_text("placeholder", encoding="utf-8")
            return {
                "all_platforms_final_review": str(workbook_path),
                "all_platforms_upload_payload_json": str(payload_json_path),
                "all_platforms_upload_local_archive_dir": "",
                "all_platforms_upload_skipped_archive_json": "",
                "all_platforms_upload_skipped_archive_xlsx": "",
                "row_count": 0,
                "source_row_count": 0,
                "skipped_row_count": 0,
            }

        def fake_require_success(response, label):
            if label.endswith("scrape start"):
                raise RuntimeError(f"{label} timeout")
            return response.get_json()

        keep_list_runner._load_runtime_dependencies = lambda: {
            "backend_app": backend_app,
            "build_all_platforms_final_review_artifacts": fake_build_all_platforms_final_review_artifacts,
            "collect_final_exports": lambda platforms: {},
            "prepare_screening_inputs": fake_prepare_screening_inputs,
            "count_passed_profiles": lambda scrape_job: 0,
            "export_platform_artifacts": lambda client, platform, export_dir: {},
            "poll_job": lambda client, job_id, label, interval: {},
            "require_success": fake_require_success,
            "reset_backend_runtime_state": lambda: None,
            "upload_final_review_payload_to_bitable": lambda client, **kwargs: (_ for _ in ()).throw(
                AssertionError("upload should not be called when no platform succeeds")
            ),
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            keep_path = temp_root / "keep.xlsx"
            template_path = temp_root / "template.xlsx"
            keep_path.touch()
            template_path.touch()
            env_path = self._write_env_file(temp_root)
            summary = keep_list_runner.run_keep_list_screening_pipeline(
                keep_workbook=keep_path,
                template_workbook=template_path,
                env_file=env_path,
                output_root=temp_root / "run",
                platform_filters=["tiktok", "instagram", "youtube"],
                skip_visual=True,
                skip_positioning_card_analysis=True,
            )

        self.assertEqual(summary["status"], "failed")
        self.assertEqual(summary["error_code"], "NO_SUCCESSFUL_PLATFORMS")
        self.assertEqual(summary["verdict"]["outcome"], "failed")
        self.assertEqual(summary["verdict"]["recommended_action"], "inspect_runtime")
        self.assertEqual(summary["failure"]["stage"], "platform_runtime")
        self.assertEqual(summary["failure_decision"]["category"], "runtime")
        self.assertEqual(summary["artifacts"]["all_platforms_upload_source_row_count"], 0)
        self.assertEqual(summary["artifacts"]["all_platforms_upload_row_count"], 0)
        self.assertEqual(summary["artifacts"]["feishu_upload_result_json"], "")
        self.assertEqual(summary["platforms"]["tiktok"]["status"], "failed")
        self.assertEqual(summary["platforms"]["instagram"]["status"], "failed")
        self.assertEqual(summary["platforms"]["youtube"]["status"], "failed")


if __name__ == "__main__":
    unittest.main()
