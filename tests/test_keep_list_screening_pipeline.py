from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import scripts.run_keep_list_screening_pipeline as keep_list_runner


class FakeResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code

    def get_json(self, silent=False):
        return self._payload


class FakeClient:
    def __init__(self, preflight=None):
        self.probe_calls = []
        self.visual_start_calls = []
        self.preflight = json.loads(json.dumps(preflight or {}))

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
            return FakeResponse({"success": True, "job": {"id": "scrape-job-1"}})
        if url == "/api/jobs/visual-review":
            self.visual_start_calls.append(json or {})
            return FakeResponse({"success": True, "job": {"id": "visual-job-1"}})
        raise AssertionError(f"unexpected POST {url}")

    def get(self, url):
        if url.endswith("/status"):
            return FakeResponse({"success": True, "available": {"final_review": False}})
        raise AssertionError(f"unexpected GET {url}")


class FakeFlaskApp:
    def __init__(self, client):
        self._client = client

    def test_client(self):
        return self._client


class FakeBackendApp:
    PLATFORM_ACTORS = {"instagram": "actor"}

    def __init__(self, preflight):
        self._preflight = json.loads(json.dumps(preflight))
        self._metadata = {"instagram": {"alpha": {"handle": "alpha"}}}
        self._client = FakeClient(preflight=preflight)
        self.app = FakeFlaskApp(self._client)

    def iso_now(self):
        return "2026-03-28T01:02:03Z"

    def load_upload_metadata(self, platform):
        return self._metadata.get(platform, {})

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

    def write_json_file(self, path, payload):
        output_path = Path(path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


class KeepListRunnerSummaryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.original_loader = keep_list_runner._load_runtime_dependencies

    def tearDown(self) -> None:
        keep_list_runner._load_runtime_dependencies = self.original_loader

    def test_summary_includes_vision_preflight_for_staging_only_run(self) -> None:
        preflight = {
            "status": "configured",
            "error_code": "",
            "message": "视觉模型已就绪：openai",
            "configured_provider_names": ["openai"],
            "runnable_provider_names": ["openai"],
            "providers": [{"name": "openai", "runnable": True}],
        }
        backend_app = FakeBackendApp(preflight)

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

            summary = keep_list_runner.run_keep_list_screening_pipeline(
                keep_workbook=keep_path,
                template_workbook=template_path,
                output_root=temp_root / "run",
                platform_filters=["instagram"],
                skip_scrape=True,
            )

        self.assertEqual(summary["vision_preflight"]["status"], "configured")
        self.assertEqual(summary["platforms"]["instagram"]["vision_preflight"]["status"], "configured")
        self.assertEqual(
            summary["platforms"]["instagram"]["visual_gate"]["preflight_status"],
            "configured",
        )
        self.assertEqual(summary["vision_probe"]["success"], True)
        self.assertEqual(summary["vision_probe"]["provider"], "openai")

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

            summary = keep_list_runner.run_keep_list_screening_pipeline(
                keep_workbook=keep_path,
                template_workbook=template_path,
                output_root=temp_root / "run",
                summary_json=summary_path,
                platform_filters=["instagram"],
                skip_scrape=False,
                skip_visual=False,
            )
            persisted_summary = json.loads(summary_path.read_text(encoding="utf-8"))

        self.assertEqual(summary["status"], "vision_probe_failed")
        self.assertEqual(summary["vision_probe"]["error_code"], "VISION_PROVIDER_PREFLIGHT_FAILED")
        self.assertEqual(summary["vision_probe"]["vision_preflight"]["status"], "degraded")
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
            summary = keep_list_runner.run_keep_list_screening_pipeline(
                keep_workbook=keep_path,
                template_workbook=template_path,
                output_root=temp_root / "run",
                platform_filters=["instagram"],
                vision_provider="openai",
            )

        self.assertEqual(summary["requested_vision_provider"], "openai")
        self.assertEqual(summary["vision_probe"]["success"], True)
        self.assertEqual(summary["platforms"]["instagram"]["requested_vision_provider"], "openai")
        self.assertEqual(
            backend_app.app.test_client().probe_calls[0]["provider"],
            "openai",
        )
        self.assertEqual(
            backend_app.app.test_client().visual_start_calls[0]["payload"]["provider"],
            "openai",
        )


if __name__ == "__main__":
    unittest.main()
