from __future__ import annotations

import os
import unittest
from unittest import mock

import backend.app as backend_app


class DummyProviderResponse:
    def __init__(self, payload, status_code=200, text=""):
        self._payload = payload
        self.status_code = status_code
        self.text = text

    def json(self):
        return self._payload


class VisualProviderDiagnosticsTests(unittest.TestCase):
    ENV_KEYS = {
        "OPENAI_API_KEY",
        "OPENAI_BASE_URL",
        "OPENAI_VISION_MODEL",
        "VISION_MODEL",
        "VISION_PROVIDER_PREFERENCE",
    }

    def setUp(self) -> None:
        self.original_provider_configs = backend_app.VISION_PROVIDER_CONFIGS
        self.original_dotenv_values = dict(backend_app.DOTENV_LOCAL_VALUES)
        self.original_dotenv_loaded_keys = set(backend_app.DOTENV_LOCAL_LOADED_KEYS)
        self.original_env = {key: os.environ.get(key) for key in self.ENV_KEYS}
        for key in self.ENV_KEYS:
            os.environ.pop(key, None)
        backend_app.DOTENV_LOCAL_VALUES = {}
        backend_app.DOTENV_LOCAL_LOADED_KEYS = set()
        backend_app.VISION_PROVIDER_CONFIGS = (
            {
                "name": "openai",
                "base_url_env_key": "OPENAI_BASE_URL",
                "default_base_url": "https://api.openai.com/v1",
                "env_key": "OPENAI_API_KEY",
                "api_style": backend_app.VISION_API_STYLE_RESPONSES,
                "model_env_key": "OPENAI_VISION_MODEL",
            },
        )
        with backend_app.JOBS_LOCK:
            backend_app.JOBS.clear()
        self.client = backend_app.app.test_client()

    def tearDown(self) -> None:
        backend_app.VISION_PROVIDER_CONFIGS = self.original_provider_configs
        backend_app.DOTENV_LOCAL_VALUES = self.original_dotenv_values
        backend_app.DOTENV_LOCAL_LOADED_KEYS = self.original_dotenv_loaded_keys
        for key, value in self.original_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    def test_snapshot_reports_env_local_source_and_global_model_fallback(self) -> None:
        os.environ["OPENAI_API_KEY"] = "sk-dotenv-12345678"
        backend_app.DOTENV_LOCAL_VALUES = {"OPENAI_API_KEY": "sk-dotenv-12345678"}
        backend_app.DOTENV_LOCAL_LOADED_KEYS = {"OPENAI_API_KEY"}

        snapshot = backend_app.build_vision_provider_snapshot(backend_app.VISION_PROVIDER_CONFIGS[0])

        self.assertTrue(snapshot["runnable"])
        self.assertEqual(snapshot["api_key_source"], "env.local")
        self.assertEqual(snapshot["base_url_source"], "default")
        self.assertEqual(snapshot["model_source"], "default")
        self.assertTrue(snapshot["model_uses_global_fallback"])
        self.assertEqual(snapshot["model"], backend_app.DEFAULT_VISION_MODEL)
        self.assertEqual(snapshot["api_style"], backend_app.VISION_API_STYLE_RESPONSES)

    def test_preflight_marks_degraded_when_provider_is_present_but_invalid(self) -> None:
        os.environ["OPENAI_API_KEY"] = "sk-live-12345678"
        os.environ["OPENAI_BASE_URL"] = "not-a-url"

        preflight = backend_app.build_vision_preflight()

        self.assertEqual(preflight["status"], "degraded")
        self.assertEqual(preflight["error_code"], "VISION_PROVIDER_PREFLIGHT_FAILED")
        self.assertEqual(preflight["configured_provider_names"], ["openai"])
        self.assertEqual(preflight["runnable_provider_names"], [])
        self.assertIn("invalid_base_url", preflight["providers"][0]["issues"])

    def test_health_check_includes_rich_vision_preflight_contract(self) -> None:
        os.environ["OPENAI_API_KEY"] = "sk-live-12345678"

        response = self.client.get("/api/health")
        payload = response.get_json()

        self.assertEqual(response.status_code, 200)
        preflight = payload["checks"]["vision_preflight"]
        self.assertEqual(preflight["status"], "configured")
        self.assertEqual(preflight["runnable_provider_names"], ["openai"])
        self.assertEqual(payload["checks"]["vision_providers"], ["openai"])
        provider = preflight["providers"][0]
        self.assertIn("name", provider)
        self.assertIn("api_key_present", provider)
        self.assertIn("base_url", provider)
        self.assertIn("model", provider)
        self.assertIn("api_style", provider)
        self.assertIn("runnable", provider)

    def test_preflight_respects_requested_provider_preference(self) -> None:
        os.environ["OPENAI_API_KEY"] = "sk-live-12345678"
        os.environ["VISION_PROVIDER_PREFERENCE"] = "openai"

        preflight = backend_app.build_vision_preflight()

        self.assertEqual(preflight["requested_provider"], "openai")
        self.assertEqual(preflight["requested_provider_source"], "env")
        self.assertEqual(preflight["preferred_provider"], "openai")
        self.assertTrue(preflight["requested_provider_runnable"])

    def test_visual_review_start_returns_preflight_error_payload(self) -> None:
        os.environ["OPENAI_API_KEY"] = "sk-live-12345678"
        os.environ["OPENAI_BASE_URL"] = "not-a-url"

        response = self.client.post(
            "/api/jobs/visual-review",
            json={"platform": "instagram", "payload": {"identifiers": ["alpha"]}},
        )
        payload = response.get_json()

        self.assertEqual(response.status_code, 400)
        self.assertFalse(payload["success"])
        self.assertEqual(payload["error_code"], "VISION_PROVIDER_PREFLIGHT_FAILED")
        self.assertEqual(payload["vision_preflight"]["status"], "degraded")
        self.assertEqual(payload["vision_preflight"]["configured_provider_names"], ["openai"])

    def test_probe_endpoint_returns_success_payload_for_selected_provider(self) -> None:
        os.environ["OPENAI_API_KEY"] = "sk-live-12345678"

        with mock.patch.object(
            backend_app.requests,
            "post",
            return_value=DummyProviderResponse(
                {"output": [{"content": [{"text": "ok"}]}]},
                status_code=200,
            ),
        ) as mocked_post:
            response = self.client.post("/api/vision/providers/probe", json={"provider": "openai"})

        payload = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertTrue(payload["success"])
        self.assertEqual(payload["provider"], "openai")
        self.assertEqual(payload["probe"]["provider"], "openai")
        self.assertEqual(payload["vision_preflight"]["requested_provider"], "openai")
        mocked_post.assert_called_once()

    def test_probe_endpoint_surfaces_provider_auth_failure(self) -> None:
        os.environ["OPENAI_API_KEY"] = "sk-live-12345678"

        with mock.patch.object(
            backend_app.requests,
            "post",
            return_value=DummyProviderResponse(
                {"error": {"message": "auth_not_found: no auth available"}},
                status_code=500,
                text='{"error":{"message":"auth_not_found: no auth available"}}',
            ),
        ):
            response = self.client.post("/api/vision/providers/probe", json={"provider": "openai"})

        payload = response.get_json()
        self.assertEqual(response.status_code, 502)
        self.assertFalse(payload["success"])
        self.assertEqual(payload["error_code"], "VISION_PROVIDER_PROBE_FAILED")
        self.assertIn("auth_not_found", payload["error"])
        self.assertEqual(payload["provider"], "openai")


if __name__ == "__main__":
    unittest.main()
