from __future__ import annotations

import base64
import io
import os
import struct
import time
import unittest
from unittest import mock

import backend.app as backend_app
import pandas as pd


class DummyProviderResponse:
    def __init__(self, payload, status_code=200, text="", headers=None, content=None):
        self._payload = payload
        self.status_code = status_code
        self.text = text
        self.headers = headers or {}
        self.content = content if content is not None else str(text or "").encode("utf-8")

    def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


class VisualProviderDiagnosticsTests(unittest.TestCase):
    ENV_KEYS = {
        "OPENAI_API_KEY",
        "OPENAI_BASE_URL",
        "OPENAI_VISION_MODEL",
        "VISION_MODEL",
        "VISION_PROVIDER_PREFERENCE",
        "VISUAL_REVIEW_ITEM_TIMEOUT_SECONDS",
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

    def test_test_info_export_keeps_only_profile_review_sheet(self) -> None:
        with mock.patch.object(
            backend_app,
            "load_profile_reviews",
            return_value=[
                {
                    "username": "alpha",
                    "profile_url": "https://instagram.com/alpha",
                    "status": "Pass",
                    "reason": "ok",
                    "upload_metadata": {},
                    "stats": {},
                }
            ],
        ):
            response = self.client.get("/api/download/instagram/test-info")

        self.assertEqual(response.status_code, 200)
        workbook = pd.ExcelFile(io.BytesIO(response.data))
        self.assertEqual(workbook.sheet_names, ["Profile Reviews"])
        frame = workbook.parse("Profile Reviews")
        self.assertNotIn("platform", frame.columns)

    def test_test_info_json_hides_internal_runtime_fields(self) -> None:
        with mock.patch.object(
            backend_app,
            "load_profile_reviews",
            return_value=[
                {
                    "username": "alpha",
                    "profile_url": "https://instagram.com/alpha",
                    "status": "Pass",
                    "reason": "ok",
                    "upload_metadata": {},
                    "stats": {},
                }
            ],
        ):
            response = self.client.get("/api/download/instagram/test-info-json")

        payload = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["row_count"], 1)
        self.assertIn("profile_reviews", payload)
        self.assertNotIn("platform", payload)
        self.assertNotIn("raw_items", payload)
        self.assertNotIn("upload_metadata", payload)
        self.assertNotIn("raw_source_path", payload)

    def test_artifact_status_reports_final_review_block_when_missing_profiles_exist(self) -> None:
        with mock.patch.object(
            backend_app,
            "load_profile_reviews",
            return_value=[
                {
                    "username": "ghost",
                    "profile_url": "https://instagram.com/ghost",
                    "status": "Missing",
                    "reason": "名单账号未在本次抓取结果中返回",
                    "upload_metadata": {},
                    "stats": {},
                }
            ],
        ), mock.patch.object(
            backend_app,
            "load_visual_results",
            return_value={"ghost": {"decision": "Pass"}},
        ):
            response = self.client.get("/api/artifacts/instagram/status")

        payload = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertTrue(payload["final_review_export_blocked"])
        self.assertEqual(payload["missing_profile_count"], 1)
        self.assertEqual(payload["missing_profiles_preview"][0]["identifier"], "ghost")
        self.assertFalse(payload["saved_final_review_artifacts_available"])

    def test_final_review_export_is_blocked_when_profile_reviews_contain_missing(self) -> None:
        with mock.patch.object(
            backend_app,
            "load_profile_reviews",
            return_value=[
                {
                    "username": "ghost",
                    "profile_url": "https://instagram.com/ghost",
                    "status": "Missing",
                    "reason": "名单账号未在本次抓取结果中返回",
                    "upload_metadata": {},
                    "stats": {},
                }
            ],
        ):
            response = self.client.post("/api/download/instagram/final-review", json={})

        payload = response.get_json()
        self.assertEqual(response.status_code, 409)
        self.assertEqual(payload["error_code"], "FINAL_REVIEW_BLOCKED_BY_MISSING_PROFILES")
        self.assertEqual(payload["missing_profile_count"], 1)
        self.assertEqual(payload["missing_profiles"][0]["identifier"], "ghost")

    def test_perform_scrape_retries_missing_profiles_before_returning(self) -> None:
        batch_calls = []

        def fake_run_apify_batch(platform, batch, payload, progress_callback=None, cancel_check=None):
            batch_calls.append(list(batch))
            if len(batch_calls) == 1:
                return {
                    "success": True,
                    "raw_items": [
                        {
                            "url": "https://instagram.com/alpha",
                            "username": "alpha",
                            "biography": "NYC creator",
                            "latestPosts": [{"timestamp": "2026-03-29T00:00:00+00:00", "displayUrl": "https://example.com/a.jpg"}],
                        }
                    ],
                    "apify": {"usage_total_usd": 0.1},
                }
            return {
                "success": True,
                "raw_items": [
                    {
                        "url": "https://instagram.com/beta",
                        "username": "beta",
                        "biography": "LA CA lifestyle",
                        "latestPosts": [{"timestamp": "2026-03-29T00:00:00+00:00", "displayUrl": "https://example.com/b.jpg"}],
                    }
                ],
                "apify": {"usage_total_usd": 0.05},
            }

        with mock.patch.object(
            backend_app,
            "run_apify_batch",
            side_effect=fake_run_apify_batch,
        ), mock.patch.object(
            backend_app,
            "load_upload_metadata",
            return_value={
                "alpha": {"handle": "alpha", "region": "US"},
                "beta": {"handle": "beta", "region": "US"},
            },
        ), mock.patch.object(
            backend_app,
            "load_active_rulespec",
            return_value={},
        ), mock.patch.object(
            backend_app,
            "write_json_file",
        ), mock.patch.object(
            backend_app,
            "save_profile_reviews",
        ):
            result = backend_app.perform_scrape(
                "instagram",
                {"usernames": ["alpha", "beta"]},
            )

        self.assertTrue(result["success"])
        self.assertEqual(batch_calls, [["alpha", "beta"], ["beta"]])
        self.assertEqual(result["retry_summary"]["attempt_count"], 1)
        self.assertEqual(result["retry_summary"]["retried_identifier_count"], 1)
        self.assertEqual(result["retry_summary"]["remaining_missing_count"], 0)
        self.assertEqual(sorted(result["successful_identifiers"]), ["alpha", "beta"])
        self.assertEqual(
            [item["status"] for item in result["profile_reviews"]],
            ["Pass", "Pass"],
        )

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

    def test_probe_endpoint_sanitizes_html_upstream_error(self) -> None:
        os.environ["OPENAI_API_KEY"] = "sk-live-12345678"

        with mock.patch.object(
            backend_app.requests,
            "post",
            return_value=DummyProviderResponse(
                ValueError("not json"),
                status_code=413,
                text="<html><head><title>413 Request Entity Too Large</title></head><body><center><h1>413 Request Entity Too Large</h1></center></body></html>",
                headers={"Content-Type": "text/html"},
            ),
        ):
            response = self.client.post("/api/vision/providers/probe", json={"provider": "openai"})

        payload = response.get_json()
        self.assertEqual(response.status_code, 502)
        self.assertFalse(payload["success"])
        self.assertIn("HTTP 413", payload["error"])
        self.assertIn("请求体过大", payload["error"])
        self.assertIn("HTML 错误页", payload["error"])
        self.assertNotIn("<html>", payload["error"])
        self.assertNotIn("https://", payload["error"])

    def test_export_rows_hide_internal_provider_model_and_platform_columns(self) -> None:
        rows = backend_app.build_final_review_rows(
            "instagram",
            [
                {
                    "username": "alpha",
                    "profile_url": "https://instagram.com/alpha",
                    "status": "Pass",
                    "reason": "prescreen ok",
                    "stats": {},
                    "upload_metadata": {},
                }
            ],
            {
                "alpha": {
                    "decision": "Pass",
                    "reason": "visual ok",
                    "signals": ["主体清晰"],
                    "provider": "reelx",
                    "model": "qwen-vl-max",
                    "route": "preferred_parallel",
                    "judge_used": True,
                    "usage": {"prompt_tokens": 12, "total_tokens": 34},
                }
            },
        )

        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertNotIn("platform", row)
        self.assertNotIn("visual_provider", row)
        self.assertNotIn("visual_model", row)
        self.assertNotIn("visual_route", row)
        self.assertNotIn("visual_judge_used", row)
        self.assertNotIn("visual_prompt_tokens", row)
        self.assertNotIn("visual_total_tokens", row)
        self.assertEqual(row["visual_status"], "Pass")
        self.assertEqual(row["visual_reason"], "visual ok")
        self.assertEqual(row["final_status"], "Pass")

    def test_perform_visual_review_marks_stalled_future_as_timeout_error(self) -> None:
        class HangingFuture:
            def __init__(self) -> None:
                self.cancelled = False

            def done(self) -> bool:
                return False

            def cancel(self) -> bool:
                self.cancelled = True
                return True

        class FakeExecutor:
            def __init__(self, *args, **kwargs) -> None:
                self.future = HangingFuture()

            def submit(self, *args, **kwargs):
                return self.future

            def shutdown(self, wait=False, cancel_futures=False) -> None:
                return None

        def fake_wait(*args, **kwargs):
            time.sleep(0.06)
            return set(), set()

        with mock.patch.object(
            backend_app,
            "build_vision_preflight",
            return_value={"preferred_provider": "openai"},
        ), mock.patch.object(
            backend_app,
            "get_available_vision_providers",
            return_value=[{"name": "openai"}],
        ), mock.patch.object(
            backend_app,
            "get_available_vision_provider_names",
            return_value=["openai"],
        ), mock.patch.object(
            backend_app,
            "resolve_visual_review_targets",
            return_value=[{"username": "alpha", "status": "Pass"}],
        ), mock.patch.object(
            backend_app,
            "load_visual_results",
            return_value={},
        ), mock.patch.object(
            backend_app,
            "save_visual_results",
        ), mock.patch.object(
            backend_app,
            "ThreadPoolExecutor",
            FakeExecutor,
        ), mock.patch.object(
            backend_app,
            "wait",
            side_effect=fake_wait,
        ):
            result = backend_app.perform_visual_review(
                "instagram",
                {"provider": "openai", "max_workers": 1, "item_timeout_seconds": 0.05},
            )

        self.assertTrue(result["success"])
        self.assertEqual(result["summary"]["error"], 1)
        self.assertIn("alpha", result["visual_results"])
        self.assertFalse(result["visual_results"]["alpha"]["success"])
        self.assertIn("视觉复核超时", result["visual_results"]["alpha"]["error"])


class VisualProviderConfigDefaultsTests(unittest.TestCase):
    def test_reelx_defaults_to_generate_content_with_qwen_model(self) -> None:
        provider = next(item for item in backend_app.VISION_PROVIDER_CONFIGS if item["name"] == "reelx")

        self.assertEqual(provider["api_style"], backend_app.VISION_API_STYLE_GENERATE_CONTENT)
        self.assertEqual(provider["default_base_url"], backend_app.DEFAULT_REELX_BASE_URL)
        self.assertEqual(
            provider["default_base_url_fallbacks"],
            backend_app.DEFAULT_REELX_BASE_URL_FALLBACKS,
        )
        self.assertEqual(provider["default_model"], "qwen-vl-max")
        self.assertEqual(provider["default_fallback_model"], "gemini-3-flash-preview")

    def test_quan2go_defaults_to_chat_completions(self) -> None:
        provider = next(item for item in backend_app.VISION_PROVIDER_CONFIGS if item["name"] == "quan2go")
        self.assertEqual(provider["api_style"], backend_app.VISION_API_STYLE_CHAT_COMPLETIONS)

    def test_qiandao_defaults_to_chat_completions_with_gemini_model(self) -> None:
        provider = next(item for item in backend_app.VISION_PROVIDER_CONFIGS if item["name"] == "qiandao")

        self.assertEqual(provider["api_style"], backend_app.VISION_API_STYLE_CHAT_COMPLETIONS)
        self.assertEqual(provider["default_base_url"], "https://api2.qiandao.mom/v1")
        self.assertEqual(provider["default_model"], "gemini-2.5-pro-preview-p")
        self.assertEqual(provider["default_fallback_model"], "gemini-3-flash-preview-S")

    def test_build_qiandao_chat_body_uses_non_streaming_max_tokens_shape(self) -> None:
        provider = next(item for item in backend_app.VISION_PROVIDER_CONFIGS if item["name"] == "qiandao")

        body = backend_app.build_vision_provider_chat_body(provider, [{"role": "user", "content": "ok"}])

        self.assertEqual(body["model"], "gemini-2.5-pro-preview-p")
        self.assertEqual(body["max_tokens"], 900)
        self.assertEqual(body["temperature"], 0.2)
        self.assertFalse(body["stream"])
        self.assertNotIn("max_completion_tokens", body)

    def test_qiandao_25p_uses_tighter_default_visual_worker_count(self) -> None:
        workers = backend_app.resolve_visual_review_max_workers({}, 10, requested_provider="qiandao")

        self.assertEqual(workers, 2)

    def test_qiandao_25p_clamps_requested_visual_worker_count_to_three(self) -> None:
        workers = backend_app.resolve_visual_review_max_workers(
            {"max_workers": 5},
            10,
            requested_provider="qiandao",
        )

        self.assertEqual(workers, 3)

    def test_qiandao_flash_keeps_global_visual_worker_default(self) -> None:
        with mock.patch.dict(os.environ, {"VISION_QIANDAO_MODEL": "gemini-3-flash-preview-S"}, clear=False):
            workers = backend_app.resolve_visual_review_max_workers({}, 10, requested_provider="qiandao")

        self.assertEqual(workers, backend_app.DEFAULT_VISUAL_REVIEW_MAX_WORKERS)

    def test_tiered_routing_without_25p_models_keeps_global_worker_profile(self) -> None:
        workers = backend_app.resolve_visual_review_max_workers({}, 10, routing_strategy="tiered")

        self.assertEqual(workers, backend_app.DEFAULT_VISUAL_REVIEW_MAX_WORKERS)

    def test_probe_ranked_with_25p_selected_uses_tighter_worker_profile(self) -> None:
        workers = backend_app.resolve_visual_review_max_workers(
            {},
            10,
            requested_provider="qiandao",
            requested_model="gemini-2.5-pro-preview-p",
            routing_strategy="probe_ranked",
        )

        self.assertEqual(workers, 2)

    def test_resolve_quan2go_base_url_rewrites_legacy_openai_suffix(self) -> None:
        provider = {
            "name": "quan2go",
            "base_url_env_key": "VISION_QUAN2GO_BASE_URL",
            "default_base_url": "https://capi.quan2go.com/openai",
        }
        with mock.patch.dict(os.environ, {"VISION_QUAN2GO_BASE_URL": "https://capi.quan2go.com/openai"}, clear=False):
            resolved = backend_app.resolve_vision_provider_base_url(provider)

        self.assertEqual(resolved, "https://capi.quan2go.com/v1")

    def test_resolve_reelx_base_urls_prefers_llmxapi_and_keeps_failover_pool(self) -> None:
        provider = next(item for item in backend_app.VISION_PROVIDER_CONFIGS if item["name"] == "reelx")

        resolved = backend_app.resolve_vision_provider_base_urls(provider)

        self.assertEqual(
            resolved,
            [
                "https://llmxapi.com/v1beta",
                "https://reelxai.com/v1beta",
                "https://hk.llmxapi.com/v1beta",
                "https://hk.reelxai.com/v1beta",
            ],
        )

    def test_resolve_reelx_base_urls_accepts_env_override_and_extra_fallbacks(self) -> None:
        provider = next(item for item in backend_app.VISION_PROVIDER_CONFIGS if item["name"] == "reelx")

        with mock.patch.dict(
            os.environ,
            {
                "VISION_REELX_BASE_URL": "https://hk.llmxapi.com/v1beta",
                "VISION_REELX_BASE_URL_FALLBACKS": "https://llmxapi.com/v1beta, https://reelxai.com/v1beta",
            },
            clear=False,
        ):
            resolved = backend_app.resolve_vision_provider_base_urls(provider)

        self.assertEqual(
            resolved,
            [
                "https://hk.llmxapi.com/v1beta",
                "https://llmxapi.com/v1beta",
                "https://reelxai.com/v1beta",
            ],
        )

    def test_mimo_defaults_to_chat_completions_with_api_key_header(self) -> None:
        provider = next(item for item in backend_app.VISION_PROVIDER_CONFIGS if item["name"] == "mimo")

        self.assertEqual(provider["api_style"], backend_app.VISION_API_STYLE_CHAT_COMPLETIONS)
        self.assertEqual(provider["default_base_url"], "https://api.xiaomimimo.com/v1")
        self.assertEqual(provider["auth_header_name"], "api-key")
        self.assertEqual(provider["default_model"], backend_app.DEFAULT_MIMO_VISION_MODEL)
        self.assertEqual(provider["default_max_completion_tokens"], backend_app.DEFAULT_MIMO_MAX_COMPLETION_TOKENS)

    def test_mimo_probe_request_uses_raw_api_key_and_completion_budget(self) -> None:
        provider_config = next(item for item in backend_app.VISION_PROVIDER_CONFIGS if item["name"] == "mimo")
        provider = {
            **provider_config,
            "api_key": "mimo-test-key",
            "base_url": "https://api.xiaomimimo.com/v1",
            "model": backend_app.resolve_vision_provider_model(provider_config),
        }

        request_payload = backend_app.build_vision_provider_probe_request(provider)

        self.assertEqual(request_payload["provider_name"], "mimo")
        self.assertEqual(request_payload["api_style"], backend_app.VISION_API_STYLE_CHAT_COMPLETIONS)
        self.assertEqual(request_payload["url"], "https://api.xiaomimimo.com/v1/chat/completions")
        self.assertEqual(request_payload["headers"]["api-key"], "mimo-test-key")
        self.assertNotIn("Authorization", request_payload["headers"])
        self.assertEqual(request_payload["body"]["model"], backend_app.DEFAULT_MIMO_VISION_MODEL)
        self.assertEqual(
            request_payload["body"]["max_completion_tokens"],
            backend_app.DEFAULT_MIMO_MAX_COMPLETION_TOKENS,
        )
        self.assertEqual(request_payload["body"]["messages"][0]["content"][0]["type"], "text")

    def test_build_visual_review_prompt_prefers_active_bundle_platform_prompt(self) -> None:
        active_visual_prompts = {
            "instagram": {
                "prompt": "品牌定制视觉规则：命中 Tapo 家庭 / 宠物 / 户外生活内容特征才可通过。",
            }
        }

        with mock.patch.object(
            backend_app,
            "load_active_visual_prompts",
            return_value=active_visual_prompts,
        ):
            selection = backend_app.resolve_visual_review_prompt_selection("openai", "instagram", "gpt-5.4")
            prompt = backend_app.build_visual_review_prompt("openai", "instagram", "alpha", model_name="gpt-5.4")

        self.assertEqual(selection["source"], "platform_prompt")
        self.assertIn("品牌定制视觉规则", selection["prompt"])
        self.assertIn("品牌定制视觉规则", prompt)
        self.assertNotIn("重点排查", prompt)

    def test_visual_review_prompt_selection_falls_back_to_rulespec_visual_contract(self) -> None:
        active_rulespec = {
            "goal": "优先保留家庭生活感强的账号",
            "rules": [
                {
                    "type": "visual_feature_group",
                    "platform": "instagram",
                    "cover_count": 5,
                    "min_hit_features": 2,
                    "features": [
                        {"label": "家庭场景"},
                        {"label": "宠物陪伴"},
                        {"label": "户外生活"},
                    ],
                },
                {
                    "type": "green_screen",
                    "platforms": ["instagram"],
                },
            ],
        }

        selection = backend_app.resolve_visual_review_prompt_selection(
            "openai",
            "instagram",
            model_name="gpt-5.4",
            active_visual_prompts={},
            active_rulespec=active_rulespec,
        )

        self.assertEqual(selection["source"], "rulespec_visual_contract")
        self.assertEqual(selection["visual_contract_source"], "active_rulespec.rules")
        self.assertEqual(selection["resolved_cover_limit"], 5)
        self.assertEqual(selection["visual_runtime_contract"]["goal"], "优先保留家庭生活感强的账号")
        self.assertEqual(selection["visual_runtime_contract"]["positive_feature_labels"], ["家庭场景", "宠物陪伴", "户外生活"])
        self.assertIn("审核目标：优先保留家庭生活感强的账号", selection["prompt"])
        self.assertIn("优先确认是否命中以下至少 2 类视觉特征", selection["prompt"])
        self.assertIn("同时排除以下视觉风险", selection["prompt"])
        self.assertIn("出现绿幕背景", selection["prompt"])

    def test_visual_review_prompt_selection_prefers_provider_then_model_then_platform(self) -> None:
        active_visual_prompts = {
            "instagram": {
                "provider_prompts": {
                    "openai": "provider level prompt",
                },
                "model_prompts": {
                    "qwen-vl-max": "model level prompt",
                },
                "prompt": "platform level prompt",
            }
        }

        provider_selection = backend_app.resolve_visual_review_prompt_selection(
            "openai",
            "instagram",
            model_name="gpt-5.4",
            active_visual_prompts=active_visual_prompts,
        )
        model_selection = backend_app.resolve_visual_review_prompt_selection(
            "reelx",
            "instagram",
            model_name="qwen-vl-max",
            active_visual_prompts=active_visual_prompts,
        )
        platform_selection = backend_app.resolve_visual_review_prompt_selection(
            "mimo",
            "instagram",
            model_name="mimo-v2-omni",
            active_visual_prompts=active_visual_prompts,
        )

        self.assertEqual(provider_selection["source"], "provider_prompts")
        self.assertEqual(provider_selection["prompt"], "provider level prompt")
        self.assertEqual(model_selection["source"], "model_prompts")
        self.assertEqual(model_selection["prompt"], "model level prompt")
        self.assertEqual(platform_selection["source"], "platform_prompt")
        self.assertEqual(platform_selection["prompt"], "platform level prompt")

    def test_build_visual_review_prompt_keeps_generic_provider_fallbacks_without_active_bundle(self) -> None:
        with mock.patch.object(backend_app, "load_active_visual_prompts", return_value={}):
            mimo_selection = backend_app.resolve_visual_review_prompt_selection("mimo", "instagram", "mimo-v2-omni")
            qwen_selection = backend_app.resolve_visual_review_prompt_selection("reelx", "instagram", "qwen-vl-max")
            mimo_prompt = backend_app.build_visual_review_prompt("mimo", "instagram", "alpha")
            openai_prompt = backend_app.build_visual_review_prompt("openai", "instagram", "alpha")
            qwen_prompt = backend_app.build_visual_review_prompt("reelx", "instagram", "alpha", model_name="qwen-vl-max")

        self.assertEqual(mimo_selection["source"], "generic_fallback")
        self.assertEqual(qwen_selection["source"], "generic_fallback")
        self.assertIn("不要逐图解释", mimo_prompt)
        self.assertNotIn("不要逐图解释", openai_prompt)
        self.assertIn("不要为了写得完整而乱猜", qwen_prompt)
        self.assertIn("只有当多张图都出现强且直接的视觉证据时，才输出 Reject", qwen_prompt)
        self.assertIn("平台：Instagram", mimo_prompt)
        self.assertIn("达人：alpha", mimo_prompt)

    def test_minimal_visual_probe_image_meets_reelx_min_size_requirement(self) -> None:
        prefix = "data:image/png;base64,"
        self.assertTrue(backend_app.MINIMAL_VISUAL_REVIEW_PROBE_IMAGE_DATA_URL.startswith(prefix))

        payload = backend_app.MINIMAL_VISUAL_REVIEW_PROBE_IMAGE_DATA_URL[len(prefix):]
        png_bytes = base64.b64decode(payload)
        width, height = struct.unpack("!II", png_bytes[16:24])

        self.assertGreaterEqual(width, 16)
        self.assertGreaterEqual(height, 16)

    def test_evaluate_profile_visual_review_honors_requested_provider(self) -> None:
        with mock.patch.object(
            backend_app,
            "build_visual_review_candidate_cover_urls",
            return_value=["https://example.com/demo.jpg"],
        ), mock.patch.object(
            backend_app,
            "get_available_vision_providers",
            return_value=[
                {"name": "mimo", "api_key": "mimo-key"},
                {"name": "openai", "api_key": "openai-key"},
            ],
        ) as mocked_get_providers, mock.patch.object(
            backend_app,
            "call_vision_provider",
            return_value={
                "decision": "Pass",
                "reason": "ok",
                "signals": [],
                "provider": "mimo",
                "cover_count": 1,
                "candidate_cover_count": 1,
                "skipped_cover_count": 0,
            },
        ) as mocked_call:
            result = backend_app.evaluate_profile_visual_review(
                "instagram",
                {"username": "alpha"},
                requested_provider="mimo",
            )

        mocked_get_providers.assert_called_once_with("mimo")
        self.assertEqual(mocked_call.call_args.args[0]["name"], "mimo")
        self.assertEqual(result["provider"], "mimo")

    def test_extract_vision_response_text_reads_chat_completion_string_content(self) -> None:
        payload = {
            "choices": [
                {
                    "message": {
                        "content": "{\"decision\":\"Pass\",\"reason\":\"图片正常\",\"signals\":[\"画面清晰\"]}",
                        "role": "assistant",
                        "reasoning_content": "ignored",
                    }
                }
            ]
        }

        raw_text = backend_app.extract_vision_response_text(payload)
        parsed = backend_app.parse_visual_review_result(raw_text)

        self.assertEqual(raw_text, "{\"decision\":\"Pass\",\"reason\":\"图片正常\",\"signals\":[\"画面清晰\"]}")
        self.assertEqual(parsed["decision"], "Pass")
        self.assertEqual(parsed["reason"], "图片正常")
        self.assertEqual(parsed["signals"], ["画面清晰"])

    def test_parse_streaming_chat_completion_payload_reads_sse_chunks(self) -> None:
        raw_text = """data: {"id":"x","object":"chat.completion.chunk","created":1,"model":"gpt-5.4","choices":[{"index":0,"delta":{"role":"assistant","content":"{\\"decision\\":\\"Pass\\""},"finish_reason":null}]}

data: {"id":"x","object":"chat.completion.chunk","created":1,"model":"gpt-5.4","choices":[{"index":0,"delta":{"content":",\\"reason\\":\\"ok\\",\\"signals\\":[]}"}}]}

data: {"id":"x","object":"chat.completion.chunk","created":1,"model":"gpt-5.4","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}

data: [DONE]
"""
        payload = backend_app.parse_streaming_chat_completion_payload(raw_text)
        raw_response_text = backend_app.extract_vision_response_text(payload)
        parsed = backend_app.parse_visual_review_result(raw_response_text)

        self.assertEqual(payload["model"], "gpt-5.4")
        self.assertEqual(raw_response_text, '{"decision":"Pass","reason":"ok","signals":[]}')
        self.assertEqual(parsed["decision"], "Pass")
        self.assertEqual(parsed["reason"], "ok")

    def test_parse_visual_review_result_repairs_mojibake_reason(self) -> None:
        raw_text = '{"decision":"Pass","reason":"ç»é¢æ­£å¸¸","signals":["æ é«é£é©ä¿¡å·"]}'

        parsed = backend_app.parse_visual_review_result(raw_text)

        self.assertEqual(parsed["decision"], "Pass")
        self.assertEqual(parsed["reason"], "画面正常")
        self.assertEqual(parsed["signals"], ["无高风险信号"])

    def test_parse_vision_provider_response_payload_prefers_utf8_bytes_over_broken_text(self) -> None:
        raw_sse = (
            'data: {"id":"x","object":"chat.completion.chunk","created":1,"model":"gpt-5.4","choices":[{"index":0,"delta":{"role":"assistant","content":"{\\"decision\\":\\"Pass\\",\\"reason\\":\\"画面正常\\",\\"signals\\":[\\"无高风险信号\\"]}"},"finish_reason":"stop"}]}\n'
            "data: [DONE]\n"
        )
        response = DummyProviderResponse(
            ValueError("not json"),
            status_code=200,
            text='data: {"choices":[{"delta":{"content":"{\\"decision\\":\\"Pass\\",\\"reason\\":\\"ç»é¢æ­£å¸¸\\"}"}}]}\n',
            content=raw_sse.encode("utf-8"),
        )

        payload = backend_app.parse_vision_provider_response_payload(response)
        raw_text = backend_app.extract_vision_response_text(payload)
        parsed = backend_app.parse_visual_review_result(raw_text)

        self.assertEqual(parsed["reason"], "画面正常")

    def test_extract_vision_provider_text_error_detects_embedded_provider_failure(self) -> None:
        parsed = backend_app.extract_vision_provider_text_error("codex: status=500 Internal Server Error")

        self.assertEqual(parsed["status_code"], 500)
        self.assertTrue(parsed["retryable"])
        self.assertEqual(parsed["message"], "codex: status=500 Internal Server Error")

    def test_extract_vision_provider_text_error_detects_not_supported_400(self) -> None:
        parsed = backend_app.extract_vision_provider_text_error("codex: status=400 qwen-vl-max is not supported")

        self.assertEqual(parsed["status_code"], 400)
        self.assertFalse(parsed["retryable"])
        self.assertEqual(parsed["message"], "codex: status=400 qwen-vl-max is not supported")

    def test_call_vision_provider_supports_reelx_generate_content(self) -> None:
        provider = {
            "name": "reelx",
            "base_url": "https://llmxapi.com/v1beta",
            "api_key": "reelx-test-key",
            "api_style": backend_app.VISION_API_STYLE_GENERATE_CONTENT,
            "default_model": "qwen-vl-max",
            "default_fallback_model": "gemini-3-flash-preview",
        }
        response = DummyProviderResponse(
            {
                "candidates": [
                    {
                        "content": {
                            "parts": [
                                {
                                    "text": '{"decision":"Pass","reason":"画面正常","signals":["无高风险信号"]}'
                                }
                            ]
                        }
                    }
                ]
            },
            status_code=200,
        )

        with mock.patch.object(backend_app.requests, "post", return_value=response) as mocked_post:
            parsed = backend_app.call_vision_provider(
                provider,
                "instagram",
                "alpha",
                ["data:image/jpeg;base64,ZmFrZQ=="],
            )

        self.assertEqual(parsed["decision"], "Pass")
        self.assertEqual(parsed["provider"], "reelx")
        self.assertEqual(parsed["model"], "qwen-vl-max")
        self.assertEqual(parsed["configured_model"], "qwen-vl-max")
        self.assertEqual(parsed["requested_model"], "qwen-vl-max")
        self.assertEqual(parsed["effective_model"], "qwen-vl-max")
        self.assertEqual(
            mocked_post.call_args.kwargs["json"]["generationConfig"]["responseMimeType"],
            "application/json",
        )
        self.assertEqual(
            mocked_post.call_args.args[0],
            "https://llmxapi.com/v1beta/models/qwen-vl-max:generateContent",
        )

    def test_call_vision_provider_retries_reelx_across_base_urls_before_fallback_model(self) -> None:
        provider = {
            "name": "reelx",
            "base_url": "https://llmxapi.com/v1beta",
            "base_url_candidates": [
                "https://llmxapi.com/v1beta",
                "https://reelxai.com/v1beta",
            ],
            "api_key": "reelx-test-key",
            "api_style": backend_app.VISION_API_STYLE_GENERATE_CONTENT,
            "default_model": "qwen-vl-max",
            "default_fallback_model": "gemini-3-flash-preview",
        }
        response = DummyProviderResponse(
            {
                "modelVersion": "qwen-vl-max",
                "candidates": [
                    {
                        "content": {
                            "parts": [
                                {
                                    "text": '{"decision":"Pass","reason":"画面正常","signals":["无高风险信号"]}'
                                }
                            ]
                        }
                    }
                ]
            },
            status_code=200,
        )

        with mock.patch.object(
            backend_app.requests,
            "post",
            side_effect=[backend_app.requests.exceptions.ReadTimeout("read timed out"), response],
        ) as mocked_post:
            parsed = backend_app.call_vision_provider(
                provider,
                "instagram",
                "alpha",
                ["data:image/jpeg;base64,ZmFrZQ=="],
            )

        self.assertEqual(parsed["decision"], "Pass")
        self.assertEqual(parsed["model"], "qwen-vl-max")
        self.assertEqual(parsed["base_url"], "https://reelxai.com/v1beta")
        self.assertEqual(mocked_post.call_args_list[0].args[0], "https://llmxapi.com/v1beta/models/qwen-vl-max:generateContent")
        self.assertEqual(mocked_post.call_args_list[1].args[0], "https://reelxai.com/v1beta/models/qwen-vl-max:generateContent")

    def test_call_vision_provider_rejects_non_json_visual_contract(self) -> None:
        provider = {
            "name": "reelx",
            "base_url": "https://llmxapi.com/v1beta",
            "api_key": "reelx-test-key",
            "api_style": backend_app.VISION_API_STYLE_GENERATE_CONTENT,
            "default_model": "qwen-vl-max",
            "default_fallback_model": "gemini-3-flash-preview",
        }
        response = DummyProviderResponse(
            {
                "modelVersion": "qwen-vl-max",
                "candidates": [
                    {
                        "content": {
                            "parts": [{"text": "{\n  \"decision"}]
                        }
                    }
                ]
            },
            status_code=200,
        )

        with mock.patch.object(backend_app.requests, "post", return_value=response):
            with self.assertRaises(backend_app.VisionProviderError) as ctx:
                backend_app.call_vision_provider(
                    provider,
                    "instagram",
                    "alpha",
                    ["data:image/jpeg;base64,ZmFrZQ=="],
                )

        self.assertIn("合法的视觉 JSON contract", str(ctx.exception))

    def test_probe_vision_provider_retries_reelx_across_base_urls(self) -> None:
        provider = {
            "name": "reelx",
            "base_url": "https://llmxapi.com/v1beta",
            "base_url_candidates": [
                "https://llmxapi.com/v1beta",
                "https://hk.llmxapi.com/v1beta",
            ],
            "api_key": "reelx-test-key",
            "api_style": backend_app.VISION_API_STYLE_GENERATE_CONTENT,
            "model": "qwen-vl-max",
            "default_model": "qwen-vl-max",
        }
        response = DummyProviderResponse(
            {
                "candidates": [
                    {
                        "content": {
                            "parts": [{"text": "ok"}]
                        }
                    }
                ]
            },
            status_code=200,
        )

        with mock.patch.object(
            backend_app.requests,
            "post",
            side_effect=[backend_app.requests.exceptions.ReadTimeout("read timed out"), response],
        ) as mocked_post:
            result = backend_app.probe_vision_provider(provider)

        self.assertTrue(result["success"])
        self.assertEqual(result["base_url"], "https://hk.llmxapi.com/v1beta")
        self.assertEqual(mocked_post.call_args_list[0].args[0], "https://llmxapi.com/v1beta/models/qwen-vl-max:generateContent")
        self.assertEqual(mocked_post.call_args_list[1].args[0], "https://hk.llmxapi.com/v1beta/models/qwen-vl-max:generateContent")

    def test_call_vision_provider_sanitizes_html_413_without_base_url_leak(self) -> None:
        provider = {
            "name": "reelx",
            "base_url": "https://hk.reelxai.com/v1beta",
            "api_key": "reelx-test-key",
            "api_style": backend_app.VISION_API_STYLE_GENERATE_CONTENT,
            "default_model": "qwen-vl-max",
            "default_fallback_model": "gemini-3-flash-preview",
        }
        response = DummyProviderResponse(
            ValueError("not json"),
            status_code=413,
            text="<html><head><title>413 Request Entity Too Large</title></head><body><center><h1>413 Request Entity Too Large</h1></center></body></html>",
            headers={"Content-Type": "text/html"},
        )

        with mock.patch.object(backend_app.requests, "post", return_value=response):
            with self.assertRaises(backend_app.VisionProviderError) as ctx:
                backend_app.call_vision_provider(
                    provider,
                    "instagram",
                    "alpha",
                    ["data:image/jpeg;base64,ZmFrZQ=="],
                )

        message = str(ctx.exception)
        self.assertIn("reelx: HTTP 413", message)
        self.assertIn("请求体过大", message)
        self.assertIn("HTML 错误页", message)
        self.assertNotIn("https://hk.reelxai.com/v1beta", message)
        self.assertNotIn("<html>", message)

    def test_call_vision_provider_falls_back_to_secondary_qiandao_model(self) -> None:
        provider = {
            "name": "qiandao",
            "base_url": "https://api2.qiandao.mom/v1",
            "api_key": "qiandao-test-key",
            "api_style": backend_app.VISION_API_STYLE_CHAT_COMPLETIONS,
            "default_model": "gemini-2.5-pro-preview-p",
            "default_fallback_model": "gemini-3-flash-preview-S",
        }
        first_response = DummyProviderResponse(
            {"error": {"message": "bad gateway"}},
            status_code=502,
            text='{"error":{"message":"bad gateway"}}',
        )
        second_response = DummyProviderResponse(
            {
                "model": "gemini-3-flash-preview-S",
                "choices": [
                    {
                        "message": {
                            "content": '{"decision":"Pass","reason":"画面正常","signals":["无高风险信号"]}'
                        }
                    }
                ]
            },
            status_code=200,
        )

        with mock.patch.object(backend_app.requests, "post", side_effect=[first_response, second_response]) as mocked_post:
            parsed = backend_app.call_vision_provider(
                provider,
                "instagram",
                "alpha",
                ["data:image/jpeg;base64,ZmFrZQ=="],
            )

        self.assertEqual(parsed["decision"], "Pass")
        self.assertEqual(parsed["model"], "gemini-3-flash-preview-S")
        self.assertEqual(parsed["configured_model"], "gemini-2.5-pro-preview-p")
        self.assertEqual(parsed["requested_model"], "gemini-3-flash-preview-S")
        self.assertEqual(parsed["response_model"], "gemini-3-flash-preview-S")
        self.assertEqual(parsed["effective_model"], "gemini-3-flash-preview-S")
        self.assertEqual(mocked_post.call_args_list[0].kwargs["json"]["model"], "gemini-2.5-pro-preview-p")
        self.assertEqual(mocked_post.call_args_list[1].kwargs["json"]["model"], "gemini-3-flash-preview-S")

    def test_tiered_routing_escalates_borderline_primary_to_backup(self) -> None:
        def fake_get_available(provider_name=None):
            provider_name = (provider_name or "").strip().lower()
            if provider_name == "reelx":
                return [{"name": "reelx", "base_url": "https://reelxai.com/v1beta", "api_key": "key", "api_style": backend_app.VISION_API_STYLE_GENERATE_CONTENT}]
            if provider_name == "openai":
                return [{"name": "openai", "base_url": "https://example.com/v1", "api_key": "key", "api_style": backend_app.VISION_API_STYLE_RESPONSES}]
            return []

        with mock.patch.object(
            backend_app,
            "build_visual_review_candidate_cover_urls",
            return_value=["data:image/jpeg;base64,ZmFrZQ=="],
        ), mock.patch.object(
            backend_app,
            "get_available_vision_providers",
            side_effect=fake_get_available,
        ), mock.patch.object(
            backend_app,
            "call_vision_provider",
            side_effect=[
                {
                    "decision": "Pass",
                    "reason": "可能存在边界情况",
                    "signals": ["可能擦边"],
                    "model": "gemini-3-flash-preview-S",
                    "provider": "reelx",
                },
                {
                    "decision": "Reject",
                    "reason": "存在明确高风险视觉信号",
                    "signals": ["明显暴露"],
                    "model": "gemini-2.5-pro-preview-p",
                    "provider": "reelx",
                },
            ],
        ):
            result = backend_app.evaluate_profile_visual_review(
                "instagram",
                {"username": "alpha", "upload_metadata": {}},
                routing_strategy="tiered",
            )

        self.assertEqual(result["route"], "backup")
        self.assertFalse(result["judge_used"])
        self.assertEqual(len(result["trace"]), 2)
        self.assertEqual(result["trace"][0]["stage"], "primary")
        self.assertEqual(result["trace"][0]["escalation_reasons"], ["borderline_output"])
        self.assertEqual(result["trace"][1]["stage"], "backup")
        self.assertEqual(result["decision"], "Reject")

    def test_tiered_routing_escalates_to_judge_after_primary_error_and_invalid_backup(self) -> None:
        def fake_get_available(provider_name=None):
            provider_name = (provider_name or "").strip().lower()
            if provider_name == "reelx":
                return [{"name": "reelx", "base_url": "https://reelxai.com/v1beta", "api_key": "key", "api_style": backend_app.VISION_API_STYLE_GENERATE_CONTENT}]
            if provider_name == "openai":
                return [{"name": "openai", "base_url": "https://example.com/v1", "api_key": "key", "api_style": backend_app.VISION_API_STYLE_RESPONSES}]
            return []

        with mock.patch.object(
            backend_app,
            "build_visual_review_candidate_cover_urls",
            return_value=["data:image/jpeg;base64,ZmFrZQ=="],
        ), mock.patch.object(
            backend_app,
            "get_available_vision_providers",
            side_effect=fake_get_available,
        ), mock.patch.object(
            backend_app,
            "call_vision_provider",
            side_effect=[
                RuntimeError("primary timeout"),
                {
                    "decision": "Pass",
                    "reason": "",
                    "signals": [],
                    "model": "gemini-2.5-pro-preview-p",
                    "provider": "reelx",
                },
                {
                    "decision": "Reject",
                    "reason": "高价值账号复判后判定拒绝",
                    "signals": ["终审拒绝"],
                    "model": "gpt-5.4",
                    "provider": "openai",
                },
            ],
        ):
            result = backend_app.evaluate_profile_visual_review(
                "instagram",
                {"username": "alpha", "upload_metadata": {}},
                routing_strategy="tiered",
            )

        self.assertEqual(result["route"], "judge")
        self.assertTrue(result["judge_used"])
        self.assertEqual([item["stage"] for item in result["trace"]], ["primary", "backup", "judge"])
        self.assertFalse(result["trace"][0]["ok"])
        self.assertEqual(result["trace"][1]["escalation_reasons"], ["missing_reason", "missing_signals"])
        self.assertEqual(result["decision"], "Reject")

    def test_probe_ranked_race_prefers_gpt54_when_all_candidates_probe_success(self) -> None:
        def fake_get_runnable(provider_name, *, model="", timeout_seconds=None):
            return {
                "name": provider_name,
                "model": model,
                "default_model": model,
                "request_timeout_seconds": timeout_seconds,
            }

        def fake_probe(provider, platform="instagram", cover_urls=None):
            return {
                "success": True,
                "provider": provider["name"],
                "model": provider["model"],
                "checked_at": "2026-03-28T00:00:00Z",
                "decision": "Pass",
                "reason": "ok",
                "signals": ["ok"],
                "response_excerpt": "ok",
            }

        with mock.patch.object(backend_app, "get_runnable_vision_provider", side_effect=fake_get_runnable), mock.patch.object(
            backend_app,
            "probe_vision_provider_with_image",
            side_effect=fake_probe,
        ):
            race = backend_app.run_probe_ranked_visual_provider_race()

        self.assertTrue(race["success"])
        self.assertEqual(race["selected_stage"], "preferred")
        self.assertEqual(race["selected_provider"], "openai")
        self.assertEqual(race["selected_model"], "gpt-5.4")
        self.assertFalse(race["dual_active_enabled"])
        self.assertEqual(
            [item["stage"] for item in race["active_preferred_candidates"]],
            ["preferred"],
        )
        self.assertEqual(
            [item["stage"] for item in race["candidates"]],
            ["preferred", "preferred_parallel", "secondary", "tertiary"],
        )
        self.assertEqual(race["candidates"][0]["configured_model"], "gpt-5.4")
        self.assertEqual(race["candidates"][0]["requested_model"], "gpt-5.4")
        self.assertEqual(race["candidates"][0]["effective_model"], "gpt-5.4")
        self.assertEqual(race["active_preferred_candidates"][0]["configured_model"], "gpt-5.4")
        self.assertEqual(race["active_preferred_candidates"][0]["requested_model"], "gpt-5.4")
        self.assertEqual(race["active_preferred_candidates"][0]["effective_model"], "gpt-5.4")
        self.assertEqual(race["candidates"][1]["configured_model"], "qwen-vl-max")
        self.assertEqual(race["candidates"][1]["requested_model"], "qwen-vl-max")
        self.assertEqual(race["candidates"][1]["effective_model"], "qwen-vl-max")

    def test_probe_ranked_race_falls_back_to_qwen_when_preferred_probe_fails(self) -> None:
        def fake_get_runnable(provider_name, *, model="", timeout_seconds=None):
            return {
                "name": provider_name,
                "model": model,
                "default_model": model,
                "request_timeout_seconds": timeout_seconds,
            }

        def fake_probe(provider, platform="instagram", cover_urls=None):
            if provider["name"] == "openai":
                raise RuntimeError("503 no channel")
            return {
                "success": True,
                "provider": provider["name"],
                "model": provider["model"],
                "checked_at": "2026-03-28T00:00:00Z",
                "decision": "Pass",
                "reason": "ok",
                "signals": ["ok"],
                "response_excerpt": "ok",
            }

        with mock.patch.object(backend_app, "get_runnable_vision_provider", side_effect=fake_get_runnable), mock.patch.object(
            backend_app,
            "probe_vision_provider_with_image",
            side_effect=fake_probe,
        ):
            race = backend_app.run_probe_ranked_visual_provider_race()

        self.assertTrue(race["success"])
        self.assertEqual(race["selected_stage"], "preferred_parallel")
        self.assertEqual(race["selected_provider"], "reelx")
        self.assertEqual(race["selected_model"], "qwen-vl-max")
        self.assertFalse(race["candidates"][0]["ok"])
        self.assertTrue(race["candidates"][1]["ok"])
        self.assertEqual(race["candidates"][1]["configured_model"], "qwen-vl-max")
        self.assertEqual(race["candidates"][1]["requested_model"], "qwen-vl-max")
        self.assertEqual(race["candidates"][1]["effective_model"], "qwen-vl-max")

    def test_probe_ranked_race_keeps_gemini_after_qwen_in_fallback_order(self) -> None:
        def fake_get_runnable(provider_name, *, model="", timeout_seconds=None):
            return {
                "name": provider_name,
                "model": model,
                "default_model": model,
                "request_timeout_seconds": timeout_seconds,
            }

        def fake_probe(provider, platform="instagram", cover_urls=None):
            return {
                "success": True,
                "provider": provider["name"],
                "model": provider["model"],
                "checked_at": "2026-03-28T00:00:00Z",
                "decision": "Pass",
                "reason": "ok",
                "signals": ["ok"],
                "response_excerpt": "ok",
            }

        with mock.patch.object(backend_app, "get_runnable_vision_provider", side_effect=fake_get_runnable), mock.patch.object(
            backend_app,
            "probe_vision_provider_with_image",
            side_effect=fake_probe,
        ):
            race = backend_app.run_probe_ranked_visual_provider_race()

        self.assertEqual(
            [(item["provider"], item["model"]) for item in race["fallback_candidates"]],
            [
                ("reelx", "qwen-vl-max"),
                ("reelx", "gemini-3-pro-preview"),
                ("reelx", "gemini-3-flash-preview"),
            ],
        )

    def test_probe_ranked_candidate_order_shards_across_dual_preferred_pool(self) -> None:
        race = {
            "success": True,
            "selected_stage": "preferred",
            "selected_provider": "openai",
            "selected_model": "gpt-5.4",
            "candidates": [
                {
                    "stage": "preferred",
                    "group": backend_app.VISUAL_REVIEW_PROBE_RANKED_GROUP_PREFERRED,
                    "provider": "openai",
                    "model": "gpt-5.4",
                    "timeout_seconds": 30,
                    "ok": True,
                },
                {
                    "stage": "preferred_parallel",
                    "group": backend_app.VISUAL_REVIEW_PROBE_RANKED_GROUP_PREFERRED,
                    "provider": "reelx",
                    "model": "gpt-5.4",
                    "timeout_seconds": 30,
                    "ok": True,
                },
                {
                    "stage": "secondary",
                    "group": backend_app.VISUAL_REVIEW_PROBE_RANKED_GROUP_FALLBACK,
                    "provider": "reelx",
                    "model": "gemini-2.5-pro-preview-p",
                    "timeout_seconds": 25,
                    "ok": True,
                },
            ],
        }

        first_stages = {
            backend_app.build_probe_ranked_candidate_order("instagram", {"username": f"user-{index}"}, race)[0]["stage"]
            for index in range(20)
        }

        self.assertEqual(first_stages, {"preferred", "preferred_parallel"})

    def test_probe_ranked_visual_review_falls_back_per_item_after_primary_failure(self) -> None:
        race = {
            "success": True,
            "selected_stage": backend_app.VISUAL_REVIEW_PROBE_RANKED_SELECTED_STAGE_PREFERRED_POOL,
            "selected_provider": "openai",
            "selected_model": "gpt-5.4",
            "candidates": [
                {
                    "stage": "preferred",
                    "group": backend_app.VISUAL_REVIEW_PROBE_RANKED_GROUP_PREFERRED,
                    "provider": "openai",
                    "model": "gpt-5.4",
                    "timeout_seconds": 30,
                    "ok": True,
                    "selected": True,
                },
                {
                    "stage": "preferred_parallel",
                    "group": backend_app.VISUAL_REVIEW_PROBE_RANKED_GROUP_FALLBACK,
                    "provider": "reelx",
                    "model": "qwen-vl-max",
                    "timeout_seconds": 30,
                    "ok": True,
                    "selected": False,
                },
                {
                    "stage": "secondary",
                    "group": backend_app.VISUAL_REVIEW_PROBE_RANKED_GROUP_FALLBACK,
                    "provider": "reelx",
                    "model": "gemini-2.5-pro-preview-p",
                    "timeout_seconds": 25,
                    "ok": True,
                    "selected": False,
                },
            ],
        }

        def fake_get_runnable(provider_name, *, model="", timeout_seconds=None):
            return {
                "name": provider_name,
                "model": model,
                "default_model": model,
                "request_timeout_seconds": timeout_seconds,
            }

        def fake_call(provider, platform, username, cover_urls):
            if provider["name"] == "openai":
                raise RuntimeError("openai 503")
            return {
                "decision": "Pass",
                "reason": "备用 5.4 通道成功",
                "signals": ["fallback"],
                "provider": provider["name"],
                "model": provider["model"],
            }

        with mock.patch.object(
            backend_app,
            "build_visual_review_candidate_cover_urls",
            return_value=["data:image/jpeg;base64,ZmFrZQ=="],
        ), mock.patch.object(
            backend_app,
            "build_probe_ranked_candidate_order",
            return_value=race["candidates"],
        ), mock.patch.object(
            backend_app,
            "get_runnable_vision_provider",
            side_effect=fake_get_runnable,
        ), mock.patch.object(
            backend_app,
            "call_vision_provider",
            side_effect=fake_call,
        ):
            result = backend_app.evaluate_profile_visual_review(
                "instagram",
                {"username": "alpha", "upload_metadata": {}},
                routing_strategy="probe_ranked",
                routing_context=race,
            )

        self.assertEqual(result["route"], "preferred_parallel")
        self.assertEqual(result["provider"], "reelx")
        self.assertEqual(result["model"], "qwen-vl-max")
        self.assertEqual(result["trace"][0]["configured_model"], "gpt-5.4")
        self.assertEqual(result["trace"][0]["requested_model"], "gpt-5.4")
        self.assertEqual([item["stage"] for item in result["trace"]], ["preferred", "preferred_parallel"])
        self.assertEqual(result["channel_race"]["selected_provider"], "openai")
        self.assertEqual(result["decision"], "Pass")

    def test_probe_ranked_visual_review_retries_preferred_pool_after_full_retryable_first_pass(self) -> None:
        race = {
            "success": True,
            "selected_stage": "preferred",
            "selected_provider": "openai",
            "selected_model": "gpt-5.4",
            "candidates": [
                {
                    "stage": "preferred",
                    "group": backend_app.VISUAL_REVIEW_PROBE_RANKED_GROUP_PREFERRED,
                    "provider": "openai",
                    "model": "gpt-5.4",
                    "timeout_seconds": 30,
                    "ok": True,
                },
                {
                    "stage": "preferred_parallel",
                    "group": backend_app.VISUAL_REVIEW_PROBE_RANKED_GROUP_FALLBACK,
                    "provider": "reelx",
                    "model": "qwen-vl-max",
                    "timeout_seconds": 30,
                    "ok": True,
                },
                {
                    "stage": "secondary",
                    "group": backend_app.VISUAL_REVIEW_PROBE_RANKED_GROUP_FALLBACK,
                    "provider": "reelx",
                    "model": "gemini-2.5-pro-preview-p",
                    "timeout_seconds": 25,
                    "ok": True,
                },
            ],
        }

        def fake_get_runnable(provider_name, *, model="", timeout_seconds=None):
            return {
                "name": provider_name,
                "model": model,
                "default_model": model,
                "request_timeout_seconds": timeout_seconds,
            }

        call_log = []

        def fake_call(provider, platform, username, cover_urls):
            provider_name = provider["name"]
            call_log.append(provider_name)
            if call_log == ["openai"]:
                raise backend_app.VisionProviderError(provider_name, "HTTP 503 upstream connect error", status_code=503, retryable=True)
            if call_log == ["openai", "reelx"]:
                raise backend_app.VisionProviderError(provider_name, "HTTP 522", status_code=522, retryable=True)
            if call_log == ["openai", "reelx", "reelx"]:
                raise backend_app.VisionProviderError(provider_name, "write operation timed out", retryable=True)
            return {
                "decision": "Pass",
                "reason": "优先池二次重试成功",
                "signals": ["retry-recovered"],
                "provider": provider_name,
                "model": provider["model"],
                "configured_model": provider["model"],
                "requested_model": provider["model"],
                "response_model": provider["model"],
                "effective_model": provider["model"],
            }

        with mock.patch.object(
            backend_app,
            "build_visual_review_candidate_cover_urls",
            return_value=["data:image/jpeg;base64,ZmFrZQ=="],
        ), mock.patch.object(
            backend_app,
            "build_probe_ranked_candidate_order",
            return_value=race["candidates"],
        ), mock.patch.object(
            backend_app,
            "get_runnable_vision_provider",
            side_effect=fake_get_runnable,
        ), mock.patch.object(
            backend_app,
            "call_vision_provider",
            side_effect=fake_call,
        ):
            result = backend_app.evaluate_profile_visual_review(
                "instagram",
                {"username": "alpha", "upload_metadata": {}},
                routing_strategy="probe_ranked",
                routing_context=race,
            )

        self.assertEqual(call_log, ["openai", "reelx", "reelx", "openai"])
        self.assertEqual(result["route"], "preferred")
        self.assertEqual(result["provider"], "openai")
        self.assertEqual(result["decision"], "Pass")
        self.assertEqual(
            [item["stage"] for item in result["trace"]],
            ["preferred", "preferred_parallel", "secondary", "preferred"],
        )
        self.assertTrue(result["trace"][0]["retryable"])
        self.assertTrue(result["trace"][1]["retryable"])
        self.assertTrue(result["trace"][2]["retryable"])
        self.assertEqual(result["trace"][3]["effective_model"], "gpt-5.4")

    def test_probe_ranked_runtime_disables_unhealthy_stage_after_threshold(self) -> None:
        runtime_context = backend_app.build_probe_ranked_runtime_context(
            {
                "success": True,
                "selected_stage": "preferred",
                "selected_provider": "openai",
                "selected_model": "gpt-5.4",
                "candidates": [
                    {
                        "stage": "preferred",
                        "group": backend_app.VISUAL_REVIEW_PROBE_RANKED_GROUP_PREFERRED,
                        "provider": "openai",
                        "model": "gpt-5.4",
                        "timeout_seconds": 30,
                        "ok": True,
                    },
                    {
                        "stage": "preferred_parallel",
                        "group": backend_app.VISUAL_REVIEW_PROBE_RANKED_GROUP_FALLBACK,
                        "provider": "reelx",
                        "model": "qwen-vl-max",
                        "timeout_seconds": 30,
                        "ok": True,
                    },
                ],
            }
        )
        candidate = runtime_context["channel_race"]["candidates"][0]

        backend_app.record_probe_ranked_candidate_outcome(runtime_context, candidate, ok=False)
        backend_app.record_probe_ranked_candidate_outcome(runtime_context, candidate, ok=False)

        snapshot = backend_app.snapshot_probe_ranked_channel_race(runtime_context)
        ordered = backend_app.build_probe_ranked_candidate_order("instagram", {"username": "alpha"}, runtime_context)

        self.assertEqual(snapshot["runtime_disabled_stages"], ["preferred"])
        self.assertEqual(ordered[0]["stage"], "preferred_parallel")

    def test_should_retry_vision_payload_for_mimo_truncated_empty_content(self) -> None:
        payload = {
            "choices": [
                {
                    "finish_reason": "length",
                    "message": {
                        "content": "",
                        "role": "assistant",
                        "reasoning_content": "very long reasoning",
                    },
                }
            ]
        }

        self.assertTrue(backend_app.should_retry_vision_payload("mimo", payload))
        self.assertFalse(backend_app.should_retry_vision_payload("openai", payload))

    def test_should_not_retry_vision_payload_when_text_content_exists(self) -> None:
        payload = {
            "choices": [
                {
                    "finish_reason": "length",
                    "message": {
                        "content": "{\"decision\":\"Pass\",\"reason\":\"ok\",\"signals\":[]}",
                        "role": "assistant",
                    },
                }
            ]
        }

        self.assertFalse(backend_app.should_retry_vision_payload("mimo", payload))

    def test_extract_vision_usage_reads_chat_completion_usage_details(self) -> None:
        payload = {
            "usage": {
                "prompt_tokens": 1200,
                "completion_tokens": 80,
                "total_tokens": 1280,
                "completion_tokens_details": {"reasoning_tokens": 64},
                "prompt_tokens_details": {"image_tokens": 900, "cached_tokens": 12},
            }
        }

        usage = backend_app.extract_vision_usage(payload)

        self.assertEqual(
            usage,
            {
                "prompt_tokens": 1200,
                "completion_tokens": 80,
                "total_tokens": 1280,
                "reasoning_tokens": 64,
                "image_tokens": 900,
                "cached_tokens": 12,
            },
        )

    def test_summarize_visual_usage_sums_usage_blocks(self) -> None:
        summary = backend_app.summarize_visual_usage(
            {
                "alpha": {
                    "usage": {
                        "prompt_tokens": 100,
                        "completion_tokens": 20,
                        "total_tokens": 120,
                        "reasoning_tokens": 10,
                        "image_tokens": 70,
                        "cached_tokens": 3,
                    }
                },
                "beta": {
                    "usage": {
                        "prompt_tokens": 200,
                        "completion_tokens": 30,
                        "total_tokens": 230,
                        "reasoning_tokens": 15,
                        "image_tokens": 120,
                        "cached_tokens": 5,
                    }
                },
            }
        )

        self.assertEqual(
            summary,
            {
                "prompt_tokens": 300,
                "completion_tokens": 50,
                "total_tokens": 350,
                "reasoning_tokens": 25,
                "image_tokens": 190,
                "cached_tokens": 8,
            },
        )


if __name__ == "__main__":
    unittest.main()
