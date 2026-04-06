from __future__ import annotations

import tempfile
import unittest
from concurrent.futures import Future
from pathlib import Path
from unittest import mock

import backend.app as backend_app
import backend.creator_cache as creator_cache_module


class PositioningAnalysisCacheTests(unittest.TestCase):
    def test_perform_positioning_card_analysis_skips_cached_targets(self) -> None:
        class ImmediateExecutor:
            def __init__(self, *args, **kwargs) -> None:
                self.submissions = []

            def submit(self, fn, *args, **kwargs):
                future = Future()
                future.set_result(fn(*args, **kwargs))
                self.submissions.append((fn, args, kwargs))
                return future

            def shutdown(self, wait=False, cancel_futures=False) -> None:
                return None

        cached_result = {
            "success": True,
            "username": "cached_user",
            "fit_recommendation": "High Fit",
            "positioning_labels": ["家居"],
            "fit_summary": "cached",
            "reviewed_at": "2026-04-06T00:00:00+00:00",
        }
        fresh_result = {
            "fit_recommendation": "Medium Fit",
            "positioning_labels": ["收纳"],
            "fit_summary": "fresh",
            "evidence_signals": ["口播测评"],
            "provider": "openai",
            "effective_model": "gpt-5.4",
            "reviewed_at": "2026-04-06T01:00:00+00:00",
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "creator_cache.db"
            creator_cache_module.persist_positioning_cache_entry(
                "tiktok",
                "cached_user",
                cached_result,
                db_path,
                updated_at="2026-04-06T00:00:00+00:00",
                context_key="ctx-positioning",
                context_payload={"version": "test"},
            )
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
                "resolve_positioning_card_analysis_targets",
                return_value=[
                    {"username": "cached_user", "profile_url": "https://www.tiktok.com/@cached_user", "_visual_result": {"decision": "Pass"}},
                    {"username": "fresh_user", "profile_url": "https://www.tiktok.com/@fresh_user", "_visual_result": {"decision": "Pass"}},
                ],
            ), mock.patch.object(
                backend_app,
                "load_positioning_card_results",
                return_value={},
            ), mock.patch.object(
                backend_app,
                "merge_upload_metadata_into_review_item",
                side_effect=lambda platform, review_item: review_item,
            ), mock.patch.object(
                backend_app,
                "build_positioning_card_cache_context",
                return_value={
                    "context_key": "ctx-positioning",
                    "context_payload": {"version": "test"},
                },
            ), mock.patch.object(
                backend_app,
                "evaluate_profile_positioning_card_analysis",
                return_value=fresh_result,
            ) as mocked_evaluate, mock.patch.object(
                backend_app,
                "save_positioning_card_results",
            ), mock.patch.object(
                backend_app,
                "DaemonThreadPoolExecutor",
                ImmediateExecutor,
            ):
                result = backend_app.perform_positioning_card_analysis(
                    "tiktok",
                    {
                        "provider": "openai",
                        "creator_cache_db_path": str(db_path),
                        "use_creator_cache": True,
                    },
                )

        self.assertTrue(result["success"])
        self.assertEqual(mocked_evaluate.call_count, 1)
        self.assertEqual(result["creator_cache"]["positioning_hit_count"], 1)
        self.assertEqual(result["creator_cache"]["positioning_miss_count"], 1)
        self.assertEqual(result["positioning_card_results"]["cached_user"]["fit_summary"], "cached")
        self.assertEqual(result["positioning_card_results"]["fresh_user"]["fit_summary"], "fresh")

    def test_force_refresh_creator_cache_bypasses_positioning_cache(self) -> None:
        class ImmediateExecutor:
            def __init__(self, *args, **kwargs) -> None:
                self.submissions = []

            def submit(self, fn, *args, **kwargs):
                future = Future()
                future.set_result(fn(*args, **kwargs))
                self.submissions.append((fn, args, kwargs))
                return future

            def shutdown(self, wait=False, cancel_futures=False) -> None:
                return None

        cached_result = {
            "success": True,
            "username": "alpha",
            "fit_recommendation": "High Fit",
            "fit_summary": "cached",
            "reviewed_at": "2026-04-06T00:00:00+00:00",
        }
        fresh_result = {
            "fit_recommendation": "Low Fit",
            "positioning_labels": ["泛娱乐"],
            "fit_summary": "fresh rerun",
            "evidence_signals": ["直播切片"],
            "provider": "openai",
            "effective_model": "gpt-5.4",
            "reviewed_at": "2026-04-06T02:00:00+00:00",
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "creator_cache.db"
            creator_cache_module.persist_positioning_cache_entry(
                "instagram",
                "alpha",
                cached_result,
                db_path,
                updated_at="2026-04-06T00:00:00+00:00",
                context_key="ctx-positioning",
                context_payload={"version": "test"},
            )
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
                "resolve_positioning_card_analysis_targets",
                return_value=[{"username": "alpha", "profile_url": "https://www.instagram.com/alpha", "_visual_result": {"decision": "Pass"}}],
            ), mock.patch.object(
                backend_app,
                "load_positioning_card_results",
                return_value={"alpha": {"fit_summary": "stale disk result"}},
            ), mock.patch.object(
                backend_app,
                "merge_upload_metadata_into_review_item",
                side_effect=lambda platform, review_item: review_item,
            ), mock.patch.object(
                backend_app,
                "build_positioning_card_cache_context",
                return_value={
                    "context_key": "ctx-positioning",
                    "context_payload": {"version": "test"},
                },
            ), mock.patch.object(
                backend_app,
                "evaluate_profile_positioning_card_analysis",
                return_value=fresh_result,
            ) as mocked_evaluate, mock.patch.object(
                backend_app,
                "save_positioning_card_results",
            ), mock.patch.object(
                backend_app,
                "DaemonThreadPoolExecutor",
                ImmediateExecutor,
            ):
                result = backend_app.perform_positioning_card_analysis(
                    "instagram",
                    {
                        "provider": "openai",
                        "creator_cache_db_path": str(db_path),
                        "use_creator_cache": True,
                        "force_refresh_creator_cache": True,
                    },
                )

        self.assertTrue(result["success"])
        self.assertEqual(mocked_evaluate.call_count, 1)
        self.assertEqual(result["creator_cache"]["positioning_hit_count"], 0)
        self.assertEqual(result["positioning_card_results"]["alpha"]["fit_summary"], "fresh rerun")

    def test_positioning_cache_is_source_of_truth_when_json_artifact_is_empty(self) -> None:
        cached_result = {
            "success": True,
            "username": "alpha",
            "fit_recommendation": "High Fit",
            "fit_summary": "cached only in sqlite",
            "reviewed_at": "2026-04-06T00:00:00+00:00",
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "creator_cache.db"
            creator_cache_module.persist_positioning_cache_entry(
                "instagram",
                "alpha",
                cached_result,
                db_path,
                updated_at="2026-04-06T00:00:00+00:00",
                context_key="ctx-positioning",
                context_payload={"version": "test"},
            )
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
                "resolve_positioning_card_analysis_targets",
                return_value=[{"username": "alpha", "profile_url": "https://www.instagram.com/alpha", "_visual_result": {"decision": "Pass"}}],
            ), mock.patch.object(
                backend_app,
                "load_positioning_card_results",
                return_value={},
            ), mock.patch.object(
                backend_app,
                "build_positioning_card_cache_context",
                return_value={
                    "context_key": "ctx-positioning",
                    "context_payload": {"version": "test"},
                },
            ), mock.patch.object(
                backend_app,
                "evaluate_profile_positioning_card_analysis",
                side_effect=AssertionError("cached positioning result should skip provider call"),
            ), mock.patch.object(
                backend_app,
                "save_positioning_card_results",
            ):
                result = backend_app.perform_positioning_card_analysis(
                    "instagram",
                    {
                        "provider": "openai",
                        "creator_cache_db_path": str(db_path),
                        "use_creator_cache": True,
                    },
                )

        self.assertTrue(result["success"])
        self.assertEqual(result["creator_cache"]["positioning_hit_count"], 1)
        self.assertEqual(result["positioning_card_results"]["alpha"]["fit_summary"], "cached only in sqlite")


if __name__ == "__main__":
    unittest.main()
