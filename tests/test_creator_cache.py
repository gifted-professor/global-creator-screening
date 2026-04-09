from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from backend import creator_cache


class CreatorCacheTests(unittest.TestCase):
    def test_load_positioning_cache_entries_returns_empty_when_cache_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "creator_cache.db"

            loaded = creator_cache.load_positioning_cache_entries(
                "tiktok",
                ["alice"],
                db_path,
                "ctx-1",
            )

        self.assertEqual(loaded, {})

    def test_persist_and_load_positioning_cache_entry(self) -> None:
        result = {
            "success": True,
            "fit_recommendation": "High Fit",
            "positioning_labels": ["收纳", "家居"],
            "fit_summary": "适合家居收纳品牌",
            "evidence_signals": ["家庭场景"],
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "creator_cache.db"

            ok = creator_cache.persist_positioning_cache_entry(
                "tiktok",
                "alice",
                result,
                db_path,
                updated_at="2026-04-06T00:00:00+00:00",
                context_key="ctx-1",
                context_payload={"version": 1},
            )
            loaded = creator_cache.load_positioning_cache_entries(
                "tiktok",
                ["alice"],
                db_path,
                "ctx-1",
            )

        self.assertTrue(ok)
        self.assertEqual(loaded["alice"]["fit_recommendation"], "High Fit")
        self.assertEqual(loaded["alice"]["positioning_labels"], ["收纳", "家居"])

    def test_positioning_cache_entries_are_scoped_by_context_key(self) -> None:
        old_result = {
            "success": True,
            "fit_recommendation": "High Fit",
            "fit_summary": "old context",
        }
        new_result = {
            "success": True,
            "fit_recommendation": "Low Fit",
            "fit_summary": "new context",
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "creator_cache.db"
            creator_cache.persist_positioning_cache_entry(
                "instagram",
                "alpha",
                old_result,
                db_path,
                updated_at="2026-04-06T00:00:00+00:00",
                context_key="ctx-old",
                context_payload={"version": "old"},
            )
            creator_cache.persist_positioning_cache_entry(
                "instagram",
                "alpha",
                new_result,
                db_path,
                updated_at="2026-04-06T01:00:00+00:00",
                context_key="ctx-new",
                context_payload={"version": "new"},
            )

            old_entries = creator_cache.load_positioning_cache_entries(
                "instagram",
                ["alpha"],
                db_path,
                "ctx-old",
            )
            new_entries = creator_cache.load_positioning_cache_entries(
                "instagram",
                ["alpha"],
                db_path,
                "ctx-new",
            )

        self.assertEqual(old_entries["alpha"]["fit_summary"], "old context")
        self.assertEqual(new_entries["alpha"]["fit_summary"], "new context")

    def test_non_cacheable_positioning_error_payload_does_not_persist(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "creator_cache.db"

            ok = creator_cache.persist_positioning_cache_entry(
                "youtube",
                "alpha",
                {
                    "success": False,
                    "error": "timeout",
                },
                db_path,
                updated_at="2026-04-06T00:00:00+00:00",
                context_key="ctx-1",
                context_payload={"version": 1},
            )
            loaded = creator_cache.load_positioning_cache_entries(
                "youtube",
                ["alpha"],
                db_path,
                "ctx-1",
            )

        self.assertFalse(ok)
        self.assertEqual(loaded, {})


if __name__ == "__main__":
    unittest.main()
