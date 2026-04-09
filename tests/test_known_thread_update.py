from __future__ import annotations

import unittest

from email_sync.known_thread_update import process_known_thread_updates


class KnownThreadUpdateTests(unittest.TestCase):
    def test_process_known_thread_updates_routes_screened_unscreened_and_new_rows(self) -> None:
        result = process_known_thread_updates(
            [
                {
                    "keep_row": {"evidence_thread_key": "mid:<alpha-root>"},
                    "owner_scope": "ou_owner",
                    "creator_id": "alpha",
                    "platform": "instagram",
                    "thread_assignment_resolution": {"status": "cache_hit"},
                },
                {
                    "keep_row": {"evidence_thread_key": "mid:<beta-root>"},
                    "owner_scope": "ou_owner",
                    "creator_id": "beta",
                    "platform": "tiktok",
                    "thread_assignment_resolution": {"status": "cache_miss"},
                },
                {
                    "keep_row": {},
                    "owner_scope": "ou_owner",
                    "creator_id": "gamma",
                    "platform": "instagram",
                    "thread_assignment_resolution": {"status": "cache_miss"},
                },
            ],
            existing_index={
                "ou_owner::alpha::instagram": {
                    "record_id": "rec_alpha",
                    "fields": {"ai 是否通过": "是"},
                },
                "ou_owner::beta::tiktok": {
                    "record_id": "rec_beta",
                    "fields": {"ai 是否通过": ""},
                },
            },
            owner_scope_enabled=True,
        )

        stats = result["stats"]
        self.assertEqual(stats["candidate_count"], 3)
        self.assertEqual(stats["known_thread_hit_count"], 2)
        self.assertEqual(stats["thread_assignment_cache_hit_count"], 1)
        self.assertEqual(stats["mail_only_count"], 1)
        self.assertEqual(stats["full_screening_count"], 2)
        self.assertEqual(stats["existing_screened_count"], 1)
        self.assertEqual(stats["existing_unscreened_count"], 1)
        self.assertEqual(stats["new_creator_count"], 1)
        self.assertEqual(len(result["mail_only_candidates"]), 1)
        self.assertEqual(len(result["full_screening_candidates"]), 2)
        self.assertEqual(result["mail_only_candidates"][0]["record_key"], "ou_owner::alpha::instagram")

    def test_process_known_thread_updates_matches_without_owner_scope_when_disabled(self) -> None:
        result = process_known_thread_updates(
            [
                {
                    "keep_row": {"evidence_thread_key": "mid:<alpha-root>"},
                    "owner_scope": "",
                    "creator_id": "Alpha",
                    "platform": "Instagram",
                }
            ],
            existing_index={
                "alpha::instagram": {
                    "record_id": "rec_alpha",
                    "fields": {"ai是否通过": "是"},
                }
            },
            owner_scope_enabled=False,
        )

        self.assertEqual(result["stats"]["mail_only_count"], 1)
        self.assertEqual(result["stats"]["known_thread_hit_count"], 1)
        self.assertEqual(result["mail_only_candidates"][0]["record_key"], "alpha::instagram")


if __name__ == "__main__":
    unittest.main()
