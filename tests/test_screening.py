from __future__ import annotations

from datetime import datetime, timezone
import unittest

from backend.screening import check_instagram_profile, has_instagram_allowed_region


def _active_post() -> dict[str, str]:
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "displayUrl": "https://example.com/cover.jpg",
    }


class InstagramRegionDetectionTests(unittest.TestCase):
    def test_region_detection_accepts_nyc_bio(self) -> None:
        profile = {
            "biography": "NYC creator\ncollabs@example.com",
            "latestPosts": [_active_post()],
        }

        review = check_instagram_profile(profile, upload_metadata={}, runtime_rules={"allowed_regions": ["US"]})

        self.assertEqual(review["status"], "Pass")

    def test_region_detection_accepts_state_name_in_bio(self) -> None:
        profile = {
            "biography": "Photographer and hiker. Sedona, Arizona.",
            "latestPosts": [_active_post()],
        }

        self.assertTrue(has_instagram_allowed_region(profile, {}, ["US"]))

    def test_region_detection_accepts_city_state_abbreviation_pattern(self) -> None:
        profile = {
            "biography": "Food | Beauty | Lifestyle\nLA CA\nbrand@example.com",
            "latestPosts": [_active_post()],
        }

        self.assertTrue(has_instagram_allowed_region(profile, {}, ["US"]))

    def test_region_detection_uses_external_urls_and_full_name_fields(self) -> None:
        profile = {
            "fullName": "Ashley Sedona Arizona Photographer",
            "externalUrls": [{"title": "based in chicago", "url": "https://example.com"}],
            "latestPosts": [_active_post()],
        }

        self.assertTrue(has_instagram_allowed_region(profile, {}, ["US"]))

    def test_region_detection_keeps_reject_for_missing_signal(self) -> None:
        profile = {
            "biography": "Travel creator based in Madrid",
            "latestPosts": [_active_post()],
        }

        review = check_instagram_profile(profile, upload_metadata={}, runtime_rules={"allowed_regions": ["US"]})

        self.assertEqual(review["status"], "Reject")
        self.assertEqual(review["reason"], "简介或资料字段未识别到允许地区线索")
