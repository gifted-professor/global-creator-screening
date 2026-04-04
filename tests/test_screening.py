from __future__ import annotations

from datetime import datetime, timezone
import unittest

from backend.screening import (
    check_instagram_profile,
    extract_platform_identifier,
    filter_scraped_items,
    has_instagram_allowed_region,
)


def _active_post() -> dict[str, str]:
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "displayUrl": "https://example.com/cover.jpg",
    }


class InstagramRegionDetectionTests(unittest.TestCase):
    def test_region_detection_is_disabled_when_allowed_regions_empty(self) -> None:
        profile = {
            "biography": "Travel creator based in Madrid",
            "latestPosts": [_active_post()],
        }

        review = check_instagram_profile(profile, upload_metadata={}, runtime_rules={"allowed_regions": []})

        self.assertEqual(review["status"], "Pass")

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


class TikTokScrapeErrorHandlingTests(unittest.TestCase):
    def test_extract_platform_identifier_uses_search_query_for_tiktok_search_urls(self) -> None:
        identifier = extract_platform_identifier(
            "tiktok",
            "https://www.tiktok.com/search?q=tinozach&t=1773382255532",
        )

        self.assertEqual(identifier, "tinozach")

    def test_tiktok_error_placeholder_is_exported_as_reject_not_missing(self) -> None:
        result = filter_scraped_items(
            "tiktok",
            [
                {
                    "url": "https://www.tiktok.com/@farrobear",
                    "input": "farrobear",
                    "error": "This profile/hashtag does not exist.",
                }
            ],
            expected_profiles=["farrobear"],
            upload_metadata_lookup={
                "farrobear": {
                    "handle": "farrobear",
                    "url": "https://www.tiktok.com/@farrobear",
                }
            },
        )

        self.assertEqual(result["successful_identifiers"], ["farrobear"])
        self.assertEqual(len(result["missing_profiles"]), 0)
        self.assertEqual(len(result["profile_reviews"]), 1)
        review = result["profile_reviews"][0]
        self.assertEqual(review["status"], "Reject")
        self.assertEqual(review["reason"], "抓取返回账号不存在或不可访问")
        self.assertEqual(review["username"], "farrobear")

    def test_tiktok_real_content_wins_over_error_placeholder_for_same_identifier(self) -> None:
        active_now = datetime.now(timezone.utc).isoformat()
        result = filter_scraped_items(
            "tiktok",
            [
                {
                    "url": "https://www.tiktok.com/@farrobear",
                    "input": "farrobear",
                    "error": "This profile/hashtag does not exist.",
                },
                {
                    "authorMeta": {
                        "name": "farrobear",
                        "profileUrl": "https://www.tiktok.com/@farrobear",
                    },
                    "createTimeISO": active_now,
                    "playCount": 25000,
                    "videoMeta": {"coverUrl": "https://example.com/cover.jpg"},
                },
            ],
            expected_profiles=["farrobear"],
        )

        self.assertEqual(result["successful_identifiers"], ["farrobear"])
        self.assertEqual(len(result["profile_reviews"]), 1)
        review = result["profile_reviews"][0]
        self.assertEqual(review["status"], "Pass")
        self.assertIn("播放量达标", review["reason"])
