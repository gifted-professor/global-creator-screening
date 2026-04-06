from __future__ import annotations

import unittest

import backend.app as backend_app


class PositioningCacheContextTests(unittest.TestCase):
    def test_build_positioning_card_cache_context_is_stable_for_same_inputs(self) -> None:
        providers = [
            {
                "name": "openai",
                "api_style": backend_app.VISION_API_STYLE_RESPONSES,
                "model": "gpt-5.4",
                "base_url": "https://api.openai.com/v1",
            }
        ]
        active_rulespec = {
            "goal": "优先保留家庭生活感强的账号",
            "rules": [
                {
                    "type": "visual_feature_group",
                    "platform": "instagram",
                    "cover_count": 5,
                    "min_hit_features": 2,
                    "features": [{"label": "家庭场景"}],
                }
            ],
        }

        ctx1 = backend_app.build_positioning_card_cache_context(
            "instagram",
            requested_provider="openai",
            providers=providers,
            active_rulespec=active_rulespec,
        )
        ctx2 = backend_app.build_positioning_card_cache_context(
            "instagram",
            requested_provider="openai",
            providers=providers,
            active_rulespec=active_rulespec,
        )

        self.assertEqual(ctx1["context_key"], ctx2["context_key"])
        self.assertEqual(
            ctx1["context_payload"]["prompt"]["resolved_cover_limit"],
            5,
        )

    def test_build_positioning_card_cache_context_changes_when_prompt_changes(self) -> None:
        providers = [
            {
                "name": "openai",
                "api_style": backend_app.VISION_API_STYLE_RESPONSES,
                "model": "gpt-5.4",
                "base_url": "https://api.openai.com/v1",
            }
        ]
        base_rulespec = {
            "goal": "优先保留家庭生活感强的账号",
            "rules": [
                {
                    "type": "visual_feature_group",
                    "platform": "instagram",
                    "cover_count": 5,
                    "min_hit_features": 2,
                    "features": [{"label": "家庭场景"}],
                }
            ],
        }
        changed_rulespec = {
            "goal": "优先保留强测评导购属性账号",
            "rules": [
                {
                    "type": "visual_feature_group",
                    "platform": "instagram",
                    "cover_count": 7,
                    "min_hit_features": 1,
                    "features": [{"label": "商品对比"}],
                }
            ],
        }

        ctx1 = backend_app.build_positioning_card_cache_context(
            "instagram",
            requested_provider="openai",
            providers=providers,
            active_rulespec=base_rulespec,
        )
        ctx2 = backend_app.build_positioning_card_cache_context(
            "instagram",
            requested_provider="openai",
            providers=providers,
            active_rulespec=changed_rulespec,
        )

        self.assertNotEqual(ctx1["context_key"], ctx2["context_key"])
        self.assertNotEqual(
            ctx1["context_payload"]["prompt"]["prompt_key"],
            ctx2["context_payload"]["prompt"]["prompt_key"],
        )


if __name__ == "__main__":
    unittest.main()
