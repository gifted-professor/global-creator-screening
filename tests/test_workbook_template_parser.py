from __future__ import annotations

import json
import tempfile
from pathlib import Path
import unittest

from openpyxl import load_workbook

from workbook_template_parser import build_visual_prompt_artifacts, compile_workbook


FIXTURE_WORKBOOK = Path(__file__).resolve().parent / "fixtures" / "template_parser" / "11.xlsx"

SAMPLE_VISUAL_REUSE_SPEC = {
    "version": "v1",
    "goal": "判断达人是否符合家庭 / 宠物 / 户外生活内容合作标准",
    "requested_platforms": ["tiktok", "instagram"],
    "visual_scope": {
        "cover_count": 18,
        "min_hit_features": 1,
        "positive_features": [
            {
                "key": "kid_interaction",
                "label": "孩子互动",
                "note": "家庭或真实生活场景中出现互动",
                "reuse_status": "runtime_supported",
            },
            {
                "key": "product_display",
                "label": "产品展示",
                "reuse_status": "runtime_supported",
            },
            {
                "key": "blind_box_unboxing",
                "label": "盲盒开箱",
                "reuse_status": "template_only",
            },
        ],
        "negative_features": [
            {
                "key": "green_screen",
                "label": "绿幕",
                "operator": "must_not_appear",
                "reuse_status": "runtime_supported",
            },
            {
                "key": "selfie_or_couple_ratio",
                "label": "自拍 / 情侣出镜占比",
                "operator": "ratio_gt",
                "threshold_percent": 70,
                "reuse_status": "runtime_supported",
            },
            {
                "key": "minor_presence",
                "label": "未成年/小孩出镜",
                "operator": "must_not_appear",
                "note": "不符合本次投放要求",
                "reuse_status": "template_only",
            },
        ],
    },
    "manual_review_items": [
        {"key": "persona_or_niche_manual", "label": "鲜明人设 / 垂直 niche"},
    ],
    "compliance_notes": [
        {
            "key": "protected_attribute_notice",
            "label": "受保护属性相关判断",
            "value": "不要根据年龄、种族等受保护属性做判断",
            "policy": "never_compile_to_automation",
        },
        {
            "key": "blocked_sensitive_attribute",
            "label": "出镜人数黑人占50%以上是否直接排除",
            "value": "是",
            "policy": "never_compile_to_automation",
        },
    ],
}


class WorkbookTemplateParserTests(unittest.TestCase):
    def test_compile_workbook_writes_expected_artifacts(self) -> None:
        self.assertTrue(FIXTURE_WORKBOOK.exists(), FIXTURE_WORKBOOK)
        with tempfile.TemporaryDirectory() as tmpdir:
            report = compile_workbook(FIXTURE_WORKBOOK, Path(tmpdir))

            self.assertTrue(report["success"], report)
            self.assertIn("warnings", report, report)
            artifacts = report["artifacts"]
            self.assertEqual(
                sorted(artifacts.keys()),
                [
                    "rulespec_json",
                    "structured_requirement_json",
                    "visual_prompts_json",
                    "visual_reuse_spec_json",
                ],
                artifacts,
            )

            expected_names = {
                "structured_requirement_json": "structured_requirement.json",
                "rulespec_json": "rulespec.json",
                "visual_reuse_spec_json": "visual_reuse_spec.json",
                "visual_prompts_json": "visual_prompts.json",
            }
            for key, name in expected_names.items():
                path = Path(artifacts[key])
                self.assertTrue(path.exists(), path)
                self.assertEqual(path.name, name)

            report_path = Path(report["output_dir"]) / "compile_report.json"
            self.assertTrue(report_path.exists(), report_path)

            prompts = json.loads(Path(artifacts["visual_prompts_json"]).read_text(encoding="utf-8"))
            self.assertTrue(prompts, prompts)
            self.assertGreaterEqual(len(prompts), 1, prompts)
            first_prompt = next(iter(prompts.values()))
            self.assertIn("prompt", first_prompt, first_prompt)
            self.assertTrue(first_prompt["prompt"].strip(), first_prompt)

    def test_build_visual_prompt_artifacts_renders_platform_prompts(self) -> None:
        bundles = build_visual_prompt_artifacts(SAMPLE_VISUAL_REUSE_SPEC)

        self.assertEqual(set(bundles), {"tiktok", "instagram"}, bundles)
        tiktok_prompt = bundles["tiktok"]["prompt"]
        self.assertIn("你是 TikTok 达人初筛流程中的视觉复核员", tiktok_prompt)
        self.assertIn("孩子互动：家庭或真实生活场景中出现互动", tiktok_prompt)
        self.assertIn("产品展示", tiktok_prompt)
        self.assertIn("盲盒开箱", tiktok_prompt)
        self.assertIn("出现绿幕背景", tiktok_prompt)
        self.assertIn("自拍 / 情侣出镜占比 > 70%", tiktok_prompt)
        self.assertIn("未成年/小孩出镜", tiktok_prompt)
        self.assertIn("鲜明人设 / 垂直 niche", tiktok_prompt)
        self.assertIn("不要根据年龄、种族、民族、肤色、宗教等受保护属性做判断", tiktok_prompt)
        self.assertNotIn("黑人", tiktok_prompt)

    def test_build_visual_prompt_artifacts_supports_single_platform_override(self) -> None:
        bundles = build_visual_prompt_artifacts(SAMPLE_VISUAL_REUSE_SPEC, explicit_platform="youtube")

        self.assertEqual(set(bundles), {"youtube"}, bundles)
        self.assertEqual(bundles["youtube"]["platform"], "youtube", bundles)
        self.assertIn("你是 YouTube 达人初筛流程中的视觉复核员", bundles["youtube"]["prompt"])

    def test_compile_workbook_accepts_standardized_main_sheet_on_sheet1(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            workbook_path = Path(tmpdir) / "sheet1-template.xlsx"
            workbook = load_workbook(FIXTURE_WORKBOOK)
            workbook["需求主表"].title = "Sheet1"
            workbook.save(workbook_path)

            report = compile_workbook(workbook_path, Path(tmpdir) / "compiled")

            self.assertTrue(report["success"], report)
            artifacts = report["artifacts"]
            structured_requirement = json.loads(
                Path(artifacts["structured_requirement_json"]).read_text(encoding="utf-8")
            )
            self.assertEqual(structured_requirement["basic_info"]["project_name"], "Tapo")

    def test_compile_workbook_preserves_label_only_manual_review_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            workbook_path = Path(tmpdir) / "manual-review-inline.xlsx"
            workbook = load_workbook(FIXTURE_WORKBOOK)
            worksheet = workbook[workbook.sheetnames[0]]
            worksheet["A65"] = "当封面出现奶瓶等情况，判断达人为哺乳期妈妈时需要人工复核"
            worksheet["B65"] = None
            worksheet["C65"] = None
            workbook.save(workbook_path)

            report = compile_workbook(workbook_path, Path(tmpdir) / "compiled")

            self.assertTrue(report["success"], report)
            artifacts = report["artifacts"]
            rulespec = json.loads(Path(artifacts["rulespec_json"]).read_text(encoding="utf-8"))
            prompts = json.loads(Path(artifacts["visual_prompts_json"]).read_text(encoding="utf-8"))

            self.assertIn(
                "当封面出现奶瓶等情况，判断达人为哺乳期妈妈时需要人工复核",
                json.dumps(rulespec["manual_review_items"], ensure_ascii=False),
            )
            first_prompt = next(iter(prompts.values()))["prompt"]
            self.assertIn("人工判断提醒", first_prompt)
            self.assertIn("奶瓶等情况", first_prompt)


if __name__ == "__main__":
    unittest.main()
