from __future__ import annotations

import json
import tempfile
from pathlib import Path
import unittest

from openpyxl import Workbook

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

    def test_compile_workbook_preserves_label_only_manual_review_items_and_manual_review_prompt(self) -> None:
        workbook = Workbook()
        sheet = workbook.active
        sheet.title = "需求主表"
        rows = [
            ("字段", "内容", "说明"),
            ("A. 基本信息", "", ""),
            ("项目名称", "MINISO", ""),
            ("品牌 / 产品", "MINISO 母婴场景", ""),
            ("适用平台", "Instagram", ""),
            ("B. 步骤1：基础资质审核", "", ""),
            ("地区要求", "美国", ""),
            ("C. 步骤2：数据审核", "", ""),
            ("中位数播放量阈值", 10000, ""),
            ("D. 步骤3：内容 / 视觉审核", "", ""),
            ("产品展示", "需要", ""),
            ("E. 步骤4：排除项审核", "", ""),
            ("不符合时处理", "排除", ""),
            ("F. 人工判断项 / 合规提醒", "", ""),
            ("当封面出现奶瓶时，需要人工复核，不要直接判断达人为哺乳期妈妈", "", ""),
            ("合规提醒", "只记录看得到的母婴用品线索，不要推断身份", ""),
            ("G. 最终判定逻辑", "", ""),
            ("人工判断项命中时如何处理", "转人工", ""),
            ("满足条件时输出", "通过", ""),
            ("不满足时输出", "不通过", ""),
        ]
        for row_index, row in enumerate(rows, start=1):
            for column_index, value in enumerate(row, start=1):
                sheet.cell(row=row_index, column=column_index).value = value

        with tempfile.TemporaryDirectory() as tmpdir:
            workbook_path = Path(tmpdir) / "miniso_manual_review.xlsx"
            workbook.save(workbook_path)

            report = compile_workbook(workbook_path, Path(tmpdir))
            artifacts = report["artifacts"]
            structured = json.loads(Path(artifacts["structured_requirement_json"]).read_text(encoding="utf-8"))
            prompts = json.loads(Path(artifacts["visual_prompts_json"]).read_text(encoding="utf-8"))

        self.assertTrue(report["success"], report)
        self.assertEqual(len(structured["manual_review"]["extra_items"]), 1)
        self.assertEqual(
            structured["manual_review"]["extra_items"][0]["source_cell"],
            "B15",
        )
        self.assertEqual(
            structured["manual_review"]["extra_items"][0]["label"],
            "当封面出现奶瓶时，需要人工复核，不要直接判断达人为哺乳期妈妈",
        )
        self.assertIsNone(structured["manual_review"]["extra_items"][0]["value"])
        self.assertIsNone(structured["manual_review"]["extra_items"][0]["note"])
        self.assertEqual(len(structured["manual_review"]["compliance_notes"]), 1)
        self.assertEqual(structured["manual_review"]["compliance_notes"][0]["source_cell"], "B16")
        self.assertEqual(structured["manual_review"]["compliance_notes"][0]["label"], "合规提醒")
        self.assertEqual(
            structured["manual_review"]["compliance_notes"][0]["value"],
            "只记录看得到的母婴用品线索，不要推断身份",
        )
        self.assertEqual(
            structured["manual_review"]["compliance_notes"][0]["policy"],
            "never_compile_to_automation",
        )
        prompt = prompts["instagram"]["prompt"]
        self.assertIn("人工复核提醒：以下事项只用于补充需要人工关注的可见线索", prompt)
        self.assertIn("只记录画面里直接看到的物体、人物、场景或动作", prompt)
        self.assertIn("不要把身份、关系、阶段等推断当成事实", prompt)
        self.assertIn("如果人工判断项本身带有推断性结论，也不要直接沿用该结论", prompt)
        self.assertIn("最终 `decision` 仍只能输出 `Pass` 或 `Reject`", prompt)
        self.assertIn("当封面出现奶瓶时，需要人工复核，不要直接判断达人为哺乳期妈妈", prompt)
        self.assertIn("合规提醒：", prompt)
        self.assertIn("只记录看得到的母婴用品线索，不要推断身份", prompt)


if __name__ == "__main__":
    unittest.main()
