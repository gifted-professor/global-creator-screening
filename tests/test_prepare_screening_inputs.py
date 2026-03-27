from __future__ import annotations

import json
import tempfile
from pathlib import Path
import unittest
from unittest.mock import patch

from openpyxl import Workbook


REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_TEMPLATE = REPO_ROOT / "tests" / "fixtures" / "template_parser" / "11.xlsx"

IMPORT_ERROR: Exception | None = None
prepare_screening_inputs = None

try:
    from scripts.prepare_screening_inputs import prepare_screening_inputs
except Exception as exc:  # pragma: no cover - dependency availability differs by runtime
    IMPORT_ERROR = exc


def build_creator_workbook(path: Path) -> None:
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Creators"
    sheet.append(["Platform", "@username", "nickname", "Region", "URL"])
    sheet.append(["Instagram", "@creatoralpha", "Alpha", "US", "https://www.instagram.com/creatoralpha/"])
    sheet.append(["TikTok", "@creatorbeta", "Beta", "US", "https://www.tiktok.com/@creatorbeta"])
    workbook.save(path)


def build_sending_list_workbook(path: Path) -> None:
    workbook = Workbook()
    first_sheet = workbook.active
    first_sheet.title = "TikTokPool"
    first_sheet.append(["Country", "Creator", "邮箱地址", "IGlink"])
    first_sheet.append(["US", "Creator TikTok", "tiktok@example.com", "https://www.tiktok.com/@creatorbeta"])
    first_sheet.append(["US", "Creator Insta", "insta@example.com", "https://www.instagram.com/creatoralpha/"])

    second_sheet = workbook.create_sheet("Mixed")
    second_sheet.append(["国家", "Creator", "邮箱地址", "IGlink", "TTlink", "YTlink"])
    second_sheet.append([
        "US",
        "Creator Mixed",
        "mixed@example.com",
        "https://www.instagram.com/creatormixed/",
        "@creatormixed",
        "https://www.youtube.com/@creatormixed",
    ])
    workbook.save(path)


@unittest.skipIf(prepare_screening_inputs is None, f"screening deps unavailable: {IMPORT_ERROR}")
class PrepareScreeningInputsTests(unittest.TestCase):
    def test_prepare_screening_inputs_persists_rulespec_and_upload_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            creator_workbook = tmp_path / "creator_upload.xlsx"
            screening_data_dir = tmp_path / "screening_data"
            config_dir = tmp_path / "config"
            temp_dir = tmp_path / "temp"
            template_output_dir = tmp_path / "parsed_outputs"
            summary_json = tmp_path / "summary.json"

            build_creator_workbook(creator_workbook)

            summary = prepare_screening_inputs(
                creator_workbook=creator_workbook,
                template_workbook=FIXTURE_TEMPLATE,
                template_output_dir=template_output_dir,
                screening_data_dir=screening_data_dir,
                config_dir=config_dir,
                temp_dir=temp_dir,
                summary_json=summary_json,
            )

            self.assertEqual(summary["rulespec"]["source"], "template_workbook", summary)
            self.assertEqual(summary["upload"]["metadata_count_by_platform"]["instagram"], 1, summary)
            self.assertEqual(summary["upload"]["metadata_count_by_platform"]["tiktok"], 1, summary)
            self.assertEqual(summary["upload"]["metadata_count_by_platform"]["youtube"], 0, summary)

            active_rulespec_path = Path(summary["active_rulespec_path"])
            self.assertTrue(active_rulespec_path.exists(), active_rulespec_path)

            instagram_metadata_path = Path(summary["upload"]["upload_metadata_paths"]["instagram"])
            tiktok_metadata_path = Path(summary["upload"]["upload_metadata_paths"]["tiktok"])
            self.assertTrue(instagram_metadata_path.exists(), instagram_metadata_path)
            self.assertTrue(tiktok_metadata_path.exists(), tiktok_metadata_path)

            instagram_metadata = json.loads(instagram_metadata_path.read_text(encoding="utf-8"))
            tiktok_metadata = json.loads(tiktok_metadata_path.read_text(encoding="utf-8"))
            self.assertIn("creatoralpha", instagram_metadata, instagram_metadata)
            self.assertIn("creatorbeta", tiktok_metadata, tiktok_metadata)

            self.assertTrue(summary_json.exists(), summary_json)

    def test_prepare_screening_inputs_normalizes_sending_list_workbook(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            sending_list_workbook = tmp_path / "sending_list.xlsx"
            screening_data_dir = tmp_path / "screening_data"
            config_dir = tmp_path / "config"
            temp_dir = tmp_path / "temp"
            template_output_dir = tmp_path / "parsed_outputs"

            build_sending_list_workbook(sending_list_workbook)

            summary = prepare_screening_inputs(
                creator_workbook=sending_list_workbook,
                template_workbook=FIXTURE_TEMPLATE,
                template_output_dir=template_output_dir,
                screening_data_dir=screening_data_dir,
                config_dir=config_dir,
                temp_dir=temp_dir,
            )

            self.assertEqual(summary["upload"]["parsed_source_kind"], "sending_list", summary)
            self.assertTrue(summary["upload"]["normalized_upload_source_path"], summary)
            self.assertTrue(Path(summary["upload"]["normalized_upload_source_path"]).exists(), summary)
            self.assertEqual(summary["upload"]["metadata_count_by_platform"]["instagram"], 2, summary)
            self.assertEqual(summary["upload"]["metadata_count_by_platform"]["tiktok"], 2, summary)
            self.assertEqual(summary["upload"]["metadata_count_by_platform"]["youtube"], 1, summary)

            instagram_metadata_path = Path(summary["upload"]["upload_metadata_paths"]["instagram"])
            tiktok_metadata_path = Path(summary["upload"]["upload_metadata_paths"]["tiktok"])
            youtube_metadata_path = Path(summary["upload"]["upload_metadata_paths"]["youtube"])
            instagram_metadata = json.loads(instagram_metadata_path.read_text(encoding="utf-8"))
            tiktok_metadata = json.loads(tiktok_metadata_path.read_text(encoding="utf-8"))
            youtube_metadata = json.loads(youtube_metadata_path.read_text(encoding="utf-8"))

            self.assertEqual(instagram_metadata["creatoralpha"]["email"], "insta@example.com", instagram_metadata)
            self.assertEqual(tiktok_metadata["creatormixed"]["email"], "mixed@example.com", tiktok_metadata)
            self.assertEqual(youtube_metadata["creatormixed"]["region"], "US", youtube_metadata)

    def test_prepare_screening_inputs_can_source_task_upload_assets(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            creator_workbook = tmp_path / "downloaded_sending_list.xlsx"
            screening_data_dir = tmp_path / "screening_data"
            config_dir = tmp_path / "config"
            temp_dir = tmp_path / "temp"
            summary_json = tmp_path / "summary.json"

            build_sending_list_workbook(creator_workbook)

            fake_task_source = {
                "recordId": "rec-task-miniso",
                "taskName": "MINISO",
                "taskUploadUrl": "https://example.com/wiki/task-upload",
                "downloadDir": str(tmp_path / "downloads"),
                "templateFileToken": "file-template-001",
                "templateFileName": "miniso-template.xlsx",
                "templateDownloadedPath": str(FIXTURE_TEMPLATE),
                "sendingListFileToken": "file-sending-001",
                "sendingListFileName": "miniso-sending-list.xlsx",
                "sendingListDownloadedPath": str(creator_workbook),
            }

            with patch(
                "scripts.prepare_screening_inputs.resolve_task_upload_source_files",
                return_value=fake_task_source,
            ) as mocked_resolver:
                summary = prepare_screening_inputs(
                    task_name="MINISO",
                    task_upload_url="https://example.com/wiki/task-upload",
                    screening_data_dir=screening_data_dir,
                    config_dir=config_dir,
                    temp_dir=temp_dir,
                    summary_json=summary_json,
                )

            mocked_resolver.assert_called_once()
            self.assertEqual(summary["taskSource"]["taskName"], "MINISO", summary)
            self.assertEqual(summary["rulespec"]["source"], "task_upload_template", summary)
            self.assertEqual(summary["upload"]["parsed_source_kind"], "sending_list", summary)
            self.assertEqual(summary["upload"]["metadata_count_by_platform"]["instagram"], 2, summary)
            self.assertEqual(summary["upload"]["metadata_count_by_platform"]["tiktok"], 2, summary)
            self.assertEqual(summary["upload"]["metadata_count_by_platform"]["youtube"], 1, summary)
            self.assertTrue(summary_json.exists(), summary_json)


if __name__ == "__main__":
    unittest.main()
