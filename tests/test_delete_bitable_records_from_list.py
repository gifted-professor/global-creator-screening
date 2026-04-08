from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import scripts.delete_bitable_records_from_list as delete_script


class _FakeDeleteClient:
    def __init__(self, *, failing_record_ids: set[str] | None = None) -> None:
        self.deleted_record_ids: list[str] = []
        self.failing_record_ids = set(failing_record_ids or set())

    def delete_api_json(self, url_path: str, *, body=None, headers=None):  # type: ignore[override]
        record_id = url_path.rsplit("/", 1)[-1]
        if record_id in self.failing_record_ids:
            raise RuntimeError(f"delete failed for {record_id}")
        self.deleted_record_ids.append(record_id)
        return {"data": {}}


class DeleteBitableRecordsFromListTests(unittest.TestCase):
    def _resolved_view(self) -> SimpleNamespace:
        return SimpleNamespace(
            source_url="https://example.com/base/app_token?table=tbl123&view=vew123",
            app_token="app_token",
            table_id="tbl123",
            table_name="AI回信管理",
            view_id="vew123",
            view_name="总视图",
        )

    def test_parser_accepts_linked_bitable_url_alias(self) -> None:
        parser = delete_script._build_parser()
        args = parser.parse_args(
            [
                "--linked-bitable-url",
                "https://example.com/base/app_token?table=tbl123&view=vew123",
                "--record-id-list",
                "/tmp/ids.txt",
            ]
        )
        self.assertEqual(args.url, "https://example.com/base/app_token?table=tbl123&view=vew123")
        self.assertEqual(args.record_id_list, "/tmp/ids.txt")

    def test_dry_run_does_not_delete(self) -> None:
        client = _FakeDeleteClient()
        with tempfile.TemporaryDirectory() as tmpdir:
            record_list = Path(tmpdir) / "ids.txt"
            record_list.write_text("rec_1\nrec_2\n", encoding="utf-8")
            with patch(
                "scripts.delete_bitable_records_from_list.resolve_bitable_view_from_url",
                return_value=self._resolved_view(),
            ), patch(
                "scripts.delete_bitable_records_from_list._canonicalize_target_url",
                return_value=self._resolved_view().source_url,
            ):
                result = delete_script.delete_bitable_records_from_list(
                    client=client,
                    linked_bitable_url=self._resolved_view().source_url,
                    record_id_list_path=record_list,
                    output_root=Path(tmpdir) / "out",
                    execute=False,
                )
        self.assertTrue(result["ok"])
        self.assertTrue(result["dry_run"])
        self.assertEqual(result["planned_delete_count"], 2)
        self.assertEqual(result["deleted_count"], 0)
        self.assertEqual(client.deleted_record_ids, [])

    def test_execute_deletes_only_input_record_ids(self) -> None:
        client = _FakeDeleteClient()
        with tempfile.TemporaryDirectory() as tmpdir:
            record_list = Path(tmpdir) / "ids.json"
            record_list.write_text(json.dumps(["rec_keep", "rec_delete_1", "rec_delete_2"]), encoding="utf-8")
            with patch(
                "scripts.delete_bitable_records_from_list.resolve_bitable_view_from_url",
                return_value=self._resolved_view(),
            ), patch(
                "scripts.delete_bitable_records_from_list._canonicalize_target_url",
                return_value=self._resolved_view().source_url,
            ):
                result = delete_script.delete_bitable_records_from_list(
                    client=client,
                    linked_bitable_url=self._resolved_view().source_url,
                    record_id_list_path=record_list,
                    output_root=Path(tmpdir) / "out",
                    execute=True,
                )
        self.assertTrue(result["ok"])
        self.assertEqual(result["deleted_count"], 3)
        self.assertEqual(client.deleted_record_ids, ["rec_keep", "rec_delete_1", "rec_delete_2"])

    def test_empty_list_returns_noop(self) -> None:
        client = _FakeDeleteClient()
        with tempfile.TemporaryDirectory() as tmpdir:
            record_list = Path(tmpdir) / "ids.txt"
            record_list.write_text("", encoding="utf-8")
            with patch(
                "scripts.delete_bitable_records_from_list.resolve_bitable_view_from_url",
                return_value=self._resolved_view(),
            ), patch(
                "scripts.delete_bitable_records_from_list._canonicalize_target_url",
                return_value=self._resolved_view().source_url,
            ):
                result = delete_script.delete_bitable_records_from_list(
                    client=client,
                    linked_bitable_url=self._resolved_view().source_url,
                    record_id_list_path=record_list,
                    output_root=Path(tmpdir) / "out",
                    execute=True,
                )
        self.assertTrue(result["ok"])
        self.assertTrue(result["no_op"])
        self.assertEqual(result["planned_delete_count"], 0)
        self.assertEqual(client.deleted_record_ids, [])

    def test_result_summary_collects_failures(self) -> None:
        client = _FakeDeleteClient(failing_record_ids={"rec_bad"})
        with tempfile.TemporaryDirectory() as tmpdir:
            record_list = Path(tmpdir) / "ids.txt"
            record_list.write_text("rec_good\nrec_bad\n", encoding="utf-8")
            with patch(
                "scripts.delete_bitable_records_from_list.resolve_bitable_view_from_url",
                return_value=self._resolved_view(),
            ), patch(
                "scripts.delete_bitable_records_from_list._canonicalize_target_url",
                return_value=self._resolved_view().source_url,
            ):
                result = delete_script.delete_bitable_records_from_list(
                    client=client,
                    linked_bitable_url=self._resolved_view().source_url,
                    record_id_list_path=record_list,
                    output_root=Path(tmpdir) / "out",
                    execute=True,
                )
        self.assertTrue(result["ok"])
        self.assertEqual(result["deleted_count"], 1)
        self.assertEqual(result["failed_count"], 1)
        self.assertEqual(result["failed_record_ids"], ["rec_bad"])
        self.assertEqual(client.deleted_record_ids, ["rec_good"])

    def test_expected_table_validation_blocks_delete(self) -> None:
        client = _FakeDeleteClient()
        with tempfile.TemporaryDirectory() as tmpdir:
            record_list = Path(tmpdir) / "ids.txt"
            record_list.write_text("rec_1\n", encoding="utf-8")
            with patch(
                "scripts.delete_bitable_records_from_list.resolve_bitable_view_from_url",
                return_value=self._resolved_view(),
            ), patch(
                "scripts.delete_bitable_records_from_list._canonicalize_target_url",
                return_value=self._resolved_view().source_url,
            ):
                result = delete_script.delete_bitable_records_from_list(
                    client=client,
                    linked_bitable_url=self._resolved_view().source_url,
                    record_id_list_path=record_list,
                    output_root=Path(tmpdir) / "out",
                    execute=True,
                    expected_table_id="tbl_other",
                )
        self.assertFalse(result["ok"])
        self.assertTrue(result["guard_blocked"])
        self.assertEqual(result["deleted_count"], 0)
        self.assertEqual(client.deleted_record_ids, [])


if __name__ == "__main__":
    unittest.main()
