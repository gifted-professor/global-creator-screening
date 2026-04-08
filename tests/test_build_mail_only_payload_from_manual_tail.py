from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from openpyxl import Workbook

from scripts.build_mail_only_payload_from_manual_tail import run


class BuildMailOnlyPayloadFromManualTailTests(unittest.TestCase):
    def test_manual_tail_rows_are_sorted_stably_before_assigning_synthetic_ids(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            owner_payload_path = root / "task_owner.json"
            owner_payload_path.write_text(
                json.dumps({"task_owner": {"task_name": "MINISO", "linked_bitable_url": "https://example.com/base"}}),
                encoding="utf-8",
            )

            headers = [
                "latest_external_full_body",
                "latest_external_sent_at",
                "latest_external_from",
                "subject",
                "resolution_stage_final",
                "resolution_confidence_final",
                "thread_key",
                "raw_path",
                "brand_keyword",
                "final_id_final",
            ]

            def build_payload_id_map(workbook_path: Path, rows: list[list[str]]) -> dict[str, str]:
                wb = Workbook()
                ws = wb.active
                ws.append(headers)
                for row in rows:
                    ws.append(row)
                wb.save(workbook_path)

                result = run(
                    SimpleNamespace(
                        manual_tail_workbook=str(workbook_path),
                        task_owner_payload_json=str(owner_payload_path),
                        task_name="MINISO",
                        local_date="2026-04-08",
                        output_prefix=str(root / workbook_path.stem),
                    )
                )
                payload = json.loads(Path(result["payload_path"]).read_text(encoding="utf-8"))
                return {item["full body"]: item["达人ID"] for item in payload["rows"]}

            later_row = [
                "body later",
                "2026-04-08T11:00:00+08:00",
                "later@example.com",
                "later",
                "llm",
                "medium",
                "thread-later",
                "later.eml",
                "MINISO",
                "later_candidate",
            ]
            earlier_row = [
                "body earlier",
                "2026-04-08T10:00:00+08:00",
                "earlier@example.com",
                "earlier",
                "llm",
                "medium",
                "thread-earlier",
                "earlier.eml",
                "MINISO",
                "earlier_candidate",
            ]

            ids_a = build_payload_id_map(root / "a.xlsx", [later_row, earlier_row])
            ids_b = build_payload_id_map(root / "b.xlsx", [earlier_row, later_row])

        self.assertEqual(ids_a, ids_b)
        self.assertEqual(ids_a["body later"], "MINISO4/8转人工1")
        self.assertEqual(ids_a["body earlier"], "MINISO4/8转人工2")


if __name__ == "__main__":
    unittest.main()
