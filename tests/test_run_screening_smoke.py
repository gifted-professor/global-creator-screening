from __future__ import annotations

from io import BytesIO
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import scripts.run_screening_smoke as run_screening_smoke


class _FakeResponse:
    def __init__(self, *, status_code: int = 200, data: bytes = b"", payload=None):
        self.status_code = status_code
        self.data = data
        self._payload = payload

    def get_json(self, silent: bool = False):
        return self._payload


class _FakeClient:
    def get(self, url: str):
        if url.endswith("/prescreen-review"):
            return _FakeResponse(data=b"prescreen")
        if url.endswith("/image-review"):
            return _FakeResponse(data=b"image")
        if url.endswith("/test-info"):
            return _FakeResponse(data=b"test-info")
        if url.endswith("/test-info-json"):
            return _FakeResponse(data=b'{"row_count": 1}', payload={"row_count": 1})
        if url.endswith("/status"):
            return _FakeResponse(payload={"success": True, "saved_positioning_card_artifacts_available": False})
        raise AssertionError(f"unexpected GET {url}")

    def post(self, url: str, json=None):
        if url.endswith("/final-review"):
            return _FakeResponse(
                status_code=409,
                payload={
                    "error": "存在名单账号未在本次抓取结果中返回，已阻止导出 final review。请先补抓或重新筛号。",
                    "error_code": "FINAL_REVIEW_BLOCKED_BY_MISSING_PROFILES",
                    "platform": "youtube",
                    "missing_profile_count": 1,
                },
            )
        raise AssertionError(f"unexpected POST {url}")


class RunScreeningSmokeTests(unittest.TestCase):
    def test_export_platform_artifacts_recovers_final_review_when_endpoint_returns_missing_profile_block(self) -> None:
        client = _FakeClient()
        with tempfile.TemporaryDirectory() as tmpdir, mock.patch.object(
            run_screening_smoke.backend_app,
            "load_profile_reviews",
            return_value=[{"username": "ghost", "status": "Missing", "reason": "not found"}],
        ), mock.patch.object(
            run_screening_smoke.backend_app,
            "load_visual_results",
            return_value={},
        ), mock.patch.object(
            run_screening_smoke.backend_app,
            "build_final_review_rows",
            return_value=[{"达人ID": "ghost", "status": "Missing"}],
        ), mock.patch.object(
            run_screening_smoke.backend_app,
            "workbook_bytes_from_sheets",
            return_value=BytesIO(b"final-review-fallback"),
        ):
            export_dir = Path(tmpdir) / "exports" / "youtube"
            outputs = run_screening_smoke.export_platform_artifacts(client, "youtube", export_dir)
            self.assertEqual(outputs["final_review_export_mode"], "local_missing_profile_fallback")
            self.assertEqual((export_dir / "youtube_final_review.xlsx").read_bytes(), b"final-review-fallback")
            self.assertEqual((export_dir / "youtube_prescreen_review.xlsx").read_bytes(), b"prescreen")
            self.assertEqual((export_dir / "youtube_image_review.xlsx").read_bytes(), b"image")
            self.assertEqual((export_dir / "youtube_test_info.xlsx").read_bytes(), b"test-info")
            self.assertEqual((export_dir / "youtube_test_info.json").read_bytes(), b'{"row_count": 1}')


if __name__ == "__main__":
    unittest.main()
