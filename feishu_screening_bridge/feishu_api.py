from __future__ import annotations

from dataclasses import dataclass
import json
import mimetypes
from pathlib import Path
import re
from typing import Any
from urllib import error, parse, request


DEFAULT_FEISHU_BASE_URL = "https://open.feishu.cn/open-apis"

_BOX_TOKEN_PATTERN = re.compile(r"(box[a-zA-Z0-9_-]+)")


class FeishuApiError(RuntimeError):
    pass


@dataclass(frozen=True)
class DownloadedFeishuFile:
    file_token: str
    file_name: str
    content_type: str
    content: bytes
    source_url: str


@dataclass(frozen=True)
class UploadedFeishuFile:
    file_token: str
    file_name: str
    size_bytes: int
    source_url: str


class FeishuOpenClient:
    def __init__(
        self,
        *,
        app_id: str,
        app_secret: str,
        base_url: str = DEFAULT_FEISHU_BASE_URL,
        timeout_seconds: float = 30.0,
    ) -> None:
        self.app_id = str(app_id or "").strip()
        self.app_secret = str(app_secret or "").strip()
        self.base_url = str(base_url or DEFAULT_FEISHU_BASE_URL).rstrip("/")
        self.timeout_seconds = float(timeout_seconds)
        self._tenant_access_token: str | None = None
        if not self.app_id:
            raise ValueError("缺少飞书 app_id。")
        if not self.app_secret:
            raise ValueError("缺少飞书 app_secret。")
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds 必须大于 0。")

    def get_tenant_access_token(self) -> str:
        if self._tenant_access_token:
            return self._tenant_access_token
        payload = self._request_json(
            "POST",
            "/auth/v3/tenant_access_token/internal",
            body={"app_id": self.app_id, "app_secret": self.app_secret},
        )
        token = str(payload.get("tenant_access_token") or "").strip()
        if not token:
            raise FeishuApiError("飞书鉴权成功，但响应里没有 tenant_access_token。")
        self._tenant_access_token = token
        return token

    def download_file(self, file_token_or_url: str, *, desired_name: str | None = None) -> DownloadedFeishuFile:
        file_token = extract_file_token(file_token_or_url)
        access_token = self.get_tenant_access_token()
        last_error: Exception | None = None

        for resource in ("files", "medias"):
            url_path = f"/drive/v1/{resource}/{parse.quote(file_token, safe='')}/download"
            try:
                status, headers, body, resolved_url = self._request_bytes(
                    "GET",
                    url_path,
                    headers={"Authorization": f"Bearer {access_token}"},
                )
            except FeishuApiError as exc:
                last_error = exc
                continue
            if status != 200:
                last_error = FeishuApiError(f"飞书下载失败: status={status} url={resolved_url}")
                continue
            if _looks_like_json(headers.get("Content-Type", ""), body):
                try:
                    payload = json.loads(body.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError):
                    payload = {}
                code = int(payload.get("code") or -1)
                if code != 0:
                    last_error = FeishuApiError(
                        f"飞书下载失败: code={code} msg={payload.get('msg') or payload.get('message') or ''}"
                    )
                    continue
            file_name = _normalize_download_name(
                desired_name
                or _extract_filename_from_headers(headers)
                or f"{file_token}{_guess_extension(headers.get('Content-Type', ''))}"
            )
            return DownloadedFeishuFile(
                file_token=file_token,
                file_name=file_name,
                content_type=str(headers.get("Content-Type") or "application/octet-stream"),
                content=body,
                source_url=resolved_url,
            )

        if last_error is not None:
            raise FeishuApiError(str(last_error)) from last_error
        raise FeishuApiError(f"无法下载飞书文件: {file_token}")

    def upload_local_file(
        self,
        local_path: str | Path,
        *,
        parent_type: str = "bitable_file",
        parent_node: str = "",
        file_name: str | None = None,
    ) -> UploadedFeishuFile:
        path = Path(str(local_path)).expanduser().resolve()
        if not path.exists() or not path.is_file():
            raise FileNotFoundError(f"待上传文件不存在: {path}")
        normalized_parent_node = str(parent_node or "").strip()
        if not normalized_parent_node:
            raise ValueError("上传飞书附件缺少 parent_node。")

        file_bytes = path.read_bytes()
        multipart_body, boundary = _build_multipart_form_data(
            fields={
                "file_name": str(file_name or path.name),
                "parent_type": str(parent_type or "bitable_file"),
                "parent_node": normalized_parent_node,
                "size": str(len(file_bytes)),
            },
            file_field_name="file",
            file_name=str(file_name or path.name),
            file_bytes=file_bytes,
            content_type=mimetypes.guess_type(str(path.name))[0] or "application/octet-stream",
        )
        access_token = self.get_tenant_access_token()
        status, _, response_body, resolved_url = self._open(
            "POST",
            "/drive/v1/medias/upload_all",
            data=multipart_body,
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": f"multipart/form-data; boundary={boundary}",
            },
        )
        if status != 200:
            raise FeishuApiError(f"飞书附件上传失败: status={status} url={resolved_url}")
        try:
            payload = json.loads(response_body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise FeishuApiError("飞书附件上传返回了非 JSON 响应。") from exc
        if int(payload.get("code") or 0) != 0:
            raise FeishuApiError(
                f"飞书附件上传失败: code={payload.get('code')} msg={payload.get('msg') or payload.get('message') or ''}"
            )
        data = payload.get("data") or {}
        file_token = str(data.get("file_token") or data.get("fileToken") or "").strip()
        if not file_token:
            raise FeishuApiError("飞书附件上传成功，但响应里没有 file_token。")
        return UploadedFeishuFile(
            file_token=file_token,
            file_name=str(data.get("name") or data.get("file_name") or file_name or path.name),
            size_bytes=len(file_bytes),
            source_url=resolved_url,
        )

    def get_api_json(self, url_path: str, *, headers: dict[str, str] | None = None) -> dict[str, Any]:
        access_token = self.get_tenant_access_token()
        return self._request_api_json(
            "GET",
            url_path,
            headers={"Authorization": f"Bearer {access_token}", **(headers or {})},
        )

    def post_api_json(
        self,
        url_path: str,
        *,
        body: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        access_token = self.get_tenant_access_token()
        return self._request_api_json(
            "POST",
            url_path,
            body=body,
            headers={"Authorization": f"Bearer {access_token}", **(headers or {})},
        )

    def resolve_wiki_node(self, wiki_token: str) -> dict[str, Any]:
        encoded = parse.quote(str(wiki_token or "").strip(), safe="")
        payload = self.get_api_json(f"/wiki/v2/spaces/get_node?token={encoded}")
        node = payload.get("data", {}).get("node")
        if not isinstance(node, dict):
            raise FeishuApiError(f"飞书 wiki 节点解析失败: {wiki_token}")
        return node

    def _request_json(
        self,
        method: str,
        url_path: str,
        *,
        body: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        payload = json.dumps(body or {}, ensure_ascii=False).encode("utf-8")
        response_body = self._open(
            method,
            url_path,
            data=payload,
            headers={"Content-Type": "application/json; charset=utf-8", **(headers or {})},
        )[2]
        try:
            parsed = json.loads(response_body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise FeishuApiError("飞书接口返回了非 JSON 响应。") from exc
        if int(parsed.get("code") or 0) != 0:
            raise FeishuApiError(
                f"飞书接口返回错误: code={parsed.get('code')} msg={parsed.get('msg') or parsed.get('message') or ''}"
            )
        return parsed

    def _request_api_json(
        self,
        method: str,
        url_path: str,
        *,
        body: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        payload_bytes = None
        request_headers = dict(headers or {})
        if body is not None:
            payload_bytes = json.dumps(body or {}, ensure_ascii=False).encode("utf-8")
            request_headers = {"Content-Type": "application/json; charset=utf-8", **request_headers}
        response_body = self._open(
            method,
            url_path,
            data=payload_bytes,
            headers=request_headers,
        )[2]
        try:
            parsed = json.loads(response_body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise FeishuApiError("飞书接口返回了非 JSON 响应。") from exc
        if int(parsed.get("code") or 0) != 0:
            raise FeishuApiError(
                f"飞书接口返回错误: code={parsed.get('code')} msg={parsed.get('msg') or parsed.get('message') or ''}"
            )
        return parsed

    def _request_bytes(
        self,
        method: str,
        url_path: str,
        *,
        headers: dict[str, str] | None = None,
    ) -> tuple[int, dict[str, str], bytes, str]:
        return self._open(method, url_path, headers=headers or {})

    def _open(
        self,
        method: str,
        url_path: str,
        *,
        data: bytes | None = None,
        headers: dict[str, str] | None = None,
    ) -> tuple[int, dict[str, str], bytes, str]:
        url = _join_url(self.base_url, url_path)
        req = request.Request(url, data=data, headers=headers or {}, method=method.upper())
        try:
            with request.urlopen(req, timeout=self.timeout_seconds) as response:
                response_headers = {key: value for key, value in response.headers.items()}
                return response.status, response_headers, response.read(), str(response.geturl())
        except error.HTTPError as exc:
            body = exc.read()
            detail = _describe_http_error(body)
            raise FeishuApiError(f"飞书请求失败: status={exc.code} url={url} {detail}".strip()) from exc
        except error.URLError as exc:
            raise FeishuApiError(f"飞书请求失败: url={url} reason={exc.reason}") from exc


def extract_file_token(file_token_or_url: str) -> str:
    raw = str(file_token_or_url or "").strip()
    if not raw:
        raise ValueError("file_token 或 file_url 不能为空。")
    if "://" not in raw:
        return raw

    parsed = parse.urlparse(raw)
    query_token = str(parse.parse_qs(parsed.query).get("file_token", [""])[0] or "").strip()
    if query_token:
        return query_token

    match = _BOX_TOKEN_PATTERN.search(raw)
    if match:
        return match.group(1)

    for segment in reversed([item for item in parsed.path.split("/") if item]):
        candidate = str(segment or "").strip()
        if candidate.startswith("box"):
            return candidate
    raise ValueError(f"无法从飞书链接中提取 file_token: {raw}")


def _describe_http_error(body: bytes) -> str:
    if not body:
        return ""
    try:
        payload = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        snippet = body.decode("utf-8", errors="replace").strip()
        return snippet[:200]
    msg = payload.get("msg") or payload.get("message") or ""
    code = payload.get("code")
    if code is None and not msg:
        return ""
    return f"code={code} msg={msg}".strip()


def _extract_filename_from_headers(headers: dict[str, str]) -> str:
    content_disposition = str(headers.get("Content-Disposition") or headers.get("content-disposition") or "").strip()
    if not content_disposition:
        return ""

    match = re.search(r"filename\*=UTF-8''([^;]+)", content_disposition, flags=re.IGNORECASE)
    if match:
        return parse.unquote(match.group(1))

    match = re.search(r'filename="([^"]+)"', content_disposition, flags=re.IGNORECASE)
    if match:
        return match.group(1).strip()

    match = re.search(r"filename=([^;]+)", content_disposition, flags=re.IGNORECASE)
    if match:
        return match.group(1).strip().strip('"')
    return ""


def _guess_extension(content_type: str) -> str:
    normalized = str(content_type or "").lower()
    if "spreadsheetml" in normalized or "excel" in normalized:
        return ".xlsx"
    return ""


def _join_url(base_url: str, url_path: str) -> str:
    if not url_path.startswith("/"):
        url_path = "/" + url_path
    return base_url.rstrip("/") + url_path


def _looks_like_json(content_type: str, body: bytes) -> bool:
    normalized_type = str(content_type or "").lower()
    if "application/json" in normalized_type or normalized_type.endswith("+json"):
        return True
    prefix = body[:32].lstrip()
    return prefix.startswith(b"{") or prefix.startswith(b"[")


def _normalize_download_name(file_name: str) -> str:
    candidate = Path(str(file_name or "").strip()).name
    candidate = candidate.replace("\x00", "")
    return candidate or "downloaded-workbook.xlsx"


def _build_multipart_form_data(
    *,
    fields: dict[str, str],
    file_field_name: str,
    file_name: str,
    file_bytes: bytes,
    content_type: str,
) -> tuple[bytes, str]:
    boundary = f"----CodexFeishuBoundary{Path(file_name).stem[:24]}{len(file_bytes)}"
    chunks: list[bytes] = []
    for key, value in fields.items():
        chunks.extend(
            [
                f"--{boundary}\r\n".encode("utf-8"),
                f'Content-Disposition: form-data; name="{key}"\r\n\r\n'.encode("utf-8"),
                str(value).encode("utf-8"),
                b"\r\n",
            ]
        )
    chunks.extend(
        [
            f"--{boundary}\r\n".encode("utf-8"),
            (
                f'Content-Disposition: form-data; name="{file_field_name}"; filename="{Path(file_name).name}"\r\n'
            ).encode("utf-8"),
            f"Content-Type: {content_type}\r\n\r\n".encode("utf-8"),
            file_bytes,
            b"\r\n",
            f"--{boundary}--\r\n".encode("utf-8"),
        ]
    )
    return b"".join(chunks), boundary
