from __future__ import annotations

import html
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from email import policy
from email.header import decode_header
from email.parser import BytesParser
from email.utils import getaddresses, parsedate_to_datetime
from typing import Dict, List, Optional


@dataclass
class AttachmentMetadata:
    part_index: int
    filename: Optional[str]
    content_type: str
    size_bytes: int
    content_id: Optional[str]
    content_disposition: Optional[str]
    is_inline: bool


@dataclass
class ParsedMessage:
    account_email: str
    folder_name: str
    uid: int
    uidvalidity: int
    message_id: Optional[str]
    subject: str
    in_reply_to: Optional[str]
    references_header: Optional[str]
    sent_at: Optional[str]
    sent_at_raw: Optional[str]
    internal_date: Optional[str]
    internal_date_raw: Optional[str]
    flags: List[str]
    size_bytes: int
    from_addresses: List[Dict[str, str]]
    to_addresses: List[Dict[str, str]]
    cc_addresses: List[Dict[str, str]]
    bcc_addresses: List[Dict[str, str]]
    reply_to_addresses: List[Dict[str, str]]
    sender_addresses: List[Dict[str, str]]
    body_text: str
    body_html: str
    snippet: str
    headers: Dict[str, List[str]]
    has_attachments: bool
    attachment_count: int
    attachments: List[AttachmentMetadata]


def _sanitize_text(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    return value.encode("utf-8", errors="replace").decode("utf-8")


def _decode_bytes_value(value: bytes, charset: Optional[str]) -> str:
    candidates = []
    if charset:
        candidates.append(charset)
    candidates.extend(["utf-8", "gb18030", "gbk", "big5", "latin-1"])

    seen = set()
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        try:
            return _sanitize_text(value.decode(candidate)) or ""
        except (LookupError, UnicodeDecodeError):
            continue

    return _sanitize_text(value.decode("utf-8", errors="replace")) or ""


def _decode_header_value(value: Optional[str]) -> str:
    if not value:
        return ""

    try:
        parts = decode_header(value)
    except Exception:  # noqa: BLE001
        return _sanitize_text(value) or ""

    decoded_parts: List[str] = []
    for chunk, charset in parts:
        if isinstance(chunk, bytes):
            decoded_parts.append(_decode_bytes_value(chunk, charset))
        else:
            decoded_parts.append(_sanitize_text(chunk) or "")

    return _sanitize_text("".join(decoded_parts)) or ""


def _header_values(headers: Dict[str, List[str]], header_name: str) -> List[str]:
    target = header_name.lower()
    values: List[str] = []
    for name, header_values in headers.items():
        if name.lower() == target:
            values.extend(header_values)
    return values


def _first_header(headers: Dict[str, List[str]], header_name: str) -> Optional[str]:
    values = _header_values(headers, header_name)
    if not values:
        return None
    return values[0]


def _parse_addresses(headers: Dict[str, List[str]], header_name: str) -> List[Dict[str, str]]:
    result: List[Dict[str, str]] = []
    values = [_decode_header_value(value) for value in _header_values(headers, header_name)]
    for name, address in getaddresses(values):
        if not name and not address:
            continue
        result.append(
            {
                "name": _sanitize_text(name) or "",
                "address": _sanitize_text(address) or "",
            }
        )
    return result


def _parse_datetime(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    try:
        dt = parsedate_to_datetime(value)
    except (TypeError, ValueError, IndexError):
        return None
    if isinstance(dt, datetime):
        return dt.isoformat()
    return None


def _decode_text(part) -> str:
    payload = part.get_payload(decode=True)
    if payload is None:
        payload = part.get_payload()
        return _sanitize_text(payload) if isinstance(payload, str) else ""

    candidates = []
    charset = part.get_content_charset()
    if charset:
        candidates.append(charset)
    candidates.extend(["utf-8", "gb18030", "gbk", "big5", "latin-1"])

    seen = set()
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        try:
            return _sanitize_text(payload.decode(candidate)) or ""
        except (LookupError, UnicodeDecodeError):
            continue

    return _sanitize_text(payload.decode("utf-8", errors="replace")) or ""


def _html_to_text(value: str) -> str:
    no_script = re.sub(r"(?is)<(script|style).*?>.*?</\1>", " ", value)
    no_tags = re.sub(r"(?s)<[^>]+>", " ", no_script)
    return re.sub(r"\s+", " ", html.unescape(no_tags)).strip()


def _build_snippet(body_text: str, body_html: str) -> str:
    source = body_text.strip() or _html_to_text(body_html)
    source = re.sub(r"\s+", " ", source).strip()
    return source[:280]


def parse_email_message(
    raw_bytes: bytes,
    account_email: str,
    folder_name: str,
    uid: int,
    uidvalidity: int,
    flags: List[str],
    internal_date_raw: Optional[str],
    size_bytes: int,
) -> ParsedMessage:
    message = BytesParser(policy=policy.default).parsebytes(raw_bytes)

    headers: Dict[str, List[str]] = defaultdict(list)
    for name, value in message.raw_items():
        headers[_sanitize_text(name) or ""].append(_sanitize_text(value) or "")

    message_id = _sanitize_text(_first_header(headers, "Message-ID"))
    subject = _decode_header_value(_first_header(headers, "Subject"))
    in_reply_to = _sanitize_text(_first_header(headers, "In-Reply-To"))
    references_header = _sanitize_text(_first_header(headers, "References"))
    sent_at_raw = _sanitize_text(_first_header(headers, "Date"))

    text_parts: List[str] = []
    html_parts: List[str] = []
    attachments: List[AttachmentMetadata] = []

    for index, part in enumerate(message.walk()):
        if part.is_multipart():
            continue

        content_type = part.get_content_type()
        filename = part.get_filename()
        disposition = part.get_content_disposition()
        payload = part.get_payload(decode=True) or b""
        is_attachment = disposition == "attachment" or bool(filename)

        if is_attachment:
            attachments.append(
                AttachmentMetadata(
                    part_index=index,
                    filename=_sanitize_text(filename),
                    content_type=_sanitize_text(content_type) or "application/octet-stream",
                    size_bytes=len(payload),
                    content_id=_sanitize_text(part.get("Content-ID")),
                    content_disposition=_sanitize_text(disposition),
                    is_inline=disposition == "inline",
                )
            )
            continue

        if content_type == "text/plain":
            text_parts.append(_decode_text(part))
        elif content_type == "text/html":
            html_parts.append(_decode_text(part))

    body_text = _sanitize_text("\n\n".join(part for part in text_parts if part).strip()) or ""
    body_html = _sanitize_text("\n\n".join(part for part in html_parts if part).strip()) or ""

    return ParsedMessage(
        account_email=account_email,
        folder_name=folder_name,
        uid=uid,
        uidvalidity=uidvalidity,
        message_id=message_id,
        subject=subject,
        in_reply_to=in_reply_to,
        references_header=references_header,
        sent_at=_parse_datetime(sent_at_raw),
        sent_at_raw=sent_at_raw,
        internal_date=_parse_datetime(internal_date_raw),
        internal_date_raw=_sanitize_text(internal_date_raw),
        flags=flags,
        size_bytes=size_bytes,
        from_addresses=_parse_addresses(headers, "From"),
        to_addresses=_parse_addresses(headers, "To"),
        cc_addresses=_parse_addresses(headers, "Cc"),
        bcc_addresses=_parse_addresses(headers, "Bcc"),
        reply_to_addresses=_parse_addresses(headers, "Reply-To"),
        sender_addresses=_parse_addresses(headers, "Sender"),
        body_text=body_text,
        body_html=body_html,
        snippet=_sanitize_text(_build_snippet(body_text, body_html)) or "",
        headers=dict(headers),
        has_attachments=bool(attachments),
        attachment_count=len(attachments),
        attachments=attachments,
    )
