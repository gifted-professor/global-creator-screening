from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import imaplib
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Dict, Iterable, List, Optional, Sequence

from .config import Settings
from .db import Database
from .filesystem import store_raw_message
from .imap_utf7 import decode as decode_imap_utf7
from .mail_parser import ParsedMessage, parse_email_message


LIST_PATTERN = re.compile(rb'^\((?P<flags>[^)]*)\)\s+(?P<delimiter>NIL|"[^"]*")\s+(?P<name>.+)$')
UID_PATTERN = re.compile(rb"UID (\d+)")
SIZE_PATTERN = re.compile(rb"RFC822\.SIZE (\d+)")
INTERNALDATE_PATTERN = re.compile(rb'INTERNALDATE "([^"]+)"')
PARALLEL_FETCH_BATCH_SIZE = 20
SHARED_BACKUP_FOLDER_RETRY_LIMIT = 3
RETRYABLE_IMAP_ERROR_MARKERS = (
    "socket error: eof",
    "connection aborted",
    "server closed connection",
    "timed out",
    "system busy",
)


@dataclass
class MailboxInfo:
    display_name: str
    imap_name: str
    delimiter: Optional[str]
    flags: List[str]


@dataclass
class SyncResult:
    folder_name: str
    fetched: int
    skipped_state_advance: bool
    last_seen_uid: int
    uidvalidity: Optional[int]
    message_count_on_server: Optional[int]


@dataclass
class FetchedMessage:
    parsed: ParsedMessage
    raw_bytes: bytes


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_flags(flags: Sequence[str]) -> List[str]:
    return [flag.lower() for flag in flags]


def _unquote_ascii(token: bytes) -> str:
    token = token.strip()
    if token == b"NIL":
        return ""
    if token.startswith(b'"') and token.endswith(b'"'):
        token = token[1:-1].replace(b'\\"', b'"').replace(b"\\\\", b"\\")
    return token.decode("ascii", errors="replace")


def _extract_number(pattern: re.Pattern[bytes], value: bytes) -> Optional[int]:
    match = pattern.search(value)
    if not match:
        return None
    return int(match.group(1))


def _extract_internal_date(value: bytes) -> Optional[str]:
    match = INTERNALDATE_PATTERN.search(value)
    if not match:
        return None
    return match.group(1).decode("ascii", errors="replace")


def connect(settings: Settings) -> imaplib.IMAP4_SSL:
    client = imaplib.IMAP4_SSL(settings.imap_host, settings.imap_port)
    client.login(settings.account_email, settings.auth_code)
    return client


def discover_mailboxes(client: imaplib.IMAP4_SSL) -> List[MailboxInfo]:
    status, data = client.list()
    if status != "OK":
        raise RuntimeError("无法列出 IMAP 文件夹。")

    mailboxes: List[MailboxInfo] = []
    for item in data:
        if not item:
            continue
        match = LIST_PATTERN.match(item)
        if not match:
            continue

        flags_raw = match.group("flags").decode("ascii", errors="replace")
        flags = [flag for flag in flags_raw.split() if flag]
        delimiter = _unquote_ascii(match.group("delimiter")) or None
        imap_name = _unquote_ascii(match.group("name"))
        display_name = decode_imap_utf7(imap_name)

        mailboxes.append(
            MailboxInfo(
                display_name=display_name,
                imap_name=imap_name,
                delimiter=delimiter,
                flags=flags,
            )
        )

    mailboxes.sort(key=lambda mailbox: mailbox.display_name.lower())
    return mailboxes


def resolve_mailboxes(discovered: Sequence[MailboxInfo], requested_folders: Optional[Sequence[str]]) -> List[MailboxInfo]:
    selectable = [mailbox for mailbox in discovered if "\\noselect" not in _normalize_flags(mailbox.flags)]
    if not requested_folders:
        return selectable

    selected: List[MailboxInfo] = []
    lower_map: Dict[str, MailboxInfo] = {}

    for mailbox in discovered:
        lower_map[mailbox.display_name.lower()] = mailbox
        lower_map[mailbox.imap_name.lower()] = mailbox

    for requested in requested_folders:
        mailbox = lower_map.get(requested.lower())
        if mailbox is None:
            available = ", ".join(item.display_name for item in selectable)
            raise ValueError(f"找不到文件夹 {requested!r}。可用文件夹：{available}")
        if "\\noselect" in _normalize_flags(mailbox.flags):
            raise ValueError(f"文件夹 {requested!r} 不能被选中同步。")
        if mailbox not in selected:
            selected.append(mailbox)

    return selected


def _quote_mailbox_name(imap_name: str) -> str:
    escaped = imap_name.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def _response_number(client: imaplib.IMAP4_SSL, code: str) -> Optional[int]:
    _, data = client.response(code)
    if not data or not data[0]:
        return None
    try:
        return int(data[0])
    except (TypeError, ValueError):
        return None


def _format_imap_date(value: date) -> str:
    return value.strftime("%d-%b-%Y")


def _is_retryable_imap_error(error: object) -> bool:
    text = str(error or "").strip().lower()
    if not text:
        return False
    return any(marker in text for marker in RETRYABLE_IMAP_ERROR_MARKERS)


def _is_shared_backup_mailbox(mailbox: MailboxInfo) -> bool:
    display_name = str(mailbox.display_name or "").strip()
    return "邮件备份" in display_name


def _mailbox_retry_limit(mailbox: MailboxInfo) -> int:
    return SHARED_BACKUP_FOLDER_RETRY_LIMIT if _is_shared_backup_mailbox(mailbox) else 1


def _checkpoint_sync_state(
    db: Database,
    *,
    settings: Settings,
    mailbox: MailboxInfo,
    uidvalidity: Optional[int],
    highest_uid: int,
    fetched_count: int,
    started_at: str,
) -> None:
    db.update_sync_state(
        account_email=settings.account_email,
        folder_name=mailbox.display_name,
        uidvalidity=uidvalidity,
        last_seen_uid=highest_uid,
        last_run_synced=fetched_count,
        last_sync_started_at=started_at,
        last_sync_completed_at=None,
        last_error=None,
    )


def _search_uids(
    client: imaplib.IMAP4_SSL,
    last_seen_uid: int,
    sent_since: Optional[date] = None,
) -> List[int]:
    criteria: List[str] = []
    if last_seen_uid > 0:
        criteria.append(f"UID {last_seen_uid + 1}:*")
    if sent_since is not None:
        criteria.append(f"SINCE {_format_imap_date(sent_since)}")
    if not criteria:
        criteria.append("ALL")

    status, data = client.uid("search", None, *criteria)
    if status != "OK":
        raise RuntimeError(f"IMAP SEARCH 失败：{' '.join(criteria)}")
    if not data or not data[0]:
        return []
    return [int(item) for item in data[0].split() if item]


def _fetch_raw_message(client: imaplib.IMAP4_SSL, uid: int) -> Dict[str, object]:
    status, data = client.uid("fetch", str(uid), "(UID FLAGS INTERNALDATE RFC822.SIZE BODY.PEEK[])")
    if status != "OK":
        raise RuntimeError(f"IMAP FETCH 失败，UID={uid}")

    for item in data:
        if isinstance(item, tuple):
            metadata_bytes, raw_bytes = item
            flags = [
                flag.decode("ascii", errors="replace") if isinstance(flag, bytes) else str(flag)
                for flag in imaplib.ParseFlags(metadata_bytes)
            ]
            return {
                "uid": _extract_number(UID_PATTERN, metadata_bytes) or uid,
                "flags": flags,
                "internal_date_raw": _extract_internal_date(metadata_bytes),
                "size_bytes": _extract_number(SIZE_PATTERN, metadata_bytes) or len(raw_bytes),
                "raw_bytes": raw_bytes,
            }

    raise RuntimeError(f"没有拿到邮件正文，UID={uid}")


def _build_fetched_message(
    settings: Settings,
    mailbox: MailboxInfo,
    uidvalidity: int,
    payload: Dict[str, object],
) -> FetchedMessage:
    raw_bytes = payload["raw_bytes"]
    assert isinstance(raw_bytes, bytes)

    parsed = parse_email_message(
        raw_bytes=raw_bytes,
        account_email=settings.account_email,
        folder_name=mailbox.display_name,
        uid=int(payload["uid"]),
        uidvalidity=uidvalidity,
        flags=list(payload["flags"]),
        internal_date_raw=payload["internal_date_raw"],
        size_bytes=int(payload["size_bytes"]),
    )
    return FetchedMessage(parsed=parsed, raw_bytes=raw_bytes)


def _persist_fetched_message(settings: Settings, db: Database, fetched_message: FetchedMessage) -> None:
    parsed = fetched_message.parsed
    raw_path, raw_sha256, raw_size_bytes = store_raw_message(
        settings.data_dir,
        settings.raw_dir,
        parsed.account_email,
        parsed.folder_name,
        parsed.uidvalidity,
        parsed.uid,
        fetched_message.raw_bytes,
    )
    db.upsert_message(parsed, raw_path, raw_sha256, raw_size_bytes)


def _chunk_uids(uids: Sequence[int], batch_size: int) -> List[List[int]]:
    if batch_size <= 0:
        raise ValueError("batch_size must be > 0")
    return [list(uids[index : index + batch_size]) for index in range(0, len(uids), batch_size)]


def _fetch_uid_batch(
    settings: Settings,
    mailbox: MailboxInfo,
    uidvalidity: int,
    uid_batch: Sequence[int],
) -> tuple[list[FetchedMessage], list[tuple[int, str]]]:
    successes: list[FetchedMessage] = []
    errors: list[tuple[int, str]] = []
    client: Optional[imaplib.IMAP4_SSL] = None

    try:
        client = connect(settings)
        status, _ = client.select(_quote_mailbox_name(mailbox.imap_name), readonly=settings.readonly)
        if status != "OK":
            raise RuntimeError(f"无法选择文件夹 {mailbox.display_name}")

        for uid in uid_batch:
            try:
                payload = _fetch_raw_message(client, uid)
                successes.append(_build_fetched_message(settings, mailbox, uidvalidity, payload))
            except Exception as message_error:  # noqa: BLE001
                errors.append((uid, str(message_error)))
    except Exception as batch_error:  # noqa: BLE001
        return [], [(uid, str(batch_error)) for uid in uid_batch]
    finally:
        if client is not None:
            try:
                client.close()
            except Exception:  # noqa: BLE001
                pass
            try:
                client.logout()
            except Exception:  # noqa: BLE001
                pass

    return successes, errors


def _iter_parallel_fetch_batches(
    settings: Settings,
    mailbox: MailboxInfo,
    uidvalidity: int,
    selected_uids: Sequence[int],
    workers: int,
) -> Iterable[tuple[list[FetchedMessage], list[tuple[int, str]]]]:
    batches = _chunk_uids(selected_uids, PARALLEL_FETCH_BATCH_SIZE)
    if not batches:
        return []

    max_workers = min(max(1, workers), len(batches))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(_fetch_uid_batch, settings, mailbox, uidvalidity, batch)
            for batch in batches
        ]
        for future in as_completed(futures):
            yield future.result()


def _record_message_error(db: Database, account_email: str, folder_name: str, uid: int, error_message: str) -> None:
    db.record_sync_error(
        account_email,
        folder_name,
        uid,
        "message",
        error_message,
    )
    print(f"[warn] {folder_name} UID={uid} 处理失败: {error_message}")


def _emit_progress(folder_name: str, processed_count: int, total_count: int, last_reported: int) -> int:
    while processed_count >= last_reported + 25:
        last_reported += 25
        print(f"[sync] {folder_name}: 已抓取 {last_reported}/{total_count} 封")
    return last_reported


def sync_mailboxes(
    settings: Settings,
    db: Database,
    requested_folders: Optional[Sequence[str]] = None,
    limit: Optional[int] = None,
    reset_state: bool = False,
    workers: int = 1,
    sent_since: Optional[date] = None,
) -> List[SyncResult]:
    if workers <= 0:
        raise ValueError("workers must be > 0")

    results: List[SyncResult] = []
    client = connect(settings)

    try:
        discovered = discover_mailboxes(client)
        mailboxes = resolve_mailboxes(discovered, requested_folders or settings.mail_folders)

        for mailbox in mailboxes:
            started_at = _utc_now()
            fetched_count = 0
            skipped_state_advance = False
            highest_uid = 0
            state_last_seen_uid = 0
            uidvalidity: Optional[int] = None
            message_count_on_server: Optional[int] = None

            retry_limit = _mailbox_retry_limit(mailbox)
            for attempt_index in range(retry_limit):
                mailbox_client: Optional[imaplib.IMAP4_SSL] = None
                try:
                    mailbox_client = connect(settings)
                    status, data = mailbox_client.select(_quote_mailbox_name(mailbox.imap_name), readonly=settings.readonly)
                    if status != "OK":
                        raise RuntimeError(f"无法选择文件夹 {mailbox.display_name}")

                    message_count_on_server = int(data[0]) if data and data[0] else None
                    uidvalidity = _response_number(mailbox_client, "UIDVALIDITY")

                    db.record_mailbox(
                        account_email=settings.account_email,
                        folder_name=mailbox.display_name,
                        imap_name=mailbox.imap_name,
                        delimiter=mailbox.delimiter,
                        flags=mailbox.flags,
                        uidvalidity=uidvalidity,
                        message_count_on_server=message_count_on_server,
                    )

                    state = db.get_sync_state(settings.account_email, mailbox.display_name)
                    last_seen_uid = 0
                    if state and not reset_state:
                        state_uidvalidity = state["uidvalidity"]
                        if state_uidvalidity == uidvalidity:
                            last_seen_uid = int(state["last_seen_uid"])

                    candidate_uids = _search_uids(mailbox_client, last_seen_uid, sent_since=sent_since)
                    selected_uids = candidate_uids
                    if limit is not None and limit > 0 and len(candidate_uids) > limit:
                        selected_uids = candidate_uids[-limit:]
                        skipped_state_advance = True

                    highest_uid = max(highest_uid, last_seen_uid)
                    processed_count = 0
                    last_reported = 0

                    if workers > 1 and len(selected_uids) > 1:
                        for fetched_messages, message_errors in _iter_parallel_fetch_batches(
                            settings,
                            mailbox,
                            uidvalidity or 0,
                            selected_uids,
                            workers,
                        ):
                            for fetched_message in fetched_messages:
                                _persist_fetched_message(settings, db, fetched_message)
                                highest_uid = max(highest_uid, fetched_message.parsed.uid)
                                fetched_count += 1

                            if not skipped_state_advance and fetched_messages:
                                _checkpoint_sync_state(
                                    db,
                                    settings=settings,
                                    mailbox=mailbox,
                                    uidvalidity=uidvalidity,
                                    highest_uid=highest_uid,
                                    fetched_count=fetched_count,
                                    started_at=started_at,
                                )

                            if message_errors and not fetched_messages and all(
                                _is_retryable_imap_error(error_message)
                                for _, error_message in message_errors
                            ):
                                raise RuntimeError(message_errors[0][1])

                            for uid, error_message in message_errors:
                                _record_message_error(
                                    db,
                                    settings.account_email,
                                    mailbox.display_name,
                                    uid,
                                    error_message,
                                )

                            processed_count += len(fetched_messages) + len(message_errors)
                            last_reported = _emit_progress(
                                mailbox.display_name,
                                processed_count,
                                len(selected_uids),
                                last_reported,
                            )
                    else:
                        for uid in selected_uids:
                            try:
                                payload = _fetch_raw_message(mailbox_client, uid)
                                fetched_message = _build_fetched_message(settings, mailbox, uidvalidity or 0, payload)
                                _persist_fetched_message(settings, db, fetched_message)
                                highest_uid = max(highest_uid, fetched_message.parsed.uid)
                                fetched_count += 1
                                if not skipped_state_advance:
                                    _checkpoint_sync_state(
                                        db,
                                        settings=settings,
                                        mailbox=mailbox,
                                        uidvalidity=uidvalidity,
                                        highest_uid=highest_uid,
                                        fetched_count=fetched_count,
                                        started_at=started_at,
                                    )
                            except Exception as message_error:  # noqa: BLE001
                                if _is_retryable_imap_error(message_error):
                                    raise RuntimeError(str(message_error)) from message_error
                                _record_message_error(
                                    db,
                                    settings.account_email,
                                    mailbox.display_name,
                                    uid,
                                    str(message_error),
                                )

                            processed_count += 1
                            last_reported = _emit_progress(
                                mailbox.display_name,
                                processed_count,
                                len(selected_uids),
                                last_reported,
                            )

                    state_last_seen_uid = highest_uid if not skipped_state_advance else last_seen_uid
                    completed_at = _utc_now()
                    db.update_sync_state(
                        account_email=settings.account_email,
                        folder_name=mailbox.display_name,
                        uidvalidity=uidvalidity,
                        last_seen_uid=state_last_seen_uid,
                        last_run_synced=fetched_count,
                        last_sync_started_at=started_at,
                        last_sync_completed_at=completed_at,
                        last_error=None,
                    )
                    break
                except Exception as folder_error:  # noqa: BLE001
                    retryable_error = _is_retryable_imap_error(folder_error)
                    should_retry = retryable_error and attempt_index + 1 < retry_limit
                    if should_retry:
                        db.record_sync_error(
                            settings.account_email,
                            mailbox.display_name,
                            None,
                            "folder_retry",
                            f"{folder_error} (retry {attempt_index + 1}/{retry_limit - 1})",
                        )
                        print(f"[warn] {mailbox.display_name}: {folder_error}，准备重连重试 ({attempt_index + 1}/{retry_limit - 1})")
                        continue

                    db.record_sync_error(
                        settings.account_email,
                        mailbox.display_name,
                        None,
                        "folder",
                        str(folder_error),
                    )
                    existing_state = db.get_sync_state(settings.account_email, mailbox.display_name)
                    db.update_sync_state(
                        account_email=settings.account_email,
                        folder_name=mailbox.display_name,
                        uidvalidity=uidvalidity,
                        last_seen_uid=int(existing_state["last_seen_uid"]) if existing_state else 0,
                        last_run_synced=fetched_count,
                        last_sync_started_at=started_at,
                        last_sync_completed_at=_utc_now(),
                        last_error=str(folder_error),
                    )
                    print(f"[error] {mailbox.display_name}: {folder_error}")
                    state_last_seen_uid = int(existing_state["last_seen_uid"]) if existing_state else 0
                    break
                finally:
                    if mailbox_client is not None:
                        try:
                            mailbox_client.close()
                        except Exception:  # noqa: BLE001
                            pass
                        try:
                            mailbox_client.logout()
                        except Exception:  # noqa: BLE001
                            pass

            results.append(
                SyncResult(
                    folder_name=mailbox.display_name,
                    fetched=fetched_count,
                    skipped_state_advance=skipped_state_advance,
                    last_seen_uid=state_last_seen_uid,
                    uidvalidity=uidvalidity,
                    message_count_on_server=message_count_on_server,
                )
            )
    finally:
        try:
            client.close()
        except Exception:  # noqa: BLE001
            pass
        try:
            client.logout()
        except Exception:  # noqa: BLE001
            pass

    return results
