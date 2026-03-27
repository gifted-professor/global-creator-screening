from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import List, Optional

from .config import Settings
from .creator_enrichment import enrich_creator_workbook
from .creator_review import prepare_duplicate_review, review_duplicate_groups
from .db import Database, MessageQuery
from .imap_sync import connect, discover_mailboxes, sync_mailboxes
from .relation_index import rebuild_relation_index


LOCAL_TZ = datetime.now().astimezone().tzinfo


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Sync mailbox data to local SQLite and raw .eml files.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    list_folders = subparsers.add_parser("list-folders", help="列出可同步的 IMAP 文件夹")
    list_folders.add_argument("--env-file", default=".env", help="配置文件路径，默认 ./.env")

    sync = subparsers.add_parser("sync", help="同步历史邮件并增量更新")
    sync.add_argument("--env-file", default=".env", help="配置文件路径，默认 ./.env")
    sync.add_argument("--folder", action="append", help="只同步指定文件夹，可重复传入")
    sync.add_argument("--limit", type=int, help="只抓最新 N 封用于测试，不推进增量游标")
    sync.add_argument("--reset-state", action="store_true", help="忽略本地游标，重新全量扫描")
    sync.add_argument("--workers", type=int, default=1, help="并发抓取 worker 数，默认 1")
    sync.add_argument("--sent-since", help="只抓这个日期及之后的邮件，格式 YYYY-MM-DD")

    stats = subparsers.add_parser("stats", help="查看本地 SQLite 里的统计")
    stats.add_argument("--env-file", default=".env", help="配置文件路径，默认 ./.env")

    query = subparsers.add_parser("query", help="按条件筛选本地邮件数据")
    query.add_argument("--env-file", default=".env", help="配置文件路径，默认 ./.env")
    query.add_argument("--folder", action="append", help="只查指定文件夹，可重复传入")
    query.add_argument("--from-contains", help="发件人名称或邮箱模糊匹配")
    query.add_argument("--subject-contains", help="主题模糊匹配")
    query.add_argument("--keyword", help="全文关键词，查主题/正文/发件人/收件人/附件名")
    query.add_argument("--attachment-name", help="附件名模糊匹配")
    query.add_argument("--sent-after", help="发件时间下限，支持 YYYY-MM-DD 或 ISO 时间")
    query.add_argument("--sent-before", help="发件时间上限，支持 YYYY-MM-DD 或 ISO 时间")
    attachment_group = query.add_mutually_exclusive_group()
    attachment_group.add_argument("--has-attachments", action="store_true", help="只看有附件的邮件")
    attachment_group.add_argument("--no-attachments", action="store_true", help="只看无附件的邮件")
    query.add_argument("--limit", type=int, default=20, help="最多返回多少条，默认 20")
    query.add_argument("--json", action="store_true", help="输出 JSON")

    index = subparsers.add_parser("index", help="重建联系人和线程索引")
    index.add_argument("--env-file", default=".env", help="配置文件路径，默认 ./.env")

    contacts = subparsers.add_parser("contacts", help="查看外部联系人列表")
    contacts.add_argument("--env-file", default=".env", help="配置文件路径，默认 ./.env")
    contacts.add_argument("--contains", help="按邮箱或姓名模糊匹配")
    contacts.add_argument("--limit", type=int, default=20, help="最多返回多少条，默认 20")
    contacts.add_argument("--json", action="store_true", help="输出 JSON")

    threads = subparsers.add_parser("threads", help="查看线程列表，可按联系人筛选")
    threads.add_argument("--env-file", default=".env", help="配置文件路径，默认 ./.env")
    threads.add_argument("--contact", help="只看某个邮箱地址参与的线程")
    threads.add_argument("--subject-contains", help="按标准化主题模糊匹配")
    threads.add_argument("--limit", type=int, default=20, help="最多返回多少条，默认 20")
    threads.add_argument("--json", action="store_true", help="输出 JSON")

    thread = subparsers.add_parser("thread", help="查看某个线程里的全部邮件")
    thread.add_argument("--env-file", default=".env", help="配置文件路径，默认 ./.env")
    thread.add_argument("--thread-key", required=True, help="线程主键，可从 threads 命令结果里复制")
    thread.add_argument("--json", action="store_true", help="输出 JSON")

    enrich = subparsers.add_parser("enrich-creators", help="把达人库和本地邮件库做映射，补齐最后一封邮件和报价")
    enrich.add_argument("--env-file", default=".env", help="配置文件路径，默认 ./.env")
    enrich.add_argument("--input", required=True, help="达人库 xlsx 路径")
    enrich.add_argument(
        "--output-prefix",
        default="exports/达人邮件可获取信息_v1",
        help="输出文件前缀，默认 exports/达人邮件可获取信息_v1",
    )

    review = subparsers.add_parser("prepare-duplicate-review", help="把共享同一 last_mail 的重复达人组整理成 sample-first 审核输入")
    review.add_argument("--env-file", default=".env", help="配置文件路径，默认 ./.env")
    review.add_argument("--input", required=True, help="高置信 enrichment xlsx 路径")
    review.add_argument("--db-path", help="覆盖邮件库 SQLite 路径；默认沿用 .env 里的 DB_PATH / DATA_DIR")
    review.add_argument(
        "--output-prefix",
        default="temp/duplicate_review_sample",
        help="输出文件前缀，默认 temp/duplicate_review_sample",
    )
    review.add_argument("--group-key", action="append", help="只准备指定 duplicate group key，可重复传入")
    review.add_argument("--sample-limit", type=int, default=3, help="默认只选前 N 个重复组，避免一次性全量跑")

    adjudicate = subparsers.add_parser("review-duplicate-groups", help="对 sample duplicate groups 做 group-level LLM 审核")
    adjudicate.add_argument("--env-file", default=".env", help="配置文件路径，默认 ./.env")
    adjudicate.add_argument("--input", required=True, help="高置信 enrichment xlsx 路径")
    adjudicate.add_argument("--db-path", help="覆盖邮件库 SQLite 路径；默认沿用 .env 里的 DB_PATH / DATA_DIR")
    adjudicate.add_argument(
        "--output-prefix",
        default="temp/duplicate_review_run",
        help="输出文件前缀，默认 temp/duplicate_review_run",
    )
    adjudicate.add_argument("--group-key", action="append", help="只审核指定 duplicate group key，可重复传入")
    adjudicate.add_argument("--sample-limit", type=int, default=3, help="默认只审核前 N 个 duplicate groups")
    adjudicate.add_argument("--base-url", help="覆盖 LLM base url；默认从 .env/.env.local 读取")
    adjudicate.add_argument("--api-key", help="覆盖 LLM api key；默认从 .env/.env.local 读取")
    adjudicate.add_argument("--model", help="覆盖 LLM model；默认从 .env/.env.local 读取")

    return parser


def _cmd_list_folders(settings: Settings) -> int:
    client = connect(settings)
    try:
        mailboxes = discover_mailboxes(client)
    finally:
        try:
            client.logout()
        except Exception:  # noqa: BLE001
            pass

    for mailbox in mailboxes:
        flags = " ".join(mailbox.flags) if mailbox.flags else "-"
        delimiter = mailbox.delimiter or "-"
        print(f"{mailbox.display_name}\tflags={flags}\tdelimiter={delimiter}\timap={mailbox.imap_name}")
    return 0


def _cmd_sync(
    settings: Settings,
    folders: Optional[List[str]],
    limit: Optional[int],
    reset_state: bool,
    workers: int,
    sent_since: Optional[str],
) -> int:
    if limit is not None and limit <= 0:
        raise ValueError("--limit 必须是大于 0 的整数。")
    if workers <= 0:
        raise ValueError("--workers 必须是大于 0 的整数。")
    sent_since_date = date.fromisoformat(sent_since) if sent_since else None

    settings.ensure_directories()
    db = Database(settings.db_path)
    try:
        db.init_schema()
        results = sync_mailboxes(
            settings,
            db,
            requested_folders=folders,
            limit=limit,
            reset_state=reset_state,
            workers=workers,
            sent_since=sent_since_date,
        )
    finally:
        db.close()

    for result in results:
        suffix = " (limit 模式，未推进游标)" if result.skipped_state_advance else ""
        print(
            f"{result.folder_name}: fetched={result.fetched}, "
            f"uidvalidity={result.uidvalidity}, server_count={result.message_count_on_server}, "
            f"last_seen_uid={result.last_seen_uid}{suffix}"
        )
    return 0


def _cmd_stats(settings: Settings) -> int:
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    db = Database(settings.db_path)
    try:
        db.init_schema()
        rows = db.fetch_stats()
    finally:
        db.close()

    if not rows:
        print("本地还没有任何邮件数据。先运行 python3 -m email_sync sync")
        return 0

    for row in rows:
        print(
            f"{row['folder_name']}\tmessages={row['message_count']}\t"
            f"with_attachments={row['messages_with_attachments'] or 0}\t"
            f"latest_uid={row['latest_uid']}\tlatest_sent_at={row['latest_sent_at'] or '-'}"
        )
    return 0


def _parse_datetime_input(value: str) -> tuple[datetime, bool]:
    stripped = value.strip()
    if "T" not in stripped and " " not in stripped:
        parsed_date = date.fromisoformat(stripped)
        if LOCAL_TZ is None:
            return datetime.combine(parsed_date, time.min), False
        return datetime.combine(parsed_date, time.min, tzinfo=LOCAL_TZ), False

    normalized = stripped.replace(" ", "T")
    dt = datetime.fromisoformat(normalized)
    if dt.tzinfo is None and LOCAL_TZ is not None:
        dt = dt.replace(tzinfo=LOCAL_TZ)
    return dt, True


def _normalize_after(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    dt, _ = _parse_datetime_input(value)
    return dt.isoformat()


def _normalize_before(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    dt, has_time = _parse_datetime_input(value)
    if not has_time:
        dt = dt + timedelta(days=1)
    else:
        dt = dt + timedelta(microseconds=1)
    return dt.isoformat()


def _format_addresses(value: str) -> str:
    try:
        entries = json.loads(value)
    except json.JSONDecodeError:
        return value

    parts = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        name = str(entry.get("name", "") or "").strip()
        address = str(entry.get("address", "") or "").strip()
        if name and address:
            parts.append(f"{name} <{address}>")
        elif address:
            parts.append(address)
        elif name:
            parts.append(name)
    return ", ".join(parts)


def _cmd_query(
    settings: Settings,
    folders: Optional[List[str]],
    from_contains: Optional[str],
    subject_contains: Optional[str],
    keyword: Optional[str],
    attachment_name: Optional[str],
    sent_after: Optional[str],
    sent_before: Optional[str],
    has_attachments: bool,
    no_attachments: bool,
    limit: int,
    as_json: bool,
) -> int:
    if limit <= 0:
        raise ValueError("--limit 必须是大于 0 的整数。")

    attachment_filter: Optional[bool] = None
    if has_attachments:
        attachment_filter = True
    if no_attachments:
        attachment_filter = False

    query = MessageQuery(
        folders=folders,
        from_contains=from_contains,
        subject_contains=subject_contains,
        keyword=keyword,
        attachment_name=attachment_name,
        sent_after=_normalize_after(sent_after),
        sent_before=_normalize_before(sent_before),
        has_attachments=attachment_filter,
        limit=limit,
    )

    db = Database(settings.db_path)
    try:
        db.init_schema()
        rows = db.search_messages(query)
    finally:
        db.close()

    if not rows:
        print("没有匹配到邮件。")
        return 0

    if as_json:
        print(json.dumps([dict(row) for row in rows], ensure_ascii=False, indent=2))
        return 0

    for row in rows:
        sender = _format_addresses(row["from_json"])
        print(
            f"[{row['id']}] {row['sent_at'] or '-'}  {row['folder_name']}  "
            f"UID={row['uid']}  attachments={row['attachment_count']}"
        )
        print(f"from: {sender or '-'}")
        print(f"subject: {row['subject'] or '-'}")
        if row["attachment_names"]:
            print(f"attachment_names: {row['attachment_names']}")
        print(f"snippet: {row['snippet'] or '-'}")
        print(f"raw: {row['raw_path']}")
        print("")
    return 0


def _cmd_index(settings: Settings) -> int:
    db = Database(settings.db_path)
    try:
        stats = rebuild_relation_index(db)
    finally:
        db.close()

    print(
        f"index rebuilt: messages={stats['messages_indexed']} "
        f"contacts={stats['contacts']} threads={stats['threads']} "
        f"thread_contact_links={stats['thread_contact_links']}"
    )
    return 0


def _cmd_contacts(settings: Settings, contains: Optional[str], limit: int, as_json: bool) -> int:
    if limit <= 0:
        raise ValueError("--limit 必须是大于 0 的整数。")

    db = Database(settings.db_path)
    try:
        db.init_schema()
        rows = db.fetch_contacts(limit=limit, contains=contains)
    finally:
        db.close()

    if not rows:
        print("没有可用的联系人索引。先运行 python3 -m email_sync index")
        return 0

    if as_json:
        print(json.dumps([dict(row) for row in rows], ensure_ascii=False, indent=2))
        return 0

    for row in rows:
        label = row["email_normalized"]
        if row["display_name"]:
            label = f"{row['display_name']} <{row['email_normalized']}>"
        print(f"[{row['id']}] {label}")
        print(
            f"messages={row['message_count']}  threads={row['thread_count']}  "
            f"inbound={row['inbound_message_count']}  outbound={row['outbound_message_count']}"
        )
        print(f"first_seen={row['first_seen_at'] or '-'}  last_seen={row['last_seen_at'] or '-'}")
        print("")
    return 0


def _cmd_threads(
    settings: Settings,
    contact: Optional[str],
    subject_contains: Optional[str],
    limit: int,
    as_json: bool,
) -> int:
    if limit <= 0:
        raise ValueError("--limit 必须是大于 0 的整数。")

    db = Database(settings.db_path)
    try:
        db.init_schema()
        rows = db.fetch_threads(limit=limit, contact_email=contact, subject_contains=subject_contains)
    finally:
        db.close()

    if not rows:
        print("没有可用的线程索引。先运行 python3 -m email_sync index")
        return 0

    if as_json:
        print(json.dumps([dict(row) for row in rows], ensure_ascii=False, indent=2))
        return 0

    for row in rows:
        print(f"{row['thread_key']}")
        print(
            f"messages={row['message_count']}  contacts={row['external_contact_count']}  "
            f"last_sent_at={row['last_sent_at'] or '-'}"
        )
        print(f"subject: {row['example_subject'] or row['normalized_subject'] or '-'}")
        print(f"contacts: {row['contact_labels'] or '-'}")
        print("")
    return 0


def _cmd_thread(settings: Settings, thread_key: str, as_json: bool) -> int:
    db = Database(settings.db_path)
    try:
        db.init_schema()
        rows = db.fetch_thread_messages(thread_key=thread_key)
    finally:
        db.close()

    if not rows:
        print("没有找到这个线程。先用 python3 -m email_sync threads 看可用 thread_key。")
        return 0

    if as_json:
        print(json.dumps([dict(row) for row in rows], ensure_ascii=False, indent=2))
        return 0

    for row in rows:
        print(
            f"[{row['id']}] {row['sent_at'] or row['internal_date'] or '-'}  "
            f"{row['direction']}  depth={row['thread_depth']}  {row['folder_name']}"
        )
        print(f"from: {_format_addresses(row['from_json']) or '-'}")
        print(f"to: {_format_addresses(row['to_json']) or '-'}")
        cc_value = _format_addresses(row["cc_json"])
        if cc_value:
            print(f"cc: {cc_value}")
        print(f"subject: {row['subject'] or '-'}")
        if row["attachment_names"]:
            print(f"attachment_names: {row['attachment_names']}")
        print(f"snippet: {row['snippet'] or '-'}")
        print(f"raw: {row['raw_path']}")
        print("")
    return 0


def _cmd_enrich_creators(settings: Settings, input_path: str, output_prefix: str) -> int:
    db = Database(settings.db_path)
    try:
        result = enrich_creator_workbook(
            db=db,
            input_path=Path(input_path),
            output_prefix=Path(output_prefix),
        )
    finally:
        db.close()

    print(
        f"creator enrichment finished: rows={result['rows']} matched={result['matched_rows']} "
        f"high_confidence={result['high_confidence_rows']}"
    )
    print(f"all csv: {result['csv_path']}")
    print(f"all xlsx: {result['xlsx_path']}")
    print(f"high csv: {result['high_csv_path']}")
    print(f"high xlsx: {result['high_xlsx_path']}")
    return 0


def _cmd_prepare_duplicate_review(
    settings: Settings,
    input_path: str,
    output_prefix: str,
    sample_limit: int,
    group_keys: Optional[List[str]],
    db_path_override: Optional[str],
) -> int:
    db_path = Path(db_path_override).expanduser() if db_path_override else settings.db_path
    db = Database(db_path)
    try:
        result = prepare_duplicate_review(
            db=db,
            input_path=Path(input_path),
            output_prefix=Path(output_prefix),
            sample_limit=sample_limit,
            group_keys=group_keys,
        )
    finally:
        db.close()

    print(
        f"duplicate review prepared: selected_groups={result['selected_group_count']} "
        f"duplicate_groups={result['stats']['duplicate_group_count']} "
        f"singleton_groups={result['stats']['singleton_group_count']}"
    )
    print(f"selected group keys: {', '.join(result['selected_group_keys']) or '-'}")
    print(f"groups json: {result['groups_json_path']}")
    print(f"summary json: {result['summary_json_path']}")
    return 0


def _cmd_review_duplicate_groups(
    settings: Settings,
    input_path: str,
    output_prefix: str,
    sample_limit: int,
    group_keys: Optional[List[str]],
    db_path_override: Optional[str],
    env_file: str,
    base_url: Optional[str],
    api_key: Optional[str],
    model: Optional[str],
) -> int:
    db_path = Path(db_path_override).expanduser() if db_path_override else settings.db_path
    db = Database(db_path)
    try:
        result = review_duplicate_groups(
            db=db,
            input_path=Path(input_path),
            output_prefix=Path(output_prefix),
            env_path=env_file,
            sample_limit=sample_limit,
            group_keys=group_keys,
            base_url=base_url,
            api_key=api_key,
            model=model,
        )
    finally:
        db.close()

    print(
        f"duplicate review run finished: selected_groups={result['selected_group_count']} "
        f"duplicate_groups={result['stats']['duplicate_group_count']}"
    )
    print(f"selected group keys: {', '.join(result['selected_group_keys']) or '-'}")
    print(f"audit json: {result['audit_json_path']}")
    print(f"annotated csv: {result['annotated_csv_path']}")
    print(f"annotated xlsx: {result['annotated_xlsx_path']}")
    print(f"summary json: {result['review_summary_json_path']}")
    return 0


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    try:
        require_credentials = args.command in {"list-folders", "sync"}
        settings = Settings.from_environment(args.env_file, require_credentials=require_credentials)
    except Exception as exc:  # noqa: BLE001
        print(f"[config error] {exc}", file=sys.stderr)
        return 2

    try:
        if args.command == "list-folders":
            return _cmd_list_folders(settings)
        if args.command == "sync":
            return _cmd_sync(settings, args.folder, args.limit, args.reset_state, args.workers, args.sent_since)
        if args.command == "stats":
            return _cmd_stats(settings)
        if args.command == "query":
            return _cmd_query(
                settings,
                args.folder,
                args.from_contains,
                args.subject_contains,
                args.keyword,
                args.attachment_name,
                args.sent_after,
                args.sent_before,
                args.has_attachments,
                args.no_attachments,
                args.limit,
                args.json,
            )
        if args.command == "index":
            return _cmd_index(settings)
        if args.command == "contacts":
            return _cmd_contacts(settings, args.contains, args.limit, args.json)
        if args.command == "threads":
            return _cmd_threads(settings, args.contact, args.subject_contains, args.limit, args.json)
        if args.command == "thread":
            return _cmd_thread(settings, args.thread_key, args.json)
        if args.command == "enrich-creators":
            return _cmd_enrich_creators(settings, args.input, args.output_prefix)
        if args.command == "prepare-duplicate-review":
            return _cmd_prepare_duplicate_review(
                settings,
                args.input,
                args.output_prefix,
                args.sample_limit,
                args.group_key,
                args.db_path,
            )
        if args.command == "review-duplicate-groups":
            return _cmd_review_duplicate_groups(
                settings,
                args.input,
                args.output_prefix,
                args.sample_limit,
                args.group_key,
                args.db_path,
                args.env_file,
                args.base_url,
                args.api_key,
                args.model,
            )
        raise ValueError(f"未知命令: {args.command}")
    except Exception as exc:  # noqa: BLE001
        print(f"[error] {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
