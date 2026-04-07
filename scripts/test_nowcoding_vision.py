from __future__ import annotations

import argparse
import base64
import json
import mimetypes
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any
from urllib import error, request


DEFAULT_BASE_URL = "https://nowcoding.ai/v1"
DEFAULT_MODEL = "gpt-5.4-openai-compact"
DEFAULT_PROMPT = "请用一句话描述这张图片里最主要的内容。"
DEFAULT_TEXT_PROMPT = "只回复：测试成功"


def parse_dotenv_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key:
            values[key] = value
    return values


def resolve_setting(name: str, cli_value: str | None, env_values: dict[str, str], default: str | None = None) -> str | None:
    if cli_value:
        return cli_value
    env_value = os.environ.get(name)
    if env_value:
        return env_value
    file_value = env_values.get(name)
    if file_value:
        return file_value
    return default


def guess_image_mime_type(image_path: Path) -> str:
    guessed, _encoding = mimetypes.guess_type(image_path.name)
    if guessed:
        return guessed
    suffix_map = {
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".png": "image/png",
        ".gif": "image/gif",
        ".webp": "image/webp",
        ".bmp": "image/bmp",
    }
    return suffix_map.get(image_path.suffix.lower(), "application/octet-stream")


def build_image_data_url(image_path: Path) -> str:
    mime_type = guess_image_mime_type(image_path)
    encoded = base64.b64encode(image_path.read_bytes()).decode("ascii")
    return f"data:{mime_type};base64,{encoded}"


def build_payload(args: argparse.Namespace, image_data_url: str | None) -> dict[str, Any]:
    if image_data_url:
        content: Any = [
            {"type": "text", "text": args.prompt},
            {"type": "image_url", "image_url": {"url": image_data_url}},
        ]
    else:
        content = args.prompt
    return {
        "model": args.model,
        "stream": args.stream,
        "max_tokens": args.max_tokens,
        "messages": [
            {
                "role": "user",
                "content": content,
            }
        ],
    }


def parse_stream_body(raw_text: str) -> tuple[str, list[dict[str, Any]]]:
    content_parts: list[str] = []
    chunks: list[dict[str, Any]] = []
    for line in raw_text.splitlines():
        if not line.startswith("data: "):
            continue
        payload_text = line[6:].strip()
        if not payload_text or payload_text == "[DONE]":
            continue
        try:
            payload = json.loads(payload_text)
        except json.JSONDecodeError:
            continue
        chunks.append(payload)
        for choice in payload.get("choices", []):
            delta = choice.get("delta") or {}
            piece = delta.get("content")
            if isinstance(piece, str):
                content_parts.append(piece)
    return "".join(content_parts).strip(), chunks


def parse_non_stream_body(raw_text: str) -> tuple[str, dict[str, Any] | None]:
    try:
        payload = json.loads(raw_text)
    except json.JSONDecodeError:
        return "", None

    choices = payload.get("choices") or []
    if not choices:
        return "", payload
    message = choices[0].get("message") or {}
    content = message.get("content")
    if isinstance(content, str):
        return content.strip(), payload
    if isinstance(content, list):
        text_parts: list[str] = []
        for item in content:
            if isinstance(item, dict) and isinstance(item.get("text"), str):
                text_parts.append(item["text"])
        return "".join(text_parts).strip(), payload
    return "", payload


def clip_text(value: str, limit: int = 240) -> str:
    text = " ".join(value.split())
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def run_probe(index: int, args: argparse.Namespace, payload_bytes: bytes) -> dict[str, Any]:
    endpoint = args.base_url.rstrip("/") + "/chat/completions"
    req = request.Request(
        endpoint,
        data=payload_bytes,
        method="POST",
        headers={
            "Authorization": f"Bearer {args.api_key}",
            "Content-Type": "application/json",
        },
    )
    started = time.perf_counter()
    status_code = 0
    raw_text = ""
    error_message = ""

    try:
        with request.urlopen(req, timeout=args.timeout) as response:
            status_code = response.getcode()
            raw_text = response.read().decode("utf-8", errors="replace")
    except error.HTTPError as exc:
        status_code = exc.code
        raw_text = exc.read().decode("utf-8", errors="replace")
        error_message = f"HTTP {exc.code}"
    except Exception as exc:  # pragma: no cover - network/runtime dependent
        error_message = str(exc)
    elapsed = time.perf_counter() - started

    if args.stream and raw_text:
        content, parsed_payload = parse_stream_body(raw_text)
    else:
        content, parsed_payload = parse_non_stream_body(raw_text)

    content_ok = bool(content)
    if args.require_substring:
        content_ok = args.require_substring in content

    ok = status_code == 200 and content_ok
    if status_code == 200 and not content_ok and not error_message:
        error_message = "empty_content" if not args.require_substring else f"missing_substring:{args.require_substring}"
    if status_code != 200 and not error_message:
        error_message = f"HTTP {status_code}"

    return {
        "index": index,
        "ok": ok,
        "status_code": status_code,
        "elapsed_seconds": round(elapsed, 3),
        "content": content,
        "error": error_message,
        "body_excerpt": clip_text(raw_text),
        "parsed_payload": parsed_payload,
    }


def build_summary(results: list[dict[str, Any]]) -> dict[str, Any]:
    ok_count = sum(1 for result in results if result["ok"])
    avg_elapsed = round(sum(result["elapsed_seconds"] for result in results) / len(results), 3) if results else 0.0
    status_counts: dict[str, int] = {}
    for result in results:
        key = str(result["status_code"])
        status_counts[key] = status_counts.get(key, 0) + 1
    return {
        "total_requests": len(results),
        "ok_requests": ok_count,
        "failed_requests": len(results) - ok_count,
        "average_elapsed_seconds": avg_elapsed,
        "status_counts": status_counts,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Smoke/load test nowcoding.ai chat/completions text or vision requests.")
    parser.add_argument("--env-file", default=".env", help="env 文件路径，默认 ./.env")
    parser.add_argument("--api-key", help="覆盖 NOWCODING_API_KEY")
    parser.add_argument("--base-url", help=f"覆盖 NOWCODING_BASE_URL，默认 {DEFAULT_BASE_URL}")
    parser.add_argument("--model", help=f"覆盖 NOWCODING_MODEL，默认 {DEFAULT_MODEL}")
    parser.add_argument("--prompt", help="要发送的 prompt；带图片时默认是图片描述 prompt，纯文本时默认是文本 smoke prompt")
    parser.add_argument("--image-path", help="本地图片路径；传入后会自动转成 data URL")
    parser.add_argument("--max-tokens", type=int, default=120, help="max_tokens，默认 120")
    parser.add_argument("--timeout", type=float, default=60, help="单请求超时秒数，默认 60")
    parser.add_argument("--repeat", type=int, default=1, help="总请求数，默认 1")
    parser.add_argument("--concurrency", type=int, default=1, help="并发 worker 数，默认 1")
    parser.add_argument("--require-substring", help="如果设置，则返回内容里必须包含这个子串才算成功")
    parser.add_argument("--output-json", help="把完整结果写到 JSON 文件")
    parser.add_argument("--no-stream", action="store_true", help="关闭 stream=true；nowcoding 当前更推荐保留流式")
    parser.add_argument("--show-body", action="store_true", help="打印每个请求的响应摘要")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    env_values = parse_dotenv_file(Path(args.env_file))

    args.api_key = resolve_setting("NOWCODING_API_KEY", args.api_key, env_values)
    args.base_url = resolve_setting("NOWCODING_BASE_URL", args.base_url, env_values, DEFAULT_BASE_URL)
    args.model = resolve_setting("NOWCODING_MODEL", args.model, env_values, DEFAULT_MODEL)
    args.stream = not args.no_stream

    if not args.api_key:
        print("缺少 NOWCODING_API_KEY；可通过 --api-key、环境变量或 env 文件提供。", file=sys.stderr)
        return 2

    if args.repeat <= 0:
        print("--repeat 必须大于 0", file=sys.stderr)
        return 2
    if args.concurrency <= 0:
        print("--concurrency 必须大于 0", file=sys.stderr)
        return 2

    image_data_url = None
    if args.image_path:
        image_path = Path(args.image_path).expanduser().resolve()
        if not image_path.exists():
            print(f"图片不存在: {image_path}", file=sys.stderr)
            return 2
        image_data_url = build_image_data_url(image_path)

    if not args.prompt:
        args.prompt = DEFAULT_PROMPT if image_data_url else DEFAULT_TEXT_PROMPT

    payload = build_payload(args, image_data_url)
    payload_bytes = json.dumps(payload, ensure_ascii=False).encode("utf-8")

    print(
        json.dumps(
            {
                "endpoint": args.base_url.rstrip("/") + "/chat/completions",
                "model": args.model,
                "stream": args.stream,
                "has_image": bool(image_data_url),
                "repeat": args.repeat,
                "concurrency": args.concurrency,
            },
            ensure_ascii=False,
        )
    )

    results: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
        futures = [executor.submit(run_probe, index + 1, args, payload_bytes) for index in range(args.repeat)]
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            line = f"[{result['index']}/{args.repeat}] ok={result['ok']} status={result['status_code']} elapsed={result['elapsed_seconds']}s"
            if result["content"]:
                line += f" content={result['content']!r}"
            if result["error"]:
                line += f" error={result['error']}"
            print(line)
            if args.show_body:
                print(f"  body={result['body_excerpt']}")

    results.sort(key=lambda item: item["index"])
    summary = build_summary(results)
    print(json.dumps({"summary": summary}, ensure_ascii=False))

    if args.output_json:
        output_path = Path(args.output_json).expanduser().resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps({"request": payload, "summary": summary, "results": results}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(f"wrote {output_path}")

    return 0 if summary["failed_requests"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
