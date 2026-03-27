from __future__ import annotations

import base64


def encode(value: str) -> str:
    result = []
    buffer = []

    def flush_buffer() -> None:
        if not buffer:
            return
        chunk = "".join(buffer).encode("utf-16-be")
        encoded = base64.b64encode(chunk).decode("ascii").rstrip("=").replace("/", ",")
        result.append("&" + encoded + "-")
        buffer.clear()

    for char in value:
        code_point = ord(char)
        if 0x20 <= code_point <= 0x7E:
            flush_buffer()
            if char == "&":
                result.append("&-")
            else:
                result.append(char)
        else:
            buffer.append(char)

    flush_buffer()
    return "".join(result)


def decode(value: str) -> str:
    result = []
    index = 0

    while index < len(value):
        if value[index] != "&":
            result.append(value[index])
            index += 1
            continue

        end_index = value.find("-", index)
        if end_index == -1:
            raise ValueError("非法的 IMAP modified UTF-7 字符串。")

        if end_index == index + 1:
            result.append("&")
            index = end_index + 1
            continue

        chunk = value[index + 1 : end_index].replace(",", "/")
        padding = "=" * (-len(chunk) % 4)
        decoded = base64.b64decode(chunk + padding).decode("utf-16-be")
        result.append(decoded)
        index = end_index + 1

    return "".join(result)
