from __future__ import annotations

from importlib import import_module
from typing import Any


__all__ = [
    "DEFAULT_OUTPUT_DIRNAME",
    "MAIN_SHEET_NAME",
    "VERSION",
    "build_rulespec",
    "build_structured_requirement",
    "build_visual_prompt_artifacts",
    "build_visual_reuse_spec",
    "collect_workbooks",
    "compile_workbook",
]


def __getattr__(name: str) -> Any:
    if name == "build_visual_prompt_artifacts":
        module = import_module(".visual_prompt_adapter", __name__)
        return getattr(module, name)

    if name in {
        "DEFAULT_OUTPUT_DIRNAME",
        "MAIN_SHEET_NAME",
        "VERSION",
        "build_rulespec",
        "build_structured_requirement",
        "build_visual_reuse_spec",
        "collect_workbooks",
        "compile_workbook",
    }:
        module = import_module(".workbook_visual_reuse_compiler", __name__)
        return getattr(module, name)

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
