from __future__ import annotations

import html
import json
from pathlib import Path
import re
from typing import Any

from workbook_template_parser import compile_workbook


def write_json(path: str | Path, payload: Any) -> Path:
    target = Path(path).expanduser()
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return target


def load_json(path: str | Path) -> dict[str, Any]:
    target = Path(path).expanduser()
    return json.loads(target.read_text(encoding="utf-8"))


def safe_path_component(value: str) -> str:
    normalized = re.sub(r"[\\/]+", "-", str(value or "").strip())
    normalized = re.sub(r"\s+", "_", normalized)
    normalized = re.sub(r"[^\w.-]+", "-", normalized, flags=re.UNICODE)
    normalized = normalized.strip("._-")
    return normalized or "repo-local-runtime"


def build_next_steps(*, workbook_path: Path, summary_path: Path, task_name: str = "") -> list[dict[str, str]]:
    next_steps = [
        {
            "label": "inspect_summary",
            "description": "查看 repo-local summary，确认模板解析和产物路径。",
            "path": str(summary_path),
        },
        {
            "label": "prepare_screening_inputs",
            "description": "如需继续接入筛号运行态，用模板 workbook 走当前仓库的输入准备脚本。",
            "command": (
                "python3 scripts/prepare_screening_inputs.py "
                f'--template-workbook "{workbook_path}" '
                f'--summary-json "{summary_path.parent / "prepare_screening_inputs_summary.json"}"'
            ),
        },
    ]
    if task_name:
        next_steps.append(
            {
                "label": "run_single_entry_pipeline",
                "description": "如果这是任务上传里的正式任务，优先走 repo-local 单入口主线。",
                "command": (
                    "python3 scripts/run_task_upload_to_keep_list_pipeline.py "
                    f'--task-name "{task_name}"'
                ),
            }
        )
    return next_steps


def build_repo_local_workbook_runtime(
    *,
    workbook_path: str | Path,
    runtime_root: str | Path,
    project_code: str,
    primary_category: str,
    owner_name: str = "",
    task_name: str = "",
    record_id: str = "",
    linked_bitable_url: str = "",
    source_url: str = "",
    dashboard_output: str | Path | None = None,
    summary_name: str = "summary.json",
) -> dict[str, Any]:
    resolved_workbook_path = Path(workbook_path).expanduser().resolve()
    resolved_runtime_root = Path(runtime_root).expanduser().resolve()
    resolved_runtime_root.mkdir(parents=True, exist_ok=True)

    parse_output_root = resolved_runtime_root / "parsed_outputs"
    compile_report = compile_workbook(resolved_workbook_path, parse_output_root)
    artifacts = dict(compile_report.get("artifacts") or {})
    structured = load_json(artifacts["structured_requirement_json"])
    rulespec = load_json(artifacts["rulespec_json"])
    basic_info = dict(structured.get("basic_info") or {})
    project_name = (
        str(basic_info.get("project_name") or "").strip()
        or str(task_name or "").strip()
        or str(project_code or "").strip()
        or resolved_workbook_path.stem
    )
    platforms = [str(item).strip() for item in (basic_info.get("platform_scope") or []) if str(item).strip()]
    summary_path = resolved_runtime_root / summary_name
    project_state_path = resolved_runtime_root / "project_state.json"
    dashboard_path = (
        Path(dashboard_output).expanduser().resolve()
        if dashboard_output is not None
        else resolved_runtime_root / "dashboard.html"
    )
    next_steps = build_next_steps(
        workbook_path=resolved_workbook_path,
        summary_path=summary_path,
        task_name=task_name,
    )
    rules = list(rulespec.get("rules") or [])
    project_state = {
        "mode": "repo_local",
        "project_code": project_code,
        "project_name": project_name,
        "primary_category": primary_category,
        "owner_name": owner_name,
        "task_name": task_name,
        "record_id": record_id,
        "linked_bitable_url": linked_bitable_url,
        "source_url": source_url,
        "workbook_path": str(resolved_workbook_path),
        "runtime_root": str(resolved_runtime_root),
        "template_parse_output_dir": str(compile_report["output_dir"]),
        "template_parse_report_path": str(Path(compile_report["output_dir"]) / "compile_report.json"),
        "template_parse_artifacts": artifacts,
        "platforms": platforms,
        "rule_count": len(rules),
        "next_steps": next_steps,
    }
    summary = {
        "ok": True,
        "mode": "repo_local",
        "projectCode": project_code,
        "projectName": project_name,
        "primaryCategory": primary_category,
        "ownerName": owner_name,
        "taskName": task_name,
        "recordId": record_id,
        "linkedBitableUrl": linked_bitable_url,
        "compiledRowCount": len(platforms),
        "platforms": platforms,
        "savedWorkbookPath": str(resolved_workbook_path),
        "summaryJson": str(summary_path),
        "projectStatePath": str(project_state_path),
        "dashboardOutput": str(dashboard_path),
        "templateParseOutputDir": str(compile_report["output_dir"]),
        "templateParseReportPath": str(Path(compile_report["output_dir"]) / "compile_report.json"),
        "templateParseArtifacts": artifacts,
        "templateParseWarnings": list(compile_report.get("warnings") or []),
        "templateParseStats": dict(compile_report.get("stats") or {}),
        "nextSteps": next_steps,
    }
    write_json(project_state_path, project_state)
    write_json(summary_path, summary)
    write_dashboard_html(dashboard_path, summary)
    return summary


def write_dashboard_html(path: str | Path, summary: dict[str, Any]) -> Path:
    target = Path(path).expanduser()
    target.parent.mkdir(parents=True, exist_ok=True)
    project_name = html.escape(str(summary.get("projectName") or "-"))
    project_code = html.escape(str(summary.get("projectCode") or "-"))
    workbook_path = html.escape(str(summary.get("savedWorkbookPath") or "-"))
    summary_json = html.escape(str(summary.get("summaryJson") or "-"))
    project_state = html.escape(str(summary.get("projectStatePath") or "-"))
    artifacts = summary.get("templateParseArtifacts") or {}
    artifact_items = "\n".join(
        f"<li><strong>{html.escape(str(key))}</strong>: <code>{html.escape(str(value))}</code></li>"
        for key, value in artifacts.items()
    )
    next_steps = summary.get("nextSteps") or []
    next_step_items = "\n".join(
        "<li>"
        f"<strong>{html.escape(str(item.get('label') or '-'))}</strong>: "
        f"{html.escape(str(item.get('description') or ''))}"
        + (
            f"<br><code>{html.escape(str(item.get('command') or item.get('path') or ''))}</code>"
            if item.get("command") or item.get("path")
            else ""
        )
        + "</li>"
        for item in next_steps
    )
    target.write_text(
        (
            "<!doctype html>\n"
            "<html lang=\"zh-CN\">\n"
            "<head><meta charset=\"utf-8\"><title>Repo-local Workbook Runtime</title></head>\n"
            "<body>\n"
            "<h1>Repo-local Workbook Runtime</h1>\n"
            f"<p><strong>project</strong>: {project_code} / {project_name}</p>\n"
            f"<p><strong>workbook</strong>: <code>{workbook_path}</code></p>\n"
            f"<p><strong>summary</strong>: <code>{summary_json}</code></p>\n"
            f"<p><strong>project_state</strong>: <code>{project_state}</code></p>\n"
            "<h2>Template Artifacts</h2>\n"
            f"<ul>{artifact_items}</ul>\n"
            "<h2>Next Steps</h2>\n"
            f"<ul>{next_step_items}</ul>\n"
            "</body>\n"
            "</html>\n"
        ),
        encoding="utf-8",
    )
    return target
