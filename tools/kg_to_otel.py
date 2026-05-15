#!/usr/bin/env python3
"""Export Agent KG workflow records as OpenTelemetry JSON trace spans."""

import argparse
import calendar
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Tuple


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_KG_PATH = ROOT_DIR / "config" / "agent_kg.json"
DEFAULT_OUT_PATH = ROOT_DIR / "output" / "otel_traces.json"
SERVICE_NAME = "helicomponents-multi-agent"
SERVICE_VERSION = "0.1.0"
SCOPE_NAME = "agent_kg_exporter"
SCOPE_VERSION = "1.0"
MAX_OUTPUT_BYTES = 10 * 1024 * 1024


Projection = Dict[str, List[Dict[str, Any]]]


def _empty_projection() -> Projection:
    return {"workflows": [], "handoffs": [], "contexts": []}


def _load_active(path: Path) -> Projection:
    with path.open("r", encoding="utf-8") as handle:
        raw = json.load(handle)
    if not isinstance(raw, dict):
        raise ValueError(f"expected JSON object in {path}")

    data = _empty_projection()
    for key in data:
        values = raw.get(key, [])
        if not isinstance(values, list):
            raise ValueError(f"expected list at {key} in {path}")
        data[key].extend(item for item in values if isinstance(item, dict))
    return data


def _archive_paths(kg_path: Path) -> List[Path]:
    config_dir = kg_path.resolve().parent
    paths = list(config_dir.glob("agent_kg_archive*.jsonl"))
    archive_dir = config_dir / "agent_kg_archive"
    if archive_dir.exists():
        paths.extend(archive_dir.glob("**/*.jsonl"))
    return sorted(set(paths))


def _synthetic_id(prefix: str, item: Dict[str, Any]) -> str:
    raw = json.dumps(item, ensure_ascii=False, sort_keys=True)
    return f"{prefix}_{hashlib.sha256(raw.encode('utf-8')).hexdigest()[:16]}"


def _load_archive(kg_path: Path) -> Projection:
    data = _empty_projection()
    for jsonl_path in _archive_paths(kg_path):
        with jsonl_path.open("r", encoding="utf-8") as handle:
            for line_number, raw_line in enumerate(handle, start=1):
                if not raw_line.strip():
                    continue
                record = json.loads(raw_line)
                if not isinstance(record, dict):
                    raise ValueError(f"expected JSON object in {jsonl_path}:{line_number}")
                record_type = record.get("type")
                item = {k: v for k, v in record.items() if k != "type"}
                if record_type == "workflow":
                    data["workflows"].append(item)
                elif record_type == "handoff":
                    item.setdefault("handoff_id", _synthetic_id("archive_handoff", item))
                    data["handoffs"].append(item)
                elif record_type == "context":
                    item.setdefault("context_id", _synthetic_id("archive_context", item))
                    data["contexts"].append(item)
    return data


def _merge(base: Projection, extra: Projection) -> Projection:
    merged = _empty_projection()
    keys = {
        "workflows": "workflow_id",
        "handoffs": "handoff_id",
        "contexts": "context_id",
    }
    for bucket, id_key in keys.items():
        seen = set()
        for item in base[bucket] + extra[bucket]:
            item_id = item.get(id_key)
            if not item_id or item_id in seen:
                continue
            seen.add(item_id)
            merged[bucket].append(item)
    return merged


def _trace_id(workflow_id: str) -> str:
    return hashlib.sha256(workflow_id.encode("utf-8")).hexdigest()[:32]


def _span_id(seed: str) -> str:
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()[:16]


def _parse_time(value: Any, label: str) -> datetime:
    if not value:
        raise ValueError(f"missing timestamp for {label}")
    text = str(value)
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _unix_nano(value: Any, label: str) -> int:
    parsed = _parse_time(value, label)
    seconds = calendar.timegm(parsed.utctimetuple())
    return seconds * 1_000_000_000 + parsed.microsecond * 1_000


def _attr_value(value: Any) -> Dict[str, Any]:
    if isinstance(value, bool):
        return {"boolValue": value}
    if isinstance(value, int):
        return {"intValue": str(value)}
    return {"stringValue": _to_text(value)}


def _attributes(items: Iterable[Tuple[str, Any]]) -> List[Dict[str, Any]]:
    attrs = []
    for key, value in items:
        if value is None or value == "":
            continue
        attrs.append({"key": key, "value": _attr_value(value)})
    return attrs


def _truncate(value: Any, limit: int) -> str:
    text = _to_text(value)
    if len(text) <= limit:
        return text
    return text[:limit]


def _to_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value)


def _last_handoff(workflow_id: str, handoffs: List[Dict[str, Any]]) -> Dict[str, Any]:
    relevant = [item for item in handoffs if item.get("workflow_id") == workflow_id]
    if not relevant:
        return {}
    return sorted(relevant, key=lambda item: str(item.get("created_at") or ""))[-1]


def _context_events(workflow_id: str, contexts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    events = []
    for context in contexts:
        if context.get("workflow_id") != workflow_id:
            continue
        timestamp = context.get("created_at") or context.get("updated_at")
        events.append(
            {
                "timeUnixNano": _unix_nano(timestamp, f"context {context.get('context_id')}"),
                "name": f"context_{context.get('context_type', 'unknown')}",
                "attributes": _attributes(
                    (
                        ("agent.context_id", context.get("context_id")),
                        ("agent.context_type", context.get("context_type")),
                        ("agent.name", context.get("agent")),
                        ("agent.content", _truncate(context.get("content"), 500)),
                    )
                ),
            }
        )
    return events


def _workflow_span(
    workflow: Dict[str, Any],
    handoffs: List[Dict[str, Any]],
    contexts: List[Dict[str, Any]],
) -> Dict[str, Any]:
    workflow_id = str(workflow.get("workflow_id") or "")
    if not workflow_id:
        raise ValueError("workflow without workflow_id")
    trace_id = _trace_id(workflow_id)
    span_id = _span_id(f"{trace_id}:workflow_root")
    last_handoff = _last_handoff(workflow_id, handoffs)
    start = workflow.get("created_at")
    end = workflow.get("updated_at") or workflow.get("closed_at") or last_handoff.get("created_at")
    end = end or start
    caps = workflow.get("caps") if isinstance(workflow.get("caps"), dict) else {}
    usage = workflow.get("usage") if isinstance(workflow.get("usage"), dict) else {}

    return {
        "traceId": trace_id,
        "spanId": span_id,
        "name": f"workflow_{workflow_id}",
        "kind": "SPAN_KIND_INTERNAL",
        "startTimeUnixNano": _unix_nano(start, f"workflow {workflow_id} start"),
        "endTimeUnixNano": _unix_nano(end, f"workflow {workflow_id} end"),
        "attributes": _attributes(
            (
                ("agent.workflow_id", workflow_id),
                ("agent.goal", _truncate(workflow.get("goal"), 500)),
                ("agent.owner", workflow.get("owner")),
                ("agent.phase", workflow.get("phase")),
                ("agent.status", workflow.get("status")),
                ("agent.caps.max_steps", caps.get("max_steps")),
                ("agent.caps.max_tokens", caps.get("max_tokens")),
                ("agent.usage.cumulative_steps", usage.get("cumulative_steps")),
                ("agent.usage.cumulative_tokens", usage.get("cumulative_tokens")),
            )
        ),
        "events": _context_events(workflow_id, contexts),
        "status": {"code": "STATUS_CODE_OK"},
    }


def _handoff_span(handoff: Dict[str, Any], root_span_id: str) -> Dict[str, Any]:
    workflow_id = str(handoff.get("workflow_id") or "")
    handoff_id = str(handoff.get("handoff_id") or "")
    if not workflow_id or not handoff_id:
        raise ValueError("handoff without workflow_id or handoff_id")
    timestamp = handoff.get("created_at")
    usage = handoff.get("usage") if isinstance(handoff.get("usage"), dict) else {}
    agent = handoff.get("agent") or "unknown"
    step = handoff.get("plan_step_id") or "unknown"

    return {
        "traceId": _trace_id(workflow_id),
        "spanId": _span_id(handoff_id),
        "parentSpanId": root_span_id,
        "name": f"handoff_{agent}_{step}",
        "kind": "SPAN_KIND_INTERNAL",
        "startTimeUnixNano": _unix_nano(timestamp, f"handoff {handoff_id} start"),
        "endTimeUnixNano": _unix_nano(timestamp, f"handoff {handoff_id} end"),
        "attributes": _attributes(
            (
                ("agent.name", handoff.get("agent")),
                ("agent.risk_tier", handoff.get("risk_tier")),
                ("agent.user_goal", _truncate(handoff.get("user_goal"), 500)),
                ("agent.changes", _truncate(handoff.get("changes"), 500)),
                ("agent.usage.model", usage.get("model")),
                ("agent.usage.est_tokens", usage.get("est_tokens")),
                ("agent.usage.source", usage.get("source")),
                ("agent.trace_id", handoff.get("trace_id")),
                ("agent.plan_step_id", handoff.get("plan_step_id")),
                ("agent.next_owner", handoff.get("next_owner")),
            )
        ),
        "status": {"code": "STATUS_CODE_OK"},
    }


def _project(data: Projection, workflow_filter: str) -> Tuple[Dict[str, Any], int, int, int]:
    workflows = data["workflows"]
    if workflow_filter:
        workflows = [item for item in workflows if item.get("workflow_id") == workflow_filter]
    if workflow_filter and not workflows:
        raise ValueError(f"workflow not found: {workflow_filter}")

    spans = []
    handoff_count = 0
    context_count = 0
    for workflow in workflows:
        workflow_id = str(workflow.get("workflow_id") or "")
        wf_handoffs = [item for item in data["handoffs"] if item.get("workflow_id") == workflow_id]
        wf_contexts = [item for item in data["contexts"] if item.get("workflow_id") == workflow_id]
        root_span = _workflow_span(workflow, wf_handoffs, wf_contexts)
        spans.append(root_span)
        root_span_id = root_span["spanId"]
        for handoff in wf_handoffs:
            spans.append(_handoff_span(handoff, root_span_id))
        handoff_count += len(wf_handoffs)
        context_count += len(wf_contexts)

    payload = _resource_spans(spans)
    return payload, len(workflows), handoff_count, context_count


def _resource_spans(spans: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "resourceSpans": [
            {
                "resource": {
                    "attributes": _attributes(
                        (
                            ("service.name", SERVICE_NAME),
                            ("service.version", SERVICE_VERSION),
                        )
                    )
                },
                "scopeSpans": [
                    {
                        "scope": {"name": SCOPE_NAME, "version": SCOPE_VERSION},
                        "spans": list(spans),
                    }
                ],
            }
        ]
    }


def _payload_spans(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    try:
        return payload["resourceSpans"][0]["scopeSpans"][0]["spans"]
    except (KeyError, IndexError, TypeError):
        return []


def _encoded_size(payload: Dict[str, Any], pretty: bool) -> int:
    text = json.dumps(payload, ensure_ascii=False, indent=2 if pretty else None)
    return len((text + "\n").encode("utf-8"))


def _chunk_path(path: Path, index: int) -> Path:
    return path.with_name(f"{path.stem}.{index:03d}{path.suffix}")


def _write_json(path: Path, payload: Dict[str, Any], pretty: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2 if pretty else None)
        handle.write("\n")


def _write_output(path: Path, payload: Dict[str, Any], pretty: bool) -> List[Path]:
    if _encoded_size(payload, pretty) <= MAX_OUTPUT_BYTES:
        _write_json(path, payload, pretty)
        return [path]

    spans = _payload_spans(payload)
    written: List[Path] = []
    current: List[Dict[str, Any]] = []
    for span in spans:
        candidate = current + [span]
        if current and _encoded_size(_resource_spans(candidate), pretty) > MAX_OUTPUT_BYTES:
            chunk_path = _chunk_path(path, len(written) + 1)
            _write_json(chunk_path, _resource_spans(current), pretty)
            written.append(chunk_path)
            current = [span]
        else:
            current = candidate

    if current:
        chunk_path = _chunk_path(path, len(written) + 1)
        _write_json(chunk_path, _resource_spans(current), pretty)
        written.append(chunk_path)
    return written


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        "--kg",
        dest="input",
        type=Path,
        default=DEFAULT_KG_PATH,
        help="Path to Agent KG JSON",
    )
    parser.add_argument(
        "--output",
        "--out",
        dest="output",
        type=Path,
        default=DEFAULT_OUT_PATH,
        help="Output OTel JSON path",
    )
    parser.add_argument("--workflow-id", default="", help="Optional workflow_id filter")
    parser.add_argument("--include-archive", action="store_true", help="Also read Agent KG JSONL archives")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    data = _load_active(args.input)
    if args.include_archive:
        data = _merge(data, _load_archive(args.input))

    payload, workflows, handoffs, contexts = _project(data, args.workflow_id)
    written = _write_output(args.output, payload, args.pretty)
    target = ", ".join(str(path) for path in written)
    print(f"Exported {workflows} workflows, {handoffs} handoffs, {contexts} context events -> {target}")


if __name__ == "__main__":
    main()
