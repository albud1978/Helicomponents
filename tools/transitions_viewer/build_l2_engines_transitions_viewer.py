#!/usr/bin/env python3
import json
import os
from datetime import datetime, timezone


ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
INPUT_TRANSITIONS_JSON = os.path.join(
    ROOT_DIR, "config", "transitions", "transitions_rules_l2_engines.json"
)
OUTPUT_HTML = os.path.join(
    ROOT_DIR, "tools", "transitions_viewer", "l2_engines", "index.html"
)


def escape_html(text: str) -> str:
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#x27;")
    )


def format_condition(cond: dict) -> str:
    if not cond:
        return ""
    if "all" in cond:
        parts = [escape_html(c.get("expr", "")) for c in cond.get("all", [])]
        return "\nAND\n".join(parts)
    if "any" in cond:
        parts = [escape_html(c.get("expr", "")) for c in cond.get("any", [])]
        return "\nOR\n".join(parts)
    if "expr" in cond:
        return escape_html(cond.get("expr", ""))
    return ""


def render_driver_tags(tags: list) -> str:
    if not tags:
        return ""
    html = ['<span class="driver-tags">']
    for tag in tags:
        tag_text = escape_html(str(tag))
        tag_class = "driver-tag"
        if tag == "planner-status-driven":
            tag_class += " driver-planner"
        elif tag == "engine-runtime-driven":
            tag_class += " driver-engine"
        html.append(f'<span class="{tag_class}">{tag_text}</span>')
    html.append("</span>")
    return "".join(html)


def render_rule_card(rule: dict) -> str:
    rule_id = escape_html(str(rule.get("id", "")))
    notes = escape_html(str(rule.get("notes", ""))) if rule.get("notes") else ""
    pre_text = format_condition(rule.get("pre"))
    post_text = format_condition(rule.get("post"))
    driver_tags = render_driver_tags(rule.get("driver_tags") or [])

    html = []
    html.append('<details class="rule-card">')
    html.append(
        f'<summary><span class="badge">rule</span><span class="rule-id">{rule_id}</span>{driver_tags}</summary>'
    )
    html.append('<div class="rule-body">')
    if pre_text:
        html.append('<div class="rule-section"><div class="label">pre</div>')
        html.append(f'<div class="value pre">{pre_text}</div></div>')
    if post_text:
        html.append('<div class="rule-section"><div class="label">post</div>')
        html.append(f'<div class="value post">{post_text}</div></div>')
    html.append('<div class="rule-section"><div class="label">effects</div>')
    html.append(f'<div class="value">set state := {rule.get("to")}</div></div>')
    if notes:
        html.append(f'<div class="notes">{notes}</div>')
    html.append("</div>")
    html.append("</details>")
    return "\n".join(html)


def render_metadata(metadata: dict) -> str:
    if not metadata:
        return ""
    html = []
    html.append('<div class="section"><h2>Metadata</h2>')
    html.append('<table class="meta-table">')
    html.append("<thead><tr><th>Key</th><th>Value</th></tr></thead><tbody>")
    for key, value in metadata.items():
        html.append(
            "<tr>"
            f"<td>{escape_html(str(key))}</td>"
            f"<td>{escape_html(str(value))}</td>"
            "</tr>"
        )
    html.append("</tbody></table></div>")
    return "\n".join(html)


def render_states(states: dict) -> str:
    if not states:
        return ""
    items = sorted(states.items(), key=lambda item: int(item[0]))
    html = ['<div class="section"><h2>States</h2><div class="states">']
    for key, value in items:
        html.append(
            f'<div class="state-badge">{escape_html(str(key))}: {escape_html(str(value))}</div>'
        )
    html.append("</div></div>")
    return "\n".join(html)


def render_contract_statuses(statuses: list) -> str:
    if not statuses:
        return ""
    html = [
        '<div class="section" id="contract_statuses"><h2>Contract Statuses</h2><div class="states">'
    ]
    for status in statuses:
        html.append(f'<div class="status-badge">{escape_html(str(status))}</div>')
    html.append("</div></div>")
    return "\n".join(html)


def render_variables(variables: list) -> str:
    if not variables:
        return ""
    html = ['<div class="section"><h2>Variables</h2><div class="states">']
    for name in variables:
        html.append(f'<div class="var-badge">{escape_html(str(name))}</div>')
    html.append("</div></div>")
    return "\n".join(html)


def render_runtime_compatibility(items: list) -> str:
    if not items:
        return ""
    html = ['<div class="section"><h2>Runtime Compatibility</h2>']
    for item in items:
        item_id = escape_html(str(item.get("id", "")))
        note = escape_html(str(item.get("note", "")))
        html.append('<div class="list-item">')
        html.append(f'<div class="list-id">{item_id}</div>')
        if note:
            html.append(f'<div class="list-text">{note}</div>')
        html.append("</div>")
    html.append("</div>")
    return "\n".join(html)


def render_invariant_principles(items: list) -> str:
    if not items:
        return ""
    html = ['<div class="section"><h2>Invariant Principles</h2>']
    for item in items:
        item_id = escape_html(str(item.get("id", "")))
        claim = escape_html(str(item.get("claim", "")))
        html.append('<div class="list-item">')
        html.append(f'<div class="list-id">{item_id}</div>')
        html.append(f'<div class="list-text">{claim}</div>')
        html.append("</div>")
    html.append("</div>")
    return "\n".join(html)


def render_location_contract(items: list) -> str:
    if not items:
        return ""
    html = ['<div class="section"><h2>Location Contract</h2>']
    for item in items:
        item_id = escape_html(str(item.get("id", "")))
        rule = escape_html(str(item.get("rule", "")))
        exception = escape_html(str(item.get("exception", ""))) if item.get("exception") else ""
        html.append('<div class="list-item">')
        html.append(f'<div class="list-id">{item_id}</div>')
        html.append(f'<div class="list-text"><strong>Rule:</strong> {rule}</div>')
        if exception:
            html.append(
                f'<div class="list-text"><strong>Exception:</strong> {exception}</div>'
            )
        html.append("</div>")
    html.append("</div>")
    return "\n".join(html)


def render_matrix(transitions_data: dict) -> str:
    rules = transitions_data.get("rules") or []
    matrix = transitions_data.get("matrix") or {"from": "state", "to": "state"}
    states = transitions_data.get("states") or {}

    from_ids = sorted([int(k) for k in states.keys()])
    to_ids = sorted([int(k) for k in states.keys()])

    html = []
    html.append(
        f'<div class="section"><h2>Matrix ({escape_html(matrix.get("from", "state"))} → {escape_html(matrix.get("to", "state"))})</h2>'
    )
    html.append('<table class="matrix">')
    html.append("<thead><tr>")
    html.append(
        f"<th>{escape_html(matrix.get('from', 'state'))} \\ {escape_html(matrix.get('to', 'state'))}</th>"
    )
    for to_id in to_ids:
        name = states.get(str(to_id), f"state_{to_id}")
        html.append(f"<th>{to_id}<br><small>{escape_html(name)}</small></th>")
    html.append("</tr></thead><tbody>")

    for from_id in from_ids:
        from_name = states.get(str(from_id), f"state_{from_id}")
        html.append(
            f'<tr><th class="from-header">{from_id}<br><small>{escape_html(from_name)}</small></th>'
        )
        for to_id in to_ids:
            cell_rules = [
                rule for rule in rules if rule.get("from") == from_id and rule.get("to") == to_id
            ]
            html.append("<td>")
            if not cell_rules:
                html.append('<div class="empty">—</div>')
            else:
                for rule in cell_rules:
                    html.append(render_rule_card(rule))
            html.append("</td>")
        html.append("</tr>")
    html.append("</tbody></table></div>")
    return "\n".join(html)


def render_rtc_execution_order(items: list) -> str:
    if not items:
        return ""
    html = ['<div class="section"><h2>RTC Execution Order</h2>']
    html.append('<table class="rtc-table">')
    html.append(
        "<thead><tr><th>#</th><th>Phase</th><th>Layer</th><th>Function</th><th>State</th><th>Notes</th></tr></thead><tbody>"
    )
    for item in items:
        html.append("<tr>")
        html.append(f"<td>{escape_html(str(item.get('order', '')))}</td>")
        html.append(f"<td>{escape_html(str(item.get('phase', '')))}</td>")
        html.append(f"<td>{escape_html(str(item.get('layer', '')))}</td>")
        html.append(f"<td>{escape_html(str(item.get('function', '')))}</td>")
        html.append(f"<td>{escape_html(str(item.get('state', '')))}</td>")
        html.append(f"<td>{escape_html(str(item.get('notes', '')))}</td>")
        html.append("</tr>")
    html.append("</tbody></table></div>")
    return "\n".join(html)


def build_html(transitions_data: dict) -> str:
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    html = []
    html.append("<!doctype html>")
    html.append('<html lang="en">')
    html.append("<head>")
    html.append('  <meta charset="utf-8" />')
    html.append('  <meta name="viewport" content="width=device-width, initial-scale=1" />')
    html.append("  <title>L2 Engines Transitions Viewer</title>")
    html.append("  <style>")
    html.append("    :root { color-scheme: light dark; }")
    html.append("    body { font-family: Arial, Helvetica, sans-serif; margin: 24px; }")
    html.append("    h1, h2 { margin: 0 0 8px; }")
    html.append("    .meta { color: #666; font-size: 12px; margin-bottom: 16px; }")
    html.append("    .section { margin-bottom: 20px; }")
    html.append("    .states { display: flex; flex-wrap: wrap; gap: 8px; }")
    html.append(
        "    .state-badge { background: #2a5bd7; color: white; padding: 3px 8px; border-radius: 10px; font-size: 12px; }"
    )
    html.append(
        "    .var-badge { background: #2a5bd7; color: white; padding: 3px 8px; border-radius: 10px; font-size: 12px; }"
    )
    html.append(
        "    .status-badge { background: #2a5bd7; color: white; padding: 3px 8px; border-radius: 10px; font-size: 12px; }"
    )
    html.append("    table.matrix { width: 100%; border-collapse: collapse; table-layout: fixed; }")
    html.append("    table.matrix th, table.matrix td { border: 1px solid #ccc; padding: 6px; vertical-align: top; }")
    html.append("    table.matrix th { background: #f3f3f3; }")
    html.append("    .from-header { text-align: left; }")
    html.append("    .empty { color: #888; text-align: center; }")
    html.append("    details.rule-card { margin: 6px 0; border: 1px solid #ddd; border-radius: 6px; padding: 6px; background: #fafafa; }")
    html.append("    summary { cursor: pointer; list-style: none; }")
    html.append("    summary::-webkit-details-marker { display: none; }")
    html.append("    .badge { display: inline-block; padding: 2px 6px; border-radius: 10px; background: #2a5bd7; color: white; font-size: 11px; margin-right: 6px; }")
    html.append("    .rule-id { font-weight: 600; margin-right: 8px; }")
    html.append("    .driver-tags { display: inline-flex; gap: 4px; flex-wrap: wrap; vertical-align: middle; }")
    html.append("    .driver-tag { font-size: 10px; padding: 2px 6px; border-radius: 10px; background: #e0e0e0; border: 1px solid #c8c8c8; color: #222; }")
    html.append("    .driver-tag.driver-planner { background: #f3c18b; border-color: #e2a55f; color: #4a2d00; }")
    html.append("    .driver-tag.driver-engine { background: #b7d7f1; border-color: #8fbde8; color: #0b2b4a; }")
    html.append("    .rule-body { margin-top: 8px; }")
    html.append("    .rule-section { margin-bottom: 6px; }")
    html.append("    .label { font-size: 11px; color: #444; text-transform: uppercase; }")
    html.append("    .value { font-size: 12px; white-space: pre-wrap; }")
    html.append("    .value.pre, .value.post { font-family: monospace; }")
    html.append("    .notes { font-size: 11px; color: #666; }")
    html.append("    table.rtc-table { width: 100%; border-collapse: collapse; font-size: 12px; }")
    html.append("    table.rtc-table th, table.rtc-table td { border: 1px solid #ccc; padding: 6px; vertical-align: top; }")
    html.append("    table.rtc-table th { background: #f3f3f3; }")
    html.append("    table.meta-table { width: 100%; border-collapse: collapse; font-size: 12px; }")
    html.append("    table.meta-table th, table.meta-table td { border: 1px solid #ccc; padding: 6px; vertical-align: top; }")
    html.append("    table.meta-table th { background: #f3f3f3; text-align: left; width: 180px; }")
    html.append("    .list-item { margin-bottom: 10px; }")
    html.append("    .list-id { font-weight: 600; }")
    html.append("    .list-text { font-size: 12px; }")
    html.append("  </style>")
    html.append("</head>")
    html.append("<body>")
    html.append("  <h1>L2 Engines Transitions Viewer</h1>")
    html.append(
        "  <div class=\"meta\">Generated from "
        "config/transitions/transitions_rules_l2_engines.json at "
        f"{generated_at}</div>"
    )
    html.append(
        '  <div class="meta">Run: python3 tools/transitions_viewer/build_l2_engines_transitions_viewer.py</div>'
    )
    html.append(render_metadata(transitions_data.get("metadata") or {}))
    html.append(render_states(transitions_data.get("states") or {}))
    html.append(
        render_contract_statuses(transitions_data.get("contract_statuses") or [])
    )
    html.append(
        render_runtime_compatibility(
            transitions_data.get("runtime_compatibility") or []
        )
    )
    html.append(render_variables(transitions_data.get("variables") or []))
    html.append(render_invariant_principles(transitions_data.get("invariant_principles") or []))
    html.append(render_location_contract(transitions_data.get("location_contract") or []))
    html.append(render_matrix(transitions_data))
    html.append(render_rtc_execution_order(transitions_data.get("rtc_execution_order") or []))
    html.append("</body>")
    html.append("</html>")

    return "\n".join(html)


def main() -> None:
    with open(INPUT_TRANSITIONS_JSON, "r", encoding="utf-8") as f:
        transitions_data = json.load(f)
    html = build_html(transitions_data)
    os.makedirs(os.path.dirname(OUTPUT_HTML), exist_ok=True)
    with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Wrote {OUTPUT_HTML}")


if __name__ == "__main__":
    main()
