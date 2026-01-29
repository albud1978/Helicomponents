#!/usr/bin/env python3
import json
import os
from datetime import datetime, timezone


ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
INPUT_JSON = os.path.join(ROOT_DIR, "config", "transitions", "transitions.json")
OUTPUT_HTML = os.path.join(ROOT_DIR, "output", "transitions_matrix.html")


def escape_html(text: str) -> str:
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#x27;")
    )


def render_rule_card(rule: dict) -> str:
    rule_id = escape_html(str(rule.get("id", "")))
    precedence = rule.get("precedence", "")
    owner = escape_html(str(rule.get("owner_module", ""))) if rule.get("owner_module") else ""
    notes = escape_html(str(rule.get("notes", ""))) if rule.get("notes") else ""

    pre = rule.get("pre")
    post = rule.get("post")

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

    pre_text = format_condition(pre)
    post_text = format_condition(post)

    html = []
    html.append('<details class="rule-card">')
    html.append(
        f'<summary><span class="badge">rule</span><span class="rule-id">{rule_id}</span>'
        f'<span class="rule-prec">prec: {precedence}</span></summary>'
    )
    html.append('<div class="rule-body">')
    html.append('<div class="rule-section"><div class="label">transition</div>')
    html.append(
        f'<div class="value">{rule.get("from")} <span class="arrow">→</span> {rule.get("to")}</div>'
    )
    html.append("</div>")
    if pre_text:
        html.append('<div class="rule-section"><div class="label">pre</div>')
        html.append(f'<div class="value pre">{pre_text}</div></div>')
    if post_text:
        html.append('<div class="rule-section"><div class="label">post</div>')
        html.append(f'<div class="value post">{post_text}</div></div>')
    html.append('<div class="rule-section"><div class="label">effects</div>')
    html.append(f'<div class="value">set state := {rule.get("to")}</div></div>')
    if owner:
        html.append('<div class="rule-section"><div class="label">owner</div>')
        html.append(f'<div class="value owner">{owner}</div></div>')
    if notes:
        html.append(f'<div class="notes">{notes}</div>')
    html.append("</div>")
    html.append("</details>")
    return "\n".join(html)


def build_html(data: dict) -> str:
    states = data.get("states", {})
    rules = data.get("rules", [])
    derived = data.get("derived", [])
    exceptions = data.get("exceptions", [])

    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    html = []
    html.append("<!doctype html>")
    html.append('<html lang="en">')
    html.append("<head>")
    html.append('  <meta charset="utf-8" />')
    html.append('  <meta name="viewport" content="width=device-width, initial-scale=1" />')
    html.append("  <title>Transitions Matrix 7x7</title>")
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
    html.append("    .rule-prec { color: #666; font-size: 11px; }")
    html.append("    .rule-body { margin-top: 8px; }")
    html.append("    .rule-section { margin-bottom: 6px; }")
    html.append("    .label { font-size: 11px; color: #444; text-transform: uppercase; }")
    html.append("    .value { font-size: 12px; white-space: pre-wrap; }")
    html.append("    .value.pre, .value.post { font-family: monospace; }")
    html.append("    .notes { font-size: 11px; color: #666; }")
    html.append("    .arrow { color: #2a5bd7; }")
    html.append("  </style>")
    html.append("</head>")
    html.append("<body>")
    html.append("  <h1>Transitions Matrix 7x7</h1>")
    html.append(f'  <div class="meta">Generated from config/transitions/transitions.json at {generated_at}</div>')
    html.append("  <div class=\"section\">")
    html.append("    <h2>States</h2>")
    html.append("    <div class=\"states\">")
    for i in range(1, 8):
        name = escape_html(str(states.get(str(i), f"state_{i}")))
        html.append(f'      <div class="state-badge">{i}: {name}</div>')
    html.append("    </div>")
    html.append("  </div>")

    if derived:
        html.append("  <div class=\"section\">")
        html.append("    <h2>Derived</h2>")
        for item in derived:
            name = escape_html(str(item.get("name", "")))
            expr = escape_html(str(item.get("expr", "")))
            html.append(f"    <div>{name} = {expr}</div>")
        html.append("  </div>")

    html.append("  <div class=\"section\">")
    html.append("    <h2>Matrix (from → to)</h2>")
    html.append("    <table class=\"matrix\">")
    html.append("      <thead>")
    html.append("        <tr>")
    html.append("          <th>FROM \\ TO</th>")
    for to_state in range(1, 8):
        name = escape_html(str(states.get(str(to_state), f"state_{to_state}")))
        html.append(f"          <th>{to_state}<br><small>{name}</small></th>")
    html.append("        </tr>")
    html.append("      </thead>")
    html.append("      <tbody>")
    for from_state in range(1, 8):
        name = escape_html(str(states.get(str(from_state), f"state_{from_state}")))
        html.append("        <tr>")
        html.append(f'          <th class="from-header">{from_state}<br><small>{name}</small></th>')
        for to_state in range(1, 8):
            cell_rules = [r for r in rules if r.get("from") == from_state and r.get("to") == to_state]
            html.append("          <td>")
            if not cell_rules:
                html.append('            <div class="empty">—</div>')
            else:
                for rule in cell_rules:
                    html.append(render_rule_card(rule))
            html.append("          </td>")
        html.append("        </tr>")
    html.append("      </tbody>")
    html.append("    </table>")
    html.append("  </div>")

    if exceptions:
        html.append("  <div class=\"section\">")
        html.append("    <h2>Exceptions</h2>")
        for ex in exceptions:
            ex_id = escape_html(str(ex.get("id", "")))
            ex_expr = escape_html(str(ex.get("expr", "")))
            ex_notes = escape_html(str(ex.get("notes", ""))) if ex.get("notes") else ""
            html.append(f"    <div><strong>{ex_id}</strong>: {ex_expr}</div>")
            if ex_notes:
                html.append(f"    <div class=\"notes\">{ex_notes}</div>")
        html.append("  </div>")

    html.append("  <script>")
    html.append("    const AUTO_RELOAD_MS = 5000;")
    html.append("    setInterval(() => location.reload(), AUTO_RELOAD_MS);")
    html.append("  </script>")
    html.append("</body>")
    html.append("</html>")

    return "\n".join(html)


def main() -> None:
    with open(INPUT_JSON, "r", encoding="utf-8") as f:
        data = json.load(f)
    html = build_html(data)
    os.makedirs(os.path.dirname(OUTPUT_HTML), exist_ok=True)
    with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Wrote {OUTPUT_HTML}")


if __name__ == "__main__":
    main()
