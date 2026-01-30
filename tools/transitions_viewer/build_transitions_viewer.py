#!/usr/bin/env python3
import json
import os
from datetime import datetime, timezone


ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
INPUT_TRANSITIONS_JSON = os.path.join(
    ROOT_DIR, "config", "transitions", "transitions_rules.json"
)
INPUT_QUOTA_JSON = os.path.join(ROOT_DIR, "config", "transitions", "quota_rules.json")
OUTPUT_HTML = os.path.join(ROOT_DIR, "tools", "transitions_viewer", "index.html")


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
    if owner:
        html.append(
            f'<summary><span class="badge">rule</span><span class="rule-owner">{owner}</span></summary>'
        )
    else:
        html.append(
            f'<summary><span class="badge">rule</span><span class="rule-id">{rule_id}</span></summary>'
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


def build_html(transitions_data: dict, quota_data: dict) -> str:
    embedded = json.dumps(
        {"transitions": transitions_data, "quota": quota_data}, ensure_ascii=False
    )

    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    html = []
    html.append("<!doctype html>")
    html.append('<html lang="en">')
    html.append("<head>")
    html.append('  <meta charset="utf-8" />')
    html.append('  <meta name="viewport" content="width=device-width, initial-scale=1" />')
    html.append("  <title>Transitions Viewer</title>")
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
    html.append("    .rule-owner { color: #666; font-size: 11px; }")
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
    html.append("  <h1>Transitions Viewer</h1>")
    html.append(
        "  <div class=\"meta\">Generated from "
        "config/transitions/transitions_rules.json and quota_rules.json at "
        f"{generated_at}</div>"
    )
    html.append('  <div class="meta">Run: python3 tools/transitions_viewer/build_transitions_viewer.py</div>')
    html.append("  <div id=\"app\"></div>")
    html.append("  <script>")
    html.append(f"    const TRANSITIONS_DATA = {embedded};")
    html.append("    function escapeHtml(text) {")
    html.append("      const div = document.createElement('div');")
    html.append("      div.textContent = text ?? '';")
    html.append("      return div.innerHTML;")
    html.append("    }")
    html.append("    function formatCondition(cond) {")
    html.append("      if (!cond) return '';")
    html.append("      if (cond.all) return cond.all.map(c => escapeHtml(c.expr)).join('\\nAND\\n');")
    html.append("      if (cond.any) return cond.any.map(c => escapeHtml(c.expr)).join('\\nOR\\n');")
    html.append("      if (cond.expr) return escapeHtml(cond.expr);")
    html.append("      return '';")
    html.append("    }")
    html.append("    function renderRuleCard(rule, targetLabel) {")
    html.append("      let html = '<details class=\"rule-card\">';")
    html.append("      const owner = rule.owner_module ? escapeHtml(rule.owner_module) : '';")
    html.append("      html += owner")
    html.append("        ? `<summary><span class=\\\"badge\\\">rule</span><span class=\\\"rule-owner\\\">${owner}</span></summary>`")
    html.append("        : `<summary><span class=\\\"badge\\\">rule</span><span class=\\\"rule-id\\\">${escapeHtml(rule.id)}</span></summary>`;")
    html.append("      html += '<div class=\"rule-body\">';")
    html.append("      if (rule.pre) html += `<div class=\"rule-section\"><div class=\"label\">pre</div><div class=\"value pre\">${formatCondition(rule.pre)}</div></div>`;")
    html.append("      if (rule.post) html += `<div class=\"rule-section\"><div class=\"label\">post</div><div class=\"value post\">${formatCondition(rule.post)}</div></div>`;")
    html.append("      if (rule.effects) html += `<div class=\\\"rule-section\\\"><div class=\\\"label\\\">effects</div><div class=\\\"value\\\">${escapeHtml(JSON.stringify(rule.effects))}</div></div>`;")
    html.append("      if (rule.notes) html += `<div class=\"notes\">${escapeHtml(rule.notes)}</div>`;")
    html.append("      html += '</div></details>';")
    html.append("      return html;")
    html.append("    }")
    html.append("    function getIds(mapObj, rules, key) {")
    html.append("      const ids = Object.keys(mapObj || {}).map(Number).filter(n => !Number.isNaN(n));")
    html.append("      if (ids.length) return ids.sort((a, b) => a - b);")
    html.append("      const vals = (rules || []).map(r => r[key]);")
    html.append("      return Array.from(new Set(vals)).filter(n => n !== undefined).sort((a, b) => a - b);")
    html.append("    }")
    html.append("    function renderLegend(title, mapObj) {")
    html.append("      const ids = Object.keys(mapObj || {}).map(Number).filter(n => !Number.isNaN(n)).sort((a, b) => a - b);")
    html.append("      if (!ids.length) return '';")
    html.append("      let html = `<div class=\\\"section\\\"><h2>${escapeHtml(title)}</h2><div class=\\\"states\\\">`;")
    html.append("      ids.forEach(id => {")
    html.append("        const name = mapObj?.[String(id)] || `${title.toLowerCase()}_${id}`;")
    html.append("        html += `<div class=\\\"state-badge\\\">${id}: ${escapeHtml(name)}</div>`;")
    html.append("      });")
    html.append("      html += '</div></div>';")
    html.append("      return html;")
    html.append("    }")
    html.append("    function renderMatrixBlock(data, title) {")
    html.append("      const app = document.getElementById('app');")
    html.append("      const matrix = data.matrix || { from: 'state', to: 'state' };")
    html.append("      const fromMap = matrix.from === 'intent' ? (data.intents || {}) : (data.states || {});")
    html.append("      const toMap = matrix.to === 'intent' ? (data.intents || {}) : (data.states || {});")
    html.append("      const fromIds = getIds(fromMap, data.rules, 'from');")
    html.append("      const toIds = getIds(toMap, data.rules, 'to');")
    html.append("      let html = '';")
    html.append("      html += `<div class=\\\"section\\\"><h2>${escapeHtml(title)}</h2></div>`;")
    html.append("      html += renderLegend('States', data.states);")
    html.append("      if (data.intents) html += renderLegend('Intents', data.intents);")
    html.append("      if (data.derived?.length) {")
    html.append("        html += '<div class=\\\"section\\\"><h2>Derived</h2>';")
    html.append("        data.derived.forEach(item => {")
    html.append("          html += `<div>${escapeHtml(item.name)} = ${escapeHtml(item.expr)}</div>`;")
    html.append("        });")
    html.append("        html += '</div>';")
    html.append("      }")
    html.append("      html += `<div class=\\\"section\\\"><h2>Matrix (${escapeHtml(matrix.from)} → ${escapeHtml(matrix.to)})</h2><table class=\\\"matrix\\\">`;")
    html.append("      html += `<thead><tr><th>${escapeHtml(matrix.from)} \\\\ ${escapeHtml(matrix.to)}</th>`;")
    html.append("      toIds.forEach(id => {")
    html.append("        const name = toMap?.[String(id)] || `${matrix.to}_${id}`;")
    html.append("        html += `<th>${id}<br><small>${escapeHtml(name)}</small></th>`;")
    html.append("      });")
    html.append("      html += '</tr></thead><tbody>';")
    html.append("      fromIds.forEach(fromId => {")
    html.append("        const fromName = fromMap?.[String(fromId)] || `${matrix.from}_${fromId}`;")
    html.append("        html += `<tr><th class=\\\"from-header\\\">${fromId}<br><small>${escapeHtml(fromName)}</small></th>`;")
    html.append("        toIds.forEach(toId => {")
    html.append("          const cellRules = (data.rules || []).filter(r => r.from === fromId && r.to === toId);")
    html.append("          html += '<td>';")
    html.append("          if (!cellRules.length) {")
    html.append("            html += '<div class=\\\"empty\\\">—</div>';")
    html.append("          } else {")
    html.append("            cellRules.forEach(rule => {")
    html.append("              html += renderRuleCard(rule);")
    html.append("            });")
    html.append("          }")
    html.append("          html += '</td>';")
    html.append("        });")
    html.append("        html += '</tr>';")
    html.append("      });")
    html.append("      html += '</tbody></table></div>';")
    html.append("      if ((data.rules || []).length === 0) {")
    html.append("        html = '<div class=\\\"section\\\"><div class=\\\"notes\\\">Rules list is empty.</div></div>' + html;")
    html.append("      }")
    html.append("      app.innerHTML += html;")
    html.append("    }")
    html.append("    function renderQuotaSection(title, items) {")
    html.append("      if (!items || !items.length) return '';")
    html.append("      let html = `<div class=\\\"section\\\"><h2>${escapeHtml(title)}</h2>`;")
    html.append("      items.forEach(item => {")
    html.append("        html += '<details class=\\\"rule-card\\\">';")
    html.append("        const owner = item.owner_module ? escapeHtml(item.owner_module) : '';")
    html.append("        const id = item.id ? escapeHtml(item.id) : 'item';")
    html.append("        html += owner")
    html.append("          ? `<summary><span class=\\\"badge\\\">rule</span><span class=\\\"rule-owner\\\">${owner}</span></summary>`")
    html.append("          : `<summary><span class=\\\"badge\\\">rule</span><span class=\\\"rule-id\\\">${id}</span></summary>`;")
    html.append("        html += '<div class=\\\"rule-body\\\">';")
    html.append("        if (item.expr) html += `<div class=\\\"rule-section\\\"><div class=\\\"label\\\">expr</div><div class=\\\"value pre\\\">${escapeHtml(item.expr)}</div></div>`;")
    html.append("        if (item.notes) html += `<div class=\\\"notes\\\">${escapeHtml(item.notes)}</div>`;")
    html.append("        html += '</div></details>';")
    html.append("      });")
    html.append("      html += '</div>';")
    html.append("      return html;")
    html.append("    }")
    html.append("    function renderMessageBucket(bucket, title) {")
    html.append("      if (!bucket) return '';")
    html.append("      let html = `<div class=\\\"section\\\"><h2>${escapeHtml(title)}</h2>`;")
    html.append("      html += `<div><strong>keys</strong>: ${escapeHtml((bucket.keys || []).join(', '))}</div>`;")
    html.append("      html += `<div><strong>payload</strong>: ${escapeHtml((bucket.payload || []).join(', '))}</div>`;")
    html.append("      if (bucket.notes) html += `<div class=\\\"notes\\\">${escapeHtml(bucket.notes)}</div>`;")
    html.append("      html += '</div>';")
    html.append("      return html;")
    html.append("    }")
    html.append("    function renderQuotaBlock(quota) {")
    html.append("      let html = '<div class=\\\"section\\\"><h2>Quota Logic</h2></div>';")
    html.append("      html += renderQuotaSection('Quota Flow', quota.quota_flow || []);")
    html.append("      html += renderQuotaSection('Selection Rules', quota.selection_rules || []);")
    html.append("      html += renderQuotaSection('RepairLine Rules', quota.repair_line_rules || []);")
    html.append("      html += renderQuotaSection('Spawn Rules', quota.spawn_rules || []);")
    html.append("      if (quota.message_bucket) {")
    html.append("        html += renderMessageBucket(quota.message_bucket.quota_bucket, 'MessageBucket: Quota');")
    html.append("        html += renderMessageBucket(quota.message_bucket.spawn_bucket, 'MessageBucket: Spawn');")
    html.append("      }")
    html.append("      return html;")
    html.append("    }")
    html.append("    function renderViewer(data) {")
    html.append("      const app = document.getElementById('app');")
    html.append("      app.innerHTML = '';")
    html.append("      renderMatrixBlock(data.transitions, 'Transitions (state → state)');")
    html.append("      app.innerHTML += renderQuotaBlock(data.quota || {});")
    html.append("    }")
    html.append("    renderViewer(TRANSITIONS_DATA);")
    html.append("  </script>")
    html.append("</body>")
    html.append("</html>")

    return "\n".join(html)


def main() -> None:
    with open(INPUT_TRANSITIONS_JSON, "r", encoding="utf-8") as f:
        transitions_data = json.load(f)
    with open(INPUT_QUOTA_JSON, "r", encoding="utf-8") as f:
        quota_data = json.load(f)
    html = build_html(transitions_data, quota_data)
    os.makedirs(os.path.dirname(OUTPUT_HTML), exist_ok=True)
    with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Wrote {OUTPUT_HTML}")


if __name__ == "__main__":
    main()
