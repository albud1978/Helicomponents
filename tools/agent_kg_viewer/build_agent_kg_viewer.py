#!/usr/bin/env python3
"""
Генератор HTML-визуализации Agent KG (config/agent_kg.json).

Строит статический HTML с:
- Текущими workflow и их состояниями
- Timeline handoff'ов
- Контекстами

Использование:
    python tools/agent_kg_viewer/build_agent_kg_viewer.py
"""

import json
import os
from datetime import datetime, timezone


ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
INPUT_JSON = os.path.join(ROOT_DIR, "config", "agent_kg.json")
OUTPUT_HTML = os.path.join(ROOT_DIR, "tools", "agent_kg_viewer", "index.html")


def escape_html(text: str) -> str:
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#x27;")
    )


def build_html(data: dict) -> str:
    embedded = json.dumps(data, ensure_ascii=False)
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    html = []
    html.append("<!doctype html>")
    html.append('<html lang="ru">')
    html.append("<head>")
    html.append('  <meta charset="utf-8" />')
    html.append('  <meta name="viewport" content="width=device-width, initial-scale=1" />')
    html.append("  <title>Agent KG Viewer</title>")
    html.append("  <style>")
    html.append("""
    :root { color-scheme: light dark; }
    body { font-family: Arial, Helvetica, sans-serif; margin: 24px; max-width: 1200px; }
    h1, h2, h3 { margin: 0 0 8px; }
    .meta { color: #666; font-size: 12px; margin-bottom: 16px; }
    .section { margin-bottom: 24px; }
    .empty { color: #888; font-style: italic; }

    /* Workflow cards */
    .workflow-card {
      border: 2px solid #2a5bd7; border-radius: 8px; padding: 12px;
      margin-bottom: 12px; background: #f8f9ff;
    }
    .workflow-header { display: flex; gap: 8px; align-items: center; margin-bottom: 8px; }
    .badge {
      display: inline-block; padding: 2px 8px; border-radius: 10px;
      font-size: 11px; font-weight: 600; color: white;
    }
    .badge-active { background: #2a9d2a; }
    .badge-completed { background: #666; }
    .badge-phase { background: #2a5bd7; }
    .badge-owner { background: #d7832a; }
    .workflow-goal { font-size: 14px; margin: 4px 0; }
    .workflow-meta { font-size: 11px; color: #666; }

    /* Handoff timeline */
    .timeline { position: relative; padding-left: 24px; }
    .timeline::before {
      content: ''; position: absolute; left: 8px; top: 0; bottom: 0;
      width: 2px; background: #ccc;
    }
    .handoff-card {
      position: relative; border: 1px solid #ddd; border-radius: 6px;
      padding: 10px; margin-bottom: 10px; background: #fafafa;
    }
    .handoff-card::before {
      content: ''; position: absolute; left: -20px; top: 14px;
      width: 10px; height: 10px; border-radius: 50%; background: #2a5bd7;
    }
    .handoff-header { display: flex; gap: 6px; align-items: center; flex-wrap: wrap; }
    .handoff-agent { font-weight: 600; font-size: 13px; }
    .handoff-body { margin-top: 6px; font-size: 12px; }
    .handoff-field { margin: 2px 0; }
    .field-label { font-weight: 600; color: #444; text-transform: uppercase; font-size: 10px; }
    .field-value { white-space: pre-wrap; }

    /* Context cards */
    .context-card {
      border: 1px solid #ddd; border-radius: 6px;
      padding: 10px; margin-bottom: 10px; background: #fffef5;
    }
    .context-type { font-weight: 600; font-size: 13px; color: #8a6d00; }
    .context-content { font-size: 12px; white-space: pre-wrap; margin-top: 4px; max-height: 200px; overflow-y: auto; }
    """)
    html.append("  </style>")
    html.append("</head>")
    html.append("<body>")
    html.append("  <h1>Agent KG Viewer</h1>")
    html.append(f'  <div class="meta">Generated from config/agent_kg.json at {generated_at}</div>')
    html.append('  <div class="meta">Run: python3 tools/agent_kg_viewer/build_agent_kg_viewer.py</div>')
    html.append('  <div id="app"></div>')
    html.append("  <script>")
    html.append(f"    const KG_DATA = {embedded};")
    html.append("""
    function esc(text) {
      const div = document.createElement('div');
      div.textContent = text ?? '';
      return div.innerHTML;
    }

    function renderWorkflows(workflows) {
      if (!workflows || !workflows.length) return '<div class="empty">Нет активных workflow</div>';
      return workflows.map(w => {
        const statusClass = w.status === 'active' ? 'badge-active' : 'badge-completed';
        return `
          <div class="workflow-card">
            <div class="workflow-header">
              <span class="badge ${statusClass}">${esc(w.status)}</span>
              <span class="badge badge-phase">${esc(w.phase)}</span>
              <span class="badge badge-owner">${esc(w.owner)}</span>
              <strong>${esc(w.workflow_id)}</strong>
            </div>
            <div class="workflow-goal">${esc(w.goal)}</div>
            <div class="workflow-meta">Created: ${esc(w.created_at || '?')} | Updated: ${esc(w.updated_at || '?')}</div>
          </div>`;
      }).join('');
    }

    function renderHandoffs(handoffs, workflowId) {
      const filtered = workflowId
        ? handoffs.filter(h => h.workflow_id === workflowId)
        : handoffs;
      const sorted = [...filtered].sort((a, b) => (b.created_at || '').localeCompare(a.created_at || ''));
      if (!sorted.length) return '<div class="empty">Нет handoff\\'ов</div>';
      return '<div class="timeline">' + sorted.map(h => `
        <div class="handoff-card">
          <div class="handoff-header">
            <span class="handoff-agent">${esc(h.agent)}</span>
            <span class="badge badge-phase">${esc(h.workflow_id)}</span>
            <span style="font-size:11px;color:#666">${esc(h.created_at || '')}</span>
          </div>
          <div class="handoff-body">
            <div class="handoff-field"><span class="field-label">Goal:</span> <span class="field-value">${esc(h.goal)}</span></div>
            <div class="handoff-field"><span class="field-label">Changes:</span> <span class="field-value">${esc(h.changes)}</span></div>
            <div class="handoff-field"><span class="field-label">Evidence:</span> <span class="field-value">${esc(h.evidence)}</span></div>
            <div class="handoff-field"><span class="field-label">Risks:</span> <span class="field-value">${esc(h.risks)}</span></div>
            <div class="handoff-field"><span class="field-label">Next:</span> <span class="field-value">${esc(h.next_owner)}</span></div>
            ${h.open_questions ? `<div class="handoff-field"><span class="field-label">Questions:</span> <span class="field-value">${esc(h.open_questions)}</span></div>` : ''}
          </div>
        </div>`).join('') + '</div>';
    }

    function renderContexts(contexts, workflowId) {
      const filtered = workflowId
        ? contexts.filter(c => c.workflow_id === workflowId)
        : contexts;
      const sorted = [...filtered].sort((a, b) => (b.updated_at || '').localeCompare(a.updated_at || ''));
      if (!sorted.length) return '<div class="empty">Нет контекстов</div>';
      return sorted.map(c => `
        <div class="context-card">
          <div style="display:flex;gap:8px;align-items:center">
            <span class="context-type">${esc(c.context_type)}</span>
            <span style="font-size:11px;color:#666">${esc(c.agent || '?')} | ${esc(c.updated_at || '')}</span>
          </div>
          <div class="context-content">${esc((c.content || '').substring(0, 1000))}${(c.content || '').length > 1000 ? '...' : ''}</div>
        </div>`).join('');
    }

    function render(data) {
      const app = document.getElementById('app');
      let html = '';

      html += '<div class="section"><h2>Workflows</h2>';
      html += renderWorkflows(data.workflows || []);
      html += '</div>';

      html += '<div class="section"><h2>Handoffs</h2>';
      html += renderHandoffs(data.handoffs || []);
      html += '</div>';

      html += '<div class="section"><h2>Contexts</h2>';
      html += renderContexts(data.contexts || []);
      html += '</div>';

      app.innerHTML = html;
    }

    render(KG_DATA);
    """)
    html.append("  </script>")
    html.append("</body>")
    html.append("</html>")

    return "\n".join(html)


def main() -> None:
    if not os.path.exists(INPUT_JSON):
        print(f"Input not found: {INPUT_JSON}")
        raise SystemExit(1)

    with open(INPUT_JSON, "r", encoding="utf-8") as f:
        data = json.load(f)

    result = build_html(data)
    os.makedirs(os.path.dirname(OUTPUT_HTML), exist_ok=True)
    with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
        f.write(result)
    print(f"Wrote {OUTPUT_HTML}")


if __name__ == "__main__":
    main()
