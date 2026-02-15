#!/usr/bin/env python3
"""
Генератор HTML-визуализации Agent KG (config/agent_kg.json).

Строит статический HTML с:
- текущими workflow и их состояниями
- детализацией handoff (trace/plan/approval/drift)
- секциями WorkflowTrace, GovernanceGates, RLMReuse
- контекстами

Использование:
    python tools/agent_kg_viewer/build_agent_kg_viewer.py
"""

import json
import os
from datetime import datetime, timezone
from html import escape
from typing import Any, Dict


ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
INPUT_JSON = os.path.join(ROOT_DIR, "config", "agent_kg.json")
CAPSULES_JSON = os.path.join(ROOT_DIR, "config", "capsules_manifest.json")
OUTPUT_HTML = os.path.join(ROOT_DIR, "tools", "agent_kg_viewer", "index.html")


def _load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _load_optional_json(path: str, fallback: Dict[str, Any]) -> Dict[str, Any]:
    if not os.path.exists(path):
        return fallback
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return fallback


def _text(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _short(value: Any, max_len: int = 140) -> str:
    raw = " ".join(_text(value).split())
    if not raw:
        return "N/A"
    if len(raw) <= max_len:
        return raw
    return raw[: max_len - 1] + "…"


def _latest_event_for_workflow(
    workflow_id: str, handoffs: list[Dict[str, Any]], contexts: list[Dict[str, Any]]
) -> Dict[str, str]:
    wf_handoffs = sorted(
        (h for h in handoffs if h.get("workflow_id") == workflow_id),
        key=lambda x: _text(x.get("created_at")),
        reverse=True,
    )
    wf_contexts = sorted(
        (c for c in contexts if c.get("workflow_id") == workflow_id),
        key=lambda x: _text(x.get("updated_at") or x.get("created_at")),
        reverse=True,
    )

    last_h = wf_handoffs[0] if wf_handoffs else None
    last_c = wf_contexts[0] if wf_contexts else None
    h_ts = _text(last_h.get("created_at")) if last_h else ""
    c_ts = _text(last_c.get("updated_at") or last_c.get("created_at")) if last_c else ""

    if h_ts and (not c_ts or h_ts >= c_ts):
        return {
            "type": "handoff",
            "at": h_ts,
            "actor": _text(last_h.get("agent")) or "N/A",
            "stage": _text(last_h.get("plan_step_id")) or "N/A",
            "summary": _short(
                _text(last_h.get("changes"))
                or _text(last_h.get("facts"))
                or _text(last_h.get("user_goal"))
                or _text(last_h.get("goal"))
            ),
        }
    if c_ts:
        return {
            "type": "context",
            "at": c_ts,
            "actor": _text(last_c.get("agent")) or "N/A",
            "stage": _text(last_c.get("context_type")) or "N/A",
            "summary": _short(_text(last_c.get("content")), 100),
        }

    return {"type": "N/A", "at": "N/A", "actor": "N/A", "stage": "N/A", "summary": "N/A"}


def _build_server_snapshot(data: Dict[str, Any]) -> str:
    workflows = data.get("workflows", [])
    handoffs = data.get("handoffs", [])
    contexts = data.get("contexts", [])
    if not isinstance(workflows, list):
        workflows = []
    if not isinstance(handoffs, list):
        handoffs = []
    if not isinstance(contexts, list):
        contexts = []

    active = [w for w in workflows if _text(w.get("status")).lower() == "active"]
    active.sort(key=lambda w: _text(w.get("updated_at")), reverse=True)

    block: list[str] = []
    block.append('<div class="section">')
    block.append("<h2>Текущий статус исполнения (server snapshot)</h2>")
    block.append(
        '<div class="section-note">Снимок построен при генерации файла из config/agent_kg.json. '
        "Если JS не исполняется, этот блок остаётся рабочим.</div>"
    )

    if not active:
        block.append('<div class="empty">Активных workflow нет</div>')
    else:
        block.append('<div class="table-wrap"><table><thead><tr>')
        block.append(
            "<th>Workflow</th><th>Phase</th><th>Owner</th><th>LastEvent</th>"
            "<th>Actor</th><th>Stage</th><th>Summary</th>"
        )
        block.append("</tr></thead><tbody>")
        for w in active:
            wf_id = _text(w.get("workflow_id"))
            ev = _latest_event_for_workflow(wf_id, handoffs, contexts)
            event_label = f"{ev['type']} @ {ev['at']}"
            block.append("<tr>")
            block.append(f"<td>{escape(wf_id)}</td>")
            block.append(f"<td>{escape(_text(w.get('phase')) or 'N/A')}</td>")
            block.append(f"<td>{escape(_text(w.get('owner')) or 'N/A')}</td>")
            block.append(f"<td>{escape(event_label)}</td>")
            block.append(f"<td>{escape(ev['actor'])}</td>")
            block.append(f"<td>{escape(ev['stage'])}</td>")
            block.append(f"<td>{escape(ev['summary'])}</td>")
            block.append("</tr>")
        block.append("</tbody></table></div>")
    block.append("</div>")

    all_events: list[Dict[str, str]] = []
    for h in handoffs:
        all_events.append(
            {
                "ts": _text(h.get("created_at")),
                "workflow": _text(h.get("workflow_id")) or "N/A",
                "type": "handoff",
                "actor": _text(h.get("agent")) or "N/A",
                "stage": _text(h.get("plan_step_id")) or "N/A",
                "summary": _short(
                    _text(h.get("changes"))
                    or _text(h.get("facts"))
                    or _text(h.get("user_goal"))
                    or _text(h.get("goal"))
                ),
            }
        )
    for c in contexts:
        all_events.append(
            {
                "ts": _text(c.get("updated_at") or c.get("created_at")),
                "workflow": _text(c.get("workflow_id")) or "N/A",
                "type": "context",
                "actor": _text(c.get("agent")) or "N/A",
                "stage": _text(c.get("context_type")) or "N/A",
                "summary": _short(_text(c.get("content")), 120),
            }
        )
    all_events = [e for e in all_events if e["ts"]]
    all_events.sort(key=lambda e: e["ts"], reverse=True)
    top_events = all_events[:25]

    block.append('<div class="section">')
    block.append("<h2>Последние взаимодействия агентов (server snapshot)</h2>")
    if not top_events:
        block.append('<div class="empty">События взаимодействия отсутствуют</div>')
    else:
        block.append('<div class="table-wrap"><table><thead><tr>')
        block.append("<th>Time</th><th>Workflow</th><th>Type</th><th>Actor</th><th>Stage</th><th>Summary</th>")
        block.append("</tr></thead><tbody>")
        for e in top_events:
            block.append("<tr>")
            block.append(f"<td>{escape(e['ts'])}</td>")
            block.append(f"<td>{escape(e['workflow'])}</td>")
            block.append(f"<td>{escape(e['type'])}</td>")
            block.append(f"<td>{escape(e['actor'])}</td>")
            block.append(f"<td>{escape(e['stage'])}</td>")
            block.append(f"<td>{escape(e['summary'])}</td>")
            block.append("</tr>")
        block.append("</tbody></table></div>")
    block.append("</div>")

    return "".join(block)


def build_html(data: Dict[str, Any], capsules_data: Dict[str, Any]) -> str:
    embedded_kg = json.dumps(data, ensure_ascii=False)
    embedded_caps = json.dumps(capsules_data, ensure_ascii=False)
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    html = []
    html.append("<!doctype html>")
    html.append('<html lang="ru">')
    html.append("<head>")
    html.append('  <meta charset="utf-8" />')
    html.append('  <meta name="viewport" content="width=device-width, initial-scale=1" />')
    html.append("  <title>Agent KG Viewer</title>")
    html.append("  <style>")
    html.append(
        """
    :root { color-scheme: light dark; }
    body { font-family: Arial, Helvetica, sans-serif; margin: 24px; max-width: 1300px; }
    h1, h2, h3 { margin: 0 0 8px; }
    .meta { color: #666; font-size: 12px; margin-bottom: 10px; }
    .section { margin-bottom: 26px; }
    .empty { color: #888; font-style: italic; }
    .section-note { font-size: 12px; color: #666; margin: 2px 0 10px; }

    .grid-two { display: grid; grid-template-columns: 1fr 1fr; gap: 18px; }
    @media (max-width: 1000px) { .grid-two { grid-template-columns: 1fr; } }

    .status-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 12px; }
    .status-card {
      border: 1px solid #ddd; border-radius: 8px; padding: 10px;
      background: #f5fbff;
    }
    .status-title { font-weight: 700; margin-bottom: 6px; }
    .status-line { font-size: 12px; margin: 3px 0; }
    .status-key { font-weight: 600; color: #444; }

    .workflow-card {
      border: 2px solid #2a5bd7; border-radius: 8px; padding: 12px;
      margin-bottom: 12px; background: #f8f9ff;
    }
    .workflow-header { display: flex; gap: 8px; align-items: center; margin-bottom: 8px; flex-wrap: wrap; }
    .badge {
      display: inline-block; padding: 2px 8px; border-radius: 10px;
      font-size: 11px; font-weight: 600; color: white;
    }
    .badge-active { background: #2a9d2a; }
    .badge-completed { background: #666; }
    .badge-phase { background: #2a5bd7; }
    .badge-owner { background: #d7832a; }
    .badge-approval-approved { background: #2a9d2a; }
    .badge-approval-pending { background: #d7832a; }
    .badge-approval-rejected { background: #be2e2e; }
    .workflow-goal { font-size: 14px; margin: 4px 0; }
    .workflow-meta { font-size: 11px; color: #666; }

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
    .handoff-field { margin: 3px 0; }
    .field-label { font-weight: 600; color: #444; text-transform: uppercase; font-size: 10px; }
    .field-value { white-space: pre-wrap; }

    .table-wrap { overflow-x: auto; border: 1px solid #ddd; border-radius: 8px; }
    table { border-collapse: collapse; width: 100%; font-size: 12px; }
    th, td { border-bottom: 1px solid #ddd; padding: 8px; text-align: left; vertical-align: top; }
    th { font-size: 11px; text-transform: uppercase; color: #555; background: #f7f7f7; }

    .trace-item {
      border: 1px solid #ddd; border-radius: 6px; padding: 8px; margin-bottom: 8px; background: #fefefe;
    }
    .trace-title { font-weight: 600; margin-bottom: 4px; }
    .trace-meta { font-size: 11px; color: #666; }

    .context-card {
      border: 1px solid #ddd; border-radius: 6px;
      padding: 10px; margin-bottom: 10px; background: #fffef5;
    }
    .context-type { font-weight: 600; font-size: 13px; color: #8a6d00; }
    .context-content { font-size: 12px; white-space: pre-wrap; margin-top: 4px; max-height: 200px; overflow-y: auto; }
    """
    )
    html.append("  </style>")
    html.append("</head>")
    html.append("<body>")
    html.append("  <h1>Agent KG Viewer</h1>")
    html.append(f'  <div class="meta">Generated from config/agent_kg.json at {generated_at}</div>')
    html.append('  <div class="meta">Run: python3 tools/agent_kg_viewer/build_agent_kg_viewer.py</div>')
    html.append(_build_server_snapshot(data))
    html.append('  <div id="app"></div>')
    html.append("  <script>")
    html.append(f"    const KG_DATA = {embedded_kg};")
    html.append(f"    const CAPSULES_DATA = {embedded_caps};")
    html.append(
        """
    function esc(text) {
      const div = document.createElement('div');
      div.textContent = text ?? '';
      return div.innerHTML;
    }

    function asArray(value) {
      return Array.isArray(value) ? value : [];
    }

    function sortByDateDesc(items, key) {
      return [...items].sort((a, b) => (b[key] || '').localeCompare(a[key] || ''));
    }

    function sortByDateAsc(items, key) {
      return [...items].sort((a, b) => (a[key] || '').localeCompare(b[key] || ''));
    }

    function approvalBadgeClass(status) {
      if (status === 'approved') return 'badge-approval-approved';
      if (status === 'rejected') return 'badge-approval-rejected';
      if (status === 'pending') return 'badge-approval-pending';
      return 'badge-completed';
    }

    function renderWorkflows(workflows) {
      const list = asArray(workflows);
      if (!list.length) return '<div class="empty">Нет workflow</div>';
      return list.map(w => {
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

    function truncateText(text, maxLen = 140) {
      const raw = String(text || '').replace(/\\s+/g, ' ').trim();
      if (!raw) return 'N/A';
      if (raw.length <= maxLen) return raw;
      return raw.slice(0, maxLen - 1) + '…';
    }

    function renderExecutionStatus(workflows, handoffs, contexts) {
      const active = asArray(workflows).filter(w => (w.status || '').toLowerCase() === 'active');
      if (!active.length) return '<div class="empty">Активных workflow нет</div>';

      return '<div class="status-grid">' + active.map(w => {
        const wfHandoffs = sortByDateDesc(asArray(handoffs).filter(h => h.workflow_id === w.workflow_id), 'created_at');
        const wfContexts = sortByDateDesc(asArray(contexts).filter(c => c.workflow_id === w.workflow_id), 'updated_at');
        const lastHandoff = wfHandoffs[0];
        const lastContext = wfContexts[0];

        const handoffTs = (lastHandoff && lastHandoff.created_at) ? lastHandoff.created_at : '';
        const contextTs = (lastContext && (lastContext.updated_at || lastContext.created_at))
          ? (lastContext.updated_at || lastContext.created_at)
          : '';

        let latestType = 'N/A';
        let latestAt = 'N/A';
        let latestActor = 'N/A';
        let latestSummary = 'N/A';

        if (handoffTs && (!contextTs || handoffTs >= contextTs)) {
          latestType = 'handoff';
          latestAt = handoffTs;
          latestActor = lastHandoff.agent || 'N/A';
          latestSummary = `${lastHandoff.agent || '?'} -> ${lastHandoff.next_owner || '?'} | step=${lastHandoff.plan_step_id || 'N/A'}`;
        } else if (contextTs) {
          latestType = 'context';
          latestAt = contextTs;
          latestActor = lastContext.agent || 'N/A';
          latestSummary = `${lastContext.context_type || 'N/A'} | ${truncateText(lastContext.content, 100)}`;
        }

        return `
          <div class="status-card">
            <div class="status-title">${esc(w.workflow_id || '')}</div>
            <div class="status-line"><span class="status-key">Статус:</span> ${esc(w.status || 'N/A')}</div>
            <div class="status-line"><span class="status-key">Фаза:</span> ${esc(w.phase || 'N/A')}</div>
            <div class="status-line"><span class="status-key">Текущий владелец:</span> ${esc(w.owner || 'N/A')}</div>
            <div class="status-line"><span class="status-key">Последнее событие:</span> ${esc(latestType)} @ ${esc(latestAt)}</div>
            <div class="status-line"><span class="status-key">Кто:</span> ${esc(latestActor)}</div>
            <div class="status-line"><span class="status-key">Детали:</span> ${esc(latestSummary)}</div>
            <div class="status-line"><span class="status-key">Goal:</span> ${esc(truncateText(w.goal, 160))}</div>
          </div>
        `;
      }).join('') + '</div>';
    }

    function renderHandoffs(handoffs) {
      const sorted = sortByDateDesc(asArray(handoffs), 'created_at');
      if (!sorted.length) return '<div class="empty">Нет handoff\\'ов</div>';
      return '<div class="timeline">' + sorted.map(h => {
        const approvalRaw = [h.approval_gate_id || '', h.approval_status || '', h.approval_source || '']
          .filter(Boolean)
          .join(' | ');
        const approvalBadge = h.approval_status
          ? `<span class="badge ${approvalBadgeClass(h.approval_status)}">${esc(h.approval_status)}</span>`
          : '';
        return `
        <div class="handoff-card">
          <div class="handoff-header">
            <span class="handoff-agent">${esc(h.agent)}</span>
            <span class="badge badge-phase">${esc(h.workflow_id)}</span>
            ${approvalBadge}
            <span style="font-size:11px;color:#666">${esc(h.created_at || '')}</span>
          </div>
          <div class="handoff-body">
            <div class="handoff-field"><span class="field-label">TraceID:</span> <span class="field-value">${esc(h.trace_id || 'N/A')}</span></div>
            <div class="handoff-field"><span class="field-label">PlanStepID:</span> <span class="field-value">${esc(h.plan_step_id || 'N/A')}</span></div>
            <div class="handoff-field"><span class="field-label">UserGoal:</span> <span class="field-value">${esc(h.user_goal || h.goal || '')}</span></div>
            <div class="handoff-field"><span class="field-label">Changes:</span> <span class="field-value">${esc(h.changes || '')}</span></div>
            <div class="handoff-field"><span class="field-label">Facts:</span> <span class="field-value">${esc(h.facts || h.evidence || '')}</span></div>
            ${h.assumptions ? `<div class="handoff-field"><span class="field-label">Assumptions:</span> <span class="field-value">${esc(h.assumptions)}</span></div>` : ''}
            ${approvalRaw ? `<div class="handoff-field"><span class="field-label">ApprovalGate:</span> <span class="field-value">${esc(approvalRaw)}</span></div>` : ''}
            ${h.drift_check ? `<div class="handoff-field"><span class="field-label">DriftCheck:</span> <span class="field-value">${esc(h.drift_check)}</span></div>` : ''}
            <div class="handoff-field"><span class="field-label">Risks:</span> <span class="field-value">${esc(h.risks || '')}</span></div>
            <div class="handoff-field"><span class="field-label">NextOwner:</span> <span class="field-value">${esc(h.next_owner || '')}</span></div>
          </div>
        </div>`;
      }).join('') + '</div>';
    }

    function renderWorkflowTrace(workflows, handoffs) {
      const wfList = asArray(workflows);
      const handoffList = sortByDateAsc(asArray(handoffs), 'created_at');
      if (!wfList.length || !handoffList.length) {
        return '<div class="empty">Недостаточно данных для WorkflowTrace</div>';
      }

      const grouped = {};
      for (const h of handoffList) {
        const wf = h.workflow_id || 'unknown';
        if (!grouped[wf]) grouped[wf] = [];
        grouped[wf].push(h);
      }

      return wfList.map(w => {
        const items = grouped[w.workflow_id] || [];
        if (!items.length) return '';
        const rows = items.map(i => `
          <div class="trace-item">
            <div class="trace-title">${esc(i.agent || '?')} -> ${esc(i.next_owner || '?')}</div>
            <div class="trace-meta">
              ${esc(i.created_at || '')} | step=${esc(i.plan_step_id || 'N/A')} | trace=${esc(i.trace_id || 'N/A')}
            </div>
          </div>
        `).join('');
        return `
          <div class="workflow-card">
            <div class="workflow-header">
              <strong>${esc(w.workflow_id)}</strong>
              <span class="badge badge-phase">${esc(w.phase || '?')}</span>
              <span class="badge ${w.status === 'active' ? 'badge-active' : 'badge-completed'}">${esc(w.status || '?')}</span>
            </div>
            ${rows}
          </div>
        `;
      }).join('') || '<div class="empty">Нет trace-событий</div>';
    }

    function renderGovernanceGates(handoffs) {
      const rows = sortByDateDesc(asArray(handoffs).filter(h =>
        (h.approval_gate_id || h.approval_status || h.approval_source)
      ), 'created_at');

      if (!rows.length) return '<div class="empty">ApprovalGate записи отсутствуют</div>';

      const body = rows.map(r => `
        <tr>
          <td>${esc(r.workflow_id || '')}</td>
          <td>${esc(r.agent || '')}</td>
          <td>${esc(r.approval_gate_id || 'N/A')}</td>
          <td>${esc(r.approval_status || 'N/A')}</td>
          <td>${esc(r.approval_source || 'N/A')}</td>
          <td>${esc(r.trace_id || 'N/A')}</td>
          <td>${esc(r.plan_step_id || 'N/A')}</td>
          <td>${esc(r.created_at || '')}</td>
        </tr>
      `).join('');

      return `
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Workflow</th><th>Agent</th><th>GateID</th><th>Status</th>
                <th>Source</th><th>TraceID</th><th>PlanStepID</th><th>Created</th>
              </tr>
            </thead>
            <tbody>${body}</tbody>
          </table>
        </div>
      `;
    }

    function renderLatestInteractions(handoffs, contexts, limit = 25) {
      const handoffEvents = asArray(handoffs).map(h => ({
        ts: h.created_at || '',
        workflow: h.workflow_id || '',
        type: 'handoff',
        actor: h.agent || 'N/A',
        stage: h.plan_step_id || 'N/A',
        summary: truncateText(h.changes || h.facts || h.user_goal || h.goal, 160),
      }));

      const contextEvents = asArray(contexts).map(c => ({
        ts: c.updated_at || c.created_at || '',
        workflow: c.workflow_id || '',
        type: 'context',
        actor: c.agent || 'N/A',
        stage: c.context_type || 'N/A',
        summary: truncateText(c.content, 160),
      }));

      const all = [...handoffEvents, ...contextEvents]
        .filter(e => e.ts)
        .sort((a, b) => (b.ts || '').localeCompare(a.ts || ''))
        .slice(0, limit);

      if (!all.length) return '<div class="empty">События взаимодействия отсутствуют</div>';

      const body = all.map(e => `
        <tr>
          <td>${esc(e.ts)}</td>
          <td>${esc(e.workflow)}</td>
          <td>${esc(e.type)}</td>
          <td>${esc(e.actor)}</td>
          <td>${esc(e.stage)}</td>
          <td>${esc(e.summary)}</td>
        </tr>
      `).join('');

      return `
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Time</th><th>Workflow</th><th>Type</th><th>Actor</th><th>Stage</th><th>Summary</th>
              </tr>
            </thead>
            <tbody>${body}</tbody>
          </table>
        </div>
      `;
    }

    function renderRlmReuse(handoffs, capsulesData) {
      const capsules = asArray((capsulesData || {}).capsules);
      if (!capsules.length) return '<div class="empty">capsules_manifest.json не найден или пуст</div>';

      const handoffList = asArray(handoffs);
      const rows = capsules.map(c => {
        const id = (c.id || '').toString();
        const idLower = id.toLowerCase();
        let mentions = 0;
        const workflows = new Set();

        for (const h of handoffList) {
          const body = `${h.changes || ''} ${h.facts || ''}`.toLowerCase();
          if (!idLower || !body.includes(idLower)) continue;
          mentions += 1;
          if (h.workflow_id) workflows.add(h.workflow_id);
        }

        return {
          id,
          domain: c.domain || '',
          updated: c.updated || c.updated_at || '',
          mentions,
          workflows: Array.from(workflows),
        };
      }).sort((a, b) => b.mentions - a.mentions || a.id.localeCompare(b.id));

      const body = rows.map(r => `
        <tr>
          <td>${esc(r.id || '')}</td>
          <td>${esc(r.domain || '')}</td>
          <td>${esc(r.updated || 'N/A')}</td>
          <td>${esc(String(r.mentions))}</td>
          <td>${esc(r.workflows.join(', ') || 'N/A')}</td>
        </tr>
      `).join('');

      return `
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>CapsuleID</th><th>Domain</th><th>Updated</th><th>MentionsInHandoffs</th><th>Workflows</th>
              </tr>
            </thead>
            <tbody>${body}</tbody>
          </table>
        </div>
      `;
    }

    function renderContexts(contexts) {
      const sorted = sortByDateDesc(asArray(contexts), 'updated_at');
      if (!sorted.length) return '<div class="empty">Нет контекстов</div>';
      return sorted.map(c => `
        <div class="context-card">
          <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">
            <span class="context-type">${esc(c.context_type)}</span>
            <span style="font-size:11px;color:#666">${esc(c.agent || '?')} | ${esc(c.updated_at || '')}</span>
            <span style="font-size:11px;color:#666">${esc(c.workflow_id || '')}</span>
          </div>
          <div class="context-content">${esc((c.content || '').substring(0, 1000))}${(c.content || '').length > 1000 ? '...' : ''}</div>
        </div>`).join('');
    }

    function render(data, capsulesData) {
      const app = document.getElementById('app');
      const workflows = asArray(data.workflows);
      const handoffs = asArray(data.handoffs);
      const contexts = asArray(data.contexts);
      let html = '';

      html += '<div class="section"><h2>Текущий статус исполнения</h2>';
      html += '<div class="section-note">Активные workflow: текущая фаза, владелец, последнее взаимодействие (handoff/context) из Agent KG.</div>';
      html += renderExecutionStatus(workflows, handoffs, contexts);
      html += '</div>';

      html += '<div class="section"><h2>Последние взаимодействия агентов</h2>';
      html += '<div class="section-note">Лента последних handoff/context событий, отсортированная по времени.</div>';
      html += renderLatestInteractions(handoffs, contexts, 25);
      html += '</div>';

      html += '<div class="section"><h2>Workflows</h2>';
      html += renderWorkflows(workflows);
      html += '</div>';

      html += '<div class="section grid-two">';
      html += '<div><h2>WorkflowTrace</h2>' + renderWorkflowTrace(workflows, handoffs) + '</div>';
      html += '<div><h2>GovernanceGates</h2>' + renderGovernanceGates(handoffs) + '</div>';
      html += '</div>';

      html += '<div class="section"><h2>Handoffs</h2>';
      html += renderHandoffs(handoffs);
      html += '</div>';

      html += '<div class="section"><h2>RLMReuse</h2>';
      html += renderRlmReuse(handoffs, capsulesData);
      html += '</div>';

      html += '<div class="section"><h2>Contexts</h2>';
      html += renderContexts(contexts);
      html += '</div>';

      app.innerHTML = html;
    }

    render(KG_DATA, CAPSULES_DATA);
    """
    )
    html.append("  </script>")
    html.append("</body>")
    html.append("</html>")

    return "\n".join(html)


def main() -> None:
    if not os.path.exists(INPUT_JSON):
        print(f"Input not found: {INPUT_JSON}")
        raise SystemExit(1)

    kg_data = _load_json(INPUT_JSON)
    capsules_data = _load_optional_json(CAPSULES_JSON, {"capsules": []})

    result = build_html(kg_data, capsules_data)
    os.makedirs(os.path.dirname(OUTPUT_HTML), exist_ok=True)
    with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
        f.write(result)
    print(f"Wrote {OUTPUT_HTML}")


if __name__ == "__main__":
    main()
