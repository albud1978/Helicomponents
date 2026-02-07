#!/usr/bin/env python3
"""
Build standalone HTML viewer for multi-agent workflow interactions.

Sources:
  - config/agent_system/graph.json   — agent graph (roles, edges)
  - .cursor/agents/*.md              — agent profiles (zones, permissions)
  - .cursor/rules/90_multiagent_workflow.mdc — workflow rules

Output:
  - tools/agents_viewer/index.html   — standalone HTML viewer
"""

import json
import os
import re
import glob
from datetime import datetime, timezone

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
GRAPH_JSON = os.path.join(ROOT_DIR, "config", "agent_system", "graph.json")
AGENTS_DIR = os.path.join(ROOT_DIR, ".cursor", "agents")
WORKFLOW_MDC = os.path.join(ROOT_DIR, ".cursor", "rules", "90_multiagent_workflow.mdc")
OUTPUT_HTML = os.path.join(ROOT_DIR, "tools", "agents_viewer", "index.html")


def parse_agent_md(filepath: str) -> dict:
    """Parse agent .md file, extract frontmatter and key sections."""
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()

    agent = {"file": os.path.basename(filepath)}

    # Parse YAML frontmatter
    fm_match = re.match(r"^---\s*\n(.*?)\n---", content, re.DOTALL)
    if fm_match:
        for line in fm_match.group(1).split("\n"):
            if ":" in line:
                key, val = line.split(":", 1)
                agent[key.strip()] = val.strip()

    # Extract zones (lines starting with - under "## Зона работы" or "## Зона ответственности")
    zones = []
    zone_match = re.search(
        r"## Зона (?:работы|ответственности)\s*\n((?:- .+\n)*)", content
    )
    if zone_match:
        for line in zone_match.group(1).strip().split("\n"):
            line = line.strip().lstrip("- ").strip()
            if line:
                zones.append(line)
    agent["zones"] = zones

    # Extract restrictions (lines starting with - under "## Запреты")
    restrictions = []
    restr_match = re.search(r"## Запреты\s*\n((?:[-–] .+\n)*)", content)
    if restr_match:
        for line in restr_match.group(1).strip().split("\n"):
            line = line.strip().lstrip("-–").strip()
            if line:
                restrictions.append(line)
    agent["restrictions"] = restrictions

    # Extract permissions (lines starting with - under "## Разрешено")
    permissions = []
    perm_match = re.search(r"## Разрешено\s*\n((?:- .+\n)*)", content)
    if perm_match:
        for line in perm_match.group(1).strip().split("\n"):
            line = line.strip().lstrip("- ").strip()
            if line:
                permissions.append(line)
    agent["permissions"] = permissions

    return agent


def parse_workflow_mdc(filepath: str) -> dict:
    """Parse workflow .mdc file for pipeline, governance, handoff info."""
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()

    workflow = {}

    # Extract pipeline steps from "## Процесс (канонический цикл)"
    pipeline = []
    pipe_match = re.search(
        r"## Процесс \(канонический цикл\)\s*\n((?:\d+\..+\n)*)", content
    )
    if pipe_match:
        for line in pipe_match.group(1).strip().split("\n"):
            step = re.sub(r"^\d+\.\s*", "", line).strip()
            if step:
                pipeline.append(step)
    workflow["pipeline"] = pipeline

    # Extract governance gates
    gates = []
    gate_match = re.search(
        r"## Governance.гейты\s*\n(?:.*?\n)?((?:- .+\n)*)", content
    )
    if gate_match:
        for line in gate_match.group(1).strip().split("\n"):
            line = line.strip().lstrip("-–").strip()
            if line:
                gates.append(line)
    workflow["governance_gates"] = gates

    # Extract handoff fields
    handoff_fields = []
    hf_match = re.search(
        r"\*\*Handoff\*\*\s*\n((?:- \*\*.+\n)*)", content
    )
    if hf_match:
        for line in hf_match.group(1).strip().split("\n"):
            field_match = re.match(r"- \*\*(\w+)\*\*:?\s*(.*)", line.strip())
            if field_match:
                handoff_fields.append({
                    "name": field_match.group(1),
                    "desc": field_match.group(2).strip(". ")
                })
    workflow["handoff_fields"] = handoff_fields

    # Extract sequential pipeline order
    seq_match = re.search(r"\*\*Порядок:\*\*\s*(.+)", content)
    if seq_match:
        workflow["sequential_order"] = seq_match.group(1).strip()

    # Extract iteration limit
    iter_match = re.search(r"Максимум \*\*(\d+) итераци", content)
    if iter_match:
        workflow["iteration_limit"] = int(iter_match.group(1))

    # Extract conflict resolution priorities
    priorities = []
    prio_match = re.search(
        r"## Разрешение конфликтов\s*\n.*?\n((?:\d+\..+\n)*)", content, re.DOTALL
    )
    if prio_match:
        for line in prio_match.group(1).strip().split("\n"):
            p = re.sub(r"^\d+\.\s*", "", line).strip()
            if p:
                priorities.append(p)
    workflow["conflict_priorities"] = priorities

    return workflow


def build_interaction_matrix(agents: list, graph_data: dict) -> list:
    """Build interaction matrix from graph edges and workflow knowledge."""
    agent_names = [a.get("name", "") for a in agents]

    # Define known interactions based on sequential pipeline
    interactions = [
        {"from": "orchestrator", "to": "coder-flame",
         "type": "task", "label": "Задача на реализацию (FLAME GPU/RTC)"},
        {"from": "orchestrator", "to": "coder-general",
         "type": "task", "label": "Задача на реализацию (общий код/ETL)"},
        {"from": "orchestrator", "to": "analyst-sql-graph",
         "type": "task", "label": "Сбор контекста, SQL-запросы, граф"},
        {"from": "orchestrator", "to": "reviewer-flame",
         "type": "task", "label": "Запрос на code review"},
        {"from": "orchestrator", "to": "validator-judge",
         "type": "task", "label": "Запрос на валидацию результатов"},
        {"from": "orchestrator", "to": "capsule-builder",
         "type": "task", "label": "Обновление контекстной капсулы"},
        {"from": "coder-flame", "to": "orchestrator",
         "type": "handoff", "label": "Handoff: результат реализации"},
        {"from": "coder-general", "to": "orchestrator",
         "type": "handoff", "label": "Handoff: результат реализации"},
        {"from": "analyst-sql-graph", "to": "orchestrator",
         "type": "handoff", "label": "Handoff: контекст и данные"},
        {"from": "reviewer-flame", "to": "orchestrator",
         "type": "handoff", "label": "Handoff: вердикт ревью"},
        {"from": "validator-judge", "to": "orchestrator",
         "type": "handoff", "label": "Handoff: PASS/FAIL"},
        {"from": "capsule-builder", "to": "orchestrator",
         "type": "handoff", "label": "Handoff: капсула обновлена"},
        {"from": "orchestrator", "to": "human",
         "type": "escalation", "label": "Эскалация / governance / финальный отчёт"},
        {"from": "human", "to": "orchestrator",
         "type": "goal", "label": "Цель + ограничения"},
    ]

    return interactions


def build_html(agents: list, graph_data: dict, workflow: dict,
               interactions: list) -> str:
    """Generate standalone HTML viewer."""
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    data = {
        "agents": agents,
        "graph": graph_data,
        "workflow": workflow,
        "interactions": interactions,
    }
    embedded = json.dumps(data, ensure_ascii=False, indent=None)

    html = []
    html.append("<!doctype html>")
    html.append('<html lang="ru">')
    html.append("<head>")
    html.append('  <meta charset="utf-8" />')
    html.append('  <meta name="viewport" content="width=device-width, initial-scale=1" />')
    html.append("  <title>Agents Interaction Viewer</title>")
    html.append("  <style>")
    html.append("""
    :root { color-scheme: light dark; }
    body { font-family: Arial, Helvetica, sans-serif; margin: 24px; max-width: 1400px; }
    h1 { margin: 0 0 4px; }
    h2 { margin: 20px 0 8px; border-bottom: 2px solid #2a5bd7; padding-bottom: 4px; }
    h3 { margin: 12px 0 6px; }
    .meta { color: #666; font-size: 12px; margin-bottom: 16px; }
    .section { margin-bottom: 20px; }

    /* Agent cards */
    .agents-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(320px, 1fr)); gap: 12px; }
    .agent-card { border: 1px solid #ccc; border-radius: 8px; padding: 12px; background: #fafafa; }
    .agent-card.orchestrator { border-color: #2a5bd7; border-width: 2px; background: #f0f4ff; }
    .agent-card.human { border-color: #d72a2a; border-width: 2px; background: #fff0f0; }
    .agent-name { font-size: 16px; font-weight: 700; margin-bottom: 4px; }
    .agent-desc { font-size: 12px; color: #555; margin-bottom: 8px; }
    .agent-section { margin-bottom: 6px; }
    .agent-section .label { font-size: 11px; color: #444; text-transform: uppercase; font-weight: 600; }
    .agent-section ul { margin: 2px 0 0 16px; padding: 0; font-size: 12px; }
    .agent-section li { margin-bottom: 2px; }
    .badge { display: inline-block; padding: 2px 8px; border-radius: 10px; font-size: 11px; margin-right: 4px; color: white; }
    .badge-role { background: #2a5bd7; }
    .badge-zone { background: #27a844; }
    .badge-restrict { background: #d72a2a; }
    .badge-perm { background: #e89b0c; }

    /* Pipeline */
    .pipeline { display: flex; align-items: center; flex-wrap: wrap; gap: 4px; margin: 8px 0; }
    .pipeline-step { background: #2a5bd7; color: white; padding: 6px 14px; border-radius: 20px; font-size: 13px; font-weight: 600; }
    .pipeline-arrow { font-size: 20px; color: #2a5bd7; }

    /* Interaction matrix */
    table.matrix { border-collapse: collapse; font-size: 12px; }
    table.matrix th, table.matrix td { border: 1px solid #ccc; padding: 6px 8px; text-align: center; }
    table.matrix th { background: #f3f3f3; font-size: 11px; }
    table.matrix td.task { background: #e8f0fe; }
    table.matrix td.handoff { background: #e6f4ea; }
    table.matrix td.escalation { background: #fce8e6; }
    table.matrix td.goal { background: #fff3e0; }
    table.matrix td.empty { color: #ccc; }
    .matrix-legend { display: flex; gap: 16px; margin: 8px 0; font-size: 12px; }
    .legend-item { display: flex; align-items: center; gap: 4px; }
    .legend-box { width: 14px; height: 14px; border-radius: 3px; border: 1px solid #ccc; }

    /* Governance */
    .gate-list { list-style: none; padding: 0; }
    .gate-list li { padding: 4px 0; font-size: 13px; border-bottom: 1px solid #eee; }
    .gate-list li:before { content: "\\26A0"; margin-right: 6px; }

    /* Handoff */
    .handoff-table { border-collapse: collapse; font-size: 13px; width: 100%; }
    .handoff-table th, .handoff-table td { border: 1px solid #ccc; padding: 6px 10px; text-align: left; }
    .handoff-table th { background: #f3f3f3; width: 160px; }

    /* Conflict priorities */
    .priority-list { list-style: none; padding: 0; counter-reset: prio; }
    .priority-list li { padding: 4px 0; font-size: 13px; border-bottom: 1px solid #eee; counter-increment: prio; }
    .priority-list li:before { content: counter(prio) ". "; font-weight: 700; color: #2a5bd7; }
    """)
    html.append("  </style>")
    html.append("</head>")
    html.append("<body>")
    html.append("  <h1>Agents Interaction Viewer</h1>")
    html.append(
        f'  <div class="meta">Generated from config/agent_system/graph.json '
        f'and .cursor/agents/*.md at {generated_at}</div>'
    )
    html.append(
        '  <div class="meta">Run: python3 tools/agents_viewer/build_agents_viewer.py</div>'
    )
    html.append('  <div id="app"></div>')
    html.append("  <script>")
    html.append(f"    const DATA = {embedded};")

    # JavaScript renderer
    html.append("""
    function escapeHtml(text) {
      const div = document.createElement('div');
      div.textContent = text ?? '';
      return div.innerHTML;
    }

    function render() {
      const app = document.getElementById('app');
      let h = '';

      // --- Sequential Pipeline ---
      h += '<h2>Sequential Pipeline</h2>';
      const order = DATA.workflow.sequential_order || '';
      if (order) {
        const steps = order.split(/\\s*→\\s*/);
        h += '<div class="pipeline">';
        steps.forEach((s, i) => {
          h += `<div class="pipeline-step">${escapeHtml(s)}</div>`;
          if (i < steps.length - 1) h += '<div class="pipeline-arrow">→</div>';
        });
        h += '</div>';
      }
      if (DATA.workflow.iteration_limit) {
        h += `<div style="font-size:13px;margin-top:6px;">Лимит итераций (implement→review): <b>${DATA.workflow.iteration_limit}</b> — затем эскалация человеку.</div>`;
      }

      // --- Agent Cards ---
      h += '<h2>Агенты</h2>';
      h += '<div class="agents-grid">';

      // Human card
      h += '<div class="agent-card human">';
      h += '<div class="agent-name">Человек (Alexey)</div>';
      h += '<div class="agent-desc">Утверждает архитектурные/алгоритмические решения и решения с высоким риском</div>';
      h += '<div class="agent-section"><div class="label">Роль</div>';
      h += '<span class="badge badge-role">Owner / Governance</span></div>';
      h += '</div>';

      DATA.agents.forEach(a => {
        const isOrch = a.name === 'orchestrator';
        h += `<div class="agent-card${isOrch ? ' orchestrator' : ''}">`;
        h += `<div class="agent-name">${escapeHtml(a.name)}</div>`;
        if (a.description) h += `<div class="agent-desc">${escapeHtml(a.description)}</div>`;
        if (a.model) h += `<div style="font-size:11px;color:#888;">model: ${escapeHtml(a.model)}</div>`;

        if (a.zones && a.zones.length) {
          h += '<div class="agent-section"><div class="label">Зона</div><ul>';
          a.zones.forEach(z => { h += `<li>${escapeHtml(z)}</li>`; });
          h += '</ul></div>';
        }
        if (a.restrictions && a.restrictions.length) {
          h += '<div class="agent-section"><div class="label">Запреты</div><ul>';
          a.restrictions.forEach(r => { h += `<li style="color:#c62828;">${escapeHtml(r)}</li>`; });
          h += '</ul></div>';
        }
        if (a.permissions && a.permissions.length) {
          h += '<div class="agent-section"><div class="label">Разрешено</div><ul>';
          a.permissions.forEach(p => { h += `<li style="color:#2e7d32;">${escapeHtml(p)}</li>`; });
          h += '</ul></div>';
        }
        h += '</div>';
      });
      h += '</div>';

      // --- Interaction Matrix ---
      h += '<h2>Матрица взаимодействий</h2>';
      h += '<div class="matrix-legend">';
      h += '<div class="legend-item"><div class="legend-box" style="background:#e8f0fe;"></div> Задача (task)</div>';
      h += '<div class="legend-item"><div class="legend-box" style="background:#e6f4ea;"></div> Handoff</div>';
      h += '<div class="legend-item"><div class="legend-box" style="background:#fce8e6;"></div> Эскалация</div>';
      h += '<div class="legend-item"><div class="legend-box" style="background:#fff3e0;"></div> Цель от человека</div>';
      h += '</div>';

      const participants = ['human', 'orchestrator'];
      DATA.agents.forEach(a => { if (a.name !== 'orchestrator') participants.push(a.name); });
      const labels = { human: 'Человек' };
      DATA.agents.forEach(a => { labels[a.name] = a.name; });

      h += '<table class="matrix"><tr><th></th>';
      participants.forEach(p => { h += `<th>${escapeHtml(labels[p] || p)}</th>`; });
      h += '</tr>';

      participants.forEach(from => {
        h += `<tr><th style="text-align:left;">${escapeHtml(labels[from] || from)}</th>`;
        participants.forEach(to => {
          const ints = DATA.interactions.filter(i => i.from === from && i.to === to);
          if (ints.length === 0) {
            h += '<td class="empty">—</td>';
          } else {
            const cls = ints[0].type;
            const text = ints.map(i => escapeHtml(i.label)).join('<br/>');
            h += `<td class="${cls}">${text}</td>`;
          }
        });
        h += '</tr>';
      });
      h += '</table>';

      // --- Handoff Template ---
      h += '<h2>Handoff-шаблон</h2>';
      if (DATA.workflow.handoff_fields && DATA.workflow.handoff_fields.length) {
        h += '<table class="handoff-table"><tr><th>Поле</th><th>Описание</th></tr>';
        DATA.workflow.handoff_fields.forEach(f => {
          h += `<tr><td><b>${escapeHtml(f.name)}</b></td><td>${escapeHtml(f.desc)}</td></tr>`;
        });
        h += '</table>';
      }

      // --- Governance Gates ---
      h += '<h2>Governance-гейты (требуют подтверждения человека)</h2>';
      if (DATA.workflow.governance_gates && DATA.workflow.governance_gates.length) {
        h += '<ul class="gate-list">';
        DATA.workflow.governance_gates.forEach(g => { h += `<li>${escapeHtml(g)}</li>`; });
        h += '</ul>';
      }

      // --- Conflict Resolution ---
      h += '<h2>Приоритеты разрешения конфликтов</h2>';
      if (DATA.workflow.conflict_priorities && DATA.workflow.conflict_priorities.length) {
        h += '<ol class="priority-list">';
        DATA.workflow.conflict_priorities.forEach(p => {
          h += `<li>${escapeHtml(p)}</li>`;
        });
        h += '</ol>';
      }

      // --- Canonical Cycle ---
      h += '<h2>Канонический цикл</h2>';
      if (DATA.workflow.pipeline && DATA.workflow.pipeline.length) {
        h += '<div class="pipeline">';
        DATA.workflow.pipeline.forEach((s, i) => {
          const parts = s.split(' — ');
          const name = parts[0].replace(/\\*\\*/g, '');
          h += `<div class="pipeline-step">${escapeHtml(name)}</div>`;
          if (i < DATA.workflow.pipeline.length - 1) h += '<div class="pipeline-arrow">→</div>';
        });
        h += '</div>';
        h += '<table class="handoff-table" style="margin-top:8px;"><tr><th>Этап</th><th>Описание</th></tr>';
        DATA.workflow.pipeline.forEach(s => {
          const parts = s.split(' — ');
          const name = parts[0].replace(/\\*\\*/g, '');
          const desc = parts.length > 1 ? parts[1] : '';
          h += `<tr><td><b>${escapeHtml(name)}</b></td><td>${escapeHtml(desc)}</td></tr>`;
        });
        h += '</table>';
      }

      app.innerHTML = h;
    }
    render();
    """)
    html.append("  </script>")
    html.append("</body>")
    html.append("</html>")

    return "\n".join(html)


def main():
    # Load graph
    with open(GRAPH_JSON, "r", encoding="utf-8") as f:
        graph_data = json.load(f)

    # Load agent profiles
    agents = []
    for md_file in sorted(glob.glob(os.path.join(AGENTS_DIR, "*.md"))):
        agents.append(parse_agent_md(md_file))

    # Load workflow
    workflow = parse_workflow_mdc(WORKFLOW_MDC)

    # Build interactions
    interactions = build_interaction_matrix(agents, graph_data)

    # Generate HTML
    html = build_html(agents, graph_data, workflow, interactions)

    with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"Generated {OUTPUT_HTML}")
    print(f"  Agents: {len(agents)}")
    print(f"  Interactions: {len(interactions)}")
    print(f"  Pipeline steps: {len(workflow.get('pipeline', []))}")
    print(f"  Governance gates: {len(workflow.get('governance_gates', []))}")


if __name__ == "__main__":
    main()
