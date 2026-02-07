SHELL := /bin/bash

-include .env

.PHONY: sync-domain-graph agent-kg-viewer transitions-viewer

# Синхронизация доменного графа (JSON SSoT -> Neo4j Aura)
sync-domain-graph:
	python3 code/utils/sync_domain_graph.py

sync-domain-graph-clear:
	python3 code/utils/sync_domain_graph.py --clear

sync-domain-graph-dry:
	python3 code/utils/sync_domain_graph.py --dry-run

# Визуализация Agent KG (JSON -> HTML)
agent-kg-viewer:
	python3 tools/agent_kg_viewer/build_agent_kg_viewer.py

# Визуализация переходов (JSON -> HTML)
transitions-viewer:
	python3 tools/transitions_viewer/build_transitions_viewer.py
