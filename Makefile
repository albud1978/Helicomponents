SHELL := /bin/bash

-include .env

.PHONY: sync-domain-graph sync-domain-graph-clear sync-domain-graph-dry
.PHONY: neo4j-local-up neo4j-local-down neo4j-local-status neo4j-local-logs
.PHONY: kg-project-neo4j kg-project-neo4j-full
.PHONY: agent-kg-viewer transitions-viewer

# Синхронизация доменного графа (JSON SSoT -> Neo4j)
sync-domain-graph:
	python3 code/utils/sync_domain_graph.py

sync-domain-graph-clear:
	python3 code/utils/sync_domain_graph.py --clear

sync-domain-graph-dry:
	python3 code/utils/sync_domain_graph.py --dry-run

neo4j-local-up:
	@cd deploy/neo4j-local && docker compose up -d
	@echo "Neo4j Browser: http://localhost:7474"
	@echo "Bolt endpoint: bolt://localhost:7687"

neo4j-local-down:
	@cd deploy/neo4j-local && docker compose down

neo4j-local-status:
	@cd deploy/neo4j-local && docker compose ps

neo4j-local-logs:
	@cd deploy/neo4j-local && docker compose logs -f neo4j

kg-project-neo4j:
	python3 tools/agent_kg_to_neo4j.py

kg-project-neo4j-full:
	python3 tools/agent_kg_to_neo4j.py --include-archive --reset

# Визуализация Agent KG (JSON -> HTML)
agent-kg-viewer:
	python3 tools/agent_kg_viewer/build_agent_kg_viewer.py

# Визуализация переходов (JSON -> HTML)
transitions-viewer:
	python3 tools/transitions_viewer/build_transitions_viewer.py
