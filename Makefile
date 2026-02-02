SHELL := /bin/bash

NEO4J_CONTAINER := neo4j-local
NEO4J_IMAGE := neo4j:5
NEO4J_HTTP_PORT ?= 7474
NEO4J_BOLT_PORT ?= 7687
NEO4J_DATA_DIR ?= output/neo4j/data
NEO4J_LOG_DIR ?= output/neo4j/logs

-include .env

.PHONY: kg-up kg-down kg-logs kg-status

kg-up:
	@mkdir -p "$(NEO4J_DATA_DIR)" "$(NEO4J_LOG_DIR)"
	@if docker ps -a --format '{{.Names}}' | grep -q '^$(NEO4J_CONTAINER)$$'; then \
		docker start "$(NEO4J_CONTAINER)"; \
	else \
		if [ -z "$${KG_NEO4J_PASSWORD}" ]; then \
			echo "KG_NEO4J_PASSWORD is required in .env"; \
			exit 1; \
		fi; \
		docker run -d --name "$(NEO4J_CONTAINER)" \
			-p "$(NEO4J_HTTP_PORT):7474" \
			-p "$(NEO4J_BOLT_PORT):7687" \
			-e NEO4J_AUTH="$${KG_NEO4J_USER:-neo4j}/$${KG_NEO4J_PASSWORD}" \
			-v "$$(pwd)/$(NEO4J_DATA_DIR)":/data \
			-v "$$(pwd)/$(NEO4J_LOG_DIR)":/logs \
			"$(NEO4J_IMAGE)"; \
	fi

kg-down:
	@docker stop "$(NEO4J_CONTAINER)" || true
	@docker rm "$(NEO4J_CONTAINER)" || true

kg-logs:
	@docker logs "$(NEO4J_CONTAINER)" --tail 50

kg-status:
	@docker ps --filter "name=$(NEO4J_CONTAINER)"
