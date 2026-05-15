# Neo4j local Domain Graph

Локальный Neo4j Community Server используется как производная визуализация
доменной модели. Источник истины остаётся в JSON:
`config/transitions/transitions_rules.json` и
`config/transitions/quota_rules.json`.

## Quickstart

1. Скопируй настройки в корневой `.env` и задай локальный пароль:

   ```bash
   cp deploy/neo4j-local/.env.example .env
   ```

2. Запусти контейнер:

   ```bash
   make neo4j-local-up
   ```

3. Открой Neo4j Browser:

   ```text
   http://localhost:7474
   ```

4. Первый вход: `neo4j` / `changeme-on-first-login`.

5. Синхронизируй Domain Graph после проверки `.env`:

   ```bash
   make sync-domain-graph
   ```

## Operations

```bash
make neo4j-local-status
make neo4j-local-logs
make neo4j-local-down
```

## Reset local data

Останови контейнер и удали локальные Docker volumes:

```bash
make neo4j-local-down
docker volume rm neo4j-local_neo4j-data neo4j-local_neo4j-logs neo4j-local_neo4j-import
```

## Troubleshooting

- Если Browser не открывается, проверь `make neo4j-local-status`.
- Если Bolt недоступен, проверь `make neo4j-local-logs`.
- Если пароль менялся, обнови `DOMAIN_NEO4J_PASSWORD` в корневом `.env`.
- `make sync-domain-graph` не является проверкой логики: логику проверять только
  по JSON SSoT и validation layer.

## License note

Neo4j Community Server распространяется под GPL v3 и используется как отдельный
процесс через `bolt://localhost:7687`, без linking к исходному коду Neo4j
Server. Подробности см. в `THIRD_PARTY.md`.
