#!/usr/bin/env python3
"""
Тест подключений к Neo4j графам.

Agent KG (локальный) — шина коммуникации агентов
Domain Graph (облачный Aura) — визуализация доменной модели
"""

import os
import sys

from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable, AuthError


def test_connection(name: str, uri: str, user: str, password: str, db: str) -> bool:
    """Проверяет подключение к Neo4j."""
    try:
        driver = GraphDatabase.driver(uri, auth=(user, password))
        with driver.session(database=db) as session:
            result = session.run("RETURN 1 as test")
            record = result.single()
            if record and record["test"] == 1:
                print(f"{name}: OK")
                driver.close()
                return True
        driver.close()
        print(f"{name}: FAIL (unexpected result)")
        return False
    except AuthError as e:
        print(f"{name}: FAIL (auth error: {e})")
        return False
    except ServiceUnavailable as e:
        print(f"{name}: FAIL (service unavailable: {e})")
        return False
    except Exception as e:
        print(f"{name}: FAIL ({type(e).__name__}: {e})")
        return False


def main() -> int:
    """Проверяет оба подключения."""
    results = []

    # Agent KG (локальный Neo4j)
    kg_uri = os.getenv("KG_NEO4J_URI")
    kg_user = os.getenv("KG_NEO4J_USER")
    kg_password = os.getenv("KG_NEO4J_PASSWORD")
    kg_db = os.getenv("KG_NEO4J_DB", "neo4j")

    if kg_uri and kg_user and kg_password:
        results.append(test_connection(
            f"Agent KG ({kg_uri})",
            kg_uri, kg_user, kg_password, kg_db
        ))
    else:
        print("Agent KG: SKIP (KG_NEO4J_* env vars not set)")
        results.append(None)

    # Domain Graph (облачный Neo4j Aura)
    domain_uri = os.getenv("DOMAIN_NEO4J_URI")
    domain_user = os.getenv("DOMAIN_NEO4J_USER")
    domain_password = os.getenv("DOMAIN_NEO4J_PASSWORD")
    domain_db = os.getenv("DOMAIN_NEO4J_DB", "neo4j")

    if domain_uri and domain_user and domain_password:
        results.append(test_connection(
            f"Domain Graph ({domain_uri})",
            domain_uri, domain_user, domain_password, domain_db
        ))
    else:
        print("Domain Graph: SKIP (DOMAIN_NEO4J_* env vars not set)")
        results.append(None)

    # Итог
    print()
    ok_count = sum(1 for r in results if r is True)
    fail_count = sum(1 for r in results if r is False)
    skip_count = sum(1 for r in results if r is None)

    print(f"Summary: {ok_count} OK, {fail_count} FAIL, {skip_count} SKIP")

    if fail_count > 0:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
