#!/usr/bin/env python3
"""
Тест подключения к Domain Graph (Neo4j).

SSoT — JSON-файлы в репозитории (config/transitions/*.json).
Default: Neo4j Community local (Docker) — см. deploy/neo4j-local/.
Aura/любой Neo4j Server поддерживается через DOMAIN_NEO4J_URI.
"""

import os
import sys

from neo4j import GraphDatabase
from neo4j.exceptions import AuthError, ServiceUnavailable


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
    """Проверяет подключение к Domain Graph."""
    domain_uri = os.getenv("DOMAIN_NEO4J_URI")
    domain_user = os.getenv("DOMAIN_NEO4J_USER")
    domain_password = os.getenv("DOMAIN_NEO4J_PASSWORD")
    domain_db = os.getenv("DOMAIN_NEO4J_DB", "neo4j")

    if domain_uri and domain_user and domain_password:
        ok = test_connection(
            f"Domain Graph ({domain_uri})",
            domain_uri, domain_user, domain_password, domain_db
        )
        if not ok:
            return 1
        return 0
    else:
        print("Domain Graph: SKIP (DOMAIN_NEO4J_* env vars not set)")
        return 0


if __name__ == "__main__":
    sys.exit(main())
