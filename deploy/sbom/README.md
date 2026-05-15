# SBOM (Software Bill of Materials)

## Файл

`sbom.cdx.json` — CycloneDX 1.6 SBOM для проекта Helicomponents.

## Что включено

### Auto-generated

- Python dependencies из `requirements.txt`
- Version pins из `constraints.txt`, где они применимы

### Manual entries

- `pyflamegpu` 2.0.0rc4+cuda130 (AGPL-3.0-only / Commercial dual)
- CUDA 13.0 Toolkit (NVIDIA EULA proprietary)
- Neo4j Community Server 5.x (Docker, GPL-3.0-only)
- Apache Superset (Docker, Apache-2.0)
- ClickHouse Server (managed, Apache-2.0)
- Cursor AI (SaaS, proprietary)

## Regenerate

```bash
pip install --user cyclonedx-bom
cyclonedx-py requirements requirements.txt --output-format JSON --output-file deploy/sbom/sbom.cdx.json --spec-version 1.6 --output-reproducible
```

Затем вручную merge system-level entries из `THIRD_PARTY.md` и применить pins
из `constraints.txt` для зависимостей, где они заданы.

## Связанные документы

- `THIRD_PARTY.md` — human-readable license inventory и AGPL section 13 analysis
- `SECURITY.md` — security model
- `requirements.txt` / `constraints.txt` — Python dependencies source
