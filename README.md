# Assignment 3: Data Warehouse & Polyglot Persistence

Jinwoo Choi · Cloud-Native Data Engineering · Domain: Kitchen-Aware Restaurant Ranking (continued from Assignments 1 and 2)

## Deliverables

| Deliverable | File(s) |
|---|---|
| Written report | [`report/report.pdf`](report/report.pdf) |
| Pipeline and transformation code | `ingest_api.py` (live API ingestion: pagination, rate limiting, quality checks), `pipeline.py` (Bronze/Silver/Gold, PII masking, warehouse loading) |
| Warehouse SQL script | `warehouse.sql`: Bronze/Silver/Gold DDL, star schema DDL, ELT loads, SCD2 change, Part 2 analytical queries (with `WITH ROLLUP`), the optimization, and the Part 4 SQL queries |
| NoSQL code (Neo4j, Option C) | `graph.cypher`: constraints, load statements and all Cypher queries, including Part 4. `neo4j_graph.py` runs them against Aura |
| Supporting | `run_sql.py` (runs one section of `warehouse.sql`), `results/` (saved query output), `diagrams/` (star schema, graph model, and the zone weather/traffic map as PNG and interactive HTML), `requirements.txt` |

## How to run

```bash
pip install -r requirements.txt neo4j
python ingest_api.py                     # Part 1a
python pipeline.py                       # Part 1b (needs RDS_HOST, RDS_USER, PII_SALT. Password is prompted)
python run_sql.py STAR                   # Part 2: then CHECKS, ANALYTICS, PROFILE, OPTIMIZE, SHOWCASE
python neo4j_graph.py load               # Part 3: then part3, showcase (needs NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD)
```

**Credential hygiene:** no passwords, endpoints or keys are stored in any file. All connection details come from environment variables or a run-time prompt, and identifying details are masked in the screenshots.
