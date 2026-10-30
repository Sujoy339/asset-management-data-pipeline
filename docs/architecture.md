# Architecture

The project follows a medallion architecture.

Sources:
- Security Master
- Transactions
- Positions
- Market Prices
- Restricted Securities

Bronze:
Raw source records and ingestion metadata.

Silver:
Schema validation, business validation,
deduplication and quarantine.

Gold:
Trusted portfolio positions and compliance exceptions.

Audit:
Run-level reconciliation, metrics and lineage.

Technology:
PySpark, Spark SQL and Parquet.