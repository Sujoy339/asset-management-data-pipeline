
# Asset Management Data Pipeline

PySpark and Spark SQL prototype for ingesting, validating,
reconciling and publishing asset-management data.

## Features

- Five source datasets
- Bronze/Silver/Gold medallion architecture
- Technical and business data validation
- Rejected and quarantined records
- Effective-dated security validation
- Transaction deduplication
- Position roll-forward
- Portfolio market valuation
- Restricted-security compliance exceptions
- Run-level reconciliation
- Record-level lineage
- Failure injection and replay
- Synthetic data generation

## Prerequisites

- Python 3.10 or later
- Java 17
- Apache Spark 3.5.x
- PySpark 3.5.5

## Setup

Create a virtual environment:

    python -m venv .venv

Activate it:

    .\.venv\Scripts\Activate.ps1

Install dependencies:

    pip install -r requirements.txt

Generate synthetic data:

    python src/generate_data.py --scale 1

## Execute pipeline

    python src/pipeline.py

Specify valuation date:

    python src/pipeline.py \
      --valuation-date 2026-10-30

## Failure injection

    python src/pipeline.py --fail-after-silver

Expected:
- Bronze completed
- Silver completed
- Controlled exception raised
- Gold not published by the failed execution

Restart:

    python src/pipeline.py

## Tests

    pytest -v tests/

## Volume tests

    python src/generate_data.py --scale 10
    python src/pipeline.py

    python src/generate_data.py --scale 100
    python src/pipeline.py

## Output structure

output/
    bronze/
    silver/
    quarantine/
    gold/
    audit/
    _staging/

## Gold outputs

- portfolio_positions
- compliance_exceptions
- position_quarantine
- transaction_quarantine

## Business rules

Market value = Closing quantity * Market price.

Closing quantity =
Opening quantity + Net transaction quantity.

Transactions are checked against effective-dated
Security Master records.

Compliance exceptions are generated when a transaction
occurs during an active restriction.

Missing prices prevent affected positions from being
published as trusted valuations.

## Assumptions

- Position input is an opening snapshot.
- Transaction quantities are signed.
- Currency conversion is not implemented.
- Price must match valuation date.
- Synthetic data only.

## Production limitations

This is a local prototype, not a production deployment.

For production, introduce:
- Delta Lake or Iceberg
- Cloud object storage
- Atomic multi-table publication
- Centralized secrets
- IAM/RBAC
- Airflow orchestration
- Centralized monitoring
- Integration and performance tests
