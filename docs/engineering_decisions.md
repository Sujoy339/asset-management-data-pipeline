
# Engineering Decision Record

## ADR-001: Asset Management Data Pipeline

### 1. Business Keys

- Transaction: transaction_id
- Position: portfolio_id + security_id + position_date
- Security: security_id
- Market Price: security_id + price_date
- Restriction: security_id + restriction_type + effective period

### 2. Idempotency

SHA-256 identifies identical deliveries.

Business transactions are deduplicated using transaction_id.
The highest valid correction_version supersedes earlier versions.

Positions are derived from opening snapshots and unique transaction
movements.

### 3. Late Arriving Data

Late transactions are identified by trade_date and processing date.

A late transaction should trigger a controlled replay of the
affected valuation date and downstream dates where applicable.

Late market prices should trigger a controlled revaluation.

Previously published runs must remain available for audit.

### 4. Schema Evolution

Unexpected columns or missing required columns cause ingestion failure.

Schema changes require an approved schema version.

Additive changes can be supported through an explicit compatibility
policy.

### 5. Security

Use separate service identities for ingestion, transformation
and reporting.

Secrets must be stored outside source code.

Apply encryption, least privilege and auditable access.

### 6. Monitoring

Monitor freshness, volume, quality, duplicate deliveries,
quarantine counts, execution failures and publication status.

### 7. Scaling

Move from local Spark to distributed Spark.

Adopt Delta Lake or Iceberg for transactional publication,
incremental processing and time travel.

Use partition pruning, broadcast joins and adaptive execution.

### 8. Recovery

Retry from persisted Bronze/Silver data.

Do not expose incomplete Gold datasets.

Retain run-specific outputs and publish through an atomic
manifest or transactional table commit.

### 9. Valuation

Market Value = Closing Quantity × Market Price.

Only same-currency, valid and sufficiently fresh prices
are eligible for valuation.

### 10. Compliance

A transaction executed during an active restriction period
creates a separate compliance exception.

Compliance exceptions do not automatically remove
the transaction from operational processing.
