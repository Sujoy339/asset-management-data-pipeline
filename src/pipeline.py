# ============================================================
# ASSET MANAGEMENT DATA PIPELINE
# File: src/pipeline.py
# Framework: PySpark
# Architecture: Bronze -> Silver -> Gold
# ============================================================

import argparse
import hashlib
import json
import logging
import os
import time
import uuid

from datetime import datetime, timezone

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window


# ============================================================
# 1. CONFIGURATION
# ============================================================

DATASETS = {
    "security_master": [
        "security_id",
        "isin",
        "asset_class",
        "currency",
        "effective_from",
        "effective_to"
    ],

    "transactions": [
        "transaction_id",
        "portfolio_id",
        "security_id",
        "trade_date",
        "quantity",
        "price",
        "currency",
        "correction_version"
    ],

    "positions": [
        "portfolio_id",
        "security_id",
        "position_date",
        "quantity",
        "book_value",
        "currency"
    ],

    "market_prices": [
        "security_id",
        "price_date",
        "price",
        "price_currency",
        "price_source"
    ],
    "restricted_securities": [
        "security_id",
        "restriction_type",
        "effective_from",
        "effective_to",
        "status"
    ]
}

SQL_FILE = "src/sql/gold_transformations.sql"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

LOG = logging.getLogger("asset_management_pipeline")


# ============================================================
# 2. SPARK SESSION
# ============================================================

def create_spark():

    return (
        SparkSession.builder
        .appName("AssetManagementPipeline")
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


# ============================================================
# 3. UTILITY FUNCTIONS
# ============================================================

def calculate_checksum(file_path):

    digest = hashlib.sha256()

    with open(file_path, "rb") as file:

        for chunk in iter(
            lambda: file.read(1024 * 1024),
            b""
        ):

            digest.update(chunk)

    return digest.hexdigest()


def write_parquet(df, path):

    (
        df.write
        .mode("overwrite")
        .parquet(path)
    )


def save_json(data, path):

    os.makedirs(
        os.path.dirname(path),
        exist_ok=True
    )

    with open(path, "w") as file:

        json.dump(
            data,
            file,
            indent=4,
            default=str
        )


# ============================================================
# 4. BRONZE INGESTION
# ============================================================

def ingest_dataset(
    spark,
    input_path,
    dataset,
    run_id
):

    file_path = os.path.join(
        input_path,
        dataset + ".csv"
    )

    if not os.path.exists(file_path):

        raise FileNotFoundError(file_path)

    checksum = calculate_checksum(file_path)

    df = (
        spark.read
        .option("header", True)
        .option("mode", "FAILFAST")
        .csv(file_path)
    )

    expected_columns = DATASETS[dataset]

    actual_columns = df.columns

    missing = set(expected_columns) - set(actual_columns)

    unexpected = set(actual_columns) - set(expected_columns)

    if missing or unexpected:

        raise ValueError(
            f"Schema mismatch for {dataset}. "
            f"Missing={missing}, Unexpected={unexpected}"
        )

    df = df.select(*expected_columns)

    df = (
        df
        .withColumn(
            "source_file",
            F.lit(os.path.basename(file_path))
        )
        .withColumn(
            "file_checksum",
            F.lit(checksum)
        )
        .withColumn(
            "run_id",
            F.lit(run_id)
        )
        .withColumn(
            "ingested_at",
            F.current_timestamp()
        )
    )

    return df


# ============================================================
# 5. DATA TYPE NORMALIZATION
# ============================================================
def normalize_dataset(df, dataset):

    if dataset == "transactions":

        df = (
            df
            .withColumn(
                "trade_date",
                F.to_date("trade_date", "yyyy-MM-dd")
            )
            .withColumn(
                "quantity",
                F.col("quantity").cast("double")
            )
            .withColumn(
                "price",
                F.col("price").cast("double")
            )
            .withColumn(
                "correction_version",
                F.col("correction_version").cast("integer")
            )
        )

    elif dataset == "positions":

        df = (
            df
            .withColumn(
                "position_date",
                F.to_date("position_date", "yyyy-MM-dd")
            )
            .withColumn(
                "quantity",
                F.col("quantity").cast("double")
            )
            .withColumn(
                "book_value",
                F.col("book_value").cast("double")
            )
        )

    elif dataset == "market_prices":

        df = (
            df
            .withColumn(
                "price_date",
                F.to_date("price_date", "yyyy-MM-dd")
            )
            .withColumn(
                "price",
                F.col("price").cast("double")
            )
        )

    elif dataset == "security_master":

        df = (
            df
            .withColumn(
                "effective_from",
                F.to_date("effective_from", "yyyy-MM-dd")
            )
            .withColumn(
                "effective_to",
                F.to_date("effective_to", "yyyy-MM-dd")
            )
        )

    elif dataset == "restricted_securities":

        df = (
            df
            .withColumn(
                "effective_from",
                F.to_date("effective_from", "yyyy-MM-dd")
            )
            .withColumn(
                "effective_to",
                F.to_date("effective_to", "yyyy-MM-dd")
            )
        )

    return df


# ============================================================
# 6. DATA QUALITY VALIDATIONS
# ============================================================

def validate_dataset(df, dataset):

    reason = F.lit(None).cast("string")

    if dataset == "transactions":

        reason = (
            F.when(
                F.col("transaction_id").isNull() |
                (F.trim(F.col("transaction_id")) == ""),
                "MISSING_TRANSACTION_ID"
            )
            .when(
                F.col("portfolio_id").isNull(),
                "MISSING_PORTFOLIO_ID"
            )
            .when(
                F.col("security_id").isNull(),
                "MISSING_SECURITY_ID"
            )
            .when(
                F.col("trade_date").isNull(),
                "INVALID_TRADE_DATE"
            )
            .when(
                F.col("quantity").isNull() |
                (F.col("quantity") == 0),
                "INVALID_QUANTITY"
            )

            .when(
                F.col("price").isNull() |
                (F.col("price") <= 0),
                "INVALID_TRANSACTION_PRICE"
            )
            .when(
                F.col("currency").isNull(),
                "MISSING_CURRENCY"
            )
            .when(
                F.col("correction_version").isNull() |
                (F.col("correction_version") < 1),
                "INVALID_CORRECTION_VERSION"
            )
        )

    elif dataset == "positions":

        reason = (
            F.when(
                F.col("portfolio_id").isNull(),
                "MISSING_PORTFOLIO_ID"
            )
            .when(
                F.col("security_id").isNull(),
                "MISSING_SECURITY_ID"
            )
            .when(
                F.col("position_date").isNull(),
                "INVALID_POSITION_DATE"
            )
            .when(
                F.col("quantity").isNull(),
                "INVALID_POSITION_QUANTITY"
            )
            .when(
                F.col("book_value").isNull(),
                "INVALID_BOOK_VALUE"
            )
        )

    elif dataset == "market_prices":

        reason = (
            F.when(
                F.col("security_id").isNull(),
                "MISSING_SECURITY_ID"
            )
            .when(
                F.col("price_date").isNull(),
                "INVALID_PRICE_DATE"
            )
            .when(
                F.col("price").isNull() |
                (F.col("price") <= 0),
                "INVALID_MARKET_PRICE"
            )
            .when(
                F.col("price_currency").isNull(),
                "MISSING_PRICE_CURRENCY"
            )
        )

    elif dataset == "security_master":

        reason = (
            F.when(
                F.col("security_id").isNull(),
                "MISSING_SECURITY_ID"
            )
            .when(
                F.col("effective_from").isNull(),
                "INVALID_EFFECTIVE_FROM"
            )
            .when(
                F.col("effective_to").isNotNull() &
                (
                    F.col("effective_to") <
                    F.col("effective_from")
                ),
                "INVALID_EFFECTIVE_PERIOD"
            )
        )

    elif dataset == "restricted_securities":

        reason = (
            F.when(
                F.col("security_id").isNull(),
                "MISSING_SECURITY_ID"
            )
            .when(
                F.col("effective_from").isNull(),
                "INVALID_EFFECTIVE_FROM"
            )
            .when(
                F.col("effective_to").isNotNull() &
                (
                    F.col("effective_to") <
                    F.col("effective_from")
                ),
                "INVALID_EFFECTIVE_PERIOD"
            )
            .when(
                F.col("status").isNull(),
                "MISSING_RESTRICTION_STATUS"
            )
        )

    return df.withColumn(
        "rejection_reason",
        reason
    )


# ============================================================
# 7. TRANSACTION DEDUPLICATION
# ============================================================

def classify_transactions(df):

    """
    Business key: transaction_id.

    Identical transaction versions are duplicates.
    Highest correction_version wins.
    Conflicting versions are quarantined.
    """

    version_window = Window.partitionBy(
        "transaction_id",
        "correction_version"
    )

    df = df.withColumn(
        "_record_hash",
        F.sha2(
            F.concat_ws(
                "||",
                F.coalesce(F.col("portfolio_id"), F.lit("")),
                F.coalesce(F.col("security_id"), F.lit("")),
                F.coalesce(
                    F.col("trade_date").cast("string"),
                    F.lit("")
                ),
                F.coalesce(
                    F.col("quantity").cast("string"),
                    F.lit("")
                ),
                F.coalesce(
                    F.col("price").cast("string"),
                    F.lit("")
                ),
                F.coalesce(F.col("currency"), F.lit(""))
            ),
            256
        )
    )

    df = df.withColumn(
        "_distinct_versions",
        F.size(
            F.collect_set("_record_hash").over(version_window)
        )
    )

    conflicts = (
        df
        .filter(F.col("_distinct_versions") > 1)
        .withColumn(
            "rejection_reason",
            F.lit("CONFLICTING_TRANSACTION_VERSION")
        )
    )

    clean = df.filter(
        F.col("_distinct_versions") == 1
    )

    unique_versions = clean.dropDuplicates(
        ["transaction_id", "correction_version"]
    )

    correction_window = (
        Window
        .partitionBy("transaction_id")
        .orderBy(
            F.col("correction_version").desc()
        )
    )

    winners = (
        unique_versions
        .withColumn(
            "_rn",
            F.row_number().over(correction_window)
        )
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # Includes exact duplicates and superseded correction versions.
    duplicate_count = (
        clean.count() -
        winners.count()
    )

    return winners, conflicts, duplicate_count


# ============================================================
# 8. SILVER PROCESSING
# ============================================================

def process_silver(
    bronze,
    output,
    run_id
):

    silver = {}
    metrics = {}

    for dataset, raw in bronze.items():

        normalized = normalize_dataset(
            raw,
            dataset
        )

        validated = validate_dataset(
            normalized,
            dataset
        )

        valid = validated.filter(
            F.col("rejection_reason").isNull()
        )

        rejected = validated.filter(
            F.col("rejection_reason").isNotNull()
        )

        duplicate_count = 0

        if dataset == "transactions":

            winners, conflicts, duplicate_count = (
                classify_transactions(valid)
            )

            rejected = rejected.unionByName(
                conflicts,
                allowMissingColumns=True
            )

            valid = winners

        received_count = raw.count()

        accepted_count = valid.count()

        rejected_count = rejected.count()

        metrics[dataset] = {
            "received": received_count,
            "accepted": accepted_count,
            "rejected": rejected_count,
            "duplicates": duplicate_count
        }

        silver[dataset] = valid

        write_parquet(
            valid,
            os.path.join(
                output,
                "silver",
                run_id,
                dataset
            )
        )

        if rejected.limit(1).count() > 0:

            rejected = rejected.withColumn(
                "dataset",
                F.lit(dataset)
            )

            write_parquet(
                rejected,
                os.path.join(
                    output,
                    "quarantine",
                    run_id,
                    dataset
                )
            )

        if (
            received_count !=
            accepted_count +
            rejected_count +
            duplicate_count
        ):

            raise RuntimeError(
                f"Reconciliation failed for {dataset}"
            )

    return silver, metrics


# ============================================================
# 9. REFERENCE DATA VALIDATION
# ============================================================

def validate_reference_data(silver):

    security = silver["security_master"]

    window = (
        Window
        .partitionBy("security_id")
        .orderBy("effective_from")
    )

    security = security.withColumn(
        "_next_effective_from",
        F.lead("effective_from").over(window)
    )

    overlapping = security.filter(
        F.col("_next_effective_from").isNotNull() &
        (
            F.col("effective_to").isNull() |
            (
                F.col("effective_to") >=
                F.col("_next_effective_from")
            )
        )
    )

    if overlapping.limit(1).count() > 0:

        raise ValueError(
            "Overlapping Security Master effective periods"
        )

    duplicate_security = (
        security
        .groupBy("security_id", "effective_from")
        .count()
        .filter("count > 1")
    )

    if duplicate_security.limit(1).count() > 0:

        raise ValueError(
            "Duplicate Security Master business keys"
        )

    duplicate_positions = (
        silver["positions"]
        .groupBy(
            "portfolio_id",
            "security_id",
            "position_date"
        )
        .count()
        .filter("count > 1")
    )

    if duplicate_positions.limit(1).count() > 0:

        raise ValueError(
            "Duplicate position snapshot business keys"
        )

    duplicate_prices = (
        silver["market_prices"]
        .groupBy(
            "security_id",
            "price_date",
            "price_currency"
        )
        .count()
        .filter("count > 1")
    )

    if duplicate_prices.limit(1).count() > 0:

        raise ValueError(
            "Duplicate market price business keys"
        )


# ============================================================
# 10. EXECUTE GOLD SQL
# ============================================================

def execute_gold_sql(
    spark,
    silver,
    valuation_date,
    max_price_age_days
):

    for name, df in silver.items():

        if (
            name == "transactions"
            and "rejection_reason" in df.columns
        ):
            df = df.drop("rejection_reason")

        df.createOrReplaceTempView(name)


    with open(SQL_FILE, "r") as file:

        sql_text = file.read()

    sql_text = (
        sql_text
        .replace(
            "${valuation_date}",
            valuation_date
        )
        .replace(
            "${max_price_age_days}",
            str(max_price_age_days)
        )
    )


    # ----------------------------------------------------
    # SPLIT AND CLEAN SQL STATEMENTS
    # ----------------------------------------------------

    statements = []

    for raw_statement in sql_text.split(";"):

        # Remove full-line SQL comments
        cleaned_lines = [
            line
            for line in raw_statement.splitlines()
            if line.strip()
            and not line.lstrip().startswith("--")
        ]

        statement = "\n".join(cleaned_lines).strip()

        # Ignore empty or comment-only statements
        if statement:
            statements.append(statement)

    # ----------------------------------------------------
    # EXECUTE GOLD SQL
    # ----------------------------------------------------

    for index, statement in enumerate(statements, start=1):

        try:
            spark.sql(statement)

        except Exception:
            LOG.exception(
                "Gold SQL statement %s failed:\n%s",
                index,
                statement
            )
            raise


    gold = {
        "portfolio_positions":
            spark.table("trusted_positions"),

        "compliance_exceptions":
            spark.table("compliance_exceptions"),

        "position_quarantine":
            spark.table("position_quarantine"),

        "transaction_quarantine":
            spark.table("transaction_security_quarantine"),

        "portfolio_reconciliation":
            spark.table("portfolio_reconciliation")
    }

    return gold


# ============================================================
# 11. RUN-LEVEL RECONCILIATION
# ============================================================

def build_reconciliation(
    metrics,
    gold,
    run_id
):

    result = []

    for dataset, counts in metrics.items():

        received = counts["received"]

        accepted = counts["accepted"]

        rejected = counts["rejected"]

        duplicates = counts["duplicates"]

        result.append({
            "run_id": run_id,
            "dataset": dataset,
            "records_received": received,
            "records_accepted": accepted,
            "records_rejected": rejected,
            "records_duplicates": duplicates,
            "reconciliation_difference": (
                received -
                accepted -
                rejected -
                duplicates
            )
        })

    positions = gold["portfolio_positions"].count()

    position_quarantine = (
        gold["position_quarantine"].count()
    )

    exceptions = gold["compliance_exceptions"].count()

    result.extend([
        {
            "run_id": run_id,
            "dataset": "gold_positions",
            "records_received": positions + position_quarantine,
            "records_accepted": positions,
            "records_rejected": position_quarantine,
            "records_duplicates": 0,
            "reconciliation_difference": 0
        },
        {
            "run_id": run_id,
            "dataset": "compliance_exceptions",
            "records_received": exceptions,
            "records_accepted": exceptions,
            "records_rejected": 0,
            "records_duplicates": 0,
            "reconciliation_difference": 0
        }
    ])

    return result


# ============================================================
# 12. STAGED GOLD PUBLICATION
# ============================================================

def publish_gold(
    gold,
    output,
    run_id
):

    gold_root = os.path.join(
        output,
        "gold"
    )

    runs_root = os.path.join(
        gold_root,
        "runs"
    )

    run_root = os.path.join(
        runs_root,
        run_id
    )

    staging_root = os.path.join(
        output,
        "_staging",
        run_id
    )

    os.makedirs(staging_root, exist_ok=True)

    os.makedirs(runs_root, exist_ok=True)

    if os.path.exists(run_root):

        raise FileExistsError(run_root)

    for name, df in gold.items():

        write_parquet(
            df,
            os.path.join(
                staging_root,
                name
            )
        )

    for name in gold:

        path = os.path.join(
            staging_root,
            name
        )

        if not os.path.exists(path):

            raise RuntimeError(
                f"Staged output missing: {name}"
            )

    manifest = {
        "run_id": run_id,
        "published_at": datetime.now(
            timezone.utc
        ).isoformat(),
        "outputs": {
            name: f"gold/runs/{run_id}/{name}"
            for name in gold
        }
    }

    save_json(
        manifest,
        os.path.join(
            staging_root,
            "manifest.json"
        )
    )

    os.rename(
        staging_root,
        run_root
    )

    pointer = os.path.join(
        gold_root,
        "current.json"
    )

    temporary_pointer = (
        pointer + "." + run_id + ".tmp"
    )

    save_json(
        manifest,
        temporary_pointer
    )

    os.replace(
        temporary_pointer,
        pointer
    )

    LOG.info(
        "Gold successfully published: %s",
        run_id
    )


# ============================================================
# 13. MAIN PIPELINE ORCHESTRATION
# ============================================================

def run_pipeline(
    input_path,
    output,
    valuation_date,
    max_price_age_days=0,
    fail_after_silver=False
):

    # ----------------------------------------------------
    # VALIDATE INPUT PARAMETERS
    # ----------------------------------------------------

    try:
        parsed_date = datetime.strptime(
            valuation_date,
            "%Y-%m-%d"
        )

        if parsed_date.strftime("%Y-%m-%d") != valuation_date:
            raise ValueError

    except ValueError:
        raise ValueError(
            "Invalid valuation date. Expected YYYY-MM-DD."
        )

    if (
        not isinstance(max_price_age_days, int)
        or isinstance(max_price_age_days, bool)
        or max_price_age_days < 0
    ):
        raise ValueError(
            "max_price_age_days must be a non-negative integer."
        )

    # ----------------------------------------------------
    # START PIPELINE
    # ----------------------------------------------------

    start = time.time()

    run_id = str(uuid.uuid4())

    LOG.info(
        "Pipeline started. Run ID=%s",
        run_id
    )

    spark = create_spark()

    try:

        # ----------------------------------------------------
        # BRONZE
        # ----------------------------------------------------

        bronze = {}

        for dataset in DATASETS:

            df = ingest_dataset(
                spark,
                input_path,
                dataset,
                run_id
            )

            bronze[dataset] = df

            write_parquet(
                df,
                os.path.join(
                    output,
                    "bronze",
                    run_id,
                    dataset
                )
            )

        LOG.info("Bronze ingestion completed")

        # ----------------------------------------------------
        # SILVER
        # ----------------------------------------------------

        silver, metrics = process_silver(
            bronze,
            output,
            run_id
        )

        validate_reference_data(silver)

        LOG.info("Silver validation completed")

        # ----------------------------------------------------
        # FAILURE INJECTION
        # ----------------------------------------------------

        if fail_after_silver:

            raise RuntimeError(
                "INJECTED_FAILURE_AFTER_SILVER"
            )

        # ----------------------------------------------------
        # GOLD
        # ----------------------------------------------------

        gold = execute_gold_sql(
            spark,
            silver,
            valuation_date,
            max_price_age_days
        )

        LOG.info("Gold transformations completed")

        # ----------------------------------------------------
        # RECONCILIATION
        # ----------------------------------------------------

        reconciliation = build_reconciliation(
            metrics,
            gold,
            run_id
        )

        for row in reconciliation:

            if row["reconciliation_difference"] != 0:

                raise RuntimeError(
                    f"Reconciliation failed: {row}"
                )

        save_json(
            reconciliation,
            os.path.join(
                output,
                "audit",
                run_id,
                "reconciliation.json"
            )
        )

        # ----------------------------------------------------
        # OPERATIONAL METRICS
        # ----------------------------------------------------

        duration = time.time() - start

        operational_metrics = {
            "run_id": run_id,
            "status": "SUCCESS",
            "valuation_date": valuation_date,
            "duration_seconds": round(duration, 2),
            "trusted_positions":
                gold["portfolio_positions"].count(),
            "quarantined_positions":
                gold["position_quarantine"].count(),
            "compliance_exceptions":
                gold["compliance_exceptions"].count()
        }

        save_json(
            operational_metrics,
            os.path.join(
                output,
                "audit",
                run_id,
                "operational_metrics.json"
            )
        )

        # ----------------------------------------------------
        # PUBLISH GOLD
        # ----------------------------------------------------

        publish_gold(
            gold,
            output,
            run_id
        )

        # ----------------------------------------------------
        # DISPLAY RESULTS
        # ----------------------------------------------------

        print("\n========== RECONCILIATION ==========")

        print(
            json.dumps(
                reconciliation,
                indent=4
            )
        )

        print("\n========== PORTFOLIO POSITIONS ==========")

        gold["portfolio_positions"].show(
            20,
            truncate=False
        )

        print("\n========== COMPLIANCE EXCEPTIONS ==========")

        gold["compliance_exceptions"].show(
            truncate=False
        )

        print("\n========== POSITION QUARANTINE ==========")

        gold["position_quarantine"].show(
            truncate=False
        )

        print("\n========== TRANSACTION QUARANTINE ==========")

        gold["transaction_quarantine"].show(
            truncate=False
        )

        print(f"\nSUCCESS: RUN_ID={run_id}")

        print(
            f"DURATION_SECONDS={duration:.2f}"
        )

    except Exception:

        LOG.exception(
            "Pipeline failed. "
            "Previous Gold publication remains unchanged."
        )

        raise

    finally:

        spark.stop()


# ============================================================
# 14. COMMAND LINE INTERFACE
# ============================================================

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Asset Management PySpark Pipeline"
    )

    parser.add_argument(
        "--input",
        default="data/input"
    )

    parser.add_argument(
        "--output",
        default="output"
    )

    parser.add_argument(
        "--valuation-date",
        default="2026-10-30"
    )

    parser.add_argument(
        "--max-price-age-days",
        type=int,
        default=0
    )

    parser.add_argument(
        "--fail-after-silver",
        action="store_true"
    )

    args = parser.parse_args()

    if args.max_price_age_days < 0:

        parser.error(
            "--max-price-age-days cannot be negative"
        )

    run_pipeline(
        input_path=args.input,
        output=args.output,
        valuation_date=args.valuation_date,
        max_price_age_days=args.max_price_age_days,
        fail_after_silver=args.fail_after_silver
    )
    