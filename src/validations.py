
from pyspark.sql import functions as F


def add_validation_columns(df, dataset):
    """
    Technical validation.
    Dataset-specific business checks are applied separately.
    """

    if dataset == "transactions":
        df = df.withColumn(
            "rejection_reason",
            F.when(
                F.col("transaction_id").isNull() |
                (F.trim("transaction_id") == ""),
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
                F.col("trade_date").isNull(),
                "INVALID_TRADE_DATE"
            )
        )

    elif dataset == "positions":
        df = df.withColumn(
            "rejection_reason",
            F.when(
                F.col("portfolio_id").isNull(),
                "MISSING_PORTFOLIO_ID"
            )
            .when(
                F.col("security_id").isNull(),
                "MISSING_SECURITY_ID"
            )
            .when(
                F.col("quantity").isNull(),
                "INVALID_POSITION_QUANTITY"
            )
            .when(
                F.col("position_date").isNull(),
                "INVALID_POSITION_DATE"
            )
        )

    elif dataset == "market_prices":
        df = df.withColumn(
            "rejection_reason",
            F.when(
                F.col("security_id").isNull(),
                "MISSING_SECURITY_ID"
            )
            .when(
                F.col("price").isNull() |
                (F.col("price") <= 0),
                "INVALID_MARKET_PRICE"
            )
            .when(
                F.col("price_date").isNull(),
                "INVALID_PRICE_DATE"
            )
        )

    else:
        df = df.withColumn(
            "rejection_reason",
            F.lit(None).cast("string")
        )

    return df


def split_valid_invalid(df):
    valid = df.filter(F.col("rejection_reason").isNull())
    invalid = df.filter(F.col("rejection_reason").isNotNull())

    return valid, invalid


def deduplicate_transactions(df):
    from pyspark.sql.window import Window

    window = Window.partitionBy(
        "transaction_id"
    ).orderBy(
        F.col("correction_version").desc(),
        F.col("ingested_at").desc()
    )

    return (
        df.withColumn("_row_number", F.row_number().over(window))
          .filter(F.col("_row_number") == 1)
          .drop("_row_number")
    )
