
import argparse
import json
import os

from pyspark.sql import SparkSession


def main():

    parser = argparse.ArgumentParser(
        description="Inspect Asset Management Pipeline Outputs"
    )

    parser.add_argument(
        "--output",
        default="output"
    )

    parser.add_argument(
        "--layer",
        choices=["bronze", "silver", "gold"],
        required=True
    )

    parser.add_argument(
        "--dataset",
        required=True
    )

    parser.add_argument(
        "--run-id",
        default=None
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=20
    )

    args = parser.parse_args()

    if args.limit < 1:
        parser.error("--limit must be positive")

    manifest = None

    if args.layer == "gold":

        current_file = os.path.join(
            args.output,
            "gold",
            "current.json"
        )

        with open(current_file) as file:
            manifest = json.load(file)

        run_id = args.run_id or manifest["run_id"]

        if run_id not in [
            os.path.basename(path)
            for path in os.listdir(
                os.path.join(args.output, "gold", "runs")
            )
        ]:
            raise ValueError(
                f"Gold run not found: {run_id}"
            )

        dataset_path = os.path.join(
            args.output,
            "gold",
            "runs",
            run_id,
            args.dataset
        )

    else:

        if not args.run_id:
            parser.error(
                "--run-id is required for Bronze and Silver"
            )

        dataset_path = os.path.join(
            args.output,
            args.layer,
            args.run_id,
            args.dataset
        )

    if not os.path.exists(dataset_path):
        raise FileNotFoundError(dataset_path)

    spark = (
        SparkSession.builder
        .appName("AssetManagementOutputInspector")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    try:

        df = spark.read.parquet(dataset_path)

        print("\n===================================")
        print("PIPELINE DATA INSPECTION")
        print("===================================")

        print("Layer:", args.layer)
        print("Dataset:", args.dataset)
        print("Path:", dataset_path)

        print("\nSCHEMA")

        df.printSchema()

        print("\nRECORD COUNT")

        print(df.count())

        print("\nDATA PREVIEW")

        df.show(
            args.limit,
            truncate=False
        )

    finally:

        spark.stop()


if __name__ == "__main__":
    main()
