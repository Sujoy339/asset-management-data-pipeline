
import argparse
import csv
import os
from datetime import date, timedelta


def write_csv(path, rows, columns):
    os.makedirs(os.path.dirname(path), exist_ok=True)

    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def generate_data(output="data/input", scale=1):
    os.makedirs(output, exist_ok=True)

    security_count = 100 * scale
    transaction_count = 1000 * scale

    securities = [
        {
            "security_id": f"SEC{i:05d}",
            "isin": f"IN000000{i:05d}",
            "asset_class": "EQUITY",
            "currency": "USD",
            "effective_from": "2020-01-01",
            "effective_to": ""
        }
        for i in range(1, security_count + 1)
    ]

    prices = [
        {
            "security_id": f"SEC{i:05d}",
            "price_date": "2026-10-30",
            "price": 100 + i,
            "price_currency": "USD",
            "price_source": "SYNTHETIC"
        }
        for i in range(1, security_count + 1)
    ]

    positions = [
        {
            "portfolio_id": "PORT001",
            "security_id": f"SEC{i:05d}",
            "position_date": "2026-10-29",
            "quantity": 100,
            "book_value": 9000,
            "currency": "USD"
        }
        for i in range(1, security_count + 1)
    ]

    transactions = [
        {
            "transaction_id": f"TXN{i:08d}",
            "portfolio_id": "PORT001",
            "security_id": f"SEC{(i % security_count) + 1:05d}",
            "trade_date": "2026-10-30",
            "quantity": 10,
            "price": 100,
            "currency": "USD",
            "correction_version": 1
        }
        for i in range(1, transaction_count + 1)
    ]

    restrictions = [
        {
            "security_id": "SEC00001",
            "restriction_type": "COMPLIANCE",
            "effective_from": "2026-10-30",
            "effective_to": "2026-11-30",
            "status": "ACTIVE"
        }
    ]

    # Deliberate validation scenarios
    transactions[0]["security_id"] = "SEC00001"

    # Missing market price for one security
    prices = [
        p for p in prices
        if p["security_id"] != "SEC00002"
    ]

    write_csv(
        f"{output}/security_master.csv",
        securities,
        list(securities[0])
    )

    write_csv(
        f"{output}/transactions.csv",
        transactions,
        list(transactions[0])
    )

    write_csv(
        f"{output}/positions.csv",
        positions,
        list(positions[0])
    )

    write_csv(
        f"{output}/market_prices.csv",
        prices,
        list(prices[0])
    )

    write_csv(
        f"{output}/restricted_securities.csv",
        restrictions,
        list(restrictions[0])
    )

    print(
        f"Generated {security_count} securities, "
        f"{transaction_count} transactions"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="data/input")
    parser.add_argument("--scale", type=int, default=1)

    args = parser.parse_args()

    generate_data(args.output, args.scale)
