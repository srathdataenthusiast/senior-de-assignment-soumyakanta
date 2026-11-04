from datetime import date
from decimal import Decimal
from kpi_assessment.storage.delta_writer import write_daily_account_summary

from kpi_assessment.transformations.daily_account_summary import build_daily_account_summary


def test_daily_account_summary_calculations(spark):

    columns = [
        "transaction_id",
        "account_id",
        "transaction_date",
        "amount",
        "currency",
        "transaction_type",
        "merchant_name",
        "merchant_category",
        "status",
        "country_code",
        "_is_duplicate",
    ]

    rows = [
        ("TXN-001", "ACC-1001", "2024-01-01T10:00:00Z",
         "100.00", "USD", "debit", "Amazon", "e-commerce",
         "completed", "US", False),

        ("TXN-002", "ACC-1001", "2024-01-01T11:00:00Z",
         "50.00", "USD", "credit", "Payroll", "payroll",
         "completed", "US", False),

        ("TXN-003", "ACC-1001", "2024-01-01T12:00:00Z",
         "20.00", "EUR", "debit", "Zara", "retail",
         "completed", "DE", False),

        # Pending transaction must be excluded.
        ("TXN-004", "ACC-1001", "2024-01-01T13:00:00Z",
         "999.00", "USD", "debit", "Shop", "groceries",
         "pending", "US", False),

        # Duplicate copy must also be excluded.
        ("TXN-005", "ACC-1001", "2024-01-01T14:00:00Z",
         "100.00", "USD", "debit", "Amazon", "e-commerce",
         "completed", "US", True),
    ]

    transactions = spark.createDataFrame(rows, columns)

    summary = build_daily_account_summary(transactions)

    results = summary.collect()

    # One account and one date should produce one summary row.
    assert len(results) == 1

    result = results[0]

    assert result["account_id"] == "ACC-1001"
    assert result["transaction_date"] == date(2024, 1, 1)

    # Verify currency-specific financial calculations.
    assert result["total_debit_amount"] == {
        "USD": Decimal("100.00"),
        "EUR": Decimal("20.00"),
    }

    assert result["total_credit_amount"] == {
        "USD": Decimal("50.00"),
        "EUR": Decimal("0.00"),
    }

    assert result["net_amount"] == {
        "USD": Decimal("-50.00"),
        "EUR": Decimal("-20.00"),
    }

    assert result["transaction_count"] == 3
    assert result["distinct_merchants"] == 3

    assert result["currencies"] == ["EUR", "USD"]

    assert result["top_category"] == {
        "USD": "e-commerce",
        "EUR": "retail",
    }

    assert result["updated_at"] is not None

def test_daily_summary_upsert_is_idempotent(spark, tmp_path):

    output_path = str(tmp_path / "daily_account_summary")

    columns = [
        "transaction_id",
        "account_id",
        "transaction_date",
        "amount",
        "currency",
        "transaction_type",
        "merchant_name",
        "merchant_category",
        "status",
        "country_code",
        "_is_duplicate",
    ]

    rows = [
        ("TXN-001", "ACC-1001", "2024-01-01T10:00:00Z",
         "100.00", "USD", "debit", "Amazon", "e-commerce",
         "completed", "US", False),

        ("TXN-002", "ACC-1001", "2024-01-01T11:00:00Z",
         "50.00", "USD", "credit", "Payroll", "payroll",
         "completed", "US", False),
    ]

    transactions = spark.createDataFrame(rows, columns)
    summary = build_daily_account_summary(transactions)

    # First run creates the Delta table.
    write_daily_account_summary(summary, output_path)

    first_run = spark.read.format("delta").load(output_path)
    first_timestamp = first_run.first()["updated_at"]

    # Second run with unchanged data must not create another row.
    write_daily_account_summary(summary, output_path)

    second_run = spark.read.format("delta").load(output_path)

    assert second_run.count() == 1
    assert second_run.first()["updated_at"] == first_timestamp

    # Change the debit amount to simulate changed source data.
    changed_rows = [
        ("TXN-001", "ACC-1001", "2024-01-01T10:00:00Z",
         "125.00", "USD", "debit", "Amazon", "e-commerce",
         "completed", "US", False),

        ("TXN-002", "ACC-1001", "2024-01-01T11:00:00Z",
         "50.00", "USD", "credit", "Payroll", "payroll",
         "completed", "US", False),
    ]

    changed_transactions = spark.createDataFrame(changed_rows, columns)
    changed_summary = build_daily_account_summary(changed_transactions)

    # Third run must update the existing summary row.
    write_daily_account_summary(changed_summary, output_path)

    updated_run = spark.read.format("delta").load(output_path)

    assert updated_run.count() == 1
    assert updated_run.first()["total_debit_amount"]["USD"] == Decimal("125.00")