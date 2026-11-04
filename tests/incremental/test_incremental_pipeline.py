"""
Integration test for incremental ingestion and watermark management.
"""

from pathlib import Path

from pyspark.sql import functions as F

from kpi_assessment.ingestion.csv_source import read_transactions_csv
from kpi_assessment.ingestion.pipeline import run_ingestion_pipeline
from kpi_assessment.incremental.watermark import (
    get_watermark,
    get_extraction_start_date,
    save_watermark,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CSV_PATH = PROJECT_ROOT / "data/backup/transactions.csv"
SCHEMA_PATH = PROJECT_ROOT / "data/backup/transactions_schema.json"


def test_incremental_ingestion_two_runs(spark, tmp_path):

    bronze_path = str(tmp_path / "bronze")
    quarantine_path = str(tmp_path / "quarantine")
    watermark_path = str(tmp_path / "watermark")

    # Load the supplied backup CSV for the first ingestion run.
    raw_transactions = read_transactions_csv(
        spark=spark,
        csv_path=CSV_PATH,
        schema_path=SCHEMA_PATH,
        batch_id="batch-001",
    )

    # First run processes the complete dataset.
    first_result = run_ingestion_pipeline(
        raw_transactions,
        str(SCHEMA_PATH),
        bronze_path,
        quarantine_path,
    )

    save_watermark(
        spark,
        watermark_path,
        "transactions",
        first_result["max_transaction_date"],
        "batch-001",
    )

    first_bronze_count = (
        spark.read.format("delta").load(bronze_path).count()
    )

    first_quarantine_count = (
        spark.read.format("delta").load(quarantine_path).count()
    )

    first_watermark = get_watermark(
        spark,
        watermark_path,
        "transactions",
    )

    assert first_bronze_count == first_result["valid_count"]
    assert first_quarantine_count == first_result["invalid_count"]
    assert first_watermark == first_result["max_transaction_date"]

    # Calculate the next extraction window using the saved watermark.
    extraction_start = get_extraction_start_date(first_watermark)

    # Simulate the API date filter using the configured lookback window.
    # Invalid timestamps return NULL and are excluded from this extraction.
    parsed_transaction_date = F.try_to_timestamp(
        F.col("transaction_date"),
        F.lit("yyyy-MM-dd'T'HH:mm:ssX"),
    )

    lookback_records = raw_transactions.filter(
        parsed_transaction_date
        >= F.to_timestamp(
            F.lit(extraction_start),
            "yyyy-MM-dd'T'HH:mm:ssX",
        )
    )

    # Add one new valid transaction to simulate newly arrived data.
    new_record = (
        raw_transactions.limit(1)
        .withColumn("transaction_id", F.lit("TXN-9999"))
        .withColumn("account_id", F.lit("ACC-1001"))
        .withColumn("transaction_date", F.lit("2024-03-31T23:59:59Z"))
        .withColumn("amount", F.lit("10.00"))
        .withColumn("currency", F.lit("USD"))
        .withColumn("transaction_type", F.lit("debit"))
        .withColumn("merchant_name", F.lit("Incremental Test Merchant"))
        .withColumn("merchant_category", F.lit("retail"))
        .withColumn("status", F.lit("completed"))
        .withColumn("country_code", F.lit("US"))
        .withColumn("_batch_id", F.lit("batch-002"))
        .withColumn("_ingestion_timestamp", F.current_timestamp())
    )

    second_batch = lookback_records.unionByName(new_record)

    # Second run processes the overlapping records and the new transaction.
    second_result = run_ingestion_pipeline(
        second_batch,
        str(SCHEMA_PATH),
        bronze_path,
        quarantine_path,
    )

    save_watermark(
        spark,
        watermark_path,
        "transactions",
        second_result["max_transaction_date"],
        "batch-002",
    )

    second_bronze = spark.read.format("delta").load(bronze_path)
    second_quarantine = spark.read.format("delta").load(quarantine_path)

    second_watermark = get_watermark(
        spark,
        watermark_path,
        "transactions",
    )

    # Existing transactions must not be inserted again.
    assert second_bronze.count() == first_bronze_count + 1

    # The newly arrived transaction must exist exactly once.
    assert (
        second_bronze
        .filter(F.col("transaction_id") == "TXN-9999")
        .count()
        == 1
    )

    # Replayed invalid transactions must not create duplicate quarantine rows.
    assert second_quarantine.count() == first_quarantine_count

    # The watermark must advance to the new maximum transaction date.
    assert second_watermark > first_watermark

    # An empty run must complete without advancing the watermark.
    empty_records = raw_transactions.filter(F.lit(False))

    empty_result = run_ingestion_pipeline(
        empty_records,
        str(SCHEMA_PATH),
        bronze_path,
        quarantine_path,
    )

    save_watermark(
        spark,
        watermark_path,
        "transactions",
        empty_result["max_transaction_date"],
        "batch-003",
    )

    assert empty_result["max_transaction_date"] is None

    assert (
        get_watermark(spark, watermark_path, "transactions")
        == second_watermark
    )