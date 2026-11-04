"""
Orchestrate transaction validation, duplicate detection and Delta persistence.
"""

from datetime import datetime

from pyspark.sql import DataFrame, functions as F

from kpi_assessment.quality.validation import validate_transactions
from kpi_assessment.quality.duplicate_detection import flag_duplicate_transactions
from kpi_assessment.storage.delta_writer import write_delta, merge_delta
from kpi_assessment.incremental.watermark import get_max_transaction_date


def run_ingestion_pipeline(
    raw_transactions: DataFrame,
    transaction_schema_path: str,
    bronze_path: str,
    quarantine_path: str,
) -> dict[str, int | datetime | None]:
    
    """Process transactions and persist Bronze and Quarantine records."""

    # Validate transactions and separate valid from invalid records.
    valid_records, invalid_records = validate_transactions(
        dataframe=raw_transactions,
        transaction_schema_path=transaction_schema_path,
    )

    # Flag duplicate transactions without deleting any records.
    bronze_records = flag_duplicate_transactions(valid_records)

    # Find the latest valid transaction date for watermark management
    max_transaction_date = get_max_transaction_date(valid_records)

    # Collect basic ingestion counts for reporting.
    input_count = raw_transactions.count()
    valid_count = valid_records.count()
    invalid_count = invalid_records.count()

    duplicate_count = (
        bronze_records
        .filter(F.col("_is_duplicate"))
        .count()
    )

    # Merge Bronze records to prevent duplicate transaction IDs on reruns.
    merge_delta(
        dataframe=bronze_records,
        output_path=bronze_path,
        merge_keys=["transaction_id"],
    )

    # Persist valid and invalid records separately.
    write_delta(invalid_records, quarantine_path)

    return {
        "input_count": input_count,
        "valid_count": valid_count,
        "invalid_count": invalid_count,
        "duplicate_count": duplicate_count,
        "max_transaction_date": max_transaction_date
    }