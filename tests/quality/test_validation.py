"""
Integration tests for transaction data quality validation.

Uses the HR-provided backup CSV and JSON schema.
API link is not working so exluded from the Case Study for now.
"""

from pathlib import Path

import pytest
from pyspark.sql import functions as F

from kpi_assessment.ingestion.csv_source import read_transactions_csv
from kpi_assessment.quality.validation import validate_transactions


# Locate the assessment input files.
PROJECT_ROOT = Path(__file__).resolve().parents[2]

CSV_PATH = PROJECT_ROOT / "data/backup/transactions.csv"
SCHEMA_PATH = PROJECT_ROOT / "data/backup/transactions_schema.json"


@pytest.fixture(scope="session")
def raw_transactions(spark):
    """Load the supplied CSV using the transaction source adapter."""

    return read_transactions_csv(
        spark=spark,
        csv_path=CSV_PATH,
        schema_path=SCHEMA_PATH,
        batch_id="validation-test",
    )


@pytest.fixture(scope="session")
def validation_results(raw_transactions):
    """Run transaction validation once for the test suite."""

    return validate_transactions(
        dataframe=raw_transactions,
        transaction_schema_path=SCHEMA_PATH,
    )


def test_all_input_records_are_accounted_for(
    raw_transactions,
    validation_results,
):
    """Every input record must be either valid or invalid."""

    valid_records, invalid_records = validation_results

    assert raw_transactions.count() == (
        valid_records.count() + invalid_records.count()
    )

    assert "error_reason" not in valid_records.columns
    assert "error_reason" in invalid_records.columns

    assert invalid_records.filter(
        F.col("error_reason").isNull()
    ).count() == 0


def test_known_defective_transactions_are_quarantined(validation_results):
    """Verify the three intentionally defective source records."""

    valid_records, invalid_records = validation_results

    invalid_by_id = {
        row["transaction_id"]: row["error_reason"]
        for row in invalid_records.select(
            "transaction_id", "error_reason"
        ).collect()
    }

    # TXN-0345: Invalid date, merchant category and country code.
    assert "transaction_date" in invalid_by_id["TXN-0345"]
    assert "merchant_category" in invalid_by_id["TXN-0345"]
    assert "country_code" in invalid_by_id["TXN-0345"]

    # TXN-0346: Invalid date, negative amount, blank merchant and status.
    assert "transaction_date" in invalid_by_id["TXN-0346"]
    assert "amount" in invalid_by_id["TXN-0346"]
    assert "merchant_name" in invalid_by_id["TXN-0346"]
    assert "status" in invalid_by_id["TXN-0346"]

    # TXN-0347: Zero amount and invalid enum/country values.
    assert "amount" in invalid_by_id["TXN-0347"]
    assert "transaction_type" in invalid_by_id["TXN-0347"]
    assert "merchant_category" in invalid_by_id["TXN-0347"]
    assert "country_code" in invalid_by_id["TXN-0347"]

    # None of the defective records should appear in valid records.
    valid_ids = {
        row["transaction_id"]
        for row in valid_records.select("transaction_id").collect()
    }

    assert not {"TXN-0345", "TXN-0346", "TXN-0347"}.intersection(valid_ids)