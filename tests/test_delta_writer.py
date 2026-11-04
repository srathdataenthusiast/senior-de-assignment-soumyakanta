from kpi_assessment.storage.delta_writer import write_delta,merge_delta


def test_write_delta_persists_bronze_and_quarantine(spark, tmp_path):

    bronze_path = str(tmp_path / "bronze")
    quarantine_path = str(tmp_path / "quarantine")

    valid_records = spark.createDataFrame(
        [
            ("TXN-001", "ACC-001", 100.0),
        ],
        ["transaction_id", "account_id", "amount"],
    )

    invalid_records = spark.createDataFrame(
        [
            ("TXN-002", "ACC-002", -50.0, "Invalid amount"),
        ],
        ["transaction_id", "account_id", "amount", "error_reason"],
    )

    # Write valid and invalid records to separate Delta locations.
    write_delta(valid_records, bronze_path)
    write_delta(invalid_records, quarantine_path)

    # Read the saved Delta data to verify persistence.
    bronze_data = spark.read.format("delta").load(bronze_path)
    quarantine_data = spark.read.format("delta").load(quarantine_path)

    assert bronze_data.count() == 1
    assert quarantine_data.count() == 1

    assert "error_reason" not in bronze_data.columns
    assert "error_reason" in quarantine_data.columns

def test_merge_delta_prevents_duplicate_records(spark, tmp_path):

    bronze_path = str(tmp_path / "bronze")

    columns = ["transaction_id", "amount"]

    first_batch = spark.createDataFrame(
        [
            ("TXN-001", 100),
            ("TXN-002", 200),
        ],
        columns,
    )

    # First run creates the Bronze Delta table.
    merge_delta(
        first_batch,
        bronze_path,
        merge_keys=["transaction_id"],
    )

    second_batch = spark.createDataFrame(
        [
            ("TXN-002", 250),
            ("TXN-003", 300),
        ],
        columns,
    )

    # Second run updates TXN-002 and inserts TXN-003.
    merge_delta(
        second_batch,
        bronze_path,
        merge_keys=["transaction_id"],
    )

    bronze_records = spark.read.format("delta").load(bronze_path)

    results = {
        row["transaction_id"]: row["amount"]
        for row in bronze_records.collect()
    }

    assert bronze_records.count() == 3

    assert results == {
        "TXN-001": 100,
        "TXN-002": 250,
        "TXN-003": 300,
    }