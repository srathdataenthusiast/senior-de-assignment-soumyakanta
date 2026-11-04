from datetime import datetime,timezone

from kpi_assessment.incremental.watermark import (
    get_max_transaction_date,
    get_watermark,
    get_extraction_start_date,
    save_watermark,
)


def test_first_run_has_no_watermark(spark, tmp_path):

    watermark_path = str(tmp_path / "watermark")

    # No previous ingestion means a full initial extraction.
    watermark = get_watermark(
        spark,
        watermark_path,
        "transactions",
    )

    assert watermark is None
    assert get_extraction_start_date(watermark) is None


def test_lookback_start_date():

    last_watermark = datetime(
    2024, 3, 10, 18, 30, tzinfo=timezone.utc
)

    extraction_start = get_extraction_start_date(
        last_watermark,
        lookback_days=3,
    )

    assert extraction_start == "2024-03-07T18:30:00Z"


def test_watermark_is_created_and_advanced(spark, tmp_path):

    watermark_path = str(tmp_path / "watermark")

    first_date = datetime(
    2024, 3, 10, 18, 30, tzinfo=timezone.utc
)

    second_date = datetime(
        2024, 3, 12, 7, 0, tzinfo=timezone.utc
    )

    # First successful run creates the watermark.
    save_watermark(
        spark,
        watermark_path,
        "transactions",
        first_date,
        "batch-001",
    )

    first_watermark = get_watermark(
        spark,
        watermark_path,
        "transactions",
    )

    assert first_watermark == first_date

    # Second successful run advances the existing watermark.
    save_watermark(
        spark,
        watermark_path,
        "transactions",
        second_date,
        "batch-002",
    )

    updated_watermark = get_watermark(
        spark,
        watermark_path,
        "transactions",
    )

    assert updated_watermark == second_date

    # An empty run must not advance the watermark.
    save_watermark(
        spark,
        watermark_path,
        "transactions",
        None,
        "batch-003",
    )

    assert get_watermark(
        spark,
        watermark_path,
        "transactions",
    ) == second_date

    # There must still be only one watermark record for this source.
    assert spark.read.format("delta").load(watermark_path).count() == 1


def test_max_transaction_date(spark):

    transactions = spark.createDataFrame(
        [
            ("2024-03-10T10:00:00Z",),
            ("2024-03-12T15:30:00Z",),
            ("2024-03-11T08:00:00Z",),
        ],
        ["transaction_date"],
    )

    maximum_date = get_max_transaction_date(transactions)

    assert maximum_date == datetime(
    2024, 3, 12, 15, 30, tzinfo=timezone.utc
)

    # An empty input should not produce a watermark.
    empty_transactions = transactions.filter("1 = 0")

    assert get_max_transaction_date(empty_transactions) is None