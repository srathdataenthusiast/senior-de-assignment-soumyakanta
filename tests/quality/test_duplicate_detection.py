from kpi_assessment.quality.duplicate_detection import (
    flag_duplicate_transactions,
)


def test_duplicate_transactions_are_flagged_without_removing_records(spark):

    columns = [
        "transaction_id",
        "account_id",
        "transaction_date",
        "amount",
        "currency",
        "transaction_type",
        "merchant_name",
        "merchant_category",
        "country_code",
        "status",
    ]

    rows = [
        ("TXN-001", "ACC-001", "2026-10-01", "100.00", "USD",
         "debit", "Amazon", "shopping", "US", "completed"),

        ("TXN-002", "ACC-001", "2026-10-01", "100.00", "USD",
         "debit", "Amazon", "shopping", "US", "completed"),

        ("TXN-003", "ACC-001", "2026-10-01", "200.00", "USD",
         "debit", "Amazon", "shopping", "US", "completed"),
    ]

    input_df = spark.createDataFrame(rows, columns)

    result_df = flag_duplicate_transactions(input_df)

    # Every input record must remain in the output.
    assert result_df.count() == 3

    # The lowest transaction_id is canonical; the matching copy is flagged.
    duplicate_flags = {
        row["transaction_id"]: row["_is_duplicate"]
        for row in result_df.select("transaction_id", "_is_duplicate").collect()
    }

    assert duplicate_flags == {
        "TXN-001": False,
        "TXN-002": True,
        "TXN-003": False,
    }

    # Internal ranking column must not appear in the final output.
    assert "_duplicate_rank" not in result_df.columns