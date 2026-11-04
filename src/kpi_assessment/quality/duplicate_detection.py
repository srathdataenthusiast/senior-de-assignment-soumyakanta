"""
Detect duplicate transactions by comparing all transaction fields except transaction_id.
Flag duplicate copies while retaining all records for auditability.
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


# A transaction is considered a duplicate when all these fields match.
# transaction_id is intentionally excluded because duplicate records
# may have different IDs.
NATURAL_KEY_COLUMNS = (
    "account_id",
    "transaction_date",
    "amount",
    "currency",
    "transaction_type",
    "merchant_name",
    "merchant_category",
    "country_code",
    "status",
)


def flag_duplicate_transactions(valid_transactions: DataFrame) -> DataFrame:
    """Identify duplicate transactions without removing any records."""

    # Avoid overwriting a column that may already exist in the input.
    if "_is_duplicate" in valid_transactions.columns:
        raise ValueError(
            "Input already contains the reserved column '_is_duplicate'."
        )

    # Group transactions that have identical business fields.Within each group, order by transaction_id.
    duplicate_window = (
        Window
        .partitionBy(*NATURAL_KEY_COLUMNS)
        .orderBy(F.col("transaction_id").asc())
    )

    # Rank records within each group. Rank 1 is the canonical and later are duplicates;
    ranked_transactions = valid_transactions.withColumn(
        "_duplicate_rank",
        F.row_number().over(duplicate_window),
    )

    # Flag duplicate copies, but retain every record for auditability.
    return (
        ranked_transactions
        .withColumn("_is_duplicate", F.col("_duplicate_rank") > 1)
        .drop("_duplicate_rank")
    )