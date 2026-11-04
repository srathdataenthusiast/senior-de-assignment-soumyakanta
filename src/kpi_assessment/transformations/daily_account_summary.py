"""
Build daily account summaries from validated Bronze transactions.
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def build_daily_account_summary(
    bronze_records: DataFrame,
) -> DataFrame:
    """Aggregate completed transactions into daily account summaries."""

    # Exclude duplicate copies and transactions that are not completed.
    completed_records = (
        bronze_records
        .filter(
            (F.col("status") == "completed")
            & (F.col("_is_duplicate") == F.lit(False))
        )
        .withColumn(
            "_transaction_day",
            F.to_date(
                F.to_timestamp(
                    F.col("transaction_date"),
                    "yyyy-MM-dd'T'HH:mm:ssX",
                )
            ),
        )
        .withColumn(
            "_amount",
            F.col("amount").cast("decimal(18,2)"),
        )
    )

    # Calculate debit and credit totals separately for each currency.
    currency_totals = (
        completed_records
        .groupBy("account_id", "_transaction_day", "currency")
        .agg(
            F.sum(
                F.when(
                    F.col("transaction_type") == "debit",
                    F.col("_amount"),
                ).otherwise(F.lit(0).cast("decimal(18,2)"))
            ).alias("total_debit_amount"),
            F.sum(
                F.when(
                    F.col("transaction_type") == "credit",
                    F.col("_amount"),
                ).otherwise(F.lit(0).cast("decimal(18,2)"))
            ).alias("total_credit_amount"),
        )
        .withColumn(
            "net_amount",
            F.col("total_credit_amount") - F.col("total_debit_amount"),
        )
    )

    # Combine currency-specific totals into one row per account and date.
    amount_summary = (
        currency_totals
        .groupBy("account_id", "_transaction_day")
        .agg(
            F.map_from_entries(
                F.sort_array(
                    F.collect_list(
                        F.struct("currency", "total_debit_amount")
                    )
                )
            ).alias("total_debit_amount"),

            F.map_from_entries(
                F.sort_array(
                    F.collect_list(
                        F.struct("currency", "total_credit_amount")
                    )
                )
            ).alias("total_credit_amount"),

            F.map_from_entries(
                F.sort_array(
                    F.collect_list(
                        F.struct("currency", "net_amount")
                    )
                )
            ).alias("net_amount"),

            F.sort_array(F.collect_set("currency")).alias("currencies"),
        )
    )

    # Count completed transactions and distinct merchants per account-day.
    daily_metrics = (
        completed_records
        .groupBy("account_id", "_transaction_day")
        .agg(
            F.count("*").alias("transaction_count"),
            F.countDistinct("merchant_name").alias("distinct_merchants"),
        )
    )

    # Find the highest-spending merchant category separately per currency.
    category_spend = (
        completed_records
        .filter(F.col("transaction_type") == "debit")
        .groupBy(
            "account_id",
            "_transaction_day",
            "currency",
            "merchant_category",
        )
        .agg(F.sum("_amount").alias("_category_spend"))
    )

    category_window = (
        Window
        .partitionBy("account_id", "_transaction_day", "currency")
        .orderBy(
            F.col("_category_spend").desc(),
            F.col("merchant_category").asc(),
        )
    )

    top_categories = (
        category_spend
        .withColumn("_rank", F.row_number().over(category_window))
        .filter(F.col("_rank") == 1)
        .groupBy("account_id", "_transaction_day")
        .agg(
            F.map_from_entries(
                F.sort_array(
                    F.collect_list(
                        F.struct("currency", "merchant_category")
                    )
                )
            ).alias("top_category")
        )
    )

    # Combine all daily metrics into the required summary output.
    return (
        daily_metrics
        .join(
            amount_summary,
            ["account_id", "_transaction_day"],
            "inner",
        )
        .join(
            top_categories,
            ["account_id", "_transaction_day"],
            "left",
        )
        .select(
            "account_id",
            F.col("_transaction_day").alias("transaction_date"),
            "total_debit_amount",
            "total_credit_amount",
            "net_amount",
            "transaction_count",
            "distinct_merchants",
            "top_category",
            "currencies",
            F.current_timestamp().alias("updated_at"),
        )
    )