"""
Persist validated and quarantined transaction records in Delta format.
"""

from pyspark.sql import DataFrame, functions as F
from delta.tables import DeltaTable
from pyspark.sql.types import MapType


def write_delta(
    dataframe: DataFrame,
    output_path: str,
    mode: str = "append",
    partition_columns: list[str] | None = None,
    write_options: dict[str, str] | None = None,
) -> None:
    """Write a DataFrame to Delta with optional partitioning and writer options."""

    writer = (
        dataframe.write
        .format("delta")
        .mode(mode)
    )

    # Partition the output when required by data volume and query patterns.
    if partition_columns:
        writer = writer.partitionBy(*partition_columns)

    # Apply additional Delta writer options when supplied.
    for option_name, option_value in (write_options or {}).items():
        writer = writer.option(option_name, option_value)

    writer.save(output_path)


def write_daily_account_summary(
    summary: DataFrame,
    output_path: str,
) -> None:
    """Upsert daily summaries while preserving unchanged records."""

    # Create the Delta table on its first run.
    if not DeltaTable.isDeltaTable(summary.sparkSession, output_path):
        summary.write.format("delta").mode("overwrite").save(output_path)
        return

    # Identify changes in summary values, excluding the key and update timestamp.
    comparison_conditions = []

    for field in summary.schema.fields:
        column = field.name

        if column in ("account_id", "transaction_date", "updated_at"):
            continue

        # Compare currency maps using sorted entries.
        if isinstance(field.dataType, MapType):
            comparison_conditions.append(
                f"NOT (array_sort(map_entries(target.`{column}`)) "
                f"<=> array_sort(map_entries(source.`{column}`)))"
            )
        else:
            comparison_conditions.append(
                f"NOT (target.`{column}` <=> source.`{column}`)"
            )

    changed_condition = " OR ".join(comparison_conditions)

    update_values = {
        column: f"source.`{column}`"
        for column in summary.columns
    }

    delta_table = DeltaTable.forPath(summary.sparkSession, output_path)

    (
        delta_table.alias("target")
        .merge(
            summary.alias("source"),
            """
            target.account_id = source.account_id
            AND target.transaction_date = source.transaction_date
            """,
        )
        .whenMatchedUpdate(
            condition=changed_condition,
            set=update_values,
        )
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceDelete()
        .execute()
    )

def merge_delta(
    dataframe: DataFrame,
    output_path: str,
    merge_keys: list[str],
) -> None:
    """Insert new records and update existing records using the specified keys."""

    # Ensure merge keys are supplied and available in the source.
    if not merge_keys:
        raise ValueError("At least one merge key must be provided.")

    missing_columns = [
        column for column in merge_keys
        if column not in dataframe.columns
    ]

    if missing_columns:
        raise ValueError(f"Missing merge key columns: {missing_columns}")

    # Prevent ambiguous MERGE operations caused by repeated source keys.
    duplicate_keys = (
        dataframe
        .groupBy(*merge_keys)
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )

    if duplicate_keys:
        raise ValueError("Source contains duplicate merge keys.")

    # Create the Delta table on its first run.
    if not DeltaTable.isDeltaTable(dataframe.sparkSession, output_path):
        dataframe.write.format("delta").mode("overwrite").save(output_path)
        return

    # Match existing records using the supplied keys.
    merge_condition = " AND ".join(
        f"target.`{column}` = source.`{column}`"
        for column in merge_keys
    )

    delta_table = DeltaTable.forPath(dataframe.sparkSession, output_path)

    (
        delta_table.alias("target")
        .merge(dataframe.alias("source"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )