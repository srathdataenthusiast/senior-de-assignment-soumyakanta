"""
Manage incremental ingestion watermarks using Delta Lake.
"""

from datetime import datetime, timedelta, timezone

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
)


def get_max_transaction_date(valid_records: DataFrame) -> datetime | None:
    """Get the latest transaction timestamp from validated records."""

    result = (
        valid_records
        .select(
            F.max(
                F.unix_timestamp(
                    F.to_timestamp(
                        "transaction_date",
                        "yyyy-MM-dd'T'HH:mm:ssX",
                    )
                )
            ).alias("max_epoch_seconds")
        )
        .first()
    )

    max_epoch = result["max_epoch_seconds"]

    if max_epoch is None:
        return None

    return datetime.fromtimestamp(max_epoch, timezone.utc)


def get_watermark(
    spark: SparkSession,
    watermark_path: str,
    source_name: str,
) -> datetime | None:
    """Read the last successful transaction timestamp for a source."""

    if not DeltaTable.isDeltaTable(spark, watermark_path):
        return None

    result = (
        spark.read.format("delta")
        .load(watermark_path)
        .filter(F.col("source_name") == source_name)
        .select(
            F.unix_timestamp(
                "last_successful_transaction_date"
            ).alias("watermark_epoch_seconds")
        )
        .first()
    )

    if result is None or result["watermark_epoch_seconds"] is None:
        return None

    return datetime.fromtimestamp(
        result["watermark_epoch_seconds"],
        timezone.utc,
    )


def get_extraction_start_date(
    last_watermark: datetime | None,
    lookback_days: int = 3,
) -> str | None:
    """Calculate the next extraction timestamp with a lookback window."""

    # No previous watermark means this is the first full extraction.
    if last_watermark is None:
        return None

    # Keep watermark calculations in UTC.
    if last_watermark.tzinfo is None:
        raise ValueError("Watermark must be timezone-aware.")

    last_watermark = last_watermark.astimezone(timezone.utc)

    extraction_start = last_watermark - timedelta(days=lookback_days)

    return extraction_start.strftime("%Y-%m-%dT%H:%M:%SZ")


def save_watermark(
    spark: SparkSession,
    watermark_path: str,
    source_name: str,
    max_transaction_date: datetime | None,
    batch_id: str,
) -> None:
    """Save the latest successfully processed transaction timestamp."""

    # Do not advance the watermark when no valid records were processed.
    if max_transaction_date is None:
        return

    if max_transaction_date.tzinfo is None:
        raise ValueError("Maximum transaction date must be timezone-aware.")

    max_transaction_date = max_transaction_date.astimezone(timezone.utc)
    run_timestamp = datetime.now(timezone.utc)

    watermark_schema = StructType([
        StructField("source_name", StringType(), False),
        StructField("last_successful_transaction_date", TimestampType(), False),
        StructField("last_successful_run_timestamp", TimestampType(), False),
        StructField("batch_id", StringType(), False),
    ])

    watermark_record = spark.createDataFrame(
        [
            (
                source_name,
                max_transaction_date,
                run_timestamp,
                batch_id,
            )
        ],
        schema=watermark_schema,
    )

    # Create the watermark Delta table on its first successful run.
    if not DeltaTable.isDeltaTable(spark, watermark_path):
        watermark_record.write.format("delta").mode("overwrite").save(
            watermark_path
        )
        return

    # Maintain one watermark record per source.
    delta_table = DeltaTable.forPath(spark, watermark_path)

    (
        delta_table.alias("target")
        .merge(
            watermark_record.alias("source"),
            "target.source_name = source.source_name",
        )
        .whenMatchedUpdate(
            set={
                "last_successful_transaction_date":
                    """
                    greatest(
                        target.last_successful_transaction_date,
                        source.last_successful_transaction_date
                    )
                    """,
                "last_successful_run_timestamp":
                    "source.last_successful_run_timestamp",
                "batch_id": "source.batch_id",
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )