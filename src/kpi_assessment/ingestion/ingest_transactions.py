"""
Main entry point for transaction ingestion.
"""

import argparse
import logging
import sys
import uuid
from pathlib import Path

from pyspark.sql import SparkSession

# Add the src directory to the Python import path.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from kpi_assessment.settings import Settings
from kpi_assessment.ingestion.csv_source import read_transactions_csv
from kpi_assessment.ingestion.pipeline import run_ingestion_pipeline
from kpi_assessment.incremental.watermark import save_watermark
from kpi_assessment.transformations.daily_account_summary import (
    build_daily_account_summary,
)
from kpi_assessment.storage.delta_writer import write_daily_account_summary


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--environment",
        choices=["local", "databricks"],
        default=None,
    )
    args = parser.parse_args()

    settings = Settings.from_environment(args.environment)
    batch_id = str(uuid.uuid4())

    # Create Spark session.
    builder = SparkSession.builder.appName("KPI-Assessment")

    if settings.environment == "local":
        from delta import configure_spark_with_delta_pip

        builder = (
            builder
            .config(
                "spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension",
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )

        spark = configure_spark_with_delta_pip(builder).getOrCreate()
    else:
        spark = builder.getOrCreate()

    spark.conf.set("spark.sql.session.timeZone", "UTC")

    try:
        # 1. Read source transactions.
        raw_transactions = read_transactions_csv(
            spark,
            settings.paths["transactions_csv"],
            settings.paths["transaction_schema"],
            batch_id,
        )

        # 2. Validate, detect duplicates and persist Bronze/Quarantine.
        results = run_ingestion_pipeline(
            raw_transactions,
            settings.paths["transaction_schema"],
            settings.paths["bronze"],
            settings.paths["quarantine"],
        )

        logger.info("Ingestion results: %s", results)

        # 3. Build daily account summary from persisted Bronze data.
        bronze_records = (
            spark.read.format("delta")
            .load(settings.paths["bronze"])
        )

        daily_summary = build_daily_account_summary(bronze_records)

        write_daily_account_summary(
            daily_summary,
            settings.paths["daily_account_summary"],
        )

        # 4. Update watermark after successful data processing.
        save_watermark(
            spark,
            settings.paths["watermark"],
            "backup_csv",
            results["max_transaction_date"],
            batch_id,
        )

        # 5. Export review samples when enabled.
        if settings.export_sample_outputs:
            (
                spark.read.format("delta")
                .load(settings.paths["quarantine"])
                .limit(100)
                .coalesce(1)
                .write.mode("overwrite")
                .option("header", "true")
                .csv(f"{settings.paths['samples']}/quarantine")
            )

            (
                daily_summary.limit(100)
                .coalesce(1)
                .write.mode("overwrite")
                .json(f"{settings.paths['samples']}/daily_account_summary")
            )

        logger.info("Ingestion pipeline completed successfully.")

    finally:
        if settings.environment == "local":
            spark.stop()


if __name__ == "__main__":
    main()