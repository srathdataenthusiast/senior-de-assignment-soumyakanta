
"""
CSV source adapter for transaction ingestion.

Responsibilities:
    - Read the HR-provided backup transaction dataset.
    - Use the supplied JSON schema to establish expected columns.
    - Preserve source values as strings for downstream validation.
    - Attach ingestion metadata to every source record.

The adapter does not perform data quality validation,
deduplication, or Delta Lake persistence.
"""

import json
import logging

from pathlib import Path
from typing import Union

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    current_timestamp,
    input_file_name,
    lit,
)
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)


logger = logging.getLogger(__name__)

RESERVED_COLUMNS = {
    "_source_file",
    "_source_system",
    "_ingestion_timestamp",
    "_batch_id",
}


def load_expected_columns(schema_path: Union[str, Path]) -> list[str]:
    """
    Load expected transaction columns from the supplied JSON schema file for Data validation 
    / Schema Validation.

    The JSON schema is the source of truth for column names.
    """

    schema_file = Path(schema_path)

    if not schema_file.is_file():
        raise FileNotFoundError(
            f"Transaction schema not found: {schema_file}"
        )

    try:
        with schema_file.open("r", encoding="utf-8") as file:
            schema_definition = json.load(file)

    except json.JSONDecodeError as exc:
        raise ValueError(
            f"Invalid JSON schema file: {schema_file}"
        ) from exc

    required_fields = schema_definition.get("required")
    properties = schema_definition.get("properties")

    if not isinstance(required_fields, list):
        raise ValueError("JSON schema must define required fields.")

    if not isinstance(properties, dict):
        raise ValueError("JSON schema must define properties.")

    expected_columns = list(properties.keys())

    if not expected_columns:
        raise ValueError("Transaction schema contains no columns.")

    if len(expected_columns) != len(set(expected_columns)):
        raise ValueError("Duplicate column definitions found.")

    if set(required_fields) != set(expected_columns):
        raise ValueError(
            "Expected all transaction properties to be required "
            "for this assessment."
        )

    conflicting_columns = RESERVED_COLUMNS.intersection(expected_columns)

    if conflicting_columns:
        raise ValueError(
            f"Source schema uses reserved metadata columns: "
            f"{sorted(conflicting_columns)}"
        )

    return expected_columns


def read_transactions_csv(
    spark: SparkSession,
    csv_path: Union[str, Path],
    schema_path: Union[str, Path],
    batch_id: str,
) -> DataFrame:
    """
    Read transaction CSV into a raw Spark DataFrame.

    All source fields are initially read as strings.

    Metadata columns:
        _source_file
        _source_system
        _ingestion_timestamp
        _batch_id

    Args:
        spark: Active SparkSession.
        csv_path: Transaction CSV location.
        schema_path: Supplied JSON schema location.
        batch_id: Unique identifier for this ingestion run.

    Returns:
        Raw Spark DataFrame with source and ingestion metadata.
    """

    csv_path = str(csv_path)
    batch_id = batch_id.strip()

    if not csv_path.strip():
        raise ValueError("CSV source path cannot be empty.")

    if not batch_id:
        raise ValueError("Batch ID cannot be empty.")

    expected_columns = load_expected_columns(schema_path)

    # Explicit string schema prevents automatic type inference.
    raw_schema = StructType(
        [
            StructField(column, StringType(), True)
            for column in expected_columns
        ]
    )

    logger.info(
        "Starting CSV ingestion. Source=%s, BatchId=%s",
        csv_path,
        batch_id,
    )

    dataframe = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .option("enforceSchema", "false")
        .option("mode", "PERMISSIVE")
        .option("encoding", "UTF-8")
        .schema(raw_schema)
        .load(csv_path)
    )

    # Attach operational metadata.
    dataframe = (
        dataframe
        .withColumn("_source_file", input_file_name())
        .withColumn("_source_system", lit("backup_csv"))
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_batch_id", lit(batch_id))
    )

    logger.info(
        "CSV source DataFrame created. "
        "ExpectedColumns=%s, BatchId=%s",
        len(expected_columns),
        batch_id,
    )

    return dataframe
