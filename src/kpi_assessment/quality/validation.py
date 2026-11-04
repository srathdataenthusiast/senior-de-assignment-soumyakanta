
"""
Transaction Data Quality and Validation Module

Responsibilities:
    1. Load and validate the supplied transaction JSON schema.
    2. Validate raw transaction records against schema rules.
    3. Apply additional business and semantic validation.
    4. Collect all validation errors for each record.
    5. Separate valid and invalid records.

Input:
    Raw Spark DataFrame with transaction fields represented as strings.
    Additional ingestion metadata columns are permitted.

Output:
    valid_records:
        Records satisfying all validation rules.

    invalid_records:
        Defective records with an error_reason column.
"""

# ============================================================
# 1. IMPORTS
# ============================================================

import json
import logging

from pathlib import Path
from typing import Any

import pycountry

from jsonschema import Draft7Validator

from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


# ============================================================
# 2. MODULE CONFIGURATION
# ============================================================

logger = logging.getLogger(__name__)

VALIDATION_ERRORS_COLUMN = "_validation_errors"
VALIDATION_AMOUNT_COLUMN = "_validation_amount"
VALIDATION_TIMESTAMP_COLUMN = "_validation_timestamp"

RESERVED_COLUMNS = {
    VALIDATION_ERRORS_COLUMN,
    VALIDATION_AMOUNT_COLUMN,
    VALIDATION_TIMESTAMP_COLUMN,
}
# Officially assigned ISO 3166-1 alpha-2 country codes.
ASSIGNED_COUNTRY_CODES = tuple(
    sorted(country.alpha_2 for country in pycountry.countries)
)

# ============================================================
# 3. SCHEMA LOADING
# ============================================================

def _load_schema(
    transaction_schema_path: str | Path,
) -> dict[str, Any]:
    """
    Load and validate the supplied JSON Schema definition.
    """

    schema_file = Path(transaction_schema_path)

    # Check whether the schema file exists
    if not schema_file.is_file():
        raise FileNotFoundError(
            f"Transaction schema file not found: {schema_file}"
        )
    
    # Read the JSON Schema
    try:
        with schema_file.open("r", encoding="utf-8") as file:
            schema = json.load(file)

    except json.JSONDecodeError as exc:
        raise ValueError(
            f"Invalid transaction JSON schema: {schema_file}"
        ) from exc

    Draft7Validator.check_schema(schema)

    # Check whether properties and required fields ar e defineed correctly
    if not isinstance(schema.get("properties"), dict):
        raise ValueError(
            "Transaction schema has not dictionary properties must define properties."
        )

    # Check whether properties and required fields are defined correctly
    if not isinstance(schema.get("required"), list):
        raise ValueError(
            "Transaction schema must define required fields as it is is not list."
        )

    # Every required field must also be defined in properies.
    if not set(schema["required"]).issubset(schema["properties"]):
        raise ValueError(
            "Required fields must be defined in schema properties."
        )

    return schema


# ============================================================
# 4. COMMON VALIDATION HELPERS
# ============================================================

def _is_missing(column: str) -> Column:
    """
    Check whether a string field is NULL or whitespace-only.
    """

    value = F.col(column)

    return (
        value.isNull()
        | (F.length(F.trim(value)) == 0)
    )


def _validate_dataframe_structure(
    dataframe: DataFrame,
    schema: dict[str, Any],
) -> None:
    """
    Validate incoming DataFrame columns and source data types.
    """

    # Get the expected transaction columsn from the supplied schema.
    properties = schema["properties"]
    expected_columns = set(properties.keys())

    source_columns = {
        column
        for column in dataframe.columns
        if not column.startswith("_")
    }

    # Identify the missing and unexpected source columns
    missing_columns = expected_columns - source_columns
    unexpected_columns = source_columns - expected_columns

    # Fail if any necessary column is missing.
    if missing_columns:
        raise ValueError(
            f"Missing transaction columns: {sorted(missing_columns)}"
        )

    # Reject additional/extra columns if the schema does not allow them.
    if unexpected_columns and schema.get("additionalProperties") is False:
        raise ValueError(
            f"Unexpected transaction columns: "
            f"{sorted(unexpected_columns)}"
        )

    # Prevent conflicts with temporary validation columns or in other words 
    # Make sure the input does not already contain columns that we will create during validation.
    conflicting_columns = RESERVED_COLUMNS.intersection(
        dataframe.columns
    )

    if conflicting_columns:
        raise ValueError(
            f"Input contains reserved validation columns: "
            f"{sorted(conflicting_columns)}"
        )
    # Ensure transaction fields are still strings so we can validate the original values before converting them.
    non_string_columns = [
        field.name
        for field in dataframe.schema.fields
        if field.name in expected_columns
        and not isinstance(field.dataType, StringType)
    ]

    if non_string_columns:
        raise TypeError(
            "Raw transaction columns must be strings. "
            f"Unexpected types: {non_string_columns}"
        )


# ============================================================
# 5. MAIN TRANSACTION VALIDATION
# ============================================================


def validate_transactions(
    dataframe: DataFrame,
    transaction_schema_path: str | Path,
) -> tuple[DataFrame, DataFrame]:
    """
    Validate transaction records against the supplied JSON schema.

    Returns:
        valid_records: Records passing all validation rules.
        invalid_records: Records with one or more validation errors.
    """

    # 1. Load schema and validate input structure.
    schema = _load_schema(transaction_schema_path)
    properties = schema["properties"]

    _validate_dataframe_structure(dataframe, schema)

    logger.info("Starting transaction validation.")

    # 2. Prepare temporary amount and timestamp columns.
    working_df = (
        dataframe
        .withColumn(
            VALIDATION_AMOUNT_COLUMN,
            F.expr("try_cast(amount AS DECIMAL(38,18))"),
        )
        .withColumn(
            VALIDATION_TIMESTAMP_COLUMN,
            F.try_to_timestamp(
                F.col("transaction_date"),
                F.lit("yyyy-MM-dd'T'HH:mm:ssX"),
            ),
        )
    )

    # Reference transaction columns for validation.
    transaction_id = F.col("transaction_id")
    account_id = F.col("account_id")
    transaction_date = F.col("transaction_date")
    amount = F.col(VALIDATION_AMOUNT_COLUMN)
    currency = F.col("currency")
    transaction_type = F.col("transaction_type")
    merchant_name = F.col("merchant_name")
    merchant_category = F.col("merchant_category")
    status = F.col("status")
    country_code = F.col("country_code")

    # Identify fields containing values.
    transaction_id_present = ~_is_missing("transaction_id")
    account_id_present = ~_is_missing("account_id")
    transaction_date_present = ~_is_missing("transaction_date")
    amount_present = ~_is_missing("amount")
    currency_present = ~_is_missing("currency")
    transaction_type_present = ~_is_missing("transaction_type")
    merchant_name_present = ~_is_missing("merchant_name")
    merchant_category_present = ~_is_missing("merchant_category")
    status_present = ~_is_missing("status")
    country_code_present = ~_is_missing("country_code")

    # 3. Define validation conditions for each field.
    validation_errors = [

        # Transaction ID validation.
        F.when(
            _is_missing("transaction_id"),
            F.lit("transaction_id: required value is missing or blank"),
        ),
        F.when(
            transaction_id_present
            & ~transaction_id.rlike(properties["transaction_id"]["pattern"]),
            F.lit("transaction_id: invalid format"),
        ),

        # Account ID validation.
        F.when(
            _is_missing("account_id"),
            F.lit("account_id: required value is missing or blank"),
        ),
        F.when(
            account_id_present
            & ~account_id.rlike(properties["account_id"]["pattern"]),
            F.lit("account_id: invalid format"),
        ),

        # Transaction date validation.
        F.when(
            _is_missing("transaction_date"),
            F.lit("transaction_date: required value is missing or blank"),
        ),
        F.when(
            transaction_date_present
            & (
                ~transaction_date.rlike(
                    properties["transaction_date"]["pattern"]
                )
                | F.col(VALIDATION_TIMESTAMP_COLUMN).isNull()
            ),
            F.lit("transaction_date: invalid ISO 8601 UTC timestamp"),
        ),

        # Amount validation.
        F.when(
            _is_missing("amount"),
            F.lit("amount: required value is missing or blank"),
        ),
        F.when(
            amount_present & amount.isNull(),
            F.lit("amount: invalid decimal value"),
        ),
        F.when(
            amount_present
            & amount.isNotNull()
            & (
                amount
                <= F.lit(
                    str(properties["amount"]["exclusiveMinimum"])
                ).cast("decimal(38,18)")
            ),
            F.lit("amount: must be greater than zero"),
        ),
        F.when(
            amount_present
            & amount.isNotNull()
            & (amount != F.round(amount, 2)),
            F.lit("amount: must be a multiple of 0.01"),
        ),

        # Currency validation.
        F.when(
            _is_missing("currency"),
            F.lit("currency: required value is missing or blank"),
        ),
        F.when(
            currency_present
            & ~currency.isin(*properties["currency"]["enum"]),
            F.lit("currency: unsupported currency"),
        ),

        # Transaction type validation.
        F.when(
            _is_missing("transaction_type"),
            F.lit("transaction_type: required value is missing or blank"),
        ),
        F.when(
            transaction_type_present
            & ~transaction_type.isin(
                *properties["transaction_type"]["enum"]
            ),
            F.lit("transaction_type: invalid value"),
        ),

        # Merchant name validation.
        F.when(
            _is_missing("merchant_name"),
            F.lit("merchant_name: required value is missing or blank"),
        ),
        F.when(
            merchant_name_present
            & ~merchant_name.rlike(
                properties["merchant_name"]["pattern"]
            ),
            F.lit("merchant_name: must contain a non-whitespace character"),
        ),

        # Merchant category validation.
        F.when(
            _is_missing("merchant_category"),
            F.lit("merchant_category: required value is missing or blank"),
        ),
        F.when(
            merchant_category_present
            & ~merchant_category.isin(
                *properties["merchant_category"]["enum"]
            ),
            F.lit("merchant_category: unsupported category"),
        ),

        # Status validation.
        F.when(
            _is_missing("status"),
            F.lit("status: required value is missing or blank"),
        ),
        F.when(
            status_present
            & ~status.isin(*properties["status"]["enum"]),
            F.lit("status: invalid value"),
        ),

        # Country code validation.
        F.when(
            _is_missing("country_code"),
            F.lit("country_code: required value is missing or blank"),
        ),
        F.when(
            country_code_present
            & ~country_code.rlike(properties["country_code"]["pattern"]),
            F.lit("country_code: invalid ISO alpha-2 format"),
        ),
        F.when(
            country_code_present
            & country_code.rlike(properties["country_code"]["pattern"])
            & ~country_code.isin(*ASSIGNED_COUNTRY_CODES),
            F.lit("country_code: not an officially assigned ISO code"),
        ),
    ]

    # 4. Attach all validation errors to each transaction.
    validated_df = working_df.withColumn(
        VALIDATION_ERRORS_COLUMN,
        F.filter(
            F.array(*validation_errors),
            lambda error: error.isNotNull(),
        ),
    )

    # 5. Separate valid and invalid transaction records.
    temporary_columns = [
        VALIDATION_ERRORS_COLUMN,
        VALIDATION_AMOUNT_COLUMN,
        VALIDATION_TIMESTAMP_COLUMN,
    ]

    valid_records = (
        validated_df
        .filter(F.size(F.col(VALIDATION_ERRORS_COLUMN)) == 0)
        .drop(*temporary_columns)
    )

    invalid_records = (
        validated_df
        .filter(F.size(F.col(VALIDATION_ERRORS_COLUMN)) > 0)
        .withColumn(
            "error_reason",
            F.concat_ws("; ", F.col(VALIDATION_ERRORS_COLUMN)),
        )
        .drop(*temporary_columns)
    )

    logger.info("Transaction validation completed.")

    return valid_records, invalid_records
