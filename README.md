# Senior Data Engineer Take-Home Assessment

**Transaction Ingestion and Daily Account KPI Pipeline**

A PySpark and Delta Lake implementation covering transaction ingestion, data-quality validation, duplicate detection, incremental processing, and daily account-level summaries.

---

## 1. Project Overview

This project implements a transaction data pipeline that ingests source transactions, validates data quality, separates invalid records into quarantine, persists valid records in Delta Lake, and generates daily account summaries.

The implementation is designed to support both local development and deployment on Azure Databricks through environment-specific configuration.

### Assessment objectives

- Ingest transaction records from the supplied backup dataset or API.
- Validate transaction structure, data types, required fields, and permitted values.
- Quarantine invalid records with clear rejection reasons.
- Identify duplicate business transactions without losing source records.
- Persist validated transactions in Bronze Delta Lake.
- Generate daily account-level transaction summaries.
- Maintain transaction-date watermarks for incremental extraction.
- Demonstrate repeatable processing through tests and Delta MERGE operations.

## 2. Architecture

```text
                  Transaction Source
                   /             \
              REST API        Backup CSV
                  |               |
                  |          csv_source.py
                  |               |
             api_client.py       |
                  |               |
                  +-------+-------+
                          |
                          v
                 Transaction DataFrame
                          |
                          v
                    pipeline.py
                          |
                  Data Quality Checks
                    /          \
                   /            \
              Valid Records    Invalid Records
                   |                |
             Duplicate Flagging   Quarantine
                   |
                   v
               Bronze Delta
                   |
                   v
          Daily Account Summary
                   |
                   v
          Daily Summary Delta

          Watermark Management
          (Incremental Processing)
```

### Main processing components

| Component | Responsibility |
|---|---|
| `csv_source.py` | Reads the supplied CSV and attaches ingestion metadata |
| `api_client.py` | Handles API requests, pagination, retries and date filtering |
| `pipeline.py` | Coordinates validation, duplicate detection and ingestion persistence |
| `validation.py` | Applies transaction schema and data-quality rules |
| `duplicate_detection.py` | Identifies duplicate business transactions |
| `delta_writer.py` | Writes and merges Delta tables |
| `watermark.py` | Reads, calculates and persists extraction watermarks |
| `daily_account_summary.py` | Builds daily account-level KPI summaries |
| `ingest_transactions.py` | Main execution entry point |

## 3. Technology Stack

| Technology | Purpose |
|---|---|
| Python 3.11 | Application and orchestration logic |
| PySpark | Distributed data processing and transformations |
| Delta Lake | Transactional storage and MERGE operations |
| PyYAML | Environment-specific configuration |
| Requests | REST API integration |
| Tenacity | Retry handling with exponential backoff |
| JSON Schema | Source schema validation |
| Pytest | Unit and integration-style testing |
| Azure Databricks | Target execution platform |

## 4. Repository Structure

```text
senior-de-assignment-soumyakanta/
│
├── config/
│   ├── local.yml
│   └── databricks.yml
│
├── data/
│   └── backup/
│       ├── transactions.csv
│       └── transactions_schema.json
│
├── docs/
│
├── outputs/
│   └── samples/
│       ├── quarantine/
│       └── daily_account_summary/
│
├── scripts/
│   ├── check_config.py
│   └── test_api_connection.py
│
├── src/kpi_assessment/
│   ├── incremental/
│   │   └── watermark.py
│   ├── ingestion/
│   │   ├── api_client.py
│   │   ├── csv_source.py
│   │   ├── ingest_transactions.py
│   │   └── pipeline.py
│   ├── quality/
│   │   ├── duplicate_detection.py
│   │   └── validation.py
│   ├── storage/
│   │   └── delta_writer.py
│   ├── transformations/
│   │   └── daily_account_summary.py
│   └── settings.py
│
├── tests/
│   ├── incremental/
│   ├── ingestion/
│   ├── quality/
│   ├── transformations/
│   ├── conftest.py
│   └── test_delta_writer.py
│
├── .env.example
├── .gitignore
├── requirements.txt
├── requirements-dev.txt
└── README.md
```

## 5. Local Setup

### Prerequisites

- Python 3.11
- Java 17
- Git

### Create and activate the virtual environment

```bash
python3.11 -m venv .venv
source .venv/bin/activate
```

### Install dependencies

```bash
python -m pip install --upgrade pip
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

### Configure the environment

Copy the example environment file:

```bash
cp .env.example .env
```

Set the local environment:

```dotenv
APP_ENV=local
```

The application loads `config/local.yml` and uses the configured CSV, schema, and output paths.

API credentials are supplied through environment variables, not committed configuration files.

## 6. Environment Configuration

The project uses YAML configuration files to separate environment-specific paths from application logic.

### Local configuration

`config/local.yml`

```yaml
paths:
  transactions_csv: data/backup/transactions.csv
  transaction_schema: data/backup/transactions_schema.json

  bronze: outputs/bronze_delta
  quarantine: outputs/quarantine_delta
  watermark: outputs/watermark_delta
  daily_account_summary: outputs/daily_account_summary_delta
  samples: outputs/samples

export_sample_outputs: true
```

### Databricks configuration

`config/databricks.yml` contains the corresponding Unity Catalog Volume paths.

Before deployment, replace the catalog, schema, and volume placeholders with the actual Databricks environment values.

Select the configuration using:

```dotenv
APP_ENV=local
```

For Databricks:

```text
APP_ENV=databricks
```

The application uses the same settings interface in both environments.

## 7. Execute the Pipeline

From the project root, run:

```bash
PYTHONPATH=src python -m kpi_assessment.ingestion.ingest_transactions
```

The entry point performs the following operations:

1. Loads environment-specific configuration.
2. Creates or obtains the Spark session.
3. Reads transactions from the configured CSV source.
4. Validates records and identifies duplicates.
5. Persists valid transactions in Bronze Delta.
6. Persists rejected transactions in Quarantine.
7. Builds and persists the daily account summary.
8. Updates the transaction watermark after successful processing.
9. Exports review-friendly samples when enabled.

## 8. Data Quality and Duplicate Handling

### Validation

The validation module checks:

- Required transaction fields.
- Expected source schema and column structure.
- Transaction identifiers and account identifiers.
- Valid transaction timestamps.
- Positive transaction amounts and permitted precision.
- Currency, transaction type, status, and merchant category values.
- Assigned country codes.
- Missing or blank mandatory values.

Invalid records are separated from valid records and stored in Quarantine.

Each rejected record contains:

| Field | Description |
|---|---|
| `error_reason` | Explains the validation failure |
| `_ingestion_timestamp` | When the transaction was processed |
| `_batch_id` | Identifies the ingestion execution |
| `_source_system` | Identifies the source |

### Duplicate detection

Duplicate detection compares business transaction attributes while excluding `transaction_id`.

The natural-key comparison uses:

- account_id
- transaction_date
- amount
- currency
- transaction_type
- merchant_name
- merchant_category
- country_code
- status

Duplicate records are retained in Bronze and flagged using `_is_duplicate`.

The daily summary excludes duplicate copies.

This preserves source-level information while preventing duplicate business activity from inflating the aggregated metrics.

## 9. Daily Account Summary

The daily summary includes completed transactions only and excludes records that were quarantined or flagged as duplicate copies.

The output contains:

| Field | Description |
|---|---|
| `account_id` | Account identifier |
| `transaction_date` | Transaction calendar date |
| `total_debit_amount` | Debit totals by currency |
| `total_credit_amount` | Credit totals by currency |
| `net_amount` | Credit minus debit, by currency |
| `transaction_count` | Number of eligible transactions |
| `distinct_merchants` | Number of distinct merchants |
| `top_category` | Highest debit-spend category by currency |
| `currencies` | Currencies present for the account and date |
| `updated_at` | Summary processing timestamp |

### Currency handling

The supplied data contains account-days with transactions in multiple currencies.

Combining amounts in different currencies would produce misleading financial totals.

Therefore, debit, credit, net amount, and top category are represented as currency-keyed maps.

For example:

```json
{
  "account_id": "ACC-1001",
  "transaction_date": "2024-01-09",
  "total_debit_amount": {
    "USD": 238.20
  },
  "total_credit_amount": {
    "USD": 0.00
  },
  "net_amount": {
    "USD": -238.20
  },
  "transaction_count": 1,
  "distinct_merchants": 1,
  "top_category": {
    "USD": "home_and_garden"
  },
  "currencies": ["USD"]
}
```

The daily summary is persisted using Delta MERGE keyed by account and transaction date, supporting repeatable summary updates.

## 10. Incremental Processing and Watermarks

The watermark table stores:

- `source_name`
- `last_successful_transaction_date`
- `last_successful_run_timestamp`
- `batch_id`

The watermark is based on the maximum valid transaction timestamp.

The implementation includes:

- Initial extraction when no watermark exists.
- A configurable lookback calculation, currently three days.
- UTC-normalized watermark handling.
- Protection against moving the stored watermark backward.
- No watermark advancement when there are no valid transaction dates.
- Watermark persistence after successful data-output processing.

Bronze records are merged using `transaction_id` to avoid inserting the same transaction repeatedly.

**Current implementation limitation:** the local CSV entry point reads the full supplied backup file on each run. The watermark and lookback helpers are implemented and tested, but the CSV entry point does not yet apply the calculated lookback as a source filter. The API client's date-filter capability is intended for the API extraction path.

The incremental tests simulate repeated extraction and verify the watermark and idempotency behaviour.

## 11. Output Locations

The local execution produces:

```text
outputs/
├── bronze_delta/
├── quarantine_delta/
├── watermark_delta/
├── daily_account_summary_delta/
└── samples/
    ├── quarantine/
    └── daily_account_summary/
```

The four Delta directories contain the persisted datasets and Delta transaction logs.

The samples directory contains CSV and JSON exports for convenient assessment review.

Set `export_sample_outputs` to `false` in the environment configuration when review samples are not required.

## 12. Testing

Run the complete test suite:

```bash
PYTHONPATH=src python -m pytest -v tests/
```

The current test suite contains 12 passing tests covering:

- Transaction validation.
- Duplicate detection.
- Delta persistence and MERGE behaviour.
- Watermark creation and advancement.
- Lookback calculations.
- Repeated incremental processing.
- Daily account summary calculations.
- Idempotent summary updates.

**Latest local verification:** 12 tests passed.

A local end-to-end execution also processed the supplied backup dataset:

| Metric | Result |
|---|---:|
| Input transactions | 352 |
| Valid transactions | 349 |
| Invalid transactions | 3 |
| Duplicate transactions flagged | 5 |
| Maximum valid transaction timestamp | 2024-03-30 22:35:29 UTC |

## 13. Assumptions, Limitations and Future Improvements

The following points should be considered before production deployment.

1. **Live API connectivity:** The assessment API endpoint could not be resolved during development because of a DNS resolution failure. The supplied backup CSV was used as the fallback dataset. The API connector implementation is present but has not been validated against a successful live API response.

2. **Quarantine history:** Quarantine currently uses append writes. Reprocessing the same invalid transaction can create another quarantine record. A production implementation could use a transaction-keyed MERGE for current quarantine state or maintain a separate append-only rejection history.

3. **Incremental extraction:** The watermark and lookback utilities are implemented, but end-to-end source filtering is not yet wired into the CSV entry point.

4. **Late-arriving transactions:** A lookback window helps capture records arriving with earlier transaction dates. The appropriate window should be agreed with source-system owners and monitored.

5. **Operational monitoring:** Production deployment should include ingestion counts, validation failure rates, duplicate rates, watermark freshness, processing duration, and pipeline failure alerts.

6. **Currency conversion:** Cross-currency aggregation requires an agreed exchange-rate source, conversion timestamp, and base currency. This implementation deliberately avoids implicit conversion.

7. **Production governance:** Additional controls should include ownership, lineage, retention policies, access controls, schema-change monitoring, and documented data-quality expectations.

## 14. Databricks Deployment Target

The implementation separates source reading, processing, transformation, storage, and orchestration so that the same application logic can run on Azure Databricks.

The intended deployment uses:

- Azure Databricks with Unity Catalog.
- Unity Catalog Volumes for input and output paths.
- Databricks-managed Spark and Delta Lake.
- Environment-specific configuration through `APP_ENV=databricks`.
- A Lakeflow Job to execute the packaged application.

Databricks deployment and execution should be validated separately before considering the solution production-ready. The current verification results are from the local Spark and Delta environment.

## 15. AI Assistance and Verification

AI tools were used as development assistance for architecture discussions, implementation guidance, code review, and documentation.

The resulting implementation was reviewed and verified through local execution and automated tests. Design decisions, assumptions, limitations, and output behaviour are documented here for independent technical review.


