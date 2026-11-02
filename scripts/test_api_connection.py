
import logging

from kpi_assessment.settings import Settings
from kpi_assessment.ingestion.api_client import TransactionsAPIClient


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    settings = Settings.from_environment()
    client = TransactionsAPIClient(settings)

    total_records = 0
    total_pages = 0
    sample_fields = set()

    try:
        for page in client.iter_pages():
            total_pages += 1
            total_records += len(page)

            if page and not sample_fields:
                sample_fields = set(page[0].keys())

        print("\nAPI extraction successful")
        print("Total pages fetched:", total_pages)
        print("Total records fetched:", total_records)
        print("Fields returned:", sorted(sample_fields))

    finally:
        client.close()


if __name__ == "__main__":
    main()
