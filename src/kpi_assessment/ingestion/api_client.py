
"""
REST API client for transaction ingestion.

Responsibilities:
    - Authenticate with the source REST API.
    - Fetch transactions using offset-based pagination.
    - Retry transient HTTP and network failures.
    - Expose records page-by-page without collecting the full dataset.

"""

import logging
import random
import time

from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Iterator, Optional

import requests

from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
)

from kpi_assessment.settings import Settings


logger = logging.getLogger(__name__)

TransactionRecord = dict[str, Any]
TransactionPage = list[TransactionRecord]


class APIClientError(RuntimeError):
    """Base exception for transaction API client failures."""


class APIResponseError(APIClientError):
    """Raised when the API returns an unexpected response."""


class RetryableAPIError(APIClientError):
    """Raised for transient HTTP failures that should be retried."""

    def __init__(
        self,
        message: str,
        status_code: Optional[int] = None,
        retry_after: Optional[float] = None,
    ):
        super().__init__(message)

        self.status_code = status_code
        self.retry_after = retry_after


def _parse_retry_after(value: Optional[str]) -> Optional[float]:
    """
    Parse HTTP Retry-After header.

    Supports:
        - Delay in seconds.
        - HTTP-date format.

    Returns:
        Number of seconds to wait, or None if the
        header is missing or invalid.
    """

    if not value:
        return None

    # Retry-After: 5
    try:
        return max(0.0, float(value))
    except ValueError:
        pass

    # Retry-After: Wed, 21 Oct 2026 07:28:00 GMT
    try:
        retry_datetime = parsedate_to_datetime(value)

        if retry_datetime.tzinfo is None:
            retry_datetime = retry_datetime.replace(
                tzinfo=timezone.utc
            )

        return max(
            0.0,
            (
                retry_datetime - datetime.now(timezone.utc)
            ).total_seconds(),
        )

    except (TypeError, ValueError, OverflowError):
        logger.warning("Received an invalid Retry-After header.")
        return None


def _retry_wait(retry_state) -> float:
    """
    Calculate retry delay.

    Prefer the server-provided Retry-After value.
    Otherwise use exponential backoff with jitter.
    """

    exception = retry_state.outcome.exception()

    if isinstance(exception, RetryableAPIError):
        if exception.retry_after is not None:
            # Bound unusually large server-provided delays.
            return min(exception.retry_after, 120.0)

    attempt = retry_state.attempt_number

    exponential_delay = min(
        2 ** (attempt - 1),
        30,
    )

    # Full-jitter-style bounded delay.
    return random.uniform(0, exponential_delay)


def _log_retry(retry_state) -> None:
    """Log retry metadata without exposing credentials or payloads."""

    exception = retry_state.outcome.exception()

    logger.warning(
        "Transaction API request failed. "
        "Attempt=%s, ErrorType=%s, RetryDelay=%.2f seconds",
        retry_state.attempt_number,
        type(exception).__name__,
        retry_state.next_action.sleep,
    )


class TransactionsAPIClient:
    """
    Client for extracting transaction records from the REST API.

    Example:
        settings = Settings.from_environment()

        with TransactionsAPIClient(settings) as client:
            for page in client.iter_pages():
                process(page)
    """

    MAX_ATTEMPTS = 5

    def __init__(self, settings: Settings):
        self.settings = settings

        self.base_url = settings.api_base_url.rstrip("/")

        self.endpoint = f"{self.base_url}/transactions"

        self.session = requests.Session()

        self.session.headers.update(
            {
                "apikey": settings.api_key,
                "Authorization": f"Bearer {settings.auth_token}",
                "Accept": "application/json",
                "User-Agent": "senior-de-assignment/1.0",
            }
        )

        logger.info(
            "Transaction API client initialized. "
            "PageSize=%s, Timeout=%s seconds",
            settings.page_size,
            settings.timeout_seconds,
        )

    @retry(
        retry=retry_if_exception_type(
            (
                requests.Timeout,
                requests.ConnectionError,
                RetryableAPIError,
            )
        ),
        wait=_retry_wait,
        stop=stop_after_attempt(MAX_ATTEMPTS),
        before_sleep=_log_retry,
        reraise=True,
    )
    def _get_page(self, params: dict[str, Any]) -> TransactionPage:
        """
        Execute one API request.

        Retries:
            - HTTP 429
            - HTTP 5xx
            - Connection failures
            - Request timeouts

        Other HTTP 4xx responses fail immediately.
        """

        try:
            response = self.session.get(
                self.endpoint,
                params=params,
                timeout=self.settings.timeout_seconds,
            )

        except (requests.Timeout, requests.ConnectionError):
            # Let Tenacity handle transient network failures.
            raise

        except requests.RequestException as exc:
            raise APIClientError(
                "Unexpected transaction API request failure."
            ) from exc

        status_code = response.status_code

        # Retry rate limiting and server-side failures.
        if status_code == 429 or 500 <= status_code <= 599:
            raise RetryableAPIError(
                message=f"Transient API failure: HTTP {status_code}",
                status_code=status_code,
                retry_after=_parse_retry_after(
                    response.headers.get("Retry-After")
                ),
            )

        # HTTP 4xx other than 429 are not retried.
        try:
            response.raise_for_status()

        except requests.HTTPError as exc:
            logger.error(
                "Transaction API returned HTTP %s.",
                status_code,
            )

            raise APIClientError(
                f"Transaction API request rejected: HTTP {status_code}"
            ) from exc

        # Parse API response.
        try:
            records = response.json()

        except ValueError as exc:
            raise APIResponseError(
                "Transaction API returned invalid JSON."
            ) from exc

        if not isinstance(records, list):
            raise APIResponseError(
                "Unexpected API response: expected a JSON array."
            )

        # Ensure each API element is a JSON object.
        if not all(isinstance(record, dict) for record in records):
            raise APIResponseError(
                "Unexpected API response: each transaction "
                "must be a JSON object."
            )

        return records

    def iter_pages(
        self,
        transaction_date_gte: Optional[str] = None,
    ) -> Iterator[TransactionPage]:
        """
        Fetch transactions page-by-page.

        Uses deterministic ordering to make offset pagination
        more predictable.

        Pagination terminates when:
            - API returns an empty page.
            - API returns fewer records than the requested limit.

        Args:
            transaction_date_gte:
                Optional ISO 8601 UTC timestamp used for
                incremental extraction.

        Yields:
            A list of transaction records for each page.
        """

        if transaction_date_gte is not None:
            if not transaction_date_gte.strip():
                raise ValueError(
                    "transaction_date_gte cannot be empty."
                )

        limit = self.settings.page_size
        offset = 0
        page_number = 0

        logger.info(
            "Starting transaction extraction. "
            "Incremental=%s",
            transaction_date_gte is not None,
        )

        while True:
            params: dict[str, Any] = {
                "limit": limit,
                "offset": offset,
                "order": (
                    "transaction_date.asc,"
                    "transaction_id.asc"
                ),
            }

            if transaction_date_gte is not None:
                params["transaction_date"] = (
                    f"gte.{transaction_date_gte}"
                )

            records = self._get_page(params)

            record_count = len(records)

            if record_count == 0:
                logger.info(
                    "No more transactions returned by API. "
                    "FinalOffset=%s",
                    offset,
                )
                break

            page_number += 1

            logger.info(
                "Transaction page fetched. "
                "Page=%s, Offset=%s, Records=%s",
                page_number,
                offset,
                record_count,
            )

            yield records

            # A short page indicates the final API page.
            if record_count < limit:
                logger.info(
                    "Final transaction page reached. "
                    "Pages=%s",
                    page_number,
                )
                break

            offset += limit

        logger.info(
            "Transaction extraction pagination completed. "
            "TotalPages=%s",
            page_number,
        )

    def iter_records(
        self,
        transaction_date_gte: Optional[str] = None,
    ) -> Iterator[TransactionRecord]:
        """
        Flatten paginated responses into individual records.

        Use iter_pages() when downstream processing should
        preserve page/batch boundaries.
        """

        for page in self.iter_pages(transaction_date_gte):
            yield from page

    def close(self) -> None:
        """Close the underlying HTTP session."""

        self.session.close()

        logger.debug("Transaction API session closed.")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        return False
