import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Loads .env for local development.
# Databricks will supply configuration through its runtime environment.
load_dotenv(PROJECT_ROOT / ".env", override=False)


@dataclass(frozen=True)
class Settings:
    api_base_url: str
    api_key: str
    auth_token: str
    page_size: int
    timeout_seconds: int

    @classmethod
    def from_environment(cls):
        required = [
            "ASSESSMENT_API_BASE_URL",
            "ASSESSMENT_API_KEY",
            "ASSESSMENT_AUTH_TOKEN",
        ]

        missing = [
            key for key in required
            if not os.getenv(key, "").strip()
        ]

        if missing:
            raise ValueError(
                f"Missing required environment variables: {missing}"
            )

        page_size = int(os.getenv("API_PAGE_SIZE", "100"))
        timeout = int(os.getenv("API_TIMEOUT_SECONDS", "30"))

        if page_size <= 0 or timeout <= 0:
            raise ValueError("Page size and timeout must be positive.")

        return cls(
            api_base_url=os.environ["ASSESSMENT_API_BASE_URL"].rstrip("/"),
            api_key=os.environ["ASSESSMENT_API_KEY"],
            auth_token=os.environ["ASSESSMENT_AUTH_TOKEN"],
            page_size=page_size,
            timeout_seconds=timeout,
        )