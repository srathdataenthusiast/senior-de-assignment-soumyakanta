import os
from dataclasses import dataclass
from pathlib import Path

import yaml
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[2]

load_dotenv(PROJECT_ROOT / ".env", override=False)


@dataclass(frozen=True)
class Settings:
    environment: str
    paths: dict
    export_sample_outputs: bool

    api_base_url: str | None
    api_key: str | None
    auth_token: str | None

    page_size: int
    timeout_seconds: int

    @classmethod
    def from_environment(cls):
        environment = os.getenv("APP_ENV", "local")

        config_file = PROJECT_ROOT / "config" / f"{environment}.yml"

        with open(config_file, encoding="utf-8") as file:
            config = yaml.safe_load(file)

        paths = config["paths"]

        # Resolve local relative paths against the project root.
        for name, path in paths.items():
            if not path.startswith(("/", "dbfs:/")) and "://" not in path:
                paths[name] = str(PROJECT_ROOT / path)

        return cls(
            environment=environment,
            paths=paths,
            export_sample_outputs=config.get("export_sample_outputs", False),
            api_base_url=os.getenv("ASSESSMENT_API_BASE_URL"),
            api_key=os.getenv("ASSESSMENT_API_KEY"),
            auth_token=os.getenv("ASSESSMENT_AUTH_TOKEN"),
            page_size=int(os.getenv("API_PAGE_SIZE", "100")),
            timeout_seconds=int(os.getenv("API_TIMEOUT_SECONDS", "30")),
        )