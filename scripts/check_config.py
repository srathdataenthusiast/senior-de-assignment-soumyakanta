from src.kpi_assessment.settings import Settings

settings = Settings.from_environment()

print("Configuration loaded successfully")
print("Page size:", settings.page_size)
print("Timeout:", settings.timeout_seconds)
print("API key configured:", bool(settings.api_key))
print("Auth token configured:", bool(settings.auth_token))