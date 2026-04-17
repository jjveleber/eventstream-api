"""Application configuration using pydantic-settings."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # Database
    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/eventstream"

    # Logging
    log_level: str = "INFO"

    # External API polling intervals (seconds)
    github_poll_interval: int = 60
    hackernews_poll_interval: int = 120
    reddit_poll_interval: int = 120

    # Event retention
    max_events_per_source: int = 1000

    # Rate limiting
    rate_limit_enabled: bool = True
    rate_limit_per_minute: int = 60

    # Authentication (simple API key for now)
    api_key_enabled: bool = False
    api_key: str | None = None

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )


settings = Settings()
