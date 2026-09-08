"""Flexible, YAML-driven configuration loader with environment variable overrides."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    def load_dotenv(*args: Any, **kwargs: Any) -> bool:
        return False

try:
    import yaml
except ImportError:  # pragma: no cover
    yaml = None

# Load .env from project root if present
ROOT_DIR = Path(__file__).resolve().parents[2]
load_dotenv(dotenv_path=ROOT_DIR / ".env")


def _get_env(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    return value if value not in (None, "") else default


def _interpolate_env(text: str) -> str:
    """Interpolate ${VAR} or ${VAR:-default} patterns using environment variables."""
    pattern = re.compile(r"\$\{([^}^{]+)\}")

    def replace(match: re.Match) -> str:
        expr = match.group(1)
        if ":-" in expr:
            var_name, default_val = expr.split(":-", 1)
        else:
            var_name, default_val = expr, ""
        return os.getenv(var_name, default_val)

    return pattern.sub(replace, text)


@dataclass
class TheOddsApiConfig:
    base_url: str = "https://api.the-odds-api.com"
    regions: str = "us"
    markets: str = "h2h,spreads,totals"
    odds_format: str = "american"
    bookmakers: list[str] = field(default_factory=lambda: ["fanduel"])
    sports: list[str] = field(
        default_factory=lambda: ["americanfootball_nfl", "americanfootball_ncaaf"]
    )


@dataclass
class GcpTablesConfig:
    odds_table: str = "daily_odds_data"
    metadata_table: str = "game_metadata"
    scores_table: str = "game_results"


@dataclass
class GcpStorageConfig:
    bucket_name: str = "odds-data"
    odds_prefix: str = "betting_odds"


@dataclass
class GcpConfig:
    project_id: str = "odds-tracker-402301"
    dataset_id: str = "nfl_data"
    tables: GcpTablesConfig = field(default_factory=GcpTablesConfig)
    storage: GcpStorageConfig = field(default_factory=GcpStorageConfig)

    @property
    def full_odds_table(self) -> str:
        return f"{self.project_id}.{self.dataset_id}.{self.tables.odds_table}"


@dataclass
class IngestionConfig:
    default_destination: str = "bq"
    test_mode: bool = False
    monthly_quota_limit: int = 500
    alert_quota_remaining: int = 50


@dataclass
class AppConfig:
    name: str = "sports-betting-odds-tracker"
    environment: str = "development"
    the_odds_api: TheOddsApiConfig = field(default_factory=TheOddsApiConfig)
    gcp: GcpConfig = field(default_factory=GcpConfig)
    ingestion: IngestionConfig = field(default_factory=IngestionConfig)


def load_config(config_path: str | Path | None = None) -> AppConfig:
    """Load config from YAML file, apply environment variable overrides, and return AppConfig."""
    target_path = (
        Path(config_path)
        if config_path
        else Path(os.getenv("CONFIG_PATH", ROOT_DIR / "config/config.yaml"))
    )

    data: dict[str, Any] = {}
    if target_path.exists() and yaml is not None:
        try:
            raw_text = target_path.read_text(encoding="utf-8")
            interpolated = _interpolate_env(raw_text)
            parsed = yaml.safe_load(interpolated)
            if isinstance(parsed, dict):
                data = parsed
        except Exception as exc:  # pragma: no cover
            print(f"Warning: Failed to parse {target_path}: {exc}. Using defaults.")

    app_data = data.get("app", {})
    api_data = data.get("the_odds_api", {})
    gcp_data = data.get("gcp", {})
    ingest_data = data.get("ingestion", {})

    # Build TheOddsApiConfig
    api_cfg = TheOddsApiConfig(
        base_url=api_data.get("base_url", "https://api.the-odds-api.com"),
        regions=api_data.get("regions", "us"),
        markets=api_data.get("markets", "h2h,spreads,totals"),
        odds_format=api_data.get("odds_format", "american"),
        bookmakers=api_data.get("bookmakers", ["fanduel"]),
        sports=api_data.get("sports", ["americanfootball_nfl", "americanfootball_ncaaf"]),
    )

    # Build GCP config with environment overrides
    tables_data = gcp_data.get("tables", {})
    storage_data = gcp_data.get("storage", {})

    project_id = _get_env("GCP_PROJECT_ID", gcp_data.get("project_id", "odds-tracker-402301"))
    dataset_id = _get_env(
        "BIGQUERY_DATASET_ID",
        _get_env("DATASET_ID", gcp_data.get("dataset_id", "nfl_data")),
    )
    bucket_name = _get_env(
        "GCP_STORAGE_BUCKET",
        storage_data.get("bucket_name", "odds-data"),
    )

    gcp_cfg = GcpConfig(
        project_id=project_id,
        dataset_id=dataset_id,
        tables=GcpTablesConfig(
            odds_table=tables_data.get("odds_table", "daily_odds_data"),
            metadata_table=tables_data.get("metadata_table", "game_metadata"),
            scores_table=tables_data.get("scores_table", "game_results"),
        ),
        storage=GcpStorageConfig(
            bucket_name=bucket_name,
            odds_prefix=storage_data.get("odds_prefix", "betting_odds"),
        ),
    )

    # Build Ingestion config
    ingest_cfg = IngestionConfig(
        default_destination=ingest_data.get("default_destination", "bq"),
        test_mode=bool(ingest_data.get("test_mode", False)),
        monthly_quota_limit=int(ingest_data.get("monthly_quota_limit", 500)),
        alert_quota_remaining=int(ingest_data.get("alert_quota_remaining", 50)),
    )

    return AppConfig(
        name=app_data.get("name", "sports-betting-odds-tracker"),
        environment=_get_env("APP_ENV", app_data.get("environment", "development")),
        the_odds_api=api_cfg,
        gcp=gcp_cfg,
        ingestion=ingest_cfg,
    )


def get_gcp_credentials():
    credentials_path = _get_env("GOOGLE_APPLICATION_CREDENTIALS")
    if credentials_path and Path(credentials_path).exists():
        from google.oauth2 import service_account

        return service_account.Credentials.from_service_account_file(credentials_path)

    try:
        from google.auth import default

        credentials, _ = default()
        return credentials
    except Exception:  # pragma: no cover
        return None


# Global settings singleton
settings = load_config()

# Backward-compatible variable exports
my_api_key = _get_env("THE_ODDS_API_KEY") or _get_env("API_KEY")
GCP_PROJECT_ID = settings.gcp.project_id
DATASET_ID = settings.gcp.dataset_id
gcp_service_accnt = get_gcp_credentials()
