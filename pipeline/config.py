"""Configuration loading: config.yaml + environment (.env)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

import yaml
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATABASE_URL = "postgresql://mdm@localhost:5433/mdm"

load_dotenv(REPO_ROOT / ".env")


@dataclass(frozen=True)
class SourcesConfig:
    primary: str
    reconcile: str | None = None


@dataclass(frozen=True)
class AssetConfig:
    symbol: str
    asset_class: str  # 'crypto' | 'equity'
    sources: SourcesConfig
    backfill_start: str

    @property
    def periods_per_year(self) -> int:
        return 365 if self.asset_class == "crypto" else 252


@dataclass(frozen=True)
class StrategyConfig:
    name: str
    params: dict[str, Any]


@dataclass(frozen=True)
class BacktestConfig:
    initial_cash: float
    fee_bps: float
    slippage_bps: float
    strategies: list[StrategyConfig]


@dataclass(frozen=True)
class ExportConfig:
    path: str
    equity_curve_max_points: int


@dataclass(frozen=True)
class AppConfig:
    granularity: str
    assets: list[AssetConfig]
    backtest: BacktestConfig
    export: ExportConfig


def load_config(path: Path | None = None) -> AppConfig:
    raw = yaml.safe_load((path or REPO_ROOT / "config.yaml").read_text())
    assets = [
        AssetConfig(
            symbol=a["symbol"],
            asset_class=a["asset_class"],
            sources=SourcesConfig(**a["sources"]),
            backfill_start=a["backfill_start"],
        )
        for a in raw["assets"]
    ]
    bt = raw["backtest"]
    return AppConfig(
        granularity=raw["granularity"],
        assets=assets,
        backtest=BacktestConfig(
            initial_cash=bt["initial_cash"],
            fee_bps=bt["fee_bps"],
            slippage_bps=bt["slippage_bps"],
            strategies=[StrategyConfig(s["name"], s["params"]) for s in bt["strategies"]],
        ),
        export=ExportConfig(**raw["export"]),
    )


def database_url() -> str:
    return os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL)


def tiingo_api_key() -> str | None:
    return os.environ.get("TIINGO_API_KEY") or None


def dbt_env() -> dict[str, str]:
    """Discrete connection vars for dbt, derived from DATABASE_URL."""
    u = urlparse(database_url())
    return {
        "MDM_PG_HOST": u.hostname or "localhost",
        "MDM_PG_PORT": str(u.port or 5432),
        "MDM_PG_USER": unquote(u.username) if u.username else "mdm",
        "MDM_PG_PASSWORD": unquote(u.password) if u.password else "",
        "MDM_PG_DB": unquote((u.path or "/mdm").lstrip("/")),
    }
