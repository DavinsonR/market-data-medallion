"""Configuration loading: config.yaml + environment (.env)."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

import yaml
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATABASE_URL = "postgresql://mdm@localhost:5433/mdm"

# Trading periods per year, by asset class: crypto trades every calendar day,
# listed markets (equities, FX) roughly 252 business days.
PERIODS_PER_YEAR = {"crypto": 365, "equity": 252, "fx": 252}

# Asset classes without a centralized volume tape.
CLASSES_WITHOUT_VOLUME = frozenset({"fx"})

load_dotenv(REPO_ROOT / ".env")


@dataclass(frozen=True)
class SourcesConfig:
    primary: str
    reconcile: str | None = None

    @property
    def all(self) -> list[str]:
        return [self.primary] + ([self.reconcile] if self.reconcile else [])


@dataclass(frozen=True)
class AssetConfig:
    symbol: str
    asset_class: str  # 'crypto' | 'equity' | 'fx'
    region: str  # 'us' | 'latam' | 'emerging' | 'global'
    name: str
    sources: SourcesConfig
    backfill_start: str
    # Latin American ADRs only: the USDXXX rate of the home currency (e.g. EC -> USDCOP),
    # used by mart_fx_decomposition to split the USD return into company vs currency.
    fx_pair: str | None = None

    @property
    def periods_per_year(self) -> int:
        return PERIODS_PER_YEAR.get(self.asset_class, 252)

    @property
    def has_volume(self) -> bool:
        return self.asset_class not in CLASSES_WITHOUT_VOLUME


@dataclass(frozen=True)
class StrategyConfig:
    name: str
    params: dict[str, Any]
    requires_volume: bool = False

    def applies_to(self, asset: AssetConfig) -> bool:
        """A volume-based strategy is meaningless where no volume exists."""
        return asset.has_volume or not self.requires_volume


@dataclass(frozen=True)
class CombinationsConfig:
    """AND-combinations of the configured strategies.

    Storing an equity curve for every combination would not fit the database's
    free tier, so combinations keep metrics only unless explicitly enabled.
    """

    enabled: bool = True
    store_curves: bool = False


@dataclass(frozen=True)
class BacktestConfig:
    initial_cash: float
    fee_bps: float
    slippage_bps: float
    strategies: list[StrategyConfig]
    combinations: CombinationsConfig = field(default_factory=CombinationsConfig)
    # Share of the series used in-sample; the remainder is never used to select
    # anything, which is what makes the out-of-sample numbers meaningful.
    train_fraction: float = 0.7


@dataclass(frozen=True)
class ExportConfig:
    dir: str
    index_file: str
    per_symbol_dir: str
    equity_curve_max_points: int

    @property
    def index_path(self) -> Path:
        return REPO_ROOT / self.dir / self.index_file

    def symbol_path(self, symbol: str) -> Path:
        return REPO_ROOT / self.dir / self.per_symbol_dir / f"{symbol}.json"


@dataclass(frozen=True)
class AppConfig:
    granularity: str
    assets: list[AssetConfig]
    backtest: BacktestConfig
    export: ExportConfig
    _by_symbol: dict[str, AssetConfig] = field(default_factory=dict, repr=False)

    def asset(self, symbol: str) -> AssetConfig | None:
        return self._by_symbol.get(symbol)

    def strategies_for(self, asset: AssetConfig) -> list[StrategyConfig]:
        return [s for s in self.backtest.strategies if s.applies_to(asset)]


def load_config(path: Path | None = None) -> AppConfig:
    raw = yaml.safe_load((path or REPO_ROOT / "config.yaml").read_text())
    defaults = raw.get("defaults") or {}
    assets = [
        AssetConfig(
            symbol=a["symbol"],
            asset_class=a["asset_class"],
            region=a.get("region", "global"),
            name=a.get("name", a["symbol"]),
            sources=SourcesConfig(**a["sources"]),
            backfill_start=a.get("backfill_start") or defaults["backfill_start"],
            fx_pair=a.get("fx_pair"),
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
            strategies=[
                StrategyConfig(
                    name=s["name"],
                    params=s["params"],
                    requires_volume=s.get("requires_volume", False),
                )
                for s in bt["strategies"]
            ],
            combinations=CombinationsConfig(**(bt.get("combinations") or {})),
            train_fraction=bt.get("train_fraction", 0.7),
        ),
        export=ExportConfig(**raw["export"]),
        _by_symbol={a.symbol: a for a in assets},
    )


def database_url() -> str:
    return os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL)


def tiingo_api_key() -> str | None:
    return os.environ.get("TIINGO_API_KEY") or None


def dbt_env() -> dict[str, str]:
    """Discrete connection vars for dbt, derived from DATABASE_URL.

    ``sslmode`` follows the URL's query string when present; managed providers
    need TLS while the local development server has none.
    """
    u = urlparse(database_url())
    sslmode = parse_qs(u.query).get("sslmode", ["prefer"])[0]
    return {
        "MDM_PG_HOST": u.hostname or "localhost",
        "MDM_PG_PORT": str(u.port or 5432),
        "MDM_PG_USER": unquote(u.username) if u.username else "mdm",
        "MDM_PG_PASSWORD": unquote(u.password) if u.password else "",
        "MDM_PG_DB": unquote((u.path or "/mdm").lstrip("/")),
        "MDM_PG_SSLMODE": sslmode,
    }
