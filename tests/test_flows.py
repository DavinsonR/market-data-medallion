"""Tests for the daily flow's failure gates.

These exist because of FALLO-26: for four consecutive nights the flow ingested
nothing (every Tiingo call was refused 403), yet still ran dbt, still wrote
exports/index.json, and still finished green. The website reads generated_at
from that file, so it advertised a fresh refresh over four-day-old prices —
a failure that looked exactly like health.
"""

from __future__ import annotations

import logging
from typing import Any

import pytest

from pipeline import flows
from pipeline.config import AssetConfig, SourcesConfig
from pipeline.models import IngestResult


def _asset(symbol: str, source: str = "tiingo") -> AssetConfig:
    return AssetConfig(
        symbol=symbol,
        asset_class="equity",
        region="us",
        name=symbol,
        sources=SourcesConfig(primary=source),
        backfill_start="2022-01-01",
    )


def _result(source: str, symbol: str, **flags: Any) -> IngestResult:
    return IngestResult(
        source=source,
        symbol=symbol,
        granularity="1d",
        window_start=None,
        window_end=None,
        rows_fetched=0,
        rows_inserted=0,
        status=flags.pop("status", "failed"),
        error=flags.pop("error", None),
        **flags,
    )


class _Cfg:
    granularity = "1d"

    def __init__(self, assets: list[AssetConfig]) -> None:
        self.assets = assets


def test_auth_refusal_short_circuits_the_rest_of_the_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One 403 is enough. The other symbols share the credential, so asking them
    can only produce the same rejection — 46 symbols x 3 retries x 30s of it."""
    calls: list[str] = []

    def fake_ingest(source_name: str, asset: AssetConfig, granularity: str) -> IngestResult:
        calls.append(f"{source_name}/{asset.symbol}")
        return _result(source_name, asset.symbol, auth_failed=True, error="HTTP 403")

    monkeypatch.setattr(flows, "ingest_asset", fake_ingest)
    cfg = _Cfg([_asset(s) for s in ("SPY", "QQQ", "DIA", "IWM")])

    succeeded, failures, deferred, auth_failed = flows._ingest_all(
        cfg, logging.getLogger("test")
    )

    assert calls == ["tiingo/SPY"], "only the first symbol may pay for the bad credential"
    assert succeeded == 0
    assert failures == ["tiingo/SPY"]
    assert deferred == []
    assert auth_failed == {"tiingo": "HTTP 403"}


def test_auth_failure_on_one_source_does_not_block_another(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Coinbase needs no key. Its symbols must still ingest when Tiingo's is wrong."""

    def fake_ingest(source_name: str, asset: AssetConfig, granularity: str) -> IngestResult:
        if source_name == "tiingo":
            return _result(source_name, asset.symbol, auth_failed=True, error="HTTP 403")
        return _result(source_name, asset.symbol, status="success")

    monkeypatch.setattr(flows, "ingest_asset", fake_ingest)
    cfg = _Cfg([_asset("SPY"), _asset("BTC-USD", source="coinbase"), _asset("QQQ")])

    succeeded, _failures, _deferred, auth_failed = flows._ingest_all(
        cfg, logging.getLogger("test")
    )

    assert succeeded == 1, "the healthy source must still run"
    assert set(auth_failed) == {"tiingo"}


def test_flow_aborts_before_dbt_and_export_when_a_credential_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The gate that FALLO-26 was missing: no new data, no publish."""
    ran: list[str] = []

    monkeypatch.setattr(flows, "load_config", lambda: _Cfg([_asset("SPY")]))
    monkeypatch.setattr(
        flows,
        "_ingest_all",
        lambda cfg, logger: (0, ["tiingo/SPY"], [], {"tiingo": "HTTP 403: credential rejected"}),
    )
    monkeypatch.setattr(flows, "run_dbt", lambda: ran.append("dbt"))
    monkeypatch.setattr(flows.export, "export_json", lambda cfg: ran.append("export"))

    with pytest.raises(RuntimeError, match="rejected our credentials"):
        flows.daily_flow.fn()

    assert ran == [], "a run with no new data must not overwrite the published export"


def test_rate_limit_still_defers_rather_than_aborting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A 429 is normal on a free tier: defer and resume tomorrow, do not go red."""

    def fake_ingest(source_name: str, asset: AssetConfig, granularity: str) -> IngestResult:
        return _result(source_name, asset.symbol, rate_limited=True)

    monkeypatch.setattr(flows, "ingest_asset", fake_ingest)
    cfg = _Cfg([_asset(s) for s in ("SPY", "QQQ", "DIA")])

    succeeded, failures, deferred, auth_failed = flows._ingest_all(
        cfg, logging.getLogger("test")
    )

    assert auth_failed == {}, "a quota refusal is not a credential problem"
    assert deferred == ["tiingo/SPY", "tiingo/QQQ", "tiingo/DIA"]
    assert failures == [] and succeeded == 0
