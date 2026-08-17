"""Run backtests for all configured assets against current gold data.

Usage: python -m pipeline.backtest
"""

from pipeline.config import load_config
from pipeline.flows import backtest_symbol


def main() -> None:
    cfg = load_config()
    for asset in cfg.assets:
        backtest_symbol.fn(asset, cfg)


if __name__ == "__main__":
    main()
