import argparse
import json
import math
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yfinance as yf


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "dash_presentation" / "data"
LOOKBACK_TRADING_DAYS = 20
FETCH_PERIOD = "6mo"
RISK_FREE_RATE = 0.04
VOL_FLOOR = 0.22
OPTION_MULTIPLIER = 100.0
FUTURE_MULTIPLIER = 20.0


def _round_to_increment(value: float, increment: float) -> float:
    if increment <= 0:
        return float(round(value, 2))
    return round(round(value / increment) * increment, 2)


def _year_fraction(days: int) -> float:
    return max(days, 1) / 365.0


def _norm_cdf(value: float) -> float:
    return 0.5 * (1.0 + math.erf(value / math.sqrt(2.0)))


def _norm_pdf(value: float) -> float:
    return math.exp(-0.5 * value * value) / math.sqrt(2.0 * math.pi)


def _third_friday(reference: date) -> date:
    first_day = reference.replace(day=1)
    first_friday_offset = (4 - first_day.weekday()) % 7
    first_friday = first_day + timedelta(days=first_friday_offset)
    return first_friday + timedelta(days=14)


def _next_quarterly_expiry(latest_trade_date: date, days_forward: int = 75) -> date:
    probe = latest_trade_date + timedelta(days=days_forward)
    quarter_month = min(12, (((probe.month - 1) // 3) + 1) * 3)
    year = probe.year
    if quarter_month < probe.month:
        quarter_month += 3
        if quarter_month > 12:
            quarter_month -= 12
            year += 1
    expiry = _third_friday(date(year, quarter_month, 1))
    if expiry <= latest_trade_date:
        quarter_month += 3
        if quarter_month > 12:
            quarter_month -= 12
            year += 1
        expiry = _third_friday(date(year, quarter_month, 1))
    return expiry


def _black_scholes_metrics(
    spot: float,
    strike: float,
    time_to_expiry: float,
    volatility: float,
    right: str,
    risk_free_rate: float = RISK_FREE_RATE,
) -> Dict[str, float]:
    if spot <= 0 or strike <= 0:
        return {"mark": 0.0, "delta": 0.0, "gamma": 0.0, "vega": 0.0, "theta": 0.0}
    sigma = max(volatility, 0.0001)
    tau = max(time_to_expiry, 1.0 / 365.0)
    sqrt_tau = math.sqrt(tau)
    d1 = (math.log(spot / strike) + (risk_free_rate + 0.5 * sigma * sigma) * tau) / (sigma * sqrt_tau)
    d2 = d1 - sigma * sqrt_tau
    discount = math.exp(-risk_free_rate * tau)
    pdf_d1 = _norm_pdf(d1)
    cdf_d1 = _norm_cdf(d1)
    cdf_d2 = _norm_cdf(d2)
    if right == "P":
        mark = strike * discount * _norm_cdf(-d2) - spot * _norm_cdf(-d1)
        delta = cdf_d1 - 1.0
        theta = (
            -(spot * pdf_d1 * sigma) / (2.0 * sqrt_tau)
            + risk_free_rate * strike * discount * _norm_cdf(-d2)
        )
    else:
        mark = spot * cdf_d1 - strike * discount * cdf_d2
        delta = cdf_d1
        theta = (
            -(spot * pdf_d1 * sigma) / (2.0 * sqrt_tau)
            - risk_free_rate * strike * discount * cdf_d2
        )
    gamma = pdf_d1 / (spot * sigma * sqrt_tau)
    vega = spot * pdf_d1 * sqrt_tau
    return {
        "mark": max(mark, 0.05),
        "delta": delta,
        "gamma": gamma,
        "vega": vega,
        "theta": theta,
    }


@dataclass(frozen=True)
class EquityPosition:
    symbol: str
    desk: str
    strategy: str
    sector: str
    quantity: float


@dataclass(frozen=True)
class OptionOverlay:
    symbol: str
    desk: str
    strategy: str
    sector: str
    quantity: float
    right: str
    strike_ratio: float


@dataclass(frozen=True)
class NonEquityPosition:
    market_symbol: str
    display_symbol: str
    desk: str
    strategy: str
    sector: str
    asset_class: str
    instrument_type: str
    quantity: float
    multiplier: float
    beta_override: Optional[float] = None


EQUITY_POSITIONS: List[EquityPosition] = [
    EquityPosition("AAPL", "Mega Cap Tech", "Platform Alpha", "Technology Hardware", 1800),
    EquityPosition("MSFT", "Growth L/S", "Software Compounders", "Software", 920),
    EquityPosition("NVDA", "Semis Delta One", "AI Momentum", "Semiconductors", 1450),
    EquityPosition("AMZN", "Platform Alpha", "Consumer Internet", "Internet Platforms", -720),
    EquityPosition("META", "Platform Alpha", "Ad Tech Carry", "Internet Platforms", 860),
    EquityPosition("GOOGL", "Growth L/S", "Search Quality", "Internet Platforms", -610),
    EquityPosition("AVGO", "Semis Delta One", "Infrastructure Beta", "Semiconductors", 520),
    EquityPosition("AMD", "Semis Delta One", "Relative Value", "Semiconductors", 2250),
    EquityPosition("QCOM", "Semis Delta One", "Handset Hedge", "Semiconductors", -940),
    EquityPosition("NFLX", "Media Tactical", "Streaming Momentum", "Media", 410),
    EquityPosition("TSLA", "Consumer Tactical", "Event Driven", "Automotive", -760),
    EquityPosition("COST", "Consumer Tactical", "Defensive Growth", "Retail", 360),
    EquityPosition("MU", "Semis Delta One", "Memory Cycle", "Semiconductors", 1625),
    EquityPosition("PANW", "Growth L/S", "Cyber Carry", "Cybersecurity", 560),
    EquityPosition("CRWD", "Growth L/S", "High Beta Short", "Cybersecurity", -490),
    EquityPosition("SNOW", "Growth L/S", "Data Platform", "Cloud Software", 630),
]


OPTION_OVERLAYS: List[OptionOverlay] = [
    OptionOverlay("AAPL", "Volatility", "Upside Calls", "Technology Hardware", 130, "C", 1.03),
    OptionOverlay("MSFT", "Volatility", "Upside Calls", "Software", 78, "C", 1.04),
    OptionOverlay("NVDA", "Volatility", "Gamma Overlay", "Semiconductors", 92, "C", 1.06),
    OptionOverlay("AMZN", "Volatility", "Protective Puts", "Internet Platforms", 108, "P", 0.94),
    OptionOverlay("META", "Volatility", "Momentum Calls", "Internet Platforms", 66, "C", 1.05),
    OptionOverlay("GOOGL", "Volatility", "Search Hedge", "Internet Platforms", 88, "P", 0.95),
    OptionOverlay("AMD", "Volatility", "Call Spread Proxy", "Semiconductors", 220, "C", 1.08),
    OptionOverlay("TSLA", "Volatility", "Crash Hedge", "Automotive", 72, "P", 0.90),
]


NON_EQUITY_POSITIONS: List[NonEquityPosition] = [
    NonEquityPosition("QQQ", "QQQ", "Index Overlay", "Core Hedge", "Index ETF", "ETF", "ETF", -3200, 1.0, 1.00),
    NonEquityPosition("TQQQ", "TQQQ", "Index Overlay", "Tactical Beta", "Levered ETF", "ETF", "ETF", 950, 1.0, 3.00),
    NonEquityPosition("SQQQ", "SQQQ", "Index Overlay", "Crash Overlay", "Inverse ETF", "ETF", "ETF", 620, 1.0, -3.00),
    NonEquityPosition("NQ=F", "NQ", "Index Overlay", "Future Hedge", "Nasdaq Future", "Index Future", "Future", -24, FUTURE_MULTIPLIER, 1.05),
]


OUTPUT_COLUMNS = [
    "trade_date",
    "desk",
    "strategy",
    "asset_class",
    "instrument_type",
    "sector",
    "symbol",
    "underlier",
    "instrument",
    "expiry",
    "option_right",
    "strike",
    "multiplier",
    "position_qty",
    "price",
    "prev_price",
    "day_return",
    "price_norm_20d",
    "volume",
    "adv20_shares",
    "realized_vol_20d",
    "market_value",
    "gross_exposure",
    "net_exposure",
    "day_pnl",
    "mtd_pnl",
    "cum_pnl_20d",
    "delta_usd",
    "gamma_usd",
    "vega_usd",
    "theta_usd",
    "beta_adj_exposure",
    "adv_pct",
    "scenario_pnl_1pct",
]

TEXT_OUTPUT_COLUMNS = {
    "trade_date",
    "desk",
    "strategy",
    "asset_class",
    "instrument_type",
    "sector",
    "symbol",
    "underlier",
    "instrument",
    "expiry",
    "option_right",
}


ALL_YAHOO_SYMBOLS = sorted(
    {position.symbol for position in EQUITY_POSITIONS}
    | {position.symbol for position in OPTION_OVERLAYS}
    | {position.market_symbol for position in NON_EQUITY_POSITIONS}
)


def _download_market_history() -> Dict[str, pd.DataFrame]:
    raw = yf.download(
        ALL_YAHOO_SYMBOLS,
        period=FETCH_PERIOD,
        interval="1d",
        auto_adjust=False,
        progress=False,
        threads=True,
        group_by="ticker",
    )
    if raw.empty:
        raise RuntimeError("Yahoo Finance returned no market history.")
    history_by_symbol: Dict[str, pd.DataFrame] = {}
    for symbol in ALL_YAHOO_SYMBOLS:
        if symbol not in raw.columns.get_level_values(0):
            raise RuntimeError(f"Yahoo Finance missing expected symbol: {symbol}")
        frame = raw[symbol].copy()
        frame.columns = [str(column).lower().replace(" ", "_") for column in frame.columns]
        frame = frame.rename(columns={"adj_close": "adj_close"})
        frame.index = pd.to_datetime(frame.index).tz_localize(None)
        frame = frame.reset_index(names="trade_date")
        frame = frame.dropna(subset=["close"]).copy()
        if frame.empty:
            raise RuntimeError(f"Yahoo Finance returned empty price history for {symbol}")
        frame["trade_date"] = pd.to_datetime(frame["trade_date"]).dt.date
        history_by_symbol[symbol] = frame
    return history_by_symbol


def _coerce_output_frame(frame: pd.DataFrame) -> pd.DataFrame:
    working = frame.copy()
    for column in OUTPUT_COLUMNS:
        if column not in working.columns:
            working[column] = "" if column in TEXT_OUTPUT_COLUMNS else np.nan
    for column in OUTPUT_COLUMNS:
        if column in TEXT_OUTPUT_COLUMNS:
            working[column] = working[column].fillna("").astype(str)
        else:
            working[column] = pd.to_numeric(working[column], errors="coerce")
    return working[OUTPUT_COLUMNS]


def _compute_symbol_features(frame: pd.DataFrame, benchmark_returns: pd.Series) -> pd.DataFrame:
    working = frame.copy().sort_values("trade_date").reset_index(drop=True)
    working["close"] = working["close"].astype(float)
    working["volume"] = working["volume"].fillna(0).astype(float)
    working["prev_close"] = working["close"].shift(1).fillna(working["close"])
    working["ret_1d"] = working["close"].pct_change().fillna(0.0)
    working["adv20_shares"] = working["volume"].rolling(20, min_periods=5).mean().bfill().fillna(working["volume"].replace(0, np.nan).median())
    working["adv20_shares"] = working["adv20_shares"].fillna(1.0).clip(lower=1.0)
    working["realized_vol_20d"] = working["ret_1d"].rolling(20, min_periods=5).std().fillna(0.0) * math.sqrt(252.0)
    working["realized_vol_20d"] = working["realized_vol_20d"].fillna(VOL_FLOOR).clip(lower=VOL_FLOOR, upper=1.25)
    local_returns = working.set_index("trade_date")["ret_1d"]
    aligned = pd.concat(
        [local_returns.rename("asset"), benchmark_returns.rename("benchmark")],
        axis=1,
        join="inner",
    ).dropna()
    if len(aligned) >= 5 and aligned["benchmark"].var() > 0:
        beta = float(aligned["asset"].cov(aligned["benchmark"]) / aligned["benchmark"].var())
    else:
        beta = 1.0
    working["beta_to_qqq"] = beta
    return working


def _build_equity_rows(symbol_frames: Dict[str, pd.DataFrame], trimmed_dates: List[date]) -> pd.DataFrame:
    records: List[Dict[str, object]] = []
    trimmed_date_set = set(trimmed_dates)
    for position in EQUITY_POSITIONS:
        frame = symbol_frames[position.symbol]
        frame = frame[frame["trade_date"].isin(trimmed_date_set)].copy()
        first_close = float(frame["close"].iloc[0])
        frame["price_norm_20d"] = 100.0 * frame["close"] / max(first_close, 0.01)
        frame["day_pnl"] = position.quantity * (frame["close"] - frame["prev_close"])
        frame["mtd_pnl"] = frame.groupby(frame["trade_date"].map(lambda value: (value.year, value.month)))["day_pnl"].cumsum()
        frame["cum_pnl_lookback"] = frame["day_pnl"].cumsum()
        frame["market_value"] = position.quantity * frame["close"]
        frame["gross_exposure"] = frame["market_value"].abs()
        frame["net_exposure"] = frame["market_value"]
        frame["delta_usd"] = frame["market_value"]
        frame["gamma_usd"] = 0.0
        frame["vega_usd"] = 0.0
        frame["theta_usd"] = 0.0
        frame["beta_adj_exposure"] = frame["market_value"] * frame["beta_to_qqq"]
        adv_dollar = frame["adv20_shares"] * frame["close"]
        frame["adv_pct"] = np.where(adv_dollar > 0, frame["gross_exposure"] / adv_dollar, 0.0)
        frame["scenario_pnl_1pct"] = frame["delta_usd"] * 0.01
        for row in frame.itertuples(index=False):
            records.append(
                {
                    "trade_date": row.trade_date.isoformat(),
                    "desk": position.desk,
                    "strategy": position.strategy,
                    "asset_class": "Cash Equity",
                    "instrument_type": "Equity",
                    "sector": position.sector,
                    "symbol": position.symbol,
                    "underlier": position.symbol,
                    "instrument": f"{position.symbol} US Equity",
                    "expiry": "",
                    "option_right": "",
                    "strike": np.nan,
                    "multiplier": 1.0,
                    "position_qty": float(position.quantity),
                    "price": float(row.close),
                    "prev_price": float(row.prev_close),
                    "day_return": float(row.ret_1d),
                    "price_norm_20d": float(row.price_norm_20d),
                    "volume": float(row.volume),
                    "adv20_shares": float(row.adv20_shares),
                    "realized_vol_20d": float(row.realized_vol_20d),
                    "market_value": float(row.market_value),
                    "gross_exposure": float(row.gross_exposure),
                    "net_exposure": float(row.net_exposure),
                    "day_pnl": float(row.day_pnl),
                    "mtd_pnl": float(row.mtd_pnl),
                    "cum_pnl_20d": float(row.cum_pnl_lookback),
                    "delta_usd": float(row.delta_usd),
                    "gamma_usd": float(row.gamma_usd),
                    "vega_usd": float(row.vega_usd),
                    "theta_usd": float(row.theta_usd),
                    "beta_adj_exposure": float(row.beta_adj_exposure),
                    "adv_pct": float(row.adv_pct),
                    "scenario_pnl_1pct": float(row.scenario_pnl_1pct),
                }
            )
    return pd.DataFrame.from_records(records)


def _build_option_rows(
    symbol_frames: Dict[str, pd.DataFrame],
    trimmed_dates: List[date],
    latest_trade_date: date,
) -> pd.DataFrame:
    records: List[Dict[str, object]] = []
    trimmed_date_set = set(trimmed_dates)
    expiry = _next_quarterly_expiry(latest_trade_date)
    for overlay in OPTION_OVERLAYS:
        frame = symbol_frames[overlay.symbol]
        frame = frame[frame["trade_date"].isin(trimmed_date_set)].copy().reset_index(drop=True)
        latest_spot = float(frame["close"].iloc[-1])
        strike_increment = 5.0 if latest_spot >= 100 else 2.5
        strike = _round_to_increment(latest_spot * overlay.strike_ratio, strike_increment)
        marks: List[float] = []
        deltas: List[float] = []
        gammas: List[float] = []
        vegas: List[float] = []
        thetas: List[float] = []
        for row in frame.itertuples(index=False):
            days_to_expiry = max((expiry - row.trade_date).days, 1)
            metrics = _black_scholes_metrics(
                spot=float(row.close),
                strike=strike,
                time_to_expiry=_year_fraction(days_to_expiry),
                volatility=max(float(row.realized_vol_20d), VOL_FLOOR),
                right=overlay.right,
            )
            marks.append(metrics["mark"])
            deltas.append(metrics["delta"])
            gammas.append(metrics["gamma"])
            vegas.append(metrics["vega"])
            thetas.append(metrics["theta"])
        frame["mark"] = marks
        frame["prev_mark"] = frame["mark"].shift(1).fillna(frame["mark"])
        frame["delta_ratio"] = deltas
        frame["gamma_ratio"] = gammas
        frame["vega_ratio"] = vegas
        frame["theta_ratio"] = thetas
        frame["day_pnl"] = overlay.quantity * OPTION_MULTIPLIER * (frame["mark"] - frame["prev_mark"])
        frame["mtd_pnl"] = frame.groupby(frame["trade_date"].map(lambda value: (value.year, value.month)))["day_pnl"].cumsum()
        frame["cum_pnl_lookback"] = frame["day_pnl"].cumsum()
        frame["market_value"] = overlay.quantity * OPTION_MULTIPLIER * frame["mark"]
        frame["gross_exposure"] = frame["market_value"].abs()
        frame["net_exposure"] = frame["market_value"]
        frame["delta_usd"] = overlay.quantity * OPTION_MULTIPLIER * frame["delta_ratio"] * frame["close"]
        frame["gamma_usd"] = overlay.quantity * OPTION_MULTIPLIER * frame["gamma_ratio"] * (frame["close"] ** 2) * 0.01
        frame["vega_usd"] = overlay.quantity * OPTION_MULTIPLIER * frame["vega_ratio"] * 0.01
        frame["theta_usd"] = overlay.quantity * OPTION_MULTIPLIER * frame["theta_ratio"] / 252.0
        frame["beta_adj_exposure"] = frame["delta_usd"] * frame["beta_to_qqq"]
        adv_dollar = frame["adv20_shares"] * frame["close"]
        delta_notional = frame["delta_usd"].abs()
        frame["adv_pct"] = np.where(adv_dollar > 0, delta_notional / adv_dollar, 0.0)
        frame["scenario_pnl_1pct"] = frame["delta_usd"] * 0.01 + 0.5 * frame["gamma_usd"] * 0.0001
        option_code = f"{overlay.symbol} {expiry.strftime('%d%b%y').upper()} {strike:.0f}{overlay.right}"
        for row in frame.itertuples(index=False):
            records.append(
                {
                    "trade_date": row.trade_date.isoformat(),
                    "desk": overlay.desk,
                    "strategy": overlay.strategy,
                    "asset_class": "Single-Stock Option",
                    "instrument_type": "Option",
                    "sector": overlay.sector,
                    "symbol": overlay.symbol,
                    "underlier": overlay.symbol,
                    "instrument": option_code,
                    "expiry": expiry.isoformat(),
                    "option_right": overlay.right,
                    "strike": float(strike),
                    "multiplier": OPTION_MULTIPLIER,
                    "position_qty": float(overlay.quantity),
                    "price": float(row.mark),
                    "prev_price": float(row.prev_mark),
                    "day_return": float((row.mark / row.prev_mark) - 1.0) if row.prev_mark else 0.0,
                    "price_norm_20d": float(100.0 * row.mark / max(frame["mark"].iloc[0], 0.05)),
                    "volume": float(row.volume),
                    "adv20_shares": float(row.adv20_shares),
                    "realized_vol_20d": float(row.realized_vol_20d),
                    "market_value": float(row.market_value),
                    "gross_exposure": float(row.gross_exposure),
                    "net_exposure": float(row.net_exposure),
                    "day_pnl": float(row.day_pnl),
                    "mtd_pnl": float(row.mtd_pnl),
                    "cum_pnl_20d": float(row.cum_pnl_lookback),
                    "delta_usd": float(row.delta_usd),
                    "gamma_usd": float(row.gamma_usd),
                    "vega_usd": float(row.vega_usd),
                    "theta_usd": float(row.theta_usd),
                    "beta_adj_exposure": float(row.beta_adj_exposure),
                    "adv_pct": float(row.adv_pct),
                    "scenario_pnl_1pct": float(row.scenario_pnl_1pct),
                }
            )
    return pd.DataFrame.from_records(records)


def _build_non_equity_rows(symbol_frames: Dict[str, pd.DataFrame], trimmed_dates: List[date]) -> pd.DataFrame:
    records: List[Dict[str, object]] = []
    trimmed_date_set = set(trimmed_dates)
    for position in NON_EQUITY_POSITIONS:
        frame = symbol_frames[position.market_symbol]
        frame = frame[frame["trade_date"].isin(trimmed_date_set)].copy()
        first_close = float(frame["close"].iloc[0])
        frame["price_norm_20d"] = 100.0 * frame["close"] / max(first_close, 0.01)
        frame["day_pnl"] = position.quantity * position.multiplier * (frame["close"] - frame["prev_close"])
        frame["mtd_pnl"] = frame.groupby(frame["trade_date"].map(lambda value: (value.year, value.month)))["day_pnl"].cumsum()
        frame["cum_pnl_lookback"] = frame["day_pnl"].cumsum()
        frame["market_value"] = position.quantity * position.multiplier * frame["close"]
        frame["gross_exposure"] = frame["market_value"].abs()
        frame["net_exposure"] = frame["market_value"]
        frame["delta_usd"] = frame["market_value"]
        frame["gamma_usd"] = 0.0
        frame["vega_usd"] = 0.0
        frame["theta_usd"] = 0.0
        beta = position.beta_override if position.beta_override is not None else frame["beta_to_qqq"]
        frame["beta_adj_exposure"] = frame["market_value"] * beta
        adv_dollar = frame["adv20_shares"] * frame["close"] * position.multiplier
        frame["adv_pct"] = np.where(adv_dollar > 0, frame["gross_exposure"] / adv_dollar, 0.0)
        frame["scenario_pnl_1pct"] = frame["delta_usd"] * 0.01
        for row in frame.itertuples(index=False):
            instrument_name = f"{position.display_symbol} {position.instrument_type}"
            if position.instrument_type == "Future":
                instrument_name = "NQ Front Future"
            records.append(
                {
                    "trade_date": row.trade_date.isoformat(),
                    "desk": position.desk,
                    "strategy": position.strategy,
                    "asset_class": position.asset_class,
                    "instrument_type": position.instrument_type,
                    "sector": position.sector,
                    "symbol": position.display_symbol,
                    "underlier": position.display_symbol,
                    "instrument": instrument_name,
                    "expiry": "",
                    "option_right": "",
                    "strike": np.nan,
                    "multiplier": float(position.multiplier),
                    "position_qty": float(position.quantity),
                    "price": float(row.close),
                    "prev_price": float(row.prev_close),
                    "day_return": float(row.ret_1d),
                    "price_norm_20d": float(row.price_norm_20d),
                    "volume": float(row.volume),
                    "adv20_shares": float(row.adv20_shares),
                    "realized_vol_20d": float(row.realized_vol_20d),
                    "market_value": float(row.market_value),
                    "gross_exposure": float(row.gross_exposure),
                    "net_exposure": float(row.net_exposure),
                    "day_pnl": float(row.day_pnl),
                    "mtd_pnl": float(row.mtd_pnl),
                    "cum_pnl_20d": float(row.cum_pnl_lookback),
                    "delta_usd": float(row.delta_usd),
                    "gamma_usd": float(row.gamma_usd),
                    "vega_usd": float(row.vega_usd),
                    "theta_usd": float(row.theta_usd),
                    "beta_adj_exposure": float(row.beta_adj_exposure),
                    "adv_pct": float(row.adv_pct),
                    "scenario_pnl_1pct": float(row.scenario_pnl_1pct),
                }
            )
    return pd.DataFrame.from_records(records)


def build_dataset() -> Dict[str, object]:
    history = _download_market_history()
    qqq_returns = history["QQQ"].sort_values("trade_date").set_index("trade_date")["close"].pct_change().fillna(0.0)
    feature_frames = {
        symbol: _compute_symbol_features(frame, qqq_returns)
        for symbol, frame in history.items()
    }
    shared_dates = sorted(set.intersection(*(set(frame["trade_date"]) for frame in feature_frames.values())))
    if len(shared_dates) < LOOKBACK_TRADING_DAYS:
        raise RuntimeError(
            f"Only found {len(shared_dates)} shared trading dates across the ticker universe; need at least {LOOKBACK_TRADING_DAYS}."
        )
    trimmed_dates = shared_dates[-LOOKBACK_TRADING_DAYS:]
    latest_trade_date = trimmed_dates[-1]
    equity_rows = _coerce_output_frame(_build_equity_rows(feature_frames, trimmed_dates))
    option_rows = _coerce_output_frame(_build_option_rows(feature_frames, trimmed_dates, latest_trade_date))
    non_equity_rows = _coerce_output_frame(_build_non_equity_rows(feature_frames, trimmed_dates))
    full_history = pd.concat([equity_rows, option_rows, non_equity_rows], ignore_index=True)
    full_history = full_history.sort_values(
        ["trade_date", "desk", "asset_class", "sector", "symbol", "instrument"]
    ).reset_index(drop=True)
    latest_snapshot = full_history[full_history["trade_date"] == latest_trade_date.isoformat()].copy()
    metadata = {
        "dataset_name": "nasdaq_trader_demo",
        "source": "Yahoo Finance daily history via yfinance with generated portfolio/risk overlays",
        "generated_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "history_period": FETCH_PERIOD,
        "lookback_trading_days": LOOKBACK_TRADING_DAYS,
        "history_start": trimmed_dates[0].isoformat(),
        "history_end": latest_trade_date.isoformat(),
        "latest_trade_date": latest_trade_date.isoformat(),
        "row_count_history": int(len(full_history)),
        "row_count_snapshot": int(len(latest_snapshot)),
        "instrument_count": int(full_history[["asset_class", "instrument"]].drop_duplicates().shape[0]),
        "symbol_count": int(full_history["symbol"].nunique()),
        "symbols": sorted(full_history["symbol"].dropna().unique().tolist()),
        "fields": list(full_history.columns),
    }
    return {
        "history": full_history,
        "snapshot": latest_snapshot,
        "metadata": metadata,
    }


def write_outputs(output_dir: Path) -> Dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    dataset = build_dataset()
    history_path = output_dir / "nasdaq_trader_demo_history.parquet"
    snapshot_path = output_dir / "nasdaq_trader_demo_snapshot.parquet"
    metadata_path = output_dir / "nasdaq_trader_demo_metadata.json"
    pq.write_table(pa.Table.from_pandas(dataset["history"], preserve_index=False), history_path)
    pq.write_table(pa.Table.from_pandas(dataset["snapshot"], preserve_index=False), snapshot_path)
    metadata_path.write_text(json.dumps(dataset["metadata"], indent=2), encoding="utf-8")
    return {
        "history": history_path,
        "snapshot": snapshot_path,
        "metadata": metadata_path,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate a Yahoo-backed NASDAQ multi-asset trader demo dataset.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Directory to write output parquet/json files. Default: {DEFAULT_OUTPUT_DIR}",
    )
    args = parser.parse_args()
    outputs = write_outputs(args.output_dir)
    for name, path in outputs.items():
        print(f"{name}: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
