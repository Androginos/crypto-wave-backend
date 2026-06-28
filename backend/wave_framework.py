"""
wave_framework.py
-----------------
Elliot 3. dalga hacim onayı, Wyckoff kutu kırılımı, Futures OI ve
hibrit backtest motoru. LRC matematik motoruna dokunmaz.
"""

from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

FUTURES_BASE_URL = "https://fapi.binance.com"
VOLUME_SMA_PERIOD = 20
BB_PERIOD = 20
BB_STD = 2.0
WYCKOFF_WIDTH_LOOKBACK = 50
WYCKOFF_WIDTH_PERCENTILE = 0.25
OI_GROWTH_THRESHOLD = 0.02
BACKTEST_TARGET_PCT = 2.0
BACKTEST_FORWARD_BARS = 20
BACKTEST_MIN_SAMPLES = 3


def _vol_series(df: pd.DataFrame) -> pd.Series:
    if "quote_volume" in df.columns:
        return df["quote_volume"].astype(float)
    return df["volume"].astype(float)


def volume_above_sma(df: pd.DataFrame, period: int = VOLUME_SMA_PERIOD) -> bool:
    """Elliot 3. dalga: son kesinleşmiş mum hacmi > 20 bar Volume SMA."""
    if df is None or len(df) < period + 2:
        return False
    vol = _vol_series(df)
    sma = vol.rolling(window=period, min_periods=period).mean()
    last_vol = float(vol.iloc[-2])
    last_sma = float(sma.iloc[-2])
    if not np.isfinite(last_vol) or not np.isfinite(last_sma) or last_sma <= 0:
        return False
    return last_vol > last_sma


def wyckoff_box_breakout(df: pd.DataFrame) -> bool:
    """Dar Bollinger konsolidasyonu + hacimli yukarı kırılım (Phase C/D SOS)."""
    if df is None or len(df) < max(BB_PERIOD, WYCKOFF_WIDTH_LOOKBACK) + 3:
        return False

    close = df["close"].astype(float)
    vol = _vol_series(df)
    mid = close.rolling(BB_PERIOD, min_periods=BB_PERIOD).mean()
    std = close.rolling(BB_PERIOD, min_periods=BB_PERIOD).std()
    upper = mid + BB_STD * std
    lower = mid - BB_STD * std
    width = (upper - lower) / mid.replace(0, np.nan)
    vol_sma = vol.rolling(VOLUME_SMA_PERIOD, min_periods=VOLUME_SMA_PERIOD).mean()

    idx = -2
    if any(pd.isna(x) for x in (width.iloc[idx], upper.iloc[idx], close.iloc[idx])):
        return False

    hist_width = width.iloc[-WYCKOFF_WIDTH_LOOKBACK - 2 : -2].dropna()
    if len(hist_width) < 10:
        return False

    squeeze_thr = float(np.quantile(hist_width.values, WYCKOFF_WIDTH_PERCENTILE))
    was_squeeze = float(width.iloc[idx - 1]) <= squeeze_thr
    breakout = float(close.iloc[idx]) > float(upper.iloc[idx - 1])
    vol_ok = float(vol.iloc[idx]) > float(vol_sma.iloc[idx]) if pd.notna(vol_sma.iloc[idx]) else False

    return was_squeeze and breakout and vol_ok


def evaluate_wave_framework(
    df: pd.DataFrame,
    oi_rising: bool,
) -> dict[str, Any]:
    """3 katmanlı wave framework değerlendirmesi."""
    vol_ok = volume_above_sma(df)
    box_ok = wyckoff_box_breakout(df)
    approved = vol_ok and box_ok and oi_rising
    return {
        "volume_sma_approved": vol_ok,
        "wyckoff_box_breakout": box_ok,
        "oi_rising": oi_rising,
        "wave_framework_approved": approved,
    }


def compute_hybrid_backtest_win_rate(
    df: pd.DataFrame,
    h_len: int = 300,
    l_len: int = 300,
    *,
    detect_crossover_fn,
    target_pct: float = BACKTEST_TARGET_PCT,
    forward_bars: int = BACKTEST_FORWARD_BARS,
) -> float:
    """Son 350 bar içinde TV LRC crossover sonrası hedef karlılık oranı (%)."""
    from indicators import calculate_linreg_and_dev

    if df is None or len(df) < h_len + forward_bars + 5:
        return 0.0

    work = calculate_linreg_and_dev(df.copy(), h_len=h_len, l_len=l_len)
    needed = max(h_len, l_len)
    vol = _vol_series(work)
    vol_sma = vol.rolling(VOLUME_SMA_PERIOD, min_periods=VOLUME_SMA_PERIOD).mean()
    closes = work["close"].astype(float)

    wins = 0
    total = 0
    scan_end = len(work) - forward_bars

    for i in range(needed, scan_end):
        a_p, b_p = work["a"].iloc[i - 1], work["b"].iloc[i - 1]
        a_c, b_c = work["a"].iloc[i], work["b"].iloc[i]
        if any(pd.isna(x) for x in (a_p, b_p, a_c, b_c)):
            continue

        cross = detect_crossover_fn(a_p, b_p, a_c, b_c)
        if cross != "AL":
            continue

        if not (pd.notna(vol_sma.iloc[i]) and float(vol.iloc[i]) > float(vol_sma.iloc[i])):
            continue

        entry = float(closes.iloc[i])
        if entry <= 0:
            continue
        future = closes.iloc[i + 1 : i + forward_bars + 1]
        if future.empty:
            continue
        peak = float(future.max())
        pnl = ((peak - entry) / entry) * 100.0
        total += 1
        if pnl >= target_pct:
            wins += 1

    if total < BACKTEST_MIN_SAMPLES:
        return 0.0
    return round((wins / total) * 100.0, 1)


async def fetch_oi_rising(
    client: Any,
    symbol: str,
    cache: dict[str, tuple[float, bool]],
    cache_ttl_sec: float = 90.0,
) -> bool:
    """Binance Futures OI'da doğrusal artış (akıllı para girişi) var mı?"""
    import time

    now = time.monotonic()
    cached = cache.get(symbol)
    if cached and (now - cached[0]) < cache_ttl_sec:
        return cached[1]

    rising = False
    try:
        url = f"{FUTURES_BASE_URL}/fapi/v1/openInterestHist"
        params = {"symbol": symbol, "period": "5m", "limit": 12}
        resp = await client.get(url, params=params, timeout=12.0)
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, list) and len(data) >= 4:
                oi_vals = [float(x.get("sumOpenInterest", 0) or 0) for x in data]
                if oi_vals[-1] > 0 and oi_vals[0] > 0:
                    growth = (oi_vals[-1] - oi_vals[0]) / oi_vals[0]
                    rising = growth >= OI_GROWTH_THRESHOLD
    except Exception as exc:  # noqa: BLE001
        logger.debug("OI fetch skip %s: %s", symbol, exc)

    cache[symbol] = (now, rising)
    return rising
