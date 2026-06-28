"""
indicators.py
-------------
Doğrusal regresyon tabanlı crossover ve açı (intersection angle)
hesaplama motoru.

CRITICAL:
    Bu dosyadaki matematik motoru TradingView Pine Script ile %100 uyumlu
    olacak şekilde önceden optimize edilmiştir. `calculate_linreg_and_dev`
    ve `check_signals` fonksiyonlarının formülleri, sabitleri ve algoritmik
    sırası KESİNLİKLE değiştirilmemelidir.
"""

import numpy as np
import pandas as pd


def calculate_linreg_and_dev(df, h_len=300, l_len=300):
    close_len = len(df)
    x_h = np.arange(h_len)
    x_l = np.arange(l_len)
    
    a_vals = np.full(close_len, np.nan)
    b_vals = np.full(close_len, np.nan)
    
    high_arr = df['high'].values
    low_arr = df['low'].values
    
    for i in range(max(h_len, l_len) - 1, close_len):
        y_h = high_arr[i - h_len + 1 : i + 1]
        slope_h, intercept_h = np.polyfit(x_h, y_h, 1)
        a_vals[i] = slope_h * (h_len - 1) + intercept_h
        
        y_l = low_arr[i - l_len + 1 : i + 1]
        slope_l, intercept_l = np.polyfit(x_l, y_l, 1)
        b_vals[i] = slope_l * (l_len - 1) + intercept_l

    df['a'] = a_vals
    df['b'] = b_vals

    sma_high = df['high'].rolling(window=h_len).mean()
    sma_low = df['low'].rolling(window=l_len).mean()
    
    def calc_mad(src_series, sma_series, length):
        mad_vals = np.full(len(src_series), np.nan)
        for i in range(length - 1, len(src_series)):
            window_src = src_series[i - length + 1 : i + 1]
            current_sma = sma_series[i]
            mad_vals[i] = np.mean(np.abs(window_src - current_sma))
        return mad_vals

    df['dev_high'] = calc_mad(high_arr, sma_high.values, h_len)
    df['dev_low'] = calc_mad(low_arr, sma_low.values, l_len)
    
    df['d'] = df['a'] + df['dev_high']
    df['c'] = df['b'] - df['dev_low']
    
    return df


def check_signals(df, h_len=300, l_len=300):
    df = calculate_linreg_and_dev(df, h_len, l_len)
    
    if len(df) < max(h_len, l_len) + 2:
        return None
        
    a_curr, a_prev = df['a'].iloc[-1], df['a'].iloc[-2]
    b_curr, b_prev = df['b'].iloc[-1], df['b'].iloc[-2]
    
    if pd.isna(a_curr) or pd.isna(a_prev) or pd.isna(b_curr) or pd.isna(b_prev):
        return None

    cross_up = (a_prev < b_prev) and (a_curr > b_curr)
    cross_down = (a_prev > b_prev) and (a_curr < b_curr)
    
    if cross_up or cross_down:
        slope_a = a_curr - a_prev
        slope_b = b_curr - b_prev
        intersection_angle = abs(slope_a - slope_b)
        
        vol_avg = df['volume'].iloc[-31:-1].mean()
        vol_curr = df['volume'].iloc[-1]
        volume_spike = float(vol_curr / vol_avg) if vol_avg > 0 else 1.0
        
        return {
            "symbol": df['symbol'].iloc[-1] if 'symbol' in df.columns else "UNKNOWN",
            "type": "AL" if cross_up else "SAT",
            "price": float(df['close'].iloc[-1]),
            "angle_score": float(intersection_angle),
            "volume_spike": float(volume_spike),
            "timestamp": int(df.index[-1])
        }
    return None


# ── LRC crossover (periyot bazlı saf mum; resample YOK) ───────────────────

def detect_linreg_bar_crossover(
    high_linreg_prev: float,
    low_linreg_prev: float,
    high_linreg_curr: float,
    low_linreg_curr: float,
) -> str | None:
    """high_linreg (a) / low_linreg (b) iki ardışık mumda crossover.

    Önceki mumda a<b iken şimdiki mumda a>b → AL (crossover).
    Önceki mumda a>b iken şimdiki mumda a<b → SAT (crossunder).
    """
    vals = (high_linreg_prev, low_linreg_prev, high_linreg_curr, low_linreg_curr)
    if any(v is None or (isinstance(v, float) and np.isnan(v)) for v in vals):
        return None
    a_p, b_p, a_c, b_c = (float(v) for v in vals)
    if a_p < b_p and a_c > b_c:
        return "AL"
    if a_p > b_p and a_c < b_c:
        return "SAT"
    return None


def linreg_crossover_angle(
    high_linreg_prev: float,
    low_linreg_prev: float,
    high_linreg_curr: float,
    low_linreg_curr: float,
) -> float:
    """Crossover anındaki a/b eğim farkı (angle_score)."""
    slope_a = float(high_linreg_curr) - float(high_linreg_prev)
    slope_b = float(low_linreg_curr) - float(low_linreg_prev)
    return abs(slope_a - slope_b)


def scan_recent_linreg_crossover(
    high_linreg: pd.Series,
    low_linreg: pd.Series,
    lookback_bars: int = 3,
) -> tuple[str | None, float, int]:
    """Son `lookback_bars` mum içinde en güncel crossover.

    Returns:
        (signal_type, angle_score, bars_ago) — bars_ago 0 = son mum.
    """
    n = len(high_linreg)
    if n < 2:
        return None, 0.0, -1

    max_check = min(lookback_bars, n - 1)
    for offset in range(0, max_check):
        idx = -1 - offset
        prev_idx = idx - 1
        cross = detect_linreg_bar_crossover(
            high_linreg.iloc[prev_idx],
            low_linreg.iloc[prev_idx],
            high_linreg.iloc[idx],
            low_linreg.iloc[idx],
        )
        if cross is not None:
            angle = linreg_crossover_angle(
                high_linreg.iloc[prev_idx],
                low_linreg.iloc[prev_idx],
                high_linreg.iloc[idx],
                low_linreg.iloc[idx],
            )
            return cross, angle, offset
    return None, 0.0, -1


def crossover_lookback_for_period(period: str) -> int:
    """Periyot için dinamik crossover hafıza derinliği (bar sayısı)."""
    from config import CROSSOVER_LOOKBACK_MAP, DEFAULT_CROSSOVER_LOOKBACK

    return CROSSOVER_LOOKBACK_MAP.get(period, DEFAULT_CROSSOVER_LOOKBACK)


def effective_crossover_lookback(
    period: str,
    bar_count: int,
    h_len: int = 300,
    l_len: int = 300,
) -> int:
    """Haritadaki lookback'i LRC 300 ısınma payına göre güvenli üst sınırlar.

    `a`/`b` değerleri yalnızca `max(h_len, l_len)` bar sonrası geçerlidir;
    geriye tarama bu sınırı aşamaz.
    """
    desired = crossover_lookback_for_period(period)
    needed = max(h_len, l_len)
    max_valid = max(0, bar_count - needed)
    if max_valid <= 0:
        return 0
    return min(desired, max_valid)


def detect_tv_closed_bar_crossover(
    high_linreg: pd.Series,
    low_linreg: pd.Series,
) -> tuple[str | None, float]:
    """TV `ta.crossover(a,b)` / `ta.crossunder(a,b)` — son kapalı mum.

    ``_drop_forming_candle`` sonrası ``iloc[-1]`` en güncel kapalı bar;
    TV ile birebir karşılaştırma ``[-2] → [-1]`` aralığındadır.
    """
    if len(high_linreg) < 2:
        return None, 0.0
    cross = detect_linreg_bar_crossover(
        high_linreg.iloc[-2],
        low_linreg.iloc[-2],
        high_linreg.iloc[-1],
        low_linreg.iloc[-1],
    )
    if cross is None:
        return None, 0.0
    angle = linreg_crossover_angle(
        high_linreg.iloc[-2],
        low_linreg.iloc[-2],
        high_linreg.iloc[-1],
        low_linreg.iloc[-1],
    )
    return cross, angle


def scan_tv_crossover_lookback(
    high_linreg: pd.Series,
    low_linreg: pd.Series,
    lookback_bars: int = 3,
) -> tuple[str | None, float, int]:
    """TV uyumlu lookback: her offset'te ``[-2-offset] → [-1-offset]`` crossover.

    Returns:
        (signal_type, angle_score, bars_ago) — bars_ago 0 = en güncel kapalı mum.
    """
    n = len(high_linreg)
    if n < 2:
        return None, 0.0, -1

    max_check = min(lookback_bars, n - 1)
    for offset in range(0, max_check):
        curr_idx = -1 - offset
        prev_idx = curr_idx - 1
        cross = detect_linreg_bar_crossover(
            high_linreg.iloc[prev_idx],
            low_linreg.iloc[prev_idx],
            high_linreg.iloc[curr_idx],
            low_linreg.iloc[curr_idx],
        )
        if cross is not None:
            angle = linreg_crossover_angle(
                high_linreg.iloc[prev_idx],
                low_linreg.iloc[prev_idx],
                high_linreg.iloc[curr_idx],
                low_linreg.iloc[curr_idx],
            )
            return cross, angle, offset
    return None, 0.0, -1


def detect_pine_last_bar_crossover(
    high_linreg: pd.Series,
    low_linreg: pd.Series,
) -> tuple[str | None, float]:
    """Geriye uyumluluk: TV kapalı-mum crossover (`detect_tv_closed_bar_crossover`)."""
    return detect_tv_closed_bar_crossover(high_linreg, low_linreg)


# ── Smart Radar kurumsal filtreleri (linreg motorundan bağımsız) ──────────

def ema_last(close: pd.Series, span: int = 200) -> float | None:
    """Kapanış serisinin son EMA(span) değerini döner."""
    if close is None or len(close) < span:
        return None
    ema_series = close.ewm(span=span, adjust=False).mean()
    val = ema_series.iloc[-1]
    if pd.isna(val):
        return None
    return float(val)


def htf_trend_bullish(
    df_4h: pd.DataFrame,
    current_price: float | None,
    ema_span: int = 200,
) -> bool:
    """Anlık fiyat 4h EMA200 üzerindeyse boğa eğilimi (HTF trend)."""
    if df_4h is None or df_4h.empty or current_price is None:
        return False
    if len(df_4h) < ema_span:
        return False
    ema_val = ema_last(df_4h["close"], ema_span)
    if ema_val is None:
        return False
    return float(current_price) > ema_val


def rvol_heavy(
    df_1h: pd.DataFrame,
    quote_volume_24h: float,
    multiplier: float = 2.5,
    lookback_hours: int = 24,
) -> bool:
    """Son 1h hacim ≥ günlük ortalama saatlik hacim × multiplier mı?"""
    if df_1h is None or df_1h.empty:
        return False
    vol_col = "quote_volume" if "quote_volume" in df_1h.columns else "volume"
    last_1h = float(df_1h[vol_col].iloc[-1])
    if last_1h <= 0:
        return False

    if len(df_1h) >= lookback_hours:
        avg_hourly = float(df_1h[vol_col].iloc[-lookback_hours:].mean())
    elif quote_volume_24h > 0:
        avg_hourly = quote_volume_24h / 24.0
    else:
        return False

    if avg_hourly <= 0:
        return False
    return last_1h >= multiplier * avg_hourly


def orderbook_path_clear(
    asks: list[tuple[float, float]],
    current_price: float,
    avg_minute_volume_usdt: float,
    price_band_pct: float = 0.02,
    wall_multiplier: float = 3.0,
) -> bool:
    """Fiyatın +%2 bandındaki ask duvarlarını analiz eder.

    Tek bir satış emri, tahta ortalamasının veya 1h ortalama dakikalık
    hacmin 3 katından büyükse yol tıkalı (`False`).
    """
    if not current_price or current_price <= 0:
        return False
    if not asks:
        return True

    ceiling = current_price * (1.0 + price_band_pct)
    in_band_usdt: list[float] = []
    for price, qty in asks:
        p = float(price)
        if p < current_price:
            continue
        if p > ceiling:
            break
        q = float(qty)
        if q <= 0:
            continue
        in_band_usdt.append(p * q)

    if not in_band_usdt:
        return True

    avg_order = sum(in_band_usdt) / len(in_band_usdt)
    minute_ref = max(float(avg_minute_volume_usdt or 0.0), 0.0)

    for size in in_band_usdt:
        if size > wall_multiplier * avg_order:
            return False
        if minute_ref > 0 and size > wall_multiplier * minute_ref:
            return False
    return True
