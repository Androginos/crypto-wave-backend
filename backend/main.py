"""
main.py
-------
Kripto Çoklu Zaman Dilimi Confluence Tarayıcısı — orkestrasyon katmanı.

Sorumluluklar:
  * Uygulama yaşam döngüsü: FastAPI lifespan içinde sembol evreni
    yenileme, REST backfill, WebSocket dinleyici, periyodik crossover
    tarama ve sembol özeti hesaplama task'larını başlatır.
  * Binance REST üzerinden her periyot için PERIOD_BAR_LIMITS haritasına
    göre tarihsel kline verisini çekip bellek-içi DataFrame'ler hazırlar.
  * Binance WebSocket combined-stream üzerinden gelen kline
    güncellemelerini bu DataFrame'lere işler (sadece KAPANMIŞ mumlar
    saklanır; ek olarak taker_buy_volume ve quote_volume sütunları da
    her satırda tutulur).
  * Her sembol için TÜM PERIOTS üzerinde:
      - Mevcut linreg state (a > b ⇒ AL, a < b ⇒ SAT)
      - Crossover sinyali (indicators.check_signals)
      - Son mum hacim analitiği (taker buy / sell yüzdeleri)
    hesaplanır; bunlar confluence skoru ile birlikte
    STATE.symbol_summaries içine tek bir kayıt olarak yazılır.
  * `/signals` endpoint'i bu zenginleştirilmiş sembol-bazlı kayıtları
    confluence skoru + 24h hacim sıralamasıyla servis eder.

Tüm I/O `asyncio` üzerinden kooperatif çalıştığı için tek event loop'ta
yüzlerce sembol paralel takip edilebilir; ağır CPU (linreg) hesaplamaları
`asyncio.to_thread` ile worker thread havuzuna devredilir.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from typing import Any, Final

import httpx
import pandas as pd
import websockets
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from config import (
    BINANCE_API_KEY,
    CRON_SECRET,
    CROSSOVER_LOOKBACK_MAP,
    DEFAULT_CROSSOVER_LOOKBACK,
    H_LEN,
    H_LEN_MAP,
    IS_VERCEL,
    KLINE_FETCH_LIMIT,
    L_LEN,
    L_LEN_MAP,
    PERIOD_BAR_LIMITS,
    PERIOTS,
    REST_BASE_URL,
    SCAN_INTERVAL_SECONDS,
    TV_DASHBOARD_LABELS,
    TV_DASHBOARD_PERIODS,
    VERCEL_MAX_SYMBOLS,
    WS_BASE_URL,
    WS_PING_INTERVAL,
    WS_PING_TIMEOUT,
    WS_RECONNECT_DELAY,
    HTTP_TIMEOUT,
    MCAP_REFRESH_MINUTES,
    configure_logging,
    validate_keys,
)
from indicators import (
    calculate_linreg_and_dev,
    crossover_lookback_for_period,
    detect_linreg_bar_crossover,
    detect_tv_closed_bar_crossover,
    effective_crossover_lookback,
    htf_trend_bullish,
    orderbook_path_clear,
    rvol_heavy,
    scan_tv_crossover_lookback,
)
from wave_framework import (
    compute_hybrid_backtest_win_rate,
    evaluate_wave_framework,
    fetch_oi_rising,
)
from mcap_manager import (
    MarketCapError,
    flatten_groups,
    get_market_caps,
    get_quote_volume_map,
)

configure_logging()
logger = logging.getLogger("main")


PERIOD_TO_SECONDS: Final[dict[str, int]] = {
    "1m": 60,
    "3m": 180,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
    "2h": 7200,
    "4h": 14400,
    "6h": 21600,
    "8h": 28800,
    "12h": 43200,
    "1d": 86400,
    "3d": 259200,
    "1w": 604800,
}

# Bir DataFrame'in maksimum bar sayısı PERİYOT BAZLI hesaplanır:
#   bar_limit + max(H_LEN, L_LEN) + 10 (güvenlik payı)
# Böylece her periyot kendi ihtiyacı kadar bar tutar; haftalık periyot
# 5dk periyot kadar bellek tüketmez. Yeni bar geldiğinde eski barlar
# bu tavana göre kırpılır.
def _max_bars_for_period(period: str) -> int:
    bar_limit = PERIOD_BAR_LIMITS.get(period, KLINE_FETCH_LIMIT)
    h = H_LEN_MAP.get(period, H_LEN)
    l = L_LEN_MAP.get(period, L_LEN)
    return bar_limit + max(h, l) + 10


# Eski global tavan — geriye dönük uyumluluk için tutulur (kullanılmıyor).
_MAX_BAR_LIMIT: Final[int] = max(PERIOD_BAR_LIMITS.values()) if PERIOD_BAR_LIMITS else 100
MAX_BARS_PER_SYMBOL: Final[int] = _MAX_BAR_LIMIT + max(H_LEN, L_LEN) + 10

MAX_SIGNAL_HISTORY: Final[int] = 1000
ELITE_HISTORY_RETENTION_MS: Final[int] = 7 * 24 * 3600 * 1000
ALPHA_HISTORY_WINDOW_MS: Final[int] = 24 * 3600 * 1000
WEEKLY_PERFORMANCE_WINDOW_MS: Final[int] = 7 * 24 * 3600 * 1000
ELITE_ANALYTICS_THRESHOLD: Final[float] = 2.5
ORDERBOOK_DEPTH_LIMIT: Final[int] = 50
ORDERBOOK_PRICE_BAND_PCT: Final[float] = 0.02

# Confluence skor hesaplamasında hangi yön (AL/SAT) "dominant" sayılır.
# Eşitlikte AL tercih edilir (uzun pozisyon yanlısı varsayım).
_DEFAULT_DIRECTION_TIE: Final[str] = "AL"


class MarketState:
    """Uygulamanın merkezi bellek-içi durumu."""

    def __init__(self) -> None:
        self.symbols: list[str] = []
        self.groups: dict[str, list[str]] = {}

        # frames[symbol][period] = pd.DataFrame[index=close_time_ms]
        self.frames: dict[str, dict[str, pd.DataFrame]] = defaultdict(dict)

        # symbol başına frame mutasyonlarını seri hale getirmek için kilit
        self.frame_locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

        # sinyal kuyruğu (kronolojik)
        self.signals: deque[dict[str, Any]] = deque(maxlen=MAX_SIGNAL_HISTORY)
        self.signals_lock: asyncio.Lock = asyncio.Lock()

        # Sembol bazlı zenginleştirilmiş özet kayıtlar:
        #   STATE.symbol_summaries[symbol] = {
        #       "symbol": ..., "confluence_score": int,
        #       "confluence_max": int, "direction": "AL"|"SAT"|None,
        #       "quote_volume_24h": float,
        #       "current_price": float | None,
        #       "periods": {
        #           "5m":  {state, signal, price, timestamp, volume_total,
        #                   taker_buy_volume, taker_sell_volume,
        #                   buy_pct, sell_pct, angle_score, volume_spike},
        #           ...
        #       },
        #   }
        # `/signals` endpoint'i bu sözlüğü sıralı şekilde servis eder.
        self.symbol_summaries: dict[str, dict[str, Any]] = {}
        self.summaries_lock: asyncio.Lock = asyncio.Lock()
        self.last_summary_at: float = 0.0

        # son tarama metrikleri
        self.last_scan_at: float = 0.0
        self.last_scan_period: str | None = None
        self.last_scan_signal_count: int = 0

        # WS connection durumu
        self.ws_connected: bool = False
        self.ws_last_message_at: float = 0.0

        # Son 7 gün elit sinyal hafızası (Alpha History + Weekly Performance)
        # elite_history[symbol] = {
        #   symbol, first_triggered_at_ms, last_triggered_at_ms,
        #   trigger_count, signal_price, signal_periods, confluence_score,
        #   analytics_score, highest_price_since_signal, max_pnl_pct,
        # }
        self.elite_history: dict[str, dict[str, Any]] = {}
        self.elite_active: dict[str, bool] = {}
        self.elite_history_lock: asyncio.Lock = asyncio.Lock()

        # Wave onaylı sinyaller — Smart Radar düşüşünde bile min 24s kilitli
        self.alpha_history_pool: dict[str, dict[str, Any]] = {}
        self.alpha_pool_lock: asyncio.Lock = asyncio.Lock()
        self.alpha_radar_active: dict[str, bool] = {}
        self.alpha_last_wave_snapshot: dict[str, dict[str, Any]] = {}
        self.oi_cache: dict[str, tuple[float, bool]] = {}

    def all_symbols(self) -> list[str]:
        return list(self.symbols)


STATE = MarketState()
HTTP_CLIENT: httpx.AsyncClient | None = None
_vercel_bootstrap_lock: asyncio.Lock = asyncio.Lock()
_vercel_ready: bool = False


def _build_allowed_origins() -> list[str]:
    """CORS kökenleri — yerel + Vercel + ALLOWED_ORIGINS env."""
    origins = [
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:4173",
        "http://127.0.0.1:4173",
        "http://localhost:8001",
        "http://127.0.0.1:8001",
    ]
    extra = os.getenv("ALLOWED_ORIGINS", "")
    for item in extra.split(","):
        item = item.strip()
        if item:
            origins.append(item)
    frontend = os.getenv("FRONTEND_URL", "").strip()
    if frontend:
        origins.append(frontend.rstrip("/"))
    vercel_url = os.getenv("VERCEL_URL", "").strip()
    if vercel_url:
        origins.append(f"https://{vercel_url}")
    # Tekrarları kaldır, sırayı koru
    seen: set[str] = set()
    out: list[str] = []
    for o in origins:
        if o not in seen:
            seen.add(o)
            out.append(o)
    return out


async def vercel_bootstrap_if_needed(*, force_backfill: bool = False) -> dict[str, Any]:
    """Vercel serverless: sembol evreni + backfill + ilk özet (cron veya manuel)."""
    global _vercel_ready

    if not IS_VERCEL:
        return {"mode": "local", "skipped": True}

    async with _vercel_bootstrap_lock:
        started = time.monotonic()

        if HTTP_CLIENT is None:
            raise RuntimeError("HTTP_CLIENT başlatılmadı")

        if not STATE.symbols:
            groups = await get_market_caps()
            STATE.groups = groups
            symbols = flatten_groups(groups)
            if VERCEL_MAX_SYMBOLS > 0:
                symbols = symbols[:VERCEL_MAX_SYMBOLS]
            STATE.symbols = symbols
            logger.info(
                "Vercel bootstrap: sembol=%d (max=%s)",
                len(STATE.symbols),
                VERCEL_MAX_SYMBOLS or "∞",
            )

        need_backfill = force_backfill or not STATE.frames
        if need_backfill and STATE.symbols:
            sample = STATE.symbols[0]
            has_data = bool(STATE.frames.get(sample))
            if force_backfill or not has_data:
                await backfill_all(STATE.symbols)

        new_signals = await update_all_summaries()
        _vercel_ready = len(STATE.symbol_summaries) > 0
        elapsed = round(time.monotonic() - started, 2)

        return {
            "mode": "vercel",
            "symbols": len(STATE.symbols),
            "summaries": len(STATE.symbol_summaries),
            "new_signals": new_signals,
            "ready": _vercel_ready,
            "elapsed_sec": elapsed,
        }


def _verify_cron_secret(request: Request) -> None:
    """Vercel Cron veya manuel tetikleme için Bearer doğrulaması."""
    if not CRON_SECRET:
        return
    auth = request.headers.get("Authorization", "")
    if auth != f"Bearer {CRON_SECRET}":
        raise HTTPException(status_code=401, detail="Geçersiz cron secret")


def _kline_to_row(kline: list) -> dict[str, Any]:
    """Binance kline dizisini DataFrame satır sözlüğüne çevirir.

    Binance REST kline payload formatı (12 alan):
        [0]  open_time
        [1]  open
        [2]  high
        [3]  low
        [4]  close
        [5]  volume                       (base asset cinsi - örn. FTT)
        [6]  close_time
        [7]  quote_asset_volume           (USDT cinsi - GÖSTERİM İÇİN)
        [8]  number_of_trades
        [9]  taker_buy_base_asset_volume
        [10] taker_buy_quote_asset_volume (USDT cinsi - GÖSTERİM İÇİN)
        [11] ignore

    NOT: Hacim göstergeleri için TUTARLILIK adına `quote_volume` (USDT)
    ve `taker_buy_volume` (USDT cinsinden taker buy = index [10]) kullanılır.
    Böylece per-period hacim ile 24h hacim aynı birimdedir (USDT) ve
    karşılaştırılabilir.
    """
    return {
        "open_time": int(kline[0]),
        "open": float(kline[1]),
        "high": float(kline[2]),
        "low": float(kline[3]),
        "close": float(kline[4]),
        "volume": float(kline[5]),  # base asset (volume_spike normalizasyonu için)
        "close_time": int(kline[6]),
        # Aşağıdaki ikisi USDT (quote) cinsinden — UI/bot bu alanları gösterir.
        "quote_volume": float(kline[7]) if len(kline) > 7 else 0.0,
        "taker_buy_volume": float(kline[10]) if len(kline) > 10 else 0.0,
    }


async def fetch_klines(
    client: httpx.AsyncClient,
    symbol: str,
    interval: str,
    limit: int | None = None,
) -> pd.DataFrame:
    """REST üzerinden tarihsel kline verisini çeker, DataFrame döndürür.

    `limit` parametresi verilmezse `PERIOD_BAR_LIMITS` haritasından
    interval'a karşılık gelen değer kullanılır; o da yoksa
    `KLINE_FETCH_LIMIT` fallback'i devreye girer.
    """
    if limit is None:
        limit = PERIOD_BAR_LIMITS.get(interval, KLINE_FETCH_LIMIT)

    url = f"{REST_BASE_URL}/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": str(limit)}
    headers: dict[str, str] = {}
    if BINANCE_API_KEY and "YENI_" not in BINANCE_API_KEY:
        headers["X-MBX-APIKEY"] = BINANCE_API_KEY

    resp = await client.get(url, params=params, headers=headers)
    resp.raise_for_status()
    raw = resp.json()

    rows = [_kline_to_row(k) for k in raw]
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["symbol"] = symbol
    df.set_index("close_time", inplace=True)
    df.sort_index(inplace=True)

    # tarihsel veride sadece KAPANMIŞ mumlar gelir; yine de boyutu sınırlandır
    cap = _max_bars_for_period(interval)
    if len(df) > cap:
        df = df.iloc[-cap:].copy()
    return df


async def fetch_orderbook_depth(
    client: httpx.AsyncClient,
    symbol: str,
    limit: int = ORDERBOOK_DEPTH_LIMIT,
) -> list[tuple[float, float]]:
    """Binance emir defteri — ask tarafı (fiyat, miktar) listesi."""
    url = f"{REST_BASE_URL}/api/v3/depth"
    params = {"symbol": symbol, "limit": str(limit)}
    headers: dict[str, str] = {}
    if BINANCE_API_KEY and "YENI_" not in BINANCE_API_KEY:
        headers["X-MBX-APIKEY"] = BINANCE_API_KEY

    resp = await client.get(url, params=params, headers=headers)
    resp.raise_for_status()
    data = resp.json()
    return [(float(p), float(q)) for p, q in data.get("asks", [])]


def _avg_minute_volume_usdt(df_1h: pd.DataFrame | None) -> float:
    """Son 1h mum hacminden USDT cinsinden ortalama dakikalık hacim."""
    if df_1h is None or df_1h.empty:
        return 0.0
    vol_col = "quote_volume" if "quote_volume" in df_1h.columns else "volume"
    last_1h = float(df_1h[vol_col].iloc[-1])
    return last_1h / 60.0 if last_1h > 0 else 0.0


async def backfill_symbol(
    client: httpx.AsyncClient, symbol: str, semaphore: asyncio.Semaphore
) -> None:
    """Bir sembol için tüm PERIOTS'ları paralel olmadan sırayla doldurur."""
    async with semaphore:
        for period in PERIOTS:
            try:
                df = await fetch_klines(client, symbol, period)
                async with STATE.frame_locks[symbol]:
                    STATE.frames[symbol][period] = df
                logger.debug(
                    "Backfill OK %s %s (rows=%d)", symbol, period, len(df)
                )
            except httpx.HTTPStatusError as exc:
                if exc.response is not None and exc.response.status_code == 400:
                    logger.debug(
                        "Backfill atlandı (Binance'te yok) %s %s",
                        symbol,
                        period,
                    )
                else:
                    logger.warning(
                        "Backfill HTTP hatası %s %s: %s", symbol, period, exc
                    )
            except httpx.HTTPError as exc:
                logger.warning(
                    "Backfill ağ hatası %s %s: %s", symbol, period, exc
                )
            except Exception as exc:  # noqa: BLE001
                logger.exception(
                    "Backfill beklenmedik hata %s %s: %s", symbol, period, exc
                )
            # Binance dakikalık istek limitini aşmamak için her istek
            # sonrası nazik throttling.
            await asyncio.sleep(0.1)


async def backfill_all(symbols: list[str]) -> None:
    """Tüm semboller için paralel backfill."""
    assert HTTP_CLIENT is not None
    semaphore = asyncio.Semaphore(10)  # Binance REST için makul paralelizm
    logger.info(
        "REST backfill başlıyor: %d sembol x %d periyot = %d istek",
        len(symbols),
        len(PERIOTS),
        len(symbols) * len(PERIOTS),
    )
    started = time.monotonic()
    await asyncio.gather(
        *(backfill_symbol(HTTP_CLIENT, s, semaphore) for s in symbols)
    )
    logger.info(
        "REST backfill tamamlandı: %.1fs",
        time.monotonic() - started,
    )


def _apply_ws_kline(symbol: str, period: str, k: dict[str, Any]) -> bool:
    """
    WS'den gelen kline payload'unu DataFrame'e uygular.
    Sadece `x == True` (mum kapandı) olduğunda kalıcı yazar.

    WS kline payload alan haritası:
        v = base asset volume
        q = quote asset volume (USDT)
        V = taker buy base asset volume
        Q = taker buy quote asset volume

    Returns: True => yeni kapalı bar eklendi.
    """
    if not k.get("x"):
        return False  # mum henüz kapanmadı

    row = {
        "open_time": int(k["t"]),
        "open": float(k["o"]),
        "high": float(k["h"]),
        "low": float(k["l"]),
        "close": float(k["c"]),
        "volume": float(k["v"]),
        "close_time": int(k["T"]),
        # USDT (quote) cinsinden hacim göstergeleri — REST ile tutarlı.
        "quote_volume": float(k.get("q", 0.0) or 0.0),
        "taker_buy_volume": float(k.get("Q", 0.0) or 0.0),
        "symbol": symbol,
    }

    df = STATE.frames[symbol].get(period)
    if df is None or df.empty:
        new_df = pd.DataFrame([row]).set_index("close_time")
        STATE.frames[symbol][period] = new_df
        return True

    close_time = row["close_time"]
    if close_time in df.index:
        # idempotent güncelleme — yeni şemadaki tüm sütunlar
        for col in (
            "open", "high", "low", "close", "volume", "open_time",
            "symbol", "quote_volume", "taker_buy_volume",
        ):
            if col in row:
                df.at[close_time, col] = row[col]
    else:
        df.loc[close_time] = {k_: row[k_] for k_ in df.columns if k_ in row}
        df.sort_index(inplace=True)

    # boyut sınırlandırma — periyot bazlı tavan
    cap = _max_bars_for_period(period)
    if len(df) > cap:
        STATE.frames[symbol][period] = df.iloc[-cap:].copy()
    return True


def _build_combined_streams(symbols: list[str], periods: list[str]) -> list[str]:
    """`btcusdt@kline_5m` formatında stream isimlerini üretir."""
    streams: list[str] = []
    for s in symbols:
        sl = s.lower()
        for p in periods:
            streams.append(f"{sl}@kline_{p}")
    return streams


async def _ws_consume(stream_url: str) -> None:
    """Tek bir WS bağlantısını dinler; bağlantı koparsa dış döngü yeniden bağlanır."""
    async with websockets.connect(
        stream_url,
        ping_interval=WS_PING_INTERVAL,
        ping_timeout=WS_PING_TIMEOUT,
        max_size=2**22,
    ) as ws:
        STATE.ws_connected = True
        logger.info("WS bağlandı.")
        async for raw_msg in ws:
            STATE.ws_last_message_at = time.time()
            try:
                msg = json.loads(raw_msg)
            except json.JSONDecodeError:
                continue

            data = msg.get("data") or msg
            if not isinstance(data, dict):
                continue
            if data.get("e") != "kline":
                continue
            symbol = data.get("s")
            k = data.get("k")
            if not symbol or not isinstance(k, dict):
                continue
            period = k.get("i")
            if period not in PERIOTS:
                continue
            try:
                async with STATE.frame_locks[symbol]:
                    _apply_ws_kline(symbol, period, k)
            except Exception as exc:  # noqa: BLE001
                logger.exception("WS kline işleme hatası %s %s: %s", symbol, period, exc)


async def websocket_listener(symbols: list[str]) -> None:
    """
    Sembol sayısı arttıkça URL uzunluğu sınırlarına takılmamak için
    streamler 200'lük chunk'lara bölünür ve her chunk için bağımsız
    bir WS bağlantısı (yeniden bağlanma döngülü) açılır.
    """
    streams = _build_combined_streams(symbols, PERIOTS)
    if not streams:
        logger.warning("WS başlatılamadı: stream listesi boş.")
        return

    chunk_size = 200
    chunks = [streams[i : i + chunk_size] for i in range(0, len(streams), chunk_size)]
    logger.info("WS dinleyici: %d stream, %d bağlantı", len(streams), len(chunks))

    async def _runner(chunk: list[str], idx: int) -> None:
        url = f"{WS_BASE_URL}?streams={'/'.join(chunk)}"
        while True:
            try:
                await _ws_consume(url)
            except (websockets.ConnectionClosed, OSError) as exc:
                STATE.ws_connected = False
                logger.warning(
                    "WS#%d bağlantı koptu (%s). %ss sonra yeniden denenecek.",
                    idx,
                    exc,
                    WS_RECONNECT_DELAY,
                )
            except Exception as exc:  # noqa: BLE001
                STATE.ws_connected = False
                logger.exception("WS#%d beklenmedik hata: %s", idx, exc)
            await asyncio.sleep(WS_RECONNECT_DELAY)

    await asyncio.gather(*(_runner(c, i) for i, c in enumerate(chunks)))


def _drop_forming_candle(df: pd.DataFrame | None, period: str) -> pd.DataFrame | None:
    """Açık (henüz kapanmamış) son mumu çıkarır — Pine kapalı bar hizası.

    Binance WS yalnızca kapanmış mum yazar; REST backfill son satır açık
    mum olabilir. TV crossover kapalı mumda tetiklenir.
    """
    if df is None or df.empty:
        return df
    period_sec = PERIOD_TO_SECONDS.get(period)
    if not period_sec or len(df) < 2:
        return df
    now_ms = int(time.time() * 1000)
    last_close_ms = int(df.index[-1])
    if last_close_ms > now_ms:
        return df.iloc[:-1].copy()
    return df


def _snapshot_df(
    snapshots: dict[str, pd.DataFrame | None],
    period: str,
) -> pd.DataFrame | None:
    """Boş DataFrame'i None sayarak güvenli snapshot okur."""
    df = snapshots.get(period)
    if df is None or df.empty:
        return None
    return df


def _first_snapshot_df(
    snapshots: dict[str, pd.DataFrame | None],
    *periods: str,
) -> tuple[pd.DataFrame | None, str | None]:
    """İlk dolu periyot DataFrame'ini döner."""
    for period in periods:
        df = _snapshot_df(snapshots, period)
        if df is not None:
            return df, period
    return None, None


def _compute_period_data_sync(
    symbol: str, period: str, df: pd.DataFrame | None
) -> dict[str, Any] | None:
    """CPU-bound: tek (symbol, period) için TÜM analitiği üretir.

    PERİYOT BAZLI regresyon penceresi: `H_LEN_MAP[period]` ve
    `L_LEN_MAP[period]` kullanılır (haftalıkta 25, 5dk'da 95 vb.).
    Map'te tanımlı olmayan periyot için global `H_LEN`/`L_LEN`
    fallback olarak devreye girer.

    Tek bir `calculate_linreg_and_dev` çağrısı ile şunları hesaplar:
      * `state` — son barda a vs b (AL: a > b, SAT: a < b). Crossover
        beklemeden trend yönünü verir; confluence skoru bu alana göre
        toplanır.
      * `signal` — son barda crossover oldu mu? Crossover olmadığında
        `None`.
      * `volume_total`, `taker_buy_volume`, `taker_sell_volume`,
        `buy_pct`, `sell_pct` — son kapalı barın hacim analitiği.
      * `angle_score`, `volume_spike` — eski sinyal alanlarıyla uyumlu.

    indicators.py'deki matematik motoruna DOKUNULMAZ; bu fonksiyon
    yalnızca dışarıdan çağırır ve sonucu yorumlar.
    """
    if df is None or df.empty:
        return None

    h_len = H_LEN_MAP.get(period, H_LEN)
    l_len = L_LEN_MAP.get(period, L_LEN)

    work = _drop_forming_candle(df.copy(), period)
    if work is None or work.empty:
        return None
    work = calculate_linreg_and_dev(work, h_len=h_len, l_len=l_len)

    needed = max(h_len, l_len)
    if len(work) < needed:
        return None

    a_curr = work["a"].iloc[-1]
    b_curr = work["b"].iloc[-1]
    if pd.isna(a_curr) or pd.isna(b_curr):
        return None

    state = "AL" if float(a_curr) > float(b_curr) else "SAT"

    # ── TV Pine uyumu: son kesinleşmiş kapalı mum (iloc[-2]) crossover
    tv_signal, tv_angle = detect_tv_closed_bar_crossover(work["a"], work["b"])
    pine_cross_now = tv_signal is not None

    # ── Confluence: TV lookback penceresi — tek kaynak tv_signal mantığı
    lookback_configured = crossover_lookback_for_period(period)
    lookback_bars = effective_crossover_lookback(period, len(work), h_len, l_len)
    signal_type: str | None = None
    angle_score = 0.0
    bars_since_cross = -1
    if lookback_bars > 0 and len(work) >= needed + 2:
        signal_type, angle_score, bars_since_cross = scan_tv_crossover_lookback(
            work["a"], work["b"], lookback_bars=lookback_bars
        )

    last = work.iloc[-1]
    # ────────────────────────────────────────────────────────────────────
    # HACİM ANALİTİĞİ — TÜM RAKAMLAR USDT (QUOTE) CİNSİNDEN
    # ────────────────────────────────────────────────────────────────────
    # Binance kline'da:
    #   - volume          = base asset (örn. FTT) — gösterim için DEĞİL
    #   - quote_volume    = USDT cinsinden bar toplam hacmi
    #   - taker_buy_volume= USDT cinsinden bar taker BUY hacmi (index [10])
    # UI/bot tarafında 24h hacim de USDT olduğu için tutarlılık adına
    # her şey USDT'de tutulur. base asset hacmi yalnızca volume_spike
    # normalizasyonu için kullanılır.
    quote_vol = float(last.get("quote_volume", 0.0) or 0.0)
    taker_buy_q = float(last.get("taker_buy_volume", 0.0) or 0.0)
    taker_buy_q = min(max(taker_buy_q, 0.0), quote_vol)  # sayısal güvenlik
    taker_sell_q = max(0.0, quote_vol - taker_buy_q)
    if quote_vol > 0:
        buy_pct = (taker_buy_q / quote_vol) * 100.0
        sell_pct = 100.0 - buy_pct
    else:
        buy_pct = 50.0
        sell_pct = 50.0

    # volume_spike: son 30 bardaki ortalama HACİM (USDT) baz alınır.
    # quote_volume yoksa base volume'a düşer (geriye uyumluluk).
    volume_spike = 1.0
    spike_series = work["quote_volume"] if "quote_volume" in work.columns else work["volume"]
    if len(work) >= 31:
        vol_avg = float(spike_series.iloc[-31:-1].mean())
        cur_vol = float(spike_series.iloc[-1])
        if vol_avg > 0:
            volume_spike = cur_vol / vol_avg

    # Son 3 mum içinde AL crossover var mı? (Smart Radar tazelik filtresi)
    fresh_al_cross = False
    if len(work) >= needed + 2:
        check_bars = min(3, len(work) - needed)
        for offset in range(check_bars, 0, -1):
            idx = -offset
            a_c = work["a"].iloc[idx]
            b_c = work["b"].iloc[idx]
            a_p = work["a"].iloc[idx - 1]
            b_p = work["b"].iloc[idx - 1]
            if any(pd.isna(x) for x in (a_c, b_c, a_p, b_p)):
                continue
            if float(a_p) < float(b_p) and float(a_c) > float(b_c):
                fresh_al_cross = True
                break

    return {
        "period": period,
        "state": state,
        # Confluence skoru: lookback penceresindeki crossover (hafıza)
        "signal": signal_type,
        # TV LRC ALPEREN: ta.crossover/crossunder yalnızca son mum
        "tv_signal": tv_signal,
        "pine_cross_now": pine_cross_now,
        "tv_angle_score": float(tv_angle),
        "high_linreg": float(a_curr),
        "low_linreg": float(b_curr),
        "bars_since_cross": bars_since_cross,
        "lookback_bars": lookback_bars,
        "lookback_configured": lookback_configured,
        "price": float(last["close"]),
        "timestamp": int(work.index[-1]),
        "angle_score": float(angle_score),
        # USDT cinsinden hacim göstergeleri (24h hacim ile tutarlı)
        "volume_total": quote_vol,            # bar toplam (USDT)
        "taker_buy_volume": taker_buy_q,      # taker buy (USDT)
        "taker_sell_volume": taker_sell_q,    # taker sell (USDT)
        "buy_pct": float(buy_pct),
        "sell_pct": float(sell_pct),
        "volume_spike": float(volume_spike),
        "fresh_al_cross": fresh_al_cross,
    }


def _detect_volume_spike_bool(df: pd.DataFrame | None, lookback: int = 20) -> bool:
    """Son aktif mum hacmi, önceki `lookback` mum ortalamasının ≥1.5 katı mı?"""
    if df is None or len(df) < lookback + 1:
        return False
    series = df["quote_volume"] if "quote_volume" in df.columns else df["volume"]
    vol_avg = float(series.iloc[-(lookback + 1):-1].mean())
    cur_vol = float(series.iloc[-1])
    return vol_avg > 0 and (cur_vol / vol_avg) >= 1.5


def _extract_sparkline_24h(df: pd.DataFrame | None, bars: int = 24) -> list[float]:
    """Son 24 saatlik (1h × 24 bar) kapanış fiyat dizisi — sparkline için."""
    if df is None or df.empty:
        return []
    closes = df["close"].iloc[-bars:]
    return [float(x) for x in closes if pd.notna(x)]


def _state_from_frame(df: pd.DataFrame | None, period: str) -> str | None:
    """Tek periyot DataFrame'inden AL/SAT state döner (senkron, hafif)."""
    if df is None or df.empty:
        return None
    h_len = H_LEN_MAP.get(period, H_LEN)
    l_len = L_LEN_MAP.get(period, L_LEN)
    work = calculate_linreg_and_dev(df.copy(), h_len=h_len, l_len=l_len)
    needed = max(h_len, l_len)
    if len(work) < needed:
        return None
    a_curr = work["a"].iloc[-1]
    b_curr = work["b"].iloc[-1]
    if pd.isna(a_curr) or pd.isna(b_curr):
        return None
    return "AL" if float(a_curr) > float(b_curr) else "SAT"


def _btc_trend_aligned(coin_direction: str | None) -> bool:
    """BTCUSDT 15m + 1h yönü ile altcoin yönü uyumlu mu?"""
    if not coin_direction:
        return False
    btc_frames = STATE.frames.get("BTCUSDT", {})
    btc_15m = _state_from_frame(btc_frames.get("15m"), "15m")
    btc_1h = _state_from_frame(btc_frames.get("1h"), "1h")
    if not btc_15m or not btc_1h:
        return False
    btc_up = btc_15m == "AL" and btc_1h == "AL"
    btc_down = btc_15m == "SAT" and btc_1h == "SAT"
    if coin_direction == "AL" and btc_up:
        return True
    if coin_direction == "SAT" and btc_down:
        return True
    return False


def _compute_elite_analytics_score(summary: dict[str, Any]) -> float:
    """Alpha History ağırlıklı Smart Radar puanı (max 3.5)."""
    total = 0.0
    if summary.get("htf_trend_bullish"):
        total += 1.5
    if summary.get("rvol_heavy"):
        total += 1.0
    if summary.get("volume_spike"):
        total += 0.5
    if summary.get("is_fresh_signal"):
        total += 0.5
    return round(total, 1)


def _meets_elite_criteria(summary: dict[str, Any]) -> bool:
    """Alpha History: ≥2 crossover + ağırlıklı analitik puan ≥2.5."""
    crossover_score = int(summary.get("confluence_score") or 0)
    if crossover_score < 2:
        return False
    return _compute_elite_analytics_score(summary) >= ELITE_ANALYTICS_THRESHOLD


def _signal_periods_at(summary: dict[str, Any]) -> list[str]:
    """Aktif crossover veren periyotları PERIOTS sırasıyla döner."""
    periods = summary.get("periods") or {}
    return [p for p in PERIOTS if periods.get(p, {}).get("signal")]


def _update_elite_pnl(entry: dict[str, Any], current_price: float | None) -> None:
    """Sinyal anından bu yana görülen en yüksek fiyat ve max PnL günceller."""
    if not current_price or current_price <= 0:
        return
    signal_price = float(entry.get("signal_price") or 0.0)
    if signal_price <= 0:
        return
    prev_high = float(entry.get("highest_price_since_signal") or signal_price)
    highest = max(prev_high, float(current_price))
    entry["highest_price_since_signal"] = highest
    entry["max_pnl_pct"] = round(((highest - signal_price) / signal_price) * 100.0, 2)


async def _process_elite_history(new_summaries: dict[str, dict[str, Any]]) -> None:
    """Elit koşul geçişlerini kaydeder; 7 günlük PnL takibini günceller."""
    now_ms = int(time.time() * 1000)
    cutoff_7d = now_ms - ELITE_HISTORY_RETENTION_MS

    async with STATE.elite_history_lock:
        stale = [
            sym for sym, entry in STATE.elite_history.items()
            if int(entry.get("first_triggered_at_ms") or 0) < cutoff_7d
        ]
        for sym in stale:
            STATE.elite_history.pop(sym, None)
            STATE.elite_active.pop(sym, None)

        for sym, summary in new_summaries.items():
            meets = _meets_elite_criteria(summary)
            was_meets = STATE.elite_active.get(sym, False)
            current_price = summary.get("current_price")
            periods = _signal_periods_at(summary)
            score = int(summary.get("confluence_score") or 0)
            analytics_score = _compute_elite_analytics_score(summary)

            if sym in STATE.elite_history:
                _update_elite_pnl(STATE.elite_history[sym], current_price)

            if meets and not was_meets:
                price = float(current_price or 0.0)
                if sym in STATE.elite_history:
                    entry = STATE.elite_history[sym]
                    entry["trigger_count"] = int(entry.get("trigger_count") or 0) + 1
                    entry["last_triggered_at_ms"] = now_ms
                    entry["signal_periods"] = periods
                    entry["confluence_score"] = score
                    entry["analytics_score"] = analytics_score
                else:
                    STATE.elite_history[sym] = {
                        "symbol": sym,
                        "first_triggered_at_ms": now_ms,
                        "last_triggered_at_ms": now_ms,
                        "trigger_count": 1,
                        "signal_price": price,
                        "signal_periods": periods,
                        "confluence_score": score,
                        "analytics_score": analytics_score,
                        "highest_price_since_signal": price,
                        "max_pnl_pct": 0.0,
                    }
                    logger.info(
                        "ELITE ALPHA %s @ %.6f | periyotlar=%s skor=%d analitik=%.1f",
                        sym,
                        price,
                        ",".join(periods),
                        score,
                        analytics_score,
                    )
            elif meets and sym in STATE.elite_history:
                entry = STATE.elite_history[sym]
                entry["signal_periods"] = periods
                entry["confluence_score"] = score
                entry["analytics_score"] = analytics_score

            STATE.elite_active[sym] = meets

        for sym in list(STATE.elite_active.keys()):
            if sym not in new_summaries:
                STATE.elite_active[sym] = False


def _primary_signal_period(summary: dict[str, Any]) -> str | None:
    """En kısa periyotta aktif TV lookback sinyalini döner."""
    periods = summary.get("periods") or {}
    for p in PERIOTS:
        if periods.get(p, {}).get("signal"):
            return p
    return None


def _seal_alpha_pool_entry(
    sym: str,
    summary: dict[str, Any],
    now_ms: int,
    *,
    seal_reason: str,
) -> None:
    """Wave onaylı sinyali alpha_history_pool'a mühürler (min 24s kilit)."""
    price = float(summary.get("current_price") or 0.0)
    periods = _signal_periods_at(summary)
    trigger_period = _primary_signal_period(summary) or (periods[0] if periods else None)
    locked_until = now_ms + ALPHA_HISTORY_WINDOW_MS

    existing = STATE.alpha_history_pool.get(sym)
    if existing:
        existing_locked = int(existing.get("locked_until_ms") or 0)
        locked_until = max(locked_until, existing_locked)
        existing["last_sealed_at_ms"] = now_ms
        existing["seal_count"] = int(existing.get("seal_count") or 0) + 1
        existing["seal_reason"] = seal_reason
        existing["call_price"] = price or existing.get("call_price")
        existing["signal_periods"] = periods
        existing["trigger_period"] = trigger_period
        existing["confluence_score"] = int(summary.get("confluence_score") or 0)
        existing["backtest_win_rate"] = float(summary.get("backtest_win_rate") or 0.0)
        existing["wave_framework_approved"] = bool(summary.get("wave_framework_approved"))
        existing["locked_until_ms"] = locked_until
        return

    STATE.alpha_history_pool[sym] = {
        "symbol": sym,
        "first_sealed_at_ms": now_ms,
        "last_sealed_at_ms": now_ms,
        "locked_until_ms": locked_until,
        "seal_count": 1,
        "seal_reason": seal_reason,
        "call_price": price,
        "trigger_period": trigger_period,
        "signal_periods": periods,
        "confluence_score": int(summary.get("confluence_score") or 0),
        "backtest_win_rate": float(summary.get("backtest_win_rate") or 0.0),
        "wave_framework_approved": bool(summary.get("wave_framework_approved")),
    }
    logger.info(
        "ALPHA POOL mühür %s @ %.6f | periyot=%s backtest=%.1f%% reason=%s",
        sym,
        price,
        trigger_period,
        float(summary.get("backtest_win_rate") or 0.0),
        seal_reason,
    )


async def _process_alpha_history_pool(new_summaries: dict[str, dict[str, Any]]) -> None:
    """Wave onaylı TV sinyallerini 24s+ kilitli alpha havuzuna yazar."""
    now_ms = int(time.time() * 1000)

    async with STATE.alpha_pool_lock:
        stale = [
            sym for sym, entry in STATE.alpha_history_pool.items()
            if int(entry.get("locked_until_ms") or 0) < now_ms
            and int(entry.get("last_sealed_at_ms") or entry.get("first_sealed_at_ms") or 0)
            < now_ms - ALPHA_HISTORY_WINDOW_MS
        ]
        for sym in stale:
            STATE.alpha_history_pool.pop(sym, None)

        for sym, summary in new_summaries.items():
            wave_ok = bool(summary.get("wave_framework_approved"))
            score = int(summary.get("confluence_score") or 0)
            radar_visible = score >= 1

            if wave_ok:
                STATE.alpha_last_wave_snapshot[sym] = {
                    "summary": {
                        k: summary.get(k)
                        for k in (
                            "symbol", "current_price", "confluence_score",
                            "backtest_win_rate", "wave_framework_approved", "periods",
                        )
                    },
                    "captured_at_ms": now_ms,
                }

            if wave_ok and score >= 1:
                _seal_alpha_pool_entry(sym, summary, now_ms, seal_reason="tv_wave_active")

            was_radar = STATE.alpha_radar_active.get(sym, False)
            if was_radar and not radar_visible:
                snap = STATE.alpha_last_wave_snapshot.get(sym)
                if snap and snap.get("summary", {}).get("wave_framework_approved"):
                    _seal_alpha_pool_entry(
                        sym,
                        snap["summary"],
                        now_ms,
                        seal_reason="lookback_expired",
                    )

            STATE.alpha_radar_active[sym] = radar_visible

        for sym in list(STATE.alpha_radar_active.keys()):
            if sym not in new_summaries:
                STATE.alpha_radar_active[sym] = False


async def compute_symbol_summary(
    symbol: str,
    period_thread_semaphore: asyncio.Semaphore,
    quote_vol_map: dict[str, float],
    depth_client: httpx.AsyncClient | None = None,
    depth_semaphore: asyncio.Semaphore | None = None,
) -> dict[str, Any] | None:
    """Bir sembolün tüm PERIOTS için zenginleştirilmiş özetini üretir.

    Confluence skoru, dominant yön (AL/SAT), 24h hacim ve periyot bazlı
    state/signal/volume_analytics ile birlikte döner. Eğer hiçbir periyot
    için yeterli veri yoksa None.
    """
    async with STATE.frame_locks[symbol]:
        snapshots = {
            p: (df.copy() if (df := STATE.frames.get(symbol, {}).get(p)) is not None else None)
            for p in PERIOTS
        }

    async def _one(period: str) -> dict[str, Any] | None:
        async with period_thread_semaphore:
            return await asyncio.to_thread(
                _compute_period_data_sync, symbol, period, snapshots[period]
            )

    period_results = await asyncio.gather(*(_one(p) for p in PERIOTS))
    period_map: dict[str, dict[str, Any]] = {}
    for p, data in zip(PERIOTS, period_results):
        if data is not None:
            period_map[p] = data

    if not period_map:
        return None

    # ────────────────────────────────────────────────────────────────────
    # CONFLUENCE SKORU MANTIĞI
    # ────────────────────────────────────────────────────────────────────
    # `signal` / confluence = TV lookback crossover (tek kaynak: tv_signal mantığı)
    # `tv_signal`  = son kesinleşmiş kapalı mum (iloc[-2]) crossover
    # `state`      = anlık a vs b pozisyonu; skoru ETKİLEMEZ.
    # ────────────────────────────────────────────────────────────────────
    al_signal_count = sum(1 for d in period_map.values() if d.get("signal") == "AL")
    sat_signal_count = sum(1 for d in period_map.values() if d.get("signal") == "SAT")

    # State (pozisyon) bilgisini ayrı bir field olarak da dön — UI/bot
    # bunu trend filtresi olarak kullanabilir.
    al_state_count = sum(1 for d in period_map.values() if d.get("state") == "AL")
    sat_state_count = sum(1 for d in period_map.values() if d.get("state") == "SAT")

    if al_signal_count > sat_signal_count:
        direction: str | None = "AL"
        score = al_signal_count
    elif sat_signal_count > al_signal_count:
        direction = "SAT"
        score = sat_signal_count
    elif al_signal_count > 0:
        # eşit AL/SAT crossover sayısı → karışık, direction belirsiz
        direction = None
        score = al_signal_count + sat_signal_count
    else:
        # hiçbir periyotta crossover yok
        direction = None
        score = 0

    # TV LRC ALPEREN paneli: s15, s60, s240, sD (f_check VAR/YOK)
    tv_dashboard: dict[str, bool] = {
        TV_DASHBOARD_LABELS[p]: period_map.get(p, {}).get("tv_signal") is not None
        for p in TV_DASHBOARD_PERIODS
    }
    tv_var_count = sum(1 for v in tv_dashboard.values() if v)

    # Eğer en kısa periyodun current_price'i varsa onu öne çıkar
    current_price: float | None = None
    for p in PERIOTS:
        d = period_map.get(p)
        if d is not None:
            current_price = d["price"]
            break

    # ── Smart Radar analitik doğrulamaları (mevcut /signals şemasına ek) ──
    vol_spike_flag = any(
        _detect_volume_spike_bool(snapshots.get(p))
        for p in PERIOTS
        if snapshots.get(p) is not None
    )
    btc_aligned = _btc_trend_aligned(direction)
    fresh_signal = any(
        d.get("fresh_al_cross") for d in period_map.values()
    )
    quote_vol_24h = float(quote_vol_map.get(symbol, 0.0))
    htf_bull = htf_trend_bullish(snapshots.get("4h"), current_price)
    rvol_flag = rvol_heavy(snapshots.get("1h"), quote_vol_24h)
    elite_analytics_score = _compute_elite_analytics_score({
        "htf_trend_bullish": htf_bull,
        "rvol_heavy": rvol_flag,
        "volume_spike": vol_spike_flag,
        "is_fresh_signal": fresh_signal,
    })

    # ── Emir defteri derinlik analizi (Order Book) ──
    # Rate limit optimizasyonu: depth yalnızca confluence >= 2 VEYA
    # volume_spike olan aktif semboller için sorgulanır; diğerleri None.
    orderbook_clear: bool | None = None
    should_fetch_depth = score >= 2 or vol_spike_flag
    if (
        should_fetch_depth
        and current_price
        and depth_client is not None
        and depth_semaphore is not None
    ):
        try:
            async with depth_semaphore:
                asks = await fetch_orderbook_depth(depth_client, symbol)
                await asyncio.sleep(0.03)
            avg_min_vol = _avg_minute_volume_usdt(snapshots.get("1h"))
            orderbook_clear = orderbook_path_clear(
                asks,
                float(current_price),
                avg_min_vol,
                price_band_pct=ORDERBOOK_PRICE_BAND_PCT,
            )
        except Exception as exc:  # noqa: BLE001
            logger.debug("Orderbook depth hatası %s: %s", symbol, exc)
            orderbook_clear = False

    analytics_pass = (
        int(vol_spike_flag)
        + int(fresh_signal)
        + int(htf_bull)
        + int(rvol_flag)
        + (1 if orderbook_clear is True else 0)
    )

    sparkline = _extract_sparkline_24h(snapshots.get("1h"))
    buy_pct_1h = float(period_map.get("1h", {}).get("buy_pct", 0.0))

    # ── Wave Framework (Elliot 3 + Wyckoff Box + Futures OI) + Backtest ──
    wave_framework_approved = False
    volume_sma_approved = False
    wyckoff_box_breakout = False
    oi_rising = False
    backtest_win_rate = 0.0

    if score >= 1:
        wave_df_raw, wave_period = _first_snapshot_df(snapshots, "15m", "1h")
        wave_df = (
            _drop_forming_candle(wave_df_raw.copy(), wave_period or "15m")
            if wave_df_raw is not None
            else None
        )
        if HTTP_CLIENT is not None and wave_df is not None:
            oi_rising = await fetch_oi_rising(HTTP_CLIENT, symbol, STATE.oi_cache)
            wave_eval = evaluate_wave_framework(wave_df, oi_rising)
            wave_framework_approved = wave_eval["wave_framework_approved"]
            volume_sma_approved = wave_eval["volume_sma_approved"]
            wyckoff_box_breakout = wave_eval["wyckoff_box_breakout"]

        bt_df_raw, bt_period = _first_snapshot_df(snapshots, "1h", "15m")
        bt_df = (
            _drop_forming_candle(bt_df_raw.copy(), bt_period or "1h")
            if bt_df_raw is not None
            else None
        )
        if bt_df is not None and len(bt_df) >= 320:
            h_bt = H_LEN_MAP.get(bt_period, H_LEN)
            l_bt = L_LEN_MAP.get(bt_period, L_LEN)
            backtest_win_rate = await asyncio.to_thread(
                compute_hybrid_backtest_win_rate,
                bt_df,
                h_bt,
                l_bt,
                detect_crossover_fn=detect_linreg_bar_crossover,
            )

    return {
        "symbol": symbol,
        "confluence_score": score,
        "confluence_max": len(PERIOTS),
        "confluence_al": al_signal_count,
        "confluence_sat": sat_signal_count,
        "tv_dashboard": tv_dashboard,
        "tv_var_count": tv_var_count,
        "tv_var_max": len(TV_DASHBOARD_PERIODS),
        # state bazlı hizalanma (skoru etkilemez, sadece bilgi)
        "state_al": al_state_count,
        "state_sat": sat_state_count,
        "direction": direction,
        "quote_volume_24h": quote_vol_24h,
        "current_price": current_price,
        "periods": period_map,
        # Smart Radar VIP filtreleri (5/5 garanti seti)
        "volume_spike": vol_spike_flag,
        "btc_trend_aligned": btc_aligned,
        "is_fresh_signal": fresh_signal,
        "htf_trend_bullish": htf_bull,
        "rvol_heavy": rvol_flag,
        "orderbook_path_clear": orderbook_clear,
        "analytics_pass_count": analytics_pass,
        "analytics_pass_max": 5,
        "elite_analytics_score": elite_analytics_score,
        "elite_analytics_threshold": ELITE_ANALYTICS_THRESHOLD,
        "buy_pct_1h": buy_pct_1h,
        "sparkline_24h": sparkline,
        "wave_framework_approved": wave_framework_approved,
        "volume_sma_approved": volume_sma_approved,
        "wyckoff_box_breakout": wyckoff_box_breakout,
        "oi_rising": oi_rising,
        "backtest_win_rate": backtest_win_rate,
        "updated_at_ms": int(time.time() * 1000),
    }


async def update_all_summaries() -> int:
    """Tüm sembolleri tarayıp STATE.symbol_summaries'i toptan günceller.

    8 worker thread ile paralel; her sembolün 9 periyodu da kendi içinde
    paralel hesaplanır (period_thread_semaphore=4). Depth API yalnızca
    confluence >= 2 veya volume_spike olan sembollere gider (~30-40 istek).
    """
    symbols = STATE.all_symbols()
    if not symbols:
        return 0

    started = time.monotonic()
    quote_vol_map = get_quote_volume_map()
    symbol_semaphore = asyncio.Semaphore(8)
    period_semaphore = asyncio.Semaphore(4)
    depth_semaphore = asyncio.Semaphore(8)
    depth_client = HTTP_CLIENT

    async def _one_symbol(sym: str) -> dict[str, Any] | None:
        async with symbol_semaphore:
            try:
                return await compute_symbol_summary(
                    sym,
                    period_semaphore,
                    quote_vol_map,
                    depth_client=depth_client,
                    depth_semaphore=depth_semaphore,
                )
            except Exception as exc:  # noqa: BLE001
                logger.exception("compute_symbol_summary(%s) hatası: %s", sym, exc)
                return None

    results = await asyncio.gather(*(_one_symbol(s) for s in symbols))

    new_summaries: dict[str, dict[str, Any]] = {}
    crossover_signals: list[dict[str, Any]] = []
    for sym, summary in zip(symbols, results):
        if summary is None:
            continue
        new_summaries[sym] = summary
        # Crossover sinyallerini de yakalayıp deque'a ekle (sound notif için)
        for period, pdata in summary["periods"].items():
            if pdata.get("signal") is not None:
                crossover_signals.append({
                    "symbol": sym,
                    "period": period,
                    "type": pdata["signal"],
                    "price": pdata["price"],
                    "angle_score": pdata["angle_score"],
                    "volume_spike": pdata["volume_spike"],
                    "timestamp": pdata["timestamp"],
                })

    async with STATE.summaries_lock:
        STATE.symbol_summaries = new_summaries
        STATE.last_summary_at = time.time()

    await _process_elite_history(new_summaries)
    await _process_alpha_history_pool(new_summaries)

    new_signal_count = 0
    if crossover_signals:
        async with STATE.signals_lock:
            existing_keys = {
                (s["symbol"], s["period"], s["timestamp"]) for s in STATE.signals
            }
            for sig in crossover_signals:
                key = (sig["symbol"], sig["period"], sig["timestamp"])
                if key in existing_keys:
                    continue
                existing_keys.add(key)
                STATE.signals.append(sig)
                new_signal_count += 1
                logger.info(
                    "SİNYAL %s %s %s @ %.6f | angle=%.6f vol_spike=%.2fx",
                    sig["symbol"],
                    sig["period"],
                    sig["type"],
                    sig["price"],
                    sig["angle_score"],
                    sig["volume_spike"],
                )

    elapsed = time.monotonic() - started
    avg_score = (
        sum(s["confluence_score"] for s in new_summaries.values())
        / max(1, len(new_summaries))
    )
    depth_fetched = sum(
        1 for s in new_summaries.values()
        if s.get("orderbook_path_clear") is not None
    )
    depth_skipped = len(new_summaries) - depth_fetched
    logger.info(
        "Sembol özetleri güncellendi: sembol=%d ortalama_score=%.2f "
        "yeni_sinyal=%d depth_sorgu=%d atlandi=%d süre=%.2fs",
        len(new_summaries),
        avg_score,
        new_signal_count,
        depth_fetched,
        depth_skipped,
        elapsed,
    )
    return new_signal_count


async def summary_loop() -> None:
    """Periyodik olarak tüm sembollerin özet kayıtlarını yeniler.

    Her SCAN_INTERVAL_SECONDS'da bir update_all_summaries çağrılır;
    /signals endpoint'i bu özetleri O(1) read ile servis eder.
    """
    logger.info("Özet hesaplama döngüsü başlatıldı (her %ss).", SCAN_INTERVAL_SECONDS)
    while True:
        try:
            await update_all_summaries()
        except Exception as exc:  # noqa: BLE001
            logger.exception("update_all_summaries hatası: %s", exc)
        await asyncio.sleep(SCAN_INTERVAL_SECONDS)


async def mcap_refresh_loop() -> None:
    """Sembol evrenini periyodik tazeler; yeni listelenen / delist edilen
    Binance USDT paritelerini otomatik olarak yansıtır.

    Veri kaynağı tamamen Binance olduğu için yeni listelemeler (Binance
    Alpha vb.) bir sonraki tazelemede tarama listesine eklenir.
    """
    while True:
        await asyncio.sleep(MCAP_REFRESH_MINUTES * 60)
        try:
            groups = await get_market_caps(use_cache=False)
            new_symbols = flatten_groups(groups)
            added = sorted(set(new_symbols) - set(STATE.symbols))
            removed = sorted(set(STATE.symbols) - set(new_symbols))
            STATE.groups = groups
            STATE.symbols = new_symbols
            logger.info(
                "Sembol evreni yenilendi. eklenen=%d çıkarılan=%d toplam=%d",
                len(added),
                len(removed),
                len(new_symbols),
            )
            if added:
                logger.info("Yeni listelenenler (ilk 10): %s", added[:10])
            # NOT: WS yeniden konfigürasyonu kapsam dışı bırakıldı; restart önerilir.
        except MarketCapError as exc:
            logger.warning("Sembol evreni yenileme atlandı: %s", exc)


@asynccontextmanager
async def lifespan(_: FastAPI):
    """FastAPI yaşam döngüsü: başlangıçta tüm task'ları ayağa kaldırır."""
    global HTTP_CLIENT

    logger.info("=== Kripto Crossover & Tarama Motoru başlatılıyor ===")
    key_status = validate_keys()
    logger.info("API anahtar durumu: %s", key_status)

    HTTP_CLIENT = httpx.AsyncClient(timeout=HTTP_TIMEOUT)

    # Binance-First: sembol evreni doğrudan Binance Spot'tan
    # (exchangeInfo + 24h hacim) gelir; CMC bağımlılığı kaldırılmıştır.
    try:
        groups = await get_market_caps()
    except MarketCapError as exc:
        logger.error(
            "Başlangıçta Binance sembol evreni alınamadı: %s. "
            "Sembol listesi boş başlayacak.",
            exc,
        )
        groups = {}

    STATE.groups = groups
    STATE.symbols = flatten_groups(groups)
    logger.info(
        "Aktif sembol sayısı: %d (gruplar=%s)",
        len(STATE.symbols),
        {k: len(v) for k, v in groups.items()},
    )

    background_tasks: list[asyncio.Task] = []

    if IS_VERCEL:
        logger.info(
            "Vercel serverless modu aktif — WS/summary_loop devre dışı. "
            "Veri tazeleme: GET /cron/refresh (Vercel Cron veya manuel)."
        )
    elif STATE.symbols:
        await backfill_all(STATE.symbols)
        background_tasks.append(
            asyncio.create_task(websocket_listener(STATE.symbols), name="ws_listener")
        )
        background_tasks.append(asyncio.create_task(summary_loop(), name="summary_loop"))
        background_tasks.append(asyncio.create_task(mcap_refresh_loop(), name="mcap_refresh"))
    else:
        background_tasks.append(asyncio.create_task(summary_loop(), name="summary_loop"))
        background_tasks.append(asyncio.create_task(mcap_refresh_loop(), name="mcap_refresh"))

    try:
        yield
    finally:
        logger.info("Kapatma sinyali alındı, task'lar iptal ediliyor...")
        for t in background_tasks:
            t.cancel()
        for t in background_tasks:
            try:
                await t
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        if HTTP_CLIENT is not None:
            await HTTP_CLIENT.aclose()
        logger.info("Temiz kapatma tamamlandı.")


app = FastAPI(
    title="Kripto Crossover & Tarama Motoru",
    version="1.0.0",
    description=(
        "Binance REST/WS + TradingView-uyumlu doğrusal regresyon crossover "
        "tarayıcısı. /signals endpoint'i ile sinyallere erişin."
    ),
    lifespan=lifespan,
)

# Frontend (Vite / Vercel) + Hugging Face: varsayılan tüm kökenlere izin.
# CORS_ALLOW_ALL=0 yapılırsa ALLOWED_ORIGINS env listesi kullanılır.
_cors_allow_all = os.getenv("CORS_ALLOW_ALL", "1").strip().lower() in (
    "1", "true", "yes", "*",
)

if _cors_allow_all:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )
else:
    _allowed_origins: Final[list[str]] = _build_allowed_origins()
    app.add_middleware(
        CORSMiddleware,
        allow_origins=_allowed_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )


@app.get("/", tags=["meta"])
async def root() -> dict[str, Any]:
    return {
        "service": "kripto-crossover-tarama-motoru",
        "status": "running",
        "endpoints": [
            "/health", "/symbols", "/signals", "/groups",
            "/alpha-history", "/weekly-performance", "/scan/{period}",
        ],
        "periots": PERIOTS,
    }


@app.get("/health", tags=["meta"])
async def health() -> dict[str, Any]:
    return {
        "ok": True,
        "deployment_mode": "vercel" if IS_VERCEL else "local",
        "vercel_ready": _vercel_ready if IS_VERCEL else True,
        "summaries_count": len(STATE.symbol_summaries),
        "ws_connected": STATE.ws_connected,
        "ws_last_message_age_sec": (
            time.time() - STATE.ws_last_message_at
            if STATE.ws_last_message_at
            else None
        ),
        "symbols_tracked": len(STATE.symbols),
        "groups_breakdown": {
            group: len(syms) for group, syms in STATE.groups.items()
        },
        "server_time_ms": int(time.time() * 1000),
        "last_scan_at": STATE.last_scan_at,
        "last_scan_period": STATE.last_scan_period,
        "last_scan_signal_count": STATE.last_scan_signal_count,
    }


@app.get("/symbols", tags=["meta"])
async def symbols() -> dict[str, Any]:
    return {"count": len(STATE.symbols), "symbols": STATE.symbols}


@app.get("/groups", tags=["meta"])
async def groups_endpoint() -> dict[str, Any]:
    return {k: {"count": len(v), "symbols": v} for k, v in STATE.groups.items()}


def _latest_close_price(symbol: str, period: str) -> float | None:
    """STATE.frames'ten ilgili (symbol, period) için son bilinen kapanış
    fiyatını döner; yoksa None.
    """
    df = STATE.frames.get(symbol, {}).get(period)
    if df is None or df.empty:
        return None
    try:
        return float(df["close"].iloc[-1])
    except (KeyError, IndexError, ValueError):
        return None


@app.get("/signals", tags=["signals"])
async def list_signals(
    symbol: str | None = Query(default=None, description="Tek sembol filtresi"),
    direction: str | None = Query(default=None, description="AL veya SAT"),
    min_score: int = Query(default=0, ge=0, le=20, description="Minimum confluence skoru (varsayılan 0 = filtre yok)"),
    limit: int = Query(default=2000, ge=1, le=5000),
) -> JSONResponse:
    """Sembol-bazlı confluence kayıtlarını döner.

    DEFAULT olarak HİÇBİR FİLTRE UYGULAMAZ — skor 0 olan semboller dahil
    tüm aktif evren döner. Bu, dış sistemlerin (otomatik trading bot,
    webhook tüketicisi vs.) tam veri akışına ihtiyaç duyduğu senaryolar
    için zorunludur. İsteğe bağlı filtreler `symbol`, `direction`,
    `min_score` query parametreleriyle uygulanabilir.

    Sıralama:
        1. confluence_score DESC (en yüksek hizalama üstte)
        2. quote_volume_24h DESC (eşitlikte 24h hacim büyük olan üstte)
    """
    async with STATE.summaries_lock:
        items = list(STATE.symbol_summaries.values())

    def _match(s: dict[str, Any]) -> bool:
        if symbol and s.get("symbol") != symbol.upper():
            return False
        if direction and s.get("direction") != direction.upper():
            return False
        if (s.get("confluence_score") or 0) < min_score:
            return False
        return True

    filtered = [s for s in items if _match(s)]
    filtered.sort(
        key=lambda s: (
            -int(s.get("confluence_score") or 0),
            -float(s.get("quote_volume_24h") or 0.0),
        )
    )

    return JSONResponse({
        "count": len(filtered),
        "total": len(items),
        "min_score": min_score,
        "periots": PERIOTS,
        "updated_at_ms": int(STATE.last_summary_at * 1000) if STATE.last_summary_at else None,
        "items": filtered[:limit],
    })


@app.get("/alpha-history", tags=["history"])
async def alpha_history(
    limit: int = Query(default=500, ge=1, le=2000),
) -> JSONResponse:
    """Son 24 saat wave-onaylı alpha havuzu (lookback düşüşünde bile kilitli)."""
    now_ms = int(time.time() * 1000)

    async with STATE.alpha_pool_lock:
        pool_items = [
            dict(entry)
            for entry in STATE.alpha_history_pool.values()
            if int(entry.get("locked_until_ms") or 0) >= now_ms
            or int(entry.get("first_sealed_at_ms") or 0) >= now_ms - ALPHA_HISTORY_WINDOW_MS
        ]

    async with STATE.elite_history_lock:
        elite_items = [
            dict(entry)
            for entry in STATE.elite_history.values()
            if int(entry.get("first_triggered_at_ms") or 0) >= now_ms - ALPHA_HISTORY_WINDOW_MS
        ]

    seen: set[str] = set()
    items: list[dict[str, Any]] = []
    for entry in sorted(
        pool_items,
        key=lambda e: -int(e.get("last_sealed_at_ms") or e.get("first_sealed_at_ms") or 0),
    ):
        sym = entry.get("symbol")
        if sym and sym not in seen:
            seen.add(sym)
            items.append({
                "symbol": sym,
                "first_triggered_at_ms": entry.get("first_sealed_at_ms"),
                "last_triggered_at_ms": entry.get("last_sealed_at_ms"),
                "trigger_count": entry.get("seal_count", 1),
                "signal_price": entry.get("call_price"),
                "call_price": entry.get("call_price"),
                "trigger_period": entry.get("trigger_period"),
                "signal_periods": entry.get("signal_periods", []),
                "confluence_score": entry.get("confluence_score", 0),
                "backtest_win_rate": entry.get("backtest_win_rate", 0.0),
                "wave_framework_approved": entry.get("wave_framework_approved", False),
                "seal_reason": entry.get("seal_reason"),
                "locked_until_ms": entry.get("locked_until_ms"),
            })

    for entry in elite_items:
        sym = entry.get("symbol")
        if sym and sym not in seen:
            seen.add(sym)
            items.append({
                **entry,
                "call_price": entry.get("signal_price"),
                "trigger_period": (entry.get("signal_periods") or [None])[0],
                "backtest_win_rate": entry.get("backtest_win_rate", 0.0),
                "wave_framework_approved": entry.get("wave_framework_approved", False),
            })

    items.sort(
        key=lambda e: (
            -int(e.get("wave_framework_approved") or False),
            -float(e.get("backtest_win_rate") or 0.0),
            -int(e.get("trigger_count") or e.get("seal_count") or 0),
            -int(e.get("first_triggered_at_ms") or e.get("first_sealed_at_ms") or 0),
        )
    )

    return JSONResponse({
        "count": len(items),
        "window_hours": 24,
        "updated_at_ms": now_ms,
        "items": items[:limit],
    })


@app.get("/weekly-performance", tags=["history"])
async def weekly_performance(
    limit: int = Query(default=500, ge=1, le=2000),
) -> JSONResponse:
    """Son 7 gün elit sinyalleri + sinyal sonrası max PnL takibi."""
    now_ms = int(time.time() * 1000)
    cutoff = now_ms - WEEKLY_PERFORMANCE_WINDOW_MS

    async with STATE.elite_history_lock:
        items = [
            dict(entry)
            for entry in STATE.elite_history.values()
            if int(entry.get("first_triggered_at_ms") or 0) >= cutoff
        ]

    items.sort(
        key=lambda e: (
            -float(e.get("max_pnl_pct") or 0.0),
            -int(e.get("first_triggered_at_ms") or 0),
        )
    )

    return JSONResponse({
        "count": len(items),
        "window_days": 7,
        "updated_at_ms": now_ms,
        "items": items[:limit],
    })


@app.get("/signals/raw", tags=["signals"])
async def list_signals_raw(
    period: str | None = Query(default=None),
    symbol: str | None = Query(default=None),
    type_: str | None = Query(default=None, alias="type"),
    limit: int = Query(default=100, ge=1, le=MAX_SIGNAL_HISTORY),
) -> JSONResponse:
    """Eski tip ham crossover sinyal listesi (geriye dönük uyumluluk).

    Frontend artık `/signals` (sembol-bazlı confluence) kullanır; bu
    endpoint debug ve eski entegrasyonlar için saklanmıştır.
    """
    async with STATE.signals_lock:
        items = list(STATE.signals)

    def _match(s: dict[str, Any]) -> bool:
        if period and s.get("period") != period:
            return False
        if symbol and s.get("symbol") != symbol.upper():
            return False
        if type_ and s.get("type") != type_.upper():
            return False
        return True

    filtered = [s for s in items if _match(s)]
    filtered.reverse()

    enriched: list[dict[str, Any]] = []
    for s in filtered[:limit]:
        sym = s.get("symbol")
        per = s.get("period")
        cp = _latest_close_price(sym, per) if isinstance(sym, str) and isinstance(per, str) else None
        enriched.append({**s, "current_price": cp})

    return JSONResponse({"count": len(filtered), "items": enriched})


@app.post("/scan/refresh", tags=["signals"])
async def trigger_full_refresh() -> dict[str, Any]:
    """Tüm sembollerin özet kayıtlarını anında yeniler (manuel trigger)."""
    count = await update_all_summaries()
    return {
        "ok": True,
        "symbols_updated": len(STATE.symbol_summaries),
        "new_crossover_signals": count,
    }


@app.post("/scan/{period}", tags=["signals"])
async def trigger_scan(period: str) -> dict[str, Any]:
    """Eski endpoint — artık tüm periyotları yeniler (period parametresi
    yalnızca bilgi amaçlı)."""
    if period not in PERIOTS:
        raise HTTPException(
            status_code=400,
            detail=f"Geçersiz periyot '{period}'. İzin verilen: {PERIOTS}",
        )
    count = await update_all_summaries()
    return {"period": period, "signals_found": count}


if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("PORT", "8000"))
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=port,
        reload=False,
        log_level="info",
    )
