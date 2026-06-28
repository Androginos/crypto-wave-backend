"""
config.py
---------
Uygulama genelinde kullanılacak ortam değişkenlerini, API anahtarlarını ve
operasyonel sabitleri merkezi olarak yönetir.

Tüm gizli bilgiler `.env` dosyasından `python-dotenv` aracılığıyla okunur;
böylece kaynak koduna asla hassas veri gömülmez.
"""

from __future__ import annotations

import logging
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Final

from dotenv import load_dotenv

BASE_DIR: Final[Path] = Path(__file__).resolve().parent
ENV_PATH: Final[Path] = BASE_DIR / ".env"

# Log dosyaları proje kökünde (`botty/logs/`) tutulur — backend/
# içinde olursa uvicorn --reload her log yazımını kod değişikliği
# sanıp sonsuz reload döngüsüne girer.
LOG_DIR: Final[Path] = BASE_DIR.parent / "logs"
LOG_FILE: Final[Path] = LOG_DIR / "app.log"

load_dotenv(dotenv_path=ENV_PATH, override=False)

BINANCE_API_KEY: Final[str] = os.getenv("BINANCE_API_KEY", "")
BINANCE_SECRET_KEY: Final[str] = os.getenv("BINANCE_SECRET_KEY", "")
CMC_API_KEY: Final[str] = os.getenv("CMC_API_KEY", "")

# Multi-Timeframe Confluence taraması için aktif periyot listesi.
# NOT: Binance Spot `/api/v3/klines` endpoint'i 45m'yi DESTEKLEMEZ
# (yalnızca: 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M).
# Bu yüzden 45m liste dışında bırakılmıştır. Confluence skoru = X / 9.
PERIOTS: Final[list[str]] = [
    "5m", "15m", "30m", "1h", "2h", "4h", "8h", "1d", "1w",
]

# Her periyot için tam olarak çekilecek bar sayısı (REST backfill ve
# güncellemeler dahil). LRC 300 + dinamik crossover hafızası + EMA 200.
# Alt TF (5m–2h): 320 bar | Makro TF (4h–1w): 350 bar (4h EMA200 + backtest).
# NOT: Binance 45m desteklemez; 45m bu listede yok.
PERIOD_BAR_LIMITS: Final[dict[str, int]] = {
    "5m":  350,
    "15m": 350,
    "30m": 350,
    "1h":  350,
    "2h":  350,
    "4h":  350,
    "8h":  350,
    "1d":  350,
    "1w":  350,
}

# LRC 300 ana mimarisi — tüm periyotlarda Pine Script ile birebir pencere.
# indicators.py matematik motoru formülü değişmez; yalnızca pencere uzunluğu.
#
# İlişki: PERIOD_BAR_LIMITS[period] >= H_LEN + CROSSOVER_LOOKBACK + 2
H_LEN_MAP: Final[dict[str, int]] = {
    "5m":  300,
    "15m": 300,
    "30m": 300,
    "1h":  300,
    "2h":  300,
    "4h":  300,
    "8h":  300,
    "1d":  300,
    "1w":  300,
}
L_LEN_MAP: Final[dict[str, int]] = dict(H_LEN_MAP)

# Periyot bazlı crossover/crossunder dinamik hafıza (lookback) matrisi.
# Zaman diliminin matematiksel ağırlığına göre geriye dönük geçerlilik ömrü.
# Örn: 5m × 12 bar ≈ 1 saat | 4h × 30 bar ≈ 5 gün | 1w × 40 bar ≈ 40 hafta.
CROSSOVER_LOOKBACK_MAP: Final[dict[str, int]] = {
    "1w":  20,   # Makro trend — en geniş hafıza
    "1d":  15,
    "8h":  12,
    "4h":  12,
    "2h":  8,
    "1h":  8,
    "30m": 6,
    "15m": 6,
    "5m":  4,    # Taze momentum — en dar (≈20 dk)
}
DEFAULT_CROSSOVER_LOOKBACK: Final[int] = 12

# TradingView LRC ALPEREN paneli ile birebir karşılaştırma periyotları.
# Pine: f_check("15"), f_check("60"), f_check("240"), f_check("D")
TV_DASHBOARD_PERIODS: Final[list[str]] = ["15m", "1h", "4h", "1d"]
TV_DASHBOARD_LABELS: Final[dict[str, str]] = {
    "15m": "15",
    "1h":  "60",
    "4h":  "240",
    "1d":  "D",
}

# Eski tek-değer API'sini kullanan eski kod yolları için emniyet fallback'i.
H_LEN: Final[int] = 300
L_LEN: Final[int] = 300

# Geriye dönük uyumluluk için tutulur; PERIOD_BAR_LIMITS yoksa fallback.
KLINE_FETCH_LIMIT: Final[int] = 350

MCAP_REFRESH_MINUTES: Final[int] = 60
SCAN_INTERVAL_SECONDS: Final[int] = 60

WS_BASE_URL: Final[str] = "wss://stream.binance.com:9443/stream"
REST_BASE_URL: Final[str] = "https://api.binance.com"
CMC_BASE_URL: Final[str] = "https://pro-api.coinmarketcap.com"

MCAP_GROUPS: Final[dict[str, tuple[float, float]]] = {
    "large": (0.00, 0.10),
    "mid": (0.10, 0.40),
    "small": (0.40, 1.00),
}

# ──────────────────────────────────────────────────────────────────────
# STABLECOIN KARA LİSTESİ
# ──────────────────────────────────────────────────────────────────────
# Bu base asset'lere sahip USDT pariteleri tarama evrenine ALINMAZ.
# Stable/USD-pegli coinler hep ~1.00 USDT etrafında dalgalanır; doğrusal
# regresyon crossover ve trend analizi anlamsızdır.
#
# Hem hacim çağrısını hem 9 periyot kline backfill'i hem de WS stream'leri
# elimine eder → ~10-15 sembol × 9 periyot = ~100 daha az istek.
#
# Yeni stablecoin listelendikçe bu set'e eklenebilir.
STABLECOIN_BLACKLIST: Final[frozenset[str]] = frozenset(
    {
        # Tier-1 USD-pegli ana stablecoinler
        "USDC", "FDUSD", "TUSD", "USDP", "DAI", "BUSD", "PYUSD", "USDD",
        # Algoritmik / sentetik USD
        "USDE", "SUSDE", "USDS", "USDX", "USTC", "USD1", "USDY", "GHO",
        # Daha az bilinen stable / pegli token'lar
        "GUSD", "EURS", "EURI", "EUR", "AEUR", "EURT", "CEUR", "RLUSD",
        "FRAX", "LUSD", "USDQ", "XUSD", "CUSD", "CRVUSD", "MKUSD",
        # Wrapped / yield-bearing stable türevleri
        "SUSD", "ESDC", "FUSD", "VAI", "RSV", "AUSD",
    }
)

HTTP_TIMEOUT: Final[float] = 20.0
WS_PING_INTERVAL: Final[float] = 20.0
WS_PING_TIMEOUT: Final[float] = 10.0
WS_RECONNECT_DELAY: Final[float] = 5.0

LOG_LEVEL: Final[str] = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_FORMAT: Final[str] = (
    "%(asctime)s | %(levelname)-8s | %(name)-22s | %(message)s"
)

# Vercel serverless: arka plan WS/loop kapalı, cron ile tazeleme.
IS_VERCEL: Final[bool] = os.getenv("VERCEL") == "1" or bool(os.getenv("VERCEL_ENV"))
# Canlı testte timeout'a girmemek için sembol üst sınırı (0 = sınırsız).
VERCEL_MAX_SYMBOLS: Final[int] = int(os.getenv("VERCEL_MAX_SYMBOLS", "0") or "0")
CRON_SECRET: Final[str] = os.getenv("CRON_SECRET", "")


def configure_logging() -> None:
    """Tek noktadan logging yapılandırması.

    Konsol + dosya (rotating) handler'ları kurar. Tam çıktı her zaman
    `backend/logs/app.log` içine yazılır, böylece terminalden kopyalama
    sorununda dosyadan okunabilir.
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    level = getattr(logging, LOG_LEVEL, logging.INFO)
    formatter = logging.Formatter(LOG_FORMAT, datefmt="%Y-%m-%d %H:%M:%S")

    root = logging.getLogger()
    root.setLevel(level)

    for h in list(root.handlers):
        root.removeHandler(h)

    console = logging.StreamHandler()
    console.setLevel(level)
    console.setFormatter(formatter)
    root.addHandler(console)

    # Vercel dosya sistemi salt okunur; yalnızca konsol log.
    if not IS_VERCEL:
        file_handler = RotatingFileHandler(
            LOG_FILE,
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8",
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)

    logging.getLogger("websockets").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)


def validate_keys() -> dict[str, bool]:
    """API anahtarlarının doluluğunu doğrular (içeriği doğrulamadan)."""
    return {
        "binance_api_key": bool(BINANCE_API_KEY) and "YENI_" not in BINANCE_API_KEY,
        "binance_secret_key": bool(BINANCE_SECRET_KEY)
        and "YENI_" not in BINANCE_SECRET_KEY,
        "cmc_api_key": bool(CMC_API_KEY) and "YENI_" not in CMC_API_KEY,
    }
