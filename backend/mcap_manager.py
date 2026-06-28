"""
mcap_manager.py
---------------
Binance-First sembol evreni yöneticisi.

Eski mimari CoinMarketCap (CMC) market cap sıralamasını baz alıyordu;
bu yaklaşım yeni listelenen / Alpha coinleri kaçırıyor ve Binance'te
olmayan sembollere kline isteği atılmasına yol açıyordu. Bu modül
artık tek doğru kaynağı Binance Spot kabul eder:

  1. ``GET /api/v3/exchangeInfo`` → ``status='TRADING'`` ve
     ``quoteAsset='USDT'`` olan tüm pariteler.
  2. ``GET /api/v3/ticker/24hr``  → her parite için son 24 saatin USDT
     cinsinden hacmi (``quoteVolume``).
  3. ``quoteVolume`` desc sırasıyla evren oluşturulur ve
     ``config.MCAP_GROUPS`` dilimlerine (large/mid/small) göre üç gruba
     bölünür.

Avantajlar:
  * Borsadaki TÜM aktif USDT pariteleri (yeni listelenenler dahil) anında
    tarama listesine girer.
  * 400 Bad Request yağmuru kökten ortadan kalkar; çünkü liste zaten
    Binance'in kendisinden geliyor.
  * Tek bağımlılık Binance'tir; CMC anahtarı/quotası taraması artık
    çalışma şartı değildir (yine de ``config`` içinde tanımlı kalır,
    ileride zenginleştirme için kullanılabilir).

Geriye uyumluluk:
  * ``get_market_caps()``, ``flatten_groups()`` ve ``MarketCapError``
    isimleri korunur; ``main.py`` içinde değişiklik gerekmez.
  * Dönen sözlüğün anahtarları yine ``config.MCAP_GROUPS`` ile
    eşleşir (default: ``large`` / ``mid`` / ``small``).
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Final

import httpx

from config import (
    HTTP_TIMEOUT,
    MCAP_GROUPS,
    MCAP_REFRESH_MINUTES,
    REST_BASE_URL,
    STABLECOIN_BLACKLIST,
)

logger = logging.getLogger("mcap_manager")

EXCHANGE_INFO_ENDPOINT: Final[str] = "/api/v3/exchangeInfo"
TICKER_24HR_ENDPOINT: Final[str] = "/api/v3/ticker/24hr"

_cache_lock = asyncio.Lock()
_cache_data: dict[str, list[str]] = {}
_cache_timestamp: float = 0.0

# Sembol -> 24 saatlik USDT cinsinden işlem hacmi (quoteVolume).
# Her evren yenilemesinde güncellenir; main.py confluence sıralaması
# için bu haritayı `get_quote_volume_map()` ile sorgular.
_quote_volume_map: dict[str, float] = {}


class MarketCapError(RuntimeError):
    """Sembol evreni alınırken oluşan ortak istisna sınıfı.

    İsim geriye dönük uyumluluk için korunmuştur; artık market cap
    yerine Binance hacim/exchangeInfo hatalarını da kapsar.
    """


def _classify_symbols(usdt_symbols: list[str]) -> dict[str, list[str]]:
    """Hacme göre sıralanmış USDT sembol listesini, ``MCAP_GROUPS`` içinde
    tanımlı yüzdesel dilimlere göre dinamik olarak böler.

    ``MCAP_GROUPS`` artık (start_pct, end_pct) çiftleri tutar (0.0–1.0).
    Toplam sembol sayısı ne olursa olsun (500 veya 2000+) gruplar göreli
    konumlarına göre yeniden hesaplanır; yeni listelenen Alpha projeleri
    otomatik olarak en alttaki "small" grubuna düşer.
    """
    n = len(usdt_symbols)
    grouped: dict[str, list[str]] = {}
    for group_name, (start_pct, end_pct) in MCAP_GROUPS.items():
        start = int(round(start_pct * n))
        end = int(round(end_pct * n))
        grouped[group_name] = usdt_symbols[start:end]
    return grouped


async def _fetch_active_usdt_symbols(client: httpx.AsyncClient) -> set[str]:
    """exchangeInfo'dan aktif (TRADING) USDT çiftlerinin set'ini döner."""
    url = f"{REST_BASE_URL}{EXCHANGE_INFO_ENDPOINT}"
    try:
        resp = await client.get(url)
    except httpx.HTTPError as exc:
        raise MarketCapError(f"exchangeInfo isteği başarısız: {exc}") from exc

    if resp.status_code != 200:
        raise MarketCapError(
            f"exchangeInfo HTTP {resp.status_code}: {resp.text[:200]}"
        )

    payload = resp.json()
    symbols_raw = payload.get("symbols")
    if not isinstance(symbols_raw, list):
        raise MarketCapError("exchangeInfo yanıtında 'symbols' listesi yok.")

    active: set[str] = set()
    skipped_stable: list[str] = []
    for s in symbols_raw:
        if not (
            isinstance(s, dict)
            and s.get("status") == "TRADING"
            and s.get("quoteAsset") == "USDT"
            and isinstance(s.get("symbol"), str)
        ):
            continue
        # base asset stable ise tarama evrenine ALMA — crossover analizi
        # 1.00 etrafında dalgalanan coinler için anlamsız.
        base = s.get("baseAsset")
        if isinstance(base, str) and base.upper() in STABLECOIN_BLACKLIST:
            skipped_stable.append(s["symbol"])
            continue
        active.add(s["symbol"])

    if skipped_stable:
        logger.info(
            "Stablecoin filtresi: %d parite atlandı → %s",
            len(skipped_stable),
            sorted(skipped_stable),
        )
    return active


async def _fetch_24hr_tickers(client: httpx.AsyncClient) -> list[dict[str, Any]]:
    """ticker/24hr'dan tüm sembollerin 24 saatlik hacim ham verisini çeker."""
    url = f"{REST_BASE_URL}{TICKER_24HR_ENDPOINT}"
    try:
        resp = await client.get(url)
    except httpx.HTTPError as exc:
        raise MarketCapError(f"ticker/24hr isteği başarısız: {exc}") from exc

    if resp.status_code != 200:
        raise MarketCapError(
            f"ticker/24hr HTTP {resp.status_code}: {resp.text[:200]}"
        )

    data = resp.json()
    if not isinstance(data, list):
        raise MarketCapError("ticker/24hr yanıtı liste değil.")
    return data


async def _build_universe() -> list[str]:
    """Binance Spot'taki TÜM aktif USDT çiftlerini 24 saatlik USDT hacmine
    göre azalan sırayla döndürür.

    Sınır uygulanmaz; yeni listelenen / Alpha projeler dahil borsadaki
    her ``status='TRADING'`` USDT paritesi tarama evrenine girer.
    """
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        active_set, tickers = await asyncio.gather(
            _fetch_active_usdt_symbols(client),
            _fetch_24hr_tickers(client),
        )

    if not active_set:
        raise MarketCapError("Aktif USDT çifti bulunamadı (exchangeInfo boş).")

    candidates: list[tuple[str, float]] = []
    seen: set[str] = set()
    for entry in tickers:
        if not isinstance(entry, dict):
            continue
        sym = entry.get("symbol")
        if not isinstance(sym, str) or sym not in active_set or sym in seen:
            continue
        try:
            qvol = float(entry.get("quoteVolume", 0.0) or 0.0)
        except (TypeError, ValueError):
            qvol = 0.0
        candidates.append((sym, qvol))
        seen.add(sym)

    # Çok yeni listelenen ve henüz ticker/24hr'da görünmeyen sembolleri
    # de evrene dahil et (sıfır hacimle, en sona düşerler).
    for sym in active_set - seen:
        candidates.append((sym, 0.0))

    if not candidates:
        raise MarketCapError(
            "ticker/24hr ile exchangeInfo kesişimi boş; veri tutarsız."
        )

    candidates.sort(key=lambda x: x[1], reverse=True)
    universe = [sym for sym, _ in candidates]

    # quoteVolume haritasını modül seviyesinde cache'le (main.py için).
    global _quote_volume_map
    _quote_volume_map = {sym: vol for sym, vol in candidates}

    logger.info(
        "Binance evreni: aktif=%d, evren=%d (sınırsız). "
        "En yüksek hacimli 5: %s",
        len(active_set),
        len(universe),
        [f"{s}({v:,.0f})" for s, v in candidates[:5]],
    )
    return universe


def get_quote_volume_map() -> dict[str, float]:
    """Son evren yenilemesinde elde edilen sembol → 24h USDT hacmi haritası.

    Confluence sıralamasında tie-breaker olarak ve UI'da "24h Hacim"
    sütununda kullanılmak üzere read-only kopya döner.
    """
    return dict(_quote_volume_map)


async def get_market_caps(use_cache: bool = True) -> dict[str, list[str]]:
    """
    Binance'te aktif işlem gören TÜM USDT çiftlerini 24 saatlik hacme göre
    sıralayıp ``config.MCAP_GROUPS`` yüzdesel dilimlerine göre gruplara
    ayırır.

    Sembol evreni artık sabit 500 ile sınırlı değildir; borsadaki tüm
    pariteler (yaklaşık 2000+) dahildir. Gruplama yüzdesel olduğundan
    evren büyüse de göreli yapı korunur (default: %10 large / %30 mid /
    %60 small).

    Args:
        use_cache: True ise ``MCAP_REFRESH_MINUTES`` süresince bellek-içi
                   sonucu yeniden kullanır.

    Returns:
        ``{"large": [...], "mid": [...], "small": [...]}`` şeklinde
        Binance USDT sembollerini hacim büyüklüğüne göre içeren sözlük.
    """
    global _cache_timestamp, _cache_data

    async with _cache_lock:
        cache_age_sec = time.monotonic() - _cache_timestamp
        cache_valid = (
            use_cache
            and _cache_data
            and cache_age_sec < MCAP_REFRESH_MINUTES * 60
        )
        if cache_valid:
            logger.debug(
                "Sembol evreni cache hit (yaş=%.1fs, gruplar=%s)",
                cache_age_sec,
                {k: len(v) for k, v in _cache_data.items()},
            )
            return {k: list(v) for k, v in _cache_data.items()}

        logger.info("Binance üzerinden sembol evreni yenileniyor (sınırsız)...")
        universe = await _build_universe()

        if not universe:
            raise MarketCapError("Binance sembol evreni boş döndü.")

        grouped = _classify_symbols(universe)

        _cache_data = {k: list(v) for k, v in grouped.items()}
        _cache_timestamp = time.monotonic()

        logger.info(
            "Sembol evreni güncellendi: toplam=%d, gruplar=%s",
            len(universe),
            {k: len(v) for k, v in grouped.items()},
        )
        return grouped


def flatten_groups(groups: dict[str, list[str]]) -> list[str]:
    """Tüm grupları (large + mid + small) tek bir benzersiz listede birleştirir."""
    seen: set[str] = set()
    result: list[str] = []
    for group_name in MCAP_GROUPS.keys():
        for sym in groups.get(group_name, []):
            if sym not in seen:
                seen.add(sym)
                result.append(sym)
    return result


if __name__ == "__main__":
    import json

    from config import configure_logging

    configure_logging()

    async def _demo() -> None:
        try:
            groups = await get_market_caps()
            print(json.dumps({k: v[:5] for k, v in groups.items()}, indent=2))
            print(f"\nToplam benzersiz USDT sembolü: {len(flatten_groups(groups))}")
        except MarketCapError as exc:
            print(f"[HATA] {exc}")

    asyncio.run(_demo())
