"""
CryptoScan Burst Bot — фоновый бот для обнаружения всплесков объёма на Binance Futures.

Логика полностью повторяет режим "Объём в $" из CryptoScan:
- Берёт топ-150 ликвидных монет по объёму 24ч
- Для каждой монеты считает, сколько из последних 30 минутных свечей дали объём ≥ $1M
- Если таких свечей ≥ 10 — это "всплеск"
- Исключает тяжеловесов: если в широком окне (60 минут) доля "горячих" свечей ≥ 70%,
  это нормальное фоновое состояние монеты (например BTC, ETH), а не всплеск интереса
- При появлении НОВОГО всплеска отправляет сообщение в Telegram (не повторяет,
  пока всплеск длится непрерывно; повторно — не чаще раза в 30 минут на одну монету)

АРХИТЕКТУРА ДЛЯ БЕСПЛАТНОГО ХОСТИНГА (Render Web Service):
Бесплатный тариф Render не поддерживает отдельный тип "Background Worker" — только
"Web Service", который должен отвечать на HTTP и засыпает после 15 минут без запросов.
Поэтому бот обёрнут в минимальный Flask-сервер с одним эндпоинтом "/" — цикл проверки
всплесков работает в отдельном фоновом потоке внутри того же процесса. Чтобы Render не
укладывал сервис спать, нужно настроить бесплатный внешний пинг-сервис (например
UptimeRobot), который будет дёргать публичный URL сервиса раз в 5-10 минут.
Подробная инструкция — в README.md.
"""

import os
import time
import logging
import threading

import requests
from flask import Flask

# ── НАСТРОЙКИ (берутся из переменных окружения, см. README.md) ──────────────
TG_BOT_TOKEN = os.environ.get("TG_BOT_TOKEN", "")
TG_CHAT_ID = os.environ.get("TG_CHAT_ID", "")

# Upstash Redis (REST API) — хранит состояние дедупликации алертов (какие монеты уже
# считались всплеском, когда последний раз слался алерт по каждой) во внешнем месте,
# которое переживает перезапуск процесса. Без этого при каждом деплое/перезапуске на
# Render бот "забывает" свою память и может прислать повторные алерты по тем же монетам.
UPSTASH_REDIS_URL = os.environ.get("UPSTASH_REDIS_URL", "").rstrip("/")
UPSTASH_REDIS_TOKEN = os.environ.get("UPSTASH_REDIS_TOKEN", "")
REDIS_KEY_ALERTED = "cryptoscan:burst_alerted_syms"
REDIS_KEY_COOLDOWN = "cryptoscan:burst_alert_cooldown"

# Параметры всплеска — совпадают с теми, что в самом скринере
BURST_WINDOW_MIN = 30          # окно поиска всплеска (минут)
BURST_MIN_CANDLES = 10         # минимум "горячих" свечей в этом окне
BURST_THRESHOLD_USD = 1_000_000  # порог объёма на одну свечу ($)
HEAVYWEIGHT_WINDOW_MIN = 60     # широкое окно для проверки на тяжеловеса
HEAVYWEIGHT_RATIO = 0.45        # если 45%+ свечей в широком окне "горячие" — это норма монеты
MIN_24H_VOLUME_USD = 300_000    # минимальный объём 24ч, чтобы монета считалась ликвидной
TOP_N_BY_VOLUME = 150           # сколько топ-монет по объёму проверяем
CHECK_INTERVAL_SEC = 60         # как часто повторять проверку (секунд)
ALERT_COOLDOWN_SEC = 60 * 60    # не слать повторный алерт по той же монете чаще раза в час

# Ручной список исключений — крупные монеты и не-волатильные/привязанные токены
# (стейблкоины, токенизированное золото и т.п.), у которых высокий объём — это
# нормальное состояние, а не сигнал интереса для скальпинга. Авто-фильтр по доле
# "горячих" свечей не всегда ловит такие случаи (например SOL может быть не настолько
# стабильно объёмной, чтобы пройти порог HEAVYWEIGHT_RATIO, но всё равно неинтересна
# как "всплеск" для второго-третьего эшелона), поэтому список исключений работает
# вместе с авто-фильтром, а не вместо него.
EXCLUDED_SYMBOLS = {
    "BTC", "ETH", "SOL", "BNB", "XRP", "DOGE", "ADA", "TRX", "AVAX", "LINK",
    "PAXG", "XAUT", "XAU", "XAG",  # золото, серебро (включая товарные перпетуалы)
    "CL", "BRENT", "WTI", "NG",  # нефть, природный газ
    "FDUSD", "USDC", "USDT", "TUSD", "DAI", "BUSD",  # стейблкоины
}

BINANCE_TICKER_URL = "https://fapi.binance.com/fapi/v1/ticker/24hr"
BINANCE_KLINES_URL = "https://fapi.binance.com/fapi/v1/klines"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("burst-bot")

# Состояние между циклами: какие монеты уже считались всплеском, и когда последний
# раз отправлялся алерт по каждой монете. Хранится локально в памяти процесса как
# кэш, но синхронизируется с Upstash Redis (если он настроен), чтобы переживать
# перезапуск процесса.
burst_alerted_syms: set[str] = set()
burst_alert_cooldown: dict[str, float] = {}
last_cycle_summary = "Бот запущен, первый цикл ещё не завершён"
_redis_loaded = False  # подгружали ли мы состояние из Redis после старта процесса


def _redis_configured() -> bool:
    return bool(UPSTASH_REDIS_URL and UPSTASH_REDIS_TOKEN)


def _redis_headers() -> dict:
    return {"Authorization": f"Bearer {UPSTASH_REDIS_TOKEN}"}


def redis_load_state() -> None:
    """Подгружает burst_alerted_syms и burst_alert_cooldown из Upstash Redis при старте
    процесса — это и есть защита от повторных алертов после перезапуска/редеплоя."""
    global burst_alerted_syms, burst_alert_cooldown, _redis_loaded
    if not _redis_configured():
        log.warning(
            "UPSTASH_REDIS_URL/UPSTASH_REDIS_TOKEN не заданы — состояние дедупликации "
            "не персистентно и сбросится при следующем перезапуске процесса. См. README.md."
        )
        _redis_loaded = True
        return
    try:
        r1 = requests.get(f"{UPSTASH_REDIS_URL}/get/{REDIS_KEY_ALERTED}", headers=_redis_headers(), timeout=10)
        r2 = requests.get(f"{UPSTASH_REDIS_URL}/get/{REDIS_KEY_COOLDOWN}", headers=_redis_headers(), timeout=10)
        alerted_raw = r1.json().get("result") if r1.ok else None
        cooldown_raw = r2.json().get("result") if r2.ok else None
        import json as _json
        burst_alerted_syms = set(_json.loads(alerted_raw)) if alerted_raw else set()
        burst_alert_cooldown = _json.loads(cooldown_raw) if cooldown_raw else {}
        log.info(f"Состояние дедупликации загружено из Redis: {len(burst_alerted_syms)} активных всплесков, {len(burst_alert_cooldown)} записей cooldown")
    except Exception as e:
        log.error(f"Не удалось загрузить состояние из Redis: {e}")
    _redis_loaded = True


def redis_save_state() -> None:
    """Сохраняет текущее burst_alerted_syms и burst_alert_cooldown в Upstash Redis,
    чтобы они переживали следующий перезапуск процесса."""
    if not _redis_configured():
        return
    try:
        import json as _json
        alerted_json = _json.dumps(list(burst_alerted_syms))
        cooldown_json = _json.dumps(burst_alert_cooldown)
        requests.post(f"{UPSTASH_REDIS_URL}/set/{REDIS_KEY_ALERTED}", headers=_redis_headers(), json=alerted_json, timeout=10)
        requests.post(f"{UPSTASH_REDIS_URL}/set/{REDIS_KEY_COOLDOWN}", headers=_redis_headers(), json=cooldown_json, timeout=10)
    except Exception as e:
        log.error(f"Не удалось сохранить состояние в Redis: {e}")


def fetch_tickers() -> dict[str, dict]:
    """Получает 24h-тикеры всех USDT-перпетуалов на Binance Futures."""
    resp = requests.get(BINANCE_TICKER_URL, timeout=10)
    if resp.status_code == 451:
        raise requests.HTTPError(
            "451 — Binance блокирует запросы из региона этого сервера. "
            "Пересоздай сервис на Render с регионом Frankfurt или Singapore (см. README.md)."
        )
    resp.raise_for_status()
    data = resp.json()
    coins = {}
    for t in data:
        symbol = t.get("symbol", "")
        if not symbol.endswith("USDT"):
            continue
        sym = symbol[:-4]  # убираем суффикс USDT
        try:
            price = float(t["lastPrice"])
            change = float(t["priceChangePercent"])
            volume = float(t["quoteVolume"])
        except (KeyError, ValueError):
            continue
        coins[sym] = {"symbol": sym, "price": price, "change": change, "volume": volume}
    return coins


def fetch_klines(symbol: str, limit: int) -> list | None:
    """Получает последние N минутных свечей для монеты. Возвращает None при ошибке."""
    try:
        resp = requests.get(
            BINANCE_KLINES_URL,
            params={"symbol": f"{symbol}USDT", "interval": "1m", "limit": limit},
            timeout=5,
        )
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        log.debug(f"Klines fetch failed for {symbol}: {e}")
        return None


def compute_burst(klines: list) -> tuple[int, bool]:
    """
    Возвращает (burst_count, is_heavyweight) для списка свечей.
    klines — список свечей Binance: [openTime, open, high, low, close, volume,
    closeTime, quoteAssetVolume, ...], где quoteAssetVolume (индекс 7) — объём в USDT.
    """
    candle_vols_usd = []
    for k in klines:
        try:
            candle_vols_usd.append(float(k[7]))
        except (IndexError, ValueError, TypeError):
            candle_vols_usd.append(0.0)

    window_slice = candle_vols_usd[-BURST_WINDOW_MIN:]
    burst_count = sum(1 for v in window_slice if v >= BURST_THRESHOLD_USD)

    bg_slice = candle_vols_usd[-HEAVYWEIGHT_WINDOW_MIN:]
    bg_high_count = sum(1 for v in bg_slice if v >= BURST_THRESHOLD_USD)
    bg_ratio = bg_high_count / len(bg_slice) if bg_slice else 0
    is_heavyweight = bg_ratio >= HEAVYWEIGHT_RATIO

    return burst_count, is_heavyweight


def send_telegram_alert(sym: str, coin: dict, burst_count: int) -> None:
    """Отправляет сообщение в Telegram о новом всплеске объёма."""
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        log.warning("TG_BOT_TOKEN/TG_CHAT_ID не заданы — алерт не отправлен")
        return

    sign = "+" if coin["change"] >= 0 else ""
    text = (
        f"🔥 *Объём в $* — обнаружен всплеск\n"
        f"*{sym}/USDT* — BINANCE\n"
        f"Цена: `${coin['price']:.6g}` ({sign}{coin['change']:.2f}%)\n"
        f"Свечей ≥$1M за {BURST_WINDOW_MIN}м: `{burst_count}/{BURST_WINDOW_MIN}`\n"
        f"Объём 24ч: `${coin['volume']:,.0f}`"
    )
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    try:
        resp = requests.post(
            url,
            json={"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "Markdown"},
            timeout=10,
        )
        if resp.ok:
            log.info(f"Алерт отправлен: {sym} ({burst_count}/{BURST_WINDOW_MIN})")
        else:
            log.error(f"Telegram API ошибка: {resp.status_code} {resp.text}")
    except requests.RequestException as e:
        log.error(f"Не удалось отправить алерт в Telegram: {e}")


def run_cycle() -> None:
    """Один полный цикл проверки: тикеры → klines → расчёт всплесков → алерты."""
    global burst_alerted_syms, last_cycle_summary

    # Перечитываем состояние дедупликации из Redis в начале КАЖДОГО цикла, а не только
    # при старте процесса. Это устраняет любую зависимость от того, переживает ли
    # состояние в памяти между итерациями — единственный источник правды теперь Redis,
    # независимо от того, сколько раз и почему процесс перезапускался между циклами.
    redis_load_state()

    try:
        coins = fetch_tickers()
    except requests.RequestException as e:
        log.error(f"Не удалось получить тикеры с Binance: {e}")
        last_cycle_summary = f"Ошибка получения тикеров: {e}"
        return

    candidates = [
        c for c in coins.values()
        if c["volume"] >= MIN_24H_VOLUME_USD and c["symbol"] not in EXCLUDED_SYMBOLS
    ]
    candidates.sort(key=lambda c: c["volume"], reverse=True)
    candidates = candidates[:TOP_N_BY_VOLUME]

    limit = HEAVYWEIGHT_WINDOW_MIN + 2
    new_burst_syms: set[str] = set()

    for coin in candidates:
        sym = coin["symbol"]
        klines = fetch_klines(sym, limit)
        if not klines or len(klines) < 2:
            continue

        burst_count, is_heavyweight = compute_burst(klines)
        is_burst_now = burst_count >= BURST_MIN_CANDLES and not is_heavyweight

        if is_burst_now:
            new_burst_syms.add(sym)
            was_already = sym in burst_alerted_syms
            last_alert = burst_alert_cooldown.get(sym)
            now = time.time()
            cooled_down = last_alert is None or (now - last_alert) >= ALERT_COOLDOWN_SEC

            if not was_already and cooled_down:
                # Сразу записываем cooldown и алертед-статус в локальный кэш и в Redis
                # ДО отправки сообщения — это минимизирует (хотя не гарантированно
                # исключает на 100%, без поддержки распределённых локов Redis) окно
                # гонки, в котором два параллельных процесса могли бы оба решить, что
                # алерт по этой монете ещё не отправлялся.
                burst_alert_cooldown[sym] = now
                burst_alerted_syms.add(sym)
                redis_save_state()
                send_telegram_alert(sym, coin, burst_count)

        # Небольшая пауза между запросами, чтобы не упереться в rate limit Binance
        time.sleep(0.05)

    burst_alerted_syms = new_burst_syms
    redis_save_state()
    summary = f"Проверено {len(candidates)} монет, активных всплесков: {len(new_burst_syms)} ({', '.join(sorted(new_burst_syms)) or '—'})"
    log.info(f"Цикл завершён. {summary}")
    last_cycle_summary = summary


def background_loop() -> None:
    """Бесконечный цикл проверки всплесков — работает в отдельном потоке от Flask-сервера."""
    log.info("CryptoScan Burst Bot — фоновый цикл запущен")
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        log.warning(
            "TG_BOT_TOKEN или TG_CHAT_ID не заданы! "
            "Бот будет работать, но алерты не отправятся. См. README.md."
        )

    while True:
        start = time.time()
        try:
            run_cycle()
        except Exception as e:
            log.exception(f"Необработанная ошибка в цикле: {e}")

        elapsed = time.time() - start
        sleep_time = max(0, CHECK_INTERVAL_SEC - elapsed)
        time.sleep(sleep_time)


# ── HTTP-обвязка для бесплатного Render Web Service ──────────────────────
# Render free tier требует, чтобы сервис отвечал на HTTP-запросы (тип "Web Service"),
# поэтому оборачиваем фоновый цикл в минимальный Flask-сервер с одним эндпоинтом.
# Внешний пинг-сервис (см. README.md) должен дёргать этот URL раз в 5-10 минут,
# чтобы Render не "укладывал" процесс спать после 15 минут без запросов.
app = Flask(__name__)


@app.route("/")
def health_check():
    tg_status = "настроен" if (TG_BOT_TOKEN and TG_CHAT_ID) else "НЕ настроен (проверь переменные окружения)"
    redis_status = "настроен (алерты переживут перезапуск)" if _redis_configured() else "НЕ настроен (после перезапуска возможны повторные алерты — см. README.md)"
    return (
        f"CryptoScan Burst Bot работает.\n"
        f"Telegram: {tg_status}\n"
        f"Redis (персистентность): {redis_status}\n"
        f"Последний цикл: {last_cycle_summary}\n"
    )


if __name__ == "__main__":
    # Запускаем фоновый цикл проверки в отдельном потоке, а Flask — в основном
    worker_thread = threading.Thread(target=background_loop, daemon=True)
    worker_thread.start()

    port = int(os.environ.get("PORT", 8080))
    # debug=False и use_reloader=False — критично: иначе werkzeug может форкнуть
    # дочерний процесс, который снова выполнит весь скрипт с нуля (включая создание
    # ВТОРОГО worker_thread). Тогда два независимых цикла проверки будут читать/писать
    # в Redis почти одновременно, и оба могут отправить алерт по одной и той же монете
    # до того как другой успеет обновить состояние — именно это вызывало дублирующиеся
    # алерты, приходящие кластерами с разницей в секунды.
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)
