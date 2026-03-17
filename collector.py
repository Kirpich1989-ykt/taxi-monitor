#!/usr/bin/env python3
"""
Taxi Demand Monitor — Hourly Collector
Запускается GitHub Actions каждый час.
Сохраняет срез данных в data/latest.json и data/history.json
"""
import os
import json
import time
import requests
import feedparser
from datetime import datetime, timedelta
from pytrends.request import TrendReq

# ─────────────────────────────────────────
# КОНФИГУРАЦИЯ
# Все секреты — из GitHub Secrets (env vars)
# ─────────────────────────────────────────

TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

CITIES = {
    "Якутск": {
        "coords":      (62.0355, 129.6755),
        "kudago_slug": None,
        "trends_geo":  "RU-SA",
        "news_rss":    "https://news.yandex.ru/region/yakutsk/index.rss",
        "osrm_route":  {"from": "129.675,62.035", "to": "129.769,62.093"},
    },
    "Москва": {
        "coords":      (55.7558, 37.6176),
        "kudago_slug": "msk",
        "trends_geo":  "RU-MOW",
        "news_rss":    "https://news.yandex.ru/region/moscow/index.rss",
        "osrm_route":  {"from": "37.617,55.756", "to": "37.561,55.745"},
    },
    "Краснодар": {
        "coords":      (45.0355, 38.9753),
        "kudago_slug": "krasnodar",
        "trends_geo":  "RU-KDA",
        "news_rss":    "https://news.yandex.ru/region/krasnodar/index.rss",
        "osrm_route":  {"from": "38.975,45.035", "to": "39.082,45.005"},
    },
    "Новосибирск": {
        "coords":      (54.9833, 82.8964),
        "kudago_slug": "nsk",
        "trends_geo":  "RU-NVS",
        "news_rss":    "https://news.yandex.ru/region/novosibirsk/index.rss",
        "osrm_route":  {"from": "82.896,54.983", "to": "82.898,54.966"},
    },
}

TAXI_KEYWORDS = ["такси", "яндекс такси", "заказать такси", "вызов такси"]

IMPACT_WEIGHTS = {
    "weather": 0.30,
    "events":  0.25,
    "traffic": 0.20,
    "trends":  0.15,
    "news":    0.10,
}

ALERT_THRESHOLDS = {"green": 0.30, "yellow": 0.55, "red": 0.75}

SKIP_TRENDS = os.environ.get("SKIP_TRENDS", "false").lower() == "true"


def safe_source_result(source_name: str, city: str, value, default_val: str = "no data") -> dict:
    if isinstance(value, dict):
        value.setdefault("city", city)
        value.setdefault("timestamp", datetime.utcnow().isoformat())
        value.setdefault("status", "ok")
        value.setdefault("score", 0.0)
        value.setdefault("val", default_val)
        try:
            value["score"] = round(float(value.get("score", 0.0) or 0.0), 2)
        except (TypeError, ValueError):
            value["score"] = 0.0
        if value.get("val") is None:
            value["val"] = default_val
        if "items" in value and not isinstance(value.get("items"), list):
            value["items"] = []
        if "events" in value and not isinstance(value.get("events"), list):
            value["events"] = []
        return value

    value_type = type(value).__name__
    return {
        "city": city,
        "timestamp": datetime.utcnow().isoformat(),
        "status": "error",
        "score": 0.0,
        "val": f"{source_name} returned no result",
        "error": f"{source_name} returned {value_type}",
    }


def call_source(source_name: str, fetcher, city: str, cfg: dict, *args):
    try:
        result = fetcher(city, cfg, *args)
    except Exception as ex:
        result = {
            "city": city,
            "timestamp": datetime.utcnow().isoformat(),
            "status": "error",
            "score": 0.0,
            "val": f"{source_name} failed",
            "error": str(ex),
        }
    return safe_source_result(source_name, city, result)


def coerce_score(data) -> float:
    if not isinstance(data, dict):
        return 0.0
    try:
        return round(float(data.get("score", 0.0) or 0.0), 2)
    except (TypeError, ValueError):
        return 0.0


def safe_items_count(data, key: str) -> int:
    if not isinstance(data, dict):
        return 0
    value = data.get(key, [])
    return len(value) if isinstance(value, list) else 0


def history_scores(history: dict, city: str, limit: int = 24) -> list:
    city_history = history.get(city, [])
    if not isinstance(city_history, list):
        return []

    scores = []
    for point in city_history[-limit:]:
        if not isinstance(point, dict):
            continue
        try:
            scores.append(float(point.get("score", 0.0) or 0.0))
        except (TypeError, ValueError):
            continue
    return scores

# ─────────────────────────────────────────
# WMO WEATHER CODES → русский
# ─────────────────────────────────────────

WMO_CONDITIONS = {
    0:  ("ясно",                  False),
    1:  ("преим. ясно",           False),
    2:  ("переменная облачность", False),
    3:  ("пасмурно",              False),
    45: ("туман",                 False),
    48: ("изморозь",              False),
    51: ("морось слабая",         True),
    53: ("морось умеренная",      True),
    55: ("морось сильная",        True),
    61: ("дождь слабый",          True),
    63: ("дождь умеренный",       True),
    65: ("дождь сильный",         True),
    71: ("снег слабый",           True),
    73: ("снег умеренный",        True),
    75: ("снег сильный",          True),
    77: ("снежная крупа",         True),
    80: ("ливень слабый",         True),
    81: ("ливень умеренный",      True),
    82: ("ливень сильный",        True),
    85: ("снегопад слабый",       True),
    86: ("снегопад сильный",      True),
    95: ("гроза",                 True),
    96: ("гроза с градом",        True),
    99: ("гроза, сильный град",   True),
}

WMO_ICONS = {
    0: "☀", 1: "🌤", 2: "⛅", 3: "☁", 45: "🌫", 48: "🌫",
    51: "🌦", 53: "🌦", 55: "🌧", 61: "🌧", 63: "🌧", 65: "🌧",
    71: "🌨", 73: "🌨", 75: "❄", 77: "🌨", 80: "🌧", 81: "🌧",
    82: "⛈", 85: "🌨", 86: "❄", 95: "⛈", 96: "⛈", 99: "⛈",
}

def wmo_to_condition(code: int) -> tuple:
    return WMO_CONDITIONS.get(code, (f"код {code}", code >= 51))

def wmo_icon(code: int) -> str:
    return WMO_ICONS.get(code, "🌡")


# ─────────────────────────────────────────
# ИСТОЧНИК 1: Open-Meteo (погода)
# ─────────────────────────────────────────

def fetch_weather(city: str, cfg: dict) -> dict:
    result = {"city": city, "timestamp": datetime.utcnow().isoformat(),
              "status": "ok", "score": 0.0}
    try:
        lat, lon = cfg["coords"]
        url = (
            "https://api.open-meteo.com/v1/forecast"
            f"?latitude={lat}&longitude={lon}"
            "&current=temperature_2m,apparent_temperature,precipitation,"
            "weathercode,windspeed_10m,windgusts_10m,snowfall,rain"
            "&timezone=auto"
        )
        r   = requests.get(url, timeout=10)
        r.raise_for_status()
        cur = r.json()["current"]

        wc                    = cur["weathercode"]
        condition_str, is_bad = wmo_to_condition(wc)
        icon                  = wmo_icon(wc)
        temp                  = cur["temperature_2m"]
        precip                = cur["precipitation"]
        snow                  = cur["snowfall"]
        wind                  = cur["windspeed_10m"]

        sign = "+" if temp >= 0 else ""
        result.update({
            "temp":       temp,
            "feels_like": cur["apparent_temperature"],
            "condition":  condition_str,
            "wmo_code":   wc,
            "icon":       icon,
            "wind":       wind,
            "wind_gusts": cur["windgusts_10m"],
            "precip":     precip,
            "snowfall":   snow,
            "is_bad":     is_bad,
            # Строка для дашборда
            "val":        f"{sign}{round(temp)}°C · {condition_str}",
        })

        # Расчёт score
        score = 0.5 if is_bad else 0.0
        if precip > 2:   score += 0.15
        if precip > 8:   score += 0.20
        if precip > 20:  score += 0.10
        if snow > 1:     score += 0.10
        if wind > 12:    score += 0.05
        if wind > 20:    score += 0.05
        if temp < -20:   score += 0.10
        if temp > 35:    score += 0.05
        result["score"] = round(min(score, 1.0), 2)

    except requests.exceptions.RequestException as ex:
        result["status"] = "network_error"
        result["error"]  = str(ex)
    except Exception as ex:
        result["status"] = "error"
        result["error"]  = str(ex)

    return result


# ─────────────────────────────────────────
# ИСТОЧНИК 2: KudaGo (события)
# ─────────────────────────────────────────

HIGH_IMPACT_CATS = {"concert", "festival", "sport", "theater", "circus", "stand-up"}

def fetch_events(city: str, cfg: dict, hours_ahead: int = 6) -> dict:
    result = {"city": city, "timestamp": datetime.utcnow().isoformat(),
              "status": "ok", "events": [], "score": 0.0,
              "icon": "🎭", "val": "нет данных"}
    slug = cfg.get("kudago_slug")
    if not slug:
        result["status"] = "skipped"
        result["val"]    = "город не поддерживается"
        return result
    try:
        now      = int(time.time())
        deadline = int((datetime.now() + timedelta(hours=hours_ahead)).timestamp())
        r = requests.get(
            "https://kudago.com/public-api/v1.4/events/",
            params={
                "location":     slug,
                "actual_since": now,
                "actual_until": deadline,
                "fields":       "id,title,categories",
                "page_size":    50,
                "text_format":  "text",
            },
            timeout=10
        )
        if r.status_code != 200:
            result["status"] = "skipped"
            result["val"]    = f"KudaGo HTTP {r.status_code}"
            return result
        try:
            data = r.json()
        except Exception:
            result["status"] = "skipped"
            result["val"]    = "KudaGo вернул не JSON"
            return result
        events     = data.get("results", [])
        high_count = 0
        total      = len(events)
        for e in events:
            cats    = [c.get("slug", "") for c in e.get("categories", [])]
            is_high = bool(HIGH_IMPACT_CATS & set(cats))
            if is_high:
                high_count += 1
            result["events"].append({
                "title":   e["title"],
                "cats":    cats,
                "is_high": is_high,
            })
        result["score"] = round(min(high_count * 0.15, 1.0), 2)
        result["val"]   = (
            f"{high_count} крупных из {total}"
            if total > 0 else "событий нет"
        )
    except requests.exceptions.Timeout:
        result["status"] = "skipped"
        result["val"]    = "KudaGo таймаут"
    except requests.exceptions.ConnectionError:
        result["status"] = "skipped"
        result["val"]    = "KudaGo недоступен"
    except Exception as ex:
        result["status"] = "skipped"
        result["val"]    = f"ошибка: {str(ex)[:40]}"
    return result


# ─────────────────────────────────────────
# ИСТОЧНИК 3: OSRM (трафик)
# ─────────────────────────────────────────

def fetch_traffic(city: str, cfg: dict) -> dict:
    result = {"city": city, "timestamp": datetime.utcnow().isoformat(),
              "status": "ok", "jam_score": 0.0, "score": 0.0,
              "icon": "🚗", "val": "нет данных"}
    route = cfg.get("osrm_route")
    if not route:
        result["status"] = "no_route"
        return result
    try:
        url  = (
            f"http://router.project-osrm.org/route/v1/driving/"
            f"{route['from']};{route['to']}"
        )
        r    = requests.get(url, params={"overview": "false"}, timeout=10)
        data = r.json()

        if data.get("code") == "Ok":
            seg      = data["routes"][0]
            duration = seg["duration"]
            distance = seg["distance"]
            speed    = (distance / duration * 3.6) if duration > 0 else 60
            jam      = round(max(0, min(1, (60 - speed) / 45)), 2)
            status_txt = (
                "свободно"        if jam < 0.3 else
                "умеренные пробки" if jam < 0.6 else
                "сильные пробки"
            )
            result.update({
                "jam_score":  jam,
                "speed_kmh":  round(speed, 1),
                "status_txt": status_txt,
                "score":      jam,
                "val":        f"{round(speed)} км/ч · {status_txt}",
            })

    except Exception as ex:
        result["status"] = "error"
        result["error"]  = str(ex)

    return result


# ─────────────────────────────────────────
# ИСТОЧНИК 4: PyTrends (поисковый интерес)
# ─────────────────────────────────────────

def fetch_trends(city: str, cfg: dict, pytrends_client=None) -> dict:
    result = {"city": city, "timestamp": datetime.utcnow().isoformat(),
              "status": "ok", "data": {}, "score": 0.0,
              "icon": "🔍", "val": "нет данных"}
    if SKIP_TRENDS:
        result["status"] = "skipped"
        result["val"] = "disabled in Actions"
        return result

    geo = cfg.get("trends_geo")
    if not geo:
        result["status"] = "no_geo"
        return result
    try:
        pytrends_client.build_payload(
            kw_list=TAXI_KEYWORDS[:5],
            timeframe="now 4-H",
            geo=geo
        )
        df = pytrends_client.interest_over_time()
        if df.empty:
            result["status"] = "no_data"
            result["val"]    = "нет данных Google"
            return result

        scores       = []
        top_kw       = None
        top_kw_delta = 0

        for kw in TAXI_KEYWORDS:
            if kw in df.columns:
                cur = int(df[kw].iloc[-1])
                avg = float(df[kw].mean())
                delta = cur - avg
                result["data"][kw] = {
                    "current": cur,
                    "avg_4h":  round(avg, 1),
                    "trend":   "up" if cur > avg * 1.1 else "down",
                }
                if cur > 0:
                    scores.append(min(cur / 100, 1.0))
                if delta > top_kw_delta:
                    top_kw_delta = delta
                    top_kw       = kw

        result["score"] = round(sum(scores) / len(scores), 2) if scores else 0.0
        if top_kw and top_kw_delta > 0:
            result["val"] = f'"{top_kw}" ↑ +{round(top_kw_delta)}%'
        else:
            result["val"] = "интерес в норме"

        time.sleep(2)

    except Exception as ex:
        result["status"] = "error"
        result["error"]  = str(ex)

    return result


# ─────────────────────────────────────────
# ИСТОЧНИК 5: Яндекс.Новости RSS
# ─────────────────────────────────────────

NEWS_IMPACT = {
    "high": [
        "перекрытие", "закрытие дороги", "дтп", "авария",
        "не работает метро", "отменён рейс", "задержка рейса",
        "эвакуация", "чрезвычайная", "взрыв", "пожар",
    ],
    "medium": [
        "концерт", "матч", "фестиваль", "марафон", "перекроют",
        "снегопад", "гололёд", "ливень", "штормовое", "метель",
    ],
    "low": [
        "пробки", "ремонт дороги", "сужение проезжей",
    ],
}
LEVEL_SCORES = {"high": 0.8, "medium": 0.5, "low": 0.2}

def fetch_news(city: str, cfg: dict) -> dict:
    result = {"city": city, "timestamp": datetime.utcnow().isoformat(),
              "status": "ok", "items": [], "score": 0.0,
              "icon": "📰", "val": "без значимых событий"}
    rss_url = cfg.get("news_rss")
    if not rss_url:
        result["status"] = "no_rss"
        return result
    try:
        feed   = feedparser.parse(rss_url)
        scores = []
        for entry in feed.entries[:25]:
            text  = (entry.title + " " + entry.get("summary", "")).lower()
            level = None
            for lvl in ["high", "medium", "low"]:
                if any(kw in text for kw in NEWS_IMPACT[lvl]):
                    level = lvl
                    break
            if level:
                result["items"].append({
                    "title": entry.title,
                    "url":   entry.link,
                    "level": level,
                })
                scores.append(LEVEL_SCORES[level])

        result["score"] = round(min(sum(scores) / max(len(scores), 1), 1.0), 2)

        high_items = [i for i in result["items"] if i["level"] == "high"]
        if high_items:
            result["val"] = high_items[0]["title"][:60]
        elif result["items"]:
            result["val"] = f"{len(result['items'])} значимых новостей"

    except Exception as ex:
        result["status"] = "error"
        result["error"]  = str(ex)

    return result


# ─────────────────────────────────────────
# IMPACT SCORE
# ─────────────────────────────────────────

def calc_impact(city: str, w, e, t, tr, n) -> dict:
    ws    = IMPACT_WEIGHTS
    w_score = coerce_score(w)
    e_score = coerce_score(e)
    t_score = coerce_score(t)
    tr_score = coerce_score(tr)
    n_score = coerce_score(n)
    total = round(
        w_score  * ws["weather"] +
        e_score  * ws["events"]  +
        t_score  * ws["traffic"] +
        tr_score * ws["trends"]  +
        n_score  * ws["news"],
        3
    )
    level = (
        "red"    if total >= ALERT_THRESHOLDS["red"]    else
        "yellow" if total >= ALERT_THRESHOLDS["yellow"] else
        "green"
    )
    return {
        "city":          city,
        "timestamp":     datetime.utcnow().isoformat(),
        "score_total":   total,
        "alert":         level,
        "score_weather": w_score,
        "score_events":  e_score,
        "score_traffic": t_score,
        "score_trends":  tr_score,
        "score_news":    n_score,
    }


# ─────────────────────────────────────────
# TELEGRAM АЛЕРТЫ
# ─────────────────────────────────────────

ALERT_EMOJI = {"green": "🟢", "yellow": "🟡", "red": "🔴"}

def send_telegram(impact: dict, weather: dict, news_items: list):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    # Зелёный — тихий режим, не спамим
    if impact["alert"] == "green":
        return

    emoji     = ALERT_EMOJI[impact["alert"]]
    level_ru  = {"yellow": "ВНИМАНИЕ", "red": "КРИТИЧЕСКИЙ"}[impact["alert"]]
    top_news  = ""
    high_news = [
        i for i in news_items
        if isinstance(i, dict) and i.get("level") == "high"
    ]
    if high_news:
        top_news = f"\n📰 {high_news[0]['title'][:80]}"

    text = (
        f"{emoji} *{impact['city']}* — {level_ru}\n"
        f"Impact Score: *{impact['score_total']:.2f}*\n\n"
        f"🌡 Погода:  {weather.get('val', '?')} · {impact['score_weather']:.2f}\n"
        f"🎭 События: {impact['score_events']:.2f}\n"
        f"🚗 Трафик:  {impact['score_traffic']:.2f}\n"
        f"🔍 Поиск:   {impact['score_trends']:.2f}\n"
        f"📰 Новости: {impact['score_news']:.2f}"
        f"{top_news}"
    )
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"},
            timeout=10
        )
    except Exception as ex:
        print(f"  ⚠ Telegram: {ex}")


# ─────────────────────────────────────────
# JSON STORAGE
# Сохраняем в data/latest.json и
# data/history.json (дашборд читает их)
# ─────────────────────────────────────────

DATA_DIR     = os.path.join(os.path.dirname(__file__), "data")
LATEST_PATH  = os.path.join(DATA_DIR, "latest.json")
HISTORY_PATH = os.path.join(DATA_DIR, "history.json")

def load_history() -> dict:
    if os.path.exists(HISTORY_PATH):
        try:
            with open(HISTORY_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                history = {}
                for city in CITIES:
                    city_history = data.get(city, [])
                    history[city] = city_history if isinstance(city_history, list) else []
                return history
        except (OSError, json.JSONDecodeError, TypeError, ValueError):
            pass
    # Инициализируем пустую историю для каждого города
    return {city: [] for city in CITIES}

def save_latest(snapshot: dict):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(LATEST_PATH, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, ensure_ascii=False, indent=2)

def save_history(history: dict):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(HISTORY_PATH, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)

def append_to_history(history: dict, city: str, score: float):
    """Добавляет точку в историю города. Хранит последние 24 точки (= 24 часа)."""
    if city not in history:
        history[city] = []
    history[city].append({
        "ts":    datetime.utcnow().isoformat(),
        "score": score,
    })
    # Обрезаем до последних 168 точек (7 дней × 24 часа)
    history[city] = history[city][-168:]


# ─────────────────────────────────────────
# ГЛАВНЫЙ ЦИКЛ
# ─────────────────────────────────────────

def run():
    ts_start = datetime.now()
    print(f"\n{'='*52}")
    print(f"Такси Монитор — старт: {ts_start.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"{'='*52}")

    pytrends_client = TrendReq(
        hl="ru", tz=180,
        timeout=(10, 25),
        retries=3,
        backoff_factor=0.5,
    )

    history  = load_history()
    snapshot = {
        "generated_at": datetime.utcnow().isoformat(),
        "cities": {}
    }

    for city, cfg in CITIES.items():
        print(f"\n─── {city} ─────────────────────────")

        print("  [1/5] Погода   ", end="", flush=True)
        w = call_source("weather", fetch_weather, city, cfg)
        print(f"[{w['status']}] {w.get('val', '')} · score={w['score']:.2f}")

        print("  [2/5] События  ", end="", flush=True)
        e = call_source("events", fetch_events, city, cfg)
        print(f"[{e['status']}] {e.get('val', '')} · score={e['score']:.2f}")
        time.sleep(1)

        print("  [3/5] Трафик   ", end="", flush=True)
        t = call_source("traffic", fetch_traffic, city, cfg)
        print(f"[{t['status']}] {t.get('val', '')} · score={t['score']:.2f}")

        time.sleep(3)
        print("  [4/5] Тренды   ", end="", flush=True)
        tr = call_source("trends", fetch_trends, city, cfg, pytrends_client)
        print(f"[{tr['status']}] {tr.get('val', '')} · score={tr['score']:.2f}")
        time.sleep(3)

        print("  [5/5] Новости  ", end="", flush=True)
        n = call_source("news", fetch_news, city, cfg)
        print(f"[{n['status']}] {len(n.get('items', []))} значимых · score={n['score']:.2f}")

        impact = calc_impact(city, w, e, t, tr, n)
        send_telegram(impact, w, n.get("items", []))

        emoji = ALERT_EMOJI[impact["alert"]]
        print(f"  {emoji} Impact Score: {impact['score_total']:.3f} [{impact['alert'].upper()}]")

        append_to_history(history, city, impact["score_total"])

        # Формируем данные для дашборда (формат совместим с CITIES_DATA в HTML)
        snapshot["cities"][city] = {
            "coords":  list(cfg["coords"]),
            "impact":  impact["score_total"],
            "alert":   impact["alert"],
            "weather": {
                "score": coerce_score(w),
                "val":   w.get("val", "нет данных"),
                "icon":  w.get("icon", "🌡"),
                "live":  w["status"] == "ok",
            },
            "events": {
                "score": coerce_score(e),
                "val":   e.get("val", "нет данных"),
                "icon":  "🎭",
            },
            "traffic": {
                "score": coerce_score(t),
                "val":   t.get("val", "нет данных"),
                "icon":  "🚗",
            },
            "trends": {
                "score": coerce_score(tr),
                "val":   tr.get("val", "нет данных"),
                "icon":  "🔍",
            },
            "news": {
                "score": coerce_score(n),
                "val":   n.get("val", "нет данных"),
                "icon":  "📰",
            },
            # История для графика — последние 24 точки
            "history": history_scores(history, city),
        }

    save_latest(snapshot)
    save_history(history)

    elapsed = (datetime.now() - ts_start).seconds
    print(f"\n{'='*52}")
    print(f"Завершено за {elapsed}с")
    print("\nСводка:")
    for city, d in snapshot["cities"].items():
        e = ALERT_EMOJI[d["alert"]]
        print(f"  {e} {city:15} {d['impact']:.3f} [{d['alert']}]")
    print(f"{'='*52}")
    print(f"✓ Сохранено: data/latest.json · data/history.json")


if __name__ == "__main__":
    run()
