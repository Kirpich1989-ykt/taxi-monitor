#!/usr/bin/env python3
"""
export_json.py — экспорт последних данных из SQLite в JSON для дашборда.
Запускается после collector.py в GitHub Actions.
Результат: export/latest.json
"""

import sqlite3
import json
import os
from datetime import datetime

DB_PATH     = os.environ.get("DB_PATH", "demand.db")
EXPORT_DIR  = "export"
EXPORT_FILE = os.path.join(EXPORT_DIR, "latest.json")

os.makedirs(EXPORT_DIR, exist_ok=True)

# История для графика — последние 24 записи по городу
HISTORY_POINTS = 24

def get_latest_impact(conn) -> dict:
    """Последний Impact Score по каждому городу."""
    cur = conn.execute("""
        SELECT city, score_total, score_weather, score_events,
               score_traffic, score_trends, score_news,
               alert_level, timestamp
        FROM impact_scores
        WHERE id IN (
            SELECT MAX(id) FROM impact_scores GROUP BY city
        )
    """)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    return {r[0]: dict(zip(cols[1:], r[1:])) for r in rows}


def get_latest_weather(conn) -> dict:
    """Последняя погода по каждому городу."""
    cur = conn.execute("""
        SELECT city, temp, condition, wind_speed, precip_mm, is_bad, score
        FROM weather_snapshots
        WHERE id IN (
            SELECT MAX(id) FROM weather_snapshots GROUP BY city
        )
    """)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    return {r[0]: dict(zip(cols[1:], r[1:])) for r in rows}


def get_latest_events(conn) -> dict:
    """Количество событий и score по каждому городу за последние 6 часов."""
    cur = conn.execute("""
        SELECT city,
               COUNT(*) as total,
               SUM(is_high) as high_count,
               MAX(score) as score
        FROM events_snapshots
        WHERE timestamp >= datetime('now', '-6 hours')
        GROUP BY city
    """)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    return {r[0]: dict(zip(cols[1:], r[1:])) for r in rows}


def get_latest_traffic(conn) -> dict:
    """Последний трафик по городу."""
    cur = conn.execute("""
        SELECT city, jam_score, speed_kmh, status, score
        FROM traffic_snapshots
        WHERE id IN (
            SELECT MAX(id) FROM traffic_snapshots GROUP BY city
        )
    """)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    return {r[0]: dict(zip(cols[1:], r[1:])) for r in rows}


def get_latest_trends(conn) -> dict:
    """Последние тренды по городу — суммарный score."""
    cur = conn.execute("""
        SELECT city, MAX(score) as score,
               GROUP_CONCAT(keyword || ':' || current_val) as details
        FROM trends_snapshots
        WHERE id IN (
            SELECT MAX(id) FROM trends_snapshots GROUP BY city, keyword
        )
        GROUP BY city
    """)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    return {r[0]: dict(zip(cols[1:], r[1:])) for r in rows}


def get_latest_news(conn) -> dict:
    """Последние значимые новости по городу."""
    cur = conn.execute("""
        SELECT city, impact_level, title, score
        FROM news_snapshots
        WHERE timestamp >= datetime('now', '-2 hours')
        ORDER BY city, id DESC
    """)
    rows = cur.fetchall()
    by_city = {}
    for city, level, title, score in rows:
        if city not in by_city:
            by_city[city] = {"items": [], "score": score}
        by_city[city]["items"].append({"level": level, "title": title})
    return by_city


def get_history(conn, city: str) -> list:
    """Последние 24 Impact Score для графика."""
    cur = conn.execute("""
        SELECT score_total FROM impact_scores
        WHERE city = ?
        ORDER BY id DESC
        LIMIT ?
    """, (city, HISTORY_POINTS))
    rows = [r[0] for r in cur.fetchall()]
    rows.reverse()
    # Дополняем нулями если данных меньше 24
    while len(rows) < HISTORY_POINTS:
        rows.insert(0, 0.0)
    return rows


def build_weather_val(w: dict) -> tuple:
    """Строка описания и иконка для дашборда."""
    if not w:
        return "нет данных", "🌡"
    temp = w.get("temp")
    cond = w.get("condition", "")
    sign = "+" if temp and temp >= 0 else ""
    val  = f"{sign}{round(temp)}°C · {cond}" if temp is not None else cond

    # Иконка по тексту condition
    icon_map = {
        "ясно": "☀", "преим. ясно": "🌤", "облачно": "⛅",
        "пасмурно": "☁", "туман": "🌫", "морось": "🌦",
        "дождь": "🌧", "снег": "🌨", "ливень": "🌧",
        "снегопад": "❄", "гроза": "⛈", "крупа": "🌨"
    }
    icon = "🌡"
    for key, ico in icon_map.items():
        if key in cond:
            icon = ico
            break
    return val, icon


def export():
    if not os.path.exists(DB_PATH):
        print(f"✗ БД не найдена: {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)

    impacts  = get_latest_impact(conn)
    weather  = get_latest_weather(conn)
    events   = get_latest_events(conn)
    traffic  = get_latest_traffic(conn)
    trends   = get_latest_trends(conn)
    news     = get_latest_news(conn)

    # Координаты городов — фиксированные
    coords_map = {
        "Якутск":       [62.035, 129.675],
        "Москва":       [55.756,  37.617],
        "Краснодар":    [45.035,  38.975],
        "Новосибирск":  [54.983,  82.896],
    }

    output = {}

    for city, coord in coords_map.items():
        imp = impacts.get(city, {})
        w   = weather.get(city, {})
        e   = events.get(city, {})
        t   = traffic.get(city, {})
        tr  = trends.get(city, {})
        n   = news.get(city, {})

        w_val, w_icon = build_weather_val(w)

        # Строка трафика
        speed = t.get("speed_kmh")
        t_status = t.get("status", "нет данных")
        t_val = f"{round(speed)} км/ч · {t_status}" if speed else t_status

        # Строка трендов
        tr_score = tr.get("score", 0)
        tr_val = f"такси · score {round(tr_score * 100)}%"

        # Строка событий
        e_count = e.get("total", 0)
        e_high  = e.get("high_count", 0)
        e_val   = f"{e_count} событий, {e_high} крупных" if e_count else "нет событий"

        # Строка новостей
        n_items = n.get("items", [])
        n_val   = n_items[0]["title"][:50] if n_items else "без значимых событий"
        n_score = n.get("score", 0)

        output[city] = {
            "coords":  coord,
            "impact":  imp.get("score_total", 0),
            "alert":   imp.get("alert_level", "green"),
            "weather": {
                "score": w.get("score", 0),
                "val":   w_val,
                "icon":  w_icon,
            },
            "events": {
                "score": e.get("score", 0),
                "val":   e_val,
                "icon":  "🎭",
            },
            "traffic": {
                "score": t.get("score", 0),
                "val":   t_val,
                "icon":  "🚗",
            },
            "trends": {
                "score": tr_score,
                "val":   tr_val,
                "icon":  "🔍",
            },
            "news": {
                "score": n_score,
                "val":   n_val,
                "icon":  "📰",
            },
            "history": get_history(conn, city),
        }

    result = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "cities":       output,
    }

    with open(EXPORT_FILE, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    conn.close()
    print(f"✓ Экспортировано: {EXPORT_FILE} ({len(output)} городов)")


if __name__ == "__main__":
    export()
