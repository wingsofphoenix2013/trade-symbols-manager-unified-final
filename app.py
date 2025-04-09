from flask import Flask, render_template, request, jsonify, redirect
import sqlite3
import os
import json
import sys
import threading
import time
import websocket
import math
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

app = Flask(__name__)
DB_PATH = "/data/prices.db"
# === МОДУЛЬ 2: Интерфейсные маршруты и конфигурация канала ===

# Загрузка настроек канала из JSON-файла
def load_channel_config():
    default = {"length": 50, "deviation": 2.0}
    try:
        with open("channel_config.json", "r") as f:
            return json.load(f)
    except:
        return default

# Главная страница — список торговых пар
@app.route("/")
def index():
    return render_template("index.html")

# Страница конкретного символа (старая)
@app.route("/symbol/<symbol>")
def symbol(symbol):
    return render_template("symbol.html", symbol=symbol.upper())

# Страница новой версии просмотра пары (ticker.html)
@app.route("/ticker/<symbol>")
def ticker(symbol):
    return render_template("ticker.html", symbol=symbol.upper())

# Страница настроек канала
@app.route("/channel-settings", methods=["GET", "POST"])
def channel_settings():
    if request.method == "POST":
        length = int(request.form.get("length", 50))
        deviation = float(request.form.get("deviation", 2.0))
        with open("channel_config.json", "w") as f:
            json.dump({"length": length, "deviation": deviation}, f)
        return redirect("/")
    else:
        config = load_channel_config()
        return render_template("channel_settings.html", length=config["length"], deviation=config["deviation"])

# Заглушка для отображения BUYORDER / SELLORDER ссылок
@app.route("/order-info")
def order_info():
    return "<h2 style='text-align:center; font-family:sans-serif;'>ORDER INFO — заглушка</h2>"
# === МОДУЛЬ 3: API — управление символами и очистка ===

# Получение и добавление символов
@app.route("/api/symbols", methods=["GET", "POST"])
def api_symbols():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    if request.method == "GET":
        c.execute("SELECT name FROM symbols ORDER BY name ASC")
        symbols = [row[0].upper() for row in c.fetchall()]
        conn.close()
        return jsonify(symbols)
    elif request.method == "POST":
        data = request.get_json()
        symbol = data.get("symbol", "").upper()
        if not symbol:
            return jsonify({"error": "Symbol is required"}), 400
        c.execute("INSERT OR IGNORE INTO symbols (name) VALUES (?)", (symbol,))
        conn.commit()
        conn.close()
        return jsonify({"success": True})

# Удаление символа и его свечей
@app.route("/api/symbols/<symbol>", methods=["DELETE"])
def delete_symbol(symbol):
    symbol = symbol.upper()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("DELETE FROM symbols WHERE name = ?", (symbol,))
    c.execute("DELETE FROM prices WHERE symbol = ?", (symbol.lower(),))
    conn.commit()
    conn.close()
    return jsonify({"success": True})

# Очистка только свечей для пары
@app.route("/api/clear/<symbol>", methods=["DELETE"])
def clear_prices(symbol):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("DELETE FROM prices WHERE symbol = ?", (symbol.lower(),))
    conn.commit()
    conn.close()
    return jsonify({"success": True})
# === МОДУЛЬ 4: Приём сигналов через webhook ===

from flask import request, jsonify
import psycopg2
from datetime import datetime

# Маршрут обработки POST-запроса от TradingView и других источников
@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        # 1. Извлечение текста из POST-запроса
        if request.is_json:
            data = request.get_json()
            message = data.get("message", "")
        else:
            message = request.data.decode("utf-8")

        print("📩 Получено webhook-сообщение:", message)
        
        # 2. Парсинг строки: ожидается формат "ACTION SYMBOL"
        parts = message.strip().split()
        if len(parts) < 2:
            return jsonify({"status": "invalid format"}), 400

        action = parts[0].upper()
        raw_symbol = parts[1].upper()
        symbol = raw_symbol.replace(".P", "")  # Удаляем возможный суффикс .P

        # 3. Определение типа сигнала
        if action in ["BUY", "SELL", "BUYORDER", "SELLORDER"]:
            signal_type = "action"
        elif action in ["BUYZONE", "SELLZONE"]:
            signal_type = "control"
        else:
            signal_type = "info"

        # 4. Фиксация текущего времени (UTC) — обрезка до минут
        timestamp = datetime.utcnow().replace(second=0, microsecond=0)

        # 5. Запись сигнала в PostgreSQL
        conn = psycopg2.connect(
            dbname=os.environ.get("PG_NAME"),
            user=os.environ.get("PG_USER"),
            password=os.environ.get("PG_PASSWORD"),
            host=os.environ.get("PG_HOST"),
            port=os.environ.get("PG_PORT", 5432)
        )
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO signals (symbol, action, type, timestamp)
            VALUES (%s, %s, %s, %s)
        """, (symbol, action, signal_type, timestamp))
        conn.commit()
        conn.close()

        print(f"✅ Сигнал записан: {symbol} | {action} | {signal_type} | {timestamp}")
        return jsonify({"status": "success"}), 200

    except Exception as e:
        print("❌ Ошибка при обработке webhook:", e)
        return jsonify({"status": "error", "message": str(e)}), 500
# === МОДУЛЬ 5: API свечей + сигнал + расчёт канала на каждую свечу ===

@app.route("/api/candles/<symbol>")
def api_candles(symbol):
    interval = request.args.get("interval", "1m")
    if interval not in ["1m", "5m"]:
        return jsonify([])

    try:
        config = load_channel_config()
        length = config.get("length", 50)
        deviation = config.get("deviation", 2.0)

        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT timestamp, open, high, low, close FROM prices WHERE symbol = ? ORDER BY timestamp ASC", (symbol.lower(),))
        rows = c.fetchall()
        c.execute("SELECT timestamp, action FROM signals WHERE symbol = ?", (symbol.upper(),))
        signal_rows = [(datetime.fromisoformat(row[0]), row[1].upper()) for row in c.fetchall()]
        conn.close()
    except Exception as e:
        print("Ошибка чтения из БД:", e)
        return jsonify([])

    from collections import defaultdict

    prices_map = []
    for ts_str, o, h, l, c_ in rows:
        try:
            ts = datetime.fromisoformat(ts_str)
        except:
            continue
        prices_map.append((ts, float(o), float(h), float(l), float(c_)))

    candles_raw = []
    if interval == "5m":
        grouped = defaultdict(list)
        for ts, o, h, l, c_ in prices_map:
            minute = (ts.minute // 5) * 5
            ts_bin = ts.replace(minute=minute, second=0, microsecond=0)
            grouped[ts_bin].append((o, h, l, c_))
        for ts in sorted(grouped.keys()):
            bucket = grouped[ts]
            if not bucket:
                continue
            o = bucket[0][0]
            h = max(x[1] for x in bucket)
            l = min(x[2] for x in bucket)
            c_ = bucket[-1][3]
            candles_raw.append((ts, o, h, l, c_))
    else:
        for ts, o, h, l, c_ in prices_map:
            ts_clean = ts.replace(second=0, microsecond=0)
            candles_raw.append((ts_clean, o, h, l, c_))

    group_minutes = 5 if interval == "5m" else 1
    group = {}

    for i in range(len(candles_raw)):
        ts, o, h, l, c_ = candles_raw[i]
        key = ts.replace(second=0, microsecond=0)

        orders = []
        zones = []
        for st, act in signal_rows:
            if key <= st < key + timedelta(minutes=group_minutes):
                if "ORDER" in act:
                    orders.append((st, act))
                elif "ZONE" in act:
                    zones.append((st, act))

        signal_text = ""
        signal_type = ""

        if interval == "5m":
            if orders and (not zones or orders[0][0] < zones[0][0]):
                signal_text = orders[0][1] + " (-)"
            elif zones and (not orders or zones[0][0] < orders[0][0]):
                signal_text = orders[0][1] if orders else ""
                signal_type = zones[-1][1]
            elif zones and orders:
                signal_text = orders[0][1] + " (-)"
                signal_type = zones[-1][1]

        # расчёт канала
        window = candles_raw[max(0, i - length + 1): i + 1]
        if not window:
            continue
        avg_x = sum(range(len(window))) / len(window)
        avg_y = sum([row[4] for row in window]) / len(window)
        cov_xy = sum([(i - avg_x) * (row[4] - avg_y) for i, row in enumerate(window)])
        var_x = sum([(i - avg_x) ** 2 for i in range(len(window))])
        slope = cov_xy / var_x if var_x else 0
        intercept = avg_y - slope * avg_x

        expected = [intercept + slope * i for i in range(len(window))]
        std = (sum([(window[i][4] - expected[i]) ** 2 for i in range(len(window))]) / len(window)) ** 0.5
        upper = expected[-1] + deviation * std
        lower = expected[-1] - deviation * std
        mid = expected[-1]

        width_percent = round((upper - lower) / mid * 100, 2) if mid else 0
        angle_rad = math.atan(slope)
        angle_deg = round(math.degrees(angle_rad), 1)

        direction = "вверх" if angle_deg > 2 else "вниз" if angle_deg < -2 else "флет"

        group[key] = {
            "time": key.strftime("%Y-%m-%d %H:%M"),
            "open": o,
            "high": h,
            "low": l,
            "close": c_,
            "signal": signal_text or signal_type,
            "channel": f"{lower:.5f} / {mid:.5f} / {upper:.5f}",
        }

    result = list(reversed(list(group.values())))
    return jsonify(result)

# === МОДУЛЬ 6: Инициализация БД и поток Binance WebSocket ===

# Создание таблиц, если не существуют
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("CREATE TABLE IF NOT EXISTS symbols (name TEXT PRIMARY KEY)")
    c.execute("CREATE TABLE IF NOT EXISTS signals (id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT, action TEXT, timestamp TEXT)")
    c.execute("CREATE TABLE IF NOT EXISTS prices (id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT, timestamp TEXT, open REAL, high REAL, low REAL, close REAL)")
    conn.commit()
    conn.close()

# Поток для получения 1-минутных свечей от Binance
def fetch_kline_stream():
    def on_message(ws, msg):
        try:
            data = json.loads(msg)
            k = data['data']['k']
            if not k['x']:
                return
            symbol = data['data']['s'].lower()
            close = k['c']
            print(f"📦 Получено: {symbol} @ {close}")
            sys.stdout.flush()

            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("INSERT INTO prices (symbol, timestamp, open, high, low, close) VALUES (?, ?, ?, ?, ?, ?)", (
                symbol,
                datetime.utcfromtimestamp(k['t'] // 1000).isoformat(),
                float(k['o']), float(k['h']), float(k['l']), float(k['c'])
            ))
            conn.commit()
            conn.close()
            print(f"✅ Записано: {symbol} {k['t']} {k['c']}")
            sys.stdout.flush()
        except Exception as e:
            print("❌ Ошибка записи свечи:", e)
            sys.stdout.flush()

    def run():
        while True:
            try:
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                c.execute("SELECT name FROM symbols")
                symbols = [row[0].lower() for row in c.fetchall()]
                conn.close()
                if not symbols:
                    print("⚠️ Нет пар для подписки. Ждём...")
                    time.sleep(5)
                    continue
                streams = [f"{s}@kline_1m" for s in symbols]
                url = "wss://fstream.binance.com/stream?streams=" + "/".join(streams)
                print("🔁 Подписка на:", streams)
                sys.stdout.flush()
                ws = websocket.WebSocketApp(url, on_message=on_message)
                ws.run_forever()
            except Exception as e:
                print("❌ Ошибка WebSocket:", e)
                time.sleep(5)

    threading.Thread(target=run, daemon=True).start()
# === МОДУЛЬ 7: Debug — intercept через mid и avgX ===

@app.route("/debug/<symbol>")
def debug_channel(symbol):
    from collections import defaultdict
    from math import sqrt, atan, degrees
    from datetime import datetime
    from zoneinfo import ZoneInfo

    interval = request.args.get("interval", "5m")
    if interval != "5m":
        return "<h3>Поддерживается только interval=5m</h3>"

    try:
        config = load_channel_config()
        length = config.get("length", 50)
        deviation = config.get("deviation", 2.0)

        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT timestamp, open, high, low, close FROM prices WHERE symbol = ? ORDER BY timestamp ASC", (symbol.lower(),))
        rows = c.fetchall()
        conn.close()
    except Exception as e:
        return f"<h3>Ошибка БД: {e}</h3>"

    # 📊 Группировка данных по 5-минутным интервалам
    grouped = defaultdict(list)
    for ts_str, o, h, l, c_ in rows:
        try:
            ts = datetime.fromisoformat(ts_str)
        except:
            continue
        minute = (ts.minute // 5) * 5
        ts_bin = ts.replace(minute=minute, second=0, microsecond=0)
        grouped[ts_bin].append((float(o), float(h), float(l), float(c_)))

    candles = []
    for ts in sorted(grouped.keys()):
        bucket = grouped[ts]
        if not bucket:
            continue
        o = bucket[0][0]
        h = max(x[1] for x in bucket)
        l = min(x[2] for x in bucket)
        c_ = bucket[-1][3]
        candles.append((ts, {"open": o, "high": h, "low": l, "close": c_}))

    if len(candles) < length - 1:
        return "<h3>Недостаточно данных</h3>"

    closes = [c[1]["close"] for c in candles[-(length - 1):]]
    current_price = latest_price.get(symbol.lower())
    if not current_price:
        return "<h3>Нет текущей цены</h3>"
    closes.append(current_price)

    lows = [c[1]["low"] for c in candles[-(length - 1):]]
    highs = [c[1]["high"] for c in candles[-(length - 1):]]

    x = list(range(length))
    avgX = sum(x) / length
    mid = sum(closes) / length

    # 📐 Slope по реальным значениям (для построения канала)
    covXY = sum((x[i] - avgX) * (closes[i] - mid) for i in range(length))
    varX = sum((x[i] - avgX) ** 2 for i in range(length))
    slope = covXY / varX
    intercept = mid - slope * avgX

    # 📊 StdDev и границы канала
    dev = 0.0
    for i in range(length):
        expected = slope * i + intercept
        dev += (closes[i] - expected) ** 2
    stdDev = sqrt(dev / length)

    y_start = intercept
    y_end = intercept + slope * (length - 1)
    center = (y_start + y_end) / 2
    upper = center + deviation * stdDev
    lower = center - deviation * stdDev
    width_percent = round((upper - lower) / center * 100, 2)

    # 🔁 Угол на нормализованных данных
    base = closes[0] if closes[0] != 0 else 1
    norm_closes = [c / base for c in closes]
    norm_mid = sum(norm_closes) / length
    norm_covXY = sum((x[i] - avgX) * (norm_closes[i] - norm_mid) for i in range(length))
    norm_slope = norm_covXY / varX
    angle_deg = round(degrees(atan(norm_slope)), 2)

    # 📋 Подготовка таблицы
    rows_html = ""
    recent_candles = candles[-(length - 1):]
    for i in range(length - 1):
        ts = recent_candles[i][0].astimezone(ZoneInfo("Europe/Kyiv")).strftime("%Y-%m-%d %H:%M")
        o = recent_candles[i][1]["open"]
        h = recent_candles[i][1]["high"]
        l = recent_candles[i][1]["low"]
        c_ = recent_candles[i][1]["close"]
        rows_html += f"<tr><td>{ts}</td><td>{o}</td><td>{h}</td><td>{l}</td><td>{c_}</td><td>—</td></tr>"

    rows_html += f"<tr><td><i>Current (latest_price)</i></td><td colspan='4'>Цена: {current_price}</td><td>—</td></tr>"

    min_close = min(closes)
    max_close = max(closes)
    min_low = min(lows)
    max_high = max(highs)

    # 📤 HTML-вывод
    return f"""
    <html>
    <head>
        <title>Debug: {symbol.upper()}</title>
        <style>
            table {{ font-family: sans-serif; border-collapse: collapse; width: 100%; }}
            th, td {{ border: 1px solid #aaa; padding: 6px; text-align: right; }}
            th {{ background-color: #f0f0f0; }}
            h2 {{ font-family: sans-serif; }}
            .box {{ font-family: monospace; margin-top: 10px; padding: 10px; background: #f8f8f8; border: 1px solid #ccc; }}
        </style>
    </head>
    <body>
        <h2>DEBUG: {symbol.upper()} ({interval}) + current + intercept by mid & avgX</h2>
        <div class="box">
            slope = {round(slope, 8)}<br>
            intercept = {round(intercept, 5)}<br>
            stdDev = {round(stdDev, 8)}<br>
            КАНАЛ: <b>{round(lower,5)} / {round(center,5)} / {round(upper,5)}</b><br>
            <b>Ширина канала:</b> {width_percent}%<br>
            <b>Угол наклона (нормализ.):</b> {angle_deg}&deg;<br><br>
            <b>min(close):</b> {round(min_close,5)}<br>
            <b>max(close):</b> {round(max_close,5)}<br>
            <b>min(low):</b> {round(min_low,5)}<br>
            <b>max(high):</b> {round(max_high,5)}<br>
        </div>
        <br>
        <table>
            <thead><tr><th>Время (Киев)</th><th>Open</th><th>High</th><th>Low</th><th>Close</th><th>reg_line</th></tr></thead>
            <tbody>{rows_html}</tbody>
        </table>
    </body>
    </html>
    """
# === МОДУЛЬ 8: Поток Binance @trade — хранение текущих цен в latest_price ===
latest_price = {}

def fetch_trade_stream():
    def on_message(ws, msg):
        try:
            data = json.loads(msg)
            trade = data['data']
            symbol = trade['s'].lower()
            price = float(trade['p'])
            latest_price[symbol] = price
        except Exception as e:
            print("Ошибка обработки trade-сообщения:", e)

    def run():
        while True:
            try:
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                c.execute("SELECT name FROM symbols")
                symbols = [row[0].lower() for row in c.fetchall()]
                conn.close()
                if not symbols:
                    print("⚠️ Нет пар для подписки на @trade")
                    time.sleep(5)
                    continue
                streams = [f"{s}@trade" for s in symbols]
                url = "wss://fstream.binance.com/stream?streams=" + "/".join(streams)
                print("🔁 Подписка на @trade:", streams)
                sys.stdout.flush()
                ws = websocket.WebSocketApp(url, on_message=on_message)
                ws.run_forever()
            except Exception as e:
                print("❌ Ошибка WebSocket (@trade):", e)
                time.sleep(5)

    threading.Thread(target=run, daemon=True).start()
# === МОДУЛЬ 9: Просмотр текущих цен из latest_price ===

@app.route("/latest-prices")
def latest_prices():
    rows = []
    for symbol, price in sorted(latest_price.items()):
        rows.append(f"<tr><td>{symbol.upper()}</td><td>{price}</td></tr>")
    html = f"""
    <html>
    <head>
        <title>Текущие цены из потока @trade</title>
        <style>
            table {{ font-family: sans-serif; border-collapse: collapse; width: 400px; }}
            th, td {{ border: 1px solid #aaa; padding: 6px; text-align: right; }}
            th {{ background-color: #eee; }}
        </style>
    </head>
    <body>
        <h2>Текущие цены (из latest_price)</h2>
        <table>
            <thead><tr><th>Символ</th><th>Цена</th></tr></thead>
            <tbody>{"".join(rows) if rows else "<tr><td colspan='2'>Нет данных</td></tr>"}</tbody>
        </table>
    </body>
    </html>
    """
    return html
# === МОДУЛЬ 10: API live-channel — расчёт по логике TV (49 свечей + latest_price) ===

@app.route("/api/live-channel/<symbol>")
def api_live_channel(symbol):
    from collections import defaultdict
    from math import sqrt, atan, degrees
    from datetime import datetime, timedelta

    symbol = symbol.lower()
    interval_minutes = 5
    now = datetime.utcnow()
    start_minute = now.minute - now.minute % interval_minutes
    current_start = now.replace(minute=start_minute, second=0, microsecond=0)

    try:
        config = load_channel_config()
        length = config.get("length", 50)
        deviation = config.get("deviation", 2.0)

        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT timestamp, open, high, low, close FROM prices WHERE symbol = ? ORDER BY timestamp ASC", (symbol,))
        rows = c.fetchall()
        c.execute("SELECT timestamp, action FROM signals WHERE symbol = ?", (symbol.upper(),))
        signal_rows = [(datetime.fromisoformat(r[0]), r[1].upper()) for r in c.fetchall()]
        conn.close()
    except Exception as e:
        return jsonify({"error": f"Ошибка БД: {str(e)}"})

    # 📊 Группировка в 5-минутные свечи
    grouped = defaultdict(list)
    for ts_str, o, h, l, c_ in rows:
        try:
            ts = datetime.fromisoformat(ts_str)
        except:
            continue
        minute = (ts.minute // interval_minutes) * interval_minutes
        key = ts.replace(minute=minute, second=0, microsecond=0)
        grouped[key].append((float(o), float(h), float(l), float(c_)))

    # 📈 Построение списка свечей
    candles = []
    for ts in sorted(grouped.keys()):
        bucket = grouped[ts]
        if bucket:
            o = bucket[0][0]
            h = max(x[1] for x in bucket)
            l = min(x[2] for x in bucket)
            c_ = bucket[-1][3]
            candles.append((ts, {"open": o, "high": h, "low": l, "close": c_}))

    if len(candles) < length - 1:
        return jsonify({"error": "Недостаточно данных"})

    # 📉 Текущая цена
    current_price = latest_price.get(symbol)
    if not current_price:
        return jsonify({"error": "Нет текущей цены"})

    # 📊 Реальные цены (для построения канала)
    closes = [c[1]["close"] for c in candles[-(length - 1):]]
    closes.append(current_price)

    x = list(range(length))
    avgX = sum(x) / length
    mid = sum(closes) / length
    covXY = sum((x[i] - avgX) * (closes[i] - mid) for i in range(length))
    varX = sum((x[i] - avgX) ** 2 for i in range(length))
    slope = covXY / varX
    intercept = mid - slope * avgX

    # 📏 Ширина и отклонения
    dev = 0.0
    for i in range(length):
        expected = slope * i + intercept
        dev += (closes[i] - expected) ** 2
    stdDev = sqrt(dev / length)

    y_start = intercept
    y_end = intercept + slope * (length - 1)
    center = (y_start + y_end) / 2
    upper = center + deviation * stdDev
    lower = center - deviation * stdDev
    width_percent = round((upper - lower) / center * 100, 2)

    # 🧠 Вставка нормализованных данных ТОЛЬКО для расчёта угла
    base_price = closes[0] if closes[0] != 0 else 1
    norm_closes = [c / base_price for c in closes]
    norm_mid = sum(norm_closes) / length
    norm_covXY = sum((x[i] - avgX) * (norm_closes[i] - norm_mid) for i in range(length))
    norm_varX = sum((x[i] - avgX) ** 2 for i in range(length))
    norm_slope = norm_covXY / norm_varX
    angle_deg = round(degrees(atan(norm_slope)), 2)

    # 🧭 Направление канала
    if angle_deg > 0.01:
        direction = "восходящий ↗️"
        color = "green"
    elif angle_deg < -0.01:
        direction = "нисходящий ↘️"
        color = "red"
    else:
        direction = "флет ➡️"
        color = "black"

    # 📍 Актуальный сигнал
    signal = ""
    for st, act in signal_rows:
        if current_start <= st < current_start + timedelta(minutes=interval_minutes):
            signal = act
            break

    # 🕓 Локальное время
    local_time = now.replace(tzinfo=timezone.utc).astimezone(ZoneInfo("Europe/Kyiv"))

    return jsonify({
        "time": now.strftime("%Y-%m-%d %H:%M:%S"),
        "local_time": local_time.strftime("%Y-%m-%d %H:%M:%S"),
        "open_price": round(candles[-1][1]["open"], 5),
        "current_price": round(current_price, 5),
        "direction": direction,
        "direction_color": color,
        "angle": angle_deg,
        "width_percent": width_percent,
        "signal": signal
    })
# === МОДУЛЬ 11: Инициализация структуры БД (таблицы) ===

def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")
    c = conn.cursor()

    # Таблица торговых пар
    c.execute("""
        CREATE TABLE IF NOT EXISTS symbols (
            name TEXT PRIMARY KEY
        )
    """)

    # Таблица сигналов (входящие webhook-события)
    c.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            action TEXT,
            timestamp TEXT
        )
    """)

    # Таблица котировок/свечей
    c.execute("""
        CREATE TABLE IF NOT EXISTS prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            timestamp TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL
        )
    """)

    # Таблица сделок
    c.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            side TEXT,
            entry_time TEXT,
            entry_price REAL,
            size REAL,
            leverage REAL,
            exit_time TEXT,
            exit_price REAL,
            pnl REAL,
            status TEXT,
            strategy TEXT,
            snapshot TEXT,
            comment TEXT,
            FOREIGN KEY (symbol) REFERENCES symbols(name)
        )
    """)

    # Таблица выходов по частям
    c.execute("""
        CREATE TABLE IF NOT EXISTS trade_exits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_id INTEGER,
            time TEXT,
            price REAL,
            size REAL,
            pnl REAL,
            reason TEXT CHECK (reason IN (
                'tp-hit', 'sl-hit', 'manual', 'signal', 'expired', 'error'
            )),
            signal_type TEXT,
            snapshot TEXT,
            comment TEXT,
            FOREIGN KEY (trade_id) REFERENCES trades(id)
        )
    """)

    conn.commit()
    conn.close()
# === МОДУЛЬ 12: API + интерфейс просмотра содержимого таблиц БД ===

# HTML-интерфейс для просмотра базы данных
@app.route("/db")
def view_db():
    return render_template("db.html")

# API для загрузки данных из таблицы с фильтрацией и пагинацией
@app.route("/api/db/<table>")
def api_db_table(table):
    # Параметры запроса
    field = request.args.get("field")
    value = request.args.get("value")
    offset = int(request.args.get("offset", 0))
    limit = int(request.args.get("limit", 50))

    allowed_tables = {"symbols", "signals", "prices", "trades", "trade_exits"}
    if table not in allowed_tables:
        return jsonify({"error": "Недопустимая таблица"}), 400

    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()

        # Проверяем наличие колонки id
        c.execute(f"PRAGMA table_info({table})")
        columns = [row[1] for row in c.fetchall()]
        order_column = "id" if "id" in columns else "rowid"

        # Формируем запрос
        query = f"SELECT * FROM {table}"
        params = []

        if field and value:
            query += f" WHERE {field} LIKE ?"
            params.append(f"%{value}%")

        query += f" ORDER BY {order_column} DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        c.execute(query, params)
        rows = [dict(row) for row in c.fetchall()]
        conn.close()

        return jsonify(rows)

    except Exception as e:
        return jsonify({"error": str(e)}), 500
# === МОДУЛЬ 13: Расчёт ATR по свечам candles_5m ===

from flask import request
import psycopg2

@app.route("/api/atr/<symbol>")
def api_atr(symbol):
    try:
        # Чтение параметров запроса
        interval = request.args.get("interval", "5m")
        period = int(request.args.get("period", 14))

        if interval != "5m":
            return jsonify({"error": "Поддерживается только interval=5m"}), 400

        # Подключение к PostgreSQL
        conn = psycopg2.connect(
            dbname=os.environ.get("PG_NAME"),
            user=os.environ.get("PG_USER"),
            password=os.environ.get("PG_PASSWORD"),
            host=os.environ.get("PG_HOST"),
            port=os.environ.get("PG_PORT", 5432)
        )
        cur = conn.cursor()
        cur.execute("""
            SELECT timestamp, high, low, close
            FROM candles_5m
            WHERE symbol = %s
            ORDER BY timestamp DESC
            LIMIT %s
        """, (symbol.upper(), period + 1))
        rows = cur.fetchall()
        conn.close()

        if len(rows) <= period:
            return jsonify({"error": "Недостаточно данных для расчёта"}), 400

        # Обратный порядок: от старых к новым
        rows.reverse()

        # Расчёт True Range (TR)
        tr_list = []
        for i in range(1, len(rows)):
            high = float(rows[i][1])
            low = float(rows[i][2])
            prev_close = float(rows[i-1][3])
            tr = max(
                high - low,
                abs(high - prev_close),
                abs(low - prev_close)
            )
            tr_list.append(tr)

        # Расчёт ATR как SMA (по умолчанию)
        atr = sum(tr_list[-period:]) / period

        return jsonify({
            "symbol": symbol.upper(),
            "interval": interval,
            "period": period,
            "atr": round(atr, 6)
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500
# === МОДУЛЬ 14: API управления торговлей по символу ===

from flask import request

# Получение текущего статуса торговли для символа
@app.route("/api/symbols/<symbol>")
def get_trade_permission(symbol):
    try:
        conn = psycopg2.connect(
            dbname=os.environ.get("PG_NAME"),
            user=os.environ.get("PG_USER"),
            password=os.environ.get("PG_PASSWORD"),
            host=os.environ.get("PG_HOST"),
            port=os.environ.get("PG_PORT", 5432)
        )
        cur = conn.cursor()
        cur.execute("SELECT tradepermission FROM symbols WHERE name = %s", (symbol.upper(),))
        row = cur.fetchone()
        conn.close()

        if row:
            return jsonify({"symbol": symbol.upper(), "tradepermission": row[0]})
        else:
            return jsonify({"error": "Symbol not found"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Переключение статуса торговли (enabled ⇄ disabled)
@app.route("/api/symbols/<symbol>/toggle-trade", methods=["POST"])
def toggle_trade_permission(symbol):
    try:
        conn = psycopg2.connect(
            dbname=os.environ.get("PG_NAME"),
            user=os.environ.get("PG_USER"),
            password=os.environ.get("PG_PASSWORD"),
            host=os.environ.get("PG_HOST"),
            port=os.environ.get("PG_PORT", 5432)
        )
        cur = conn.cursor()
        cur.execute("SELECT tradepermission FROM symbols WHERE name = %s", (symbol.upper(),))
        row = cur.fetchone()

        if not row:
            conn.close()
            return jsonify({"error": "Symbol not found"}), 404

        current = row[0]
        new_status = "disabled" if current == "enabled" else "enabled"

        cur.execute("UPDATE symbols SET tradepermission = %s WHERE name = %s", (new_status, symbol.upper()))
        conn.commit()
        conn.close()

        return jsonify({"symbol": symbol.upper(), "new_status": new_status})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
# Запуск сервера + инициализация
if __name__ == "__main__":
    init_db()
    fetch_kline_stream()
    fetch_trade_stream()  # ← Добавь ЭТУ строку
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
