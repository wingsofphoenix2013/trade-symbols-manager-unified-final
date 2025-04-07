from flask import Flask, render_template, request, jsonify, redirect
import sqlite3
import os
import json
import sys
import threading
import time
import websocket
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

# Страница конкретного символа
@app.route("/symbol/<symbol>")
def symbol(symbol):
    return render_template("symbol.html", symbol=symbol.upper())

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
    c.execute("CREATE TABLE IF NOT EXISTS symbols (name TEXT PRIMARY KEY)")
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

@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        if request.is_json:
            data = request.get_json()
            message = data.get("message", "")
        else:
            message = request.data.decode("utf-8")

        print("🚨 WEBHOOK СООБЩЕНИЕ:", message)
        sys.stdout.flush()

        parts = message.strip().split()
        if not parts or len(parts) < 2:
            return jsonify({"status": "invalid format"}), 400

        action = parts[0].upper()
        raw_symbol = parts[1].upper()
        symbol = raw_symbol.replace(".P", "")  # удаляем .P

        if action not in ["BUY", "SELL", "BUYZONE", "SELLZONE", "BUYORDER", "SELLORDER"]:
            return jsonify({"status": "unknown action"}), 400

        timestamp = datetime.utcnow().replace(second=0, microsecond=0).isoformat()
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("""CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            action TEXT,
            timestamp TEXT
        )""")
        c.execute("INSERT INTO signals (symbol, action, timestamp) VALUES (?, ?, ?)", (symbol, action, timestamp))
        conn.commit()
        conn.close()

        return jsonify({"status": "success"}), 200

    except Exception as e:
        print("Webhook error:", e)
        sys.stdout.flush()
    return jsonify({"status": "ignored"}), 400
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
        if len(window) == length:
            closes = [w[4] for w in window]
            x = list(range(1, length + 1))
            sumX = sum(x)
            sumY = sum(closes)
            sumXY = sum(closes[j] * x[j] for j in range(length))
            sumX2 = sum(x[j] ** 2 for j in range(length))
            slope = (length * sumXY - sumX * sumY) / (length * sumX2 - sumX ** 2)
            average = sumY / length
            mid_index = (length - 1) / 2
            intercept = average - slope * mid_index
            line = [intercept + slope * (length - j - 1) for j in range(length)]
            stdDev = (sum((closes[j] - line[j]) ** 2 for j in range(length)) / length) ** 0.5
            center = intercept
            lower = round(center - deviation * stdDev, 5)
            center = round(center, 5)
            upper = round(center + deviation * stdDev, 5)
        else:
            lower = center = upper = ""

        group[key] = {
            "open": o, "high": h, "low": l, "close": c_,
            "signal_text": signal_text,
            "signal_type": signal_type,
            "lower": lower, "center": center, "upper": upper
        }

    candles = []
    for k in sorted(group.keys()):
        local_time = k.replace(tzinfo=timezone.utc).astimezone(ZoneInfo("Europe/Kyiv"))
        c = group[k]
        candles.append({
            "time": local_time.strftime("%Y-%m-%d %H:%M"),
            "open": c["open"],
            "high": c["high"],
            "low": c["low"],
            "close": c["close"],
            "signal": c["signal_text"],
            "signal_type": c["signal_type"],
            "channel": f"{c['lower']} / {c['center']} / {c['upper']}" if c["lower"] != "" else ""
        })

    return jsonify(candles or [])

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
# === МОДУЛЬ 7: Отладочная страница для анализа канала ===

@app.route("/debug/<symbol>")
def debug_channel(symbol):
    interval = request.args.get("interval", "5m")
    include_current = request.args.get("include_current", "false").lower() == "true"
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
        return f"<h3>DB error: {e}</h3>"

    from collections import defaultdict

    grouped = defaultdict(list)
    for ts_str, o, h, l, c_ in rows:
        try:
            ts = datetime.fromisoformat(ts_str)
        except:
            continue
        minute = (ts.minute // 5) * 5
        ts_binance = ts.replace(minute=minute, second=0, microsecond=0)
        grouped[ts_binance].append((float(o), float(h), float(l), float(c_)))

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

    if len(candles) < length - (1 if include_current else 0):
        return "<h3>Недостаточно данных</h3>"

    closes = [c[1]["close"] for c in candles[-(length - (1 if include_current else 0)):]]

    if include_current:
        price = latest_price.get(symbol.lower())
        if price:
            closes.append(price)

    if len(closes) != length:
        return "<h3>Не удалось получить текущую цену</h3>"

    x = list(range(1, length + 1))
    sumX = sum(x)
    sumY = sum(closes)
    sumXY = sum(closes[j] * x[j] for j in range(length))
    sumX2 = sum(x[j] ** 2 for j in range(length))
    slope = (length * sumXY - sumX * sumY) / (length * sumX2 - sumX ** 2)
    average = sumY / length
    mid_index = (length - 1) / 2
    intercept = average - slope * mid_index
    line = [intercept + slope * (length - j - 1) for j in range(length)]
    center = sum(line) / length
    stdDev = (sum((closes[j] - line[j]) ** 2 for j in range(length)) / (length - 1)) ** 0.5
    lower = round(center - deviation * stdDev, 5)
    center = round(center, 5)
    upper = round(center + deviation * stdDev, 5)

    rows_html = ""
    recent_candles = candles[-(length - (1 if include_current else 0)):]
    for i in range(len(recent_candles)):
        ts = recent_candles[i][0].astimezone(ZoneInfo("Europe/Kyiv")).strftime("%Y-%m-%d %H:%M")
        o = recent_candles[i][1]["open"]
        h = recent_candles[i][1]["high"]
        l = recent_candles[i][1]["low"]
        c_ = recent_candles[i][1]["close"]
        rows_html += f"<tr><td>{ts}</td><td>{o}</td><td>{h}</td><td>{l}</td><td>{c_}</td><td>{round(line[i],5)}</td></tr>"

    if include_current and len(line) == length:
        rows_html += f"<tr><td><i>Current</i></td><td colspan='4'>Текущая цена: {closes[-1]}</td><td>{round(line[-1],5)}</td></tr>"

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
        <h2>DEBUG: {symbol.upper()} ({interval}){" + текущая цена" if include_current else ""}</h2>
        <div class="box">
            slope = {round(slope, 8)}<br>
            intercept = {round(intercept, 5)}<br>
            stdDev = {round(stdDev, 8)}<br>
            КАНАЛ: <b>{lower} / {center} / {upper}</b>
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
# Запуск сервера + инициализация
if __name__ == "__main__":
    init_db()
    fetch_kline_stream()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
