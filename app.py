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

    group_minutes = 5 if interval == "5m" else 1
    group = {}
    prices_map = []

    for ts_str, o, h, l, c_ in rows:
        try:
            ts = datetime.fromisoformat(ts_str)
        except Exception:
            continue
        prices_map.append((ts, o, h, l, c_))

    for i in range(len(prices_map)):
        ts, o, h, l, c_ = prices_map[i]
        minute = ts.minute - ts.minute % group_minutes
        key = ts.replace(minute=minute, second=0, microsecond=0)

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

        # === Расчёт канала (точно как в TradingView) ===
        window = prices_map[max(0, i - length + 1): i + 1]
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
            center = intercept
            line = [intercept + slope * (length - j - 1) for j in range(length)]
            stdDev = (sum((closes[j] - line[j]) ** 2 for j in range(length)) / length) ** 0.5
            lower = round(center - deviation * stdDev, 5)
            center = round(center, 5)
            upper = round(center + deviation * stdDev, 5)
        else:
            lower = center = upper = ""

        group.setdefault(key, {
            "open": o, "high": h, "low": l, "close": c_,
            "signal_text": signal_text,
            "signal_type": signal_type,
            "lower": lower, "center": center, "upper": upper
        })

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

# Запуск сервера + инициализация
if __name__ == "__main__":
    init_db()
    fetch_kline_stream()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
