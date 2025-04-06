from flask import Flask, render_template, request, jsonify, redirect
import sqlite3
import os
import json
import sys
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import threading
import time
import websocket
import traceback

app = Flask(__name__)
DB_PATH = "/data/prices.db"

# === CONFIG ===
def load_channel_config():
    default = {"length": 50, "deviation": 2.0}
    try:
        with open("channel_config.json", "r") as f:
            return json.load(f)
    except:
        return default

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/symbol/<symbol>")
def symbol(symbol):
    return render_template("symbol.html", symbol=symbol.upper())

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

@app.route("/order-info")
def order_info():
    return "<h2 style='text-align:center; font-family:sans-serif;'>ORDER INFO — заглушка</h2>"

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

@app.route("/api/clear/<symbol>", methods=["DELETE"])
def clear_prices(symbol):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("DELETE FROM prices WHERE symbol = ?", (symbol.lower(),))
    conn.commit()
    conn.close()
    return jsonify({"success": True})

@app.route("/api/linreg/<symbol>")
def api_linreg(symbol):
    config = load_channel_config()
    length = config.get("length", 50)
    deviation = config.get("deviation", 2.0)

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT timestamp, close FROM prices WHERE symbol = ? ORDER BY timestamp DESC LIMIT ?", (symbol.lower(), length))
    rows = c.fetchall()
    conn.close()

    if len(rows) < length:
        return jsonify({"error": "Недостаточно данных"}), 400

    closes = [row[1] for row in reversed(rows)]
    sumX = sum(i + 1 for i in range(length))
    sumY = sum(closes)
    sumXY = sum(closes[i] * (i + 1) for i in range(length))
    sumX2 = sum((i + 1) ** 2 for i in range(length))

    slope = (length * sumXY - sumX * sumY) / (length * sumX2 - sumX ** 2)
    average = sumY / length
    intercept = average - slope * sumX / length + slope
    end = intercept
    stdDev = sum((closes[i] - (intercept + slope * i)) ** 2 for i in range(length)) ** 0.5 / length

    return jsonify({
        "lower": round(end - deviation * stdDev, 5),
        "center": round(end, 5),
        "upper": round(end + deviation * stdDev, 5)
    })

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
        symbol = raw_symbol.replace(".P", "")

        if action not in ["BUY", "SELL", "BUYZONE", "SELLZONE", "BUYORDER", "SELLORDER"]:
            return jsonify({"status": "unknown action"}), 400

        timestamp = datetime.utcnow().replace(second=0, microsecond=0).isoformat()
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT,
                action TEXT,
                timestamp TEXT
            )
        """)
        c.execute("INSERT INTO signals (symbol, action, timestamp) VALUES (?, ?, ?)", (symbol, action, timestamp))
        conn.commit()
        conn.close()

        return jsonify({"status": "success"}), 200

    except Exception as e:
        print("Webhook error:", e)
        traceback.print_exc()
        sys.stdout.flush()
    return jsonify({"status": "ignored"}), 400

@app.route("/api/candles/<symbol>")
def api_candles(symbol):
    interval = request.args.get("interval", "1m")
    if interval not in ["1m", "5m"]:
        return jsonify([])

    try:
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

    for ts_str, o, h, l, c_ in rows:
        try:
            ts = datetime.fromisoformat(ts_str)
        except Exception:
            continue
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

        group[key] = {
            "open": o,
            "high": h,
            "low": l,
            "close": c_,
            "signal_text": signal_text,
            "signal_type": signal_type
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
            "signal_type": c["signal_type"]
        })

    return jsonify(candles or [])

# === BINANCE STREAM ===
def fetch_kline_stream():
    def on_message(ws, msg):
        try:
            data = json.loads(msg)
            k = data['data']['k']
            if not k['x']:
                return
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
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
            c.execute("INSERT INTO prices (symbol, timestamp, open, high, low, close) VALUES (?, ?, ?, ?, ?, ?)",
                      (data['data']['s'].lower(), datetime.utcfromtimestamp(k['t'] // 1000).isoformat(),
                       float(k['o']), float(k['h']), float(k['l']), float(k['c'])))
            conn.commit()
            conn.close()
        except Exception as e:
            print("Ошибка записи свечи:", e)
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
                    time.sleep(5)
                    continue
                streams = [f"{s}@kline_1m" for s in symbols]
                url = "wss://fstream.binance.com/stream?streams=" + "/".join(streams)
                ws = websocket.WebSocketApp(url, on_message=on_message)
                ws.run_forever()
            except Exception as e:
                print("Ошибка WebSocket:", e)
                time.sleep(5)

    threading.Thread(target=run, daemon=True).start()

# === INIT DB ===
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("CREATE TABLE IF NOT EXISTS symbols (name TEXT PRIMARY KEY)")
    c.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            action TEXT,
            timestamp TEXT
        )
    """)
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
    conn.commit()
    conn.close()

if __name__ == "__main__":
    init_db()
    fetch_kline_stream()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
