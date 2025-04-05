
from flask import Flask, render_template, request, jsonify
import sqlite3
import os
import threading
import time
import json
from datetime import datetime, timedelta

app = Flask(__name__)
DB_PATH = "/data/prices.db"

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/symbol/<symbol>")
def symbol(symbol):
    return render_template("symbol.html", symbol=symbol.upper())

@app.route("/api/symbols", methods=["GET", "POST"])
def symbols_api():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    if request.method == "GET":
        c.execute("SELECT name FROM symbols ORDER BY name")
        data = [row[0].upper() for row in c.fetchall()]
        conn.close()
        return jsonify(data)
    elif request.method == "POST":
        symbol = request.json.get("symbol", "").upper()
        if symbol:
            c.execute("INSERT OR IGNORE INTO symbols (name) VALUES (?)", (symbol,))
            conn.commit()
        conn.close()
        return jsonify({"success": True})

@app.route("/api/symbols/<symbol>", methods=["DELETE"])
def delete_symbol(symbol):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("DELETE FROM symbols WHERE name = ?", (symbol.upper(),))
    conn.commit()
    conn.close()
    return jsonify({"success": True})

@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        if request.is_json:
            data = request.get_json()
            message = data.get("message", "")
        else:
            message = request.data.decode("utf-8")

        print("🚨 WEBHOOK СООБЩЕНИЕ:", message)

        parts = message.strip().split()
        if len(parts) == 2 and parts[0].lower() in ["buy", "sell"]:
            action = parts[0].capitalize()
            symbol = parts[1].upper()
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
            print(f"✅ Принят сигнал: {action} {symbol} @ {timestamp}")
            return jsonify({"status": "success"}), 200
    except Exception as e:
        print("Webhook error:", e)
    return jsonify({"status": "ignored"}), 400

@app.route("/api/candles/<symbol>")
def api_candles(symbol):
    interval = request.args.get("interval", "1m")
    if interval not in ["1m", "5m"]:
        return jsonify([])

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT timestamp, open, high, low, close FROM prices WHERE symbol = ? ORDER BY timestamp ASC", (symbol.lower(),))
    rows = c.fetchall()
    conn.close()

    group = {}
    for ts_str, o, h, l, c_ in rows:
        ts = datetime.fromisoformat(ts_str)
        minute = ts.minute - ts.minute % (5 if interval == "5m" else 1)
        key = ts.replace(minute=minute, second=0, microsecond=0)
        group[key] = {"open": o, "high": h, "low": l, "close": c_}

    candles = []
    for k in sorted(group.keys()):
        c = group[k]
        candles.append({
            "time": (k + timedelta(hours=3)).strftime("%Y-%m-%d %H:%M"),
            "open": c["open"],
            "high": c["high"],
            "low": c["low"],
            "close": c["close"]
        })
    return jsonify(candles)

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("CREATE TABLE IF NOT EXISTS symbols (name TEXT PRIMARY KEY)")
    c.execute("CREATE TABLE IF NOT EXISTS prices (id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT, timestamp TEXT, open REAL, high REAL, low REAL, close REAL)")
    conn.commit()
    conn.close()

def fetch_kline_stream():
    import websocket

    def on_message(ws, msg):
        try:
            data = json.loads(msg)
            k = data['data']['k']
            if not k['x']:
                return
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("INSERT INTO prices (symbol, timestamp, open, high, low, close) VALUES (?, ?, ?, ?, ?, ?)",
                      (data['data']['s'].lower(), datetime.utcfromtimestamp(k['t'] // 1000).isoformat(),
                       float(k['o']), float(k['h']), float(k['l']), float(k['c'])))
            conn.commit()
            conn.close()
        except Exception as e:
            print("Ошибка записи свечи:", e)

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

if __name__ == "__main__":
    init_db()
    fetch_kline_stream()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
