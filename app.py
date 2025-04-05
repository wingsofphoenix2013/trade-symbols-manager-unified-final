
# app.py
from flask import Flask, render_template, request, jsonify
import sqlite3
import os
import json
import threading
import time
from datetime import datetime, timedelta

app = Flask(__name__)
DB_PATH = "/data/prices.db"
SYMBOLS_FILE = "symbols.json"

def get_symbols():
    if not os.path.exists(SYMBOLS_FILE):
        with open(SYMBOLS_FILE, "w") as f:
            json.dump(["APTUSDT", "ATOMUSDT", "ENAUSDT"], f)
    with open(SYMBOLS_FILE, "r") as f:
        return json.load(f)

def save_symbols(symbols):
    with open(SYMBOLS_FILE, "w") as f:
        json.dump(symbols, f)

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/symbol/<symbol>")
def symbol(symbol):
    return render_template("symbol.html", symbol=symbol.upper())

@app.route("/api/symbols", methods=["GET", "POST"])
def api_symbols():
    if request.method == "GET":
        return jsonify(get_symbols())
    elif request.method == "POST":
        data = request.get_json()
        symbol = data.get("symbol", "").upper()
        if symbol:
            symbols = get_symbols()
            if symbol not in symbols:
                symbols.append(symbol)
                save_symbols(symbols)
        return jsonify({"success": True})

@app.route("/api/symbols/<symbol>", methods=["DELETE"])
def delete_symbol(symbol):
    symbols = get_symbols()
    if symbol.upper() in symbols:
        symbols.remove(symbol.upper())
        save_symbols(symbols)
    return jsonify({"success": True})

@app.route("/api/quotes/<symbol>")
def get_quotes(symbol):
    offset = int(request.args.get("offset", 0))
    limit = 30
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        SELECT timestamp, price,
            (SELECT action FROM signals WHERE signals.symbol = prices.symbol AND signals.timestamp = prices.timestamp LIMIT 1)
        FROM prices
        WHERE symbol = ?
        ORDER BY timestamp DESC
        LIMIT ? OFFSET ?
    """, (symbol.lower(), limit, offset))
    rows = c.fetchall()
    conn.close()
    quotes = []
    for row in rows:
        ts = datetime.fromisoformat(row[0]) + timedelta(hours=3)
        quotes.append({
            "time": ts.strftime("%Y-%m-%d %H:%M"),
            "price": row[1],
            "signal": row[2] or ""
        })
    return jsonify(quotes)

@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json()
    message = data.get("message", "")
    parts = message.strip().split()
    if len(parts) == 2 and parts[0].lower() in ["buy", "sell"]:
        action = parts[0].capitalize()
        symbol = parts[1].upper()
        timestamp = datetime.utcnow().replace(second=0, microsecond=0).isoformat()
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("INSERT INTO signals (symbol, action, timestamp) VALUES (?, ?, ?)", (symbol, action, timestamp))
        conn.commit()
        conn.close()
        return jsonify({"status": "success"}), 200
    return jsonify({"status": "ignored"}), 400

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            timestamp TEXT,
            price REAL
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            action TEXT,
            timestamp TEXT
        )
    """)
    conn.commit()
    conn.close()

def fetch_binance_data():
    import websocket
    import json

    def on_message(ws, message):
        data = json.loads(message)
        if "data" in data:
            item = data["data"]
            symbol = item["s"]
            price = float(item["p"])
            ts = datetime.utcnow().replace(second=0, microsecond=0).isoformat()
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("INSERT INTO prices (symbol, timestamp, price) VALUES (?, ?, ?)", (symbol.lower(), ts, price))
            conn.commit()
            conn.close()

    def run_ws():
        while True:
            try:
                symbols = get_symbols()
                if not symbols:
                    time.sleep(10)
                    continue
                streams = [s.lower() + "@trade" for s in symbols]
                url = "wss://fstream.binance.com/stream?streams=" + "/".join(streams)
                ws = websocket.WebSocketApp(url, on_message=on_message)
                ws.run_forever()
            except Exception as e:
                print("WebSocket error:", e)
                time.sleep(5)

    threading.Thread(target=run_ws, daemon=True).start()

if __name__ == "__main__":
    init_db()
    fetch_binance_data()
    port = int(os.environ.get("PORT", 5000))
    app.run(debug=False, host="0.0.0.0", port=port)
