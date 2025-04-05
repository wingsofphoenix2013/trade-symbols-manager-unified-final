
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

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/symbol/<symbol>")
def symbol(symbol):
    return render_template("symbol.html", symbol=symbol.upper())

@app.route("/api/candles/<symbol>")
def api_candles(symbol):
    interval = request.args.get("interval", "1m")
    if interval not in ["1m", "5m"]:
        return jsonify([])

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT timestamp, price FROM prices WHERE symbol = ? ORDER BY timestamp ASC", (symbol.lower(),))
    rows = c.fetchall()
    conn.close()

    candles = []
    group = {}
    for ts_str, price in rows:
        ts = datetime.fromisoformat(ts_str)
        minute = ts.minute - ts.minute % (5 if interval == "5m" else 1)
        key = ts.replace(minute=minute, second=0, microsecond=0)

        if key not in group:
            group[key] = {"open": price, "high": price, "low": price, "close": price}
        else:
            group[key]["high"] = max(group[key]["high"], price)
            group[key]["low"] = min(group[key]["low"], price)
            group[key]["close"] = price

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

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(debug=False, host="0.0.0.0", port=port)
