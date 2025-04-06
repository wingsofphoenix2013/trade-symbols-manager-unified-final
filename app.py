
from flask import Flask, render_template, request, jsonify, redirect
import sqlite3
import os
import json
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

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

    start = intercept + slope * (length - 1)
    end = intercept
    stdDev = sum((closes[i] - (intercept + slope * i)) ** 2 for i in range(length)) ** 0.5 / length

    return jsonify({
        "lower": round(end - deviation * stdDev, 5),
        "center": round(end, 5),
        "upper": round(end + deviation * stdDev, 5)
    })

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
