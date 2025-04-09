
# === МОДУЛЬ 1: Импорты и переменные подключения ===
import os
import time
import json
import threading
import psycopg2
import websocket

# Подключение к PostgreSQL
PG_HOST = os.environ.get("PG_HOST")
PG_PORT = os.environ.get("PG_PORT", 5432)
PG_NAME = os.environ.get("PG_NAME")
PG_USER = os.environ.get("PG_USER")
PG_PASSWORD = os.environ.get("PG_PASSWORD")

# === МОДУЛЬ 2: Загрузка списка символов из таблицы symbols ===
def load_symbols():
    try:
        print("🔎 Загружаем символы...")
        conn = psycopg2.connect(
            dbname=PG_NAME,
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT
        )
        cur = conn.cursor()
        cur.execute("SELECT name FROM symbols")
        symbols = [row[0].lower() for row in cur.fetchall()]
        conn.close()
        print(f"✅ {len(symbols)} символов загружено")
        return symbols
    except Exception as e:
        print("❌ Ошибка при загрузке symbols:", e)
        return []

# === МОДУЛЬ 3: Поток @trade — запись в словарь latest_price ===
latest_price = {}

def run_trade_stream():
    def on_message(ws, msg):
        try:
            data = json.loads(msg)
            trade = data['data']
            symbol = trade['s'].lower()
            price = float(trade['p'])
            latest_price[symbol] = price
            print(f"💰 TRADE: {symbol} = {price}")
        except Exception as e:
            print("❌ Ошибка обработки TRADE:", e)

    def run():
        while True:
            try:
                symbols = load_symbols()
                if not symbols:
                    print("⚠️ Нет символов для подписки на @trade")
                    time.sleep(10)
                    continue
                streams = [f"{s}@trade" for s in symbols]
                url = "wss://fstream.binance.com/stream?streams=" + "/".join(streams)
                print("🔁 Подписка на TRADE:", url)
                ws = websocket.WebSocketApp(url, on_message=on_message)
                ws.run_forever()
            except Exception as e:
                print("❌ Ошибка WebSocket TRADE:", e)
                time.sleep(10)

    threading.Thread(target=run, daemon=True).start()

# === МОДУЛЬ 4: Поток @kline_1m — запись в таблицу prices_pg ===
def run_kline_stream():
    def on_message(ws, message):
        try:
            data = json.loads(message)
            kline = data['data']['k']
            if not kline['x']:
                return
            symbol = data['data']['s'].lower()
            ts = kline['t']
            o = float(kline['o'])
            h = float(kline['h'])
            l = float(kline['l'])
            c_ = float(kline['c'])
            ts_iso = time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(ts / 1000))

            conn = psycopg2.connect(
                dbname=PG_NAME,
                user=PG_USER,
                password=PG_PASSWORD,
                host=PG_HOST,
                port=PG_PORT
            )
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO prices_pg (symbol, timestamp, open, high, low, close) VALUES (%s, %s, %s, %s, %s, %s)",
                (symbol, ts_iso, o, h, l, c_)
            )
            conn.commit()
            conn.close()
            print(f"📦 KLINE: {symbol} {ts_iso} → {o}/{h}/{l}/{c_}")
        except Exception as e:
            print("❌ Ошибка обработки KLINE:", e)

    def run():
        while True:
            try:
                symbols = load_symbols()
                if not symbols:
                    print("⚠️ Нет символов для подписки на @kline_1m")
                    time.sleep(10)
                    continue
                streams = [f"{s}@kline_1m" for s in symbols]
                url = "wss://fstream.binance.com/stream?streams=" + "/".join(streams)
                print("🔁 Подписка на KLINE:", url)
                ws = websocket.WebSocketApp(url, on_message=on_message)
                ws.run_forever()
            except Exception as e:
                print("❌ Ошибка WebSocket KLINE:", e)
                time.sleep(10)

    threading.Thread(target=run, daemon=True).start()

# === МОДУЛЬ 5: Точка входа для запуска обоих потоков ===
if __name__ == "__main__":
    print("🚀 Background Worker: TRADE + KLINE PostgreSQL")
    run_trade_stream()
    run_kline_stream()
    while True:
        time.sleep(60)
