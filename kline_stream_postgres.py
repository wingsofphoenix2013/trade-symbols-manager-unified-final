
import os
import time
import json
import threading
import psycopg2
import websocket

# === Подключение к PostgreSQL через переменные окружения ===
PG_HOST = os.environ.get("PG_HOST")
PG_PORT = os.environ.get("PG_PORT", 5432)
PG_NAME = os.environ.get("PG_NAME")
PG_USER = os.environ.get("PG_USER")
PG_PASSWORD = os.environ.get("PG_PASSWORD")

# === Получение списка символов из PostgreSQL ===
def load_symbols():
    try:
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
        return symbols
    except Exception as e:
        print("❌ Ошибка PostgreSQL при загрузке symbols:", e)
        return []

# === Обработка потока 1-минутных свечей с Binance ===
def run_kline_stream():
    print("🚀 KLINE_STREAM_POSTGRES ЗАПУЩЕН")

    def on_message(ws, message):
        try:
            data = json.loads(message)
            kline = data['data']['k']
            if not kline['x']:  # только закрытые свечи
                return

            symbol = data['data']['s'].lower()
            ts = kline['t']  # время открытия свечи (в мс)
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

            print(f"✅ {symbol} [{ts_iso}] {o} / {h} / {l} / {c_}")
        except Exception as e:
            print("❌ Ошибка при обработке свечи:", e)

    def stream_loop():
        while True:
            try:
                symbols = load_symbols()
                if not symbols:
                    print("⚠️ Нет символов для подписки. Ждём...")
                    time.sleep(5)
                    continue
                streams = [f"{s}@kline_1m" for s in symbols]
                url = "wss://fstream.binance.com/stream?streams=" + "/".join(streams)
                print("🔌 URL WebSocket подписки:", url)
                ws = websocket.WebSocketApp(url, on_message=on_message)
                ws.run_forever()
            except Exception as e:
                print("❌ Ошибка WebSocket:", e)
                time.sleep(5)

    threading.Thread(target=stream_loop, daemon=True).start()

if __name__ == "__main__":
    run_kline_stream()
    while True:
        time.sleep(60)
