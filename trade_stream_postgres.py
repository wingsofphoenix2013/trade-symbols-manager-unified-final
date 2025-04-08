import psycopg2
import websocket
import json
import time
import threading

# Настройки подключения к PostgreSQL
import os
PG_HOST = os.environ.get("PG_HOST")
PG_PORT = os.environ.get("PG_PORT", 5432)
PG_NAME = os.environ.get("PG_NAME")
PG_USER = os.environ.get("PG_USER")
PG_PASSWORD = os.environ.get("PG_PASSWORD")

# Хранилище актуальных котировок
new_latest_price = {}

# Чтение списка символов из PostgreSQL
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
        print("Ошибка PostgreSQL при загрузке символов:", repr(e), flush=True)
        return []

def run_trade_stream():
    print("🚀 PostgreSQL trade_stream запущен", flush=True)

    def on_message(ws, message):
        try:
            data = json.loads(message)
            symbol = data['data']['s'].lower()
            price = float(data['data']['p'])
            new_latest_price[symbol] = price
            print(f"[{symbol}] → {price}", flush=True)
        except Exception as e:
            print("Ошибка при обработке сообщения:", repr(e), flush=True)

    def stream_loop():
        print("🔁 stream_loop стартовал", flush=True)
        while True:
            try:
                symbols = load_symbols()
                print("✅ Список символов:", symbols, flush=True)
                if not symbols:
                    print("⚠️ Нет символов для подписки. Ждём...", flush=True)
                    time.sleep(10)
                    continue
                streams = [f"{s}@trade" for s in symbols]
                url = "wss://fstream.binance.com/stream?streams=" + "/".join(streams)
                print("🔌 URL подписки:", url, flush=True)
                ws = websocket.WebSocketApp(url, on_message=on_message)
                ws.run_forever()
            except Exception as e:
                print("❌ Ошибка WebSocket:", repr(e), flush=True)
                time.sleep(5)

    threading.Thread(target=stream_loop, daemon=True).start()

if __name__ == "__main__":
    run_trade_stream()
    while True:
        time.sleep(60)