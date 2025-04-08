
import websocket
import sqlite3
import json
import time
import threading

DB_PATH = "prices.db"
new_latest_price = {}

def load_symbols():
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT name FROM symbols")
        symbols = [row[0].lower() for row in c.fetchall()]
        conn.close()
        return symbols
    except Exception as e:
        print("Ошибка при загрузке символов:", e)
        return []

def run_trade_stream():
    print('🚀 trade_stream запущен')
    def on_message(ws, message):
        try:
            print("📨 RAW MESSAGE:", message[:100])  # покажем начало
            data = json.loads(message)
            symbol = data['data']['s'].lower()
            price = float(data['data']['p'])
            new_latest_price[symbol] = price
            print(f"[{symbol}] → {price}")
        except Exception as e:
            print("Ошибка при обработке сообщения:", e)

    def stream_loop():
        print('🔁 stream_loop стартовал')
        while True:
            try:
                symbols = load_symbols()
                print("✅ Список символов:", symbols)
                if not symbols:
                    print("⚠️ Нет символов для подписки. Ждём...")
                    time.sleep(10)
                    continue
                streams = [f"{s}@trade" for s in symbols]
                url = "wss://fstream.binance.com/stream?streams=" + "/".join(streams)
                print("🔌 URL подписки:", url)
                ws = websocket.WebSocketApp(url, on_message=on_message)
                ws.run_forever()
            except Exception as e:
                print("❌ Ошибка WebSocket:", e)
                time.sleep(5)

    threading.Thread(target=stream_loop, daemon=True).start()

if __name__ == "__main__":
    run_trade_stream()
    while True:
        time.sleep(60)
