
# === МОДУЛЬ IMPORT ===
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

# === МОДУЛЬ 1: Загрузка списка символов из таблицы symbols ===
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

# === МОДУЛЬ 2: Поток @trade — запись в словарь latest_price ===
latest_price = {}

def run_trade_stream():
    def on_message(ws, msg):
        try:
            data = json.loads(msg)
            trade = data['data']
            symbol = trade['s'].lower()
            price = float(trade['p'])
            latest_price[symbol] = price
            
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

# === МОДУЛЬ 3: Поток @kline_1m — запись в таблицу prices_pg ===
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


# === МОДУЛЬ 4: Формирование и сохранение 5-минутных свечей (candles_5m) ===

from datetime import datetime, timedelta

# Временный буфер по 5 свечей на каждый символ
buffers_5m = {}

# Агрегация и сохранение одной 5m-свечи
def aggregate_and_save_5m(symbol, buffer, conn_params):
    try:
        if len(buffer) < 5:
            return  # Недостаточно данных

        opens = [row['open'] for row in buffer]
        highs = [row['high'] for row in buffer]
        lows = [row['low'] for row in buffer]
        closes = [row['close'] for row in buffer]
        timestamp = buffer[0]['timestamp']  # время первой свечи (начало интервала)

        o = opens[0]
        h = max(highs)
        l = min(lows)
        c = closes[-1]

        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO candles_5m (symbol, timestamp, open, high, low, close) VALUES (%s, %s, %s, %s, %s, %s)",
            (symbol, timestamp, o, h, l, c)
        )
        conn.commit()
        conn.close()
        print(f"🕔 [candles_5m] {symbol} | {timestamp} | {o}-{h}-{l}-{c}", flush=True)
    except Exception as e:
        print(f"❌ Ошибка при сохранении свечи 5m: {e}", flush=True)

# Добавление новой M1-свечи в буфер и проверка на завершение пятиминутки
def process_kline_for_5m(symbol, kline, conn_params):
    ts = datetime.fromtimestamp(kline['timestamp'] / 1000).replace(second=0, microsecond=0)
    ts_5m = ts.replace(minute=(ts.minute // 5) * 5)

    candle = {
        'timestamp': ts_5m,
        'open': float(kline['open']),
        'high': float(kline['high']),
        'low': float(kline['low']),
        'close': float(kline['close'])
    }

    if symbol not in buffers_5m:
        buffers_5m[symbol] = []

    buf = buffers_5m[symbol]

    # Если новая свеча в пределах текущего интервала — добавляем
    if not buf or buf[-1]['timestamp'] == candle['timestamp']:
        buf.append(candle)
    else:
        # Интервал сменился → агрегируем старые, сохраняем, сбрасываем
        aggregate_and_save_5m(symbol, buf, conn_params)
        buffers_5m[symbol] = [candle]

# === МОДУЛЬ ENTRYPOINT ===
if __name__ == "__main__":
    print("🚀 Background Worker: TRADE + KLINE PostgreSQL")
    run_trade_stream()
    run_kline_stream()
    while True:
        time.sleep(60)
