import json
import os
import random
import signal
import threading
import time

from kafka import KafkaProducer

try:
    import websocket
except Exception:
    websocket = None


BOOTSTRAP_SERVERS = [server.strip() for server in os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092").split(",") if server.strip()]
TOPIC = os.getenv("KAFKA_TOPIC", "trades")
SYMBOLS = [symbol.strip() for symbol in os.getenv("SYMBOLS", "BINANCE:BTCUSDT,AAPL,INTC").split(",") if symbol.strip()]
FINNHUB_TOKEN = os.getenv("FINNHUB_TOKEN", "").strip()
STOP_EVENT = threading.Event()


def utc_now_ms() -> int:
    return int(time.time() * 1000)


def build_producer() -> KafkaProducer:
    while not STOP_EVENT.is_set():
        try:
            return KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda payload: json.dumps(payload).encode("utf-8"),
                key_serializer=lambda value: value.encode("utf-8") if value else None,
                retries=10,
                linger_ms=5,
                acks="all",
            )
        except Exception as exc:
            print(f"[ingesta] Esperando Kafka: {exc}", flush=True)
            time.sleep(5)
    raise SystemExit(0)


def publish(producer: KafkaProducer, payload: dict) -> None:
    producer.send(TOPIC, key=payload["symbol"], value=payload)
    producer.flush(timeout=2)
    print(f"[ingesta] {payload['symbol']} price={payload['price']:.4f} volume={payload['volume']:.4f}", flush=True)


def synthetic_stream(producer: KafkaProducer) -> None:
    prices = {
        symbol: (30000.0 if "BTC" in symbol else 180.0 if symbol == "AAPL" else 40.0)
        for symbol in SYMBOLS
    }
    while not STOP_EVENT.is_set():
        for symbol in SYMBOLS:
            base_price = prices[symbol]
            next_price = max(0.01, base_price + random.uniform(-1.5, 1.5))
            prices[symbol] = next_price
            payload = {
                "symbol": symbol,
                "price": round(next_price, 6),
                "volume": round(random.uniform(1, 25), 6),
                "timestamp": utc_now_ms(),
                "source": "synthetic",
            }
            publish(producer, payload)
        time.sleep(1)


def run_finnhub(producer: KafkaProducer) -> None:
    if websocket is None:
        raise RuntimeError("websocket-client no está disponible")

    url = f"wss://ws.finnhub.io?token={FINNHUB_TOKEN}"

    def on_open(ws):
        for symbol in SYMBOLS:
            ws.send(json.dumps({"type": "subscribe", "symbol": symbol}))
        print(f"[ingesta] Suscrito a {', '.join(SYMBOLS)}", flush=True)

    def on_message(_ws, message: str):
        try:
            payload = json.loads(message)
        except json.JSONDecodeError:
            return
        for item in payload.get("data", []):
            trade = {
                "symbol": item.get("s"),
                "price": float(item.get("p", 0.0)),
                "volume": float(item.get("v", 0.0)),
                "timestamp": int(item.get("t", utc_now_ms())),
                "source": "finnhub",
            }
            if trade["symbol"]:
                publish(producer, trade)

    def on_error(_ws, error):
        print(f"[ingesta] Error websocket: {error}", flush=True)

    def on_close(_ws, status_code, message):
        print(f"[ingesta] WebSocket cerrado ({status_code}): {message}", flush=True)

    while not STOP_EVENT.is_set():
        try:
            ws = websocket.WebSocketApp(url, on_open=on_open, on_message=on_message, on_error=on_error, on_close=on_close)
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as exc:
            print(f"[ingesta] Conexión Finnhub fallida, usando fallback sintético: {exc}", flush=True)
            synthetic_stream(producer)
            return
        if STOP_EVENT.is_set():
            break
        print("[ingesta] Reconectando a Finnhub en 5 segundos", flush=True)
        time.sleep(5)


def handle_stop(_signum, _frame):
    STOP_EVENT.set()


def main() -> None:
    signal.signal(signal.SIGTERM, handle_stop)
    signal.signal(signal.SIGINT, handle_stop)

    producer = build_producer()

    if FINNHUB_TOKEN:
        run_finnhub(producer)
    else:
        print("[ingesta] FINNHUB_TOKEN no definido, usando datos sintéticos", flush=True)
        synthetic_stream(producer)


if __name__ == "__main__":
    main()