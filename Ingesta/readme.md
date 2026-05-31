# Ingesta

Servicio encargado de generar y publicar trades en Kafka.

## Qué hace

- Se conecta al broker definido en `KAFKA_BOOTSTRAP_SERVERS`.
- Publica mensajes en el topic `KAFKA_TOPIC` con clave `symbol`.
- Intenta suscribirse al websocket de Finnhub cuando existe `FINNHUB_TOKEN`.
- Si no hay token o la conexión falla, genera una secuencia sintética de precios para mantener vivo el pipeline.

## Esquema del evento

Cada mensaje enviado a Kafka sigue esta estructura:

```json
{
	"symbol": "BINANCE:BTCUSDT",
	"price": 30000.12,
	"volume": 4.25,
	"timestamp": 1710000000000,
	"source": "finnhub"
}
```

## Variables de entorno

- `KAFKA_BOOTSTRAP_SERVERS`: brokers de Kafka, por defecto `kafka:9092`.
- `KAFKA_TOPIC`: topic de salida, por defecto `trades`.
- `SYMBOLS`: lista separada por comas, por defecto `BINANCE:BTCUSDT,AAPL,INTC`.
- `FINNHUB_TOKEN`: token opcional para usar datos reales.

## Ejecución

Normalmente se ejecuta como parte del stack completo con `docker compose up --build` desde la raíz del proyecto.
Si se lanza solo, necesita acceso a Kafka y, opcionalmente, salida a Internet para Finnhub.

## Comportamiento de fallback

Cuando no se define `FINNHUB_TOKEN`, el productor:

- crea precios base por símbolo,
- aplica variaciones pequeñas cada segundo,
- publica un trade por símbolo en cada ciclo.

Eso permite probar Procesamiento y Grafana sin dependencias externas.