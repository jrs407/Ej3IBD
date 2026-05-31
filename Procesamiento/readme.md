# Procesamiento

Servicio de streaming que consume trades desde Kafka, calcula velas OHLC y las persiste en InfluxDB.

## Qué hace

- Lee el topic `trades` con Spark Structured Streaming.
- Convierte el JSON recibido al esquema de trade.
- Usa `timestamp` como tiempo de evento y aplica watermark de 2 minutos.
- Agrupa por símbolo en ventanas de 1 minuto.
- Calcula `open`, `high`, `low`, `close`, `volume` y cantidad de trades.
- Escribe cada batch en InfluxDB en la medición `ohlc`.

## Entrada esperada

El consumidor espera el mismo esquema que genera Ingesta:

```json
{
	"symbol": "AAPL",
	"price": 180.45,
	"volume": 10.2,
	"timestamp": 1710000000000,
	"source": "synthetic"
}
```

## Salida en InfluxDB

Cada punto se guarda con:

- medición: `ohlc`
- etiqueta: `symbol`
- campos: `open`, `high`, `low`, `close`, `volume`, `trades`
- tiempo: cierre de la ventana

## Variables de entorno

- `KAFKA_BOOTSTRAP_SERVERS`: brokers de Kafka, por defecto `kafka:9092`.
- `KAFKA_TOPIC`: topic de entrada, por defecto `trades`.
- `INFLUXDB_URL`: URL de InfluxDB, por defecto `http://bd:8086`.
- `INFLUXDB_ORG`: organización, por defecto `ej3`.
- `INFLUXDB_BUCKET`: bucket destino, por defecto `market`.
- `INFLUXDB_TOKEN`: token de escritura, por defecto `ej3-influxdb-token`.

## Ejecución

Se ejecuta automáticamente con el stack completo. El contenedor instala Java y PySpark, luego arranca el stream y mantiene un checkpoint en `/tmp/ej3ibd-spark-checkpoint`.

## Validación útil

Si necesitas comprobar que el archivo sigue siendo válido Python, puedes ejecutar:

```bash
python3 -m py_compile processor.py
```