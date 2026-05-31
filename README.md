# Ej3IBD

Plataforma de streaming para datos de mercado basada en Kafka, InfluxDB y Grafana.
El entorno se ejecuta con Docker Compose y está dividido en cuatro servicios:

- `Ingesta`: genera o recibe trades y los publica en Kafka.
- `Procesamiento`: consume los trades, calcula velas OHLC por ventana de 1 minuto y las escribe en InfluxDB.
- `bd`: InfluxDB 2.7 con el bucket `market` ya inicializado.
- `grafana`: dashboard para visualizar las velas y métricas derivadas.

## Flujo de datos

1. `Ingesta` produce eventos con el esquema `{symbol, price, volume, timestamp, source}` y los envía al topic `trades`.
2. `Procesamiento` lee ese topic con Spark Structured Streaming, usa el `timestamp` como tiempo de evento y agrupa por símbolo en ventanas de 1 minuto.
3. Para cada ventana calcula `open`, `high`, `low`, `close`, `volume` y número de `trades`.
4. El resultado se persiste en InfluxDB en la medición `ohlc` dentro del bucket `market`.
5. Grafana consulta InfluxDB para mostrar las series, el último cierre y el volumen negociado.

Si `FINNHUB_TOKEN` no está configurado, `Ingesta` cambia automáticamente a datos sintéticos para que el stack siga funcionando.

## Requisitos

- Docker Engine con Docker Compose.
- Conexión a Internet para descargar imágenes la primera vez.
- Opcional: una cuenta y token de Finnhub si se quiere usar la fuente real de mercado.

## Cómo levantar el entorno

1. Exporta el token si vas a usar Finnhub:

```bash
export FINNHUB_TOKEN="tu_token"
```

2. Arranca todo el stack desde la raíz del proyecto:

```bash
docker compose up --build
```

3. Abre los servicios expuestos:

- Kafka: `kafka:9092` dentro de la red Docker.
- InfluxDB: `http://localhost:8086`
- Grafana: `http://localhost:3000`

Credenciales por defecto:

- InfluxDB: `admin` / `adminadmin123`
- Grafana: `admin` / `adminadmin`

## Verificación rápida

- En Grafana deberías ver el dashboard `Ejercicio3IBD` con velas para `BINANCE:BTCUSDT` y una serie de volumen.
- Si no hay token de Finnhub, el productor imprimirá que está usando datos sintéticos.
- InfluxDB debe contener puntos en el bucket `market` con la medición `ohlc`.

## Estructura del proyecto

- `compose.yml`: orquestación del entorno completo.
- `Ingesta/`: productor de trades hacia Kafka.
- `Procesamiento/`: consumidor y agregador OHLC con Spark.
- `Grafana/`: imagen de Grafana con datasource de InfluxDB preconfigurado y dashboard exportado.

## Detener y limpiar

Para parar los contenedores:

```bash
docker compose down
```

Para borrar también los volúmenes de datos:

```bash
docker compose down -v
```

## Notas

- El bucket de InfluxDB se crea automáticamente en el arranque.
- El procesamiento usa checkpoint en `/tmp/ej3ibd-spark-checkpoint` dentro del contenedor.
- El datasource de Grafana se configura en el build de la imagen, tomando las variables del entorno definidas en `compose.yml`.