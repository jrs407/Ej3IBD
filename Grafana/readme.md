# Grafana

Imagen de Grafana preparada para visualizar los datos almacenados en InfluxDB.

## Qué incluye

- Un datasource de InfluxDB creado durante el build de la imagen.
- Variables de entorno para conectar con el bucket `market`.
- El dashboard exportado en `Ejercicio3IBD-1780219888035.json`.

## Qué muestra el dashboard

El dashboard principal está pensado para el símbolo `BINANCE:BTCUSDT` y contiene:

- una vela OHLC con volumen,
- una tarjeta con el último cierre,
- una serie temporal de volumen negociado.

## Variables de entorno

- `INFLUXDB_URL`: URL de InfluxDB, por defecto `http://bd:8086`.
- `INFLUXDB_ORG`: organización de InfluxDB, por defecto `ej3`.
- `INFLUXDB_BUCKET`: bucket consultado, por defecto `market`.
- `INFLUXDB_TOKEN`: token para leer datos desde InfluxDB.

## Ejecución

Grafana arranca dentro de Docker Compose con el puerto `3000` expuesto al host.
La primera vez que se abre, entra con usuario `admin` y contraseña `adminadmin`.

## Nota sobre el dashboard

La imagen deja listo el datasource, pero el JSON del dashboard se conserva como exportación del panel.
Si se necesita importar manualmente, usa el archivo `Ejercicio3IBD-1780219888035.json`.