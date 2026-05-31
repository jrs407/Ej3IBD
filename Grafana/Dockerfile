FROM grafana/grafana-oss:11.1.0

USER root

RUN mkdir -p /etc/grafana/provisioning/datasources && \
    cat > /etc/grafana/provisioning/datasources/influxdb.yml <<'YAML'
apiVersion: 1
datasources:
  - name: InfluxDB
    type: influxdb
    access: proxy
    url: ${INFLUXDB_URL}
    isDefault: true
    jsonData:
      version: Flux
      organization: ${INFLUXDB_ORG}
      defaultBucket: ${INFLUXDB_BUCKET}
      httpMode: POST
    secureJsonData:
      token: ${INFLUXDB_TOKEN}
YAML

USER grafana