#!/bin/sh
# 把已签名的 grafana-clickhouse-datasource 放到 grafana/plugins，compose 直接挂载。
set -eu
ROOT=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
DEST="$ROOT/grafana/plugins/grafana-clickhouse-datasource"
if [ -f "$DEST/plugin.json" ] && [ -f "$DEST/gpx_clickhouse_linux_amd64" ]; then
  exit 0
fi
mkdir -p "$ROOT/grafana/plugins"
tmp=$(mktemp)
curl -fsSL --retry 3 --retry-delay 2 \
  -o "$tmp" \
  "https://grafana.com/api/plugins/grafana-clickhouse-datasource/versions/latest/download"
unzip -qo "$tmp" -d "$ROOT/grafana/plugins"
rm -f "$tmp"
chmod -R a+rX "$ROOT/grafana/plugins"
