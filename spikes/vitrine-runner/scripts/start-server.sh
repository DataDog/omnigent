#!/usr/bin/env bash
# Start an isolated, auth-enabled local Omnigent server for the S1 spike.
# All state lives under spikes/vitrine-runner/.run/ (gitignored).
#   HOST=<ip>               bind address (default 127.0.0.1)
#   TUNNEL_ALLOWLIST=<tok>  sets OMNIGENT_RUNNER_TUNNEL_TOKEN (check 8)
set -euo pipefail
SPIKE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
REPO_DIR="$(cd "$SPIKE_DIR/../.." && pwd)"
RUN_DIR="$SPIKE_DIR/.run"
PORT="${PORT:-18766}"
HOST="${HOST:-127.0.0.1}"
mkdir -p "$RUN_DIR/data" "$RUN_DIR/config"
[ -f "$RUN_DIR/cookie_secret" ] || openssl rand -hex 32 > "$RUN_DIR/cookie_secret"
[ -f "$RUN_DIR/admin_password" ] || openssl rand -hex 12 > "$RUN_DIR/admin_password"

export OMNIGENT_AUTH_PROVIDER=accounts
export OMNIGENT_ACCOUNTS_COOKIE_SECRET="$(cat "$RUN_DIR/cookie_secret")"
export OMNIGENT_ACCOUNTS_BASE_URL="http://$HOST:$PORT"
export OMNIGENT_ACCOUNTS_INIT_ADMIN_USERNAME=alice
export OMNIGENT_DATA_DIR="$RUN_DIR/data"
export OMNIGENT_CONFIG_HOME="$RUN_DIR/config"
export OMNIGENT_DATABASE_URI="sqlite:///$RUN_DIR/chat.db"
if [ -n "${TUNNEL_ALLOWLIST:-}" ]; then
  export OMNIGENT_RUNNER_TUNNEL_TOKEN="$TUNNEL_ALLOWLIST"
else
  unset OMNIGENT_RUNNER_TUNNEL_TOKEN
fi
echo "$HOST:$PORT" > "$RUN_DIR/server_addr"
cd "$REPO_DIR"
exec uv run --no-sync omnigent server --host "$HOST" --port "$PORT" \
  --database-uri "sqlite:///$RUN_DIR/chat.db" \
  --admin-password "$(cat "$RUN_DIR/admin_password")" --no-open \
  >> "$RUN_DIR/server.log" 2>&1
