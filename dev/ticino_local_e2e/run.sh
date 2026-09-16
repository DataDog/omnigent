#!/usr/bin/env bash
# Opt-in local live-token harness. See README.md in this directory.
set -euo pipefail

root_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
harness_dir="$root_dir/dev/ticino_local_e2e"
state_dir=${TICINO_E2E_STATE_DIR:-/tmp/omnigent-ticino-token-handoff-e2e}
runtime_env="$state_dir/runtime.env"
compose_file="$harness_dir/docker-compose.postgres.yml"
port=${TICINO_E2E_PORT:-6767}
exchange_port=${TICINO_E2E_EXCHANGE_PORT:-6768}
habitat_port=${TICINO_E2E_HABITAT_PORT:-6769}
postgres_port=${TICINO_E2E_POSTGRES_PORT:-6770}
launcher_module=${HAB_LAUNCHER_MODULE:-hab_launcher}
habitat_mode=${TICINO_E2E_HABITAT_MODE:-fake}
venv="$state_dir/venv"

die() {
  printf '%s\n' "error: $*" >&2
  exit 1
}

require_tool() {
  command -v "$1" >/dev/null 2>&1 || die "missing required tool: $1"
}

validate_state_dir() {
  # Restrict cleanup to a dedicated /tmp prefix so a typo cannot turn `down`
  # into a broad deletion.
  case "$state_dir" in
    /tmp/omnigent-ticino-token-handoff-e2e|/tmp/omnigent-ticino-token-handoff-e2e-*) ;;
    *) die "TICINO_E2E_STATE_DIR must use /tmp/omnigent-ticino-token-handoff-e2e[-<suffix>]" ;;
  esac
  [[ "/$state_dir/" != *"/../"* ]] || die "TICINO_E2E_STATE_DIR must not contain '..'"
  [[ ! -L "$state_dir" ]] || die "TICINO_E2E_STATE_DIR must not be a symlink"
}

pid_running() {
  local pid_file=$1
  [[ -f "$pid_file" ]] && kill -0 "$(<"$pid_file")" 2>/dev/null
}

write_runtime_env() {
  mkdir -p "$state_dir"
  chmod 700 "$state_dir"
  umask 077
  local db_password cookie_key credential_key workload_bearer
  db_password=$(openssl rand -hex 24)
  cookie_key=$(openssl rand -hex 32)
  credential_key=$(openssl rand -hex 32)
  workload_bearer=$(openssl rand -hex 32)
  {
    printf 'E2E_POSTGRES_DB=omnigent\n'
    printf 'E2E_POSTGRES_USER=omnigent\n'
    printf 'E2E_POSTGRES_PASSWORD=%s\n' "$db_password"
    printf 'E2E_POSTGRES_PORT=%s\n' "$postgres_port"
    printf 'OMNIGENT_OIDC_COOKIE_SECRET=%s\n' "$cookie_key"
    printf 'OMNIGENT_OIDC_CREDENTIAL_KEY=%s\n' "$credential_key"
  } >"$runtime_env"
  printf '%s' "$workload_bearer" >"$state_dir/workload-bearer"
  chmod 600 "$runtime_env" "$state_dir/workload-bearer"
}

load_runtime_env() {
  [[ -f "$runtime_env" ]] || die "missing runtime state; run '$0 up' first"
  set -a
  # shellcheck disable=SC1090
  source "$runtime_env"
  set +a
}

wait_for_url() {
  local url=$1
  local pid_file=$2
  for _ in $(seq 1 90); do
    if ! pid_running "$pid_file"; then
      die "server exited early; inspect $state_dir/server.log"
    fi
    if curl --fail --silent --show-error "$url" >/dev/null; then
      return
    fi
    sleep 1
  done
  die "timed out waiting for $url; inspect $state_dir/server.log"
}

up() {
  require_tool docker
  require_tool uv
  require_tool curl
  require_tool openssl
  validate_state_dir
  local ticino_issuer=${TICINO_ISSUER:-https://ticino.identity.local-cluster.local-dc.fabric.dog:8443/v1/issuer/sycamore}
  local ticino_authorization_endpoint=${TICINO_AUTHORIZATION_ENDPOINT:-https://ticino.us1.ddbuild.staging.dog/v1/issuer/sycamore/oauth/authorize}
  local ticino_token_endpoint=${TICINO_TOKEN_ENDPOINT:-https://ticino.us1.ddbuild.staging.dog/v1/issuer/sycamore/oauth/token}
  local ticino_jwks_uri=${TICINO_JWKS_URI:-https://ticino.us1.ddbuild.staging.dog/v1/issuer/sycamore/.well-known/keys}
  [[ -n ${HAB_LAUNCHER_WHEEL:-} || -n ${HAB_LAUNCHER_SOURCE_DIR:-} ]] || die \
    "set HAB_LAUNCHER_WHEEL or HAB_LAUNCHER_SOURCE_DIR from dd-source PR #91876"
  [[ ! -e "$runtime_env" ]] || die "state already exists at $state_dir; use status or down first"

  write_runtime_env
  load_runtime_env
  docker compose --env-file "$runtime_env" -f "$compose_file" -p omnigent_ticino_e2e up -d postgres
  local postgres_ready=false
  for _ in $(seq 1 45); do
    if docker compose --env-file "$runtime_env" -f "$compose_file" -p omnigent_ticino_e2e \
      exec -T postgres pg_isready -U "$E2E_POSTGRES_USER" -d "$E2E_POSTGRES_DB" >/dev/null; then
      postgres_ready=true
      break
    fi
    sleep 1
  done
  "$postgres_ready" || die "Postgres did not become ready"

  [[ -x "$venv/bin/python" ]] || (
    cd "$root_dir"
    UV_PROJECT_ENVIRONMENT="$venv" OMNIGENT_SKIP_WEB_UI=true uv sync --frozen --extra all --no-dev
  )
  local launcher_pythonpath=""
  if [[ -n ${HAB_LAUNCHER_SOURCE_DIR:-} ]]; then
    local launcher_source=${HAB_LAUNCHER_SOURCE_DIR%/}
    local dd_source_root
    dd_source_root=$(cd "$launcher_source/../../.." && pwd)
    [[ -f "$launcher_source/hab_launcher/production.py" ]] || die \
      "HAB_LAUNCHER_SOURCE_DIR must be dd-source/domains/ai-devx/omnigent"
    [[ -f "$dd_source_root/libs/py/dd_internal_authentication/dd_internal_authentication/ticino.py" ]] || die \
      "HAB_LAUNCHER_SOURCE_DIR must belong to a dd-source checkout with dd_internal_authentication"
    launcher_pythonpath="$launcher_source:$dd_source_root/libs/py/dd_internal_authentication"
    uv pip install --python "$venv/bin/python" grpcio protobuf cryptography click
  else
    [[ -r ${HAB_LAUNCHER_WHEEL:-} ]] || die "HAB_LAUNCHER_WHEEL is not readable"
    uv pip install --python "$venv/bin/python" --force-reinstall "$HAB_LAUNCHER_WHEEL"
  fi

  local habitat_api emissary_bind workload_file
  case "$habitat_mode" in
    fake)
      habitat_api="http://127.0.0.1:$habitat_port"
      emissary_bind="127.0.0.1:$exchange_port"
      workload_file="$state_dir/workload-bearer"
      PYTHONPATH="$launcher_pythonpath${PYTHONPATH:+:$PYTHONPATH}" \
        "$venv/bin/python" "$harness_dir/fake_services.py" \
        --receipt "$state_dir/receipt.json" \
        --exchange-port "$exchange_port" \
        --habitat-port "$habitat_port" \
        --launcher-module "$launcher_module" >"$state_dir/fake-services.log" 2>&1 &
      printf '%s\n' "$!" >"$state_dir/fake-services.pid"
      ;;
    real)
      [[ ${TICINO_E2E_ALLOW_REAL_HABITAT:-} == 1 ]] || die \
        "real Habitat requires TICINO_E2E_ALLOW_REAL_HABITAT=1"
      [[ -n ${HAB_APISERVER:-} ]] || die "real Habitat requires HAB_APISERVER"
      [[ -n ${EMISSARY_BIND_ADDRESS:-} ]] || die "real Habitat requires EMISSARY_BIND_ADDRESS"
      [[ -n ${HAB_WORKLOAD_TOKEN_FILE:-} && -r ${HAB_WORKLOAD_TOKEN_FILE:-} ]] || die \
        "real Habitat requires a readable HAB_WORKLOAD_TOKEN_FILE"
      habitat_api="$HAB_APISERVER"
      emissary_bind="$EMISSARY_BIND_ADDRESS"
      workload_file="$HAB_WORKLOAD_TOKEN_FILE"
      ;;
    *) die "TICINO_E2E_HABITAT_MODE must be fake or real" ;;
  esac

  (
    cd "$root_dir"
    exec env \
      OMNIGENT_AUTH_ENABLED=1 \
      OMNIGENT_AUTH_PROVIDER=oidc \
      PYTHONPATH="$launcher_pythonpath${PYTHONPATH:+:$PYTHONPATH}" \
      OMNIGENT_OIDC_ISSUER="$ticino_issuer" \
      OMNIGENT_OIDC_CLIENT_ID=omnigent-local \
      OMNIGENT_OIDC_REDIRECT_URI="http://127.0.0.1:$port/auth/callback" \
      OMNIGENT_OIDC_AUTHORIZATION_ENDPOINT="$ticino_authorization_endpoint" \
      OMNIGENT_OIDC_TOKEN_ENDPOINT="$ticino_token_endpoint" \
      OMNIGENT_OIDC_JWKS_URI="$ticino_jwks_uri" \
      OMNIGENT_OIDC_SKIP_EMAIL_VERIFICATION=1 \
      OMNIGENT_OIDC_COOKIE_SECRET="$OMNIGENT_OIDC_COOKIE_SECRET" \
      OMNIGENT_OIDC_CREDENTIAL_KEY="$OMNIGENT_OIDC_CREDENTIAL_KEY" \
      OMNIGENT_OIDC_ALLOWED_DOMAINS="${TICINO_ALLOWED_DOMAINS:-datadoghq.com}" \
      OMNIGENT_HAB_ENABLED=1 \
      OMNIGENT_SANDBOX_PROVIDER_MODULE="$launcher_module" \
      OMNIGENT_HAB_IMAGE=local-fake-image \
      OMNIGENT_HAB_PROFILE=local-fake-profile \
      OMNIGENT_HAB_RUNTIME=firecracker \
      OMNIGENT_HAB_ALLOW_ALL_EGRESS=false \
      OMNIGENT_PUBLIC_URL="http://127.0.0.1:$port" \
      OMNIGENT_HAB_REGISTRY_PATH="$state_dir/hab-registry.json" \
      HAB_APISERVER="$habitat_api" \
      HAB_WORKLOAD_TOKEN_FILE="$workload_file" \
      EMISSARY_ENABLED=true \
      EMISSARY_BIND_ADDRESS="$emissary_bind" \
      OMNIGENT_DATA_DIR="$state_dir/data" \
      "$venv/bin/omnigent" server \
        --host 127.0.0.1 --port "$port" --no-open \
        --database-uri "postgresql+psycopg://$E2E_POSTGRES_USER:$E2E_POSTGRES_PASSWORD@127.0.0.1:$postgres_port/$E2E_POSTGRES_DB" \
        --artifact-location "$state_dir/artifacts"
  ) >"$state_dir/server.log" 2>&1 &
  printf '%s\n' "$!" >"$state_dir/server.pid"
  wait_for_url "http://127.0.0.1:$port/health" "$state_dir/server.pid"
  printf '%s\n' "Ready: open http://127.0.0.1:$port, sign in, then create a Hab Sandbox session."
  if [[ "$habitat_mode" == fake ]]; then
    printf '%s\n' "The local fake Habitat will reject the launch after observing the exchange; run '$0 status' for the secret-free receipt."
  else
    printf '%s\n' "Real-Habitat mode is enabled; use only an approved Emissary/workload-identity environment."
  fi
}

status() {
  validate_state_dir
  [[ "$habitat_mode" == fake ]] || die "status receipts exist only in fake-Habitat mode"
  [[ -f "$state_dir/receipt.json" ]] || die "no receipt found; run '$0 up' first"
  "$venv/bin/python" - "$state_dir/receipt.json" <<'PY'
import json
import sys

receipt = json.load(open(sys.argv[1]))
required = (
    "exchange_route_matched",
    "exchange_requested_hab_audience",
    "subject_token_present",
    "subject_token_is_jwt",
    "subject_token_audience_matches_client",
    "habitat_authorization_present",
)
missing = [field for field in required if not receipt.get(field)]
if missing:
    print("NOT YET PROVEN: " + ", ".join(missing))
    raise SystemExit(1)
print("PASS: browser OIDC ID token reached the launcher exchange seam and its exchanged output reached fake Habitat.")
PY
}

down() {
  validate_state_dir
  if pid_running "$state_dir/server.pid"; then
    kill "$(<"$state_dir/server.pid")" || true
  fi
  if pid_running "$state_dir/fake-services.pid"; then
    kill "$(<"$state_dir/fake-services.pid")" || true
  fi
  if [[ -f "$runtime_env" ]]; then
    load_runtime_env
    docker compose --env-file "$runtime_env" -f "$compose_file" -p omnigent_ticino_e2e down -v
  fi
  rm -rf "$state_dir"
  printf '%s\n' "Removed local harness state and its ephemeral credentials."
}

case "${1:-}" in
  up) up ;;
  status) status ;;
  down) down ;;
  *)
    printf '%s\n' "Usage: $0 {up|status|down}" >&2
    exit 2
    ;;
esac
