#!/usr/bin/env bash
# Opt-in local live-token harness. See README.md in this directory.
set -euo pipefail

root_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
harness_dir="$root_dir/dev/ticino_local_e2e"
state_dir=${TICINO_E2E_STATE_DIR:-/tmp/omnigent-ticino-token-handoff-e2e}
runtime_env="$state_dir/runtime.env"
server_config="$state_dir/server-config.yaml"
pgpass_file="$state_dir/pgpass"
compose_file="$harness_dir/docker-compose.postgres.yml"
port=${TICINO_E2E_PORT:-6767}
exchange_port=${TICINO_E2E_EXCHANGE_PORT:-6768}
habitat_port=${TICINO_E2E_HABITAT_PORT:-6769}
postgres_port=${TICINO_E2E_POSTGRES_PORT:-6770}
launcher_module=${HAB_LAUNCHER_MODULE:-hab_launcher}
habitat_mode=${TICINO_E2E_HABITAT_MODE:-fake}
venv="$state_dir/venv"
launcher_pythonpath_file="$state_dir/launcher-pythonpath"

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

recorded_pid_is_owned() {
  local pid=$1
  [[ "$pid" =~ ^[0-9]+$ ]] || return 1
  [[ $(ps -o uid= -p "$pid" 2>/dev/null | tr -d ' ') == "$UID" ]] || return 1
  local command
  command=$(ps -o command= -p "$pid" 2>/dev/null) || return 1
  [[ "$command" == *"$state_dir"* ]]
}

stop_recorded_pid() {
  local pid_file=$1
  local label=$2
  if ! pid_running "$pid_file"; then
    return
  fi
  local pid
  pid=$(<"$pid_file")
  recorded_pid_is_owned "$pid" || die "refusing to stop an unowned $label PID"
  kill "$pid"
  for _ in $(seq 1 10); do
    ! kill -0 "$pid" 2>/dev/null && return
    sleep 1
  done
  die "recorded $label PID did not stop"
}

write_runtime_env() {
  local mode=$1
  mkdir -p "$state_dir"
  chmod 700 "$state_dir"
  umask 077
  local db_name=omnigent db_user=omnigent
  local db_password cookie_key credential_key
  db_password=$(openssl rand -hex 24)
  cookie_key=$(openssl rand -hex 32)
  credential_key=$(openssl rand -hex 32)
  {
    printf 'E2E_POSTGRES_DB=%s\n' "$db_name"
    printf 'E2E_POSTGRES_USER=%s\n' "$db_user"
    printf 'E2E_POSTGRES_PASSWORD=%s\n' "$db_password"
    printf 'E2E_POSTGRES_PORT=%s\n' "$postgres_port"
    printf 'OMNIGENT_OIDC_COOKIE_SECRET=%s\n' "$cookie_key"
    printf 'OMNIGENT_OIDC_CREDENTIAL_KEY=%s\n' "$credential_key"
  } >"$runtime_env"
  printf 'database_uri: "postgresql+psycopg://%s@127.0.0.1:%s/%s"\n' \
    "$db_user" "$postgres_port" "$db_name" >"$server_config"
  printf '127.0.0.1:%s:%s:%s:%s\n' \
    "$postgres_port" "$db_name" "$db_user" "$db_password" >"$pgpass_file"
  chmod 600 "$runtime_env" "$server_config" "$pgpass_file"
  if [[ "$mode" == fake ]]; then
    # This is deliberately non-secret fake data, but its JWT shape lets the
    # current file-exchange launcher exercise the same local readiness path.
    python3 - "$state_dir/workload-bearer" <<'PY'
import base64
import json
import sys
import time

payload = base64.urlsafe_b64encode(
    json.dumps({"aud": "identity", "exp": int(time.time()) + 3600}).encode()
).rstrip(b"=").decode()
open(sys.argv[1], "w").write(f"fake.{payload}.signature")
PY
    chmod 600 "$state_dir/workload-bearer"
  fi
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

real_check() {
  require_tool python3
  validate_state_dir
  OMNIGENT_OIDC_REDIRECT_URI="http://127.0.0.1:$port/auth/callback" \
    python3 "$harness_dir/real_check.py" --port "$port" --network
}

write_real_receipt() {
  # A local, secret-free marker makes it clear that down cannot claim remote
  # cleanup merely because it stopped the laptop-side process.
  python3 - "$state_dir/real-readiness.json" <<'PY'
import json
import os
import sys

receipt = {
    "habitat_api": os.environ["HAB_APISERVER"],
    "image": os.environ["OMNIGENT_HAB_IMAGE"],
    "mode": "real",
    "preflight": "passed",
    "profile": os.environ["OMNIGENT_HAB_PROFILE"],
    "public_url": os.environ["OMNIGENT_PUBLIC_URL"],
    "remote_cleanup": "unconfirmed",
}
with open(sys.argv[1], "w") as output:
    json.dump(receipt, output, sort_keys=True)
    output.write("\n")
PY
  printf '%s\n' "$HAB_WORKLOAD_TOKEN_FILE" >"$state_dir/real-workload-token-file"
  chmod 600 "$state_dir/real-readiness.json" "$state_dir/real-workload-token-file"
}

source_launcher_pythonpath() {
  local launcher_source=${HAB_LAUNCHER_SOURCE_DIR%/}
  local dd_source_root
  dd_source_root=$(cd "$launcher_source/../../.." && pwd)
  [[ -f "$launcher_source/hab_launcher/production.py" ]] || die \
    "HAB_LAUNCHER_SOURCE_DIR must be dd-source/domains/ai-devx/omnigent"
  [[ -f "$dd_source_root/libs/py/dd_internal_authentication/dd_internal_authentication/ticino.py" ]] || die \
    "HAB_LAUNCHER_SOURCE_DIR must belong to a dd-source checkout with dd_internal_authentication"
  printf '%s:%s' "$launcher_source" "$dd_source_root/libs/py/dd_internal_authentication"
}

existing_launcher_pythonpath() {
  if [[ -n ${HAB_LAUNCHER_SOURCE_DIR:-} ]]; then
    source_launcher_pythonpath
    return
  fi
  [[ -f "$launcher_pythonpath_file" ]] || die \
    "missing launcher state; set HAB_LAUNCHER_SOURCE_DIR used for the existing server"
  cat "$launcher_pythonpath_file"
}

start_real_server() {
  local launcher_pythonpath=$1
  local ticino_issuer=${TICINO_ISSUER:-https://ticino.identity.local-cluster.local-dc.fabric.dog:8443/v1/issuer/sycamore}
  local ticino_authorization_endpoint=${TICINO_AUTHORIZATION_ENDPOINT:-https://ticino.us1.ddbuild.staging.dog/v1/issuer/sycamore/oauth/authorize}
  local ticino_token_endpoint=${TICINO_TOKEN_ENDPOINT:-https://ticino.us1.ddbuild.staging.dog/v1/issuer/sycamore/oauth/token}
  local ticino_jwks_uri=${TICINO_JWKS_URI:-https://ticino.us1.ddbuild.staging.dog/v1/issuer/sycamore/.well-known/keys}
  local ticino_exchange_address=${OMNIGENT_HAB_TICINO_ADDRESS:-https://ticino.us1.ddbuild.staging.dog}
  (
    cd "$root_dir"
    exec nohup env \
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
      OMNIGENT_DD_HAB_LOCAL_DEV=1 \
      OMNIGENT_HAB_ENABLED=1 \
      OMNIGENT_SANDBOX_PROVIDER_MODULE="$launcher_module" \
      OMNIGENT_HAB_IMAGE="$OMNIGENT_HAB_IMAGE" \
      OMNIGENT_HAB_PROFILE="$OMNIGENT_HAB_PROFILE" \
      OMNIGENT_HAB_RUNTIME=firecracker \
      OMNIGENT_HAB_ALLOW_ALL_EGRESS=true \
      OMNIGENT_PUBLIC_URL="$OMNIGENT_PUBLIC_URL" \
      OMNIGENT_HAB_REGISTRY_PATH="$state_dir/hab-registry.json" \
      HAB_APISERVER="$HAB_APISERVER" \
      OMNIGENT_HAB_EXCHANGE_MODE=file \
      HAB_WORKLOAD_TOKEN_FILE="$HAB_WORKLOAD_TOKEN_FILE" \
      OMNIGENT_HAB_TICINO_ADDRESS="$ticino_exchange_address" \
      EMISSARY_ENABLED=false \
      OMNIGENT_DATA_DIR="$state_dir/data" \
      PGPASSFILE="$pgpass_file" \
      "$venv/bin/omnigent" server \
        --host 127.0.0.1 --port "$port" --no-open \
        --config "$server_config" \
        --artifact-location "$state_dir/artifacts"
  ) </dev/null >"$state_dir/server.log" 2>&1 &
  printf '%s\n' "$!" >"$state_dir/server.pid"
  wait_for_url "http://127.0.0.1:$port/health" "$state_dir/server.pid"
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
  case "$habitat_mode" in
    fake|real) ;;
    *) die "TICINO_E2E_HABITAT_MODE must be fake or real" ;;
  esac

  if [[ "$habitat_mode" == real ]]; then
    # Fail before Docker or a server process starts.  This is read-only and
    # rejects missing opt-in, insecure credentials, fake launch inputs, and
    # a loopback guest callback tunnel.
    real_check
  fi

  write_runtime_env "$habitat_mode"
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
  uv pip install --python "$venv/bin/python" 'psycopg[binary]>=3.1,<4'
  local launcher_pythonpath=""
  if [[ -n ${HAB_LAUNCHER_SOURCE_DIR:-} ]]; then
    launcher_pythonpath=$(source_launcher_pythonpath)
    uv pip install --python "$venv/bin/python" grpcio protobuf cryptography click
  else
    [[ -r ${HAB_LAUNCHER_WHEEL:-} ]] || die "HAB_LAUNCHER_WHEEL is not readable"
    uv pip install --python "$venv/bin/python" --force-reinstall "$HAB_LAUNCHER_WHEEL"
  fi
  printf '%s\n' "$launcher_pythonpath" >"$launcher_pythonpath_file"
  chmod 600 "$launcher_pythonpath_file"

  local habitat_api exchange_address workload_file hab_image hab_profile hab_egress public_url
  case "$habitat_mode" in
    fake)
      habitat_api="http://127.0.0.1:$habitat_port"
      exchange_address="http://127.0.0.1:$exchange_port/ticino/agent"
      workload_file="$state_dir/workload-bearer"
      hab_image="local-fake-image"
      hab_profile="local-fake-profile"
      hab_egress=false
      public_url="http://127.0.0.1:$port"
      nohup env PYTHONPATH="$launcher_pythonpath${PYTHONPATH:+:$PYTHONPATH}" \
        "$venv/bin/python" "$harness_dir/fake_services.py" \
        --receipt "$state_dir/receipt.json" \
        --exchange-port "$exchange_port" \
        --habitat-port "$habitat_port" \
        --launcher-module "$launcher_module" \
        </dev/null >"$state_dir/fake-services.log" 2>&1 &
      printf '%s\n' "$!" >"$state_dir/fake-services.pid"
      ;;
    real)
      [[ ${TICINO_E2E_ALLOW_REAL_HABITAT:-} == 1 ]] || die \
        "real Habitat requires TICINO_E2E_ALLOW_REAL_HABITAT=1"
      habitat_api="$HAB_APISERVER"
      workload_file="$HAB_WORKLOAD_TOKEN_FILE"
      exchange_address=${OMNIGENT_HAB_TICINO_ADDRESS:-https://ticino.us1.ddbuild.staging.dog}
      hab_image="$OMNIGENT_HAB_IMAGE"
      hab_profile="$OMNIGENT_HAB_PROFILE"
      hab_egress=true
      public_url="$OMNIGENT_PUBLIC_URL"
      write_real_receipt
      ;;
    *) die "TICINO_E2E_HABITAT_MODE must be fake or real" ;;
  esac

  (
    cd "$root_dir"
    exec nohup env \
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
      OMNIGENT_DD_HAB_LOCAL_DEV=1 \
      OMNIGENT_HAB_ENABLED=1 \
      OMNIGENT_SANDBOX_PROVIDER_MODULE="$launcher_module" \
      OMNIGENT_HAB_IMAGE="$hab_image" \
      OMNIGENT_HAB_PROFILE="$hab_profile" \
      OMNIGENT_HAB_RUNTIME=firecracker \
      OMNIGENT_HAB_ALLOW_ALL_EGRESS="$hab_egress" \
      OMNIGENT_PUBLIC_URL="$public_url" \
      OMNIGENT_HAB_REGISTRY_PATH="$state_dir/hab-registry.json" \
      HAB_APISERVER="$habitat_api" \
      OMNIGENT_HAB_EXCHANGE_MODE=file \
      HAB_WORKLOAD_TOKEN_FILE="$workload_file" \
      OMNIGENT_HAB_TICINO_ADDRESS="$exchange_address" \
      EMISSARY_ENABLED=false \
      OMNIGENT_DATA_DIR="$state_dir/data" \
      PGPASSFILE="$pgpass_file" \
      "$venv/bin/omnigent" server \
        --host 127.0.0.1 --port "$port" --no-open \
        --config "$server_config" \
        --artifact-location "$state_dir/artifacts"
  ) </dev/null >"$state_dir/server.log" 2>&1 &
  printf '%s\n' "$!" >"$state_dir/server.pid"
  wait_for_url "http://127.0.0.1:$port/health" "$state_dir/server.pid"
  printf '%s\n' "Ready: open http://127.0.0.1:$port, sign in, then create a Hab Sandbox session."
  if [[ "$habitat_mode" == fake ]]; then
    printf '%s\n' "The local fake Habitat will reject the launch after observing the exchange; run '$0 status' for the secret-free receipt."
  else
    printf '%s\n' "Real-Habitat mode is enabled with the explicit file-backed workload identity; local cleanup does not confirm remote Hab deletion."
  fi
}

status() {
  validate_state_dir
  if [[ "$habitat_mode" == real ]]; then
    [[ -f "$state_dir/real-readiness.json" ]] || die "no real-mode readiness receipt found; run '$0 up' first"
    cat "$state_dir/real-readiness.json"
    printf '%s\n' "WARNING: this receipt does not prove remote Hab cleanup."
    return
  fi
  [[ -f "$state_dir/receipt.json" ]] || die "no receipt found; run '$0 up' first"
  "$venv/bin/python" - "$state_dir/receipt.json" <<'PY'
import json
import sys

receipt = json.load(open(sys.argv[1]))
required = (
    "exchange_route_matched",
    "exchange_used_post",
    "exchange_form_urlencoded",
    "emissary_request_header_matches",
    "workload_authorization_present",
    "workload_authorization_distinct_from_subject_token",
    "exchange_grant_type_matches",
    "exchange_requested_hab_audience",
    "exchange_subject_token_type_matches",
    "exchange_requested_token_type_matches",
    "subject_token_present",
    "subject_token_is_jwt",
    "subject_token_audience_matches_client",
    "habitat_authorization_present",
)
missing = [field for field in required if not receipt.get(field)]
if missing:
    print("NOT YET PROVEN: " + ", ".join(missing))
    raise SystemExit(1)
print("PASS: RFC 8693 workload/subject-token exchange reached fake Habitat without persisting credentials.")
PY
}

down() {
  validate_state_dir
  stop_recorded_pid "$state_dir/server.pid" "Omnigent server"
  stop_recorded_pid "$state_dir/fake-services.pid" "fake service"
  if [[ -f "$runtime_env" ]]; then
    load_runtime_env
    docker compose --env-file "$runtime_env" -f "$compose_file" -p omnigent_ticino_e2e down -v
  fi
  if [[ -f "$state_dir/real-workload-token-file" ]]; then
    local workload_file
    workload_file=$(<"$state_dir/real-workload-token-file")
    if [[ -n "$workload_file" ]] && python3 "$harness_dir/real_check.py" \
      --port "$port" --remove-workload-file "$workload_file"; then
      printf '%s\n' "Removed the real-mode workload bearer file."
    else
      printf '%s\n' "WARNING: could not safely remove the recorded real-mode workload bearer file." >&2
    fi
    printf '%s\n' "WARNING: real mode removed local state only; it did not delete or verify any remote Hab." >&2
  fi
  rm -rf "$state_dir"
  printf '%s\n' "Removed local harness state and its ephemeral credentials."
}

reconfigure_real() {
  require_tool docker
  require_tool python3
  validate_state_dir
  [[ "$habitat_mode" == real && ${TICINO_E2E_ALLOW_REAL_HABITAT:-} == 1 ]] || die \
    "reconfigure-real requires TICINO_E2E_HABITAT_MODE=real and TICINO_E2E_ALLOW_REAL_HABITAT=1"
  [[ -f "$runtime_env" && -f "$server_config" && -f "$pgpass_file" ]] || die \
    "missing persistent runtime state; run fake mode first"
  [[ -x "$venv/bin/omnigent" ]] || die "missing existing harness virtual environment"

  # Do every fail-closed check before touching the signed-in local server.
  real_check
  load_runtime_env
  docker compose --env-file "$runtime_env" -f "$compose_file" -p omnigent_ticino_e2e \
    exec -T postgres pg_isready -U "$E2E_POSTGRES_USER" -d "$E2E_POSTGRES_DB" >/dev/null || die \
    "the existing Docker Postgres is not ready; reconfigure-real will not recreate it"
  local launcher_pythonpath
  launcher_pythonpath=$(existing_launcher_pythonpath)

  # The PID ownership check requires the command to be in this harness state
  # directory; no arbitrary process or Docker resource is stopped here.
  stop_recorded_pid "$state_dir/server.pid" "Omnigent server"
  stop_recorded_pid "$state_dir/fake-services.pid" "fake service"
  write_real_receipt
  start_real_server "$launcher_pythonpath"
  printf '%s\n' "Reconfigured the existing signed-in local runtime for real Habitat mode without recreating Postgres or credential state."
}

case "${1:-}" in
  up) up ;;
  real-check) real_check ;;
  reconfigure-real) reconfigure_real ;;
  status) status ;;
  down) down ;;
  *)
    printf '%s\n' "Usage: $0 {up|real-check|reconfigure-real|status|down}" >&2
    exit 2
    ;;
esac
