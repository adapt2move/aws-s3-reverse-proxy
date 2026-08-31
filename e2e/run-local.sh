#!/usr/bin/env bash
# The same end-to-end suite as run.sh, against binaries instead of
# containers — for environments without a Docker daemon.
#
# It covers the two policy variants, the read-only kill switch and the
# caching variant. What it cannot cover is anything the container boundary
# provides: the image build, the https upstream with its private CA, and the
# memory limit that makes the streaming test meaningful. Use run.sh where a
# daemon is available.
#
#   MINIO_BINARY=/path/to/minio ./e2e/run-local.sh
#   ./e2e/run-local.sh -run TestTenantIsolation
set -euo pipefail

cd "$(dirname "$0")/.."
work="$(mktemp -d)"
minio_bin="${MINIO_BINARY:-$(command -v minio || true)}"

if [[ -z "$minio_bin" ]]; then
  echo "no minio binary found; set MINIO_BINARY or install it from https://dl.min.io" >&2
  exit 1
fi

pids=()
cleanup() {
  for pid in "${pids[@]:-}"; do kill "$pid" 2>/dev/null || true; done
  # A proxy the restart test brought back has a pid this script never saw,
  # so its pid file is the only record of it. Leaving one running would hold
  # its ports, and the next run would then test a deployment nobody
  # configured — which require_free_port catches, but only by refusing to
  # start at all.
  for file in "$work"/*.pid; do
    if [[ -f "$file" ]]; then kill "$(cat "$file")" 2>/dev/null || true; fi
  done
  rm -rf "$work"
}
trap cleanup EXIT

echo "==> building the proxy"
go build -o "$work/proxy" ./cmd/aws-s3-reverse-proxy

echo "==> starting minio"
MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin123 \
  "$minio_bin" server "$work/data" --address :19000 --console-address :19001 \
  >"$work/minio.log" 2>&1 &
pids+=($!)

for _ in $(seq 1 60); do
  curl -fsS -o /dev/null http://127.0.0.1:19000/minio/health/live 2>/dev/null && break
  sleep 0.5
done

# A port already in use is the one failure that can masquerade as success:
# whatever is squatting on it answers /readyz perfectly well, and the run
# then exercises a deployment nobody configured. Refuse to start instead.
require_free_port() {
  local port=$1 name=$2
  if curl -fsS -o /dev/null --max-time 1 "http://127.0.0.1:$port/healthz" 2>/dev/null; then
    echo "!!! something is already serving on port $port; $name cannot start there" >&2
    return 1
  fi
  return 0
}

start_proxy() {
  local name=$1 policy=$2 api=$3 admin=$4
  shift 4
  require_free_port "$admin" "$name"
  cp "$policy" "$work/$name-policy.yaml"
  UPSTREAM_ACCESS_KEY_ID=minioadmin \
  UPSTREAM_SECRET_ACCESS_KEY=minioadmin123 \
  CREDENTIAL_PEPPER=an-e2e-deployment-wide-pepper \
    "$work/proxy" \
      --policy-file="$work/$name-policy.yaml" \
      --allowed-endpoint="127.0.0.1:$api" \
      --allowed-source-subnet=127.0.0.1/32 \
      --listen-addr=":$api" \
      --health-listen-addr=":$admin" \
      --metrics-listen-addr=":$admin" \
      --upstream-endpoint=http://127.0.0.1:19000 \
      --policy-reload-interval=2s \
      "$@" >"$work/$name.log" 2>&1 &
  local pid=$!
  pids+=("$pid")
  for _ in $(seq 1 40); do
    # Check the process before the port. If it died — most often because
    # something else already holds the port — that other process will answer
    # /readyz perfectly happily, and the whole run would then test a
    # deployment nobody configured.
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "!!! $name exited during startup" >&2
      cat "$work/$name.log" >&2
      return 1
    fi
    curl -fsS -o /dev/null "http://127.0.0.1:$admin/readyz" 2>/dev/null && return 0
    sleep 0.25
  done
  echo "!!! $name never became ready" >&2
  cat "$work/$name.log" >&2
  return 1
}

# The caching variant needs to be restartable with its cache intact, which is
# how the suite checks that a restart recovers rather than starts cold. The
# script this writes is what the test runs; keeping the launch in one place
# means the restarted proxy cannot drift from the original.
start_cache_proxy() {
  local name=$1 policy=$2 api=$3 admin=$4 dir=$5
  mkdir -p "$dir"
  cp "$policy" "$work/$name-policy.yaml"
  cat >"$work/start-$name.sh" <<SCRIPT
#!/usr/bin/env bash
UPSTREAM_ACCESS_KEY_ID=minioadmin \
UPSTREAM_SECRET_ACCESS_KEY=minioadmin123 \
CREDENTIAL_PEPPER=an-e2e-deployment-wide-pepper \
  "$work/proxy" \
    --policy-file="$work/$name-policy.yaml" \
    --allowed-endpoint="127.0.0.1:$api" \
    --allowed-source-subnet=127.0.0.1/32 \
    --listen-addr=":$api" \
    --health-listen-addr=":$admin" \
    --metrics-listen-addr=":$admin" \
    --upstream-endpoint=http://127.0.0.1:19000 \
    --cache-dir="$dir" \
    --cache-max-bytes=67108864 \
    --cache-max-object-size=8388608 \
    --cache-max-age=3s \
    >>"$work/$name.log" 2>&1 &
echo \$! > "$work/$name.pid"
SCRIPT
  cat >"$work/restart-$name.sh" <<SCRIPT
#!/usr/bin/env bash
set -e
if [ -f "$work/$name.pid" ]; then
  kill "\$(cat "$work/$name.pid")" 2>/dev/null || true
  # Wait for the port to come free, or the replacement binds nothing.
  for _ in \$(seq 1 40); do
    kill -0 "\$(cat "$work/$name.pid")" 2>/dev/null || break
    sleep 0.25
  done
fi
"$work/start-$name.sh"
SCRIPT
  chmod +x "$work/start-$name.sh" "$work/restart-$name.sh"

  require_free_port "$admin" "$name"
  "$work/start-$name.sh"
  local pid
  pid=$(cat "$work/$name.pid")
  pids+=("$pid")
  for _ in $(seq 1 40); do
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "!!! $name exited during startup" >&2
      cat "$work/$name.log" >&2
      return 1
    fi
    curl -fsS -o /dev/null "http://127.0.0.1:$admin/readyz" 2>/dev/null && return 0
    sleep 0.25
  done
  echo "!!! $name never became ready" >&2
  cat "$work/$name.log" >&2
  return 1
}

echo "==> starting the proxy variants"
start_proxy variant-a e2e/policies/policy-a.yaml 18099 18100
start_proxy variant-b e2e/policies/policy-b.yaml 18299 18300
start_proxy variant-ro e2e/policies/policy-a.yaml 18399 18400 --read-only
start_cache_proxy variant-cache e2e/policies/policy-a.yaml 18499 18500 "$work/cache"

export E2E_MINIO_ENDPOINT=http://127.0.0.1:19000
export E2E_MINIO_ACCESS_KEY=minioadmin
export E2E_MINIO_SECRET_KEY=minioadmin123
export E2E_BUCKET=e2e
export E2E_PEPPER=an-e2e-deployment-wide-pepper

status=0
# Each variant runs in a subshell so its exports cannot leak into the next
# one — which means the subshell's exit status, not a variable set inside
# it, is what says whether it passed.
run_variant() {
  echo
  echo "==> variant: $E2E_NAME"
  go test -tags e2e -count=1 -timeout 30m "$@" ./e2e/...
}

(
  export E2E_NAME=variant-a
  export E2E_PROXY_ENDPOINT=http://127.0.0.1:18099
  export E2E_ADMIN_ENDPOINT=http://127.0.0.1:18100
  export E2E_ACCESS_KEY_TEMPLATE='{tenant}{level}'
  export E2E_SECRET_TEMPLATE='{tenant}:{level}'
  export E2E_KEY_PREFIX_TEMPLATE='{tenant}/'
  export E2E_READ_LEVEL=ro E2E_WRITE_LEVEL=rw
  export E2E_TENANT_A=a1b2c3d4e5f60718293a4b5c6d7e8f90
  export E2E_TENANT_B=0f9e8d7c6b5a49382716f5e4d3c2b1a0
  export E2E_POLICY_FILE="$work/variant-a-policy.yaml"
  export E2E_POLICY_TIGHTENED="$PWD/e2e/policies/policy-a-tightened.yaml"
  export E2E_POLICY_INVALID="$PWD/e2e/policies/policy-invalid.yaml"
  export E2E_RELOAD_INTERVAL=2s
  export E2E_LARGE_OBJECT_SIZE=268435456
  run_variant "$@"
) || status=1

(
  export E2E_NAME=variant-b
  export E2E_PROXY_ENDPOINT=http://127.0.0.1:18299
  export E2E_ADMIN_ENDPOINT=http://127.0.0.1:18300
  export E2E_ACCESS_KEY_TEMPLATE='{tenant}-{level}'
  export E2E_SECRET_TEMPLATE='v1/{level}/{tenant}'
  export E2E_KEY_PREFIX_TEMPLATE='tenants/{tenant}/data/'
  export E2E_READ_LEVEL=reader E2E_WRITE_LEVEL=writer
  export E2E_TENANT_A=acmecorp1
  export E2E_TENANT_B=globex2000
  run_variant "$@"
) || status=1

(
  export E2E_NAME=variant-readonly
  export E2E_PROXY_ENDPOINT=http://127.0.0.1:18399
  export E2E_ADMIN_ENDPOINT=http://127.0.0.1:18400
  export E2E_ACCESS_KEY_TEMPLATE='{tenant}{level}'
  export E2E_SECRET_TEMPLATE='{tenant}:{level}'
  export E2E_KEY_PREFIX_TEMPLATE='{tenant}/'
  export E2E_READ_LEVEL=ro E2E_WRITE_LEVEL=rw
  export E2E_TENANT_A=a1b2c3d4e5f60718293a4b5c6d7e8f90
  export E2E_TENANT_B=0f9e8d7c6b5a49382716f5e4d3c2b1a0
  export E2E_READ_ONLY=true
  run_variant "$@"
) || status=1

(
  export E2E_NAME=variant-cache
  export E2E_PROXY_ENDPOINT=http://127.0.0.1:18499
  export E2E_ADMIN_ENDPOINT=http://127.0.0.1:18500
  export E2E_ACCESS_KEY_TEMPLATE='{tenant}{level}'
  export E2E_SECRET_TEMPLATE='{tenant}:{level}'
  export E2E_KEY_PREFIX_TEMPLATE='{tenant}/'
  export E2E_READ_LEVEL=ro E2E_WRITE_LEVEL=rw
  export E2E_TENANT_A=a1b2c3d4e5f60718293a4b5c6d7e8f90
  export E2E_TENANT_B=0f9e8d7c6b5a49382716f5e4d3c2b1a0
  export E2E_CACHE_MAX_AGE=3s
  export E2E_CACHE_MAX_OBJECT_SIZE=8388608
  export E2E_CACHE_RESTART_CMD="$work/restart-variant-cache.sh"
  run_variant "$@"
) || status=1

echo
if [[ $status -eq 0 ]]; then
  echo "==> all variants passed"
else
  echo "==> at least one variant failed" >&2
fi
exit $status
