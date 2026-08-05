#!/usr/bin/env bash
# The same end-to-end suite as run.sh, against binaries instead of
# containers — for environments without a Docker daemon.
#
# It covers the two policy variants and the read-only kill switch. What it
# cannot cover is anything the container boundary provides: the image build,
# the https upstream with its private CA, and the memory limit that makes the
# streaming test meaningful. Use run.sh where a daemon is available.
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
  rm -rf "$work"
}
trap cleanup EXIT

echo "==> building the proxy"
go build -o "$work/proxy" .

echo "==> starting minio"
MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin123 \
  "$minio_bin" server "$work/data" --address :19000 --console-address :19001 \
  >"$work/minio.log" 2>&1 &
pids+=($!)

for _ in $(seq 1 60); do
  curl -fsS -o /dev/null http://127.0.0.1:19000/minio/health/live 2>/dev/null && break
  sleep 0.5
done

start_proxy() {
  local name=$1 policy=$2 api=$3 admin=$4
  shift 4
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
  pids+=($!)
  for _ in $(seq 1 40); do
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

export E2E_MINIO_ENDPOINT=http://127.0.0.1:19000
export E2E_MINIO_ACCESS_KEY=minioadmin
export E2E_MINIO_SECRET_KEY=minioadmin123
export E2E_BUCKET=e2e
export E2E_PEPPER=an-e2e-deployment-wide-pepper

status=0
run_variant() {
  echo
  echo "==> variant: $E2E_NAME"
  go test -tags e2e -count=1 -timeout 30m "$@" ./e2e/... || status=1
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
)

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
)

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
)

echo
if [[ $status -eq 0 ]]; then
  echo "==> all variants passed"
else
  echo "==> at least one variant failed" >&2
fi
exit $status
