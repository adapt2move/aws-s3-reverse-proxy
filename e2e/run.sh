#!/usr/bin/env bash
# Bring up the containerized stack and run the end-to-end suite against each
# proxy variant in it.
#
#   ./e2e/run.sh              # everything
#   ./e2e/run.sh -run TestTenantIsolation
#
# Set E2E_KEEP=1 to leave the stack running afterwards for poking at.
set -euo pipefail

cd "$(dirname "$0")"
compose=(docker compose -f docker-compose.yml)

# The hot-reload test rewrites this file, so it starts as a copy rather than
# as the checked-in policy.
cp policies/policy-a.yaml policies/live-a.yaml

# Variant B takes both secrets as mounted files, the way a Kubernetes Secret
# volume presents them. Written with no trailing newline on purpose — a stray
# one would silently change every derived secret, which is exactly the
# mistake the proxy trims away.
mkdir -p secrets ca
printf '%s' 'minioadmin,minioadmin123' > secrets/upstream-credentials
printf '%s' 'an-e2e-deployment-wide-pepper' > secrets/pepper

cleanup() {
  if [[ "${E2E_KEEP:-0}" != "1" ]]; then
    "${compose[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  fi
  rm -f policies/live-a.yaml
  rm -rf secrets ca
}
trap cleanup EXIT

echo "==> building and starting the stack"
"${compose[@]}" up -d --build

wait_for_health() {
  local url=$1 name=$2
  for _ in $(seq 1 60); do
    if curl -fsS -o /dev/null "$url" 2>/dev/null; then
      echo "    $name is ready"
      return 0
    fi
    sleep 1
  done
  echo "!!! $name never became healthy at $url" >&2
  "${compose[@]}" logs --tail 50 >&2
  return 1
}

wait_for_health http://127.0.0.1:18100/readyz proxy-http
wait_for_health http://127.0.0.1:18300/readyz proxy-tls
wait_for_health http://127.0.0.1:18400/readyz proxy-readonly

# Shared by every variant.
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
  go test -tags e2e -count=1 -timeout 30m "$@" ./...
}

# ---------------------------------------------------------------- variant A
# http upstream; tenant ids are 32 hex characters with the level glued on;
# one prefix segment per tenant; secrets from the environment.
(
  export E2E_NAME=proxy-http
  export E2E_PROXY_ENDPOINT=http://127.0.0.1:18099
  export E2E_ADMIN_ENDPOINT=http://127.0.0.1:18100
  export E2E_MINIO_ENDPOINT=http://127.0.0.1:19000
  export E2E_ACCESS_KEY_TEMPLATE='{tenant}{level}'
  export E2E_SECRET_TEMPLATE='{tenant}:{level}'
  export E2E_KEY_PREFIX_TEMPLATE='{tenant}/'
  export E2E_READ_LEVEL=ro
  export E2E_WRITE_LEVEL=rw
  export E2E_TENANT_A=a1b2c3d4e5f60718293a4b5c6d7e8f90
  export E2E_TENANT_B=0f9e8d7c6b5a49382716f5e4d3c2b1a0
  # Only this variant exposes its policy file to the test process.
  export E2E_POLICY_FILE="$PWD/policies/live-a.yaml"
  export E2E_POLICY_TIGHTENED="$PWD/policies/policy-a-tightened.yaml"
  export E2E_POLICY_INVALID="$PWD/policies/policy-invalid.yaml"
  export E2E_RELOAD_INTERVAL=2s
  # 256 MiB through a container limited to 192 MiB: buffering would be an
  # OOM kill, not a slow test.
  export E2E_LARGE_OBJECT_SIZE=268435456
  run_variant "$@"
) || status=1

# ---------------------------------------------------------------- variant B
# https upstream behind a private CA; a different access-key-id layout,
# different level names and a key prefix several segments deep; secrets from
# mounted files.
(
  export E2E_NAME=proxy-tls
  export E2E_PROXY_ENDPOINT=http://127.0.0.1:18299
  export E2E_ADMIN_ENDPOINT=http://127.0.0.1:18300
  export E2E_MINIO_ENDPOINT=https://127.0.0.1:19443
  export E2E_ACCESS_KEY_TEMPLATE='{tenant}-{level}'
  export E2E_SECRET_TEMPLATE='v1/{level}/{tenant}'
  export E2E_KEY_PREFIX_TEMPLATE='tenants/{tenant}/data/'
  export E2E_READ_LEVEL=reader
  export E2E_WRITE_LEVEL=writer
  export E2E_TENANT_A=acmecorp1
  export E2E_TENANT_B=globex2000
  # The suite reaches MinIO directly over TLS with a certificate only the
  # stack knows about.
  export E2E_MINIO_CA="$PWD/ca/public.crt"
  run_variant "$@"
) || status=1

# ---------------------------------------------------------------- variant C
# Variant A's policy with the maintenance kill switch on: every mutation is
# refused regardless of what the policy says.
(
  export E2E_NAME=proxy-readonly
  export E2E_PROXY_ENDPOINT=http://127.0.0.1:18399
  export E2E_ADMIN_ENDPOINT=http://127.0.0.1:18400
  export E2E_MINIO_ENDPOINT=http://127.0.0.1:19000
  export E2E_ACCESS_KEY_TEMPLATE='{tenant}{level}'
  export E2E_SECRET_TEMPLATE='{tenant}:{level}'
  export E2E_KEY_PREFIX_TEMPLATE='{tenant}/'
  export E2E_READ_LEVEL=ro
  export E2E_WRITE_LEVEL=rw
  export E2E_TENANT_A=a1b2c3d4e5f60718293a4b5c6d7e8f90
  export E2E_TENANT_B=0f9e8d7c6b5a49382716f5e4d3c2b1a0
  export E2E_READ_ONLY=true
  run_variant "$@"
) || status=1

echo
if [[ $status -eq 0 ]]; then
  echo "==> all variants passed"
else
  echo "==> at least one variant failed" >&2
fi
exit $status
