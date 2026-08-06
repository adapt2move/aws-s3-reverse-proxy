# End-to-end suite

Real S3 clients, talking to a real `aws-s3-reverse-proxy`, in front of a real
MinIO. The unit suite checks what the proxy *decides*; this one checks what
actually reaches the object store — and what the object store makes of it.

```sh
./e2e/run.sh                      # containerized, all variants
./e2e/run.sh -run TestTenantIsolation

MINIO_BINARY=./minio ./e2e/run-local.sh   # no Docker daemon needed
```

## Why it exists

Three things are invisible to a suite with a stub upstream, and all three are
places this proxy can be wrong in production while passing every unit test:

1. **A real object store verifies our signatures.** The proxy re-signs every
   request with its own credentials. A stub never checks that; MinIO does.
2. **A real object store answers the way a real one does** — compressing
   responses, minting its own pagination tokens, enforcing multipart part
   sizes. Each of those has already caught a bug here.
3. **A real client is not a hand-built request.** The suite drives the AWS
   SDK, which signs, retries, checksums and runs multipart the way production
   workloads do.

Every assertion that matters is made twice: once against what the client
sees, and once against what is actually in the bucket, read with root
credentials straight from MinIO. A proxy that answers correctly while writing
to the wrong key passes the first check and fails the second.

## The variants

The suite is parameterised entirely by environment. The same test binary runs
against deployments that agree on almost nothing, which is what demonstrates
that no tenant, level name or key layout is baked into the proxy:

| | `proxy-http` | `proxy-tls` | `proxy-readonly` |
| --- | --- | --- | --- |
| upstream | `http://minio:9000` | `https://minio-tls:9000`, private CA | `http://minio:9000` |
| access-key id | `<32 hex><level>` | `<tenant>-<level>` | `<32 hex><level>` |
| levels | `ro` `rw` `rws` | `reader` `writer` `admin` | `ro` `rw` `rws` |
| key prefix | `{tenant}/` | `tenants/{tenant}/data/` | `{tenant}/` |
| secret template | `{tenant}:{level}` | `v1/{level}/{tenant}` | `{tenant}:{level}` |
| secrets from | environment | mounted files | environment |
| extras | hot reload, 192 MiB memory cap | — | `--read-only` |

The rule *paths* are deliberately identical across variants, so the same
authorization assertions apply to each.

## What each test is for

| Test | What would be broken without it |
| --- | --- |
| `TestObjectLifecycle` | the everyday path, and where the bytes land |
| `TestTenantIsolation` | two tenants asking for one key get one object |
| `TestCrossTenantEscapeAttempts` | foreign keys, forged ids, level escalation, traversal |
| `TestPolicyEnforcement` | levels, the implicit deny, the nested carve-out |
| `TestMultipartUpload` | a genuine multi-part flow, reassembled byte for byte |
| `TestPaginationAcrossContinuationTokens` | the decision to forward MinIO's tokens verbatim |
| `TestBatchDeletePerKeyAuthorization` | per-key results, and that refused keys survive |
| `TestAwsChunkedUpload` | the store receives the object, not the chunk framing |
| `TestUnsupportedOperationsAreRefused` | copy, bucket admin, presigned, anonymous |
| `TestHealthAndMetrics` | probes and metrics, with no credential in them |
| `TestPolicyHotReload` | a bad edit changes nothing, a good one takes effect |
| `TestLargeObjectStreamsThrough` | 256 MiB through a 192 MiB container |

That last one is why `proxy-http` runs under `mem_limit`. A proxy that
buffered the upload would be OOM-killed, so the test fails as a dropped
connection rather than as a subtly wrong answer — a far better check of
"memory is independent of object size" than reading a heap profile.

## Configuration

Every variable the suite reads:

| | |
| --- | --- |
| `E2E_PROXY_ENDPOINT` | required; unset skips the whole suite |
| `E2E_ADMIN_ENDPOINT` | health and metrics; skips those tests if unset |
| `E2E_MINIO_ENDPOINT`, `E2E_MINIO_ACCESS_KEY`, `E2E_MINIO_SECRET_KEY` | direct access, for checking the bucket |
| `E2E_MINIO_CA` | PEM for a private CA, when MinIO is reached over TLS |
| `E2E_BUCKET`, `E2E_REGION` | default `e2e`, `eu-central-1` |
| `E2E_PEPPER` | must match the deployment's `CREDENTIAL_PEPPER` |
| `E2E_ACCESS_KEY_TEMPLATE`, `E2E_SECRET_TEMPLATE`, `E2E_KEY_PREFIX_TEMPLATE` | mirror the policy file's `identity` block |
| `E2E_READ_LEVEL`, `E2E_WRITE_LEVEL` | level names, by the role the suite needs |
| `E2E_TENANT_A`, `E2E_TENANT_B` | must match the deployment's `accessKeyIdPattern` |
| `E2E_POLICY_FILE`, `E2E_POLICY_TIGHTENED`, `E2E_POLICY_INVALID`, `E2E_RELOAD_INTERVAL` | enable the hot-reload test |
| `E2E_READ_ONLY` | the deployment runs with the kill switch on |
| `E2E_LARGE_OBJECT_SIZE` | enables the streaming test |

The suite is behind the `e2e` build tag, so `go test ./...` never picks it up.
