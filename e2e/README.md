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

Four things are invisible to a suite with a stub upstream, and all four are
places this proxy can be wrong in production while passing every unit test:

1. **A real object store verifies our signatures.** The proxy re-signs every
   request with its own credentials. A stub never checks that; MinIO does.
2. **A real object store answers the way a real one does** — compressing
   responses, minting its own pagination tokens, enforcing multipart part
   sizes, and minting ETags nobody else gets to choose. Each of those has
   already caught a bug here.
3. **A real client is not a hand-built request.** The suite drives the AWS
   SDK, which signs, retries, checksums and runs multipart the way production
   workloads do.
4. **A real bucket can be written to behind the proxy's back.** That is the
   one inconsistency the local cache cannot detect, so it is the one the
   suite has to stage for real — see `proxy-cache` below.

Every assertion that matters is made twice: once against what the client
sees, and once against what is actually in the bucket, read with root
credentials straight from MinIO. A proxy that answers correctly while writing
to the wrong key passes the first check and fails the second.

## The variants

The suite is parameterised entirely by environment. The same test binary runs
against deployments that agree on almost nothing, which is what demonstrates
that no tenant, level name or key layout is baked into the proxy:

| | `proxy-http` | `proxy-tls` | `proxy-readonly` | `proxy-cache` |
| --- | --- | --- | --- | --- |
| upstream | `http://minio:9000` | `https://minio-tls:9000`, private CA | `http://minio:9000` | `http://minio:9000` |
| access-key id | `<32 hex><level>` | `<tenant>-<level>` | `<32 hex><level>` | `<32 hex><level>` |
| levels | `ro` `rw` `rws` | `reader` `writer` `admin` | `ro` `rw` `rws` | `ro` `rw` `rws` |
| key prefix | `{tenant}/` | `tenants/{tenant}/data/` | `{tenant}/` | `{tenant}/` |
| secret template | `{tenant}:{level}` | `v1/{level}/{tenant}` | `{tenant}:{level}` | `{tenant}:{level}` |
| secrets from | environment | mounted files | environment | environment |
| local cache | off | off | off | on, 3s maximum age, 8 MiB per object |
| extras | hot reload, 192 MiB memory cap | — | `--read-only` | 192 MiB memory cap, restartable with its cache |

The rule *paths* are deliberately identical across variants, so the same
authorization assertions apply to each.

Running the whole suite against `proxy-cache` is most of what tests the cache.
A cache that answered a request policy would have refused, or that handed one
tenant another tenant's object, fails `TestTenantIsolation` and
`TestPolicyEnforcement` rather than a test written to go looking for it. Its
maximum age is three seconds rather than the ten-minute default so that expiry
and revalidation are testable without the suite sleeping through a coffee
break, and its cache refuses anything over 8 MiB — so
`TestLargeObjectStreamsThrough` also demonstrates that an object the cache
declines still streams rather than being buffered on its way past.

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
| `TestCacheAnswersTheSecondRead` | a warm read does not reach the object store |
| `TestACachedUploadReportsTheObjectStoresETag` | the ETag recorded for an upload is the one MinIO gives the object |
| `TestCacheRevalidatesAnExpiredEntry` | expiry checks rather than re-transfers |
| `TestAnExternalWriteIsPickedUpAfterTheMaximumAge` | a write behind the proxy's back is served stale, and only until then |
| `TestPurgeMakesTheCacheForgetEverything` | the escape hatch for that write |
| `TestARangedReadIsAnsweredFromTheCache` | a 206 out of a cached object, byte for byte |
| `TestARangedMissFillsTheCacheInTheBackground` | a client that only reads ranges still fills the cache |
| `TestAnObjectTooLargeForTheCacheIsStillServed` | the cache declining an object changes nothing else |
| `TestACachedObjectIsStillSubjectToPolicy` | a warm cache is not a way around a level that may not read it |
| `TestOneTenantsWarmCacheIsAnothersMiss` | isolation holds for a warm cache, not just a cold one |
| `TestTheCacheSurvivesARestart` | a real process really stopped, and recovered its disk |
| `TestCacheMetricsAreExposed` | the cache series, with no key or credential in them |

`TestLargeObjectStreamsThrough` is why `proxy-http` and `proxy-cache` run
under `mem_limit`. A proxy that buffered the upload would be OOM-killed, so
the test fails as a dropped connection rather than as a subtly wrong answer —
a far better check of "memory is independent of object size" than reading a
heap profile.

The cache tests skip on a deployment without a cache, so the same binary runs
against all four variants. `run-local.sh` covers the caching variant too,
including the restart, since a restart of a local process needs no container
boundary.

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
| `E2E_CACHE_MAX_AGE` | marks a caching deployment and enables every cache test; must match its `--cache-max-age` |
| `E2E_CACHE_MAX_OBJECT_SIZE` | its `--cache-max-object-size`, so a test can pick a size on either side of it |
| `E2E_CACHE_RESTART_CMD` | restarts the proxy with its cache intact; without it the restart test skips |

The suite is behind the `e2e` build tag, so `go test ./...` never picks it up.
