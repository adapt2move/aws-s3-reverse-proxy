# Running on Kubernetes

The [README](../README.md) describes each feature on its own. This page puts
them together as a deployment: one manifest that runs all of them, then what
each part of it is for and what goes wrong when it is off. Every value below
is a flag or environment variable from the
[README's configuration](../README.md#configuration); nothing here changes
how the proxy behaves.

## A complete deployment

One replica, secrets from a mounted Secret, the policy from a ConfigMap, the
object cache on the node's local NVMe and the listing fallback on. Drop what
you do not use: without `CACHE_DIR` there is no cache, without
`LIST_FALLBACK` no fallback, and without `UPSTREAM_SSE_C_KEY_FILE` no
upstream encryption.

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: s3proxy
type: Opaque
stringData:
  upstream-credentials: "ACCESS_KEY_ID,SECRET_ACCESS_KEY"
  credential-pepper: "…"   # at least 16 bytes, shared with whatever issues client credentials
  cache-purge-token: "…"
  sse-c-key: "…"           # Base64 of 32 random bytes: openssl rand -base64 32
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: s3proxy-policy
data:
  policy.yaml: |
    # see policy.example.yaml
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: s3proxy
spec:
  replicas: 1                          # read "More than one replica" before raising this
  selector:
    matchLabels: {app: s3proxy}
  template:
    metadata:
      labels: {app: s3proxy}
    spec:
      terminationGracePeriodSeconds: 45  # SHUTDOWN_DELAY + SHUTDOWN_TIMEOUT, plus a margin
      securityContext:
        runAsNonRoot: true
        runAsUser: 65532                 # the image's user is named, not numeric; see below
        runAsGroup: 65532
        seccompProfile: {type: RuntimeDefault}
      containers:
        - name: s3proxy
          image: ghcr.io/adapt2move/aws-s3-reverse-proxy:<commit-sha>
          ports:
            - {name: s3, containerPort: 8099}
            - {name: admin, containerPort: 8100}
          env:
            - {name: POLICY_FILE, value: /etc/s3proxy/policy/policy.yaml}
            - {name: ALLOWED_ENDPOINT, value: "s3proxy.storage.svc.cluster.local:8099"}
            - {name: ALLOWED_SOURCE_SUBNET, value: "10.0.0.0/8"}
            - {name: UPSTREAM_ENDPOINT, value: "https://s3.eu-central-1.amazonaws.com"}
            - {name: UPSTREAM_REGION, value: eu-central-1}
            - {name: METRICS_LISTEN_ADDR, value: ":8100"}
            - {name: SHUTDOWN_DELAY, value: 10s}
            - {name: UPSTREAM_CREDENTIALS_FILE, value: /etc/s3proxy/secrets/upstream-credentials}
            - {name: CREDENTIAL_PEPPER_FILE, value: /etc/s3proxy/secrets/credential-pepper}
            - {name: CACHE_PURGE_TOKEN_FILE, value: /etc/s3proxy/secrets/cache-purge-token}
            - {name: UPSTREAM_SSE_C_KEY_FILE, value: /etc/s3proxy/secrets/sse-c-key}
            - {name: CACHE_DIR, value: /var/cache/s3proxy}
            - {name: CACHE_MAX_BYTES, value: "32212254720"}   # 30 GiB, a ceiling
            - {name: LIST_FALLBACK, value: "true"}
          readinessProbe:
            httpGet: {path: /readyz, port: admin}
            periodSeconds: 2
          livenessProbe:
            httpGet: {path: /healthz, port: admin}
            periodSeconds: 10
          resources:
            requests:
              cpu: 500m
              memory: 256Mi
              ephemeral-storage: 32Gi    # the cache, logs and the writable layer
            limits:
              memory: 1Gi
              ephemeral-storage: 34Gi
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities: {drop: [ALL]}
          volumeMounts:
            - {name: policy, mountPath: /etc/s3proxy/policy, readOnly: true}
            - {name: secrets, mountPath: /etc/s3proxy/secrets, readOnly: true}
            - {name: cache, mountPath: /var/cache/s3proxy}
      volumes:
        - name: policy
          configMap: {name: s3proxy-policy}
        - name: secrets
          secret: {secretName: s3proxy}
        - name: cache
          emptyDir: {sizeLimit: 31Gi}    # the cache ceiling plus a few percent
---
apiVersion: v1
kind: Service
metadata:
  name: s3proxy
spec:
  selector: {app: s3proxy}
  ports:
    - {name: s3, port: 8099, targetPort: s3}   # the S3 API only, never the admin port
---
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: s3proxy
spec:
  podSelector:
    matchLabels: {app: s3proxy}
  policyTypes: [Ingress]
  ingress:
    - ports: [{port: 8099}]            # narrow with `from` to where the clients are
    - from:
        - namespaceSelector:
            matchLabels: {kubernetes.io/metadata.name: monitoring}
      ports: [{port: 8100}]
```

`readOnlyRootFilesystem` is safe: the proxy writes nothing outside
`CACHE_DIR`. The image runs as a user called `proxyuser`, and the kubelet
cannot check a user given by name against `runAsNonRoot`, so the pod sets a
numeric one. Nothing in the image depends on which: the binary is readable
by everybody, an `emptyDir` is writable by everybody, and Secret and
ConfigMap files are readable by everybody unless `defaultMode` says otherwise.

Pin the image to a commit. Every build of `master` is published as
`ghcr.io/adapt2move/aws-s3-reverse-proxy:<commit-sha>`, and `latest` moves
with it.

## Secrets

Every secret has a `…_FILE` form, and a mounted Secret is what it is for: the
value never shows up in the pod spec or in `ps`. They are read **once, at
startup**. Rotating one means changing the Secret and restarting the pods,
for example with `kubectl rollout restart deployment/s3proxy`.

  * **`CREDENTIAL_PEPPER`** derives every client secret. Changing it changes
    every client credential at once, so roll it out together with whatever
    issues them.
  * **`UPSTREAM_SSE_C_KEY`** encrypts every object upstream. Keep a copy
    outside the cluster: without it, every object written with it is
    unreadable, for this proxy and for anybody else. There is no rotation:
    starting with another key makes the objects written under the old one
    unreadable. It protects the bucket only: with a cache, the cached copies
    sit unencrypted on the node, see [The object cache](#the-object-cache).
    See also [Upstream encryption](../README.md#upstream-encryption-sse-c).
  * **`CACHE_PURGE_TOKEN`** protects `POST /cache/purge`. Without it, anything
    that reaches the admin port can empty the cache.

## Policy

Mount the ConfigMap **as a directory** and point `POLICY_FILE` into it, as
above. A `subPath` mount never sees an update: the kubelet replaces a
ConfigMap by swapping a symlink in the directory, and a single file mounted
on its own is not behind that symlink.

An edit reaches the pod once the kubelet has synced the ConfigMap, which can
take about a minute, and the proxy picks it up at the next
`POLICY_RELOAD_INTERVAL` (`30s`). To apply it right away, send the process a
`SIGHUP`. It is PID 1 in the container:

```sh
kubectl exec deploy/s3proxy -- kill -HUP 1
```

A policy that fails validation is **rejected**, and the one loaded before it
keeps serving. The only sign is a log line at error level and
`s3proxy_policy_reloads_total{outcome="rejected"}`, which is worth an alert.
See [Hot reload](../README.md#hot-reload).

## Probes and rolling updates

`/healthz` answers as long as the process does; `/readyz` turns `503` the
moment the pod is told to stop. On `SIGTERM` the proxy fails readiness, keeps
serving for `SHUTDOWN_DELAY` so that Kubernetes takes the pod out of the
Service's endpoints, and then drains in-flight requests for up to
`SHUTDOWN_TIMEOUT` (`30s`).

`terminationGracePeriodSeconds` has to cover both, or the kubelet kills
downloads that were still draining. With `SHUTDOWN_DELAY=10s` and the default
timeout that is 40 seconds; the example leaves a margin and uses 45.

At startup, a pod with an object cache first clears its cache directory. On a
fresh pod there is nothing to clear. After a container restart inside the
same pod, clearing what the previous run left is instant with a few thousand
files and takes a few seconds with hundreds of thousands; the pod becomes
ready once it is done.

## Network

  * **`ALLOWED_ENDPOINT`** is the `Host` the clients' signatures cover, so it
    has to be exactly the host and port in their endpoint URL. Clients that
    reach the Service as `s3proxy.storage:8099` and others that use
    `s3proxy.storage.svc.cluster.local:8099` do not both work; pick one name
    for all of them.
  * **`ALLOWED_SOURCE_SUBNET`** is checked against the address of the
    connection itself. `X-Forwarded-For` is ignored, so behind an ingress
    controller or a service-mesh sidecar the address is the ingress pod's or
    the sidecar's, and the subnet has to include it.
  * **TLS** to the clients comes from `CERT_FILE` and `KEY_FILE`, mounted from
    a Secret (cert-manager writes one), or from whatever terminates TLS in
    front of the proxy. TLS to the object store is decided by the scheme of
    `UPSTREAM_ENDPOINT`, and it has to be `https` with an SSE-C key.
  * **The admin port** (8100 above) serves `/healthz`, `/readyz`, `/metrics`
    and, with a cache, `POST /cache/purge`, which empties the cache. Keep it
    off the S3 Service, and let only what needs it reach it: the
    NetworkPolicy above admits the monitoring namespace.

## Metrics

`METRICS_LISTEN_ADDR=:8100` puts `/metrics` on the admin listener next to the
probes. With the Prometheus operator:

```yaml
apiVersion: monitoring.coreos.com/v1
kind: PodMonitor
metadata:
  name: s3proxy
spec:
  selector:
    matchLabels: {app: s3proxy}
  podMetricsEndpoints:
    - {port: admin, path: /metrics}
```

Worth alerting on:

| Series | What it means |
| --- | --- |
| `s3proxy_policy_reloads_total{outcome="rejected"}` increasing | the policy file on disk is not the policy in force |
| `s3proxy_cache_store_failures_total` rising with `s3proxy_cache_stores_total` flat | the cache stopped taking objects: no room under the ceiling, or a disk that refuses writes |
| `s3proxy_cache_evictions_total` rising about as fast as `s3proxy_cache_stores_total` | every new object pushes an old one out: the cache is too small for what is being read |
| `s3proxy_cache_lookups_total{operation="ListObjects",result="fallback"}` increasing | listings are answered from memory: the object store is slow or failing |

`METRICS_TENANT_LABEL` adds the tenant to the authorization series. The number
of tenants is unbounded, and so is the number of series it creates; leave it
off unless the tenant count is known to be small.

## The object cache

The cache wants an `emptyDir` on the node's local NVMe. Every file in the
directory belongs to the cache, and it is emptied at every start.

**Cached objects are stored unencrypted, always**, whatever the object store
does to its own copy: SSE-S3, SSE-KMS or the proxy's SSE-C protect the bucket,
not the cache. An `emptyDir` lives on the node's disk, so anybody with access
to the node's filesystem can read every tenant's cached objects. Encrypt the
node's disk, or leave `CACHE_DIR` unset: without it the proxy keeps no object
data at rest.

### Sizing

**`CACHE_MAX_BYTES` is a ceiling.** The cache never has more than that on
disk: the entries, files that were evicted but are still being streamed to a
client, and writes in progress, which claim their length before their first
byte. Room is made by evicting before a write, never after it, and a write
that does not fit is simply not cached. How that is counted is in
[How the cache stores objects](cache-storage.md#the-ceiling).

When the disk runs short, the kubelet decides what happens, so these belong
together:

| | set it to | because |
| --- | --- | --- |
| `CACHE_MAX_BYTES` | the cache size you want | it is the ceiling the proxy enforces itself |
| `emptyDir.sizeLimit` | `CACHE_MAX_BYTES` plus a few percent | past it, the kubelet evicts the pod |
| `requests.ephemeral-storage` | at least `sizeLimit`, plus room for logs and the container's writable layer | under disk pressure, pods using more than they requested are evicted first |
| `limits.ephemeral-storage` | above the request | past it, the kubelet evicts the pod |

The few percent cover what the ceiling leaves out: about 1 MiB of
directories, and on XFS the speculative preallocation it can briefly give a
file that is still growing.

### Disk pressure on the node

Node disk pressure starts, by default, when the node filesystem has less than
10 % of its space or 5 % of its inodes left. The kubelet then evicts pods in
order: first those using more ephemeral storage than they requested, then by
priority, then by how far over their request they are. A cache pod whose
request does not cover its cache is the obvious first choice, and losing it
loses the proxy, not just the cache. With the request set as above, it is
among the last.

The proxy cannot see any of this from the inside. Asked for free space, the
filesystem reports the node's whole disk, shared with every other pod, while
`sizeLimit` and the ephemeral-storage limits are enforced by the kubelet
measuring usage. That is why the ceiling is configured rather than detected.

**Inodes count too.** Every cached object is a file, so a cache of many small
objects uses an inode for each, and the node's inode threshold can trip
before its space does. `CACHE_MAX_ENTRIES` bounds the number of files.

### Other volumes

An `emptyDir` is created writable by everybody and needs nothing. A
PersistentVolumeClaim or a hostPath is handed over owned by root, and the
proxy cannot create anything in it: it refuses to start rather than serve
without the cache it was told to use. Set `fsGroup` in the pod's security
context, matching `runAsGroup`. The cache is emptied at every start whatever
the volume, so a persistent one buys nothing.

### More than one replica

Each pod has its own cache, and a write through one pod does not invalidate
what another has cached. Read-after-write then only holds for a client that
reads through the pod it wrote through; through any other it can read the
previous version for up to `CACHE_MAX_AGE`. The README explains why, in
[More than one replica](../README.md#more-than-one-replica). On Kubernetes
that leaves three ways to run it:

  * **One replica.** The simplest, and the example above.
  * **Route by key.** A plain Service balances connections, not paths, so this
    needs an L7 proxy in front that hashes on the request path without its
    query string. With ingress-nginx that is
    `nginx.ingress.kubernetes.io/upstream-hash-by: "$uri"`; `$request_uri`
    would put an object's reads and writes on different pods, because their
    query strings differ.
  * **Accept the window**, and choose `CACHE_MAX_AGE` as the longest a client
    may read a stale object after writing through another pod.

A rolling update replaces every pod's cache with an empty one. Expect a burst
of reads against the object store while the new pods fill up.

**A purge only empties the pod it reaches.** To empty all of them, send it to
each pod from somewhere the NetworkPolicy admits:

```sh
for ip in $(kubectl get pods -l app=s3proxy -o jsonpath='{.items[*].status.podIP}'); do
  curl -fsS -X POST -H "Authorization: Bearer $CACHE_PURGE_TOKEN" "http://$ip:8100/cache/purge"
done
```

## Memory

Request and response bodies stream, so memory does not grow with object size.
What it does grow with:

| | up to |
| --- | --- |
| the listing fallback | `LIST_FALLBACK_MAX_BYTES` (64 MiB) |
| the cache's index | about 150 bytes plus the key, per cached object |
| an upload in `aws-chunked` encoding with a checksum trailer | `MAX_CHUNKED_BODY_SIZE` (64 MiB) per such request in flight |
| a listing or batch delete whose answer is rewritten | `MAX_REWRITE_BODY_SIZE` (32 MiB) per such request in flight |
| a `DeleteObjects` request body | `MAX_DELETE_BODY_SIZE` (2 MiB) per such request in flight |

Size the memory limit for the fixed part plus the per-request buffers times
how many of them run at once. The example's 1 GiB leaves room for the
fallback, an index of a few hundred thousand objects and several buffered
requests at once.

The page cache of the cache's files is charged to the pod as well. The kernel
reclaims it under pressure, so a warm cache that makes memory usage sit near
the limit is not a leak.
