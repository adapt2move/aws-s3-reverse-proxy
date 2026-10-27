# Listing fallback

Set `--list-fallback` and a listing the object store does not answer is still
answered: with the last answer the object store gave to the same listing. It
is meant for an outage of the object store — a client that lists before it
reads keeps working for as long as the objects it reads are in the cache, or
are not needed.

It is a fallback and nothing else. A listing **always** goes to the object
store first, and the remembered answer is only used when that request:

  * has not been answered within `--list-fallback-timeout` (`1s`) — the whole
    answer, body included, or
  * failed with a server error (`5xx`), was throttled (`429`, `408`), or never
    got an answer at all — the connection was refused, broke, or timed out.

A listing cut off at the deadline is not abandoned: it runs to the end in the
background, and its answer becomes the remembered one. An object store that is
healthy but slower than the deadline therefore still keeps the fallback
current, one listing behind. At most four such listings run at once; one that
finds no slot is cancelled.

Any other refusal (`4xx`) is the object store answering, and is passed on. It also
drops what was remembered for that listing: the bucket may be gone or the
credentials changed, and the remembered answer describes a state the object
store no longer agrees with. A listing nothing was remembered for is not cut
off at the deadline; it waits for the object store exactly as it would without
the fallback.

A listing answered this way carries `X-Cache: FALLBACK` and an `Age` header
with how old the answer is, and is logged as a warning with the reason.

## Configuration

Off unless `--list-fallback` is set, and independent of `--cache-dir`: the
fallback holds listings in memory, not objects on disk.

| Flag (env) | Default | Meaning |
| --- | --- | --- |
| `--list-fallback` (`LIST_FALLBACK`) | `false` | remember the last answer to each listing, and serve it when the object store does not answer |
| `--list-fallback-timeout` (`LIST_FALLBACK_TIMEOUT`) | `1s` | how long a listing with a remembered answer waits for the object store before that answer is served instead |
| `--list-fallback-max-age` (`LIST_FALLBACK_MAX_AGE`) | `1h` | how old a remembered listing may be and still be served |
| `--list-fallback-max-bytes` (`LIST_FALLBACK_MAX_BYTES`) | 64 MiB | how much memory remembered listings may occupy |
| `--list-fallback-refresh-delay` (`LIST_FALLBACK_REFRESH_DELAY`) | `1s` | how long writes under a prefix have to settle before the listings they dropped are listed again in the background; `0s` disables |

## What keeps it consistent

A listing summarizes every key under its prefix, so any write under that
prefix changes the answer. Four rules keep a remembered listing from ever
contradicting what a client was told through this proxy:

  * **A write drops every listing that would show it** — any `PutObject`,
    `DeleteObject`, `DeleteObjects` or `CompleteMultipartUpload` under the
    listing's prefix, and the in-place encryption of a legacy object — before
    the client that made the write is told it succeeded. A listing remembered
    from before a write can therefore not be served after it was
    acknowledged.
  * **A listing that overlapped a write is not remembered.** If a write under
    its prefix was in flight at any point while the listing was, which side of
    the write the object store answered from is not something the proxy can
    know, so that answer is dropped and the one before it stays only if the
    write turns out not to have happened.
  * **A write that did not happen changes nothing.** One the object store
    refused (`4xx`), or one that never reached it (the connection could not be
    made), keeps the remembered listings — an outage is exactly when writes
    fail, and dropping the fallback for every failed write would take it away
    when it is needed. A `5xx` may have come after the object changed, and
    drops them.
  * **A write nobody got an answer to stays claimed.** When the connection
    breaks after the request was sent, or the client gives up, the object
    store may still apply the write later — a multipart completion of a large
    object takes minutes. Such a write drops the listings that would show it
    and keeps its keys claimed for `--list-fallback-max-age`: no listing under
    them is remembered in that time, and nothing is refreshed, since either
    could capture the state from before the write landed. A write whose body
    never reached its end — a client that aborted an upload halfway — is not
    one of these: no object store applies part of a request, so it counts as
    refused.

What is remembered is the object store's own response, before the tenant
prefix is stripped out and before entries are filtered by access level. A
remembered listing goes through the same rewrite a live one does, with the
policy in force when it is served — so `ro` served a listing remembered from an
`rws` request still does not see the carve-out only `rws` may read. The key is
the upstream request, tenant prefix included, so two tenants listing the same
prefix never share an answer.

## Keeping it warm under writes

Dropping a listing on every write keeps the fallback consistent, and on its
own it would leave a prefix that is written to often without one most of the
time. So the listings a write dropped are listed again in the background, once
writes under them have settled for `--list-fallback-refresh-delay`:

```
PUT abc/new.csv ──▶ acknowledged ──▶ listing of abc dropped
                                      │  (more writes under abc: wait again,
                                      │   at most 10× the delay in total)
                                      ▼
                      LIST abc upstream, in the background
                                      │
                 ┌────────────────────┼─────────────────────────┐
             answered            a write overlapped it      failed / refused
                 │                    │                         │
            remembered          tried again later,         left dropped: the
                                further apart each time    next client listing
                                                           remembers it
```

The refreshed answer is the object store's, and it is captured exactly like a
client's listing: a write that overlaps it keeps it from being remembered.
Nothing is ever patched locally to match a write — the proxy cannot know what
the object store would list (pagination, `delimiter`, `LastModified`, writes
it did not see), and a listing it assembled itself would be wrong in exactly
the cases nobody can check during an outage.

Three bounds keep it cheap, and none of it delays shutdown: closing the proxy
cancels what is still running. A burst of writes under one prefix costs one
listing after the last of them. Only listings a client asked for within
`--list-fallback-max-age` are refreshed, and a refresh does not count as being
asked for, so a listing nobody uses stops being kept current. And at most four
refreshes run at a time. Pages reached through a `continuation-token` are not
refreshed: the token belongs to the listing as it was, and the refreshed first
page hands out a new one.

## What it cannot see

Writes that do not pass through this process: another replica, another
client, a lifecycle rule. A listing served from memory can miss those for up
to `--list-fallback-max-age`, which is why that bound exists and why the
fallback is off by default. With more than one replica, each one remembers and
invalidates its own listings only.

Nothing is persisted: a restart starts with no remembered listings, which is
also the conservative answer to "what did I miss while I was down". Purging the
cache (`POST /cache/purge`) empties the fallback too.

## Observability

Every answer from the fallback is logged as a warning with the reason, and
carries `cache=fallback` in the access log. The series below appear only when
the fallback is configured.

| Metric | |
| --- | --- |
| `s3proxy_cache_lookups_total{operation="ListObjects",result="fallback"}` | listings answered from memory |
| `s3proxy_list_fallback_entries`, `s3proxy_list_fallback_bytes`, `s3proxy_list_fallback_capacity_bytes` | what the fallback holds |
| `s3proxy_list_fallback_stores_total`, `s3proxy_list_fallback_store_failures_total` | listings remembered, and the ones that were not because a write overlapped them |
| `s3proxy_list_fallback_refreshes_total` | listings remembered after a background refresh of what a write dropped |
| `s3proxy_list_fallback_invalidations_total` | remembered listings dropped because a write may have changed them |
