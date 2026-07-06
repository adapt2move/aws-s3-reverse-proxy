# AWS S3 Reverse Proxy

The `aws-s3-reverse-proxy` will reverse-proxy all incoming S3 API calls to the public
AWS S3 backend by rewriting the Host header and re-signing the original request.

Possible use cases and scenarios include:
  * Auditing & logging of S3 access from your local network or specific clients
  * Redirecting S3 buckets to a different AWS Region
  * AWS DirectConnect, to run a reverse-proxy from your local network

AWS uses its [Signature
v4]((https://docs.aws.amazon.com/general/latest/gr/signature-version-4.html)) to
protect API access. All requests sent to AWS S3 have to have a valid signature
which includes your AWS security credentials (commonly known as
`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`). This signature is encoded in
the HTTP Authorization header and also contains a SHA256 of various other HTTP
headers, including the `Host` header, which directly corresponds to the
`endpoint_url`. Since we want to reverse-proxy requests, we need to rewrite this
header, and therefore need to re-sign the request.

In order to re-sign any Signature v4 request, we need the full set of AWS
security credentials, the `AWS_ACCESS_KEY_ID` is already part of the
`Authorization`, but the `AWS_SECRET_ACCESS_KEY` needs to be provided as
configuration option to the reverse proxy.

The `aws-s3-reverse-proxy` is NOT capable of reverse-proxying arbitrary AWS S3
requests from unknown (or unaware) users & clients. The Signature v4 system put
in place by AWS requires the full knowledge of the AWS security credentials to
change specific HTTP headers. This means every deployment of
`aws-s3-reverse-proxy` needs to be aware of the expected AWS security
credentials to re-sign each request.

## Key Prefix, Separate Upstream Credentials, Region, and Read-Only Mode

Six optional flags extend the basic re-signing behaviour:

  * `--key-prefix` (env `KEY_PREFIX`): a string prepended to every object
    key sent upstream. `GET /bucket/key` becomes `GET /bucket/<prefix>key`.
    For bucket-level listings the prefix is prepended to the request's
    `prefix` query parameter instead, so a client cannot enumerate keys
    outside the configured prefix.
  * `--upstream-credentials` (env `UPSTREAM_CREDENTIALS`): an
    `"AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY"` pair used to re-sign
    upstream requests instead of the client's credentials. When set, the
    client only needs a credential the proxy recognises for the incoming
    signature check — it never needs the real upstream secret.
  * `--upstream-region` (env `UPSTREAM_REGION`): the region to sign
    upstream requests for, instead of the region from the client's
    request. Useful when the client signs with a placeholder (or empty)
    region but the real backend expects a specific one.
  * `--read-only` (env `READ_ONLY`): when set to `true`, only GET and HEAD
    requests are proxied. Every mutating method (PUT, POST, DELETE, PATCH)
    is rejected immediately with HTTP 403 — before the request is signed or
    forwarded — regardless of the credentials it carries. This is an
    upstream-credential-independent safety boundary: even a fully valid
    write request never reaches S3.
  * `--read-only-key-prefix` (env `READ_ONLY_KEY_PREFIX`): like
    `--read-only`, but scoped to one or more object-key prefixes instead of
    the whole bucket. A mutating request (PUT, POST, DELETE, PATCH) whose
    object key starts with one of the listed prefixes is rejected with HTTP
    403 — before it is signed or forwarded — while reads and writes to
    other keys are proxied as usual. Prefixes are matched against the
    client-facing object key, i.e. before any `--key-prefix` is prepended.
    Bucket-level requests carry no object key and are not affected.

    To protect **several** prefixes, repeat the flag, or — when using the
    environment variable — separate them with newlines (the env var holds a
    newline-separated list, the same convention as `ALLOWED_SOURCE_SUBNET`
    and `AWS_CREDENTIALS`):

    ```sh
    # CLI: repeat the flag
    aws-s3-reverse-proxy \
      --read-only-key-prefix protected/ \
      --read-only-key-prefix locked/

    # Env (shell): newline-separated
    export READ_ONLY_KEY_PREFIX=$'protected/\nlocked/'
    ```

    ```yaml
    # Env (docker-compose): a YAML block scalar keeps it readable
    environment:
      READ_ONLY_KEY_PREFIX: |-
        protected/
        locked/
    ```
  * `--deny-key-prefix` (env `DENY_KEY_PREFIX`): like
    `--read-only-key-prefix`, but stricter. Every request — **read and
    write** — whose object key starts with one of the listed prefixes is
    rejected with HTTP 403 (before it is signed or forwarded), and matching
    keys are **hidden from bucket listings**: their `<Contents>` and
    `<CommonPrefixes>` entries are stripped from LIST responses so a client
    cannot even discover that they exist. Like `--read-only-key-prefix`,
    prefixes are matched against the client-facing object key (before any
    `--key-prefix` is prepended), bucket-level requests themselves are never
    denied (only their listings are filtered), and the flag can be repeated —
    or, via the environment variable, separated with newlines:

    ```sh
    # CLI: repeat the flag
    aws-s3-reverse-proxy \
      --deny-key-prefix hidden/ \
      --deny-key-prefix internal/

    # Env (shell): newline-separated
    export DENY_KEY_PREFIX=$'hidden/\ninternal/'
    ```

All six default to off; without them the proxy behaves exactly as
before.

### Combining `--key-prefix` and `--read-only-key-prefix`

These two flags operate on the same object key but in **different layers**, and
the order matters. For every incoming request the proxy:

1. **first** checks `--read-only-key-prefix` against the **client-facing** key
   — the key exactly as the client sent it, *before* any `--key-prefix` is
   applied — and rejects a mutating request with HTTP 403 if it matches;
2. **then**, for requests that are allowed through, prepends `--key-prefix` to
   the object key before signing and forwarding upstream.

```
client PUT /bucket/protected/x      [1] read-only check     [2] key-prefix prepend
        └── key: protected/x   ───►  matches "protected/"  ───►  (never reached) ──► 403
client PUT /bucket/data/y           matches nothing        ───►  /bucket/tenants/acme/data/y ──► S3
        └── key: data/y
```

The practical consequence: **write `--read-only-key-prefix` in client terms,
not in upstream terms.** You do not (and must not) repeat the `--key-prefix` in
it. For example, with:

```sh
aws-s3-reverse-proxy \
  --key-prefix tenants/acme/ \
  --read-only-key-prefix protected/
```

| Client request (PUT) | Read-only match against | Result | Upstream path (if allowed) |
| --- | --- | --- | --- |
| `/bucket/protected/x` | `protected/x` → matches `protected/` | **403** | — |
| `/bucket/data/y` | `data/y` → no match | proxied | `/bucket/tenants/acme/data/y` |

> **Note:** this write protection is a property of *this proxy instance*, not of
> the bucket. It confines what *this* proxy will forward; it is not a substitute
> for an S3 bucket policy or IAM permissions. A different client reaching the
> same upstream bucket directly — or through another proxy configured with a
> different (or no) `--key-prefix` — is not affected by it.

`--deny-key-prefix` follows the same layering: it is checked against the
client-facing key first (rejecting reads and writes alike, and hiding matching
keys from listings), and only allowed requests then have `--key-prefix`
prepended. The same instance-scoped caveat applies — it is not a bucket policy.

## Releases

Get the latest Docker image from [from
DockerHub](https://hub.docker.com/r/thomaskriechbaumer/aws-s3-reverse-proxy/tags)
or download the source release [from
GitHub](https://github.com/Kriechi/aws-s3-reverse-proxy/releases).

## Features

  * can reverse-proxy to a configurable AWS Region
  * limits access based on source IP and subnet of the client
  * limits access based on endpoint URL
  * full instrumentation with Prometheus metrics
  * HTTP and HTTPS support for clients
  * uses secure HTTPS for upstream connections by default
  * run as single binary or Docker container
  * configuration via CLI, or using the same options in a config file
  * read-only mode to block all mutating requests before they reach S3
  * per-prefix read-only mode to protect selected object-key prefixes from writes
  * per-prefix deny mode to block all access to selected object-key prefixes and hide them from listings

## Getting Started

The proxy uses the official AWS SDK for Go for most of the heavy lifting. More
information can be found in the [developer
guide](https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/configuring-sdk.html)

The available options and help information can be displayed with:
```
docker run --rm aws-s3-reverse-proxy --help
```

## Build
All build dependencies and steps are contained in the `Dockerfile`:
```
docker build -t aws-s3-reverse-proxy .
```

## Run

### Server Examples
```
$ docker run --rm -ti \
  -p 8099 \
  aws-s3-reverse-proxy
  --allowed-source-subnet=192.168.1.0/24
  --allowed-endpoint=my.host.example.com:8099
  --aws-credentials=AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY
```

You can also use env variables
```
$ docker run --rm -ti \
  -p 8099 \
  -e ALLOWED_SOURCE_SUBNET=192.168.1.0/24 \
  -e ALLOWED_ENDPOINT=my.host.example.com:8099 \
  -e AWS_CREDENTIALS=AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY
  aws-s3-reverse-proxy
```

Or you can use a config file:
```
# config.cfg file for aws-3-reverse-proxy
--allowed-source-subnet=192.168.1.0/24
--allowed-endpoint=my.host.example.com:8099
--aws-credentials=AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY
```
And then run it like:
```
$ docker run --rm -it -v $(pwd)/config.cfg:/config.cfg -p 8099 aws-s3-reverse-proxy @config.cfg -v
```

Or just run the binary the old-fashioned way:
```
./aws-s3-reverse-proxy --help
```

### Client Examples

Client with the [official awscli](https://aws.amazon.com/cli/):
```
$ aws s3 --endpoint-url http://my.host.example.com:8099 ls s3://my-bucket/
```

## Contributing

`aws-s3-reverse-proxy` welcomes contributions from anyone! Unlike many other
projects we are happy to accept cosmetic contributions and small contributions,
in addition to large feature requests and changes.

## License

`aws-s3-reverse-proxy` is made available under the MIT License. For more
details, see the `LICENSE` file in the repository.

## Authors

`aws-s3-reverse-proxy` was created by Thomas Kriechbaumer, and is maintained
by the community.
