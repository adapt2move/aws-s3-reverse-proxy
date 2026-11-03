FROM golang:1.25-alpine as build

# build-base (gcc + musl-dev) for cgo. No --platform=$BUILDPLATFORM: this stage
# runs natively per target platform (emulated under QEMU for arm64), so cgo
# builds against that platform's own musl instead of needing a cross-compiler.
RUN apk add -U --no-cache ca-certificates git bash build-base

WORKDIR /app
COPY . .

RUN CGO_ENABLED=1 go build -o aws-s3-reverse-proxy ./cmd/aws-s3-reverse-proxy && \
    mv ./aws-s3-reverse-proxy /go/bin

FROM alpine:3.24

WORKDIR /proxy

RUN addgroup -S proxygroup && adduser -S proxyuser -G proxygroup

USER proxyuser

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /go/bin/aws-s3-reverse-proxy /proxy

ENTRYPOINT [ "/proxy/aws-s3-reverse-proxy" ]
