// Package sigv4 is the AWS Signature Version 4 half of the proxy: verifying
// the signature a client arrived with, and attaching the one the upstream
// expects.
//
// Verification is deliberately two-phase — ReadCredential first, Verify after
// — because that is the shape of the problem: an Authorization header names
// the access-key id it was signed with, and the secret to check it against
// can only be resolved once that id is known. Splitting the two keeps every
// notion of *who* an access-key id belongs to out of this package.
package sigv4

import (
	"crypto/subtle"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	v4 "github.com/aws/aws-sdk-go/aws/signer/v4"
)

// EmptyPayloadSHA256 is the SigV4 payload hash of a zero-length body.
const EmptyPayloadSHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

// UnsignedPayload tells the upstream that the payload hash is not part of the
// signature. It is what a body forwarded as a stream — and whose digest the
// proxy therefore never sees — is signed with.
const UnsignedPayload = "UNSIGNED-PAYLOAD"

// hexSHA256Regexp matches a literal payload hash as a client sends it in
// x-amz-content-sha256.
var hexSHA256Regexp = regexp.MustCompile(`^[0-9a-f]{64}$`)

// IsPayloadHash reports whether a x-amz-content-sha256 value is a literal
// digest, as opposed to one of the STREAMING-… or UNSIGNED-PAYLOAD markers.
// A literal one can be reused for the upstream signature; a marker cannot.
func IsPayloadHash(value string) bool { return hexSHA256Regexp.MatchString(value) }

// The Credential field of an Authorization header is
// `<access-key-id>/<date>/<region>/s3/aws4_request`.
//
// The access-key-id capture is deliberately permissive. Its *shape* is the
// policy's business — anything this regexp excluded would be a layout an
// operator could configure but never authenticate with. Only the two
// characters that would make the header ambiguous are excluded: `/` separates
// the credential's own fields, and `,` separates the header's.
//
// The region capture is likewise loose, for compatibility across providers:
//   - east-eu-1 => pass (aws style)
//   - gra => pass (ceph style)
//   - "" => pass (some S3 clients, e.g. DuckDB's httpfs, leave the region
//     segment empty when no region is configured; the signature is still
//     valid, it just has an empty region scope)
var credentialRegexp = regexp.MustCompile(`Credential=([^/,\s]+)/[0-9]+/([a-zA-Z-0-9]*)/s3/aws4_request`)
var signedHeadersRegexp = regexp.MustCompile("SignedHeaders=([a-zA-Z0-9;-]+)")

// Credential is what an Authorization header claims, before anything about it
// has been checked: which key signed the request, for which region scope, and
// when.
type Credential struct {
	AccessKeyID string
	Region      string
	SignedAt    time.Time
}

// Verifier checks inbound signatures against a host and a clock.
type Verifier struct {
	// Host is the value the client is expected to have signed as its Host
	// header — the endpoint this proxy accepts requests for.
	Host string

	// MaxClockSkew bounds how far an X-Amz-Date may be from this clock in
	// either direction. It is what stops a captured signature from being
	// replayed indefinitely.
	MaxClockSkew time.Duration
}

// ReadCredential parses the Authorization and X-Amz-Date headers and checks
// the timestamp against the accepted skew. It verifies nothing about the
// signature itself: the secret needed for that can only be resolved once the
// access-key id it returns is known.
func (v *Verifier) ReadCredential(req *http.Request) (Credential, error) {
	if len(req.Header["X-Amz-Date"]) != 1 {
		return Credential{}, fmt.Errorf("X-Amz-Date header missing or set multiple times")
	}
	if len(req.Header["Authorization"]) != 1 {
		return Credential{}, fmt.Errorf("Authorization header missing or set multiple times")
	}
	match := credentialRegexp.FindStringSubmatch(req.Header["Authorization"][0])
	if len(match) != 3 {
		return Credential{}, fmt.Errorf("invalid Authorization header: Credential not found")
	}

	signedAt, err := time.Parse("20060102T150405Z", req.Header["X-Amz-Date"][0])
	if err != nil {
		return Credential{}, fmt.Errorf("malformed X-Amz-Date")
	}
	// A signature stays valid forever unless its timestamp is bounded, so a
	// captured Authorization header could be replayed at will.
	if skew := time.Since(signedAt); skew > v.MaxClockSkew || skew < -v.MaxClockSkew {
		return Credential{}, fmt.Errorf("X-Amz-Date is outside the accepted clock skew of %s", v.MaxClockSkew)
	}
	return Credential{AccessKeyID: match[1], Region: match[2], SignedAt: signedAt}, nil
}

// Verify recomputes the client's own Authorization header from `secret` and
// compares the two in constant time.
//
// The request body is never read: whatever payload hash the client signed
// travels in x-amz-content-sha256, which is among the signed headers, and the
// signer uses that header verbatim when it is present. Verification therefore
// costs nothing in memory, no matter how large the upload is.
func (v *Verifier) Verify(req *http.Request, cred Credential, secret string) error {
	signer := v4.NewSigner(credentials.NewStaticCredentialsFromCreds(credentials.Value{
		AccessKeyID:     cred.AccessKeyID,
		SecretAccessKey: secret,
	}))
	// Match how an S3 client signs: the canonical URI is the request path
	// exactly as written on the wire, not that path escaped again.
	signer.DisableURIPathEscaping = true

	expected, err := v.reconstruct(signer, req, cred)
	if err != nil {
		return err
	}

	// WORKAROUND S3CMD which dont use white space before the some commas in
	// the authorization header.
	presented := strings.Replace(req.Header["Authorization"][0], ",Signature", ", Signature", 1)
	presented = strings.Replace(presented, ",SignedHeaders", ", SignedHeaders", 1)

	if subtle.ConstantTimeCompare([]byte(expected.Header.Get("Authorization")), []byte(presented)) == 0 {
		// Deliberately no request dump here: it would carry the client's
		// Authorization header — and therefore its signature — into the log
		// at debug level.
		return fmt.Errorf("invalid signature in Authorization header")
	}
	return nil
}

// reconstruct rebuilds the canonical request the client signed and signs it
// with the derived secret, so the two Authorization headers can be compared.
func (v *Verifier) reconstruct(signer *v4.Signer, req *http.Request, cred Credential) (*http.Request, error) {
	// req.URL.String() round-trips the path in exactly the form the client
	// wrote it, which is the form it signed.
	fake, err := http.NewRequest(req.Method, req.URL.String(), nil)
	if err != nil {
		return nil, err
	}

	// We already validated that there is exactly one Authorization header.
	match := signedHeadersRegexp.FindStringSubmatch(req.Header.Get("authorization"))
	if len(match) == 2 {
		for _, header := range strings.Split(match[1], ";") {
			fake.Header.Set(header, req.Header.Get(header))
		}
	}

	// Delete a potentially double-added header
	fake.Header.Del("host")
	fake.Host = v.Host

	if _, err := signer.Sign(fake, nil, "s3", cred.Region, cred.SignedAt); err != nil {
		return nil, err
	}
	return fake, nil
}

// Signer attaches the upstream signature — the one made with the credentials
// only this process holds, which clients never see.
type Signer struct {
	inner *v4.Signer
}

// NewSigner builds a signer for the upstream credentials.
func NewSigner(accessKeyID, secretAccessKey string) *Signer {
	inner := v4.NewSigner(credentials.NewStaticCredentialsFromCreds(credentials.Value{
		AccessKeyID:     accessKeyID,
		SecretAccessKey: secretAccessKey,
	}))
	// S3 does not escape the canonical URI a second time. Without this the
	// signature would cover a path that differs from the one on the wire for
	// every key containing a character that needs escaping — a space, a `+`,
	// a non-ASCII byte.
	inner.DisableURIPathEscaping = true
	return &Signer{inner: inner}
}

// Sign signs req for a region, in place.
//
// The caller must have set X-Amz-Content-Sha256 already: the signer adopts
// that digest rather than reading the body to compute one, and that read is
// what would otherwise pull an entire upload into memory.
//
// The body survives the call. The AWS SDK's signer assigns Request.Body from
// whatever body it was handed — and it is handed none here, precisely so that
// it does not read the real one — which would leave the request with a
// ContentLength and no body at all.
func (s *Signer) Sign(req *http.Request, region string, at time.Time) error {
	body, length := req.Body, req.ContentLength
	_, err := s.inner.Sign(req, nil, "s3", region, at)
	req.Body, req.ContentLength = body, length
	return err
}
