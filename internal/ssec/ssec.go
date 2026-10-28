// Package ssec is the one customer-provided key (SSE-C) this proxy hands the
// object store, and the headers that carry it.
//
// The key is operator configuration: it is loaded once at startup and never
// comes from a client. Everything here is written so that the key cannot
// leave the process except in the three request headers S3 defines for it —
// a Key has no exported field, formats as a placeholder, and nothing in this
// package returns the raw bytes.
package ssec

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"
)

// The request headers that carry a customer key, and the response headers an
// object store uses to confirm it applied one. Canonical spelling throughout,
// so they can be used as http.Header map keys directly.
const (
	HeaderAlgorithm = "X-Amz-Server-Side-Encryption-Customer-Algorithm"
	HeaderKey       = "X-Amz-Server-Side-Encryption-Customer-Key"
	HeaderKeyMD5    = "X-Amz-Server-Side-Encryption-Customer-Key-Md5"

	// Algorithm is the only one S3 defines.
	Algorithm = "AES256"

	// KeySize is the length of an AES-256 key in bytes.
	KeySize = 32
)

// customerHeaderPrefixes cover the three headers above and their copy-source
// counterparts, which name a key for the *source* of a CopyObject.
var customerHeaderPrefixes = []string{
	"X-Amz-Server-Side-Encryption-Customer-",
	"X-Amz-Copy-Source-Server-Side-Encryption-Customer-",
}

// IsCustomerHeader reports whether a header belongs to the SSE-C family, in
// either its plain or its copy-source form.
func IsCustomerHeader(name string) bool {
	canonical := http.CanonicalHeaderKey(name)
	for _, prefix := range customerHeaderPrefixes {
		if strings.HasPrefix(canonical, prefix) {
			return true
		}
	}
	return false
}

// HasCustomerHeader reports whether any header of the family is present, with
// any value — an empty one included, since a client that sends the header at
// all is asking for something this proxy does not do.
func HasCustomerHeader(header http.Header) bool {
	for name := range header {
		if IsCustomerHeader(name) {
			return true
		}
	}
	return false
}

// StripCustomerHeaders removes every SSE-C header. It is for responses on
// their way to a client, which never asked for encryption and has no business
// learning which key fingerprint the object store confirmed.
func StripCustomerHeaders(header http.Header) {
	for name := range header {
		if IsCustomerHeader(name) {
			delete(header, name)
		}
	}
}

// Key is a validated 256-bit customer key, ready to be sent.
//
// The encoded forms are precomputed because they go on every object request;
// the raw bytes are kept only long enough to derive them.
type Key struct {
	encoded    string
	encodedMD5 string

	// spellings matches every form the key or its digest could take in a
	// document an object store sends back: Base64, percent-encoded Base64
	// with escapes in either case, the hex of the raw key, and the hex of
	// the Base64 text, which is how S3 renders a canonical request in a
	// SignatureDoesNotMatch error.
	spellings *regexp.Regexp
}

// Parse validates the Base64 form of a key: standard alphabet, padded, and
// exactly 32 bytes once decoded. Surrounding whitespace is the caller's to
// trim — a key file is read and trimmed by the configuration loader, not here.
//
// The error never contains the value, so it can be logged as is.
func Parse(encoded string) (*Key, error) {
	if encoded == "" {
		return nil, errors.New("the key is empty")
	}
	raw, err := base64.StdEncoding.Strict().DecodeString(encoded)
	if err != nil {
		return nil, errors.New("the key is not valid standard Base64")
	}
	if len(raw) != KeySize {
		return nil, fmt.Errorf("the key decodes to %d bytes, it must be exactly %d", len(raw), KeySize)
	}
	sum := md5.Sum(raw)
	k := &Key{
		encoded:    base64.StdEncoding.EncodeToString(raw),
		encodedMD5: base64.StdEncoding.EncodeToString(sum[:]),
	}
	var alternatives []string
	for _, secret := range []string{k.encoded, k.encodedMD5} {
		var escaped strings.Builder
		for _, c := range secret {
			switch c {
			case '+':
				escaped.WriteString(`(?:\+|%(?i:2b))`)
			case '/':
				escaped.WriteString(`(?:/|%(?i:2f))`)
			case '=':
				escaped.WriteString(`(?:=|%(?i:3d))`)
			default:
				escaped.WriteString(regexp.QuoteMeta(string(c)))
			}
		}
		alternatives = append(alternatives, escaped.String(), `(?i:`+hex.EncodeToString([]byte(secret))+`)`)
	}
	alternatives = append(alternatives, `(?i:`+hex.EncodeToString(raw)+`)`)
	k.spellings = regexp.MustCompile(strings.Join(alternatives, "|"))
	clear(raw)
	return k, nil
}

// Redacted is what Redact puts where key material was.
const Redacted = "[redacted]"

// Redact replaces every spelling of the key and of its digest in b. It is
// for documents the object store sends back — an error that echoes the
// request it refused can quote the key verbatim — and costs a scan per
// document, which is nothing next to the round trip that produced it.
func (k *Key) Redact(b []byte) []byte {
	return k.spellings.ReplaceAllLiteral(b, []byte(Redacted))
}

// Apply sets the three request headers that ask the object store to encrypt
// or decrypt with this key. The caller signs the request afterwards, so they
// are covered by the upstream signature like every other x-amz- header.
func (k *Key) Apply(header http.Header) {
	header.Set(HeaderAlgorithm, Algorithm)
	header.Set(HeaderKey, k.encoded)
	header.Set(HeaderKeyMD5, k.encodedMD5)
}

// Remove deletes the headers Apply set, for a request that has to be sent
// again without them.
func Remove(header http.Header) {
	header.Del(HeaderAlgorithm)
	header.Del(HeaderKey)
	header.Del(HeaderKeyMD5)
}

// Confirmation is what a response says about the encryption it was asked for.
type Confirmation int

const (
	// Confirmed means the response names AES256 and, when it names a key
	// digest at all, ours.
	Confirmed Confirmation = iota
	// Unconfirmed means the response carries no SSE-C confirmation.
	Unconfirmed
	// Mismatched means the response confirms SSE-C, but with another
	// algorithm or another key's digest.
	Mismatched
)

func (c Confirmation) String() string {
	switch c {
	case Confirmed:
		return "confirmed"
	case Unconfirmed:
		return "unconfirmed"
	default:
		return "mismatched"
	}
}

// Check reads a response's SSE-C confirmation against this key.
func (k *Key) Check(header http.Header) Confirmation {
	algorithm := header.Get(HeaderAlgorithm)
	digest := header.Get(HeaderKeyMD5)
	if algorithm == "" && digest == "" {
		return Unconfirmed
	}
	if algorithm != Algorithm {
		return Mismatched
	}
	if digest != "" && digest != k.encodedMD5 {
		return Mismatched
	}
	return Confirmed
}

// String and GoString keep the key out of anything that formats a value it
// happens to be reachable from — a %v of a config struct, a debug dump.
func (k *Key) String() string   { return "ssec.Key(redacted)" }
func (k *Key) GoString() string { return k.String() }
