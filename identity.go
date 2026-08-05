package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

// Identity is who a request is, derived entirely from its access-key id.
// Nothing about a tenant is stored or configured anywhere: the id carries
// the tenant and the level, the policy says how to read them, and the
// secret is recomputed on every request.
type Identity struct {
	// Tenant and Level as captured from the access-key id.
	Tenant string
	Level  string

	// KeyPrefix is prepended to every object key and listing prefix for
	// this identity. The client never sends it and never sees it.
	KeyPrefix string

	// SecretAccessKey is the derived secret the inbound signature is
	// verified against. It is never logged, never exported and never sent
	// upstream.
	SecretAccessKey string
}

// errUnknownAccessKeyID is returned for an access-key id that does not
// match the configured pattern. It carries no detail on purpose: telling a
// caller *why* their id was rejected hands them the id layout.
var errUnknownAccessKeyID = errors.New("unknown access key id")

// keyPrefixSafeBytes are the characters a rendered key prefix may consist
// of. Restricting it means the prefix is byte-identical in a URL's decoded
// and escaped form, so injecting it cannot desynchronise Path from RawPath
// (and cannot smuggle a query string or a path traversal into the upstream
// URL). The set is the RFC 3986 unreserved characters plus `/`.
func keyPrefixSafeByte(c byte) bool {
	switch {
	case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		return true
	case c == '-', c == '.', c == '_', c == '~', c == '/':
		return true
	}
	return false
}

// ResolveIdentity turns an access-key id into an Identity: match the
// configured pattern, render the key prefix and derive the secret.
//
// The secret is an HMAC — not a plain digest — because tenant identifiers
// are not secret. With an unkeyed sha256(tenant+level) anyone who learned a
// tenant id could compute that tenant's highest-privilege secret offline;
// with the pepper as the key, knowing the id buys nothing.
//
// The pepper is shared only between this proxy and whatever issues
// credentials to clients, so both sides derive independently: no credential
// store, no distribution problem, and rotating the pepper rotates every
// credential at once.
func (p *Policy) ResolveIdentity(accessKeyID string, pepper []byte) (*Identity, error) {
	if accessKeyID == "" {
		return nil, errUnknownAccessKeyID
	}
	match := p.accessKeyIDRe.FindStringSubmatch(accessKeyID)
	// The pattern has to match the id in full. Checking the match length
	// here rather than demanding `^…$` in the config means a pattern that
	// forgot its anchors cannot silently accept `evil-<validid>-suffix`.
	if match == nil || match[0] != accessKeyID {
		return nil, errUnknownAccessKeyID
	}

	values := make(map[string]string, len(match))
	for name, idx := range namedGroups(p.accessKeyIDRe) {
		values[name] = match[idx]
	}
	tenant, level := values["tenant"], values["level"]
	if tenant == "" || level == "" {
		// An id whose tenant or level captured empty would render a prefix
		// of another tenant's shape (or the bucket root). Fail closed.
		return nil, errUnknownAccessKeyID
	}
	if _, ok := p.levelSet[level]; !ok {
		return nil, errUnknownAccessKeyID
	}

	keyPrefix := renderTemplate(p.keyPrefixTemplate, values)
	if err := validateRenderedKeyPrefix(keyPrefix); err != nil {
		return nil, err
	}

	return &Identity{
		Tenant:          tenant,
		Level:           level,
		KeyPrefix:       keyPrefix,
		SecretAccessKey: deriveSecret(pepper, renderTemplate(p.secretTemplate, values)),
	}, nil
}

// validateRenderedKeyPrefix guards the one value in the request path that
// comes from client-controlled input (the access-key id) and ends up in the
// upstream URL. The pattern normally constrains the tenant capture to
// something harmless, but a permissive pattern must not be able to turn
// into a traversal or a URL-structure injection.
func validateRenderedKeyPrefix(prefix string) error {
	if prefix == "" {
		return fmt.Errorf("rendered key prefix is empty")
	}
	if strings.HasPrefix(prefix, "/") {
		return fmt.Errorf("rendered key prefix must not start with %q", "/")
	}
	for _, segment := range strings.Split(prefix, "/") {
		if segment == ".." || segment == "." {
			return fmt.Errorf("rendered key prefix must not contain %q or %q segments", ".", "..")
		}
	}
	for i := 0; i < len(prefix); i++ {
		if !keyPrefixSafeByte(prefix[i]) {
			return fmt.Errorf("rendered key prefix contains unsupported character %q", prefix[i])
		}
	}
	return nil
}

// deriveSecret computes the client's secret access key:
//
//	secret = hex(HMAC-SHA256(pepper, render(secretTemplate)))
//
// The hex encoding is part of the contract with the credential issuer: the
// same pepper and the same message must produce the same 64-character
// string on both sides, or every signature check fails.
func deriveSecret(pepper []byte, message string) string {
	mac := hmac.New(sha256.New, pepper)
	mac.Write([]byte(message))
	return hex.EncodeToString(mac.Sum(nil))
}
