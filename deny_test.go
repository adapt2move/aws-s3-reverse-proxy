package main

import (
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Pure-function tests of filterDeniedKeysFromListBody — fast, no network.

func TestFilterDeniedKeysFromListBody_DisabledWhenEmpty(t *testing.T) {
	body := []byte(`<Contents><Key>hidden/x</Key></Contents>`)
	assert.Equal(t, body, filterDeniedKeysFromListBody(body, nil))
}

func TestFilterDeniedKeysFromListBody_DropsMatchingContents(t *testing.T) {
	body := []byte(
		`<Contents><Key>hidden/secret.csv</Key><Size>1</Size></Contents>` +
			`<Contents><Key>public/report.csv</Key><Size>2</Size></Contents>`,
	)
	got := string(filterDeniedKeysFromListBody(body, []string{"hidden/"}))
	assert.Equal(t, `<Contents><Key>public/report.csv</Key><Size>2</Size></Contents>`, got)
}

func TestFilterDeniedKeysFromListBody_DropsUrlEncodedContents(t *testing.T) {
	// S3 URL-encodes keys when the request carries EncodingType=url. The
	// denied prefix "hidden/" appears as "hidden%2F" in that form.
	body := []byte(
		`<Contents><Key>hidden%2Fsecret.csv</Key></Contents>` +
			`<Contents><Key>public%2Freport.csv</Key></Contents>`,
	)
	got := string(filterDeniedKeysFromListBody(body, []string{"hidden/"}))
	assert.Equal(t, `<Contents><Key>public%2Freport.csv</Key></Contents>`, got)
}

func TestFilterDeniedKeysFromListBody_DropsMatchingCommonPrefixes(t *testing.T) {
	body := []byte(
		`<CommonPrefixes><Prefix>hidden/sub/</Prefix></CommonPrefixes>` +
			`<CommonPrefixes><Prefix>public/sub/</Prefix></CommonPrefixes>`,
	)
	got := string(filterDeniedKeysFromListBody(body, []string{"hidden/"}))
	assert.Equal(t, `<CommonPrefixes><Prefix>public/sub/</Prefix></CommonPrefixes>`, got)
}

func TestFilterDeniedKeysFromListBody_MultiplePrefixes(t *testing.T) {
	body := []byte(
		`<Contents><Key>hidden/a</Key></Contents>` +
			`<Contents><Key>secret/b</Key></Contents>` +
			`<Contents><Key>public/c</Key></Contents>`,
	)
	got := string(filterDeniedKeysFromListBody(body, []string{"hidden/", "secret/"}))
	assert.Equal(t, `<Contents><Key>public/c</Key></Contents>`, got)
}

func TestFilterDeniedKeysFromListBody_LeavesTopLevelPrefixUntouched(t *testing.T) {
	// The bucket-level <Prefix> element (echo of the request prefix) lives
	// outside <CommonPrefixes> and must never be dropped — only the
	// enumerating blocks are filtered.
	body := []byte(`<ListBucketResult><Prefix>hidden/</Prefix><KeyCount>0</KeyCount></ListBucketResult>`)
	got := string(filterDeniedKeysFromListBody(body, []string{"hidden/"}))
	assert.Equal(t, string(body), got)
}

func TestFilterDeniedKeysFromListBody_KeepsNonMatchingBoundary(t *testing.T) {
	// "hiddenfile" shares the bytes of "hidden" but not the "hidden/"
	// boundary, so it is a distinct key and must be kept.
	body := []byte(`<Contents><Key>hiddenfile.csv</Key></Contents>`)
	got := string(filterDeniedKeysFromListBody(body, []string{"hidden/"}))
	assert.Equal(t, string(body), got)
}

func TestFilterDeniedKeysFromListBody_FullListBucketResult(t *testing.T) {
	body := []byte(`<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>bucket</Name>
  <Prefix></Prefix>
  <Contents>
    <Key>hidden/secret.csv</Key>
    <Size>10</Size>
  </Contents>
  <Contents>
    <Key>public/report.csv</Key>
    <Size>20</Size>
  </Contents>
  <CommonPrefixes>
    <Prefix>hidden/sub/</Prefix>
  </CommonPrefixes>
  <CommonPrefixes>
    <Prefix>public/sub/</Prefix>
  </CommonPrefixes>
</ListBucketResult>`)
	got := string(filterDeniedKeysFromListBody(body, []string{"hidden/"}))
	assert.NotContains(t, got, "hidden/secret.csv", "denied object must be hidden")
	assert.NotContains(t, got, "hidden/sub/", "denied common-prefix must be hidden")
	assert.Contains(t, got, "<Key>public/report.csv</Key>", "allowed object must remain")
	assert.Contains(t, got, "<Prefix>public/sub/</Prefix>", "allowed common-prefix must remain")
	assert.Contains(t, got, "<Name>bucket</Name>", "unrelated elements untouched")
}

func TestKeyValueUnderDenyPrefix(t *testing.T) {
	assert.True(t, keyValueUnderDenyPrefix([]byte("hidden/x"), []string{"hidden/"}))
	assert.True(t, keyValueUnderDenyPrefix([]byte("hidden%2Fx"), []string{"hidden/"}))
	assert.False(t, keyValueUnderDenyPrefix([]byte("hiddenfile"), []string{"hidden/"}))
	assert.False(t, keyValueUnderDenyPrefix([]byte("public/x"), []string{"hidden/"}))
	assert.False(t, keyValueUnderDenyPrefix([]byte("anything"), nil))
}

// The deny filter runs together with the KeyPrefix strip: matching must be
// against the CLIENT-FACING key, so a key like tenants/acme/hidden/x that is
// first stripped to hidden/x is then correctly hidden.
func TestModifyResponse_DenyFilterAfterStrip(t *testing.T) {
	h := &Handler{KeyPrefix: "tenants/acme/", DenyKeyPrefixes: []string{"hidden/"}}
	body := `<ListBucketResult>
  <Contents><Key>tenants/acme/hidden/secret.csv</Key></Contents>
  <Contents><Key>tenants/acme/public/report.csv</Key></Contents>
</ListBucketResult>`
	resp := &http.Response{
		Header:  http.Header{"Content-Type": []string{"application/xml"}},
		Body:    io.NopCloser(strings.NewReader(body)),
		Request: &http.Request{URL: &url.URL{Path: "/bucket/"}},
	}
	require.NoError(t, h.modifyResponse(resp))
	got, _ := io.ReadAll(resp.Body)
	// Denied key gone; allowed key present and stripped to client form.
	assert.NotContains(t, string(got), "secret.csv")
	assert.Contains(t, string(got), "<Key>public/report.csv</Key>")
	assert.NotContains(t, string(got), "tenants/acme/")
	// Content-Length reflects the rewritten (smaller) body.
	assert.Equal(t, strconv.Itoa(len(got)), resp.Header.Get("Content-Length"))
	assert.Equal(t, int64(len(got)), resp.ContentLength)
}

// The deny filter works even without a KeyPrefix — modifyResponse must not
// early-return just because KeyPrefix is empty.
func TestModifyResponse_DenyFilterWithoutKeyPrefix(t *testing.T) {
	h := &Handler{DenyKeyPrefixes: []string{"hidden/"}}
	body := `<ListBucketResult>
  <Contents><Key>hidden/secret.csv</Key></Contents>
  <Contents><Key>public/report.csv</Key></Contents>
</ListBucketResult>`
	resp := &http.Response{
		Header:  http.Header{"Content-Type": []string{"application/xml"}},
		Body:    io.NopCloser(strings.NewReader(body)),
		Request: &http.Request{URL: &url.URL{Path: "/bucket/"}},
	}
	require.NoError(t, h.modifyResponse(resp))
	got, _ := io.ReadAll(resp.Body)
	assert.NotContains(t, string(got), "hidden/secret.csv")
	assert.Contains(t, string(got), "<Key>public/report.csv</Key>")
}

func TestModifyResponse_DenySkipsNonXml(t *testing.T) {
	// A non-XML object payload that merely contains the denied prefix as
	// text must never be rewritten.
	h := &Handler{DenyKeyPrefixes: []string{"hidden/"}}
	resp := &http.Response{
		Header:  http.Header{"Content-Type": []string{"text/csv"}},
		Body:    io.NopCloser(strings.NewReader("hidden/secret,1\n")),
		Request: &http.Request{URL: &url.URL{Path: "/bucket/public/x.csv"}},
	}
	require.NoError(t, h.modifyResponse(resp))
	got, _ := io.ReadAll(resp.Body)
	assert.Equal(t, "hidden/secret,1\n", string(got))
}
