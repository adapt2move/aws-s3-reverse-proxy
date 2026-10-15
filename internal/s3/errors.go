package s3

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"net/http"
)

// WriteError answers with the XML error document S3 clients expect, so an SDK
// surfaces a real error code instead of an empty body.
//
// The message is the caller's to choose, and the caller is expected to keep it
// generic outside debug mode: a refused caller should learn that it was
// refused, not which check refused it.
func WriteError(w http.ResponseWriter, status int, code, message string) {
	var b bytes.Buffer
	b.WriteString(xml.Header)
	b.WriteString("<Error><Code>")
	xml.EscapeText(&b, []byte(code))
	b.WriteString("</Code><Message>")
	xml.EscapeText(&b, []byte(message))
	b.WriteString("</Message></Error>")

	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set("Content-Length", fmt.Sprint(b.Len()))
	w.WriteHeader(status)
	_, _ = w.Write(b.Bytes())
}

// errorFields are the elements of an S3 error document a client is given.
// Everything else an object store puts in one is diagnostic detail about the
// request *it* received — CanonicalRequest, StringToSign and their byte dumps,
// SignatureProvided, AWSAccessKeyId, ArgumentValue — which is the proxy's
// re-signed request, not the client's, and can quote its credentials and
// headers verbatim.
var errorFields = []string{"Code", "Message", "Key", "BucketName", "Resource", "RequestId", "HostId"}

// SanitizeErrorDocument rebuilds an S3 error document from the fields a
// client may see, in their usual order. A body that is not an <Error>
// document is returned unchanged; the caller redacts what it must from it.
func SanitizeErrorDocument(body []byte) []byte {
	var doc struct {
		XMLName xml.Name
		Fields  []struct {
			XMLName xml.Name
			Value   string `xml:",chardata"`
		} `xml:",any"`
	}
	if err := xml.Unmarshal(body, &doc); err != nil || doc.XMLName.Local != "Error" {
		return body
	}
	values := map[string]string{}
	for _, field := range doc.Fields {
		if _, seen := values[field.XMLName.Local]; !seen {
			values[field.XMLName.Local] = field.Value
		}
	}
	var b bytes.Buffer
	b.WriteString(xml.Header)
	b.WriteString("<Error>")
	for _, name := range errorFields {
		value, ok := values[name]
		if !ok {
			continue
		}
		fmt.Fprintf(&b, "<%s>", name)
		_ = xml.EscapeText(&b, []byte(value))
		fmt.Fprintf(&b, "</%s>", name)
	}
	b.WriteString("</Error>")
	return b.Bytes()
}
