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
