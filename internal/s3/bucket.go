package s3

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"net/http"
)

// WriteBucketLocation writes a GetBucketLocation answer for region. us-east-1
// is the empty constraint, which is how S3 itself spells it and what the
// SDKs expect.
func WriteBucketLocation(w http.ResponseWriter, region string) {
	if region == "us-east-1" {
		region = ""
	}
	var b bytes.Buffer
	b.WriteString(xml.Header)
	b.WriteString(`<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
	xml.EscapeText(&b, []byte(region))
	b.WriteString("</LocationConstraint>")

	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set("Content-Length", fmt.Sprint(b.Len()))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(b.Bytes())
}
