package s3

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"net/http"
	"time"
)

// WriteCopyObjectResult answers a successful CopyObject. etag is the new
// object's, in header syntax; date is the HTTP date the object store accepted
// it at, and stands in for its Last-Modified the way it does for a PutObject.
func WriteCopyObjectResult(w http.ResponseWriter, etag, date string) {
	modified, err := http.ParseTime(date)
	if err != nil {
		modified = time.Now()
	}
	var b bytes.Buffer
	b.WriteString(xml.Header)
	b.WriteString(`<CopyObjectResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><LastModified>`)
	b.WriteString(modified.UTC().Format("2006-01-02T15:04:05.000Z"))
	b.WriteString("</LastModified><ETag>")
	_ = xml.EscapeText(&b, []byte(etag))
	b.WriteString("</ETag></CopyObjectResult>")

	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set("Content-Length", fmt.Sprint(b.Len()))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(b.Bytes())
}
