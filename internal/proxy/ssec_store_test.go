package proxy

import (
	"bytes"
	"crypto/md5"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Kriechi/aws-s3-reverse-proxy/internal/ssec"
)

// sseStore is an object store that does SSE-C the way the pinned MinIO does —
// which is the behaviour the proxy has to cope with — behind real TLS.
//
// The parts that matter, each checked against MinIO RELEASE.2025-09-07:
//
//   - A read of a plaintext object *with* SSE-C headers is refused, 400
//     InvalidRequest "The encryption parameters are not applicable to this
//     object", rather than answered.
//   - A read of an encrypted object without a key is 400 InvalidRequest, and
//     with the wrong key 403 AccessDenied.
//   - A precondition on a write that carries SSE-C headers is evaluated by
//     reading the existing object *with* that key, so over a plaintext object
//     it fails with the same 400 — which is why the repair completes its
//     upload without the key.
//   - HEAD errors have no body.
//   - CreateMultipartUpload does not echo the customer-key headers; only
//     PutObject, UploadPart and reads do.
//
// The switches below turn it into the stores the proxy also has to survive.
type sseStore struct {
	t      *testing.T
	server *httptest.Server

	mu       sync.Mutex
	objects  map[string]*sseObject
	uploads  map[string]*sseUpload
	nextID   int
	requests []recordedRequest

	// ignoreSSEC makes it a store without SSE-C: the headers are accepted
	// and ignored, nothing is ever encrypted, nothing is ever confirmed.
	ignoreSSEC bool

	// quirkyETags answers every ETag as `"'value'"` and refuses — with a
	// loud 400 — any validator or part list that sends a single quote back.
	quirkyETags bool

	// literalWriteIfMatch models Ceph RGW's comparison of a write's
	// If-Match against the bare stored ETag, without removing double quotes.
	literalWriteIfMatch bool

	// noConditionalComplete answers 501 to a completion carrying If-Match,
	// like a store that cannot replace an object conditionally.
	noConditionalComplete bool

	// beforeComplete runs, outside the lock, just before a completion is
	// evaluated: the window in which another writer can get in.
	beforeComplete func()

	// beforePart runs, outside the lock, before a part upload is handled.
	beforePart func()

	// failPart answers 500 to the upload of this part number.
	failPart int

	// refuseReads answers every GET and HEAD of an object with a 400 that
	// is not the plaintext refusal, carrying this message.
	refuseReads string

	// aclStatus, when set, is the answer to every GET ?acl.
	aclStatus int

	// echoSignatureErrors answers every request 403 SignatureDoesNotMatch
	// with the request it received quoted back, headers and all — which is
	// what S3 does, and what puts a customer key into an error document.
	echoSignatureErrors bool

	// uploadOwner is the owner ListParts names for an upload; "owner-1",
	// the owner of every object's default ACL, unless set.
	uploadOwner string

	// fail answers 500 to every request with this method.
	fail string
}

type sseObject struct {
	data     []byte
	header   http.Header
	keyMD5   string // empty: stored in plaintext
	etag     string // canonical
	modified time.Time
	tags     int

	// grants are extra ACL grants beyond the owner's full control, as XML.
	grants string
}

type sseUpload struct {
	key    string
	keyMD5 string
	header http.Header
	parts  map[int][]byte
	etags  map[int]string
}

func newSSEStore(t *testing.T) *sseStore {
	s := &sseStore{t: t, objects: map[string]*sseObject{}, uploads: map[string]*sseUpload{}}
	s.server = httptest.NewTLSServer(http.HandlerFunc(s.serve))
	t.Cleanup(s.server.Close)
	return s
}

func (s *sseStore) rootCAs() *x509.CertPool {
	pool := x509.NewCertPool()
	pool.AddCert(s.server.Certificate())
	return pool
}

func (s *sseStore) host() string { return strings.TrimPrefix(s.server.URL, "https://") }

// put stores an object directly, the way a client writing to the store behind
// the proxy's back would. key is the upstream key, tenant prefix included.
func (s *sseStore) put(key string, data []byte, keyMD5 string, header http.Header) *sseObject {
	s.mu.Lock()
	defer s.mu.Unlock()
	obj := &sseObject{data: data, header: header, keyMD5: keyMD5, etag: s.newETag(data, keyMD5), modified: s.clock()}
	if obj.header == nil {
		obj.header = http.Header{}
	}
	s.objects[key] = obj
	return obj
}

func (s *sseStore) object(key string) *sseObject {
	s.mu.Lock()
	defer s.mu.Unlock()
	if obj, ok := s.objects[key]; ok {
		copied := *obj
		return &copied
	}
	return nil
}

func (s *sseStore) openUploads() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.uploads)
}

func (s *sseStore) recorded() []recordedRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]recordedRequest(nil), s.requests...)
}

// requestsWhere filters the recorded requests.
func (s *sseStore) requestsWhere(match func(recordedRequest) bool) []recordedRequest {
	var out []recordedRequest
	for _, r := range s.recorded() {
		if match(r) {
			out = append(out, r)
		}
	}
	return out
}

// clock moves forward a second per write, so Last-Modified tells writes apart.
func (s *sseStore) clock() time.Time {
	return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(s.nextID) * time.Second)
}

func (s *sseStore) newETag(data []byte, keyMD5 string) string {
	s.nextID++
	if keyMD5 == "" {
		sum := md5.Sum(data)
		return hex.EncodeToString(sum[:])
	}
	// Like S3 and MinIO, the ETag of an SSE-C object is not the MD5 of its
	// bytes.
	random := make([]byte, 16)
	_, _ = rand.Read(random)
	return hex.EncodeToString(random)
}

func (s *sseStore) spell(etag string) string {
	if s.quirkyETags {
		return `"'` + etag + `'"`
	}
	return `"` + etag + `"`
}

func (s *sseStore) serve(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	recordedBody := body
	if len(recordedBody) > 4096 {
		recordedBody = recordedBody[:4096]
	}

	s.mu.Lock()
	s.requests = append(s.requests, recordedRequest{
		method: r.Method, path: r.URL.Path, query: r.URL.Query(), header: r.Header.Clone(), body: string(recordedBody),
	})
	hook, partHook := s.beforeComplete, s.beforePart
	s.mu.Unlock()

	query := r.URL.Query()
	_, isComplete := query["uploadId"]
	if r.Method == http.MethodPost && isComplete && hook != nil {
		hook()
	}
	if r.Method == http.MethodPut && query.Has("partNumber") && partHook != nil {
		partHook()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.fail == r.Method {
		s.fault(w, r, http.StatusInternalServerError, "InternalError", "We encountered an internal error.")
		return
	}
	if s.echoSignatureErrors {
		var canonical strings.Builder
		fmt.Fprintf(&canonical, "%s\n%s\n", r.Method, r.URL.Path)
		for _, name := range []string{"X-Amz-Date", ssec.HeaderAlgorithm, ssec.HeaderKey, ssec.HeaderKeyMD5} {
			fmt.Fprintf(&canonical, "%s:%s\n", strings.ToLower(name), r.Header.Get(name))
		}
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusForbidden)
		fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>SignatureDoesNotMatch</Code>`+
			`<Message>The request signature we calculated does not match the signature you provided.</Message>`+
			`<AWSAccessKeyId>UPSTREAMKEYID</AWSAccessKeyId><StringToSign>AWS4-HMAC-SHA256</StringToSign>`+
			`<CanonicalRequest>%s</CanonicalRequest><CanonicalRequestBytes>%x</CanonicalRequestBytes>`+
			`<Resource>%s</Resource><RequestId>R1</RequestId></Error>`, canonical.String(), canonical.String(), r.URL.Path)
		return
	}
	if s.quirkyETags {
		for _, name := range []string{"If-Match", "If-None-Match", "If-Range"} {
			if strings.Contains(r.Header.Get(name), "'") {
				s.fault(w, r, http.StatusBadRequest, "InvalidArgument", "single-quoted entity tag in "+name)
				return
			}
		}
	}

	parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/"), "/", 2)
	if len(parts) != 2 || parts[1] == "" {
		w.WriteHeader(http.StatusOK)
		return
	}
	key := parts[1]

	keyMD5, ok := s.customerKey(w, r)
	if !ok {
		return
	}

	switch {
	case r.Method == http.MethodPost && query.Has("uploads"):
		s.createUpload(w, r, key, keyMD5)
	case r.Method == http.MethodPut && query.Has("uploadId"):
		s.uploadPart(w, r, query, keyMD5, body)
	case r.Method == http.MethodPost && query.Has("uploadId"):
		s.completeUpload(w, r, key, query.Get("uploadId"), keyMD5, body)
	case r.Method == http.MethodDelete && query.Has("uploadId"):
		delete(s.uploads, query.Get("uploadId"))
		w.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodGet && query.Has("uploadId"):
		s.listParts(w, r, query.Get("uploadId"))
	case r.Method == http.MethodGet && query.Has("acl"):
		s.getACL(w, r, key)
	case r.Method == http.MethodPut:
		s.putObject(w, r, key, keyMD5, body)
	case r.Method == http.MethodGet, r.Method == http.MethodHead:
		s.getObject(w, r, key, keyMD5)
	case r.Method == http.MethodDelete:
		delete(s.objects, key)
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// customerKey validates the request's SSE-C headers and returns the key's
// digest, empty when it carries none.
func (s *sseStore) customerKey(w http.ResponseWriter, r *http.Request) (string, bool) {
	algorithm := r.Header.Get(ssec.HeaderAlgorithm)
	key := r.Header.Get(ssec.HeaderKey)
	digest := r.Header.Get(ssec.HeaderKeyMD5)
	if algorithm == "" && key == "" && digest == "" {
		return "", true
	}
	if s.ignoreSSEC {
		return "", true
	}
	raw, err := base64.StdEncoding.DecodeString(key)
	sum := md5.Sum(raw)
	if algorithm != "AES256" || err != nil || len(raw) != 32 || base64.StdEncoding.EncodeToString(sum[:]) != digest {
		s.fault(w, r, http.StatusBadRequest, "InvalidArgument", "Requests specifying Server Side Encryption with Customer provided keys must provide a valid encryption algorithm.")
		return "", false
	}
	if r.URL.Scheme == "http" || r.TLS == nil {
		s.fault(w, r, http.StatusBadRequest, "InvalidRequest", "Requests specifying Server Side Encryption with Customer provided keys must be made over a secure connection.")
		return "", false
	}
	return digest, true
}

// readable decides whether a request may see an object, the way MinIO does.
func (s *sseStore) readable(w http.ResponseWriter, r *http.Request, obj *sseObject, keyMD5 string) bool {
	switch {
	case obj.keyMD5 == "" && keyMD5 != "":
		s.fault(w, r, http.StatusBadRequest, "InvalidRequest", "The encryption parameters are not applicable to this object.")
		return false
	case obj.keyMD5 != "" && keyMD5 == "":
		s.fault(w, r, http.StatusBadRequest, "InvalidRequest", "The object was stored using a form of Server Side Encryption. The correct parameters must be provided to retrieve the object.")
		return false
	case obj.keyMD5 != keyMD5:
		s.fault(w, r, http.StatusForbidden, "AccessDenied", "Access Denied.")
		return false
	}
	return true
}

func (s *sseStore) confirm(w http.ResponseWriter, keyMD5 string) {
	if keyMD5 != "" {
		w.Header().Set(ssec.HeaderAlgorithm, "AES256")
		w.Header().Set(ssec.HeaderKeyMD5, keyMD5)
	}
}

// precondition evaluates If-Match / If-None-Match against an object.
func (s *sseStore) precondition(r *http.Request, obj *sseObject) (int, bool) {
	if v := r.Header.Get("If-Match"); v != "" {
		if obj == nil {
			return http.StatusNotFound, false
		}
		match := v
		if !s.literalWriteIfMatch || r.Method == http.MethodGet || r.Method == http.MethodHead {
			match = strings.Trim(v, `"`)
		}
		if v != "*" && match != obj.etag {
			return http.StatusPreconditionFailed, false
		}
	}
	if v := r.Header.Get("If-None-Match"); v != "" && obj != nil {
		if v == "*" || strings.Trim(v, `"`) == obj.etag {
			if r.Method == http.MethodGet || r.Method == http.MethodHead {
				return http.StatusNotModified, false
			}
			return http.StatusPreconditionFailed, false
		}
	}
	return 0, true
}

func (s *sseStore) putObject(w http.ResponseWriter, r *http.Request, key, keyMD5 string, body []byte) {
	existing := s.objects[key]
	if r.Header.Get("If-Match") != "" || r.Header.Get("If-None-Match") != "" {
		if existing != nil && keyMD5 != "" && !s.readable(w, r, existing, keyMD5) {
			return
		}
		if status, ok := s.precondition(r, existing); !ok {
			s.fault(w, r, status, "PreconditionFailed", "At least one of the pre-conditions you specified did not hold")
			return
		}
	}
	obj := &sseObject{data: body, header: storedHeaders(r.Header), keyMD5: keyMD5, etag: s.newETag(body, keyMD5), modified: s.clock()}
	s.objects[key] = obj
	w.Header().Set("Etag", s.spell(obj.etag))
	s.confirm(w, keyMD5)
	w.WriteHeader(http.StatusOK)
}

func (s *sseStore) getObject(w http.ResponseWriter, r *http.Request, key, keyMD5 string) {
	if s.refuseReads != "" {
		s.fault(w, r, http.StatusBadRequest, "InvalidArgument", s.refuseReads)
		return
	}
	obj := s.objects[key]
	if obj == nil {
		s.fault(w, r, http.StatusNotFound, "NoSuchKey", "The specified key does not exist.")
		return
	}
	if !s.readable(w, r, obj, keyMD5) {
		return
	}
	if status, ok := s.precondition(r, obj); !ok {
		w.Header().Set("Etag", s.spell(obj.etag))
		if status == http.StatusNotModified {
			w.WriteHeader(status)
			return
		}
		s.fault(w, r, status, "PreconditionFailed", "At least one of the pre-conditions you specified did not hold")
		return
	}
	for name, values := range obj.header {
		w.Header()[name] = values
	}
	if obj.tags > 0 {
		w.Header().Set("X-Amz-Tagging-Count", strconv.Itoa(obj.tags))
	}
	w.Header().Set("Etag", s.spell(obj.etag))
	w.Header().Set("Last-Modified", obj.modified.Format(http.TimeFormat))
	w.Header().Set("Accept-Ranges", "bytes")
	s.confirm(w, keyMD5)

	data := obj.data
	status := http.StatusOK
	if spec := r.Header.Get("Range"); spec != "" {
		var first, last int
		if _, err := fmt.Sscanf(spec, "bytes=%d-%d", &first, &last); err == nil && first <= last && first < len(data) {
			last = min(last, len(data)-1)
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", first, last, len(data)))
			data = data[first : last+1]
			status = http.StatusPartialContent
		}
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.WriteHeader(status)
	if r.Method == http.MethodGet {
		// Written without the lock: the reader may be the repair, which
		// needs this store to answer its part uploads while it streams.
		// Objects are replaced, never modified, so data cannot change.
		s.mu.Unlock()
		_, _ = w.Write(data)
		s.mu.Lock()
	}
}

func (s *sseStore) createUpload(w http.ResponseWriter, r *http.Request, key, keyMD5 string) {
	s.nextID++
	id := fmt.Sprintf("upload-%d", s.nextID)
	s.uploads[id] = &sseUpload{key: key, keyMD5: keyMD5, header: storedHeaders(r.Header), parts: map[int][]byte{}, etags: map[int]string{}}
	// Like MinIO: no confirmation on initiation. The parts confirm it.
	w.Header().Set("Content-Type", "application/xml")
	fmt.Fprintf(w, `<InitiateMultipartUploadResult><Bucket>bucket</Bucket><Key>%s</Key><UploadId>%s</UploadId></InitiateMultipartUploadResult>`, key, id)
}

func (s *sseStore) uploadPart(w http.ResponseWriter, r *http.Request, query map[string][]string, keyMD5 string, body []byte) {
	upload := s.uploads[first(query["uploadId"])]
	if upload == nil {
		s.fault(w, r, http.StatusNotFound, "NoSuchUpload", "The specified multipart upload does not exist.")
		return
	}
	if upload.keyMD5 != keyMD5 {
		s.fault(w, r, http.StatusBadRequest, "InvalidRequest", "The multipart upload initiate requested encryption. Subsequent part requests must include the appropriate encryption parameters.")
		return
	}
	number, _ := strconv.Atoi(first(query["partNumber"]))
	if s.failPart != 0 && number == s.failPart {
		s.fault(w, r, http.StatusInternalServerError, "InternalError", "We encountered an internal error.")
		return
	}
	if int64(len(body)) != r.ContentLength {
		s.fault(w, r, http.StatusBadRequest, "IncompleteBody", "You did not provide the number of bytes specified by the Content-Length HTTP header.")
		return
	}
	upload.parts[number] = body
	upload.etags[number] = s.newETag(body, "")
	w.Header().Set("Etag", s.spell(upload.etags[number]))
	s.confirm(w, keyMD5)
	w.WriteHeader(http.StatusOK)
}

func (s *sseStore) completeUpload(w http.ResponseWriter, r *http.Request, key, id, keyMD5 string, body []byte) {
	upload := s.uploads[id]
	if upload == nil {
		s.fault(w, r, http.StatusNotFound, "NoSuchUpload", "The specified multipart upload does not exist.")
		return
	}
	if s.quirkyETags && bytes.ContainsAny(body, "'") {
		s.fault(w, r, http.StatusBadRequest, "InvalidPart", "single-quoted part ETag")
		return
	}
	existing := s.objects[key]
	if r.Header.Get("If-Match") != "" || r.Header.Get("If-None-Match") != "" {
		if s.noConditionalComplete {
			s.fault(w, r, http.StatusNotImplemented, "NotImplemented", "A header you provided implies functionality that is not implemented.")
			return
		}
		if existing != nil && keyMD5 != "" && !s.readable(w, r, existing, keyMD5) {
			return
		}
		if status, ok := s.precondition(r, existing); !ok {
			code := "PreconditionFailed"
			if status == http.StatusNotFound {
				code = "NoSuchKey"
			}
			s.fault(w, r, status, code, "At least one of the pre-conditions you specified did not hold")
			return
		}
	}
	var list struct {
		Parts []struct {
			PartNumber int    `xml:"PartNumber"`
			ETag       string `xml:"ETag"`
		} `xml:"Part"`
	}
	if err := xml.Unmarshal(body, &list); err != nil || len(list.Parts) == 0 {
		s.fault(w, r, http.StatusBadRequest, "MalformedXML", "The XML you provided was not well-formed.")
		return
	}
	var data []byte
	for _, part := range list.Parts {
		if strings.Trim(part.ETag, `"`) != upload.etags[part.PartNumber] {
			s.fault(w, r, http.StatusBadRequest, "InvalidPart", "One or more of the specified parts could not be found.")
			return
		}
		data = append(data, upload.parts[part.PartNumber]...)
	}
	obj := &sseObject{data: data, header: upload.header, keyMD5: upload.keyMD5, modified: s.clock()}
	obj.etag = s.newETag(data, "x") + "-" + strconv.Itoa(len(list.Parts))
	s.objects[key] = obj
	delete(s.uploads, id)
	w.Header().Set("Etag", s.spell(obj.etag))
	w.Header().Set("Content-Type", "application/xml")
	fmt.Fprintf(w, `<CompleteMultipartUploadResult><Key>%s</Key><ETag>%s</ETag></CompleteMultipartUploadResult>`, key, xmlEscape(s.spell(obj.etag)))
}

func (s *sseStore) listParts(w http.ResponseWriter, r *http.Request, id string) {
	upload := s.uploads[id]
	if upload == nil {
		s.fault(w, r, http.StatusNotFound, "NoSuchUpload", "The specified multipart upload does not exist.")
		return
	}
	numbers := make([]int, 0, len(upload.etags))
	for n := range upload.etags {
		numbers = append(numbers, n)
	}
	sort.Ints(numbers)
	owner := s.uploadOwner
	if owner == "" {
		owner = "owner-1"
	}
	w.Header().Set("Content-Type", "application/xml")
	fmt.Fprintf(w, `<ListPartsResult><Key>%s</Key><Owner><ID>%s</ID></Owner>`, upload.key, owner)
	for _, n := range numbers {
		fmt.Fprintf(w, `<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>`, n, xmlEscape(s.spell(upload.etags[n])))
	}
	fmt.Fprint(w, `</ListPartsResult>`)
}

// getACL answers like S3 for an object with the default ACL — its owner's
// full control — plus whatever extra grants a test gave it.
func (s *sseStore) getACL(w http.ResponseWriter, r *http.Request, key string) {
	if s.aclStatus != 0 {
		s.fault(w, r, s.aclStatus, "AccessDenied", "Access Denied.")
		return
	}
	obj := s.objects[key]
	if obj == nil {
		s.fault(w, r, http.StatusNotFound, "NoSuchKey", "The specified key does not exist.")
		return
	}
	w.Header().Set("Content-Type", "application/xml")
	fmt.Fprintf(w, `<AccessControlPolicy><Owner><ID>owner-1</ID></Owner><AccessControlList>`+
		`<Grant><Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser"><ID>owner-1</ID></Grantee>`+
		`<Permission>FULL_CONTROL</Permission></Grant>%s</AccessControlList></AccessControlPolicy>`, obj.grants)
}

func (s *sseStore) fault(w http.ResponseWriter, r *http.Request, status int, code, message string) {
	if r.Method == http.MethodHead {
		w.WriteHeader(status)
		return
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>%s</Code><Message>%s</Message><Resource>%s</Resource></Error>`, code, message, r.URL.Path)
}

// storedHeaders is what a write says about the object it creates.
func storedHeaders(header http.Header) http.Header {
	out := http.Header{}
	for name, values := range header {
		switch {
		case name == "Content-Type", name == "Content-Encoding", name == "Content-Disposition",
			name == "Content-Language", name == "Cache-Control", name == "Expires", strings.HasPrefix(name, "X-Amz-Meta-"):
			out[name] = values
		}
	}
	return out
}

func xmlEscape(s string) string {
	var b bytes.Buffer
	_ = xml.EscapeText(&b, []byte(s))
	return b.String()
}

func first(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

// testKey and otherKey are the deployment's customer key and somebody else's.
var (
	testKeyEncoded  = base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0x42}, 32))
	otherKeyEncoded = base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0x17}, 32))
)

func mustKey(t *testing.T, encoded string) *ssec.Key {
	t.Helper()
	key, err := ssec.Parse(encoded)
	require.NoError(t, err)
	return key
}

func digestOf(encoded string) string {
	raw, _ := base64.StdEncoding.DecodeString(encoded)
	sum := md5.Sum(raw)
	return base64.StdEncoding.EncodeToString(sum[:])
}
