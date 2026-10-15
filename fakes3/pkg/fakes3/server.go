/*
Copyright 2026 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package fakes3 implements an in-memory, unauthenticated subset of the S3
// REST API, intended for tests and local development.
//
// Supported: CreateBucket, HeadBucket, DeleteBucket, ListBuckets,
// GetBucketLocation, PutObject (including aws-chunked bodies and checksum
// validation), CopyObject, GetObject (with Range), HeadObject, DeleteObject,
// DeleteObjects, ListObjects and ListObjectsV2 (prefix, delimiter,
// pagination). Both path-style and virtual-hosted-style addressing work.
//
// Not supported: authentication (all signatures are ignored, presigned URLs
// are accepted as plain GETs), multipart uploads, versioning, ACLs, lifecycle
// and every other bucket sub-resource.
package fakes3

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const s3Namespace = "http://s3.amazonaws.com/doc/2006-03-01/"

// Server is an http.Handler implementing the fake S3 API.
type Server struct {
	mu                sync.RWMutex
	buckets           map[string]*bucket
	autoCreateBuckets bool
	logf              func(format string, args ...any)
	region            string
}

type bucket struct {
	created time.Time
	objects map[string]*object
}

type object struct {
	data        []byte
	etag        string // hex MD5, unquoted
	modTime     time.Time
	contentType string
	metadata    map[string]string // x-amz-meta-* headers, lower-case keys
	checksums   map[string]string // x-amz-checksum-* headers, lower-case keys
}

// Option configures a Server.
type Option func(*Server)

// WithBuckets pre-creates the named buckets.
func WithBuckets(names ...string) Option {
	return func(s *Server) {
		for _, n := range names {
			if n == "" {
				continue
			}
			s.buckets[n] = newBucket()
		}
	}
}

// WithAutoCreateBuckets makes PutObject create a missing bucket instead of
// returning NoSuchBucket. Defaults to false.
func WithAutoCreateBuckets(enabled bool) Option {
	return func(s *Server) { s.autoCreateBuckets = enabled }
}

// WithLogger sets a function that receives one line per request.
func WithLogger(logf func(format string, args ...any)) Option {
	return func(s *Server) { s.logf = logf }
}

// WithRegion sets the region reported by GetBucketLocation. Defaults to
// us-east-1.
func WithRegion(region string) Option {
	return func(s *Server) { s.region = region }
}

// New returns a Server with no buckets unless WithBuckets is given.
func New(opts ...Option) *Server {
	s := &Server{
		buckets: make(map[string]*bucket),
		region:  "us-east-1",
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func newBucket() *bucket {
	return &bucket{created: time.Now().UTC(), objects: make(map[string]*object)}
}

// ServeHTTP dispatches an S3 REST request.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	requestID := newRequestID()
	w.Header().Set("x-amz-request-id", requestID)
	w.Header().Set("Server", "fakes3")

	rw := &statusRecorder{ResponseWriter: w, status: http.StatusOK}

	bucketName, key, err := s.resolve(r)
	if err != nil {
		writeError(rw, r, http.StatusBadRequest, "InvalidArgument", "Invalid Host header: "+err.Error(), "")
		if s.logf != nil {
			s.logf("%s %s host=%q -> %d", r.Method, r.URL.RequestURI(), r.Host, rw.status)
		}
		return
	}

	s.route(rw, r, bucketName, key)

	if s.logf != nil {
		s.logf("%s %s bucket=%q key=%q -> %d", r.Method, r.URL.RequestURI(), bucketName, key, rw.status)
	}
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// resolve extracts the bucket and key from either virtual-hosted-style
// (bucket.host/key) or path-style (host/bucket/key) requests. Virtual-hosted
// style is only recognised for buckets that already exist, since we cannot
// otherwise tell a bucket label apart from an ordinary hostname component.
func (s *Server) resolve(r *http.Request) (string, string, error) {
	path := strings.TrimPrefix(r.URL.Path, "/")

	host := r.Host
	if host != "" {
		if h, _, err := net.SplitHostPort(host); err != nil {
			var addrErr *net.AddrError
			if errors.As(err, &addrErr) && addrErr.Err == "missing port in address" {
				// Host does not specify a port; keep host as-is.
			} else {
				return "", "", fmt.Errorf("invalid host %q: %w", host, err)
			}
		} else {
			host = h
		}
	}
	if label, _, ok := strings.Cut(host, "."); ok && label != "" && net.ParseIP(host) == nil {
		s.mu.RLock()
		_, known := s.buckets[label]
		s.mu.RUnlock()
		if known {
			return label, path, nil
		}
	}

	bucketName, key, _ := strings.Cut(path, "/")
	return bucketName, key, nil
}

func (s *Server) route(w http.ResponseWriter, r *http.Request, bucketName, key string) {
	q := r.URL.Query()

	if bucketName == "" {
		if r.Method == http.MethodGet || r.Method == http.MethodHead {
			s.listBuckets(w, r)
			return
		}
		writeError(w, r, http.StatusMethodNotAllowed, "MethodNotAllowed", "The specified method is not allowed against this resource.", "")
		return
	}

	if key == "" {
		switch r.Method {
		case http.MethodPut:
			s.createBucket(w, r, bucketName)
		case http.MethodHead:
			s.headBucket(w, r, bucketName)
		case http.MethodDelete:
			s.deleteBucket(w, r, bucketName)
		case http.MethodGet:
			switch {
			case q.Has("location"):
				s.getBucketLocation(w, r, bucketName)
			case q.Has("uploads"):
				writeError(w, r, http.StatusNotImplemented, "NotImplemented", "Multipart uploads are not supported by fakes3.", bucketName)
			case len(q) > 0 && !q.Has("list-type") && !q.Has("prefix") && !q.Has("delimiter") && !q.Has("max-keys") && !q.Has("marker") && !q.Has("encoding-type") && !q.Has("continuation-token") && !q.Has("start-after") && !q.Has("fetch-owner"):
				// Some other bucket sub-resource (?versioning, ?acl, ...).
				writeError(w, r, http.StatusNotImplemented, "NotImplemented", "This bucket sub-resource is not supported by fakes3.", bucketName)
			default:
				s.listObjects(w, r, bucketName)
			}
		case http.MethodPost:
			if q.Has("delete") {
				s.deleteObjects(w, r, bucketName)
				return
			}
			writeError(w, r, http.StatusNotImplemented, "NotImplemented", "POST is only supported for ?delete.", bucketName)
		default:
			writeError(w, r, http.StatusMethodNotAllowed, "MethodNotAllowed", "The specified method is not allowed against this resource.", bucketName)
		}
		return
	}

	if q.Has("uploads") || q.Has("uploadId") {
		writeError(w, r, http.StatusNotImplemented, "NotImplemented", "Multipart uploads are not supported by fakes3.", bucketName+"/"+key)
		return
	}

	switch r.Method {
	case http.MethodPut:
		if r.Header.Get("x-amz-copy-source") != "" {
			s.copyObject(w, r, bucketName, key)
			return
		}
		s.putObject(w, r, bucketName, key)
	case http.MethodGet:
		s.getObject(w, r, bucketName, key, true)
	case http.MethodHead:
		s.getObject(w, r, bucketName, key, false)
	case http.MethodDelete:
		s.deleteObject(w, r, bucketName, key)
	default:
		writeError(w, r, http.StatusMethodNotAllowed, "MethodNotAllowed", "The specified method is not allowed against this resource.", bucketName+"/"+key)
	}
}

// ---------------------------------------------------------------------------
// Buckets
// ---------------------------------------------------------------------------

type listAllMyBucketsResult struct {
	XMLName xml.Name `xml:"ListAllMyBucketsResult"`
	Xmlns   string   `xml:"xmlns,attr"`
	Owner   owner    `xml:"Owner"`
	Buckets struct {
		Bucket []bucketInfo `xml:"Bucket"`
	} `xml:"Buckets"`
}

type owner struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

type bucketInfo struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}

func (s *Server) listBuckets(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	names := make([]string, 0, len(s.buckets))
	for n := range s.buckets {
		names = append(names, n)
	}
	sort.Strings(names)
	res := listAllMyBucketsResult{Xmlns: s3Namespace, Owner: fakeOwner}
	for _, n := range names {
		res.Buckets.Bucket = append(res.Buckets.Bucket, bucketInfo{Name: n, CreationDate: iso8601(s.buckets[n].created)})
	}
	s.mu.RUnlock()
	writeXML(w, r, http.StatusOK, res)
}

var fakeOwner = owner{ID: "fakes3", DisplayName: "fakes3"}

func (s *Server) createBucket(w http.ResponseWriter, r *http.Request, name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.buckets[name]; exists {
		// Real S3 returns 409 BucketAlreadyOwnedByYou in most regions; MinIO
		// does the same. Match that so callers using --ignore-existing logic
		// behave as they would against the real thing.
		writeError(w, r, http.StatusConflict, "BucketAlreadyOwnedByYou", "Your previous request to create the named bucket succeeded and you already own it.", name)
		return
	}
	s.buckets[name] = newBucket()
	w.Header().Set("Location", "/"+name)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) headBucket(w http.ResponseWriter, r *http.Request, name string) {
	s.mu.RLock()
	_, exists := s.buckets[name]
	s.mu.RUnlock()
	if !exists {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.Header().Set("x-amz-bucket-region", s.region)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) deleteBucket(w http.ResponseWriter, r *http.Request, name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, exists := s.buckets[name]
	if !exists {
		writeError(w, r, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist.", name)
		return
	}
	if len(b.objects) > 0 {
		writeError(w, r, http.StatusConflict, "BucketNotEmpty", "The bucket you tried to delete is not empty.", name)
		return
	}
	delete(s.buckets, name)
	w.WriteHeader(http.StatusNoContent)
}

type locationConstraint struct {
	XMLName  xml.Name `xml:"LocationConstraint"`
	Xmlns    string   `xml:"xmlns,attr"`
	Location string   `xml:",chardata"`
}

func (s *Server) getBucketLocation(w http.ResponseWriter, r *http.Request, name string) {
	s.mu.RLock()
	_, exists := s.buckets[name]
	s.mu.RUnlock()
	if !exists {
		writeError(w, r, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist.", name)
		return
	}
	loc := s.region
	if loc == "us-east-1" {
		// S3 reports an empty LocationConstraint for us-east-1.
		loc = ""
	}
	writeXML(w, r, http.StatusOK, locationConstraint{Xmlns: s3Namespace, Location: loc})
}

// ---------------------------------------------------------------------------
// Listing
// ---------------------------------------------------------------------------

type listBucketResult struct {
	XMLName               xml.Name       `xml:"ListBucketResult"`
	Xmlns                 string         `xml:"xmlns,attr"`
	Name                  string         `xml:"Name"`
	Prefix                string         `xml:"Prefix"`
	Delimiter             string         `xml:"Delimiter,omitempty"`
	EncodingType          string         `xml:"EncodingType,omitempty"`
	MaxKeys               int            `xml:"MaxKeys"`
	IsTruncated           bool           `xml:"IsTruncated"`
	KeyCount              *int           `xml:"KeyCount,omitempty"`
	ContinuationToken     string         `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string         `xml:"NextContinuationToken,omitempty"`
	StartAfter            string         `xml:"StartAfter,omitempty"`
	Marker                *string        `xml:"Marker,omitempty"`
	NextMarker            string         `xml:"NextMarker,omitempty"`
	Contents              []objectInfo   `xml:"Contents"`
	CommonPrefixes        []commonPrefix `xml:"CommonPrefixes"`
}

type objectInfo struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int    `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
	Owner        *owner `xml:"Owner,omitempty"`
}

type commonPrefix struct {
	Prefix string `xml:"Prefix"`
}

func (s *Server) listObjects(w http.ResponseWriter, r *http.Request, bucketName string) {
	q := r.URL.Query()
	v2 := q.Get("list-type") == "2"
	prefix := q.Get("prefix")
	delimiter := q.Get("delimiter")
	encodingType := q.Get("encoding-type")
	fetchOwner := q.Get("fetch-owner") == "true"

	maxKeys := 1000
	if mk := q.Get("max-keys"); mk != "" {
		n, err := strconv.Atoi(mk)
		if err != nil || n < 0 {
			writeError(w, r, http.StatusBadRequest, "InvalidArgument", "Argument max-keys must be a non-negative integer.", bucketName)
			return
		}
		maxKeys = n
	}

	// Determine where to start. V2 uses continuation-token (opaque, ours is
	// the base64 of the last key returned) and start-after; V1 uses marker.
	var startAfter string
	if v2 {
		startAfter = q.Get("start-after")
		if tok := q.Get("continuation-token"); tok != "" {
			decoded, err := base64.StdEncoding.DecodeString(tok)
			if err != nil {
				writeError(w, r, http.StatusBadRequest, "InvalidArgument", "The continuation token provided is incorrect.", bucketName)
				return
			}
			startAfter = string(decoded)
		}
	} else {
		startAfter = q.Get("marker")
	}

	s.mu.RLock()
	b, exists := s.buckets[bucketName]
	if !exists {
		s.mu.RUnlock()
		writeError(w, r, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist.", bucketName)
		return
	}
	keys := make([]string, 0, len(b.objects))
	for k := range b.objects {
		if strings.HasPrefix(k, prefix) && k > startAfter {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	res := listBucketResult{
		Xmlns:     s3Namespace,
		Name:      bucketName,
		Prefix:    prefix,
		Delimiter: delimiter,
		MaxKeys:   maxKeys,
	}

	// Walk keys in order, collapsing on the delimiter, until maxKeys entries
	// (objects + common prefixes) have been produced.
	seenPrefixes := make(map[string]bool)
	count := 0
	var lastKey string
	truncated := false
	for _, k := range keys {
		if count >= maxKeys {
			truncated = true
			break
		}
		if delimiter != "" {
			rest := k[len(prefix):]
			if i := strings.Index(rest, delimiter); i >= 0 {
				cp := prefix + rest[:i+len(delimiter)]
				if !seenPrefixes[cp] {
					seenPrefixes[cp] = true
					res.CommonPrefixes = append(res.CommonPrefixes, commonPrefix{Prefix: encodeKey(cp, encodingType)})
					count++
				}
				// All keys under this common prefix share it; the marker for
				// resumption must be the last key we skipped over.
				lastKey = k
				continue
			}
		}
		obj := b.objects[k]
		info := objectInfo{
			Key:          encodeKey(k, encodingType),
			LastModified: iso8601(obj.modTime),
			ETag:         `"` + obj.etag + `"`,
			Size:         len(obj.data),
			StorageClass: "STANDARD",
		}
		if fetchOwner || !v2 {
			o := fakeOwner
			info.Owner = &o
		}
		res.Contents = append(res.Contents, info)
		lastKey = k
		count++
	}
	s.mu.RUnlock()

	res.IsTruncated = truncated
	if encodingType == "url" {
		res.EncodingType = "url"
		res.Prefix = encodeKey(prefix, encodingType)
		res.Delimiter = encodeKey(delimiter, encodingType)
	}
	if v2 {
		kc := count
		res.KeyCount = &kc
		if tok := q.Get("continuation-token"); tok != "" {
			res.ContinuationToken = tok
		}
		if sa := q.Get("start-after"); sa != "" {
			res.StartAfter = encodeKey(sa, encodingType)
		}
		if truncated {
			res.NextContinuationToken = base64.StdEncoding.EncodeToString([]byte(lastKey))
		}
	} else {
		m := q.Get("marker")
		res.Marker = &m
		if truncated && delimiter != "" {
			// V1 only returns NextMarker when a delimiter is in use.
			res.NextMarker = encodeKey(lastKey, encodingType)
		}
	}
	writeXML(w, r, http.StatusOK, res)
}

func encodeKey(k, encodingType string) string {
	if encodingType == "url" {
		return url.QueryEscape(k)
	}
	return k
}

// ---------------------------------------------------------------------------
// Objects
// ---------------------------------------------------------------------------

func (s *Server) putObject(w http.ResponseWriter, r *http.Request, bucketName, key string) {
	body, trailers, err := readBody(r)
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "IncompleteBody", "Failed to read the request body: "+err.Error(), bucketName+"/"+key)
		return
	}

	// Checksums may arrive as headers (seekable bodies) or as trailers
	// (aws-chunked streaming bodies). Validate whichever we were given.
	checksums := make(map[string]string)
	for name, values := range r.Header {
		lname := strings.ToLower(name)
		if strings.HasPrefix(lname, "x-amz-checksum-") && len(values) > 0 {
			checksums[lname] = values[0]
		}
	}
	for name, value := range trailers {
		if strings.HasPrefix(name, "x-amz-checksum-") {
			checksums[name] = value
		}
	}
	for name, value := range checksums {
		if name == "x-amz-checksum-algorithm" || name == "x-amz-checksum-type" {
			delete(checksums, name)
			continue
		}
		if ok, known := verifyChecksum(name, value, body); known && !ok {
			writeError(w, r, http.StatusBadRequest, "BadDigest", fmt.Sprintf("The %s you specified did not match what we received.", name), bucketName+"/"+key)
			return
		}
	}
	if md5Header := r.Header.Get("Content-MD5"); md5Header != "" {
		sum := md5.Sum(body)
		if base64.StdEncoding.EncodeToString(sum[:]) != md5Header {
			writeError(w, r, http.StatusBadRequest, "BadDigest", "The Content-MD5 you specified did not match what we received.", bucketName+"/"+key)
			return
		}
	}

	obj := &object{
		data:        body,
		etag:        hexMD5(body),
		modTime:     time.Now().UTC().Truncate(time.Second),
		contentType: r.Header.Get("Content-Type"),
		metadata:    userMetadata(r.Header),
		checksums:   checksums,
	}

	s.mu.Lock()
	b, exists := s.buckets[bucketName]
	if !exists {
		if !s.autoCreateBuckets {
			s.mu.Unlock()
			writeError(w, r, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist.", bucketName)
			return
		}
		b = newBucket()
		s.buckets[bucketName] = b
	}
	b.objects[key] = obj
	s.mu.Unlock()

	w.Header().Set("ETag", `"`+obj.etag+`"`)
	for name, value := range checksums {
		w.Header().Set(name, value)
	}
	w.WriteHeader(http.StatusOK)
}

type copyObjectResult struct {
	XMLName      xml.Name `xml:"CopyObjectResult"`
	Xmlns        string   `xml:"xmlns,attr"`
	LastModified string   `xml:"LastModified"`
	ETag         string   `xml:"ETag"`
}

func (s *Server) copyObject(w http.ResponseWriter, r *http.Request, bucketName, key string) {
	source := r.Header.Get("x-amz-copy-source")
	decoded, err := url.PathUnescape(source)
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "InvalidArgument", "Invalid x-amz-copy-source header: "+err.Error(), bucketName+"/"+key)
		return
	}
	source = decoded
	source = strings.TrimPrefix(source, "/")
	srcBucket, srcKey, ok := strings.Cut(source, "/")
	if !ok || srcBucket == "" || srcKey == "" {
		writeError(w, r, http.StatusBadRequest, "InvalidArgument", "Copy Source must mention the source bucket and key: sourcebucket/sourcekey", bucketName+"/"+key)
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	sb, exists := s.buckets[srcBucket]
	if !exists {
		writeError(w, r, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist.", srcBucket)
		return
	}
	src, exists := sb.objects[srcKey]
	if !exists {
		writeError(w, r, http.StatusNotFound, "NoSuchKey", "The specified key does not exist.", srcBucket+"/"+srcKey)
		return
	}
	db, exists := s.buckets[bucketName]
	if !exists {
		writeError(w, r, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist.", bucketName)
		return
	}

	dst := &object{
		data:        append([]byte(nil), src.data...),
		etag:        src.etag,
		modTime:     time.Now().UTC().Truncate(time.Second),
		contentType: src.contentType,
		metadata:    src.metadata,
		checksums:   src.checksums,
	}
	if strings.EqualFold(r.Header.Get("x-amz-metadata-directive"), "REPLACE") {
		dst.metadata = userMetadata(r.Header)
		if ct := r.Header.Get("Content-Type"); ct != "" {
			dst.contentType = ct
		}
	}
	db.objects[key] = dst

	writeXML(w, r, http.StatusOK, copyObjectResult{
		Xmlns:        s3Namespace,
		LastModified: iso8601(dst.modTime),
		ETag:         `"` + dst.etag + `"`,
	})
}

func (s *Server) getObject(w http.ResponseWriter, r *http.Request, bucketName, key string, withBody bool) {
	s.mu.RLock()
	b, exists := s.buckets[bucketName]
	if !exists {
		s.mu.RUnlock()
		if withBody {
			writeError(w, r, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist.", bucketName)
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
		return
	}
	obj, exists := b.objects[key]
	if !exists {
		s.mu.RUnlock()
		if withBody {
			writeError(w, r, http.StatusNotFound, "NoSuchKey", "The specified key does not exist.", bucketName+"/"+key)
		} else {
			// HEAD responses carry no body; the SDK maps this to NotFound.
			w.WriteHeader(http.StatusNotFound)
		}
		return
	}
	// Copy out under the lock so a concurrent overwrite cannot change what we
	// are about to serve.
	data := obj.data
	etag := obj.etag
	modTime := obj.modTime
	contentType := obj.contentType
	metadata := obj.metadata
	checksums := obj.checksums
	s.mu.RUnlock()

	// Conditional requests.
	if inm := r.Header.Get("If-None-Match"); inm != "" && etagMatches(inm, etag) {
		w.Header().Set("ETag", `"`+etag+`"`)
		w.WriteHeader(http.StatusNotModified)
		return
	}
	if im := r.Header.Get("If-Match"); im != "" && !etagMatches(im, etag) {
		writeError(w, r, http.StatusPreconditionFailed, "PreconditionFailed", "At least one of the pre-conditions you specified did not hold.", bucketName+"/"+key)
		return
	}

	// Validate the range before setting any object headers, so that an
	// error response does not carry e.g. checksum headers that the SDK would
	// then try to verify against the error body.
	status := http.StatusOK
	start, end := int64(0), int64(len(data))
	rangeHeader := r.Header.Get("Range")
	if rangeHeader != "" {
		var ok bool
		start, end, ok = parseRange(rangeHeader, int64(len(data)))
		if !ok {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", len(data)))
			writeError(w, r, http.StatusRequestedRangeNotSatisfiable, "InvalidRange", "The requested range is not satisfiable", bucketName+"/"+key)
			return
		}
		status = http.StatusPartialContent
	}

	h := w.Header()
	h.Set("ETag", `"`+etag+`"`)
	h.Set("Last-Modified", modTime.Format(http.TimeFormat))
	h.Set("Accept-Ranges", "bytes")
	if contentType != "" {
		h.Set("Content-Type", contentType)
	} else {
		h.Set("Content-Type", "binary/octet-stream")
	}
	for k, v := range metadata {
		h.Set("x-amz-meta-"+k, v)
	}
	// Like S3, only report whole-object checksums on whole-object reads.
	if rangeHeader == "" && strings.EqualFold(r.Header.Get("x-amz-checksum-mode"), "ENABLED") {
		for k, v := range checksums {
			h.Set(k, v)
		}
	}
	if status == http.StatusPartialContent {
		h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end-1, len(data)))
	}
	h.Set("Content-Length", strconv.FormatInt(end-start, 10))
	w.WriteHeader(status)
	if withBody {
		if _, err := w.Write(data[start:end]); err != nil {
			log.Printf("fakes3: write getObject response: %v (client went away?)", err)
		}
	}
}

func (s *Server) deleteObject(w http.ResponseWriter, r *http.Request, bucketName, key string) {
	s.mu.Lock()
	b, exists := s.buckets[bucketName]
	if exists {
		delete(b.objects, key)
	}
	s.mu.Unlock()
	if !exists {
		writeError(w, r, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist.", bucketName)
		return
	}
	// S3 returns 204 whether or not the key existed.
	w.WriteHeader(http.StatusNoContent)
}

type deleteRequest struct {
	XMLName xml.Name `xml:"Delete"`
	Quiet   bool     `xml:"Quiet"`
	Objects []struct {
		Key string `xml:"Key"`
	} `xml:"Object"`
}

type deleteResult struct {
	XMLName xml.Name `xml:"DeleteResult"`
	Xmlns   string   `xml:"xmlns,attr"`
	Deleted []struct {
		Key string `xml:"Key"`
	} `xml:"Deleted"`
}

func (s *Server) deleteObjects(w http.ResponseWriter, r *http.Request, bucketName string) {
	body, _, err := readBody(r)
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "MalformedXML", "Failed to read the request body: "+err.Error(), bucketName)
		return
	}
	var req deleteRequest
	if err := xml.Unmarshal(body, &req); err != nil {
		writeError(w, r, http.StatusBadRequest, "MalformedXML", "The XML you provided was not well-formed or did not validate against our published schema.", bucketName)
		return
	}

	s.mu.Lock()
	b, exists := s.buckets[bucketName]
	if !exists {
		s.mu.Unlock()
		writeError(w, r, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist.", bucketName)
		return
	}
	res := deleteResult{Xmlns: s3Namespace}
	for _, o := range req.Objects {
		delete(b.objects, o.Key)
		if !req.Quiet {
			res.Deleted = append(res.Deleted, struct {
				Key string `xml:"Key"`
			}{Key: o.Key})
		}
	}
	s.mu.Unlock()
	writeXML(w, r, http.StatusOK, res)
}

// ---------------------------------------------------------------------------
// Request body handling
// ---------------------------------------------------------------------------

// readBody returns the decoded request body and any trailing headers. Bodies
// sent with the aws-chunked content encoding (used by the AWS SDKs for
// streaming uploads with trailing checksums) are de-chunked.
func readBody(r *http.Request) ([]byte, map[string]string, error) {
	if isAwsChunked(r) {
		return decodeAwsChunked(r.Body)
	}
	data, err := io.ReadAll(r.Body)
	return data, nil, err
}

func isAwsChunked(r *http.Request) bool {
	for _, enc := range strings.Split(r.Header.Get("Content-Encoding"), ",") {
		if strings.TrimSpace(enc) == "aws-chunked" {
			return true
		}
	}
	return strings.HasPrefix(r.Header.Get("x-amz-content-sha256"), "STREAMING-")
}

// decodeAwsChunked parses the aws-chunked framing:
//
//	<hex-size>[;chunk-signature=<sig>]\r\n<data>\r\n ... 0[;chunk-signature=<sig>]\r\n
//	[<trailer-name>:<value>\r\n ...]\r\n
func decodeAwsChunked(body io.Reader) ([]byte, map[string]string, error) {
	br := bufio.NewReader(body)
	var out bytes.Buffer
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return nil, nil, fmt.Errorf("reading chunk header: %w", err)
		}
		line = strings.TrimRight(line, "\r\n")
		sizeHex, _, _ := strings.Cut(line, ";")
		size, err := strconv.ParseInt(strings.TrimSpace(sizeHex), 16, 64)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid chunk size %q: %w", sizeHex, err)
		}
		if size == 0 {
			break
		}
		if _, err := io.CopyN(&out, br, size); err != nil {
			return nil, nil, fmt.Errorf("reading chunk data: %w", err)
		}
		if err := expectCRLF(br); err != nil {
			return nil, nil, err
		}
	}

	// Optional trailers, terminated by an empty line (or EOF).
	trailers := make(map[string]string)
	for {
		line, err := br.ReadString('\n')
		if err != nil && err != io.EOF {
			return nil, nil, fmt.Errorf("reading trailers: %w", err)
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			break
		}
		name, value, ok := strings.Cut(line, ":")
		if ok {
			trailers[strings.ToLower(strings.TrimSpace(name))] = strings.TrimSpace(value)
		}
		if err == io.EOF {
			break
		}
	}
	return out.Bytes(), trailers, nil
}

func expectCRLF(br *bufio.Reader) error {
	var crlf [2]byte
	if _, err := io.ReadFull(br, crlf[:]); err != nil {
		return fmt.Errorf("reading chunk terminator: %w", err)
	}
	if crlf != [2]byte{'\r', '\n'} {
		return fmt.Errorf("invalid chunk terminator %q", crlf[:])
	}
	return nil
}

// verifyChecksum checks an x-amz-checksum-* header value (base64) against
// the body. It returns (matches, algorithmKnown).
func verifyChecksum(name, value string, body []byte) (bool, bool) {
	var h hash.Hash
	switch name {
	case "x-amz-checksum-crc32":
		h = crc32.NewIEEE()
	case "x-amz-checksum-crc32c":
		h = crc32.New(crc32.MakeTable(crc32.Castagnoli))
	case "x-amz-checksum-sha1":
		h = sha1.New()
	case "x-amz-checksum-sha256":
		h = sha256.New()
	default:
		// crc64nvme and anything newer: accept without checking.
		return true, false
	}
	_, _ = h.Write(body)
	return base64.StdEncoding.EncodeToString(h.Sum(nil)) == value, true
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// parseRange handles "bytes=a-b", "bytes=a-" and "bytes=-n". It returns a
// half-open [start, end) interval and false if the range is unsatisfiable.
func parseRange(spec string, size int64) (int64, int64, bool) {
	spec = strings.TrimSpace(spec)
	if !strings.HasPrefix(spec, "bytes=") {
		return 0, 0, false
	}
	spec = strings.TrimPrefix(spec, "bytes=")
	// Multiple ranges are not supported; S3 doesn't support them either.
	if strings.Contains(spec, ",") {
		return 0, 0, false
	}
	startStr, endStr, ok := strings.Cut(spec, "-")
	if !ok {
		return 0, 0, false
	}
	if startStr == "" {
		// Suffix range: last n bytes.
		n, err := strconv.ParseInt(endStr, 10, 64)
		if err != nil || n <= 0 {
			return 0, 0, false
		}
		if n > size {
			n = size
		}
		if size == 0 {
			return 0, 0, false
		}
		return size - n, size, true
	}
	start, err := strconv.ParseInt(startStr, 10, 64)
	if err != nil || start < 0 {
		return 0, 0, false
	}
	if start >= size {
		return 0, 0, false
	}
	end := size
	if endStr != "" {
		e, err := strconv.ParseInt(endStr, 10, 64)
		if err != nil || e < start {
			return 0, 0, false
		}
		if e+1 < end {
			end = e + 1
		}
	}
	return start, end, true
}

func etagMatches(header, etag string) bool {
	for _, candidate := range strings.Split(header, ",") {
		candidate = strings.TrimSpace(candidate)
		if candidate == "*" || strings.Trim(candidate, `"`) == etag {
			return true
		}
	}
	return false
}

func userMetadata(h http.Header) map[string]string {
	var md map[string]string
	for name, values := range h {
		lname := strings.ToLower(name)
		if strings.HasPrefix(lname, "x-amz-meta-") && len(values) > 0 {
			if md == nil {
				md = make(map[string]string)
			}
			md[strings.TrimPrefix(lname, "x-amz-meta-")] = values[0]
		}
	}
	return md
}

func hexMD5(b []byte) string {
	sum := md5.Sum(b)
	return hex.EncodeToString(sum[:])
}

func iso8601(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000Z")
}

func newRequestID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return strings.ToUpper(hex.EncodeToString(b[:]))
}

type errorResponse struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource,omitempty"`
	RequestID string   `xml:"RequestId"`
}

func writeError(w http.ResponseWriter, r *http.Request, status int, code, message, resource string) {
	if resource != "" {
		resource = "/" + resource
	}
	res := errorResponse{Code: code, Message: message, Resource: resource, RequestID: w.Header().Get("x-amz-request-id")}
	if r.Method == http.MethodHead {
		w.WriteHeader(status)
		return
	}
	writeXML(w, r, status, res)
}

func writeXML(w http.ResponseWriter, r *http.Request, status int, v any) {
	var buf bytes.Buffer
	buf.WriteString(xml.Header)
	if err := xml.NewEncoder(&buf).Encode(v); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set("Content-Length", strconv.Itoa(buf.Len()))
	w.WriteHeader(status)
	if r.Method != http.MethodHead {
		if _, err := w.Write(buf.Bytes()); err != nil {
			log.Printf("fakes3: write XML response: %v (client went away?)", err)
		}
	}
}
