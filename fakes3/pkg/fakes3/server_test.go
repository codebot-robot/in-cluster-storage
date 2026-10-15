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

package fakes3

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// newTestClient starts a fake with the given buckets and returns an SDK
// client configured the way our objectstore backend configures it
// (custom endpoint, path style, static credentials).
func newTestClient(t *testing.T, opts ...Option) (*s3.Client, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(New(append([]Option{WithLogger(t.Logf)}, opts...)...))
	t.Cleanup(srv.Close)

	cfg, err := config.LoadDefaultConfig(t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("fakes3", "fakes3", "")),
	)
	if err != nil {
		t.Fatalf("LoadDefaultConfig: %v", err)
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
		o.UsePathStyle = true
	})
	return client, srv
}

func apiErrorCode(err error) string {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		return apiErr.ErrorCode()
	}
	return ""
}

func TestPutGetRoundTrip(t *testing.T) {
	ctx := t.Context()
	client, _ := newTestClient(t, WithBuckets("b"))

	content := []byte("hello fakes3")
	put, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String("b"),
		Key:         aws.String("dir/hello.txt"),
		Body:        bytes.NewReader(content),
		ContentType: aws.String("text/plain"),
		Metadata:    map[string]string{"owner": "tests"},
	})
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	sum := md5.Sum(content)
	wantETag := `"` + hex.EncodeToString(sum[:]) + `"`
	if aws.ToString(put.ETag) != wantETag {
		t.Errorf("PutObject ETag = %q, want %q", aws.ToString(put.ETag), wantETag)
	}

	got, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("b"), Key: aws.String("dir/hello.txt")})
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	defer got.Body.Close()
	data, _ := io.ReadAll(got.Body)
	if !bytes.Equal(data, content) {
		t.Errorf("GetObject body = %q, want %q", data, content)
	}
	if aws.ToString(got.ETag) != wantETag {
		t.Errorf("GetObject ETag = %q, want %q", aws.ToString(got.ETag), wantETag)
	}
	if aws.ToString(got.ContentType) != "text/plain" {
		t.Errorf("ContentType = %q, want text/plain", aws.ToString(got.ContentType))
	}
	if got.Metadata["owner"] != "tests" {
		t.Errorf("Metadata = %v, want owner=tests", got.Metadata)
	}
	if aws.ToInt64(got.ContentLength) != int64(len(content)) {
		t.Errorf("ContentLength = %d, want %d", aws.ToInt64(got.ContentLength), len(content))
	}

	head, err := client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String("b"), Key: aws.String("dir/hello.txt")})
	if err != nil {
		t.Fatalf("HeadObject: %v", err)
	}
	if aws.ToInt64(head.ContentLength) != int64(len(content)) || aws.ToString(head.ETag) != wantETag {
		t.Errorf("HeadObject = len %d etag %q", aws.ToInt64(head.ContentLength), aws.ToString(head.ETag))
	}
}

func TestLargeObject(t *testing.T) {
	ctx := t.Context()
	client, _ := newTestClient(t, WithBuckets("b"))

	content := make([]byte, 20<<20)
	rand.New(rand.NewSource(1)).Read(content)
	if _, err := client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("b"), Key: aws.String("big"), Body: bytes.NewReader(content)}); err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	got, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("b"), Key: aws.String("big")})
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	defer got.Body.Close()
	data, _ := io.ReadAll(got.Body)
	if !bytes.Equal(data, content) {
		t.Fatalf("20MB round trip mismatch (got %d bytes)", len(data))
	}
}

func TestRangeRequests(t *testing.T) {
	ctx := t.Context()
	client, _ := newTestClient(t, WithBuckets("b"))
	content := []byte("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")
	if _, err := client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("b"), Key: aws.String("r"), Body: bytes.NewReader(content)}); err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	cases := []struct {
		rng  string
		want string
	}{
		{"bytes=0-9", "0123456789"},
		{"bytes=10-14", "ABCDE"},
		{"bytes=26-", "QRSTUVWXYZ"},
		{"bytes=-5", "VWXYZ"},
		{"bytes=30-100", "UVWXYZ"}, // end past EOF is clamped
	}
	for _, tc := range cases {
		got, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("b"), Key: aws.String("r"), Range: aws.String(tc.rng)})
		if err != nil {
			t.Fatalf("GetObject %s: %v", tc.rng, err)
		}
		data, _ := io.ReadAll(got.Body)
		got.Body.Close()
		if string(data) != tc.want {
			t.Errorf("Range %s = %q, want %q", tc.rng, data, tc.want)
		}
		if aws.ToString(got.ContentRange) == "" {
			t.Errorf("Range %s: missing Content-Range", tc.rng)
		}
	}

	// Offset at or beyond EOF: S3 answers 416 InvalidRange; the objectstore
	// backend relies on that error code to return zero bytes.
	_, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("b"), Key: aws.String("r"), Range: aws.String("bytes=36-45")})
	if code := apiErrorCode(err); code != "InvalidRange" {
		t.Errorf("Range past EOF: error code = %q (err=%v), want InvalidRange", code, err)
	}
}

func TestNotFoundAndDelete(t *testing.T) {
	ctx := t.Context()
	client, _ := newTestClient(t, WithBuckets("b"))

	_, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("b"), Key: aws.String("missing")})
	var nsk *s3types.NoSuchKey
	if !errors.As(err, &nsk) {
		t.Errorf("GetObject missing: err = %v, want NoSuchKey", err)
	}

	_, err = client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String("b"), Key: aws.String("missing")})
	if code := apiErrorCode(err); code != "NotFound" {
		t.Errorf("HeadObject missing: code = %q (err=%v), want NotFound", code, err)
	}

	_, err = client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("nope"), Key: aws.String("x")})
	if code := apiErrorCode(err); code != "NoSuchBucket" {
		t.Errorf("GetObject in missing bucket: code = %q, want NoSuchBucket", code)
	}

	if _, err := client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("b"), Key: aws.String("d"), Body: strings.NewReader("x")}); err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	if _, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String("b"), Key: aws.String("d")}); err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}
	// Idempotent.
	if _, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String("b"), Key: aws.String("d")}); err != nil {
		t.Fatalf("DeleteObject (again): %v", err)
	}
	if _, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("b"), Key: aws.String("d")}); !errors.As(err, &nsk) {
		t.Errorf("GetObject after delete: err = %v, want NoSuchKey", err)
	}
}

func TestDeleteObjects(t *testing.T) {
	ctx := t.Context()
	client, _ := newTestClient(t, WithBuckets("b"))
	for _, k := range []string{"a", "b", "c"} {
		if _, err := client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("b"), Key: aws.String(k), Body: strings.NewReader(k)}); err != nil {
			t.Fatalf("PutObject %s: %v", k, err)
		}
	}
	out, err := client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String("b"),
		Delete: &s3types.Delete{Objects: []s3types.ObjectIdentifier{{Key: aws.String("a")}, {Key: aws.String("c")}, {Key: aws.String("zzz")}}},
	})
	if err != nil {
		t.Fatalf("DeleteObjects: %v", err)
	}
	if len(out.Deleted) != 3 || len(out.Errors) != 0 {
		t.Errorf("DeleteObjects: deleted=%d errors=%d, want 3/0", len(out.Deleted), len(out.Errors))
	}
	keys := listKeys(t, client, "b", "")
	if strings.Join(keys, ",") != "b" {
		t.Errorf("remaining keys = %v, want [b]", keys)
	}
}

func listKeys(t *testing.T, client *s3.Client, bucket, prefix string) []string {
	t.Helper()
	var keys []string
	p := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String(prefix)})
	for p.HasMorePages() {
		page, err := p.NextPage(t.Context())
		if err != nil {
			t.Fatalf("ListObjectsV2: %v", err)
		}
		for _, o := range page.Contents {
			keys = append(keys, aws.ToString(o.Key))
		}
	}
	return keys
}

func TestListObjectsV2(t *testing.T) {
	ctx := t.Context()
	client, _ := newTestClient(t, WithBuckets("b"))
	all := []string{"p/a/1", "p/a/2", "p/b/1", "p/c", "q/1", "r"}
	for _, k := range all {
		if _, err := client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("b"), Key: aws.String(k), Body: strings.NewReader(k)}); err != nil {
			t.Fatalf("PutObject %s: %v", k, err)
		}
	}

	if got := listKeys(t, client, "b", ""); strings.Join(got, ",") != strings.Join(all, ",") {
		t.Errorf("list all = %v, want %v", got, all)
	}
	if got := listKeys(t, client, "b", "p/a/"); strings.Join(got, ",") != "p/a/1,p/a/2" {
		t.Errorf("list p/a/ = %v", got)
	}

	// Pagination: MaxKeys=2 should take three pages for the 6 objects and
	// return them all exactly once, in order.
	var paged []string
	pages := 0
	p := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{Bucket: aws.String("b"), MaxKeys: aws.Int32(2)})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			t.Fatalf("paginated ListObjectsV2: %v", err)
		}
		pages++
		if aws.ToInt32(page.KeyCount) != int32(len(page.Contents)) {
			t.Errorf("KeyCount %d != len(Contents) %d", aws.ToInt32(page.KeyCount), len(page.Contents))
		}
		for _, o := range page.Contents {
			paged = append(paged, aws.ToString(o.Key))
		}
	}
	if pages != 3 || strings.Join(paged, ",") != strings.Join(all, ",") {
		t.Errorf("pagination: pages=%d keys=%v", pages, paged)
	}

	// Delimiter: top-level "directories" under p/.
	out, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String("b"), Prefix: aws.String("p/"), Delimiter: aws.String("/")})
	if err != nil {
		t.Fatalf("ListObjectsV2 delimiter: %v", err)
	}
	var prefixes []string
	for _, cp := range out.CommonPrefixes {
		prefixes = append(prefixes, aws.ToString(cp.Prefix))
	}
	if strings.Join(prefixes, ",") != "p/a/,p/b/" || len(out.Contents) != 1 || aws.ToString(out.Contents[0].Key) != "p/c" {
		t.Errorf("delimiter listing: prefixes=%v contents=%d", prefixes, len(out.Contents))
	}

	// StartAfter.
	out, err = client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String("b"), StartAfter: aws.String("p/c")})
	if err != nil {
		t.Fatalf("ListObjectsV2 start-after: %v", err)
	}
	if len(out.Contents) != 2 || aws.ToString(out.Contents[0].Key) != "q/1" {
		t.Errorf("start-after: got %d contents starting %q", len(out.Contents), aws.ToString(out.Contents[0].Key))
	}

	// Legacy V1 listing.
	v1, err := client.ListObjects(ctx, &s3.ListObjectsInput{Bucket: aws.String("b"), Prefix: aws.String("q/")})
	if err != nil {
		t.Fatalf("ListObjects v1: %v", err)
	}
	if len(v1.Contents) != 1 || aws.ToString(v1.Contents[0].Key) != "q/1" {
		t.Errorf("v1 listing = %d contents", len(v1.Contents))
	}
}

func TestBuckets(t *testing.T) {
	ctx := t.Context()
	client, _ := newTestClient(t, WithBuckets("pre"))

	if _, err := client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String("pre")}); err != nil {
		t.Errorf("HeadBucket pre: %v", err)
	}
	if _, err := client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String("nope")}); apiErrorCode(err) != "NotFound" {
		t.Errorf("HeadBucket nope: err = %v, want NotFound", err)
	}
	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("new")}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("new")}); apiErrorCode(err) != "BucketAlreadyOwnedByYou" {
		t.Errorf("CreateBucket again: err = %v, want BucketAlreadyOwnedByYou", err)
	}
	// Without auto-create, writing to a missing bucket fails.
	if _, err := client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("missing"), Key: aws.String("k"), Body: strings.NewReader("x")}); apiErrorCode(err) != "NoSuchBucket" {
		t.Errorf("PutObject to missing bucket: err = %v, want NoSuchBucket", err)
	}

	lb, err := client.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		t.Fatalf("ListBuckets: %v", err)
	}
	var names []string
	for _, b := range lb.Buckets {
		names = append(names, aws.ToString(b.Name))
	}
	if strings.Join(names, ",") != "new,pre" {
		t.Errorf("ListBuckets = %v", names)
	}

	loc, err := client.GetBucketLocation(ctx, &s3.GetBucketLocationInput{Bucket: aws.String("pre")})
	if err != nil {
		t.Fatalf("GetBucketLocation: %v", err)
	}
	if loc.LocationConstraint != "" {
		t.Errorf("LocationConstraint = %q, want empty for us-east-1", loc.LocationConstraint)
	}

	if _, err := client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("new"), Key: aws.String("k"), Body: strings.NewReader("x")}); err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	if _, err := client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String("new")}); apiErrorCode(err) != "BucketNotEmpty" {
		t.Errorf("DeleteBucket non-empty: err = %v, want BucketNotEmpty", err)
	}
	if _, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String("new"), Key: aws.String("k")}); err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}
	if _, err := client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String("new")}); err != nil {
		t.Errorf("DeleteBucket: %v", err)
	}
}

func TestAutoCreateBuckets(t *testing.T) {
	ctx := t.Context()
	client, _ := newTestClient(t, WithAutoCreateBuckets(true))
	if _, err := client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("fresh"), Key: aws.String("k"), Body: strings.NewReader("x")}); err != nil {
		t.Fatalf("PutObject with auto-create: %v", err)
	}
	if _, err := client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String("fresh")}); err != nil {
		t.Errorf("HeadBucket after auto-create: %v", err)
	}
}

func TestCopyObject(t *testing.T) {
	ctx := t.Context()
	client, _ := newTestClient(t, WithBuckets("src", "dst"))
	if _, err := client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("src"), Key: aws.String("a b/c"), Body: strings.NewReader("payload"), Metadata: map[string]string{"m": "1"}}); err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	if _, err := client.CopyObject(ctx, &s3.CopyObjectInput{Bucket: aws.String("dst"), Key: aws.String("copy"), CopySource: aws.String("src/a%20b/c")}); err != nil {
		t.Fatalf("CopyObject: %v", err)
	}
	got, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("dst"), Key: aws.String("copy")})
	if err != nil {
		t.Fatalf("GetObject copy: %v", err)
	}
	data, _ := io.ReadAll(got.Body)
	got.Body.Close()
	if string(data) != "payload" || got.Metadata["m"] != "1" {
		t.Errorf("copy = %q metadata %v", data, got.Metadata)
	}

	// CopyObject with invalid percent-encoding in x-amz-copy-source returns InvalidArgument.
	if _, err := client.CopyObject(ctx, &s3.CopyObjectInput{Bucket: aws.String("dst"), Key: aws.String("copy"), CopySource: aws.String("src/a%zz")}); apiErrorCode(err) != "InvalidArgument" {
		t.Errorf("CopyObject with invalid unescape: err = %v, want InvalidArgument", err)
	}
}

func TestPresignedGet(t *testing.T) {
	ctx := t.Context()
	client, _ := newTestClient(t, WithBuckets("b"))
	if _, err := client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("b"), Key: aws.String("signed"), Body: strings.NewReader("signed content")}); err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	presigned, err := s3.NewPresignClient(client).PresignGetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("b"), Key: aws.String("signed")}, s3.WithPresignExpires(time.Minute))
	if err != nil {
		t.Fatalf("PresignGetObject: %v", err)
	}
	resp, err := http.Get(presigned.URL)
	if err != nil {
		t.Fatalf("GET presigned: %v", err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || string(data) != "signed content" {
		t.Errorf("presigned GET: status %d body %q", resp.StatusCode, data)
	}
}

func TestVirtualHostedStyle(t *testing.T) {
	_, srv := newTestClient(t, WithBuckets("vhost"))

	put, _ := http.NewRequest(http.MethodPut, srv.URL+"/key1", strings.NewReader("vh"))
	put.Host = "vhost.s3.example.com"
	resp, err := http.DefaultClient.Do(put)
	if err != nil {
		t.Fatalf("PUT: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status = %d", resp.StatusCode)
	}

	// The same object must be visible through path-style addressing.
	resp, err = http.Get(srv.URL + "/vhost/key1")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || string(data) != "vh" {
		t.Errorf("GET path-style: status %d body %q", resp.StatusCode, data)
	}
}

func TestInvalidHostHeader(t *testing.T) {
	_, srv := newTestClient(t, WithBuckets("b"))
	req, err := http.NewRequest(http.MethodGet, srv.URL+"/b/somekey", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Host = "[invalid:80"
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET with invalid host: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected status 400 Bad Request, got %d", resp.StatusCode)
	}
}

func TestAwsChunkedUploadWithTrailer(t *testing.T) {
	_, srv := newTestClient(t, WithBuckets("b"))

	payload := []byte("chunk-one|chunk-two|")
	crc := crc32.ChecksumIEEE(payload)
	crcB64 := base64.StdEncoding.EncodeToString([]byte{byte(crc >> 24), byte(crc >> 16), byte(crc >> 8), byte(crc)})

	var body bytes.Buffer
	fmt.Fprintf(&body, "%x;chunk-signature=deadbeef\r\n", 10)
	body.WriteString("chunk-one|\r\n")
	fmt.Fprintf(&body, "%x;chunk-signature=deadbeef\r\n", 10)
	body.WriteString("chunk-two|\r\n")
	body.WriteString("0;chunk-signature=deadbeef\r\n")
	body.WriteString("x-amz-checksum-crc32:" + crcB64 + "\r\n")
	body.WriteString("x-amz-trailer-signature:cafe\r\n\r\n")

	req, _ := http.NewRequest(http.MethodPut, srv.URL+"/b/chunked", &body)
	req.Header.Set("Content-Encoding", "aws-chunked")
	req.Header.Set("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
	req.Header.Set("x-amz-decoded-content-length", fmt.Sprint(len(payload)))
	req.Header.Set("x-amz-trailer", "x-amz-checksum-crc32")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT: %v", err)
	}
	respBody, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status = %d: %s", resp.StatusCode, respBody)
	}

	resp, err = http.Get(srv.URL + "/b/chunked")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if !bytes.Equal(data, payload) {
		t.Errorf("de-chunked body = %q, want %q", data, payload)
	}

	// A wrong trailing checksum must be rejected.
	body.Reset()
	fmt.Fprintf(&body, "%x\r\n", len(payload))
	body.Write(payload)
	body.WriteString("\r\n0\r\nx-amz-checksum-crc32:AAAAAA==\r\n\r\n")
	req, _ = http.NewRequest(http.MethodPut, srv.URL+"/b/bad", &body)
	req.Header.Set("Content-Encoding", "aws-chunked")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT bad: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("PUT with bad checksum: status = %d, want 400", resp.StatusCode)
	}
}

func TestChecksumHeaderValidation(t *testing.T) {
	ctx := t.Context()
	client, _ := newTestClient(t, WithBuckets("b"))
	// The SDK computes and sends a CRC32 header by default; ask for SHA256
	// explicitly as well so both code paths are exercised.
	if _, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:            aws.String("b"),
		Key:               aws.String("sha"),
		Body:              strings.NewReader("checksummed"),
		ChecksumAlgorithm: s3types.ChecksumAlgorithmSha256,
	}); err != nil {
		t.Fatalf("PutObject with SHA256 checksum: %v", err)
	}
	got, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("b"), Key: aws.String("sha"), ChecksumMode: s3types.ChecksumModeEnabled})
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	data, _ := io.ReadAll(got.Body)
	got.Body.Close()
	if string(data) != "checksummed" || aws.ToString(got.ChecksumSHA256) == "" {
		t.Errorf("GetObject = %q sha256=%q", data, aws.ToString(got.ChecksumSHA256))
	}
}

func TestParseRange(t *testing.T) {
	cases := []struct {
		spec       string
		size       int64
		start, end int64
		ok         bool
	}{
		{"bytes=0-9", 100, 0, 10, true},
		{"bytes=90-", 100, 90, 100, true},
		{"bytes=-10", 100, 90, 100, true},
		{"bytes=-200", 100, 0, 100, true},
		{"bytes=50-1000", 100, 50, 100, true},
		{"bytes=100-", 100, 0, 0, false},
		{"bytes=5-2", 100, 0, 0, false},
		{"bytes=0-0", 0, 0, 0, false},
		{"bytes=0-1,5-6", 100, 0, 0, false},
		{"items=0-1", 100, 0, 0, false},
	}
	for _, tc := range cases {
		start, end, ok := parseRange(tc.spec, tc.size)
		if ok != tc.ok || (ok && (start != tc.start || end != tc.end)) {
			t.Errorf("parseRange(%q, %d) = (%d, %d, %v), want (%d, %d, %v)", tc.spec, tc.size, start, end, ok, tc.start, tc.end, tc.ok)
		}
	}
}

func TestConcurrentWriters(t *testing.T) {
	ctx := t.Context()
	client, _ := newTestClient(t, WithBuckets("b"))
	errCh := make(chan error, 32)
	for i := 0; i < 32; i++ {
		go func(i int) {
			key := fmt.Sprintf("k%d", i%4)
			_, err := client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("b"), Key: aws.String(key), Body: strings.NewReader(strings.Repeat("x", i+1))})
			if err == nil {
				_, err = client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("b"), Key: aws.String(key)})
			}
			errCh <- err
		}(i)
	}
	for i := 0; i < 32; i++ {
		if err := <-errCh; err != nil {
			t.Errorf("concurrent op: %v", err)
		}
	}
	if got := listKeys(t, client, "b", ""); len(got) != 4 {
		t.Errorf("expected 4 keys, got %v", got)
	}
}

var _ = context.Background
