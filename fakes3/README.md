# fakes3

An in-memory, unauthenticated implementation of the subset of the S3 REST API
that this repository's object storage backends use. It exists so that tests
and local development do not depend on a third-party S3-compatible server
image.

It is a separate Go module (and `ap` root) with no dependencies outside the
standard library at runtime; the AWS SDK is used only in its tests.

## Running

```bash
go run ./cmd/fakes3 --listen=:9000 --buckets=my-bucket
```

Point an S3 client at `http://localhost:9000` with path-style addressing and
any credentials, for example:

```bash
AWS_ACCESS_KEY_ID=x AWS_SECRET_ACCESS_KEY=x \
  aws --endpoint-url http://localhost:9000 s3 ls s3://my-bucket/
```

With `--listen=127.0.0.1:0` it picks a free port and prints
`fakes3 listening on http://127.0.0.1:PORT` on stdout, which is how the root
module's S3 conformance test drives it.

Flags: `--listen`, `--buckets` (comma-separated, created at startup),
`--auto-create-buckets` (default true: a PutObject to an unknown bucket
creates it), `--region`, `--quiet`.

## Container image

`images/fakes3/Dockerfile` builds a static binary into a distroless image;
`ap build` picks it up automatically. The end-to-end tests build it as
`fakes3:e2e` and deploy it into kind in place of MinIO.

## Supported API

CreateBucket, HeadBucket, DeleteBucket, ListBuckets, GetBucketLocation,
PutObject (including `aws-chunked` bodies and `x-amz-checksum-*` /
`Content-MD5` validation), CopyObject, GetObject (with `Range`, `If-Match`,
`If-None-Match`), HeadObject, DeleteObject, DeleteObjects, ListObjects and
ListObjectsV2 (prefix, delimiter, pagination, start-after). Path-style and
virtual-hosted-style addressing both work; presigned URLs are served as plain
GETs since signatures are never checked.

Not supported: authentication, multipart uploads, versioning, ACLs,
lifecycle, and other bucket sub-resources. Nothing is persisted; all data is
lost when the process exits.

## Simulating outages

Tests can simulate an unreachable object store without restarting the server or
losing stored objects. While an outage is in effect, every S3 request is answered
with `503 Service Unavailable` (`ServiceUnavailable` error body) and changes
nothing. When the outage ends, stored objects are served normally again.

The outage switch is available over HTTP on the same listener via `/_fakes3/outage`
(the `/_fakes3/` path prefix is reserved and never treated as a bucket):

- `POST /_fakes3/outage?duration=45s`: starts an outage for the specified duration.
  The outage ends automatically once the duration has passed.
- `POST /_fakes3/outage?duration=0s`: ends an active outage immediately.
- `GET /_fakes3/outage`: reports the remaining outage duration (e.g. `45s`, or `0s`
  when no outage is active).

