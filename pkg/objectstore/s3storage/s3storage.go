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

package s3storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/gke-labs/in-cluster-storage/pkg/errutil"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/blob"
)

// Config holds configuration options for initializing an S3 backend.
type Config struct {
	Bucket       string
	Prefix       string
	Endpoint     string
	Region       string
	UsePathStyle bool
}

// Backend is an AWS S3 / MinIO implementation of ObjectStorageBackend.
type Backend struct {
	client        *s3.Client
	presignClient *s3.PresignClient
	bucket        string
	prefix        string
}

// New creates a new S3 backend with the provided configuration.
func New(ctx context.Context, cfg Config) (*Backend, error) {
	if cfg.Bucket == "" {
		return nil, errors.New("s3 bucket must be specified")
	}

	var optFns []func(*config.LoadOptions) error
	if cfg.Region != "" {
		optFns = append(optFns, config.WithRegion(cfg.Region))
	} else if os.Getenv("AWS_REGION") == "" && os.Getenv("AWS_DEFAULT_REGION") == "" {
		optFns = append(optFns, config.WithRegion("us-east-1"))
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, optFns...)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	var s3OptFns []func(*s3.Options)

	endpoint := cfg.Endpoint
	if endpoint == "" {
		endpoint = os.Getenv("AWS_ENDPOINT_URL_S3")
		if endpoint == "" {
			endpoint = os.Getenv("AWS_ENDPOINT_URL")
		}
	}

	if endpoint != "" {
		s3OptFns = append(s3OptFns, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(endpoint)
		})
	}

	usePathStyle := cfg.UsePathStyle
	if !usePathStyle && endpoint != "" {
		// MinIO and custom endpoints typically require path-style addressing
		usePathStyle = true
	}
	if usePathStyle {
		s3OptFns = append(s3OptFns, func(o *s3.Options) {
			o.UsePathStyle = true
		})
	}

	client := s3.NewFromConfig(awsCfg, s3OptFns...)
	presignClient := s3.NewPresignClient(client)

	prefix := strings.Trim(cfg.Prefix, "/")

	return &Backend{
		client:        client,
		presignClient: presignClient,
		bucket:        cfg.Bucket,
		prefix:        prefix,
	}, nil
}

func storageKey(volumeID, key string) string {
	if volumeID == "" || strings.HasPrefix(key, "volumes/") || strings.HasPrefix(key, "blobs/") {
		return key
	}
	return fmt.Sprintf("%s/%s", volumeID, key)
}

func (b *Backend) fullKey(volumeID, key string) string {
	k := storageKey(volumeID, key)
	if b.prefix == "" {
		return k
	}
	return fmt.Sprintf("%s/%s", b.prefix, k)
}

func (b *Backend) relativeKey(fullKey string) string {
	if b.prefix == "" {
		return fullKey
	}
	return strings.TrimPrefix(strings.TrimPrefix(fullKey, b.prefix), "/")
}

func (b *Backend) PutObject(ctx context.Context, volumeID, key string, stream blob.ByteStream) (string, error) {
	if err := stream.Rewind(); err != nil {
		return "", fmt.Errorf("failed to rewind stream: %w", err)
	}

	s3Key := b.fullKey(volumeID, key)
	length := stream.Length()

	input := &s3.PutObjectInput{
		Bucket:        aws.String(b.bucket),
		Key:           aws.String(s3Key),
		Body:          stream,
		ContentLength: aws.Int64(length),
	}

	out, err := b.client.PutObject(ctx, input)
	if err != nil {
		return "", fmt.Errorf("failed to put s3 object %s: %w", s3Key, err)
	}

	etag := ""
	if out.ETag != nil {
		etag = strings.Trim(*out.ETag, "\"")
	}
	return etag, nil
}

func (b *Backend) GetObject(ctx context.Context, volumeID, key string, offset, length int64, w io.Writer) error {
	s3Key := b.fullKey(volumeID, key)
	input := &s3.GetObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(s3Key),
	}

	if offset > 0 || length > 0 {
		var rangeHeader string
		if length > 0 {
			rangeHeader = fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
		} else {
			rangeHeader = fmt.Sprintf("bytes=%d-", offset)
		}
		input.Range = aws.String(rangeHeader)
	}

	out, err := b.client.GetObject(ctx, input)
	if err != nil {
		var respErr *s3types.NoSuchKey
		if errors.As(err, &respErr) {
			return fmt.Errorf("object %s not found in volume %s: %w", key, volumeID, err)
		}
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			if apiErr.ErrorCode() == "NoSuchKey" || apiErr.ErrorCode() == "NotFound" {
				return fmt.Errorf("object %s not found in volume %s: %w", key, volumeID, err)
			}
			// HTTP 416 Requested Range Not Satisfiable: offset >= size -> 0 bytes
			if apiErr.ErrorCode() == "InvalidRange" {
				return nil
			}
		}
		return fmt.Errorf("failed to get s3 object %s: %w", s3Key, err)
	}
	defer errutil.CloseAndLog(out.Body, "s3 response body")

	if _, err := io.Copy(w, out.Body); err != nil {
		return fmt.Errorf("failed to stream s3 object %s: %w", s3Key, err)
	}
	return nil
}

func (b *Backend) DeleteObject(ctx context.Context, volumeID, key string) error {
	s3Key := b.fullKey(volumeID, key)
	input := &s3.DeleteObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(s3Key),
	}
	_, err := b.client.DeleteObject(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to delete s3 object %s: %w", s3Key, err)
	}
	return nil
}

func (b *Backend) GetRedirectURL(ctx context.Context, volumeID, key string) (string, error) {
	s3Key := b.fullKey(volumeID, key)
	input := &s3.GetObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(s3Key),
	}
	req, err := b.presignClient.PresignGetObject(ctx, input, s3.WithPresignExpires(15*time.Minute))
	if err != nil {
		return "", fmt.Errorf("failed to presign s3 object %s: %w", s3Key, err)
	}
	return req.URL, nil
}

func (b *Backend) ListObjects(ctx context.Context, volumeID, prefix string) ([]string, error) {
	fullPrefix := b.fullKey(volumeID, prefix)
	paginator := s3.NewListObjectsV2Paginator(b.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(b.bucket),
		Prefix: aws.String(fullPrefix),
	})

	var matches []string
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list s3 objects with prefix %s: %w", fullPrefix, err)
		}
		for _, obj := range page.Contents {
			if obj.Key != nil {
				matches = append(matches, b.relativeKey(*obj.Key))
			}
		}
	}
	return matches, nil
}
