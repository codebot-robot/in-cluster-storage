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

package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type options struct {
	serverAddr string
}

func dialServer(addr string) (pb.ObjectFSControllerClient, func() error, error) {
	if addr == "" {
		return nil, nil, fmt.Errorf("--server is required")
	}

	target := addr
	if strings.HasPrefix(target, "tcp://") {
		target = strings.TrimPrefix(target, "tcp://")
	}

	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to server at %s: %w", addr, err)
	}

	client := pb.NewObjectFSControllerClient(conn)
	return client, conn.Close, nil
}

func NewRootCommand() *cobra.Command {
	opts := &options{}

	rootCmd := &cobra.Command{
		Use:           "objectfsctl",
		Aliases:       []string{"objectfs"},
		Short:         "objectfs administrative CLI",
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	rootCmd.PersistentFlags().StringVar(&opts.serverAddr, "server", "", "ObjectFS server address (e.g. localhost:50051 or unix:///path/to/socket)")

	rootCmd.AddCommand(newBlobsCommand(opts))
	rootCmd.AddCommand(newVolumesCommand(opts))
	rootCmd.AddCommand(newSnapshotsCommand(opts))
	rootCmd.AddCommand(newMountCommand(opts))

	return rootCmd
}

func newBlobsCommand(opts *options) *cobra.Command {
	blobsCmd := &cobra.Command{
		Use:     "blobs",
		Aliases: []string{"blob"},
		Short:   "Manage underlying objects/blobs in the store",
	}

	blobsCmd.AddCommand(newBlobsListCommand(opts))
	blobsCmd.AddCommand(newBlobsGetCommand(opts))

	return blobsCmd
}

type blobsListOptions struct {
	*options
	from   string
	limit  int32
	prefix string
}

func newBlobsListCommand(opts *options) *cobra.Command {
	listOpts := &blobsListOptions{options: opts}
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List all blobs in the store",
		RunE: func(cmd *cobra.Command, args []string) error {
			if listOpts.serverAddr == "" {
				return fmt.Errorf("--server is required")
			}

			client, closeConn, err := dialServer(listOpts.serverAddr)
			if err != nil {
				return err
			}
			defer closeConn()

			fromSHA := listOpts.from
			remainingLimit := listOpts.limit
			out := cmd.OutOrStdout()

			for {
				reqLimit := remainingLimit
				resp, err := client.ListBlobs(cmd.Context(), &pb.ListBlobsRequest{
					FromSha:   fromSHA,
					Limit:     reqLimit,
					ShaPrefix: listOpts.prefix,
				})
				if err != nil {
					return fmt.Errorf("failed to list blobs: %w", err)
				}

				for _, sha := range resp.GetSha256() {
					fmt.Fprintln(out, sha)
					fromSHA = sha
					if remainingLimit > 0 {
						remainingLimit--
						if remainingLimit == 0 {
							return nil
						}
					}
				}

				if resp.GetEndOfData() || len(resp.GetSha256()) == 0 {
					break
				}
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&listOpts.from, "from", "", "List blobs starting after this SHA256")
	cmd.Flags().Int32Var(&listOpts.limit, "limit", 0, "Maximum number of blobs to list (0 = no limit)")
	cmd.Flags().StringVar(&listOpts.prefix, "prefix", "", "Filter blobs starting with this SHA256 prefix")
	return cmd
}

type blobsGetOptions struct {
	*options
	offset int64
	limit  int64
}

func newBlobsGetCommand(opts *options) *cobra.Command {
	getOpts := &blobsGetOptions{options: opts}
	cmd := &cobra.Command{
		Use:   "get <sha256>",
		Short: "Print the contents of a blob to stdout",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if getOpts.serverAddr == "" {
				return fmt.Errorf("--server is required")
			}

			sha := args[0]
			client, closeConn, err := dialServer(getOpts.serverAddr)
			if err != nil {
				return err
			}
			defer closeConn()

			stream, err := client.GetBlob(cmd.Context(), &pb.GetBlobRequest{
				Sha256: sha,
				Offset: getOpts.offset,
				Limit:  getOpts.limit,
			})
			if err != nil {
				return fmt.Errorf("failed to get blob: %w", err)
			}

			out := cmd.OutOrStdout()
			for {
				chunk, err := stream.Recv()
				if err == io.EOF {
					break
				}
				if err != nil {
					return fmt.Errorf("failed receiving blob data: %w", err)
				}
				if len(chunk.GetData()) > 0 {
					if _, err := out.Write(chunk.GetData()); err != nil {
						return fmt.Errorf("failed writing to output: %w", err)
					}
				}
			}
			return nil
		},
	}
	cmd.Flags().Int64Var(&getOpts.offset, "offset", 0, "Offset in bytes to start reading from")
	cmd.Flags().Int64Var(&getOpts.limit, "limit", 0, "Maximum number of bytes to read (0 = no limit)")
	return cmd
}

func newVolumesCommand(opts *options) *cobra.Command {
	volumesCmd := &cobra.Command{
		Use:     "volumes",
		Aliases: []string{"volume", "vol"},
		Short:   "Manage volumes in the store",
	}

	volumesCmd.AddCommand(newVolumesListCommand(opts))

	return volumesCmd
}

type volumesListOptions struct {
	*options
	from  string
	limit int32
}

func newVolumesListCommand(opts *options) *cobra.Command {
	listOpts := &volumesListOptions{options: opts}
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List all volumes in the store",
		RunE: func(cmd *cobra.Command, args []string) error {
			if listOpts.serverAddr == "" {
				return fmt.Errorf("--server is required")
			}

			client, closeConn, err := dialServer(listOpts.serverAddr)
			if err != nil {
				return err
			}
			defer closeConn()

			fromVol := listOpts.from
			remainingLimit := listOpts.limit
			out := cmd.OutOrStdout()

			for {
				reqLimit := remainingLimit
				resp, err := client.ListVolumes(cmd.Context(), &pb.ListVolumesRequest{
					FromVolumeId: fromVol,
					Limit:        reqLimit,
				})
				if err != nil {
					return fmt.Errorf("failed to list volumes: %w", err)
				}

				for _, vol := range resp.GetVolumes() {
					fmt.Fprintln(out, vol.GetVolumeId())
					fromVol = vol.GetVolumeId()
					if remainingLimit > 0 {
						remainingLimit--
						if remainingLimit == 0 {
							return nil
						}
					}
				}

				if resp.GetEndOfData() || len(resp.GetVolumes()) == 0 {
					break
				}
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&listOpts.from, "from", "", "List volumes starting after this volume ID")
	cmd.Flags().Int32Var(&listOpts.limit, "limit", 0, "Maximum number of volumes to list (0 = no limit)")
	return cmd
}

func newSnapshotsCommand(opts *options) *cobra.Command {
	snapshotsCmd := &cobra.Command{
		Use:     "snapshots",
		Aliases: []string{"snapshot", "snap"},
		Short:   "Manage snapshots for volumes",
	}

	snapshotsCmd.AddCommand(newSnapshotsListCommand(opts))
	snapshotsCmd.AddCommand(newSnapshotCreateCommand(opts))

	return snapshotsCmd
}

type snapshotsListOptions struct {
	*options
	volumeID string
	from     string
	fromTime string
	to       string
	toTime   string
	limit    int32
}

func parseTimeFlag(val string) (time.Time, bool) {
	if val == "" {
		return time.Time{}, false
	}
	formats := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05",
		"2006-01-02",
		"20060102T150405.000000Z",
		"20060102T150405Z",
	}
	for _, f := range formats {
		if t, err := time.Parse(f, strings.TrimSuffix(val, ".erofs")); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

func newSnapshotsListCommand(opts *options) *cobra.Command {
	listOpts := &snapshotsListOptions{options: opts}
	cmd := &cobra.Command{
		Use:     "list [volume_id]",
		Aliases: []string{"ls"},
		Short:   "List snapshots for a volume, paginated by time",
		Args:    cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if listOpts.serverAddr == "" {
				return fmt.Errorf("--server is required")
			}

			volID := listOpts.volumeID
			if len(args) > 0 && args[0] != "" {
				volID = args[0]
			}
			if volID == "" {
				return fmt.Errorf("volume_id is required")
			}

			client, closeConn, err := dialServer(listOpts.serverAddr)
			if err != nil {
				return err
			}
			defer closeConn()

			var fromTimePb, toTimePb *timestamppb.Timestamp
			fromSnapshot := ""

			if listOpts.fromTime != "" {
				if t, ok := parseTimeFlag(listOpts.fromTime); ok {
					fromTimePb = timestamppb.New(t)
				}
			}

			if listOpts.from != "" {
				if strings.HasSuffix(listOpts.from, ".erofs") {
					fromSnapshot = listOpts.from
				} else if t, ok := parseTimeFlag(listOpts.from); ok && listOpts.fromTime == "" {
					fromTimePb = timestamppb.New(t)
				} else {
					fromSnapshot = listOpts.from
				}
			}

			toVal := listOpts.to
			if listOpts.toTime != "" {
				toVal = listOpts.toTime
			}
			if toVal != "" {
				if t, ok := parseTimeFlag(toVal); ok {
					toTimePb = timestamppb.New(t)
				}
			}

			remainingLimit := listOpts.limit
			out := cmd.OutOrStdout()

			for {
				reqLimit := remainingLimit
				resp, err := client.ListSnapshots(cmd.Context(), &pb.ListSnapshotsRequest{
					VolumeId:     volID,
					FromTime:     fromTimePb,
					ToTime:       toTimePb,
					Limit:        reqLimit,
					FromSnapshot: fromSnapshot,
				})
				if err != nil {
					return fmt.Errorf("failed to list snapshots: %w", err)
				}

				for _, snap := range resp.GetSnapshots() {
					fmt.Fprintln(out, snap.GetName())
					fromSnapshot = snap.GetName()
					if remainingLimit > 0 {
						remainingLimit--
						if remainingLimit == 0 {
							return nil
						}
					}
				}

				if resp.GetEndOfData() || len(resp.GetSnapshots()) == 0 {
					break
				}
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&listOpts.volumeID, "volume", "", "Volume ID")
	cmd.Flags().StringVar(&listOpts.from, "from", "", "List snapshots created at or after this time, or after this snapshot name")
	cmd.Flags().StringVar(&listOpts.fromTime, "from-time", "", "List snapshots created at or after this time")
	cmd.Flags().StringVar(&listOpts.to, "to", "", "List snapshots created at or before this time")
	cmd.Flags().StringVar(&listOpts.toTime, "to-time", "", "List snapshots created at or before this time")
	cmd.Flags().Int32Var(&listOpts.limit, "limit", 0, "Maximum number of snapshots to list (0 = no limit)")
	return cmd
}

type snapshotCreateOptions struct {
	*options
	volumeID string
}

func newSnapshotCreateCommand(opts *options) *cobra.Command {
	createOpts := &snapshotCreateOptions{options: opts}
	cmd := &cobra.Command{
		Use:     "create <volume_id>",
		Aliases: []string{"new", "add"},
		Short:   "Create a new snapshot for a volume",
		Args:    cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if createOpts.serverAddr == "" {
				return fmt.Errorf("--server is required")
			}

			volID := createOpts.volumeID
			if len(args) > 0 && args[0] != "" {
				volID = args[0]
			}
			if volID == "" {
				return fmt.Errorf("volume_id is required")
			}

			client, closeConn, err := dialServer(createOpts.serverAddr)
			if err != nil {
				return err
			}
			defer closeConn()

			resp, err := client.CreateSnapshot(cmd.Context(), &pb.CreateSnapshotRequest{
				VolumeId: volID,
			})
			if err != nil {
				return fmt.Errorf("failed to create snapshot: %w", err)
			}

			out := cmd.OutOrStdout()
			fmt.Fprintln(out, resp.GetSnapshotName())
			return nil
		},
	}
	cmd.Flags().StringVar(&createOpts.volumeID, "volume", "", "Volume ID")
	return cmd
}

func main() {
	rootCmd := NewRootCommand()
	if err := rootCmd.ExecuteContext(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
