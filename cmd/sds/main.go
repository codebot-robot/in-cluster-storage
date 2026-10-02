// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"

	sdsv1 "github.com/gke-labs/in-cluster-storage/pkg/api/sds/v1"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/record"
	"github.com/gke-labs/in-cluster-storage/pkg/wal"
	walclient "github.com/gke-labs/in-cluster-storage/pkg/wal/client"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type catOptions struct {
	server   string
	stream   string
	fromSeq  uint64
	segment  string
	registry string
}

func formatRecord(seq uint64, payload []byte, reg *record.Registry) (string, error) {
	typeID, body, err := record.SplitFrame(payload)
	if err != nil {
		return "", err
	}

	switch typeID {
	case record.TypeIDTypeDefinition:
		def := &sdsv1.TypeDefinition{}
		if err := proto.Unmarshal(body, def); err != nil {
			return "", fmt.Errorf("failed to unmarshal TypeDefinition: %w", err)
		}
		_ = reg.Register(def)
		b, err := protojson.Marshal(def)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%d\t%d\tsds.v1.TypeDefinition\t%s", seq, typeID, string(b)), nil

	case record.TypeIDTxCommit:
		commit := &sdsv1.TxCommit{}
		if err := proto.Unmarshal(body, commit); err != nil {
			return "", fmt.Errorf("failed to unmarshal TxCommit: %w", err)
		}
		b, err := protojson.Marshal(commit)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%d\t%d\tsds.v1.TxCommit\t%s", seq, typeID, string(b)), nil

	case record.TypeIDSnapshotPointer:
		ptr := &sdsv1.SnapshotPointer{}
		if err := proto.Unmarshal(body, ptr); err != nil {
			return "", fmt.Errorf("failed to unmarshal SnapshotPointer: %w", err)
		}
		b, err := protojson.Marshal(ptr)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%d\t%d\tsds.v1.SnapshotPointer\t%s", seq, typeID, string(b)), nil

	case record.TypeIDPadding:
		return fmt.Sprintf("%d\t%d\tsds.v1.Padding\t%s", seq, typeID, hex.EncodeToString(body)), nil

	case record.TypeIDOpRecord:
		opRec := &sdsv1.OpRecord{}
		if err := proto.Unmarshal(body, opRec); err != nil {
			return "", fmt.Errorf("failed to unmarshal OpRecord: %w", err)
		}
		b, err := protojson.Marshal(opRec)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%d\t%d\tsds.v1.OpRecord\t%s", seq, typeID, string(b)), nil

	default:
		// Application type ID >= 16 (or custom type)
		if reg != nil {
			msgType, err := reg.ResolveMessageType(typeID)
			if err == nil && msgType != nil {
				msg := msgType.New().Interface()
				if err := proto.Unmarshal(body, msg); err == nil {
					typeName := string(msg.ProtoReflect().Descriptor().FullName())
					b, err := protojson.Marshal(msg)
					if err == nil {
						return fmt.Sprintf("%d\t%d\t%s\t%s", seq, typeID, typeName, string(b)), nil
					}
				}
			}
		}
		return fmt.Sprintf("%d\t%d\tunknown\t%s", seq, typeID, hex.EncodeToString(body)), nil
	}
}

func runCat(cmd *cobra.Command, opts *catOptions) error {
	reg := record.NewRegistry()
	if opts.registry != "" {
		data, err := os.ReadFile(opts.registry)
		if err != nil {
			return fmt.Errorf("failed to read registry file %s: %w", opts.registry, err)
		}
		regProto := &sdsv1.Registry{}
		if err := proto.Unmarshal(data, regProto); err != nil {
			return fmt.Errorf("failed to unmarshal registry proto from %s: %w", opts.registry, err)
		}
		if err := reg.Import(regProto); err != nil {
			return fmt.Errorf("failed to import registry: %w", err)
		}
	}

	out := cmd.OutOrStdout()

	if opts.server != "" {
		if opts.segment != "" {
			return errors.New("cannot specify both --server and --segment")
		}
		if opts.stream == "" {
			return errors.New("--stream is required when --server is specified")
		}
		streamID, err := uuid.Parse(opts.stream)
		if err != nil {
			return fmt.Errorf("invalid stream UUID %q: %w", opts.stream, err)
		}

		ctx := cmd.Context()
		tailIter, err := walclient.TailStream(ctx, opts.server, streamID, opts.fromSeq)
		if err != nil {
			return fmt.Errorf("failed to tail stream from server %s: %w", opts.server, err)
		}

		for seq, payload := range tailIter {
			line, err := formatRecord(seq, payload, reg)
			if err != nil {
				return err
			}
			fmt.Fprintln(out, line)
		}
		return nil
	}

	if opts.segment != "" {
		var filterStreamID uuid.UUID
		if opts.stream != "" {
			var err error
			filterStreamID, err = uuid.Parse(opts.stream)
			if err != nil {
				return fmt.Errorf("invalid stream UUID %q: %w", opts.stream, err)
			}
		}

		f, err := os.Open(opts.segment)
		if err != nil {
			return fmt.Errorf("failed to open segment file %s: %w", opts.segment, err)
		}
		defer f.Close()

		var magic [4]byte
		if _, err := io.ReadFull(f, magic[:]); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("failed to read segment header: %w", err)
		}
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return fmt.Errorf("failed to seek segment file: %w", err)
		}

		if magic == wal.LogMagicBytes {
			for {
				rec, err := wal.DecodeLogRecord(f)
				if err != nil {
					if errors.Is(err, io.EOF) || errors.Is(err, wal.ErrTruncatedRecord) {
						break
					}
					return fmt.Errorf("failed decoding log record in %s: %w", opts.segment, err)
				}
				if filterStreamID != uuid.Nil && rec.StreamID != filterStreamID {
					continue
				}
				line, err := formatRecord(rec.StreamSeq, rec.Payload, reg)
				if err != nil {
					return err
				}
				fmt.Fprintln(out, line)
			}
			return nil
		} else if magic == wal.ClientMagicBytes {
			for {
				rec, err := wal.DecodeClientRecord(f)
				if err != nil {
					if errors.Is(err, io.EOF) || errors.Is(err, wal.ErrTruncatedRecord) {
						break
					}
					return fmt.Errorf("failed decoding client record in %s: %w", opts.segment, err)
				}
				if filterStreamID != uuid.Nil && rec.StreamID != filterStreamID {
					continue
				}
				line, err := formatRecord(rec.StreamSeq, rec.Payload, reg)
				if err != nil {
					return err
				}
				fmt.Fprintln(out, line)
			}
			return nil
		}
		return fmt.Errorf("unrecognized segment format magic %q", string(magic[:]))
	}

	return errors.New("either --server or --segment must be specified")
}

// NewRootCommand creates the root cobra command for sds.
func NewRootCommand() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:           "sds",
		Short:         "Structured Data Streams inspection and management CLI",
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	opts := &catOptions{}
	catCmd := &cobra.Command{
		Use:   "cat",
		Short: "Decode and inspect stream payloads in live or offline streams",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runCat(cmd, opts)
		},
	}

	catCmd.Flags().StringVar(&opts.server, "server", "", "WAL buffer service target (e.g. localhost:50051)")
	catCmd.Flags().StringVar(&opts.stream, "stream", "", "Stream UUID")
	catCmd.Flags().Uint64Var(&opts.fromSeq, "from-seq", 0, "Lower bound stream sequence number (exclusive)")
	catCmd.Flags().StringVar(&opts.segment, "segment", "", "Offline sealed segment file path")
	catCmd.Flags().StringVar(&opts.registry, "registry", "", "Path to serialized sds.v1.Registry file")

	rootCmd.AddCommand(catCmd)
	return rootCmd
}

func main() {
	cmd := NewRootCommand()
	if err := cmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
