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

// Package fuseholder implements a holder process that owns /dev/fuse descriptors
// and hands them over a Unix domain socket.
//
// Requests and responses are newline-delimited JSON; when a response carries a
// descriptor, it is transmitted via SCM_RIGHTS ancillary data alongside the first
// byte of the response using WriteMsgUnix.
//
// If the holder itself restarts, its descriptors are gone and Get returns
// not-found; the holder is upgraded rarely and never as part of a daemon rollout.
package fuseholder

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"

	"golang.org/x/sys/unix"
	"k8s.io/klog/v2"
)

// ErrNotFound is returned when a requested mount target is not found in the holder.
var ErrNotFound = errors.New("target not found")

// Entry holds the metadata of an active FUSE mount.
type Entry struct {
	Target   string   `json:"target"`
	VolumeID string   `json:"volume_id"`
	Mode     string   `json:"mode"`
	FsName   string   `json:"fs_name,omitempty"`
	Name     string   `json:"name,omitempty"`
	MaxRead  int      `json:"max_read,omitempty"`
	Options  []string `json:"options,omitempty"`
}

// MountRequest requests mounting a FUSE filesystem.
type MountRequest struct {
	Entry
}

// GetRequest requests a descriptor and metadata for an existing mount.
type GetRequest struct {
	Target string `json:"target"`
}

// ListRequest requests metadata for all held mounts.
type ListRequest struct{}

// UnmountRequest requests unmounting a target.
type UnmountRequest struct {
	Target string `json:"target"`
	Detach bool   `json:"detach,omitempty"`
}

// Request envelopes the four holder request types.
type Request struct {
	Mount   *MountRequest   `json:"mount,omitempty"`
	Get     *GetRequest     `json:"get,omitempty"`
	List    *ListRequest    `json:"list,omitempty"`
	Unmount *UnmountRequest `json:"unmount,omitempty"`
}

// Response represents a reply from the holder.
type Response struct {
	Error   string  `json:"error,omitempty"`
	Entry   *Entry  `json:"entry,omitempty"`
	Entries []Entry `json:"entries,omitempty"`
}

func parseDescriptor(outOfBand []byte) (int, error) {
	if len(outOfBand) == 0 {
		return -1, nil
	}
	controlMessages, err := unix.ParseSocketControlMessage(outOfBand)
	if err != nil {
		return -1, fmt.Errorf("parse socket control messages: %w", err)
	}
	for _, controlMessage := range controlMessages {
		descriptors, err := unix.ParseUnixRights(&controlMessage)
		if err != nil {
			return -1, fmt.Errorf("parse unix rights: %w", err)
		}
		if len(descriptors) > 0 {
			return descriptors[0], nil
		}
	}
	return -1, nil
}

func sendResponse(connection *net.UnixConn, response Response, fd int) error {
	payload, err := json.Marshal(response)
	if err != nil {
		return err
	}
	payload = append(payload, '\n')
	var outOfBand []byte
	if fd >= 0 {
		outOfBand = unix.UnixRights(fd)
	}
	bytesWritten, _, err := connection.WriteMsgUnix(payload[:1], outOfBand, nil)
	if err != nil {
		return err
	}
	if len(payload) > 1 {
		if _, err := connection.Write(payload[bytesWritten:]); err != nil {
			return err
		}
	}
	return nil
}

func discardDescriptor(fd int) {
	if fd >= 0 {
		if err := unix.Close(fd); err != nil {
			klog.Warningf("failed to close discarded descriptor %d: %v", fd, err)
		}
	}
}
