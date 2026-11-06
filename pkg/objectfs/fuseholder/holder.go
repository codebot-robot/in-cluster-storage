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

package fuseholder

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"golang.org/x/sys/unix"
	"k8s.io/klog/v2"
)

type heldMount struct {
	entry Entry
	fd    int
}

// Listen creates a Unix domain socket listener that publishes atomically to socketPath.
// It listens on a temporary path in the same directory and renames it over socketPath,
// disabling unlink-on-close so shutting down the listener does not remove a successor's socket.
func Listen(socketPath string) (net.Listener, error) {
	directory := filepath.Dir(socketPath)
	if err := os.MkdirAll(directory, 0755); err != nil {
		return nil, fmt.Errorf("create socket directory: %w", err)
	}

	var randomBytes [4]byte
	if _, err := rand.Read(randomBytes[:]); err != nil {
		return nil, fmt.Errorf("generate random suffix: %w", err)
	}
	temporaryPath := filepath.Join(directory, fmt.Sprintf(".%s.%d.%x.temporary", filepath.Base(socketPath), os.Getpid(), randomBytes))

	listener, err := net.Listen("unix", temporaryPath)
	if err != nil {
		return nil, fmt.Errorf("listen on temporary socket %s: %w", temporaryPath, err)
	}

	unixListener, ok := listener.(*net.UnixListener)
	if !ok {
		if closeErr := listener.Close(); closeErr != nil && !errors.Is(closeErr, net.ErrClosed) {
			klog.Warningf("failed to close listener: %v", closeErr)
		}
		if removeErr := os.Remove(temporaryPath); removeErr != nil && !os.IsNotExist(removeErr) {
			klog.Warningf("failed to remove temporary socket %s: %v", temporaryPath, removeErr)
		}
		return nil, errors.New("listener is not a Unix listener")
	}
	unixListener.SetUnlinkOnClose(false)

	if err := os.Rename(temporaryPath, socketPath); err != nil {
		if closeErr := unixListener.Close(); closeErr != nil && !errors.Is(closeErr, net.ErrClosed) {
			klog.Warningf("failed to close listener after rename failure: %v", closeErr)
		}
		if removeErr := os.Remove(temporaryPath); removeErr != nil && !os.IsNotExist(removeErr) {
			klog.Warningf("failed to remove temporary socket %s: %v", temporaryPath, removeErr)
		}
		return nil, fmt.Errorf("rename socket from %s to %s: %w", temporaryPath, socketPath, err)
	}

	return unixListener, nil
}

// Holder manages active FUSE mount descriptors and serves requests over a Unix domain socket.
type Holder struct {
	mu          sync.Mutex
	mounts      map[string]*heldMount
	listener    net.Listener
	mountFunc   func(request MountRequest) (int, error)
	unmountFunc func(target string, detach bool) error
}

// New creates a new Holder with production mount and unmount implementations.
func New() *Holder {
	return &Holder{
		mounts:      make(map[string]*heldMount),
		mountFunc:   mountFUSE,
		unmountFunc: unmountFUSE,
	}
}

// Mount mounts a FUSE filesystem at target and tracks the descriptor.
func (h *Holder) Mount(request MountRequest) (int, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, exists := h.mounts[request.Target]; exists {
		return -1, fmt.Errorf("target %s already mounted", request.Target)
	}

	fd, err := h.mountFunc(request)
	if err != nil {
		return -1, err
	}

	h.mounts[request.Target] = &heldMount{
		entry: request.Entry,
		fd:    fd,
	}
	return fd, nil
}

// Get returns the descriptor and metadata of an existing mount.
func (h *Holder) Get(target string) (int, Entry, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	held, ok := h.mounts[target]
	if !ok {
		return -1, Entry{}, ErrNotFound
	}
	return held.fd, held.entry, nil
}

// List returns metadata for all held mounts sorted by target path.
func (h *Holder) List() []Entry {
	h.mu.Lock()
	defer h.mu.Unlock()
	entries := make([]Entry, 0, len(h.mounts))
	for _, held := range h.mounts {
		entries = append(entries, held.entry)
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Target < entries[j].Target
	})
	return entries
}

// Unmount unmounts the target, closes the held descriptor, and removes the entry.
func (h *Holder) Unmount(target string, detach bool) error {
	h.mu.Lock()
	held, ok := h.mounts[target]
	if !ok {
		h.mu.Unlock()
		return ErrNotFound
	}
	h.mu.Unlock()

	if err := h.unmountFunc(target, detach); err != nil {
		return fmt.Errorf("unmount %s: %w", target, err)
	}

	h.mu.Lock()
	delete(h.mounts, target)
	h.mu.Unlock()

	if closeErr := unix.Close(held.fd); closeErr != nil {
		return fmt.Errorf("close descriptor for %s: %w", target, closeErr)
	}
	return nil
}

// Serve accepts connections on listener and serves holder requests.
func (h *Holder) Serve(listener net.Listener) error {
	h.mu.Lock()
	h.listener = listener
	h.mu.Unlock()
	for {
		connection, err := listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		unixConnection, ok := connection.(*net.UnixConn)
		if !ok {
			if closeErr := connection.Close(); closeErr != nil {
				return closeErr
			}
			continue
		}
		go h.handleConnection(unixConnection)
	}
}

// Close closes the listener and all held file descriptors.
func (h *Holder) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	var errs []error
	if h.listener != nil {
		if err := h.listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			errs = append(errs, err)
		}
	}
	for target, held := range h.mounts {
		if err := unix.Close(held.fd); err != nil {
			errs = append(errs, err)
		}
		delete(h.mounts, target)
	}
	return errors.Join(errs...)
}

func (h *Holder) handleConnection(connection *net.UnixConn) {
	defer func() {
		if closeErr := connection.Close(); closeErr != nil && !errors.Is(closeErr, net.ErrClosed) {
			klog.Warningf("failed to close client connection: %v", closeErr)
		}
	}()
	decoder := json.NewDecoder(connection)
	for {
		var request Request
		if err := decoder.Decode(&request); err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				klog.Warningf("failed to decode client request: %v", err)
			}
			return
		}
		response, responseFd := h.dispatch(request)
		if err := sendResponse(connection, response, responseFd); err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				klog.Warningf("failed to send response to client: %v", err)
			}
			return
		}
	}
}

func (h *Holder) dispatch(request Request) (Response, int) {
	switch {
	case request.Mount != nil:
		fd, err := h.Mount(*request.Mount)
		if err != nil {
			return Response{Error: err.Error()}, -1
		}
		return Response{}, fd
	case request.Get != nil:
		fd, entry, err := h.Get(request.Get.Target)
		if err != nil {
			return Response{Error: err.Error()}, -1
		}
		return Response{Entry: &entry}, fd
	case request.List != nil:
		return Response{Entries: h.List()}, -1
	case request.Unmount != nil:
		if err := h.Unmount(request.Unmount.Target, request.Unmount.Detach); err != nil {
			return Response{Error: err.Error()}, -1
		}
		return Response{}, -1
	default:
		return Response{Error: "unknown request"}, -1
	}
}
