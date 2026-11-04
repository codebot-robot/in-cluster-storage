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
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

func createSocketpair(t *testing.T) (*net.UnixConn, *net.UnixConn) {
	t.Helper()
	descriptors, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	if err != nil {
		t.Fatalf("failed to create socketpair: %v", err)
	}
	serverFile := os.NewFile(uintptr(descriptors[0]), "serverSocket")
	clientFile := os.NewFile(uintptr(descriptors[1]), "clientSocket")
	serverConnection, err := net.FileConn(serverFile)
	if err != nil {
		t.Fatalf("failed to create server FileConn: %v", err)
	}
	clientConnection, err := net.FileConn(clientFile)
	if err != nil {
		t.Fatalf("failed to create client FileConn: %v", err)
	}
	if err := serverFile.Close(); err != nil {
		t.Fatalf("failed to close server file: %v", err)
	}
	if err := clientFile.Close(); err != nil {
		t.Fatalf("failed to close client file: %v", err)
	}
	return serverConnection.(*net.UnixConn), clientConnection.(*net.UnixConn)
}

func TestProtocolRoundTrip(t *testing.T) {
	serverUnixConnection, clientUnixConnection := createSocketpair(t)

	mockReaderPipe, mockWriterPipe, err := os.Pipe()
	if err != nil {
		t.Fatalf("failed to create mock pipe: %v", err)
	}

	holderServer := New()
	holderServer.mountFunc = func(request MountRequest) (int, error) {
		return int(mockReaderPipe.Fd()), nil
	}
	holderServer.unmountFunc = func(target string, detach bool) error {
		return nil
	}

	go holderServer.handleConnection(serverUnixConnection)
	client := NewClient(clientUnixConnection)

	mountRequest := MountRequest{
		Entry: Entry{
			Target:   "/var/mock/target",
			VolumeID: "volume-123",
			Mode:     "fsync",
			FsName:   "objectfs",
			Name:     "objectfs",
			MaxRead:  128 * 1024,
			Options:  []string{"default_permissions", "allow_other"},
		},
	}

	mountFd, err := client.Mount(mountRequest)
	if err != nil {
		t.Fatalf("client Mount failed: %v", err)
	}
	if mountFd < 0 {
		t.Fatalf("expected valid descriptor from mount, got %d", mountFd)
	}

	testMessage := []byte("pipe-roundtrip-check")
	if _, err := mockWriterPipe.Write(testMessage); err != nil {
		t.Fatalf("failed to write to pipe: %v", err)
	}
	receivedFile := os.NewFile(uintptr(mountFd), "receivedMountPipe")
	readBuffer := make([]byte, len(testMessage))
	if _, err := io.ReadFull(receivedFile, readBuffer); err != nil {
		t.Fatalf("failed to read from mounted descriptor: %v", err)
	}
	if string(readBuffer) != string(testMessage) {
		t.Fatalf("pipe content mismatch: got %s, want %s", string(readBuffer), string(testMessage))
	}
	if err := receivedFile.Close(); err != nil {
		t.Fatalf("failed to close received mount pipe: %v", err)
	}

	getFd, entry, err := client.Get("/var/mock/target")
	if err != nil {
		t.Fatalf("client Get failed: %v", err)
	}
	if getFd < 0 {
		t.Fatalf("expected valid descriptor from get, got %d", getFd)
	}
	if entry.VolumeID != "volume-123" || entry.Mode != "fsync" || entry.FsName != "objectfs" || entry.Name != "objectfs" {
		t.Fatalf("unexpected entry metadata: %+v", entry)
	}
	if err := unix.Close(getFd); err != nil {
		t.Fatalf("failed to close get descriptor: %v", err)
	}

	entries, err := client.List()
	if err != nil {
		t.Fatalf("client List failed: %v", err)
	}
	if len(entries) != 1 || entries[0].Target != "/var/mock/target" {
		t.Fatalf("unexpected list entries: %+v", entries)
	}

	if err := client.Unmount("/var/mock/target", false); err != nil {
		t.Fatalf("client Unmount failed: %v", err)
	}

	_, _, err = client.Get("/var/mock/target")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound after unmount, got: %v", err)
	}

	if err := client.Close(); err != nil {
		t.Fatalf("failed to close client: %v", err)
	}
	if err := mockWriterPipe.Close(); err != nil {
		t.Fatalf("failed to close mock writer pipe: %v", err)
	}
	if err := holderServer.Close(); err != nil {
		t.Fatalf("failed to close holder: %v", err)
	}
}

func TestHolderUnmountFailureRetainsEntry(t *testing.T) {
	serverUnixConnection, clientUnixConnection := createSocketpair(t)

	mockReaderPipe, mockWriterPipe, err := os.Pipe()
	if err != nil {
		t.Fatalf("failed to create mock pipe: %v", err)
	}

	unmountAttempts := 0
	holderServer := New()
	holderServer.mountFunc = func(request MountRequest) (int, error) {
		return int(mockReaderPipe.Fd()), nil
	}
	holderServer.unmountFunc = func(target string, detach bool) error {
		unmountAttempts++
		if unmountAttempts == 1 {
			return errors.New("device or resource busy")
		}
		return nil
	}

	go holderServer.handleConnection(serverUnixConnection)
	client := NewClient(clientUnixConnection)

	mountRequest := MountRequest{
		Entry: Entry{
			Target:   "/var/mock/busy-target",
			VolumeID: "volume-busy",
			Mode:     "fsync",
			FsName:   "objectfs",
			Name:     "objectfs",
			MaxRead:  128 * 1024,
			Options:  []string{"default_permissions"},
		},
	}

	mountFd, err := client.Mount(mountRequest)
	if err != nil {
		t.Fatalf("client Mount failed: %v", err)
	}
	if err := unix.Close(mountFd); err != nil {
		t.Fatalf("failed to close client mount descriptor: %v", err)
	}

	// First unmount fails with EBUSY.
	if err := client.Unmount("/var/mock/busy-target", false); err == nil {
		t.Fatalf("expected unmount to fail on first attempt")
	}

	// Entry and descriptor must still exist after failed unmount.
	getFd, entry, err := client.Get("/var/mock/busy-target")
	if err != nil {
		t.Fatalf("client Get failed after failed unmount: %v", err)
	}
	if entry.VolumeID != "volume-busy" {
		t.Fatalf("unexpected entry metadata after failed unmount: %+v", entry)
	}
	if err := unix.Close(getFd); err != nil {
		t.Fatalf("failed to close get descriptor: %v", err)
	}

	// Second unmount succeeds.
	if err := client.Unmount("/var/mock/busy-target", false); err != nil {
		t.Fatalf("client Unmount failed on second attempt: %v", err)
	}

	// Now Get returns ErrNotFound.
	_, _, err = client.Get("/var/mock/busy-target")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound after successful unmount, got: %v", err)
	}

	if err := client.Close(); err != nil {
		t.Fatalf("failed to close client: %v", err)
	}
	if err := mockWriterPipe.Close(); err != nil {
		t.Fatalf("failed to close mock writer pipe: %v", err)
	}
	if err := holderServer.Close(); err != nil {
		t.Fatalf("failed to close holder: %v", err)
	}
}

func TestHolderMountFUSE(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("skipping linux-only test")
	}
	if os.Geteuid() != 0 {
		t.Skip("skipping root-only test")
	}

	probeDescriptor, err := unix.Open("/dev/fuse", unix.O_RDWR|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatalf("cannot open /dev/fuse: %v", err)
	}
	if err := unix.Close(probeDescriptor); err != nil {
		t.Fatalf("failed to close probe descriptor: %v", err)
	}

	socketPath := filepath.Join(t.TempDir(), "fuse-holder.sock")
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatalf("failed to listen on socket %s: %v", socketPath, err)
	}
	defer func() {
		if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Errorf("failed to close listener: %v", err)
		}
	}()

	holderServer := New()
	defer func() {
		if err := holderServer.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Errorf("failed to close holder: %v", err)
		}
	}()
	go func() {
		if err := holderServer.Serve(listener); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Errorf("holder Serve failed: %v", err)
		}
	}()

	client, err := Dial(socketPath)
	if err != nil {
		t.Fatalf("failed to dial holder: %v", err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			t.Errorf("failed to close client: %v", err)
		}
	}()

	targetDirectory := t.TempDir()
	fd, err := client.Mount(MountRequest{
		Entry: Entry{
			Target:   targetDirectory,
			VolumeID: "test-fuse-vol",
			Mode:     "fsync",
			FsName:   "objectfs",
			Name:     "objectfs",
			MaxRead:  128 * 1024,
			Options:  []string{"default_permissions", "allow_other"},
		},
	})
	if err != nil {
		t.Fatalf("mount failed: %v", err)
	}
	if fd < 0 {
		t.Fatalf("expected positive descriptor, got %d", fd)
	}

	linkTarget, err := os.Readlink(fmt.Sprintf("/proc/self/fd/%d", fd))
	if err != nil {
		t.Fatalf("failed to readlink for descriptor %d: %v", fd, err)
	}
	if !strings.Contains(linkTarget, "fuse") {
		t.Fatalf("expected descriptor to point to /dev/fuse, got %s", linkTarget)
	}
	if err := unix.Close(fd); err != nil {
		t.Fatalf("failed to close client descriptor: %v", err)
	}

	if err := client.Unmount(targetDirectory, false); err != nil {
		t.Fatalf("failed to unmount %s: %v", targetDirectory, err)
	}

	_, _, err = client.Get(targetDirectory)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound after unmount, got: %v", err)
	}
}
