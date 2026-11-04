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
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"

	"golang.org/x/sys/unix"
)

// Client is a Go client for communicating with a FUSE holder process over a Unix domain socket.
type Client struct {
	mu         sync.Mutex
	connection *net.UnixConn
}

// Dial connects to a FUSE holder listening on socketPath.
func Dial(socketPath string) (*Client, error) {
	address := &net.UnixAddr{Name: socketPath, Net: "unix"}
	connection, err := net.DialUnix("unix", nil, address)
	if err != nil {
		return nil, fmt.Errorf("dial fuse holder at %s: %w", socketPath, err)
	}
	return &Client{connection: connection}, nil
}

// NewClient creates a Client wrapping an existing Unix domain socket connection.
func NewClient(connection *net.UnixConn) *Client {
	return &Client{connection: connection}
}

// Close closes the client's connection to the holder.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.connection != nil {
		return c.connection.Close()
	}
	return nil
}

func (c *Client) call(request Request) (int, *Response, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := json.NewEncoder(c.connection).Encode(request); err != nil {
		return -1, nil, fmt.Errorf("send request: %w", err)
	}

	var firstByte [1]byte
	outOfBand := make([]byte, unix.CmsgSpace(4))
	_, outOfBandBytes, _, _, err := c.connection.ReadMsgUnix(firstByte[:], outOfBand)
	if err != nil {
		return -1, nil, fmt.Errorf("read response header: %w", err)
	}
	fd, err := parseDescriptor(outOfBand[:outOfBandBytes])
	if err != nil {
		return -1, nil, err
	}

	reader := io.MultiReader(bytes.NewReader(firstByte[:]), c.connection)
	var response Response
	if err := json.NewDecoder(reader).Decode(&response); err != nil {
		discardDescriptor(fd)
		return -1, nil, fmt.Errorf("decode response: %w", err)
	}
	if response.Error != "" {
		discardDescriptor(fd)
		if response.Error == ErrNotFound.Error() {
			return -1, nil, ErrNotFound
		}
		return -1, nil, errors.New(response.Error)
	}
	return fd, &response, nil
}

// Mount requests the holder to mount a FUSE filesystem and return the descriptor.
func (c *Client) Mount(request MountRequest) (int, error) {
	fd, _, err := c.call(Request{Mount: &request})
	if err != nil {
		return -1, err
	}
	if fd < 0 {
		return -1, errors.New("mount succeeded but no descriptor received")
	}
	return fd, nil
}

// Get retrieves the descriptor and metadata for an existing mount.
func (c *Client) Get(target string) (int, Entry, error) {
	fd, response, err := c.call(Request{Get: &GetRequest{Target: target}})
	if err != nil {
		return -1, Entry{}, err
	}
	if fd < 0 || response.Entry == nil {
		discardDescriptor(fd)
		return -1, Entry{}, errors.New("incomplete get response")
	}
	return fd, *response.Entry, nil
}

// List returns metadata for every target held by the holder.
func (c *Client) List() ([]Entry, error) {
	fd, response, err := c.call(Request{List: &ListRequest{}})
	if err != nil {
		return nil, err
	}
	discardDescriptor(fd)
	return response.Entries, nil
}

// Unmount requests the holder to unmount target and close its descriptor.
func (c *Client) Unmount(target string, detach bool) error {
	fd, _, err := c.call(Request{Unmount: &UnmountRequest{Target: target, Detach: detach}})
	if err != nil {
		return err
	}
	discardDescriptor(fd)
	return nil
}
