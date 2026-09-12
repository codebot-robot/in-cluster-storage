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
	"flag"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	objectfuse "github.com/gke-labs/in-cluster-storage/pkg/objectfs/fuse"
	"github.com/hanwen/go-fuse/v2/fs"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"k8s.io/klog/v2"
)

var (
	endpoint          = flag.String("endpoint", "unix:///tmp/csi.sock", "CSI endpoint")
	nodeID            = flag.String("nodeid", "", "node id")
	controllerAddress = flag.String("controller-address", "objectfs-controller:50051", "ObjectFS Controller address")
	cacheSizeMB       = flag.Int64("cache-size-mb", 128, "Local node cache size in MB")
)

func main() {
	klog.InitFlags(nil)
	flag.Parse()

	if *nodeID == "" {
		klog.Fatal("nodeid must be provided")
	}

	proto, addr, err := parseEndpoint(*endpoint)
	if err != nil {
		klog.Fatal(err)
	}

	if proto == "unix" {
		addr = filepath.FromSlash(addr)
		if err := os.Remove(addr); err != nil && !os.IsNotExist(err) {
			klog.Fatalf("failed to remove %s: %v", addr, err)
		}
	}

	listener, err := net.Listen(proto, addr)
	if err != nil {
		klog.Fatalf("failed to listen: %v", err)
	}

	server := grpc.NewServer()

	driver := newDriver(*nodeID, *controllerAddress, *cacheSizeMB*1024*1024)

	csi.RegisterIdentityServer(server, driver)
	csi.RegisterNodeServer(server, driver)

	klog.Infof("ObjectFS CSI driver listening on %s", *endpoint)
	if err := server.Serve(listener); err != nil {
		klog.Fatalf("failed to serve: %v", err)
	}
}

func parseEndpoint(endpoint string) (string, string, error) {
	parts := strings.SplitN(endpoint, "://", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid endpoint: %s", endpoint)
	}
	scheme, addr := parts[0], parts[1]
	if scheme != "unix" && scheme != "tcp" {
		return "", "", fmt.Errorf("invalid endpoint: %s", endpoint)
	}
	return scheme, addr, nil
}

type activeMount struct {
	server     *gofuse.Server
	cancelFunc context.CancelFunc
	volumeID   string
}

type objectFSDriver struct {
	csi.UnimplementedIdentityServer
	csi.UnimplementedNodeServer

	nodeID            string
	controllerAddress string
	cacheBytes        int64

	clientConn *grpc.ClientConn
	client     pb.ObjectFSControllerClient

	mu     sync.Mutex
	mounts map[string]*activeMount
}

func newDriver(nodeID, controllerAddress string, cacheBytes int64) *objectFSDriver {
	return &objectFSDriver{
		nodeID:            nodeID,
		controllerAddress: controllerAddress,
		cacheBytes:        cacheBytes,
		mounts:            make(map[string]*activeMount),
	}
}

func (d *objectFSDriver) getClient() (pb.ObjectFSControllerClient, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.client != nil {
		return d.client, nil
	}

	conn, err := grpc.NewClient(d.controllerAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to controller at %s: %w", d.controllerAddress, err)
	}

	d.clientConn = conn
	d.client = pb.NewObjectFSControllerClient(conn)
	return d.client, nil
}

func (d *objectFSDriver) GetPluginInfo(ctx context.Context, req *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	return &csi.GetPluginInfoResponse{
		Name:          "objectfs.labs.gke.io",
		VendorVersion: "0.0.1",
	}, nil
}

func (d *objectFSDriver) GetPluginCapabilities(ctx context.Context, req *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
	return &csi.GetPluginCapabilitiesResponse{
		Capabilities: []*csi.PluginCapability{
			{
				Type: &csi.PluginCapability_Service_{
					Service: &csi.PluginCapability_Service{
						Type: csi.PluginCapability_Service_CONTROLLER_SERVICE,
					},
				},
			},
		},
	}, nil
}

func (d *objectFSDriver) Probe(ctx context.Context, req *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	return &csi.ProbeResponse{}, nil
}

func (d *objectFSDriver) NodeGetInfo(ctx context.Context, req *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{
		NodeId: d.nodeID,
	}, nil
}

func (d *objectFSDriver) NodeGetCapabilities(ctx context.Context, req *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: []*csi.NodeServiceCapability{
			{
				Type: &csi.NodeServiceCapability_Rpc{
					Rpc: &csi.NodeServiceCapability_RPC{
						Type: csi.NodeServiceCapability_RPC_GET_VOLUME_STATS,
					},
				},
			},
		},
	}, nil
}

func parseWriteMode(val string) pb.WriteMode {
	switch strings.ToLower(val) {
	case "lazy", "lazywrite", "lazy_write":
		return pb.WriteMode_LAZY_WRITE
	case "eager", "eagerreplication", "eager_replication":
		return pb.WriteMode_EAGER_REPLICATION
	case "fsync", "writethrough", "write_through_fsync":
		return pb.WriteMode_WRITE_THROUGH_FSYNC
	default:
		return pb.WriteMode_WRITE_THROUGH_FSYNC
	}
}

func (d *objectFSDriver) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	targetPath := req.GetTargetPath()
	volumeID := req.GetVolumeContext()["volumeID"]
	if volumeID == "" {
		volumeID = req.GetVolumeId()
	}
	if volumeID == "" {
		return nil, fmt.Errorf("volumeID is required")
	}

	writeModeStr := req.GetVolumeContext()["writeMode"]
	writeMode := parseWriteMode(writeModeStr)

	klog.Infof("Publishing ObjectFS volume %s (writeMode=%v) to %s", volumeID, writeMode, targetPath)

	if err := os.MkdirAll(targetPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create target path %s: %w", targetPath, err)
	}

	client, err := d.getClient()
	if err != nil {
		return nil, err
	}

	nodeCache := objectfuse.NewNodeCache(d.cacheBytes)
	rootNode := objectfuse.NewRootNode(client, volumeID, writeMode, nodeCache)

	sec := 1 * time.Second
	opts := &fs.Options{
		AttrTimeout:  &sec,
		EntryTimeout: &sec,
	}

	server, err := fs.Mount(targetPath, rootNode, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to mount FUSE filesystem at %s: %w", targetPath, err)
	}

	watchCtx, cancel := context.WithCancel(context.Background())
	objectfuse.StartWatcher(watchCtx, client, volumeID, d.nodeID, nodeCache)

	d.mu.Lock()
	// Clean up any previous mount at this targetPath if any
	if old, ok := d.mounts[targetPath]; ok {
		old.cancelFunc()
		_ = old.server.Unmount()
	}
	d.mounts[targetPath] = &activeMount{
		server:     server,
		cancelFunc: cancel,
		volumeID:   volumeID,
	}
	d.mu.Unlock()

	klog.Infof("Successfully mounted ObjectFS FUSE volume %s at %s", volumeID, targetPath)
	return &csi.NodePublishVolumeResponse{}, nil
}

func (d *objectFSDriver) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	targetPath := req.GetTargetPath()
	klog.Infof("Unpublishing ObjectFS volume from %s", targetPath)

	d.mu.Lock()
	mountInfo, ok := d.mounts[targetPath]
	if ok {
		delete(d.mounts, targetPath)
	}
	d.mu.Unlock()

	if ok && mountInfo != nil {
		mountInfo.cancelFunc()
		if err := mountInfo.server.Unmount(); err != nil {
			klog.Warningf("FUSE server unmount returned error: %v, attempting system unmount", err)
			_ = syscall.Unmount(targetPath, 0)
		}
	} else {
		// Fallback unmount if not tracked
		_ = syscall.Unmount(targetPath, 0)
	}

	klog.Infof("Successfully unpublished ObjectFS volume from %s", targetPath)
	return &csi.NodeUnpublishVolumeResponse{}, nil
}
