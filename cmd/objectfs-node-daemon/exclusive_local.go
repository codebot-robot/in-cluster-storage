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
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"k8s.io/klog/v2"
)

func isExclusiveLocalDir(volumeDir string) bool {
	if fi, err := os.Stat(filepath.Join(volumeDir, "upper")); err == nil && fi.IsDir() {
		return true
	}
	if b, err := os.ReadFile(filepath.Join(volumeDir, "mode")); err == nil && strings.TrimSpace(string(b)) == "exclusive-local" {
		return true
	}
	return false
}

func isNotMountPoint(path string) (bool, error) {
	stat, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return true, nil
		}
		return false, err
	}
	parentPath := filepath.Dir(path)
	parentStat, err := os.Stat(parentPath)
	if err != nil {
		return false, err
	}
	return stat.Sys().(*syscall.Stat_t).Dev == parentStat.Sys().(*syscall.Stat_t).Dev, nil
}

func unmountTarget(targetPath string) error {
	if err := syscall.Unmount(targetPath, 0); err != nil {
		if err == syscall.EBUSY {
			klog.Warningf("Target path %s busy, attempting lazy unmount (MNT_DETACH)", targetPath)
			if detachErr := syscall.Unmount(targetPath, syscall.MNT_DETACH); detachErr != nil && detachErr != syscall.EINVAL && detachErr != syscall.ENOENT {
				return fmt.Errorf("failed to lazy unmount %s: %w", targetPath, detachErr)
			}
		} else if err != syscall.EINVAL && err != syscall.ENOENT {
			return fmt.Errorf("failed to unmount %s: %w", targetPath, err)
		}
	}
	return nil
}

func (d *objectFSDriver) publishExclusiveLocal(ctx context.Context, req *csi.NodePublishVolumeRequest, volumeID string) (*csi.NodePublishVolumeResponse, error) {
	targetPath := req.GetTargetPath()
	klog.Infof("Publishing ObjectFS volume %s in exclusive-local mode to %s", volumeID, targetPath)

	volumeDir := filepath.Join(d.cacheDir, volumeID)
	lowerPath := filepath.Join(volumeDir, "lower")
	upperPath := filepath.Join(volumeDir, "upper")
	workPath := filepath.Join(volumeDir, "work")

	client, err := d.getClient()
	if err != nil {
		return nil, fmt.Errorf("failed to get controller client: %w", err)
	}

	if err := os.MkdirAll(lowerPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create lower path %s: %w", lowerPath, err)
	}
	if err := os.MkdirAll(upperPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create upper path %s: %w", upperPath, err)
	}
	if err := os.MkdirAll(workPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create work path %s: %w", workPath, err)
	}

	if err := os.WriteFile(filepath.Join(volumeDir, "mode"), []byte("exclusive-local"), 0644); err != nil {
		return nil, fmt.Errorf("failed to write mode file: %w", err)
	}
	if err := os.WriteFile(filepath.Join(volumeDir, "volume_id"), []byte(volumeID), 0644); err != nil {
		return nil, fmt.Errorf("failed to write volume_id file: %w", err)
	}

	if err := d.materialiseVolume(ctx, client, volumeID, lowerPath); err != nil {
		return nil, fmt.Errorf("failed to materialise volume %s: %w", volumeID, err)
	}

	if err := os.MkdirAll(targetPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create target path %s: %w", targetPath, err)
	}

	notMnt, err := isNotMountPoint(targetPath)
	if err != nil {
		return nil, fmt.Errorf("failed to check if %s is a mount point: %w", targetPath, err)
	}
	if !notMnt {
		klog.Infof("Volume %s already mounted at %s", volumeID, targetPath)
		return &csi.NodePublishVolumeResponse{}, nil
	}

	opts := fmt.Sprintf("lowerdir=%s,upperdir=%s,workdir=%s", lowerPath, upperPath, workPath)
	if err := syscall.Mount("overlay", targetPath, "overlay", 0, opts); err != nil {
		return nil, fmt.Errorf("failed to mount overlayfs at %s: %w", targetPath, err)
	}

	d.mu.Lock()
	if old, ok := d.mounts[targetPath]; ok {
		if old.cancelFunc != nil {
			old.cancelFunc()
		}
		if old.server != nil {
			if unmountErr := old.server.Unmount(); unmountErr != nil {
				klog.Warningf("failed to unmount previous mount at %s: %v", targetPath, unmountErr)
			}
		}
	}
	d.mounts[targetPath] = &activeMount{
		volumeID:  volumeID,
		mode:      "exclusive-local",
		volumeDir: volumeDir,
	}
	d.mu.Unlock()

	klog.Infof("Successfully mounted ObjectFS exclusive-local volume %s at %s", volumeID, targetPath)
	return &csi.NodePublishVolumeResponse{}, nil
}

func (d *objectFSDriver) unpublishExclusiveLocal(ctx context.Context, req *csi.NodeUnpublishVolumeRequest, volumeID, volumeDir string) (*csi.NodeUnpublishVolumeResponse, error) {
	targetPath := req.GetTargetPath()
	klog.Infof("Unpublishing ObjectFS exclusive-local volume %s from %s", volumeID, targetPath)

	if err := unmountTarget(targetPath); err != nil {
		return nil, fmt.Errorf("failed to unmount overlay at %s: %w", targetPath, err)
	}

	return &csi.NodeUnpublishVolumeResponse{}, nil
}
