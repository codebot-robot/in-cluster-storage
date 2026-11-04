//go:build linux

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
	"fmt"
	"strings"

	"golang.org/x/sys/unix"
)

func mountFUSE(request MountRequest) (int, error) {
	var stat unix.Stat_t
	if err := unix.Stat(request.Target, &stat); err != nil {
		return -1, fmt.Errorf("stat target: %w", err)
	}
	descriptor, err := unix.Open("/dev/fuse", unix.O_RDWR|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, fmt.Errorf("open /dev/fuse: %w", err)
	}

	flags := uintptr(unix.MS_NOSUID | unix.MS_NODEV)
	var extraOptions []string
	for _, option := range request.Options {
		switch option {
		case "ro":
			flags |= unix.MS_RDONLY
		case "rw":
			flags &^= unix.MS_RDONLY
		case "nodev":
			flags |= unix.MS_NODEV
		case "dev":
			flags &^= unix.MS_NODEV
		case "nosuid":
			flags |= unix.MS_NOSUID
		case "suid":
			flags &^= unix.MS_NOSUID
		case "noexec":
			flags |= unix.MS_NOEXEC
		case "exec":
			flags &^= unix.MS_NOEXEC
		default:
			extraOptions = append(extraOptions, option)
		}
	}

	maxRead := request.MaxRead
	if maxRead <= 0 {
		maxRead = 128 * 1024
	}

	mountOptions := []string{
		fmt.Sprintf("fd=%d", descriptor),
		fmt.Sprintf("rootmode=%o", stat.Mode&unix.S_IFMT),
		fmt.Sprintf("user_id=%d", unix.Geteuid()),
		fmt.Sprintf("group_id=%d", unix.Getegid()),
		fmt.Sprintf("max_read=%d", maxRead),
	}
	mountOptions = append(mountOptions, extraOptions...)

	fileSystemType := "fuse"
	if request.Name != "" {
		fileSystemType = "fuse." + request.Name
	}
	source := request.FsName
	if source == "" {
		source = request.Name
	}
	if source == "" {
		source = "fuse"
	}

	if err := unix.Mount(source, request.Target, fileSystemType, flags, strings.Join(mountOptions, ",")); err != nil {
		if closeErr := unix.Close(descriptor); closeErr != nil {
			return -1, fmt.Errorf("mount %s: %w (close descriptor failed: %v)", request.Target, err, closeErr)
		}
		return -1, fmt.Errorf("mount %s: %w", request.Target, err)
	}
	return descriptor, nil
}

func unmountFUSE(target string, detach bool) error {
	flags := 0
	if detach {
		flags = unix.MNT_DETACH
	}
	return unix.Unmount(target, flags)
}
