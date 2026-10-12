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

package errutil

import (
	"io"

	"k8s.io/klog/v2"
)

// CloseAndLog closes c and logs a warning on failure. For read-only files, response bodies, connections.
func CloseAndLog(c io.Closer, what string) {
	if c == nil {
		return
	}
	if err := c.Close(); err != nil {
		klog.Warningf("failed to close %s: %v", what, err)
	}
}

// CloseWithErr closes c and, if the caller's error is still nil, stores the close error there.
// For files and writers opened for writing:  defer errutil.CloseWithErr(f, &err)
func CloseWithErr(c io.Closer, err *error) {
	if c == nil {
		return
	}
	if closeErr := c.Close(); closeErr != nil && err != nil && *err == nil {
		*err = closeErr
	}
}
