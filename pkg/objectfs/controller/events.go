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

package controller

import (
	"sync"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
)

type EventBroadcaster struct {
	mu          sync.RWMutex
	subscribers map[string]map[chan *pb.WatchVolumeResponse]struct{}
}

func NewEventBroadcaster() *EventBroadcaster {
	return &EventBroadcaster{
		subscribers: make(map[string]map[chan *pb.WatchVolumeResponse]struct{}),
	}
}

func (eb *EventBroadcaster) Subscribe(volumeID string) chan *pb.WatchVolumeResponse {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	ch := make(chan *pb.WatchVolumeResponse, 128)
	if _, ok := eb.subscribers[volumeID]; !ok {
		eb.subscribers[volumeID] = make(map[chan *pb.WatchVolumeResponse]struct{})
	}
	eb.subscribers[volumeID][ch] = struct{}{}
	return ch
}

func (eb *EventBroadcaster) Unsubscribe(volumeID string, ch chan *pb.WatchVolumeResponse) {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	if subs, ok := eb.subscribers[volumeID]; ok {
		delete(subs, ch)
		if len(subs) == 0 {
			delete(eb.subscribers, volumeID)
		}
	}
	close(ch)
}

func (eb *EventBroadcaster) Broadcast(volumeID string, event *pb.WatchVolumeResponse) {
	eb.mu.RLock()
	defer eb.mu.RUnlock()

	subs, ok := eb.subscribers[volumeID]
	if !ok {
		return
	}

	for ch := range subs {
		select {
		case ch <- event:
		default:
			// If receiver channel buffer is full, drop or let next push happen
		}
	}
}
