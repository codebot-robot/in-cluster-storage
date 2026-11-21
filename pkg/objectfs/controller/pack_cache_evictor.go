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
	"context"
	"sort"
	"time"

	"github.com/gke-labs/in-cluster-storage/pkg/metrics"
)

// packCacheEvictor coordinates background eviction of uploaded packs in the local cache across volumes.
type packCacheEvictor struct {
	Runnable
	// wake signals the evictorLoop goroutine to evaluate local pack cache targets. Requires no lock.
	wake chan struct{}
	// passesCompleted counts evaluation passes completed by the evictor loop.
	passesCompleted *metrics.Counter
}

// wakeEvictor sends a non-blocking signal to wake the evictor loop.
// Safe to call with any or no locks held.
func (e *packCacheEvictor) wakeEvictor() {
	select {
	case e.wake <- struct{}{}:
	default:
	}
}

// evictorLoop runs a background loop responding to pack cache wake-up signals until ctx is cancelled.
// Requires no locks held upon entry.
func (s *Server) evictorLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.evictor.wake:
		}

		s.runPackCacheEvictionPass(ctx)
	}
}

// evictablePackCandidate tracks a single uploaded pack eligible for eviction.
type evictablePackCandidate struct {
	// volume is the volume that owns the candidate pack.
	volume *Volume
	// packID identifies the pack within the volume.
	packID uint64
	// lastRead records when this pack was last read, or its upload time if never read since upload.
	lastRead time.Time
}

// collectEvictablePackCandidates scans all volumes for evictable uploaded packs (state uploaded and no live references),
// returning them sorted least recently read first with lower pack ID breaking ties.
// Requires no locks held upon entry.
func (s *Server) collectEvictablePackCandidates(ctx context.Context) []evictablePackCandidate {
	s.diskLimits.mu.Lock()
	targetBytes := s.diskLimits.packCacheTargetBytes
	totalBytes := s.diskLimits.packCacheBytes
	s.diskLimits.mu.Unlock()

	if targetBytes <= 0 || totalBytes <= targetBytes {
		return nil
	}

	s.mu.RLock()
	volumes := make([]*Volume, 0, len(s.volumes))
	for _, volume := range s.volumes {
		volumes = append(volumes, volume)
	}
	s.mu.RUnlock()

	var candidates []evictablePackCandidate
	for _, volume := range volumes {
		if ctx.Err() != nil {
			return nil
		}
		volume.stagingMu.Lock()
		for packID, packInfo := range volume.packs {
			if packInfo.state == packUploaded && packInfo.liveReferences <= 0 {
				var lastRead time.Time
				if packInfo.pack != nil {
					lastRead = packInfo.pack.LastRead()
				}
				candidates = append(candidates, evictablePackCandidate{
					volume:   volume,
					packID:   packID,
					lastRead: lastRead,
				})
			}
		}
		volume.stagingMu.Unlock()
	}

	if len(candidates) == 0 {
		return nil
	}

	sort.Slice(candidates, func(i, j int) bool {
		if !candidates[i].lastRead.Equal(candidates[j].lastRead) {
			return candidates[i].lastRead.Before(candidates[j].lastRead)
		}
		if candidates[i].packID != candidates[j].packID {
			return candidates[i].packID < candidates[j].packID
		}
		return candidates[i].volume.volumeID < candidates[j].volume.volumeID
	})

	return candidates
}

// evictCandidatePack acquires the candidate volume's stagingMu, verifies whether the total is still
// over target and the pack remains uploaded without live references, and deletes the pack if so.
// Returns whether the pack was evicted and whether the target is reached (signaling the pass can stop).
// Requires no locks held upon entry.
func (s *Server) evictCandidatePack(candidate evictablePackCandidate) (evicted bool, targetReached bool) {
	candidate.volume.stagingMu.Lock()
	defer candidate.volume.stagingMu.Unlock()

	s.diskLimits.mu.Lock()
	target := s.diskLimits.packCacheTargetBytes
	total := s.diskLimits.packCacheBytes
	s.diskLimits.mu.Unlock()

	if target <= 0 || total <= target {
		return false, true
	}

	packInfo, exists := candidate.volume.packs[candidate.packID]
	if !exists || packInfo.state != packUploaded || packInfo.liveReferences > 0 {
		return false, false
	}

	candidate.volume.deletePackLocked(candidate.packID)

	s.diskLimits.mu.Lock()
	target = s.diskLimits.packCacheTargetBytes
	total = s.diskLimits.packCacheBytes
	s.diskLimits.mu.Unlock()

	if target <= 0 || total <= target {
		return true, true
	}
	return true, false
}

// runPackCacheEvictionPass runs a single pass of local pack cache eviction across all volumes.
// It removes evictable packs (uploaded, no live references) least recently read first across
// all volumes, lower pack ID breaking ties, until the total is at or under the target or
// nothing is evictable.
// Requires no locks held upon entry.
func (s *Server) runPackCacheEvictionPass(ctx context.Context) {
	defer s.evictor.passesCompleted.Inc()

	candidates := s.collectEvictablePackCandidates(ctx)
	if len(candidates) == 0 {
		return
	}

	for _, candidate := range candidates {
		if ctx.Err() != nil {
			return
		}

		_, targetReached := s.evictCandidatePack(candidate)
		if targetReached {
			return
		}
	}
}
