// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

type SimulationState struct {
	Files map[int]string
}

func runOpsBenchmark(dataDir string, n int) {
	fmt.Printf("Running FUSE end-to-end ops benchmark in %s (N=%d)\n", dataDir, n)

	testDir := filepath.Join(dataDir, "fuse_bench")
	_ = os.MkdirAll(testDir, 0755)

	// 1. Create benchmark
	start := time.Now()
	for i := 0; i < n; i++ {
		fname := filepath.Join(testDir, fmt.Sprintf("f%d.txt", i))
		if err := os.WriteFile(fname, []byte("data"), 0644); err != nil {
			panic(fmt.Sprintf("create %s failed: %v", fname, err))
		}
	}
	createDur := time.Since(start)
	createPerOp := createDur / time.Duration(n)

	// 2. Stat benchmark
	start = time.Now()
	for i := 0; i < n; i++ {
		fname := filepath.Join(testDir, fmt.Sprintf("f%d.txt", i))
		if _, err := os.Stat(fname); err != nil {
			panic(fmt.Sprintf("stat %s failed: %v", fname, err))
		}
	}
	statDur := time.Since(start)
	statPerOp := statDur / time.Duration(n)

	// 3. ReadDir benchmark
	start = time.Now()
	entries, err := os.ReadDir(testDir)
	if err != nil {
		panic(fmt.Sprintf("readdir failed: %v", err))
	}
	readdirDur := time.Since(start)

	fmt.Printf("Results (FUSE End-to-End):\n")
	fmt.Printf("  Create:  %v total (%v/op, %.1f ops/sec)\n", createDur, createPerOp, float64(n)/createDur.Seconds())
	fmt.Printf("  Stat:    %v total (%v/op, %.1f ops/sec)\n", statDur, statPerOp, float64(n)/statDur.Seconds())
	fmt.Printf("  ReadDir (%d entries): %v total\n", len(entries), readdirDur)
}

func main() {
	modeFlag := flag.String("mode", "sim", "Benchmark mode: sim or ops")
	opsNFlag := flag.Int("n", 1000, "Number of operations for ops mode")
	flag.Parse()

	args := flag.Args()
	if *modeFlag == "ops" {
		if len(args) < 1 {
			fmt.Println("Usage: benchmark -mode=ops [-n=1000] <data_dir>")
			os.Exit(1)
		}
		runOpsBenchmark(args[0], *opsNFlag)
		return
	}

	if len(args) < 3 {
		fmt.Println("Usage: benchmark <data_dir> <seed> <count>")
		os.Exit(1)
	}

	dataDir := args[0]
	seed, _ := strconv.ParseInt(args[1], 10, 64)
	count, _ := strconv.Atoi(args[2])

	fmt.Printf("Starting benchmark. DataDir: %s, Seed: %d, Count: %d\n", dataDir, seed, count)

	state := &SimulationState{
		Files: make(map[int]string),
	}

	mainRand := rand.New(rand.NewSource(seed))

	for step := 0; step < count; step++ {
		// Draw a 64 bit seed which becomes the seed for another random stream for this step
		stepSeed := mainRand.Int63()
		stepRand := rand.New(rand.NewSource(stepSeed))

		// Random action
		action := stepRand.Intn(5) // 0: Create, 1: Append, 2: Delete, 3: Overwrite, 4: Chmod
		fileID := stepRand.Intn(100)

		switch action {
		case 0: // Create
			if _, exists := state.Files[fileID]; !exists {
				filename := filepath.Join(dataDir, fmt.Sprintf("file-%d.txt", fileID))
				content := fmt.Sprintf("content-%d", stepRand.Intn(1000))
				if err := os.WriteFile(filename, []byte(content), 0644); err == nil {
					state.Files[fileID] = content
				} else {
					fmt.Printf("Step %d: Failed to create file %s: %v\n", step, filename, err)
				}
			}
		case 1: // Append
			if content, exists := state.Files[fileID]; exists {
				filename := filepath.Join(dataDir, fmt.Sprintf("file-%d.txt", fileID))
				appendStr := fmt.Sprintf("-appended-%d", stepRand.Intn(1000))
				if f, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY, 0644); err == nil {
					f.WriteString(appendStr)
					f.Close()
					state.Files[fileID] = content + appendStr
				} else {
					fmt.Printf("Step %d: Failed to append to file %s: %v\n", step, filename, err)
				}
			}
		case 2: // Delete
			if _, exists := state.Files[fileID]; exists {
				filename := filepath.Join(dataDir, fmt.Sprintf("file-%d.txt", fileID))
				if err := os.Remove(filename); err == nil {
					delete(state.Files, fileID)
				} else {
					fmt.Printf("Step %d: Failed to delete file %s: %v\n", step, filename, err)
				}
			}
		case 3: // Overwrite
			if _, exists := state.Files[fileID]; exists {
				filename := filepath.Join(dataDir, fmt.Sprintf("file-%d.txt", fileID))
				content := fmt.Sprintf("new-content-%d", stepRand.Intn(1000))
				if err := os.WriteFile(filename, []byte(content), 0644); err == nil {
					state.Files[fileID] = content
				} else {
					fmt.Printf("Step %d: Failed to overwrite file %s: %v\n", step, filename, err)
				}
			}
		case 4: // Chmod
			if _, exists := state.Files[fileID]; exists {
				filename := filepath.Join(dataDir, fmt.Sprintf("file-%d.txt", fileID))
				mode := os.FileMode(0600 + stepRand.Intn(77)) // Random valid-ish permission
				if err := os.Chmod(filename, mode); err != nil {
					fmt.Printf("Step %d: Failed to chmod file %s: %v\n", step, filename, err)
				}
			}
		}

		// Verify state periodically
		if step%10 == 0 {
			for id, expected := range state.Files {
				filename := filepath.Join(dataDir, fmt.Sprintf("file-%d.txt", id))
				content, err := os.ReadFile(filename)
				if err != nil {
					panic(fmt.Sprintf("Failed to read file %s at step %d: %v", filename, step, err))
				}
				if string(content) != expected {
					panic(fmt.Sprintf("Content mismatch for %d at step %d: expected %s, got %s", id, step, expected, string(content)))
				}
			}
		}

		if step%100 == 0 {
			fmt.Printf("Completed step %d\n", step)
		}
	}
	fmt.Printf("Benchmark finished successfully after %d steps.\n", count)
}
