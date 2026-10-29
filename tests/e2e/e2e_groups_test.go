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

package e2e

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func findE2ETestFunctions(t *testing.T, dir string) (map[string]*ast.FuncDecl, *token.FileSet) {
	t.Helper()
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, dir, func(fi os.FileInfo) bool {
		// Only check Go test files, excluding this test itself. Non-e2e tests in this package must live in that file.
		name := fi.Name()
		return strings.HasSuffix(name, "_test.go") && name != "e2e_groups_test.go"
	}, parser.ParseComments)
	if err != nil {
		t.Fatalf("failed to parse directory %s: %v", dir, err)
	}

	tests := make(map[string]*ast.FuncDecl)
	for _, pkg := range pkgs {
		for _, file := range pkg.Files {
			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok {
					continue
				}
				if strings.HasPrefix(fn.Name.Name, "Test") && fn.Name.Name != "TestMain" {
					tests[fn.Name.Name] = fn
				}
			}
		}
	}
	return tests, fset
}

func inspectRequireE2EGroup(fn *ast.FuncDecl) (group string, err error) {
	if fn.Body == nil || len(fn.Body.List) == 0 {
		return "", fmt.Errorf("function has an empty body")
	}

	// Verify that the first statement is a call to requireE2EGroup
	var firstCall *ast.CallExpr
	if exprStmt, ok := fn.Body.List[0].(*ast.ExprStmt); ok {
		if call, ok := exprStmt.X.(*ast.CallExpr); ok {
			if ident, ok := call.Fun.(*ast.Ident); ok && ident.Name == "requireE2EGroup" {
				firstCall = call
			}
		}
	}
	if firstCall == nil {
		return "", fmt.Errorf("must call requireE2EGroup as its first statement")
	}

	// Count all calls to requireE2EGroup in the function body
	callCount := 0
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok {
			if ident, ok := call.Fun.(*ast.Ident); ok && ident.Name == "requireE2EGroup" {
				callCount++
			}
		}
		return true
	})
	if callCount != 1 {
		return "", fmt.Errorf("calls requireE2EGroup %d times; must be called exactly once", callCount)
	}

	// Verify argument to requireE2EGroup(t, "<group>")
	if len(firstCall.Args) != 2 {
		return "", fmt.Errorf("requireE2EGroup expected 2 arguments, got %d", len(firstCall.Args))
	}
	lit, ok := firstCall.Args[1].(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return "", fmt.Errorf("requireE2EGroup group argument must be a string literal")
	}
	val, err := strconv.Unquote(lit.Value)
	if err != nil || val == "" {
		return "", fmt.Errorf("requireE2EGroup group argument must be a non-empty string literal")
	}

	return val, nil
}

func TestE2EGroups_NoDrift(t *testing.T) {
	tests, _ := findE2ETestFunctions(t, ".")
	if len(tests) == 0 {
		t.Fatal("no E2E tests found in tests/e2e")
	}

	repoRoot := filepath.Join("..", "..")
	groups := make(map[string]bool)

	for testName, fn := range tests {
		grp, err := inspectRequireE2EGroup(fn)
		if err != nil {
			t.Errorf("test %s: %v", testName, err)
			continue
		}
		groups[grp] = true
	}

	// Every non-core group must have a dev/ci/presubmits/ap-e2e-<group> or dev/ci/periodics/ap-<group> script
	for grp := range groups {
		if grp == "core" {
			continue
		}
		presubmitScript := filepath.Join(repoRoot, "dev", "ci", "presubmits", "ap-e2e-"+grp)
		periodicScript := filepath.Join(repoRoot, "dev", "ci", "periodics", "ap-"+grp)
		_, errPre := os.Stat(presubmitScript)
		_, errPer := os.Stat(periodicScript)
		if os.IsNotExist(errPre) && os.IsNotExist(errPer) {
			t.Errorf("group %q has no matching script (expected %s or %s)", grp, presubmitScript, periodicScript)
		}
	}
}

func checkScriptNoRunFlag(data []byte) error {
	// Join escaped newlines so multi-line shell commands are evaluated as full invocations
	fullCommands := strings.ReplaceAll(string(data), "\\\n", " ")
	for _, line := range strings.Split(fullCommands, "\n") {
		// Strip comments so innocent comments mentioning -run do not trigger a false positive
		if idx := strings.Index(line, "#"); idx >= 0 {
			line = line[:idx]
		}
		trimmed := strings.TrimSpace(line)
		if strings.Contains(trimmed, "go test") && strings.Contains(trimmed, "-run") {
			return fmt.Errorf("go test invocation must not contain -run list (%s); sharding is controlled by OBJECTFS_E2E_GROUP", trimmed)
		}
	}
	return nil
}

func TestE2EScripts_NoRunLists(t *testing.T) {
	repoRoot := filepath.Join("..", "..")
	scriptDirs := []string{
		filepath.Join(repoRoot, "dev", "ci", "presubmits"),
		filepath.Join(repoRoot, "dev", "ci", "periodics"),
	}

	for _, dir := range scriptDirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("failed to read dir %s: %v", dir, err)
		}
		for _, e := range entries {
			if !strings.HasPrefix(e.Name(), "ap-e2e") && !strings.HasPrefix(e.Name(), "ap-torture") {
				continue
			}
			scriptPath := filepath.Join(dir, e.Name())
			data, err := os.ReadFile(scriptPath)
			if err != nil {
				t.Fatalf("failed to read %s: %v", scriptPath, err)
			}
			if err := checkScriptNoRunFlag(data); err != nil {
				t.Errorf("script %s: %v", scriptPath, err)
			}
			if strings.Contains(string(data), "gotestsum@") {
				t.Errorf("script %s contains gotestsum@ literal; gotestsum must be invoked via tools module", scriptPath)
			}
		}
	}
}

func TestE2EGroups_DriftGuardDetection(t *testing.T) {
	cases := []struct {
		name      string
		code      string
		wantErr   string
		wantGroup string
	}{
		{
			name: "valid core",
			code: `package test
func TestValid(t *testing.T) {
	requireE2EGroup(t, "core")
}`,
			wantGroup: "core",
		},
		{
			name: "valid multinode",
			code: `package test
func TestValidMulti(t *testing.T) {
	requireE2EGroup(t, "multinode")
}`,
			wantGroup: "multinode",
		},
		{
			name: "missing requireE2EGroup",
			code: `package test
func TestMissing(t *testing.T) {
	t.Log("no gating")
}`,
			wantErr: "must call requireE2EGroup as its first statement",
		},
		{
			name: "not first statement",
			code: `package test
func TestNotFirst(t *testing.T) {
	t.Log("hello")
	requireE2EGroup(t, "core")
}`,
			wantErr: "must call requireE2EGroup as its first statement",
		},
		{
			name: "multiple calls",
			code: `package test
func TestMultiple(t *testing.T) {
	requireE2EGroup(t, "core")
	requireE2EGroup(t, "core")
}`,
			wantErr: "calls requireE2EGroup 2 times; must be called exactly once",
		},
		{
			name: "non-literal argument first",
			code: `package test
func TestNonLit(t *testing.T) {
	requireE2EGroup(t, someVar)
}`,
			wantErr: "requireE2EGroup group argument must be a string literal",
		},
		{
			name: "empty string argument",
			code: `package test
func TestEmptyStr(t *testing.T) {
	requireE2EGroup(t, "")
}`,
			wantErr: "requireE2EGroup group argument must be a non-empty string literal",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, "test.go", tc.code, 0)
			if err != nil {
				t.Fatalf("failed to parse test code: %v", err)
			}
			fn := file.Decls[0].(*ast.FuncDecl)
			grp, err := inspectRequireE2EGroup(fn)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("expected error containing %q, got %v", tc.wantErr, err)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if grp != tc.wantGroup {
					t.Fatalf("expected group %q, got %q", tc.wantGroup, grp)
				}
			}
		})
	}

	// Verify checkScriptNoRunFlag ignores innocent comments with -run and detects actual go test -run invocations
	scriptCases := []struct {
		name    string
		script  string
		wantErr bool
	}{
		{
			name: "innocent comment with -run",
			script: `#!/bin/bash
# Note: we previously used -run '^(TestFoo)$' here
RUN_E2E=1 go test -v -count=1 -timeout 15m -json ./tests/e2e/ | tee out.json
`,
			wantErr: false,
		},
		{
			name: "inline comment with -run",
			script: `#!/bin/bash
RUN_E2E=1 go test -v -count=1 -timeout 15m -json ./tests/e2e/ # formerly had -run
`,
			wantErr: false,
		},
		{
			name: "actual go test with -run flag",
			script: `#!/bin/bash
RUN_E2E=1 go test -v -count=1 -timeout 15m -run '^(TestFoo)$' -json ./tests/e2e/
`,
			wantErr: true,
		},
		{
			name: "actual go test with multiline -run flag",
			script: `#!/bin/bash
RUN_E2E=1 go test -v -count=1 \
  -timeout 15m \
  -run '^(TestFoo)$' \
  -json ./tests/e2e/
`,
			wantErr: true,
		},
	}

	for _, sc := range scriptCases {
		t.Run("script_"+sc.name, func(t *testing.T) {
			err := checkScriptNoRunFlag([]byte(sc.script))
			if sc.wantErr && err == nil {
				t.Fatalf("expected error for script %q, got nil", sc.name)
			}
			if !sc.wantErr && err != nil {
				t.Fatalf("unexpected error for script %q: %v", sc.name, err)
			}
		})
	}
}

func TestNoCommittedPrivateKeys(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join(".", "..", ".."))

	err := filepath.Walk(repoRoot, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			if info.Name() == ".git" || info.Name() == ".build" || info.Name() == "bin" {
				return filepath.SkipDir
			}
			return nil
		}

		if strings.HasSuffix(path, ".key") {
			t.Errorf("found private key file committed to repository: %s", path)
		}

		if strings.HasSuffix(path, ".pem") {
			data, readErr := os.ReadFile(path)
			if readErr == nil && strings.Contains(string(data), "PRIVATE KEY") {
				t.Errorf("found PEM file containing private key committed to repository: %s", path)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking repo: %v", err)
	}
}

func TestRBAC_TLSInitSecurity(t *testing.T) {
	manifestPath := filepath.Clean(filepath.Join(".", "..", "..", "k8s", "objectfs.yaml"))
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("failed to read %s: %v", manifestPath, err)
	}
	content := string(data)

	// Verify certsigner Job runs under dedicated serviceAccountName: objectfs-certsigner
	if !strings.Contains(content, "serviceAccountName: objectfs-certsigner") {
		t.Errorf("objectfs-certsigner Job must run under dedicated serviceAccountName: objectfs-certsigner")
	}

	// Verify controller does not have secret write privileges
	docs := strings.Split(content, "---")
	for _, doc := range docs {
		if strings.Contains(doc, "kind: Role") || strings.Contains(doc, "kind: ClusterRole") {
			if strings.Contains(doc, "name: objectfs-controller") {
				if strings.Contains(doc, "resources: [\"secrets\"]") || strings.Contains(doc, "\"secrets\"") {
					t.Errorf("controller role must not grant permissions on secrets")
				}
			}
		}
		// Verify node daemon DaemonSet never mounts the server secret
		if strings.Contains(doc, "kind: DaemonSet") && strings.Contains(doc, "name: objectfs-node-daemon") {
			if strings.Contains(doc, "objectfs-controller-server") {
				t.Errorf("objectfs-node-daemon DaemonSet must not mount objectfs-controller-server")
			}
			if !strings.Contains(doc, "objectfs-node-daemon-client") {
				t.Errorf("objectfs-node-daemon DaemonSet must mount objectfs-node-daemon-client")
			}
		}
		// Verify controller StatefulSet never mounts the admin client secret
		if strings.Contains(doc, "kind: StatefulSet") && strings.Contains(doc, "name: objectfs-controller") {
			if strings.Contains(doc, "objectfs-admin-client") {
				t.Errorf("objectfs-controller StatefulSet must not mount objectfs-admin-client")
			}
		}
	}
}
