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

package erofs

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

type memoryWriterAt struct {
	buf []byte
}

func (m *memoryWriterAt) WriteAt(p []byte, off int64) (int, error) {
	end := off + int64(len(p))
	if end > int64(len(m.buf)) {
		newBuf := make([]byte, end)
		copy(newBuf, m.buf)
		m.buf = newBuf
	}
	copy(m.buf[off:end], p)
	return len(p), nil
}

// compareTrees recursively compares a Node tree with a compiled EROFS filesystem Reader,
// ensuring count, name, type, and file contents match exactly without hardcoded offsets.
func compareTrees(t *testing.T, expected Node, reader *Reader, nid uint64) {
	if expected.IsDir() {
		expectedChildren, err := expected.Children()
		if err != nil {
			t.Fatalf("failed to get expected children: %v", err)
		}
		actualChildren, err := reader.ListDirectory(nid)
		if err != nil {
			t.Fatalf("failed to list actual directory at NID %d: %v", nid, err)
		}

		// Verify that "." and ".." are present in actualChildren
		foundDot := false
		foundDotDot := false
		for _, de := range actualChildren {
			if de.Name == "." {
				foundDot = true
				if de.FileType != FTDir {
					t.Errorf("expected FTDir for '.', got %d", de.FileType)
				}
				if de.NID != nid {
					t.Errorf("expected '.' to point to self NID %d, got %d", nid, de.NID)
				}
			}
			if de.Name == ".." {
				foundDotDot = true
				if de.FileType != FTDir {
					t.Errorf("expected FTDir for '..', got %d", de.FileType)
				}
			}
		}

		if !foundDot {
			t.Errorf("directory at NID %d does not contain '.' entry", nid)
		}
		if !foundDotDot {
			t.Errorf("directory at NID %d does not contain '..' entry", nid)
		}

		// Filter out "." and ".." from actualChildren
		var actualFiltered []Dirent
		for _, de := range actualChildren {
			if de.Name != "." && de.Name != ".." {
				actualFiltered = append(actualFiltered, de)
			}
		}

		if len(expectedChildren) != len(actualFiltered) {
			t.Fatalf("directory children count mismatch under %s: expected %d, got %d", expected.Name(), len(expectedChildren), len(actualFiltered))
		}

		for i, ec := range expectedChildren {
			ac := actualFiltered[i]
			if ec.Name() != ac.Name {
				t.Fatalf("child name mismatch under %s at index %d: expected %s, got %s", expected.Name(), i, ec.Name(), ac.Name)
			}

			// Validate file type
			var expectedFileType uint8
			if ec.IsDir() {
				expectedFileType = FTDir
			} else if (ec.Mode() & S_IFMT) == S_IFLNK {
				expectedFileType = FTSymlink
			} else {
				expectedFileType = FTRegFile
			}

			if ac.FileType != expectedFileType {
				t.Errorf("file type mismatch for %s: expected %d, got %d", ec.Name(), expectedFileType, ac.FileType)
			}

			// Recurse into children
			compareTrees(t, ec, reader, ac.NID)
		}
	} else {
		// Verify file/symlink content
		rc, err := expected.Open()
		if err != nil {
			t.Fatalf("failed to open expected file content for %s: %v", expected.Name(), err)
		}
		defer rc.Close()
		expectedBytes, err := io.ReadAll(rc)
		if err != nil {
			t.Fatalf("failed to read expected file content for %s: %v", expected.Name(), err)
		}

		r, err := reader.ReadFileContent(nid)
		if err != nil {
			t.Fatalf("failed to read actual file content at NID %d: %v", nid, err)
		}
		actualBytes, err := io.ReadAll(r)
		if err != nil {
			t.Fatalf("failed to read actual streamed content: %v", err)
		}

		if !bytes.Equal(expectedBytes, actualBytes) {
			t.Errorf("content mismatch for %s: expected %q, got %q", expected.Name(), string(expectedBytes), string(actualBytes))
		}
	}
}

func TestErofsSuccess(t *testing.T) {
	// 1. Define paths, directories, contents and modes to BuildTree
	paths := []string{
		"hello.txt",
		"foo/bar.txt",
		"foo/baz",
		"foo/baz/deep.txt",
		"sym_link",
	}
	isDirs := []bool{
		false,
		false,
		true,
		false,
		false,
	}
	contents := [][]byte{
		[]byte("Hello, EROFS!"),
		[]byte("Nested file content."),
		nil,
		[]byte("Deeply nested file."),
		[]byte("hello.txt"),
	}
	modes := []uint16{
		0644,
		0644,
		0755,
		0644,
		S_IFLNK | 0777,
	}

	rootNode, err := BuildTree(paths, isDirs, contents, modes)
	if err != nil {
		t.Fatalf("failed to build virtual tree: %v", err)
	}

	// 2. Compile to an EROFS image using memoryWriterAt
	mw := &memoryWriterAt{}
	err = WriteImage(mw, rootNode)
	if err != nil {
		t.Fatalf("failed to write EROFS image: %v", err)
	}

	imageBytes := mw.buf
	readerAt := bytes.NewReader(imageBytes)

	// 3. Perform Fsck validation
	err = Fsck(readerAt)
	if err != nil {
		t.Errorf("Fsck failed on valid image: %v", err)
	}

	// 4. Use Reader to navigate and recursively verify with expected tree
	reader, err := NewReader(readerAt)
	if err != nil {
		t.Fatalf("failed to create Reader: %v", err)
	}

	compareTrees(t, rootNode, reader, reader.sb.GetRootNID())
}

func createMinimalImage(t *testing.T) []byte {
	root, err := BuildTree(
		[]string{"file.txt"},
		[]bool{false},
		[][]byte{[]byte("test data")},
		[]uint16{0644},
	)
	if err != nil {
		t.Fatalf("failed to build minimal tree: %v", err)
	}

	mw := &memoryWriterAt{}
	err = WriteImage(mw, root)
	if err != nil {
		t.Fatalf("failed to write image: %v", err)
	}

	return mw.buf
}

func TestErofsFsckFailures(t *testing.T) {
	t.Run("Invalid Superblock Magic", func(t *testing.T) {
		img := createMinimalImage(t)
		// Corrupt SuperMagic at SuperOffset (1024)
		img[SuperOffset] = 0xAA
		img[SuperOffset+1] = 0xBB
		img[SuperOffset+2] = 0xCC
		img[SuperOffset+3] = 0xDD

		err := Fsck(bytes.NewReader(img))
		if err == nil {
			t.Error("expected Fsck to fail with invalid superblock magic")
		} else if !IsInvalidSuperblockError(err) {
			t.Errorf("expected ErrInvalidSuperblock error, got %v", err)
		}
	})

	t.Run("Superblock Checksum Verification", func(t *testing.T) {
		img := createMinimalImage(t)
		// Corrupt checksum byte at index SuperOffset + 4
		img[SuperOffset+4] ^= 0xFF

		err := Fsck(bytes.NewReader(img))
		if err == nil {
			t.Error("expected Fsck to fail with invalid superblock checksum")
		} else if !IsInvalidSuperblockError(err) {
			t.Errorf("expected ErrInvalidSuperblock error, got %v", err)
		}
	})

	t.Run("Directory Entry Null Byte Injection", func(t *testing.T) {
		root, err := BuildTree(
			[]string{"magicnullpath", "zz_lastfile"},
			[]bool{false, false},
			[][]byte{[]byte("test bytes"), []byte("more bytes")},
			[]uint16{0644, 0644},
		)
		if err != nil {
			t.Fatalf("failed to build tree: %v", err)
		}
		mw := &memoryWriterAt{}
		if err := WriteImage(mw, root); err != nil {
			t.Fatalf("failed to compile image: %v", err)
		}
		img := mw.buf

		// Find the index of "magicnullpath" filename on-disk
		idx := bytes.Index(img, []byte("magicnullpath"))
		if idx == -1 {
			t.Fatalf("could not find filename string inside compiled image")
		}

		// Corrupt a character to null byte
		corruptImg := make([]byte, len(img))
		copy(corruptImg, img)
		corruptImg[idx+5] = 0 // "magic\x00ullpath"

		err = Fsck(bytes.NewReader(corruptImg))
		if err == nil {
			t.Error("expected Fsck to fail when null byte is injected in a directory entry filename")
		} else if !IsPathTraversalError(err) {
			t.Errorf("expected ErrPathTraversal, got %v", err)
		}
	})

	t.Run("Directory Entry Slash Injection", func(t *testing.T) {
		root, err := BuildTree(
			[]string{"magicslashpath"},
			[]bool{false},
			[][]byte{[]byte("test bytes")},
			[]uint16{0644},
		)
		if err != nil {
			t.Fatalf("failed to build tree: %v", err)
		}
		mw := &memoryWriterAt{}
		if err := WriteImage(mw, root); err != nil {
			t.Fatalf("failed to compile image: %v", err)
		}
		img := mw.buf

		// Find the index of "magicslashpath" filename on-disk
		idx := bytes.Index(img, []byte("magicslashpath"))
		if idx == -1 {
			t.Fatalf("could not find filename string inside compiled image")
		}

		// Corrupt a character to a slash separator
		corruptImg := make([]byte, len(img))
		copy(corruptImg, img)
		corruptImg[idx+5] = '/' // "magic/lashpath"

		err = Fsck(bytes.NewReader(corruptImg))
		if err == nil {
			t.Error("expected Fsck to fail when slash separator is injected in a directory entry filename")
		} else if !IsPathTraversalError(err) {
			t.Errorf("expected ErrPathTraversal, got %v", err)
		}
	})

	t.Run("Cycle Detection", func(t *testing.T) {
		root, err := BuildTree(
			[]string{"dir1"},
			[]bool{true},
			[][]byte{nil},
			[]uint16{0755},
		)
		if err != nil {
			t.Fatalf("failed to build virtual tree: %v", err)
		}

		mw := &memoryWriterAt{}
		err = WriteImage(mw, root)
		if err != nil {
			t.Fatalf("failed to write image: %v", err)
		}

		imageBytes := mw.buf
		readerAt := bytes.NewReader(imageBytes)

		sb, err := ReadSuperblock(readerAt)
		if err != nil {
			t.Fatalf("failed to read superblock: %v", err)
		}

		reader, err := NewReader(readerAt)
		if err != nil {
			t.Fatalf("failed to create reader: %v", err)
		}

		rootDirents, err := reader.ListDirectory(sb.GetRootNID())
		if err != nil {
			t.Fatalf("failed to list root directory: %v", err)
		}

		var dir1NID uint64
		for _, de := range rootDirents {
			if de.Name == "dir1" {
				dir1NID = de.NID
				break
			}
		}
		if dir1NID == 0 {
			t.Fatalf("failed to find dir1 in root dirents")
		}

		// Find the directory's inode.
		inode, err := ReadInode(readerAt, sb, dir1NID)
		if err != nil {
			t.Fatalf("failed to read inode for dir1 (%d): %v", dir1NID, err)
		}

		dirents := []Dirent{
			{NID: dir1NID, Name: ".", FileType: FTDir},
			{NID: 0, Name: "..", FileType: FTDir},
			{NID: dir1NID, Name: "loop", FileType: FTDir},
		}

		block, err := BuildDirectoryBlock(dirents, BlockSize4K)
		if err != nil {
			t.Fatalf("failed to build directory block: %v", err)
		}

		dataOffset := int64(inode.RawBlkaddr) * BlockSize4K
		copy(imageBytes[dataOffset:dataOffset+BlockSize4K], block)

		err = Fsck(bytes.NewReader(imageBytes))
		if err == nil {
			t.Error("expected Fsck to return an error when a cycle is detected")
		} else if !IsCycleDetectedError(err) {
			t.Errorf("expected ErrCycleDetected, got %v", err)
		}
	})
}

func TestErofsPhysicalDirectory(t *testing.T) {
	// Build a filesystem in a tmp directory, compile it, and compare
	tmpDir, err := os.MkdirTemp("", "erofs-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create physical structure
	err = os.WriteFile(filepath.Join(tmpDir, "file1.txt"), []byte("Physical file 1 content."), 0644)
	if err != nil {
		t.Fatalf("failed to write file: %v", err)
	}

	subDir := filepath.Join(tmpDir, "subdir")
	err = os.Mkdir(subDir, 0755)
	if err != nil {
		t.Fatalf("failed to mkdir: %v", err)
	}

	err = os.WriteFile(filepath.Join(subDir, "file2.txt"), []byte("Physical file 2 content inside subdir."), 0644)
	if err != nil {
		t.Fatalf("failed to write file: %v", err)
	}

	err = os.Symlink("file1.txt", filepath.Join(tmpDir, "link1"))
	if err != nil {
		t.Fatalf("failed to symlink: %v", err)
	}

	// Build the EROFS tree using local directory node
	fsNode := NewFileSystemNode("", tmpDir)

	mw := &memoryWriterAt{}
	err = WriteImage(mw, fsNode)
	if err != nil {
		t.Fatalf("failed to write image: %v", err)
	}

	readerAt := bytes.NewReader(mw.buf)
	err = Fsck(readerAt)
	if err != nil {
		t.Errorf("Fsck failed on physically compiled EROFS image: %v", err)
	}

	reader, err := NewReader(readerAt)
	if err != nil {
		t.Fatalf("failed to create reader: %v", err)
	}

	// Recursively validate compiled EROFS tree against the physical fileSystemNode tree input!
	compareTrees(t, fsNode, reader, reader.sb.GetRootNID())
}

func TestErofsXattrsAndComposeFS(t *testing.T) {
	// 1. Create in-memory tree with xattrs and metadata-only files (composefs style)
	file1Xattrs := Xattrs{
		UserDigest: "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
		UserSHA256: "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
		Others: map[string]string{
			"trusted.mime": "text/plain",
		},
	}

	file1 := NewMemoryNode(
		"largefile.bin",
		false,
		0644,
		nil,
		nil,
		WithMetadataOnly(true),
		WithSize(100*1024*1024), // 100MB declared size
		WithXattrs(file1Xattrs),
	)

	file2 := NewMemoryNode(
		"smallfile.txt",
		false,
		0644,
		[]byte("hello world"),
		nil,
		WithXattrs(Xattrs{
			Others: map[string]string{
				"user.tag": "greeting",
			},
		}),
	)

	root := NewMemoryNode(
		"",
		true,
		0755,
		nil,
		[]Node{file1, file2},
	)

	mw := &memoryWriterAt{}
	err := WriteImage(mw, root)
	if err != nil {
		t.Fatalf("failed to write image with xattrs and composefs metadata: %v", err)
	}

	// Verify image size is tiny (not 100MB!)
	if len(mw.buf) > 64*1024 {
		t.Fatalf("expected metadata-only image to be under 64KB, got %d bytes", len(mw.buf))
	}

	// Run Fsck
	readerAt := bytes.NewReader(mw.buf)
	if err := Fsck(readerAt); err != nil {
		t.Fatalf("Fsck failed on xattr & composefs image: %v", err)
	}

	// Read using Reader
	reader, err := NewReader(readerAt)
	if err != nil {
		t.Fatalf("failed to create reader: %v", err)
	}

	dirents, err := reader.ListDirectory(reader.sb.GetRootNID())
	if err != nil {
		t.Fatalf("failed to list directory: %v", err)
	}

	var foundFile1, foundFile2 bool
	for _, de := range dirents {
		if de.Name == "largefile.bin" {
			foundFile1 = true
			inode, err := ReadInode(readerAt, reader.sb, de.NID)
			if err != nil {
				t.Fatalf("failed to read largefile inode: %v", err)
			}
			if inode.Size != 100*1024*1024 {
				t.Fatalf("expected largefile size 100MB, got %d", inode.Size)
			}

			xattrs, err := reader.GetXattrs(de.NID)
			if err != nil {
				t.Fatalf("failed to get largefile xattrs: %v", err)
			}
			if xattrs.UserDigest != file1Xattrs.UserDigest {
				t.Fatalf("expected user.digest %q, got %q", file1Xattrs.UserDigest, xattrs.UserDigest)
			}
			if xattrs.Others["trusted.mime"] != "text/plain" {
				t.Fatalf("expected trusted.mime 'text/plain', got %q", xattrs.Others["trusted.mime"])
			}
		}
		if de.Name == "smallfile.txt" {
			foundFile2 = true
			xattrs, err := reader.GetXattrs(de.NID)
			if err != nil {
				t.Fatalf("failed to get smallfile xattrs: %v", err)
			}
			if xattrs.Others["user.tag"] != "greeting" {
				t.Fatalf("expected user.tag 'greeting', got %q", xattrs.Others["user.tag"])
			}
			rc, err := reader.ReadFileContent(de.NID)
			if err != nil {
				t.Fatalf("failed to read smallfile content: %v", err)
			}
			content, _ := io.ReadAll(rc)
			if string(content) != "hello world" {
				t.Fatalf("expected content 'hello world', got %q", string(content))
			}
		}
	}

	if !foundFile1 || !foundFile2 {
		t.Fatalf("expected both files to be found in directory listing")
	}
}

func TestErofsStridedPlacementAndExtendedInodes(t *testing.T) {
	t.Run("5 GiB Extended Inode with Digest fits in stride", func(t *testing.T) {
		fileXattrs := Xattrs{
			UserDigest: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		}
		file5GB := NewMemoryNode(
			"bigfile.iso",
			false,
			0644,
			nil,
			nil,
			WithIno(8),
			WithMetadataOnly(true),
			WithSize(5*1024*1024*1024), // 5 GiB (requires extended inode > 4GB)
			WithXattrs(fileXattrs),
		)
		root := NewMemoryNode(
			"",
			true,
			0755,
			nil,
			[]Node{file5GB},
			WithIno(0),
		)

		mw := &memoryWriterAt{}
		err := WriteImage(mw, root)
		if err != nil {
			t.Fatalf("failed to write image with 5GiB file: %v", err)
		}

		readerAt := bytes.NewReader(mw.buf)
		if err := Fsck(readerAt); err != nil {
			t.Fatalf("Fsck failed on 5GiB extended inode image: %v", err)
		}

		reader, err := NewReader(readerAt)
		if err != nil {
			t.Fatalf("failed to create reader: %v", err)
		}

		inode, err := ReadInode(readerAt, reader.sb, 8)
		if err != nil {
			t.Fatalf("failed to read inode at NID 8: %v", err)
		}
		if inode.Version != 1 {
			t.Fatalf("expected extended inode (version 1), got %d", inode.Version)
		}
		if inode.Size != 5*1024*1024*1024 {
			t.Fatalf("expected size 5GiB, got %d", inode.Size)
		}

		xattrs, err := reader.GetXattrs(8)
		if err != nil {
			t.Fatalf("failed to get xattrs: %v", err)
		}
		if xattrs.UserDigest != fileXattrs.UserDigest {
			t.Fatalf("expected user.digest %q, got %q", fileXattrs.UserDigest, xattrs.UserDigest)
		}
	})

	t.Run("Overlapping placement rejected", func(t *testing.T) {
		file1 := NewMemoryNode("file1.txt", false, 0644, []byte("data"), nil, WithIno(8))
		file2 := NewMemoryNode("file2.txt", false, 0644, []byte("data"), nil, WithIno(8)) // same NID
		root := NewMemoryNode("", true, 0755, nil, []Node{file1, file2}, WithIno(0))

		mw := &memoryWriterAt{}
		err := WriteImage(mw, root)
		if err == nil {
			t.Fatalf("expected WriteImage to fail with duplicate/overlapping NID")
		}
	})

	t.Run("Over-stride footprint rejected when exceeding shared budget", func(t *testing.T) {
		// Create > 45 xattrs on an extended inode so that even with all xattrs shared,
		// the inline header + shared ID references exceed 8 slots (256 bytes).
		manyXattrs := Xattrs{
			Others: make(map[string]string),
		}
		for i := 0; i < 50; i++ {
			manyXattrs.Others[fmt.Sprintf("user.attr_%02d", i)] = fmt.Sprintf("val_%d", i)
		}
		fileTooBig := NewMemoryNode(
			"huge_xattrs.txt",
			false,
			0644,
			nil,
			nil,
			WithIno(8),
			WithMetadataOnly(true),
			WithSize(5*1024*1024*1024), // Extended inode
			WithXattrs(manyXattrs),
		)
		root := NewMemoryNode("", true, 0755, nil, []Node{fileTooBig}, WithIno(0))

		mw := &memoryWriterAt{}
		err := WriteImage(mw, root)
		if err == nil {
			t.Fatalf("expected WriteImage to fail when xattr count exceeds stride capacity")
		}
		if !strings.Contains(err.Error(), "huge_xattrs.txt") || !strings.Contains(err.Error(), "8") {
			t.Fatalf("expected error to name path and inode, got: %v", err)
		}
	})

	t.Run("Large directory places inode in stride and entries in blocks", func(t *testing.T) {
		var children []Node
		for i := 0; i < 500; i++ {
			ino := uint64((i + 1) * DefaultInodeStride)
			children = append(children, NewMemoryNode(fmt.Sprintf("entry_%04d.txt", i), false, 0644, []byte("test"), nil, WithIno(ino)))
		}
		root := NewMemoryNode("", true, 0755, nil, children, WithIno(0))

		mw := &memoryWriterAt{}
		err := WriteImage(mw, root)
		if err != nil {
			t.Fatalf("failed to write image with large directory: %v", err)
		}

		readerAt := bytes.NewReader(mw.buf)
		if err := Fsck(readerAt); err != nil {
			t.Fatalf("Fsck failed on large directory image: %v", err)
		}

		reader, err := NewReader(readerAt)
		if err != nil {
			t.Fatalf("failed to create reader: %v", err)
		}

		dirents, err := reader.ListDirectory(0)
		if err != nil {
			t.Fatalf("failed to list large directory: %v", err)
		}
		// 500 children + "." + ".." = 502
		if len(dirents) != 502 {
			t.Fatalf("expected 502 dirents, got %d", len(dirents))
		}
	})
}

// verifyWithFsckErofs runs external fsck.erofs if installed on the system.
func verifyWithFsckErofs(t *testing.T, imgBytes []byte) {
	fsckPath, err := exec.LookPath("fsck.erofs")
	if err != nil {
		t.Logf("fsck.erofs not found in PATH, skipping external fsck validation")
		return
	}

	tmpFile, err := os.CreateTemp("", "erofs_test_*.img")
	if err != nil {
		t.Fatalf("failed to create temp file for fsck.erofs: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.Write(imgBytes); err != nil {
		t.Fatalf("failed to write img to temp file: %v", err)
	}
	_ = tmpFile.Close()

	cmd := exec.Command(fsckPath, "-d9", "--xattrs", tmpFile.Name())
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("external fsck.erofs failed on generated image: %v\nOutput: %s", err, string(out))
	}
}

func TestErofsXattrSpillingTable(t *testing.T) {
	// Table from Issue #137:
	// 1. digest only, compact (file1) / extended (file2)
	// 2. digest + user.objectfs.manifest, extended (file3)
	// 3. digest + user.objectfs.manifest + one 32-byte user xattr, extended (file4) -> spills
	// 4. digest + user.objectfs.manifest + SELinux label, compact (file5) / extended (file6) -> spills

	digestVal := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	manifestVal := "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"
	user32Val := "this_is_a_32_byte_custom_user_xattr"
	selinuxVal := "system_u:object_r:container_file_t:s0"

	f1Xattrs := Xattrs{UserDigest: digestVal}
	f2Xattrs := Xattrs{UserDigest: digestVal}
	f3Xattrs := Xattrs{UserDigest: digestVal, UserManifest: manifestVal}
	f4Xattrs := Xattrs{
		UserDigest:   digestVal,
		UserManifest: manifestVal,
		Others: map[string]string{
			"user.custom32": user32Val,
		},
	}
	f5Xattrs := Xattrs{
		UserDigest:   digestVal,
		UserManifest: manifestVal,
		Others: map[string]string{
			"security.selinux": selinuxVal,
		},
	}
	f6Xattrs := Xattrs{
		UserDigest:   digestVal,
		UserManifest: manifestVal,
		Others: map[string]string{
			"security.selinux": selinuxVal,
		},
	}

	f1 := NewMemoryNode("f1_digest_compact.txt", false, 0644, []byte("f1"), nil, WithIno(8), WithXattrs(f1Xattrs))
	f2 := NewMemoryNode("f2_digest_extended.txt", false, 0644, nil, nil, WithIno(16), WithMetadataOnly(true), WithSize(5*1024*1024*1024), WithXattrs(f2Xattrs))
	f3 := NewMemoryNode("f3_manifest_extended.txt", false, 0644, nil, nil, WithIno(24), WithMetadataOnly(true), WithSize(5*1024*1024*1024), WithXattrs(f3Xattrs))
	f4 := NewMemoryNode("f4_user32_extended.txt", false, 0644, nil, nil, WithIno(32), WithMetadataOnly(true), WithSize(5*1024*1024*1024), WithXattrs(f4Xattrs))
	f5 := NewMemoryNode("f5_selinux_compact.txt", false, 0644, []byte("f5"), nil, WithIno(40), WithXattrs(f5Xattrs))
	f6 := NewMemoryNode("f6_selinux_extended.txt", false, 0644, nil, nil, WithIno(48), WithMetadataOnly(true), WithSize(5*1024*1024*1024), WithXattrs(f6Xattrs))

	root := NewMemoryNode("", true, 0755, nil, []Node{f1, f2, f3, f4, f5, f6}, WithIno(0))

	mw := &memoryWriterAt{}
	if err := WriteImage(mw, root); err != nil {
		t.Fatalf("WriteImage failed: %v", err)
	}

	readerAt := bytes.NewReader(mw.buf)
	if err := Fsck(readerAt); err != nil {
		t.Fatalf("Fsck failed: %v", err)
	}
	verifyWithFsckErofs(t, mw.buf)

	reader, err := NewReader(readerAt)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}

	tests := []struct {
		nid      uint64
		expected Xattrs
	}{
		{8, f1Xattrs},
		{16, f2Xattrs},
		{24, f3Xattrs},
		{32, f4Xattrs},
		{40, f5Xattrs},
		{48, f6Xattrs},
	}

	for _, tc := range tests {
		got, err := reader.GetXattrs(tc.nid)
		if err != nil {
			t.Fatalf("GetXattrs(nid=%d) failed: %v", tc.nid, err)
		}
		if got.UserDigest != tc.expected.UserDigest {
			t.Errorf("nid=%d UserDigest mismatch: expected %q, got %q", tc.nid, tc.expected.UserDigest, got.UserDigest)
		}
		if got.UserManifest != tc.expected.UserManifest {
			t.Errorf("nid=%d UserManifest mismatch: expected %q, got %q", tc.nid, tc.expected.UserManifest, got.UserManifest)
		}
		for k, expectedVal := range tc.expected.Others {
			if got.Others[k] != expectedVal {
				t.Errorf("nid=%d others[%q] mismatch: expected %q, got %q", tc.nid, k, expectedVal, got.Others[k])
			}
		}
	}
}

func TestErofsXattr16KiBValue(t *testing.T) {
	largeVal := strings.Repeat("A1b2C3d4E5f6G7h8", 1024) // 16 KiB
	fileXattrs := Xattrs{
		UserDigest: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		Others: map[string]string{
			"user.large_payload": largeVal,
		},
	}

	file := NewMemoryNode("large_xattr.bin", false, 0644, []byte("file data"), nil, WithIno(8), WithXattrs(fileXattrs))
	root := NewMemoryNode("", true, 0755, nil, []Node{file}, WithIno(0))

	mw := &memoryWriterAt{}
	if err := WriteImage(mw, root); err != nil {
		t.Fatalf("WriteImage failed with 16KiB xattr: %v", err)
	}

	readerAt := bytes.NewReader(mw.buf)
	if err := Fsck(readerAt); err != nil {
		t.Fatalf("Fsck failed on 16KiB xattr image: %v", err)
	}
	verifyWithFsckErofs(t, mw.buf)

	reader, err := NewReader(readerAt)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}

	got, err := reader.GetXattrs(8)
	if err != nil {
		t.Fatalf("GetXattrs failed: %v", err)
	}
	if got.UserDigest != fileXattrs.UserDigest {
		t.Errorf("UserDigest mismatch: expected %q, got %q", fileXattrs.UserDigest, got.UserDigest)
	}
	if got.Others["user.large_payload"] != largeVal {
		t.Errorf("user.large_payload mismatch: length got=%d, expected=%d", len(got.Others["user.large_payload"]), len(largeVal))
	}
}

func TestErofsSharedXattrDeduplication(t *testing.T) {
	sharedVal := strings.Repeat("shared-security-context-data-", 50) // ~1500 bytes
	sharedXattrs := Xattrs{
		UserDigest: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		Others: map[string]string{
			"security.selinux": "system_u:object_r:container_file_t:s0",
			"trusted.policy":   sharedVal,
		},
	}

	var files []Node
	for i := 0; i < 10; i++ {
		ino := uint64((i + 1) * DefaultInodeStride)
		files = append(files, NewMemoryNode(
			fmt.Sprintf("shared_file_%d.txt", i),
			false,
			0644,
			[]byte("hello"),
			nil,
			WithIno(ino),
			WithXattrs(sharedXattrs),
		))
	}
	root := NewMemoryNode("", true, 0755, nil, files, WithIno(0))

	mw := &memoryWriterAt{}
	if err := WriteImage(mw, root); err != nil {
		t.Fatalf("WriteImage failed: %v", err)
	}

	readerAt := bytes.NewReader(mw.buf)
	if err := Fsck(readerAt); err != nil {
		t.Fatalf("Fsck failed on deduplicated image: %v", err)
	}
	verifyWithFsckErofs(t, mw.buf)

	reader, err := NewReader(readerAt)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}

	// Verify all files can retrieve the identical shared xattr values accurately
	for i := 0; i < 10; i++ {
		ino := uint64((i + 1) * DefaultInodeStride)
		got, err := reader.GetXattrs(ino)
		if err != nil {
			t.Fatalf("GetXattrs for file %d failed: %v", i, err)
		}
		if got.Others["trusted.policy"] != sharedVal {
			t.Errorf("File %d trusted.policy mismatch", i)
		}
		if got.Others["security.selinux"] != "system_u:object_r:container_file_t:s0" {
			t.Errorf("File %d security.selinux mismatch", i)
		}
	}
}

func TestErofsRootRegistryMultiKilobyteXattr(t *testing.T) {
	registryPayload := strings.Repeat("stream-schema-registry-definition-metadata;", 200) // ~8.6 KiB
	rootXattrs := Xattrs{
		Others: map[string]string{
			"trusted.sds.registry": registryPayload,
		},
	}

	childXattrs := Xattrs{
		UserDigest: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
	}
	child := NewMemoryNode("data.parquet", false, 0644, []byte("data"), nil, WithIno(8), WithXattrs(childXattrs))
	root := NewMemoryNode("", true, 0755, nil, []Node{child}, WithIno(0), WithXattrs(rootXattrs))

	mw := &memoryWriterAt{}
	if err := WriteImage(mw, root); err != nil {
		t.Fatalf("WriteImage failed with root multi-kilobyte registry: %v", err)
	}

	readerAt := bytes.NewReader(mw.buf)
	if err := Fsck(readerAt); err != nil {
		t.Fatalf("Fsck failed on root registry image: %v", err)
	}
	verifyWithFsckErofs(t, mw.buf)

	reader, err := NewReader(readerAt)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}

	rootGot, err := reader.GetXattrs(0)
	if err != nil {
		t.Fatalf("GetXattrs for root failed: %v", err)
	}
	if rootGot.Others["trusted.sds.registry"] != registryPayload {
		t.Fatalf("Root registry payload mismatch: length got=%d, expected=%d", len(rootGot.Others["trusted.sds.registry"]), len(registryPayload))
	}

	childGot, err := reader.GetXattrs(8)
	if err != nil {
		t.Fatalf("GetXattrs for child failed: %v", err)
	}
	if childGot.UserDigest != childXattrs.UserDigest {
		t.Fatalf("Child digest mismatch: expected %q, got %q", childXattrs.UserDigest, childGot.UserDigest)
	}
}

func TestErofsPerInodeBudgetEnforcement(t *testing.T) {
	// 50 xattrs on an extended inode exceeds the maximum 45 references capacity for 8 slots.
	manyXattrs := Xattrs{
		Others: make(map[string]string),
	}
	for i := 0; i < 50; i++ {
		manyXattrs.Others[fmt.Sprintf("user.custom_attr_%02d", i)] = fmt.Sprintf("val_%d", i)
	}

	file := NewMemoryNode(
		"budget_test.txt",
		false,
		0644,
		nil,
		nil,
		WithIno(8),
		WithMetadataOnly(true),
		WithSize(5*1024*1024*1024), // Extended inode
		WithXattrs(manyXattrs),
	)
	root := NewMemoryNode("", true, 0755, nil, []Node{file}, WithIno(0))

	mw := &memoryWriterAt{}
	err := WriteImage(mw, root)
	if err == nil {
		t.Fatalf("expected WriteImage to fail due to per-inode budget limit")
	}

	expectedPath := "/budget_test.txt"
	expectedInode := "8"
	if !strings.Contains(err.Error(), expectedPath) {
		t.Errorf("expected error to contain path %q, got: %v", expectedPath, err)
	}
	if !strings.Contains(err.Error(), expectedInode) {
		t.Errorf("expected error to contain inode %q, got: %v", expectedInode, err)
	}
}

func TestErofsSpecialFiles(t *testing.T) {
	chrNode := NewMemoryNode("test_chr", false, S_IFCHR|0660, nil, nil, WithIno(8), WithRdev(0x0103))
	blkNode := NewMemoryNode("test_blk", false, S_IFBLK|0660, nil, nil, WithIno(16), WithRdev(0x0801))
	fifoNode := NewMemoryNode("test_fifo", false, S_IFIFO|0644, nil, nil, WithIno(24))
	sockNode := NewMemoryNode("test_sock", false, S_IFSOCK|0666, nil, nil, WithIno(32))

	root := NewMemoryNode("", true, 0755, nil, []Node{chrNode, blkNode, fifoNode, sockNode}, WithIno(0))

	mw := &memoryWriterAt{}
	if err := WriteImage(mw, root); err != nil {
		t.Fatalf("WriteImage failed: %v", err)
	}

	readerAt := bytes.NewReader(mw.buf)
	if err := Fsck(readerAt); err != nil {
		t.Fatalf("Fsck failed on special files image: %v", err)
	}

	reader, err := NewReader(readerAt)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}

	dirents, err := reader.ListDirectory(0)
	if err != nil {
		t.Fatalf("ListDirectory failed: %v", err)
	}

	type fileCheck struct {
		ft   uint8
		mode uint16
		rdev uint32
	}
	expected := map[string]fileCheck{
		"test_chr":  {ft: FTChrDev, mode: S_IFCHR | 0660, rdev: 0x0103},
		"test_blk":  {ft: FTBlkDev, mode: S_IFBLK | 0660, rdev: 0x0801},
		"test_fifo": {ft: FTFifo, mode: S_IFIFO | 0644, rdev: 0},
		"test_sock": {ft: FTSock, mode: S_IFSOCK | 0666, rdev: 0},
	}

	for _, de := range dirents {
		if de.Name == "." || de.Name == ".." {
			continue
		}
		exp, ok := expected[de.Name]
		if !ok {
			t.Errorf("unexpected dirent: %s", de.Name)
			continue
		}
		if de.FileType != exp.ft {
			t.Errorf("%s: expected FileType %d, got %d", de.Name, exp.ft, de.FileType)
		}
		inode, err := reader.ReadInode(de.NID)
		if err != nil {
			t.Errorf("%s: ReadInode failed: %v", de.Name, err)
			continue
		}
		if inode.Mode != exp.mode {
			t.Errorf("%s: expected Mode 0%o, got 0%o", de.Name, exp.mode, inode.Mode)
		}
		if inode.Rdev != exp.rdev {
			t.Errorf("%s: expected Rdev 0x%x, got 0x%x", de.Name, exp.rdev, inode.Rdev)
		}
	}
}
