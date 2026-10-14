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

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <errno.h>

static int failed = 0;

#define CHECK(cond, msg, ...) do { \
    if (!(cond)) { \
        fprintf(stderr, "FAIL: " msg " (errno=%d: %s)\n", ##__VA_ARGS__, errno, strerror(errno)); \
        failed++; \
    } else { \
        printf("PASS: " msg "\n", ##__VA_ARGS__); \
    } \
} while (0)

void test_basic_mmap_write(const char *path) {
    printf("\n=== Test 1: Basic mmap write within file size ===\n");
    int fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0644);
    CHECK(fd >= 0, "open %s", path);
    if (fd < 0) return;

    // Create file of 1000 bytes with pattern 'A'
    char buf[1000];
    memset(buf, 'A', sizeof(buf));
    ssize_t nw = write(fd, buf, sizeof(buf));
    CHECK(nw == 1000, "write 1000 bytes");

    // mmap 1000 bytes
    char *map1 = mmap(NULL, 1000, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    CHECK(map1 != MAP_FAILED, "mmap MAP_SHARED");
    if (map1 == MAP_FAILED) { close(fd); return; }

    // Write 'B' at offset 100..199
    memset(map1 + 100, 'B', 100);

    // msync
    int ms = msync(map1, 1000, MS_SYNC);
    CHECK(ms == 0, "msync MS_SYNC");

    // read back via read(2)
    char read_buf[100];
    ssize_t nr = pread(fd, read_buf, 100, 100);
    CHECK(nr == 100, "pread 100 bytes at offset 100");
    int match = 1;
    for (int i = 0; i < 100; i++) {
        if (read_buf[i] != 'B') { match = 0; break; }
    }
    CHECK(match, "read(2) verified data written via mmap");

    // read back via a second mapping
    char *map2 = mmap(NULL, 1000, PROT_READ, MAP_SHARED, fd, 0);
    CHECK(map2 != MAP_FAILED, "second mmap MAP_SHARED");
    if (map2 != MAP_FAILED) {
        match = 1;
        for (int i = 0; i < 100; i++) {
            if (map2[100 + i] != 'B') { match = 0; break; }
        }
        CHECK(match, "second mapping verified data written via first mapping");
        CHECK(munmap(map2, 1000) == 0, "munmap map2");
    }

    CHECK(munmap(map1, 1000) == 0, "munmap map1");

    // Check size
    struct stat st;
    CHECK(fstat(fd, &st) == 0, "fstat");
    CHECK(st.st_size == 1000, "file size is 1000 (actual: %lld)", (long long)st.st_size);

    CHECK(close(fd) == 0, "close fd");
    CHECK(unlink(path) == 0, "unlink %s", path);
}

void test_size_preservation_last_page(const char *path) {
    printf("\n=== Test 2: Size preservation on writeback of last page ===\n");
    int fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0644);
    CHECK(fd >= 0, "open %s", path);
    if (fd < 0) return;

    // File size is 100 bytes (sub-page)
    char buf[100];
    memset(buf, 'X', sizeof(buf));
    ssize_t nw = write(fd, buf, sizeof(buf));
    CHECK(nw == 100, "write 100 bytes");

    struct stat st_before;
    CHECK(fstat(fd, &st_before) == 0, "fstat initial size");

    // Map 4096 bytes (one page)
    char *map = mmap(NULL, 4096, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    CHECK(map != MAP_FAILED, "mmap 4096 bytes");
    if (map == MAP_FAILED) { close(fd); return; }

    // Modify bytes 10..20 (within 100 bytes)
    memset(map + 10, 'Y', 10);

    // msync
    int ms = msync(map, 4096, MS_SYNC);
    CHECK(ms == 0, "msync MS_SYNC");

    CHECK(munmap(map, 4096) == 0, "munmap 4096 bytes");

    // Stat the file: size MUST NOT have grown to 4096!
    struct stat st_after;
    CHECK(fstat(fd, &st_after) == 0, "fstat after msync");
    CHECK(st_after.st_size == 100, "file size preserved after msync (expected 100, got %lld)", (long long)st_after.st_size);

    CHECK(close(fd) == 0, "close fd");
    CHECK(unlink(path) == 0, "unlink %s", path);
}

void test_interleaved_write_and_mmap(const char *path) {
    printf("\n=== Test 3: Interleaved write(2) and mmap write to same chunk ===\n");
    int fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0644);
    CHECK(fd >= 0, "open %s", path);
    if (fd < 0) return;

    // Create 8192 bytes
    char buf[8192];
    memset(buf, 0, sizeof(buf));
    ssize_t nw = write(fd, buf, sizeof(buf));
    CHECK(nw == sizeof(buf), "write 8192 bytes");

    char *map = mmap(NULL, 8192, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    CHECK(map != MAP_FAILED, "mmap 8192 bytes");

    // Write through write(2) at 0..99
    char wbuf[100];
    memset(wbuf, 'W', 100);
    ssize_t npw = pwrite(fd, wbuf, 100, 0);
    CHECK(npw == 100, "pwrite 100 bytes at offset 0");

    // Write through mmap at 200..299
    memset(map + 200, 'M', 100);

    // msync
    int ms = msync(map, 8192, MS_SYNC);
    CHECK(ms == 0, "msync 8192 bytes");

    // Verify through read(2)
    char r1[100], r2[100];
    ssize_t nr1 = pread(fd, r1, 100, 0);
    CHECK(nr1 == 100, "pread 100 bytes at offset 0");
    ssize_t nr2 = pread(fd, r2, 100, 200);
    CHECK(nr2 == 100, "pread 100 bytes at offset 200");

    int w_ok = 1, m_ok = 1;
    for (int i = 0; i < 100; i++) {
        if (r1[i] != 'W') w_ok = 0;
        if (r2[i] != 'M') m_ok = 0;
    }
    CHECK(w_ok, "write(2) data intact at offset 0");
    CHECK(m_ok, "mmap data intact at offset 200");

    CHECK(munmap(map, 8192) == 0, "munmap 8192 bytes");
    CHECK(close(fd) == 0, "close fd");
    CHECK(unlink(path) == 0, "unlink %s", path);
}

void test_dirty_page_truncated_before_writeback(const char *path) {
    printf("\n=== Test 4: Dirty page truncated away before writeback ===\n");
    int fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0644);
    CHECK(fd >= 0, "open %s", path);
    if (fd < 0) return;

    // File size 65536 initialized to 'A'
    char buf[65536];
    memset(buf, 'A', sizeof(buf));
    ssize_t nw = write(fd, buf, sizeof(buf));
    CHECK(nw == sizeof(buf), "write 65536 bytes");

    char *map = mmap(NULL, 65536, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    CHECK(map != MAP_FAILED, "mmap 65536");

    // Dirty the page at offset 40960 (page 10)
    memset(map + 40960, 'Z', 100);

    // Truncate file down to 1024 bytes (before the dirty page!)
    int tr = ftruncate(fd, 1024);
    CHECK(tr == 0, "ftruncate to 1024 bytes");

    // msync the remaining valid range and unmap
    CHECK(msync(map, 1024, MS_SYNC) == 0, "msync 1024 bytes");
    CHECK(munmap(map, 65536) == 0, "munmap 65536 bytes");

    // Check size
    struct stat st;
    CHECK(fstat(fd, &st) == 0, "fstat after truncate and munmap");
    CHECK(st.st_size == 1024, "size remains 1024 after truncate and munmap (got %lld)", (long long)st.st_size);

    // Assert that the remaining 1024 bytes are still intact ('A')
    char buf1024[1024];
    ssize_t nr = pread(fd, buf1024, 1024, 0);
    CHECK(nr == 1024, "pread 1024 bytes after truncate");
    int intact = 1;
    for (int i = 0; i < 1024; i++) {
        if (buf1024[i] != 'A') {
            intact = 0;
            break;
        }
    }
    CHECK(intact, "first 1024 bytes remain 'A' after truncate");

    CHECK(close(fd) == 0, "close fd");
    CHECK(unlink(path) == 0, "unlink %s", path);
}

int main(int argc, char **argv) {
    const char *test_dir = (argc > 1) ? argv[1] : ".";
    char path[512];

    snprintf(path, sizeof(path), "%s/test_mmap_1.bin", test_dir);
    test_basic_mmap_write(path);

    snprintf(path, sizeof(path), "%s/test_mmap_2.bin", test_dir);
    test_size_preservation_last_page(path);

    snprintf(path, sizeof(path), "%s/test_mmap_3.bin", test_dir);
    test_interleaved_write_and_mmap(path);

    snprintf(path, sizeof(path), "%s/test_mmap_4.bin", test_dir);
    test_dirty_page_truncated_before_writeback(path);

    printf("\nTotal failures: %d\n", failed);
    return failed > 0 ? 1 : 0;
}
