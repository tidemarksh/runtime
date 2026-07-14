import { execFileSync } from "node:child_process";
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "vite-plus/test";
import {
  BUSYBOX_BINARY_PATH,
  SAMPLE_MODULE_DIR,
  goEnv,
  prepareGoRuntime,
} from "./support.js";
import {
  type RuntimeProcess,
  type RuntimeInstance,
  decoder,
  logSkip,
  runtimeDebugSummary,
  spawnWithOutput,
} from "../../../support/runtime-workload.js";

const GO_BUILD_INSPECT_WORK = process.env.TIDEMARK_ACCEPTANCE_GO_BUILD_INSPECT_WORK === "1";
type GoRuntimeOptions = NonNullable<Parameters<typeof prepareGoRuntime>[0]>;
const GO_BUILD_HOST_FILE_BACKING_MODE = process.env
  .TIDEMARK_ACCEPTANCE_GO_BUILD_HOST_FILE_BACKING_MODE as GoRuntimeOptions["hostFileBackingMode"] | undefined;
const GO_BUILD_PARALLELISM = process.env.TIDEMARK_ACCEPTANCE_GO_BUILD_PARALLELISM;
const GO_BUILD_PRECREATE_CACHE_SHARDS = process.env
  .TIDEMARK_ACCEPTANCE_GO_BUILD_PRECREATE_CACHE_SHARDS === "1";
const HOST_RISCV_GCC = process.env.TIDEMARK_TEST_HOST_RISCV_GCC ?? "riscv64-linux-gnu-gcc";
const GO_TOOLCHAIN_DIR = process.env.TIDEMARK_TEST_GO_TOOLCHAIN_DIR;
const encoder = new TextEncoder();

const CONCURRENT_FD_SOURCE_PATHS = [
  "internal/abi/abi.go",
  "internal/abi/abi_generic.go",
  "internal/abi/abi_riscv64.go",
  "internal/abi/bounds.go",
  "internal/abi/compiletype.go",
  "internal/abi/escape.go",
  "internal/abi/funcpc.go",
  "internal/abi/iface.go",
  "internal/abi/map.go",
  "internal/abi/rangefuncconsts.go",
  "internal/abi/runtime.go",
  "internal/abi/stack.go",
  "internal/abi/switch.go",
  "internal/abi/symtab.go",
  "internal/abi/type.go",
  "internal/goarch/goarch.go",
  "internal/goarch/goarch_riscv64.go",
  "internal/goarch/zgoarch_riscv64.go",
  "internal/goos/goos.go",
  "internal/goos/zgoos_linux.go",
  "runtime/arena.go",
  "runtime/mgc.go",
  "runtime/malloc.go",
] as const;

const INTERNAL_ABI_SOURCE_PATHS = [
  "abi.go",
  "abi_riscv64.go",
  "bounds.go",
  "compiletype.go",
  "escape.go",
  "funcpc.go",
  "iface.go",
  "map.go",
  "rangefuncconsts.go",
  "runtime.go",
  "stack.go",
  "switch.go",
  "symtab.go",
  "type.go",
] as const;

function fnv1a32(bytes: Uint8Array): number {
  let hash = 0x811c9dc5;
  for (const byte of bytes) {
    hash = Math.imul(hash ^ byte, 0x01000193);
  }
  return hash >>> 0;
}

function buildGoSourceMmapProbe(): Uint8Array | null {
  const tmp = mkdtempSync(join(tmpdir(), "tidemark-go-source-mmap-"));
  const sourcePath = join(tmp, "go_source_mmap.c");
  const outputPath = join(tmp, "go-source-mmap");
  try {
    writeFileSync(sourcePath, [
      "#include <errno.h>",
      "#include <fcntl.h>",
      "#include <stdio.h>",
      "#include <string.h>",
      "#include <sys/mman.h>",
      "#include <sys/stat.h>",
      "#include <unistd.h>",
      "",
      "int main(int argc, char **argv) {",
      "  if (argc != 2) { return 64; }",
      "  int fd = open(argv[1], O_RDONLY);",
      "  if (fd < 0) { perror(\"open\"); return 65; }",
      "  struct stat st;",
      "  if (fstat(fd, &st) != 0) { perror(\"fstat\"); close(fd); return 66; }",
      "  if (st.st_size <= 0) { close(fd); return 67; }",
      "  const unsigned char *data = mmap(NULL, (size_t)st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);",
      "  if (data == MAP_FAILED) { perror(\"mmap\"); close(fd); return 68; }",
      "  const char needle[] = \"package runtime\";",
      "  long found = -1;",
      "  for (long i = 0; i + (long)sizeof(needle) - 1 <= st.st_size; i++) {",
      "    if (memcmp(data + i, needle, sizeof(needle) - 1) == 0) { found = i; break; }",
      "  }",
      "  printf(\"go-source-mmap size=%lld package_offset=%ld b4095=%u b4096=%u\\n\",",
      "         (long long)st.st_size, found, data[4095], data[4096]);",
      "  if (munmap((void *)data, (size_t)st.st_size) != 0) { perror(\"munmap\"); close(fd); return 69; }",
      "  if (close(fd) != 0) { perror(\"close\"); return 70; }",
      "  return found >= 0 ? 0 : 71;",
      "}",
      "",
    ].join("\n"));
    execFileSync(HOST_RISCV_GCC, ["-static", "-O2", "-o", outputPath, sourcePath], {
      stdio: "pipe",
    });
    return new Uint8Array(readFileSync(outputPath));
  } catch (error) {
    return logSkip(`failed to build Go source mmap probe with ${HOST_RISCV_GCC}: ${
      error instanceof Error ? (error.stack ?? error.message) : String(error)
    }`);
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
}

function buildConcurrentFdReuseProbe(
  sourceCases: readonly { guestPath: string; bytes: Uint8Array }[],
): Uint8Array | null {
  const tmp = mkdtempSync(join(tmpdir(), "tidemark-fd-reuse-"));
  const sourcePath = join(tmp, "fd_reuse.c");
  const outputPath = join(tmp, "fd-reuse");
  try {
    const expectedSources = sourceCases.map(({ guestPath, bytes }) => {
      const expected = bytes.subarray(0, Math.min(bytes.length, 8192));
      return `  { ${JSON.stringify(guestPath)}, ${fnv1a32(expected)}u, ${expected.length}u },`;
    });
    writeFileSync(sourcePath, [
      "#include <fcntl.h>",
      "#include <pthread.h>",
      "#include <stdint.h>",
      "#include <stdio.h>",
      "#include <stdlib.h>",
      "#include <unistd.h>",
      "",
      "struct expected { const char *path; uint32_t hash; size_t len; };",
      "struct args { int first_source; int id; };",
      "static const struct expected expected_sources[24] = {",
      ...expectedSources,
      "};",
      "static pthread_barrier_t opened_barrier;",
      "",
      "static uint32_t hash_bytes(const unsigned char *buf, size_t len) {",
      "  uint32_t hash = 0x811c9dc5u;",
      "  for (size_t i = 0; i < len; i++) { hash = (hash ^ buf[i]) * 0x01000193u; }",
      "  return hash;",
      "}",
      "",
      "static void *run(void *opaque) {",
      "  struct args *args = (struct args *)opaque;",
      "  unsigned char buf[8192];",
      "  pthread_barrier_wait(&opened_barrier);",
      "  for (int iteration = 0; iteration < 64; iteration++) {",
      "    for (int file = 0; file < 3; file++) {",
      "      const struct expected *expected = &expected_sources[args->first_source + file];",
      "      int fd = open(expected->path, O_RDONLY);",
      "      if (fd < 0) { perror(expected->path); return (void *)1; }",
      "      ssize_t n = read(fd, buf, expected->len);",
      "      uint32_t actual_hash = n > 0 ? hash_bytes(buf, (size_t)n) : 0;",
      "      if (n != (ssize_t)expected->len || actual_hash != expected->hash) {",
      "        fprintf(stderr, \"mismatch thread=%d iteration=%d path=%s n=%ld expected_len=%zu hash=%u expected_hash=%u\\n\",",
      "                args->id, iteration, expected->path, (long)n, expected->len, actual_hash, expected->hash);",
      "        close(fd);",
      "        return (void *)2;",
      "      }",
      "      if (close(fd) != 0) { return (void *)3; }",
      "    }",
      "  }",
      "  return 0;",
      "}",
      "",
      "int main(int argc, char **argv) {",
      "  if (argc != 1) { fprintf(stderr, \"unexpected argc=%d\\n\", argc); return 64; }",
      "  pthread_t threads[8];",
      "  struct args args[8];",
      "  if (pthread_barrier_init(&opened_barrier, 0, 8) != 0) { return 65; }",
      "  for (int i = 0; i < 8; i++) {",
      "    args[i] = (struct args){ i * 3, i };",
      "    if (pthread_create(&threads[i], 0, run, &args[i]) != 0) { return 66; }",
      "  }",
      "  for (int i = 0; i < 8; i++) {",
      "    void *result = 0;",
      "    if (pthread_join(threads[i], &result) != 0 || result != 0) { return 67; }",
      "  }",
      "  pthread_barrier_destroy(&opened_barrier);",
      "  puts(\"concurrent-distinct-fd-ok\");",
      "  return 0;",
      "}",
      "",
    ].join("\n"));
    execFileSync(HOST_RISCV_GCC, ["-static", "-O2", "-pthread", "-o", outputPath, sourcePath], {
      stdio: "pipe",
    });
    return new Uint8Array(readFileSync(outputPath));
  } catch (error) {
    return logSkip(`failed to build concurrent fd reuse probe with ${HOST_RISCV_GCC}: ${
      error instanceof Error ? (error.stack ?? error.message) : String(error)
    }`);
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
}

function buildCrossThreadFdHandoffProbe(
  sourceCases: readonly { guestPath: string; bytes: Uint8Array }[],
): Uint8Array | null {
  const tmp = mkdtempSync(join(tmpdir(), "tidemark-fd-handoff-"));
  const sourcePath = join(tmp, "fd_handoff.c");
  const outputPath = join(tmp, "fd-handoff");
  try {
    const expectedSources = sourceCases.map(({ guestPath, bytes }) => {
      const expected = bytes.subarray(0, Math.min(bytes.length, 8192));
      return `  { ${JSON.stringify(guestPath)}, ${fnv1a32(expected)}u, ${expected.length}u },`;
    });
    writeFileSync(sourcePath, [
      "#include <fcntl.h>",
      "#include <pthread.h>",
      "#include <stdint.h>",
      "#include <stdio.h>",
      "#include <unistd.h>",
      "",
      "struct expected { const char *path; uint32_t hash; size_t len; };",
      `static const struct expected expected_sources[${sourceCases.length}] = {`,
      ...expectedSources,
      "};",
      "static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;",
      "static pthread_cond_t changed = PTHREAD_COND_INITIALIZER;",
      "static int shared_fd = -1;",
      "static int shared_source = -1;",
      "static int failure = 0;",
      "",
      "static uint32_t hash_bytes(const unsigned char *buf, size_t len) {",
      "  uint32_t hash = 0x811c9dc5u;",
      "  for (size_t i = 0; i < len; i++) { hash = (hash ^ buf[i]) * 0x01000193u; }",
      "  return hash;",
      "}",
      "",
      "static void *open_files(void *opaque) {",
      "  (void)opaque;",
      `  for (int iteration = 0; iteration < ${sourceCases.length * 4}; iteration++) {`,
      `    int source = (iteration / 2) % ${sourceCases.length};`,
      "    pthread_mutex_lock(&lock);",
      "    while (shared_fd != -1 && failure == 0) { pthread_cond_wait(&changed, &lock); }",
      "    pthread_mutex_unlock(&lock);",
      "    if (failure != 0) { return (void *)1; }",
      "    int fd = open(expected_sources[source].path, O_RDONLY);",
      "    if (fd < 0) { perror(expected_sources[source].path); failure = 1; return (void *)2; }",
      "    pthread_mutex_lock(&lock);",
      "    shared_source = source;",
      "    shared_fd = fd;",
      "    pthread_cond_broadcast(&changed);",
      "    while (shared_fd != -1 && failure == 0) { pthread_cond_wait(&changed, &lock); }",
      "    pthread_mutex_unlock(&lock);",
      "    if (failure != 0) { return (void *)3; }",
      "  }",
      "  return 0;",
      "}",
      "",
      "static void *read_files(void *opaque) {",
      "  (void)opaque;",
      "  unsigned char buf[8192];",
      `  for (int iteration = 0; iteration < ${sourceCases.length * 4}; iteration++) {`,
      "    pthread_mutex_lock(&lock);",
      "    while (shared_fd == -1 && failure == 0) { pthread_cond_wait(&changed, &lock); }",
      "    int fd = shared_fd;",
      "    int source = shared_source;",
      "    pthread_mutex_unlock(&lock);",
      "    if (failure != 0) { return (void *)4; }",
      "    const struct expected *expected = &expected_sources[source];",
      "    ssize_t n = read(fd, buf, expected->len);",
      "    uint32_t actual_hash = n > 0 ? hash_bytes(buf, (size_t)n) : 0;",
      "    if (n != (ssize_t)expected->len || actual_hash != expected->hash) {",
      "      fprintf(stderr, \"handoff mismatch iteration=%d fd=%d path=%s n=%ld expected_len=%zu hash=%u expected_hash=%u\\n\",",
      "              iteration, fd, expected->path, (long)n, expected->len, actual_hash, expected->hash);",
      "      failure = 1;",
      "    }",
      "    if (close(fd) != 0) { perror(\"close\"); failure = 1; }",
      "    pthread_mutex_lock(&lock);",
      "    shared_fd = -1;",
      "    pthread_cond_broadcast(&changed);",
      "    pthread_mutex_unlock(&lock);",
      "    if (failure != 0) { return (void *)5; }",
      "  }",
      "  return 0;",
      "}",
      "",
      "int main(void) {",
      "  pthread_t opener;",
      "  pthread_t reader;",
      "  if (pthread_create(&opener, 0, open_files, 0) != 0) { return 64; }",
      "  if (pthread_create(&reader, 0, read_files, 0) != 0) { return 65; }",
      "  void *open_result = 0;",
      "  void *read_result = 0;",
      "  if (pthread_join(opener, &open_result) != 0) { return 66; }",
      "  if (pthread_join(reader, &read_result) != 0) { return 67; }",
      "  if (open_result != 0 || read_result != 0 || failure != 0) { return 68; }",
      "  puts(\"cross-thread-fd-handoff-ok\");",
      "  return 0;",
      "}",
      "",
    ].join("\n"));
    execFileSync(HOST_RISCV_GCC, ["-static", "-O2", "-pthread", "-o", outputPath, sourcePath], {
      stdio: "pipe",
    });
    return new Uint8Array(readFileSync(outputPath));
  } catch (error) {
    return logSkip(`failed to build cross-thread fd handoff probe with ${HOST_RISCV_GCC}: ${
      error instanceof Error ? (error.stack ?? error.message) : String(error)
    }`);
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
}

function goBuildArgs(): string[] {
  return [
    "go",
    "build",
    ...(GO_BUILD_INSPECT_WORK ? ["-work"] : []),
    ...(GO_BUILD_PARALLELISM ? ["-p", GO_BUILD_PARALLELISM] : []),
    "-x",
    "-v",
    "-o",
    `${SAMPLE_MODULE_DIR}/hello`,
    ".",
  ];
}

function workDirFromOutput(output: string): string | null {
  const match = /^WORK=(.+)$/m.exec(output);
  return match?.[1]?.trim() || null;
}

async function precreateGoCacheShards(runtime: RuntimeInstance): Promise<void> {
  if (!GO_BUILD_PRECREATE_CACHE_SHARDS) {
    return;
  }
  for (let index = 0; index < 256; index += 1) {
    await runtime.fs.mkdir(`/tmp/gocache/${index.toString(16).padStart(2, "0")}`, 0o755)
      .catch(() => {});
  }
}

function joinGuestPath(parent: string, name: string): string {
  return parent === "/" ? `/${name}` : `${parent}/${name}`;
}

async function inspectGuestPath(runtime: RuntimeInstance, path: string): Promise<string[]> {
  try {
    const stat = await runtime.fs.stat(path);
    const type = stat.mode & 0o170000;
    if (type === 0o040000) {
      const names = await runtime.fs.readdir(path);
      return [`${path}: dir entries=${names.length} [${names.sort().slice(0, 24).join(",")}]`];
    }
    if (type !== 0o100000) {
      return [`${path}: mode=${stat.mode.toString(8)} size=${stat.size}`];
    }
    const data = await runtime.fs.readFile(path);
    const firstLine = decoder.decode(data.subarray(0, Math.min(data.length, 160)))
      .split("\n")[0]
      ?.replaceAll("\0", "\\0") ?? "";
    return [`${path}: file statSize=${stat.size} readSize=${data.length} first=${JSON.stringify(firstLine)}`];
  } catch (error) {
    return [`${path}: failed: ${error instanceof Error ? error.message : String(error)}`];
  }
}

async function inspectGuestTextLines(
  runtime: RuntimeInstance,
  path: string,
  startLine: number,
  endLine: number,
): Promise<string[]> {
  try {
    const data = await runtime.fs.readFile(path);
    const lines = decoder.decode(data).split("\n");
    const rows = [`${path}: lines ${startLine}-${endLine}`];
    for (let lineNo = startLine; lineNo <= endLine; lineNo += 1) {
      rows.push(`  ${lineNo}: ${lines[lineNo - 1] ?? "<missing>"}`);
    }
    return rows;
  } catch (error) {
    return [`${path}: line read failed: ${error instanceof Error ? error.message : String(error)}`];
  }
}

async function inspectGuestBytes(runtime: RuntimeInstance, path: string, start: number, length: number): Promise<string[]> {
  try {
    const data = await runtime.fs.readFile(path);
    const bytes = data.subarray(start, Math.min(data.length, start + length));
    const text = decoder.decode(bytes).replaceAll("\0", "\\0").replaceAll("\n", "\\n");
    return [
      `${path}: bytes ${start}-${start + bytes.length}`,
      `  hex=${[...bytes].map((byte) => byte.toString(16).padStart(2, "0")).join(" ")}`,
      `  text=${JSON.stringify(text)}`,
    ];
  } catch (error) {
    return [`${path}: byte read failed: ${error instanceof Error ? error.message : String(error)}`];
  }
}

async function inspectGoBuildFailure(
  runtime: RuntimeInstance,
  output: string,
  inspectGuestResidentRead: boolean,
): Promise<string> {
  const rows: string[] = ["go-build-failure-inspect:"];
  rows.push(`trace-page-cache-before-inspect=${JSON.stringify(await runtime.debugPageCachePath("/usr/local/go/src/runtime/trace.go"))}`);
  const sourcePaths = [
    "/usr/local/go/src/internal",
    "/usr/local/go/src/internal/goarch",
    "/usr/local/go/src/internal/goarch/goarch.go",
    "/usr/local/go/src/internal/goarch/zgoarch_riscv64.go",
    "/usr/local/go/src/internal/goos",
    "/usr/local/go/src/internal/goos/goos.go",
    "/usr/local/go/src/internal/goos/zgoos_linux.go",
    "/usr/local/go/src/internal/cpu",
    "/usr/local/go/src/internal/cpu/cpu.go",
    "/usr/local/go/src/internal/bytealg",
    "/usr/local/go/src/internal/bytealg/bytealg.go",
    "/usr/local/go/src/internal/runtime/atomic",
    "/usr/local/go/src/internal/runtime/atomic/types.go",
    "/usr/local/go/src/runtime/atomic_pointer.go",
    "/usr/local/go/src/runtime/arena.go",
    "/usr/local/go/src/runtime/cgocall.go",
    "/usr/local/go/src/runtime/extern.go",
    "/usr/local/go/src/runtime/malloc.go",
    "/usr/local/go/src/runtime/mgc.go",
    "/usr/local/go/src/runtime/mgcscavenge.go",
    "/usr/local/go/src/runtime/trace.go",
    "/tmp/gocache",
  ];
  for (const path of sourcePaths) {
    rows.push(...await inspectGuestPath(runtime, path));
  }
  rows.push(...await inspectGuestTextLines(runtime, "/usr/local/go/src/internal/goarch/zgoarch_riscv64.go", 1, 8));
  rows.push(`zgoarch-page-cache=${JSON.stringify(await runtime.debugPageCachePath("/usr/local/go/src/internal/goarch/zgoarch_riscv64.go"))}`);
  rows.push(`mgcscavenge-page-cache=${JSON.stringify(await runtime.debugPageCachePath("/usr/local/go/src/runtime/mgcscavenge.go"))}`);
  rows.push(`mgc-page-cache=${JSON.stringify(await runtime.debugPageCachePath("/usr/local/go/src/runtime/mgc.go"))}`);
  rows.push(`malloc-page-cache=${JSON.stringify(await runtime.debugPageCachePath("/usr/local/go/src/runtime/malloc.go"))}`);
  rows.push(...await inspectGuestTextLines(runtime, "/usr/local/go/src/runtime/malloc.go", 1, 12));
  rows.push(...await inspectGuestTextLines(runtime, "/usr/local/go/src/runtime/malloc.go", 94, 106));
  rows.push(...await inspectGuestBytes(runtime, "/usr/local/go/src/runtime/malloc.go", 0, 32));
  rows.push(...await inspectGuestBytes(runtime, "/usr/local/go/src/runtime/malloc.go", 4088, 32));
  rows.push(...await inspectGuestBytes(runtime, "/usr/local/go/src/runtime/malloc.go", 8184, 32));
  rows.push(...await inspectGuestTextLines(runtime, "/usr/local/go/src/runtime/mgc.go", 86, 94));
  rows.push(...await inspectGuestBytes(runtime, "/usr/local/go/src/runtime/mgc.go", 4088, 64));
  if (inspectGuestResidentRead && BUSYBOX_BINARY_PATH && existsSync(BUSYBOX_BINARY_PATH)) {
    try {
      const residentRead = await spawnWithOutput(
        runtime,
        new Uint8Array(readFileSync(BUSYBOX_BINARY_PATH)),
        [
          "/bin/busybox",
          "dd",
          "if=/usr/local/go/src/runtime/malloc.go",
          "bs=4096",
          "skip=1",
          "count=1",
        ],
        ["PATH=/bin"],
        30_000,
      );
      const packageOffset = residentRead.output.indexOf("package runtime");
      rows.push(
        `malloc-resident-read: exit=${residentRead.exitCode} packageOffset=${packageOffset}`,
        `  page1[112:176]=${JSON.stringify(residentRead.output.slice(112, 176))}`,
      );
    } catch (error) {
      rows.push(`malloc-resident-read: failed: ${error instanceof Error ? error.message : String(error)}`);
    }
  }
  rows.push(...await inspectGuestTextLines(runtime, "/usr/local/go/src/runtime/mgcscavenge.go", 70, 78));
  rows.push(...await inspectGuestBytes(runtime, "/usr/local/go/src/runtime/mgcscavenge.go", 4088, 32));
  rows.push(...await inspectGuestTextLines(runtime, "/usr/local/go/src/runtime/extern.go", 295, 300));
  rows.push(...await inspectGuestTextLines(runtime, "/usr/local/go/src/runtime/trace.go", 65, 76));
  rows.push(`trace-page-cache-after-inspect=${JSON.stringify(await runtime.debugPageCachePath("/usr/local/go/src/runtime/trace.go"))}`);

  const workDir = workDirFromOutput(output);
  rows.push(...await inspectGuestPath(runtime, "/tmp"));
  if (!workDir) {
    rows.push("go-build-work: missing WORK line");
    return rows.join("\n");
  }
  rows.push(`go-build-work=${workDir}`);
  rows.push(...await inspectGuestPath(runtime, workDir));
  rows.push(`b004-importcfg-page-cache=${JSON.stringify(await runtime.debugPageCachePath(`${workDir}/b004/importcfg`))}`);
  for (let index = 1; index <= 20; index += 1) {
    const bucket = `b${String(index).padStart(3, "0")}`;
    rows.push(...await inspectGuestPath(runtime, `${workDir}/${bucket}`));
    rows.push(...await inspectGuestPath(runtime, `${workDir}/${bucket}/importcfg`));
    rows.push(...await inspectGuestPath(runtime, `${workDir}/${bucket}/importcfg.link`));
  }
  const targets = [
    "internal/goexperiment",
    "internal/goarch",
    "internal/goos",
    "internal/runtime/atomic",
    "internal/runtime/sys",
    "unsafe",
    "runtime",
  ];

  async function walk(path: string, depth: number): Promise<void> {
    if (depth > 4 || rows.length > 180) {
      return;
    }
    let names: string[];
    try {
      names = await runtime.fs.readdir(path);
    } catch (error) {
      rows.push(`${path}: readdir failed: ${error instanceof Error ? error.message : String(error)}`);
      return;
    }
    rows.push(`${path}: walk entries=${names.length} [${names.sort().slice(0, 24).join(",")}]`);
    for (const name of names.sort()) {
      const child = joinGuestPath(path, name);
      let stat;
      try {
        stat = await runtime.fs.stat(child);
      } catch {
        continue;
      }
      const type = stat.mode & 0o170000;
      if (type === 0o040000) {
        rows.push(`${child}/: dir size=${stat.size}`);
        await walk(child, depth + 1);
        continue;
      }
      if (!name.startsWith("importcfg") && !name.endsWith(".go") && !name.endsWith(".a")) {
        continue;
      }
      rows.push(`${child}: size=${stat.size}`);
      if (name.startsWith("importcfg")) {
        let text: string;
        try {
          text = decoder.decode(await runtime.fs.readFile(child));
        } catch (error) {
          rows.push(`${child}: read failed: ${error instanceof Error ? error.message : String(error)}`);
          continue;
        }
        const matched = text
          .split("\n")
          .filter((line) => targets.some((target) => line.includes(target)));
        rows.push(...(matched.length > 0 ? matched : ["  <no target packagefile lines>"]));
      }
    }
  }

  await walk(workDir, 0);
  return rows.join("\n");
}

test("Runtime executes go build", async () => {
  const gomaxprocs = Number(process.env.TIDEMARK_ACCEPTANCE_GO_BUILD_GOMAXPROCS ?? "1");
  const timeoutMs = Number(process.env.TIDEMARK_ACCEPTANCE_GO_BUILD_TIMEOUT_MS ?? "300000");
  const prepared = await prepareGoRuntime({
    includeToolchain: true,
    includeSampleModule: true,
    hostFileBackingMode: GO_BUILD_HOST_FILE_BACKING_MODE,
  });
  if (!prepared) {
    return;
  }

  const { runtime, goBytes } = prepared;
  try {
    await precreateGoCacheShards(runtime);
    const proc: RuntimeProcess = await runtime.spawn(
      goBytes,
      goBuildArgs(),
      goEnv(gomaxprocs),
      { cwd: SAMPLE_MODULE_DIR },
    );
    let output = "";
    let partialLine = "";
    let timeoutId: ReturnType<typeof setTimeout> | null = null;
    let timedOut = false;
    let compiledPackages = 0;
    let buildCommands = 0;

    proc.onStdout((data) => {
      const text = decoder.decode(data);
      output += text;
      partialLine += text;

      const lines = partialLine.split("\n");
      partialLine = lines.pop() ?? "";
      for (const line of lines) {
        if (line.startsWith("internal/") || line.startsWith("runtime")) {
          compiledPackages += 1;
        }
        if (
          line.startsWith("WORK=")
          || line.includes("/pkg/tool/linux_riscv64/compile ")
          || line.includes("/pkg/tool/linux_riscv64/link ")
        ) {
          buildCommands += 1;
        }
      }
    });

    const exitCode = await Promise.race([
      proc.waitForExit().catch(async (error) => {
        const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
        throw new Error(`${detail}\n${await runtimeDebugSummary(runtime)}`);
      }),
      new Promise<null>((resolve) => {
        timeoutId = setTimeout(() => {
          timedOut = true;
          resolve(null);
        }, timeoutMs);
        if (typeof timeoutId === "object" && timeoutId !== null && "unref" in timeoutId) {
          timeoutId.unref();
        }
      }),
    ]);

    if (timeoutId !== null) {
      clearTimeout(timeoutId);
    }

    const workInspection = GO_BUILD_INSPECT_WORK && (timedOut || exitCode !== 0)
      ? await inspectGoBuildFailure(runtime, output, !timedOut)
      : "";
    const failedSourceInspection = exitCode !== 0
      ? (await Promise.all(
        [...new Set(output.match(/\/usr\/local\/go\/src\/[^:\s]+\.go/g) ?? [])].map(async (path) => {
          try {
            const debug = await runtime.debugPageCachePath(path);
            const bytes = await runtime.fs.readFile(path);
            const head = [...bytes.subarray(0, 32)]
              .map((byte) => byte.toString(16).padStart(2, "0"))
              .join("");
            return `${path}:pageCache=${JSON.stringify(debug)}:size=${bytes.length}:head=${head}`;
          } catch (error) {
            return `${path}:inspection-error=${error instanceof Error ? error.message : String(error)}`;
          }
        }),
      )).join("\n")
      : "";

    expect(
      timedOut,
      `go build timed out after ${timeoutMs}ms\ncompiled_packages=${compiledPackages} build_commands=${buildCommands}\n${output}\n${workInspection}\n${proc.debugInfo?.() ?? ""}\n${await runtimeDebugSummary(runtime)}`,
    ).toBe(false);

    expect(
      exitCode,
      `go build exited with ${exitCode}\n${output}\n${failedSourceInspection}\n${workInspection}\n${proc.debugInfo?.() ?? ""}\n${await runtimeDebugSummary(runtime)}`,
    ).toBe(0);
    const artifactPath = `${SAMPLE_MODULE_DIR}/hello`;
    const artifact = await runtime.fs.readFile(artifactPath);
    expect(artifact.byteLength).toBeGreaterThan(0);
    const artifactStat = await runtime.fs.stat(artifactPath);
    expect(artifactStat.size).toBeGreaterThan(0);
  } finally {
    runtime.destroy();
  }
}, Number(process.env.TIDEMARK_ACCEPTANCE_GO_BUILD_TIMEOUT_MS ?? "300000") + 60_000);

test("Runtime prints go build plan", async () => {
  const prepared = await prepareGoRuntime({
    includeToolchain: true,
    includeSampleModule: true,
  });
  if (!prepared) {
    return;
  }

  const { runtime, goBytes } = prepared;
  try {
    const { exitCode, output, debugInfo } = await spawnWithOutput(
      runtime,
      goBytes,
      ["go", "build", "-n", "-x", "-v", "-o", `${SAMPLE_MODULE_DIR}/hello`, "."],
      goEnv(1),
      120_000,
      { cwd: SAMPLE_MODULE_DIR },
    ).catch(async (error) => {
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error(`${detail}\n${await runtimeDebugSummary(runtime)}`);
    });
    expect(
      exitCode,
      `go build -n exited with ${exitCode}\n${output}\n${debugInfo ?? ""}\n${await runtimeDebugSummary(runtime)}`,
    ).toBe(0);
    expect(
      output,
      `${debugInfo ?? ""}\n${await runtimeDebugSummary(runtime)}`,
    ).toContain("/pkg/tool/linux_riscv64/compile");
  } finally {
    runtime.destroy();
  }
}, 180_000);

test("Runtime lists go package deps", async () => {
  const timeoutMs = Number(process.env.TIDEMARK_ACCEPTANCE_GO_LIST_TIMEOUT_MS ?? "120000");
  const prepared = await prepareGoRuntime({
    includeToolchain: true,
    includeSampleModule: true,
    hostFileBackingMode: GO_BUILD_HOST_FILE_BACKING_MODE,
  });
  if (!prepared) {
    return;
  }

  const { runtime, goBytes } = prepared;
  try {
    const { exitCode, output, debugInfo } = await spawnWithOutput(
      runtime,
      goBytes,
      ["go", "list", "-x", "-deps", "."],
      goEnv(1),
      timeoutMs,
      { cwd: SAMPLE_MODULE_DIR },
    ).catch(async (error) => {
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error(`${detail}\n${await runtimeDebugSummary(runtime)}`);
    });
    const workInspection = exitCode === 0 ? "" : await inspectGoBuildFailure(runtime, output);
    expect(
      exitCode,
      `go list exited with ${exitCode}\n${output}\n${workInspection}\n${debugInfo ?? ""}\n${await runtimeDebugSummary(runtime)}`,
    ).toBe(0);
    expect(output).toContain("runtime");
  } finally {
    runtime.destroy();
  }
}, Number(process.env.TIDEMARK_ACCEPTANCE_GO_LIST_TIMEOUT_MS ?? "120000") + 60_000);

test("Runtime Go compiler reads late-package source", async () => {
  const prepared = await prepareGoRuntime({
    includeToolchain: true,
    includeSampleModule: false,
    hostFileBackingMode: GO_BUILD_HOST_FILE_BACKING_MODE,
  });
  if (!prepared) {
    return;
  }

  const { runtime, goBytes } = prepared;
  try {
    const latePackageSource = [
      ...Array.from({ length: 96 }, (_, index) => (
        `// ${String(index).padStart(2, "0")} ${"boundary diagnostic ".repeat(3)}`
      )),
      "package latepkg",
      "",
      "const Value = 1",
      "",
    ].join("\n");
    await runtime.fs.writeFile("/tmp/late-package.go", encoder.encode(latePackageSource), 0o644);

    const { exitCode, output, debugInfo } = await spawnWithOutput(
      runtime,
      goBytes,
      [
        "go",
        "tool",
        "compile",
        "-o",
        "/tmp/late-package.o",
        "/tmp/late-package.go",
      ],
      goEnv(1),
      30_000,
    ).catch(async (error) => {
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error(`${detail}\n${await runtimeDebugSummary(runtime)}`);
    });
    expect(
      exitCode,
      `go tool compile exited with ${exitCode}\n${output}\n${debugInfo ?? ""}\n${await runtimeDebugSummary(runtime)}`,
    ).toBe(0);
    expect(
      output,
      `${debugInfo ?? ""}\n${await runtimeDebugSummary(runtime)}`,
    ).not.toContain("expected 'package', found 'EOF'");
  } finally {
    runtime.destroy();
  }
}, 120_000);

test("Runtime compiles internal goarch with importcfg and source fd reuse", async () => {
  const prepared = await prepareGoRuntime({
    includeToolchain: true,
    includeSampleModule: false,
    hostFileBackingMode: GO_BUILD_HOST_FILE_BACKING_MODE,
  });
  if (!prepared) {
    return;
  }

  const { runtime, goBytes } = prepared;
  try {
    await runtime.fs.writeFile("/tmp/goarch-importcfg", encoder.encode("# import config\n"), 0o644);
    const { exitCode, output, debugInfo } = await spawnWithOutput(
      runtime,
      goBytes,
      [
        "go",
        "tool",
        "compile",
        "-o",
        "/tmp/goarch.a",
        "-p",
        "internal/goarch",
        "-std",
        "-complete",
        "-nolocalimports",
        "-importcfg",
        "/tmp/goarch-importcfg",
        "-pack",
        "/usr/local/go/src/internal/goarch/goarch.go",
        "/usr/local/go/src/internal/goarch/goarch_riscv64.go",
        "/usr/local/go/src/internal/goarch/zgoarch_riscv64.go",
      ],
      goEnv(1),
      120_000,
    ).catch(async (error) => {
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error(`${detail}\n${await runtimeDebugSummary(runtime)}`);
    });
    expect(
      exitCode,
      `go tool compile exited with ${exitCode}\n${output}\n${debugInfo ?? ""}\n${await runtimeDebugSummary(runtime)}`,
    ).toBe(0);
  } finally {
    runtime.destroy();
  }
}, 180_000);

test("Runtime compiles internal abi with prepared goarch and symabis", async () => {
  if (!GO_TOOLCHAIN_DIR) {
    return;
  }
  const prepared = await prepareGoRuntime({
    includeToolchain: true,
    includeSampleModule: false,
    hostFileBackingMode: GO_BUILD_HOST_FILE_BACKING_MODE,
  });
  if (!prepared) {
    return;
  }

  const { runtime } = prepared;
  const runTool = async (args: string[], timeoutMs: number): Promise<void> => {
    const tool = args[0];
    if (!tool) {
      throw new Error("missing Go tool name");
    }
    const toolPath = `/usr/local/go/pkg/tool/linux_riscv64/${tool}`;
    const toolBytes = new Uint8Array(readFileSync(join(GO_TOOLCHAIN_DIR, "pkg", "tool", "linux_riscv64", tool)));
    const { exitCode, output, debugInfo } = await spawnWithOutput(
      runtime,
      toolBytes,
      [toolPath, ...args.slice(1)],
      goEnv(1),
      timeoutMs,
    ).catch(async (error) => {
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error(`${detail}\n${await runtimeDebugSummary(runtime)}`);
    });
    const failureInspection = exitCode === 0 || tool !== "compile"
      ? ""
      : (await Promise.all([
          ["bounds.go", 112, 121, 0],
          ["escape.go", 64, 76, 0],
          ["iface.go", 40, 52, 0],
          ["symtab.go", 108, 122, 4096],
          ["type.go", 735, 755, 20_448],
          ["switch.go", 1, 16, 0],
        ].map(async ([name, startLine, endLine, byteOffset]) => {
          const path = `/usr/local/go/src/internal/abi/${name}`;
          return [
            ...await inspectGuestTextLines(runtime, path, Number(startLine), Number(endLine)),
            ...await inspectGuestBytes(runtime, path, Number(byteOffset), 160),
            `${path}: page-cache=${JSON.stringify(await runtime.debugPageCachePath(path))}`,
          ].join("\n");
        }))).join("\n");
    expect(
      exitCode,
      `go tool ${args[0]} exited with ${exitCode}\n${output}\n${failureInspection}`
        + `\n${debugInfo ?? ""}\n${await runtimeDebugSummary(runtime)}`,
    ).toBe(0);
  };

  try {
    await runtime.fs.writeFile("/tmp/goarch-importcfg", encoder.encode("# import config\n"), 0o644);
    await runTool([
      "compile",
      "-o",
      "/tmp/goarch.a",
      "-p",
      "internal/goarch",
      "-std",
      "-complete",
      "-nolocalimports",
      "-importcfg",
      "/tmp/goarch-importcfg",
      "-pack",
      "/usr/local/go/src/internal/goarch/goarch.go",
      "/usr/local/go/src/internal/goarch/goarch_riscv64.go",
      "/usr/local/go/src/internal/goarch/zgoarch_riscv64.go",
    ], 120_000);
    await runtime.fs.writeFile(
      "/tmp/abi-importcfg",
      encoder.encode("# import config\npackagefile internal/goarch=/tmp/goarch.a\n"),
      0o644,
    );
    await runTool([
      "asm",
      "-p",
      "internal/abi",
      "-I",
      "/tmp",
      "-I",
      "/usr/local/go/pkg/include",
      "-D",
      "GOOS_linux",
      "-D",
      "GOARCH_riscv64",
      "-D",
      "GORISCV64_rva20u64",
      "-gensymabis",
      "-o",
      "/tmp/abi.symabis",
      "/usr/local/go/src/internal/abi/abi_test.s",
      "/usr/local/go/src/internal/abi/stub.s",
    ], 120_000);
    await runTool([
      "compile",
      "-o",
      "/tmp/abi.a",
      "-p",
      "internal/abi",
      "-std",
      "-symabis",
      "/tmp/abi.symabis",
      "-nolocalimports",
      "-importcfg",
      "/tmp/abi-importcfg",
      "-pack",
      "-asmhdr",
      "/tmp/go_asm.h",
      ...INTERNAL_ABI_SOURCE_PATHS.map((path) => `/usr/local/go/src/internal/abi/${path}`),
    ], 180_000);
  } finally {
    runtime.destroy();
  }
}, 480_000);

test("Runtime Go tool asm inherits cwd for relative source paths", async () => {
  const prepared = await prepareGoRuntime({
    includeToolchain: true,
    includeSampleModule: false,
    hostFileBackingMode: GO_BUILD_HOST_FILE_BACKING_MODE,
  });
  if (!prepared) {
    return;
  }

  const { runtime, goBytes } = prepared;
  try {
    const { exitCode, output, debugInfo } = await spawnWithOutput(
      runtime,
      goBytes,
      [
        "go",
        "tool",
        "asm",
        "-p",
        "internal/abi",
        "-I",
        "/tmp",
        "-I",
        "/usr/local/go/pkg/include",
        "-D",
        "GOOS_linux",
        "-D",
        "GOARCH_riscv64",
        "-D",
        "GORISCV64_rva20u64",
        "-gensymabis",
        "-o",
        "/tmp/relative.symabis",
        "./abi_test.s",
        "./stub.s",
      ],
      goEnv(1),
      180_000,
      { cwd: "/usr/local/go/src/internal/abi" },
    );
    expect(
      exitCode,
      `go tool asm exited with ${exitCode}\n${output}\n${debugInfo ?? ""}\n${await runtimeDebugSummary(runtime)}`,
    ).toBe(0);
  } finally {
    runtime.destroy();
  }
}, 240_000);

test("Runtime go build compiles internal goarch after compiler version probe", async () => {
  const prepared = await prepareGoRuntime({
    includeToolchain: true,
    includeSampleModule: false,
    hostFileBackingMode: GO_BUILD_HOST_FILE_BACKING_MODE,
  });
  if (!prepared) {
    return;
  }

  const { runtime, goBytes } = prepared;
  try {
    await precreateGoCacheShards(runtime);
    const { exitCode, output, debugInfo } = await spawnWithOutput(
      runtime,
      goBytes,
      ["go", "build", "-a", "-x", "internal/goarch"],
      goEnv(1),
      300_000,
    ).catch(async (error) => {
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error(`${detail}\n${await runtimeDebugSummary(runtime)}`);
    });
    expect(
      exitCode,
      `go build internal/goarch exited with ${exitCode}\n${output}\n${debugInfo ?? ""}\n${await runtimeDebugSummary(runtime)}`,
    ).toBe(0);
  } finally {
    runtime.destroy();
  }
}, 360_000);

test("Runtime go build compiles internal abi after compiler version probe", async () => {
  const prepared = await prepareGoRuntime({
    includeToolchain: true,
    includeSampleModule: false,
    hostFileBackingMode: GO_BUILD_HOST_FILE_BACKING_MODE,
  });
  if (!prepared) {
    return;
  }

  const { runtime, goBytes } = prepared;
  try {
    await precreateGoCacheShards(runtime);
    const { exitCode, output, debugInfo } = await spawnWithOutput(
      runtime,
      goBytes,
      ["go", "build", "-a", "-x", ...(GO_BUILD_INSPECT_WORK ? ["-work"] : []), "internal/abi"],
      goEnv(1),
      300_000,
    ).catch(async (error) => {
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error(`${detail}\n${await runtimeDebugSummary(runtime)}`);
    });
    const sourcePath = "/usr/local/go/src/internal/goarch/goarch_riscv64.go";
    const abiSymtabPath = "/usr/local/go/src/internal/abi/symtab.go";
    const abiTypePath = "/usr/local/go/src/internal/abi/type.go";
    const abiAsmPath = "/usr/local/go/src/internal/abi/abi_test.s";
    const textflagPath = "/usr/local/go/pkg/include/textflag.h";
    const workDir = /^WORK=(\S+)$/m.exec(output)?.[1];
    const goarchArchivePath = workDir ? `${workDir}/b002/_pkg_.a` : null;
    const failureInspection = exitCode === 0
      ? ""
      : [
        ...(goarchArchivePath
          ? [
            ...await inspectGuestBytes(runtime, goarchArchivePath, 0, 512),
            `goarch-archive-page-cache=${JSON.stringify(await runtime.debugPageCachePath(goarchArchivePath))}`,
          ]
          : []),
        ...await inspectGuestTextLines(runtime, sourcePath, 1, 24),
        ...await inspectGuestBytes(runtime, sourcePath, 0, 512),
        `goarch-riscv64-page-cache=${JSON.stringify(await runtime.debugPageCachePath(sourcePath))}`,
        ...await inspectGuestTextLines(runtime, abiSymtabPath, 136, 150),
        ...await inspectGuestBytes(runtime, abiSymtabPath, 5056, 128),
        `abi-symtab-page-cache=${JSON.stringify(await runtime.debugPageCachePath(abiSymtabPath))}`,
        ...await inspectGuestTextLines(runtime, abiTypePath, 739, 755),
        ...await inspectGuestBytes(runtime, abiTypePath, 20464, 160),
        `abi-type-page-cache=${JSON.stringify(await runtime.debugPageCachePath(abiTypePath))}`,
        ...await inspectGuestBytes(runtime, abiAsmPath, 0, 1024),
        `abi-asm-page-cache=${JSON.stringify(await runtime.debugPageCachePath(abiAsmPath))}`,
        ...await inspectGuestBytes(runtime, textflagPath, 0, 1600),
        `textflag-page-cache=${JSON.stringify(await runtime.debugPageCachePath(textflagPath))}`,
      ].join("\n");
    expect(
      exitCode,
      `go build internal/abi exited with ${exitCode}\n${output}\n${failureInspection}\n${debugInfo ?? ""}\n${await runtimeDebugSummary(runtime)}`,
    ).toBe(0);
  } finally {
    runtime.destroy();
  }
}, 360_000);

test("Runtime reads Go source across host-backed page boundary", async () => {
  if (!BUSYBOX_BINARY_PATH || !existsSync(BUSYBOX_BINARY_PATH)) {
    return logSkip(`missing BusyBox binary: ${BUSYBOX_BINARY_PATH || "TIDEMARK_TEST_BUSYBOX_BINARY"}`);
  }
  const prepared = await prepareGoRuntime({
    includeToolchain: true,
    includeSampleModule: false,
    hostFileBackingMode: GO_BUILD_HOST_FILE_BACKING_MODE,
  });
  if (!prepared) {
    return;
  }

  const { runtime } = prepared;
  try {
    const sourceCases = [
      ["/usr/local/go/src/runtime/trace.go", "package runtime"],
      ["/usr/local/go/src/runtime/mgc.go", "The background sweeper goroutine simply sweeps spans one-by-one."],
      ["/usr/local/go/src/unsafe/unsafe.go", "package unsafe"],
    ] as const;
    for (const [sourcePath, expectedText] of sourceCases) {
      const { exitCode, output, debugInfo } = await spawnWithOutput(
        runtime,
        new Uint8Array(readFileSync(BUSYBOX_BINARY_PATH)),
        ["/bin/busybox", "dd", `if=${sourcePath}`, "bs=4096", "count=4"],
        ["PATH=/bin"],
        30_000,
      ).catch(async (error) => {
        const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
        throw new Error(`${detail}\n${await runtimeDebugSummary(runtime)}`);
      });
      expect(
        exitCode,
        `dd exited with ${exitCode}: ${sourcePath}\n${debugInfo ?? ""}\n${await runtimeDebugSummary(runtime)}`,
      ).toBe(0);
      expect(
        output,
        `${sourcePath}\n${debugInfo ?? ""}\n${await runtimeDebugSummary(runtime)}`,
      ).toContain(expectedText);
    }
    const externSource = await spawnWithOutput(
      runtime,
      new Uint8Array(readFileSync(BUSYBOX_BINARY_PATH)),
      [
        "/bin/busybox",
        "dd",
        "if=/usr/local/go/src/runtime/extern.go",
        "bs=4096",
        "count=5",
      ],
      ["PATH=/bin"],
      30_000,
    ).catch(async (error) => {
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error(`${detail}\n${await runtimeDebugSummary(runtime)}`);
    });
    expect(
      externSource.exitCode,
      `dd exited with ${externSource.exitCode}: runtime/extern.go`
        + `\n${externSource.debugInfo ?? ""}\n${await runtimeDebugSummary(runtime)}`,
    ).toBe(0);
    expect(externSource.output).toContain("const GOARCH string = goarch.GOARCH");
    const unsafeDir = await spawnWithOutput(
      runtime,
      new Uint8Array(readFileSync(BUSYBOX_BINARY_PATH)),
      ["/bin/busybox", "ls", "-l", "/usr/local/go/src/unsafe"],
      ["PATH=/bin"],
      30_000,
    ).catch(async (error) => {
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error(`${detail}\n${await runtimeDebugSummary(runtime)}`);
    });
    expect(
      unsafeDir.exitCode,
      `ls unsafe exited with ${unsafeDir.exitCode}\n${unsafeDir.debugInfo ?? ""}`
        + `\n${await runtimeDebugSummary(runtime)}`,
    ).toBe(0);
    expect(unsafeDir.output).toMatch(/^-rw-r--r--\s+1\s+\S+\s+\S+\s+12391\s+.*unsafe\.go$/m);
    const unsafeAfterCompilerProbe = await spawnWithOutput(
      runtime,
      new Uint8Array(readFileSync(BUSYBOX_BINARY_PATH)),
      [
        "/bin/busybox",
        "sh",
        "-c",
        "/usr/local/go/pkg/tool/linux_riscv64/compile -V=full"
          + " && /bin/busybox ls -l /usr/local/go/src/unsafe"
          + " && /bin/busybox dd if=/usr/local/go/src/unsafe/unsafe.go bs=4096 count=4",
      ],
      ["PATH=/bin"],
      60_000,
    ).catch(async (error) => {
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error(`${detail}\n${await runtimeDebugSummary(runtime)}`);
    });
    expect(
      unsafeAfterCompilerProbe.exitCode,
      `unsafe compiler probe exited with ${unsafeAfterCompilerProbe.exitCode}`
        + `\n${unsafeAfterCompilerProbe.debugInfo ?? ""}\n${await runtimeDebugSummary(runtime)}`,
    ).toBe(0);
    expect(unsafeAfterCompilerProbe.output).toContain("compile version");
    expect(unsafeAfterCompilerProbe.output).toContain("package unsafe");
  } finally {
    runtime.destroy();
  }
}, 90_000);

test("Runtime preserves source identity across concurrent distinct regular files", async () => {
  if (!GO_TOOLCHAIN_DIR) {
    return;
  }
  const importConfig = encoder.encode("# import config\n");
  const sourceCases = [
    { guestPath: "/tmp/importcfg", bytes: importConfig },
    ...CONCURRENT_FD_SOURCE_PATHS.map((relativePath) => ({
      guestPath: `/usr/local/go/src/${relativePath}`,
      bytes: new Uint8Array(readFileSync(join(GO_TOOLCHAIN_DIR, "src", relativePath))),
    })),
  ];
  expect(sourceCases).toHaveLength(24);
  const probeBytes = buildConcurrentFdReuseProbe(sourceCases);
  if (!probeBytes) {
    return;
  }
  const prepared = await prepareGoRuntime({
    includeToolchain: true,
    includeSampleModule: false,
    hostFileBackingMode: GO_BUILD_HOST_FILE_BACKING_MODE,
  });
  if (!prepared) {
    return;
  }

  const { runtime } = prepared;
  try {
    await runtime.fs.writeFile("/tmp/importcfg", importConfig, 0o644);
    const { exitCode, output, debugInfo } = await spawnWithOutput(
      runtime,
      probeBytes,
      ["/work/fd-reuse"],
      ["PATH=/bin"],
      60_000,
    ).catch(async (error) => {
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error(`${detail}\n${await runtimeDebugSummary(runtime)}`);
    });
    expect(
      exitCode,
      `fd reuse probe exited with ${exitCode}\n${output}\n${debugInfo ?? ""}\n${await runtimeDebugSummary(runtime)}`,
    ).toBe(0);
  } finally {
    runtime.destroy();
  }
}, 120_000);

test("Runtime preserves regular file identity across cross-thread fd handoff", async () => {
  if (!GO_TOOLCHAIN_DIR) {
    return;
  }
  const sourceCases = CONCURRENT_FD_SOURCE_PATHS.map((relativePath) => ({
    guestPath: `/usr/local/go/src/${relativePath}`,
    bytes: new Uint8Array(readFileSync(join(GO_TOOLCHAIN_DIR, "src", relativePath))),
  }));
  const probeBytes = buildCrossThreadFdHandoffProbe(sourceCases);
  if (!probeBytes) {
    return;
  }
  const prepared = await prepareGoRuntime({
    includeToolchain: true,
    includeSampleModule: false,
    hostFileBackingMode: GO_BUILD_HOST_FILE_BACKING_MODE,
  });
  if (!prepared) {
    return;
  }

  const { runtime } = prepared;
  try {
    const { exitCode, output, debugInfo } = await spawnWithOutput(
      runtime,
      probeBytes,
      ["/work/fd-handoff"],
      ["PATH=/bin"],
      60_000,
    ).catch(async (error) => {
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error(`${detail}\n${await runtimeDebugSummary(runtime)}`);
    });
    expect(
      exitCode,
      `fd handoff probe exited with ${exitCode}\n${output}\n${debugInfo ?? ""}\n${await runtimeDebugSummary(runtime)}`,
    ).toBe(0);
  } finally {
    runtime.destroy();
  }
}, 120_000);

test("Runtime preserves distinct regular files across concurrent child exits", async () => {
  if (!BUSYBOX_BINARY_PATH || !existsSync(BUSYBOX_BINARY_PATH)) {
    return logSkip(`missing BusyBox binary: ${BUSYBOX_BINARY_PATH || "TIDEMARK_TEST_BUSYBOX_BINARY"}`);
  }
  const prepared = await prepareGoRuntime({
    includeToolchain: false,
    includeSampleModule: false,
    hostFileBackingMode: GO_BUILD_HOST_FILE_BACKING_MODE,
  });
  if (!prepared) {
    return;
  }

  const { runtime } = prepared;
  const script = [
    "round=0",
    "while [ \"$round\" -lt 8 ]; do",
    "  child=0",
    "  while [ \"$child\" -lt 4 ]; do",
    "    (",
    "      line=0",
    "      while [ \"$line\" -lt 32 ]; do",
    "        printf '%s:%s:%s\\n' \"$round\" \"$child\" \"$line\"",
    "        line=$((line + 1))",
    "      done",
    "    ) > \"/tmp/ofd-$round-$child\" &",
    "    child=$((child + 1))",
    "  done",
    "  wait || exit 70",
    "  child=0",
    "  while [ \"$child\" -lt 4 ]; do",
    "    lines=$(/bin/busybox wc -l < \"/tmp/ofd-$round-$child\") || exit 71",
    "    if [ \"$lines\" -ne 32 ]; then",
    "      bytes=$(/bin/busybox wc -c < \"/tmp/ofd-$round-$child\")",
    "      echo \"line-count-mismatch path=/tmp/ofd-$round-$child lines=$lines bytes=$bytes\" >&2",
    "      exit 72",
    "    fi",
    "    child=$((child + 1))",
    "  done",
    "  round=$((round + 1))",
    "done",
    "echo concurrent-process-files-ok",
  ].join("\n");
  try {
    const { exitCode, output, debugInfo } = await spawnWithOutput(
      runtime,
      new Uint8Array(readFileSync(BUSYBOX_BINARY_PATH)),
      ["/bin/busybox", "sh", "-c", script],
      ["PATH=/bin"],
      120_000,
    ).catch(async (error) => {
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error(`${detail}\n${await runtimeDebugSummary(runtime)}`);
    });
    expect(
      exitCode,
      `concurrent child file probe exited with ${exitCode}\n${output}\n${debugInfo ?? ""}\n${await runtimeDebugSummary(runtime)}`,
    ).toBe(0);
    expect(output).toContain("concurrent-process-files-ok");
  } finally {
    runtime.destroy();
  }
}, 180_000);

test("Runtime reads Go source in one large host-backed request", async () => {
  if (!BUSYBOX_BINARY_PATH || !existsSync(BUSYBOX_BINARY_PATH)) {
    return logSkip(`missing BusyBox binary: ${BUSYBOX_BINARY_PATH || "TIDEMARK_TEST_BUSYBOX_BINARY"}`);
  }
  const prepared = await prepareGoRuntime({
    includeToolchain: true,
    includeSampleModule: false,
    hostFileBackingMode: GO_BUILD_HOST_FILE_BACKING_MODE,
  });
  if (!prepared) {
    return;
  }

  const { runtime } = prepared;
  try {
    const { exitCode, output, debugInfo } = await spawnWithOutput(
      runtime,
      new Uint8Array(readFileSync(BUSYBOX_BINARY_PATH)),
      ["/bin/busybox", "dd", "if=/usr/local/go/src/runtime/trace.go", "bs=32768", "count=1"],
      ["PATH=/bin"],
      30_000,
    ).catch(async (error) => {
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error(`${detail}\n${await runtimeDebugSummary(runtime)}`);
    });
    expect(
      exitCode,
      `dd exited with ${exitCode}\n${debugInfo ?? ""}\n${await runtimeDebugSummary(runtime)}`,
    ).toBe(0);
    expect(
      output,
      `${debugInfo ?? ""}\n${await runtimeDebugSummary(runtime)}`,
    ).toContain("package runtime");
  } finally {
    runtime.destroy();
  }
}, 90_000);

test("Runtime mmaps Go source across host-backed page boundary", async () => {
  const probeBytes = buildGoSourceMmapProbe();
  if (!probeBytes) {
    return;
  }
  const prepared = await prepareGoRuntime({
    includeToolchain: true,
    includeSampleModule: false,
    hostFileBackingMode: GO_BUILD_HOST_FILE_BACKING_MODE,
  });
  if (!prepared) {
    return;
  }

  const { runtime } = prepared;
  try {
    const { exitCode, output, debugInfo } = await spawnWithOutput(
      runtime,
      probeBytes,
      ["/work/go-source-mmap", "/usr/local/go/src/runtime/trace.go"],
      ["PATH=/bin", "LC_ALL=C", "LANG=C"],
      30_000,
    ).catch(async (error) => {
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error(`${detail}\n${await runtimeDebugSummary(runtime)}`);
    });
    expect(
      exitCode,
      `go source mmap probe exited with ${exitCode}\n${output}\n${debugInfo ?? ""}\n${await runtimeDebugSummary(runtime)}`,
    ).toBe(0);
    expect(
      output,
      `${debugInfo ?? ""}\n${await runtimeDebugSummary(runtime)}`,
    ).toMatch(/package_offset=\d+/);
  } finally {
    runtime.destroy();
  }
}, 90_000);
