import { execFileSync } from "node:child_process";
import { existsSync, mkdtempSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { expect, test } from "vite-plus/test";
import {
  Runtime,
  logSkip,
  readKernelWasm,
  runtimeDebugSummary,
  spawnWithOutput,
} from "../../../support/runtime-workload.js";

const hostRiscvGcc = process.env.TIDEMARK_TEST_HOST_RISCV_GCC ?? "riscv64-linux-gnu-gcc";
const libraryRoots = (process.env.TIDEMARK_TEST_APT_METHOD_LIBRARY_ROOTS ?? process.env.TIDEMARK_TEST_APT_METHOD_ROOT ?? "")
  .split(":")
  .filter(Boolean);
const directSonames = [
  "libapt-pkg.so.7.0",
  "libssl.so.3",
  "libsystemd.so.0",
  "libcrypto.so.3",
  "libstdc++.so.6",
  "libgcc_s.so.1",
  "libc.so.6",
  "libm.so.6",
  "libz.so.1",
  "libbz2.so.1.0",
  "liblzma.so.5",
  "liblz4.so.1",
  "libzstd.so.1",
  "libudev.so.1",
  "libxxhash.so.0",
  "libcap.so.2",
] as const;

function candidateLibraryPaths(root: string, soname: string): string[] {
  return [
    resolve(root, "usr/lib/riscv64-linux-gnu", soname),
    resolve(root, "lib/riscv64-linux-gnu", soname),
    resolve(root, "usr/lib", soname),
    resolve(root, "lib", soname),
    resolve(root, soname),
  ];
}

function findLibrary(soname: string): string | null {
  for (const root of libraryRoots) {
    for (const candidate of candidateLibraryPaths(root, soname)) {
      try {
        if (existsSync(candidate) && statSync(candidate).isFile()) {
          return candidate;
        }
      } catch {
        // Try the next candidate.
      }
    }
  }
  return null;
}

function buildForkLargeFileProbe(): Uint8Array | null {
  const tmp = mkdtempSync(join(tmpdir(), "tidemark-fork-large-file-"));
  const sourcePath = join(tmp, "fork_large_file.c");
  const outputPath = join(tmp, "fork-large-file");
  try {
    writeFileSync(sourcePath, [
      "#include <errno.h>",
      "#include <fcntl.h>",
      "#include <string.h>",
      "#include <stdio.h>",
      "#include <sys/wait.h>",
      "#include <unistd.h>",
      "",
      "static int read_until(int fd, const char *needle) {",
      "  char buf[256];",
      "  size_t off = 0;",
      "  while (off + 1 < sizeof(buf)) {",
      "    ssize_t n = read(fd, buf + off, sizeof(buf) - 1 - off);",
      "    if (n < 0) { if (errno == EINTR || errno == EAGAIN) { usleep(1000); continue; } perror(\"read-child\"); return 20; }",
      "    if (n == 0) break;",
      "    off += (size_t)n;",
      "    buf[off] = '\\0';",
      "    if (strstr(buf, needle) != NULL) {",
      "      fputs(buf, stdout);",
      "      fflush(stdout);",
      "      return 0;",
      "    }",
      "  }",
      "  fputs(buf, stdout);",
      "  fflush(stdout);",
      "  return 21;",
      "}",
      "",
      "static int read_magic(const char *label) {",
      "  int fd = open(\"/usr/lib/riscv64-linux-gnu/libapt-pkg.so.7.0\", O_RDONLY);",
      "  if (fd < 0) { perror(label); return 10; }",
      "  unsigned char magic[4] = {0, 0, 0, 0};",
      "  ssize_t n = read(fd, magic, sizeof(magic));",
      "  close(fd);",
      "  if (n != 4 || magic[0] != 0x7f || magic[1] != 'E' || magic[2] != 'L' || magic[3] != 'F') {",
      "    fprintf(stdout, \"%s:not-elf:%02x%02x%02x%02x\\n\", label, magic[0], magic[1], magic[2], magic[3]);",
      "    fflush(stdout);",
      "    return 11;",
      "  }",
      "  fprintf(stdout, \"%s:elf\\n\", label);",
      "  fflush(stdout);",
      "  return 0;",
      "}",
      "",
      "int main(void) {",
      "  int parent = read_magic(\"parent-large-file\");",
      "  if (parent != 0) return parent;",
      "  pid_t child = fork();",
      "  if (child < 0) { perror(\"fork\"); return 12; }",
      "  if (child == 0) {",
      "    int child_result = read_magic(\"child-large-file\");",
      "    _exit(child_result);",
      "  }",
      "  int status = 0;",
      "  if (waitpid(child, &status, 0) < 0) { perror(\"waitpid\"); return 13; }",
      "  if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {",
      "    fprintf(stdout, \"child-status:%d\\n\", status);",
      "    fflush(stdout);",
      "    return 14;",
      "  }",
      "  return 0;",
      "}",
      "",
    ].join("\n"));
    execFileSync(hostRiscvGcc, ["-static", "-O2", "-o", outputPath, sourcePath], {
      stdio: "pipe",
    });
    return new Uint8Array(readFileSync(outputPath));
  } catch (error) {
    const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
    return logSkip(`failed to build fork large file probe with ${hostRiscvGcc}: ${detail}`);
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
}

function buildDynamicForkLargeFileProbe(): Uint8Array | null {
  const tmp = mkdtempSync(join(tmpdir(), "tidemark-dynamic-fork-large-file-"));
  const sourcePath = join(tmp, "dynamic_fork_large_file.c");
  const outputPath = join(tmp, "dynamic-fork-large-file");
  try {
    writeFileSync(sourcePath, [
      "#include <errno.h>",
      "#include <fcntl.h>",
      "#include <string.h>",
      "#include <stdio.h>",
      "#include <sys/wait.h>",
      "#include <unistd.h>",
      "",
      "static int read_until(int fd, const char *needle) {",
      "  char buf[256];",
      "  size_t off = 0;",
      "  buf[0] = '\\0';",
      "  while (off + 1 < sizeof(buf)) {",
      "    ssize_t n = read(fd, buf + off, sizeof(buf) - 1 - off);",
      "    if (n < 0) { if (errno == EINTR || errno == EAGAIN) { usleep(1000); continue; } perror(\"read-child\"); return 20; }",
      "    if (n == 0) break;",
      "    off += (size_t)n;",
      "    buf[off] = '\\0';",
      "    if (strstr(buf, needle) != NULL) {",
      "      fputs(buf, stdout);",
      "      fflush(stdout);",
      "      return 0;",
      "    }",
      "  }",
      "  fputs(buf, stdout);",
      "  fflush(stdout);",
      "  return 21;",
      "}",
      "",
      "static int read_magic(const char *label) {",
      "  int fd = open(\"/usr/lib/riscv64-linux-gnu/libapt-pkg.so.7.0\", O_RDONLY);",
      "  if (fd < 0) { perror(label); return 10; }",
      "  unsigned char magic[4] = {0, 0, 0, 0};",
      "  ssize_t n = read(fd, magic, sizeof(magic));",
      "  close(fd);",
      "  if (n != 4 || magic[0] != 0x7f || magic[1] != 'E' || magic[2] != 'L' || magic[3] != 'F') {",
      "    fprintf(stdout, \"%s:not-elf:%02x%02x%02x%02x\\n\", label, magic[0], magic[1], magic[2], magic[3]);",
      "    fflush(stdout);",
      "    return 11;",
      "  }",
      "  fprintf(stdout, \"%s:elf\\n\", label);",
      "  fflush(stdout);",
      "  return 0;",
      "}",
      "",
      "int main(void) {",
      "  int in_pipe[2];",
      "  int out_pipe[2];",
      "  if (pipe(in_pipe) != 0 || pipe(out_pipe) != 0) { perror(\"pipe\"); return 9; }",
      "  fcntl(out_pipe[0], F_SETFL, fcntl(out_pipe[0], F_GETFL, 0) | O_NONBLOCK);",
      "  int parent = read_magic(\"parent-dynamic-large-file\");",
      "  if (parent != 0) return parent;",
      "  pid_t child = fork();",
      "  if (child < 0) { perror(\"fork\"); return 12; }",
      "  if (child == 0) {",
      "    if (dup2(in_pipe[0], 0) < 0) { perror(\"dup2-stdin\"); _exit(125); }",
      "    if (dup2(out_pipe[1], 1) < 0) { perror(\"dup2-stdout\"); _exit(126); }",
      "    close(in_pipe[0]); close(in_pipe[1]);",
      "    close(out_pipe[0]); close(out_pipe[1]);",
      "    int child_result = read_magic(\"child-dynamic-large-file\");",
      "    _exit(child_result);",
      "  }",
      "  close(in_pipe[0]);",
      "  close(out_pipe[1]);",
      "  close(in_pipe[1]);",
      "  int child_output = read_until(out_pipe[0], \"child-dynamic-large-file:elf\");",
      "  close(out_pipe[0]);",
      "  int status = 0;",
      "  if (waitpid(child, &status, 0) < 0) { perror(\"waitpid\"); return 13; }",
      "  if (child_output != 0) return child_output;",
      "  if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {",
      "    fprintf(stdout, \"child-status:%d\\n\", status);",
      "    fflush(stdout);",
      "    return 14;",
      "  }",
      "  return 0;",
      "}",
      "",
    ].join("\n"));
    execFileSync(hostRiscvGcc, [
      "-O2",
      "-Wl,--dynamic-linker=/lib/ld-linux-riscv64-lp64d.so.1",
      "-o",
      outputPath,
      sourcePath,
    ], { stdio: "pipe" });
    return new Uint8Array(readFileSync(outputPath));
  } catch (error) {
    const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
    return logSkip(`failed to build dynamic fork large file probe with ${hostRiscvGcc}: ${detail}`);
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
}

function buildDynamicForkLargeWriteLoaderProbe(): Uint8Array | null {
  const tmp = mkdtempSync(join(tmpdir(), "tidemark-dynamic-fork-large-write-loader-"));
  const sourcePath = join(tmp, "dynamic_fork_large_write_loader.c");
  const outputPath = join(tmp, "dynamic-fork-large-write-loader");
  try {
    writeFileSync(sourcePath, [
      "#include <errno.h>",
      "#include <fcntl.h>",
      "#include <stdio.h>",
      "#include <stdlib.h>",
      "#include <string.h>",
      "#include <sys/wait.h>",
      "#include <unistd.h>",
      "",
      "#define WRITE_CHUNK 245760",
      "",
      "static int read_loader_magic(const char *label) {",
      "  int fd = open(\"/lib/ld-linux-riscv64-lp64d.so.1\", O_RDONLY);",
      "  if (fd < 0) { perror(label); return 10; }",
      "  unsigned char magic[4] = {0, 0, 0, 0};",
      "  ssize_t n = read(fd, magic, sizeof(magic));",
      "  close(fd);",
      "  if (n != 4 || magic[0] != 0x7f || magic[1] != 'E' || magic[2] != 'L' || magic[3] != 'F') {",
      "    fprintf(stdout, \"%s:not-elf:%02x%02x%02x%02x\\n\", label, magic[0], magic[1], magic[2], magic[3]);",
      "    fflush(stdout);",
      "    return 11;",
      "  }",
      "  fprintf(stdout, \"%s:elf\\n\", label);",
      "  fflush(stdout);",
      "  return 0;",
      "}",
      "",
      "static int write_all(int fd, const unsigned char *data, size_t len) {",
      "  size_t off = 0;",
      "  while (off < len) {",
      "    ssize_t n = write(fd, data + off, len - off);",
      "    if (n < 0) {",
      "      if (errno == EINTR) continue;",
      "      return -1;",
      "    }",
      "    if (n == 0) return -1;",
      "    off += (size_t)n;",
      "  }",
      "  return 0;",
      "}",
      "",
      "static int write_large_file(unsigned long long total) {",
      "  int fd = open(\"/var/lib/apt/lists/partial/repro_Packages.xz\", O_CREAT | O_TRUNC | O_WRONLY, 0644);",
      "  if (fd < 0) { perror(\"open-large-file\"); return 20; }",
      "  static unsigned char buf[WRITE_CHUNK];",
      "  unsigned long long written = 0;",
      "  while (written < total) {",
      "    size_t chunk = (size_t)((total - written) < WRITE_CHUNK ? (total - written) : WRITE_CHUNK);",
      "    for (size_t i = 0; i < chunk; i++) {",
      "      buf[i] = (unsigned char)(((written + (unsigned long long)i) * 13ull + 19ull) & 0xffu);",
      "    }",
      "    if (write_all(fd, buf, chunk) != 0) { perror(\"write-large-file\"); close(fd); return 21; }",
      "    written += (unsigned long long)chunk;",
      "  }",
      "  if (close(fd) != 0) { perror(\"close-large-file\"); return 22; }",
      "  fprintf(stdout, \"child-large-write:bytes=%llu\\n\", written);",
      "  fflush(stdout);",
      "  return written == total ? 0 : 23;",
      "}",
      "",
      "int main(int argc, char **argv) {",
      "  unsigned long long total = argc > 1 ? strtoull(argv[1], NULL, 10) : 9379800ull;",
      "  int before = read_loader_magic(\"parent-before-large-write\");",
      "  if (before != 0) return before;",
      "  pid_t child = fork();",
      "  if (child < 0) { perror(\"fork\"); return 12; }",
      "  if (child == 0) {",
      "    int child_before = read_loader_magic(\"child-before-large-write\");",
      "    if (child_before != 0) _exit(child_before);",
      "    int written = write_large_file(total);",
      "    if (written != 0) _exit(written);",
      "    int child_after = read_loader_magic(\"child-after-large-write\");",
      "    _exit(child_after);",
      "  }",
      "  int status = 0;",
      "  if (waitpid(child, &status, 0) < 0) { perror(\"waitpid\"); return 13; }",
      "  if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {",
      "    fprintf(stdout, \"child-status:%d\\n\", status);",
      "    fflush(stdout);",
      "    return 14;",
      "  }",
      "  int after = read_loader_magic(\"parent-after-large-write\");",
      "  if (after != 0) return after;",
      "  return 0;",
      "}",
      "",
    ].join("\n"));
    execFileSync(hostRiscvGcc, [
      "-O2",
      "-Wl,--dynamic-linker=/lib/ld-linux-riscv64-lp64d.so.1",
      "-o",
      outputPath,
      sourcePath,
    ], { stdio: "pipe" });
    return new Uint8Array(readFileSync(outputPath));
  } catch (error) {
    const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
    return logSkip(`failed to build dynamic fork large write loader probe with ${hostRiscvGcc}: ${detail}`);
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
}

function buildDynamicExecFanoutLargeWriteLoaderProbe(): Uint8Array | null {
  const tmp = mkdtempSync(join(tmpdir(), "tidemark-dynamic-exec-fanout-large-write-loader-"));
  const sourcePath = join(tmp, "dynamic_exec_fanout_large_write_loader.c");
  const outputPath = join(tmp, "dynamic-exec-fanout-large-write-loader");
  try {
    writeFileSync(sourcePath, [
      "#include <errno.h>",
      "#include <fcntl.h>",
      "#include <stdio.h>",
      "#include <stdlib.h>",
      "#include <string.h>",
      "#include <sys/wait.h>",
      "#include <unistd.h>",
      "",
      "#define WRITE_CHUNK 245760",
      "",
      "extern char **environ;",
      "",
      "static int read_loader_magic(const char *label) {",
      "  int fd = open(\"/lib/ld-linux-riscv64-lp64d.so.1\", O_RDONLY);",
      "  if (fd < 0) { perror(label); return 10; }",
      "  unsigned char magic[4] = {0, 0, 0, 0};",
      "  ssize_t n = read(fd, magic, sizeof(magic));",
      "  close(fd);",
      "  if (n != 4 || magic[0] != 0x7f || magic[1] != 'E' || magic[2] != 'L' || magic[3] != 'F') {",
      "    fprintf(stdout, \"%s:not-elf:%02x%02x%02x%02x\\n\", label, magic[0], magic[1], magic[2], magic[3]);",
      "    fflush(stdout);",
      "    return 11;",
      "  }",
      "  fprintf(stdout, \"%s:elf\\n\", label);",
      "  fflush(stdout);",
      "  return 0;",
      "}",
      "",
      "static int write_all(int fd, const unsigned char *data, size_t len) {",
      "  size_t off = 0;",
      "  while (off < len) {",
      "    ssize_t n = write(fd, data + off, len - off);",
      "    if (n < 0) { if (errno == EINTR) continue; return -1; }",
      "    if (n == 0) return -1;",
      "    off += (size_t)n;",
      "  }",
      "  return 0;",
      "}",
      "",
      "static int write_large_file(unsigned long long total) {",
      "  int fd = open(\"/var/lib/apt/lists/partial/repro_Packages.xz\", O_CREAT | O_TRUNC | O_WRONLY, 0644);",
      "  if (fd < 0) { perror(\"open-large-file\"); return 20; }",
      "  static unsigned char buf[WRITE_CHUNK];",
      "  unsigned long long written = 0;",
      "  while (written < total) {",
      "    size_t chunk = (size_t)((total - written) < WRITE_CHUNK ? (total - written) : WRITE_CHUNK);",
      "    for (size_t i = 0; i < chunk; i++) {",
      "      buf[i] = (unsigned char)(((written + (unsigned long long)i) * 13ull + 19ull) & 0xffu);",
      "    }",
      "    if (write_all(fd, buf, chunk) != 0) { perror(\"write-large-file\"); close(fd); return 21; }",
      "    written += (unsigned long long)chunk;",
      "  }",
      "  if (close(fd) != 0) { perror(\"close-large-file\"); return 22; }",
      "  fprintf(stdout, \"writer-large-write:bytes=%llu\\n\", written);",
      "  fflush(stdout);",
      "  return written == total ? 0 : 23;",
      "}",
      "",
      "static int wait_child(pid_t child, const char *label) {",
      "  int status = 0;",
      "  if (waitpid(child, &status, 0) < 0) { perror(label); return 30; }",
      "  if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {",
      "    fprintf(stdout, \"%s-status:%d\\n\", label, status);",
      "    fflush(stdout);",
      "    return 31;",
      "  }",
      "  return 0;",
      "}",
      "",
      "static int fork_exec(const char *mode, const char *total) {",
      "  pid_t child = fork();",
      "  if (child < 0) { perror(\"fork-exec\"); return 40; }",
      "  if (child == 0) {",
      "    char *const args[] = { \"/work/apt-fanout-loader-probe\", (char *)mode, (char *)total, NULL };",
      "    execve(\"/work/apt-fanout-loader-probe\", args, environ);",
      "    perror(\"execve-fanout-probe\");",
      "    _exit(127);",
      "  }",
      "  return wait_child(child, mode);",
      "}",
      "",
      "int main(int argc, char **argv) {",
      "  const char *total_arg = argc > 2 ? argv[2] : \"9379800\";",
      "  unsigned long long total = strtoull(total_arg, NULL, 10);",
      "  if (argc > 1 && strcmp(argv[1], \"--writer\") == 0) {",
      "    int before = read_loader_magic(\"writer-before-large-write\");",
      "    if (before != 0) return before;",
      "    int written = write_large_file(total);",
      "    if (written != 0) return written;",
      "    return read_loader_magic(\"writer-after-large-write\");",
      "  }",
      "  if (argc > 1 && strcmp(argv[1], \"--loader-check\") == 0) {",
      "    return read_loader_magic(\"post-writer-exec-loader-check\");",
      "  }",
      "  int before = read_loader_magic(\"driver-before-writer\");",
      "  if (before != 0) return before;",
      "  int writer = fork_exec(\"--writer\", total_arg);",
      "  if (writer != 0) return writer;",
      "  int after_writer = read_loader_magic(\"driver-after-writer-exit\");",
      "  if (after_writer != 0) return after_writer;",
      "  int checker = fork_exec(\"--loader-check\", total_arg);",
      "  if (checker != 0) return checker;",
      "  return read_loader_magic(\"driver-after-loader-check\");",
      "}",
      "",
    ].join("\n"));
    execFileSync(hostRiscvGcc, [
      "-O2",
      "-Wl,--dynamic-linker=/lib/ld-linux-riscv64-lp64d.so.1",
      "-o",
      outputPath,
      sourcePath,
    ], { stdio: "pipe" });
    return new Uint8Array(readFileSync(outputPath));
  } catch (error) {
    const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
    return logSkip(`failed to build dynamic exec fanout large write loader probe with ${hostRiscvGcc}: ${detail}`);
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
}

test("plain fork preserves page-cache-backed large snapshot file reads", async () => {
  const wasmBytes = readKernelWasm();
  if (!wasmBytes) {
    return;
  }
  const probeBytes = buildForkLargeFileProbe();
  if (!probeBytes) {
    return;
  }

  const largeFile = new Uint8Array(640 * 1024);
  largeFile[0] = 0x7f;
  largeFile[1] = "E".charCodeAt(0);
  largeFile[2] = "L".charCodeAt(0);
  largeFile[3] = "F".charCodeAt(0);
  for (let i = 4; i < largeFile.length; i += 1) {
    largeFile[i] = i & 0xff;
  }

  const runtime = await Runtime.create({
    wasmBytes,
    guestMemoryPolicyLimitBytes: 128 * 1024 * 1024,
    maxProcessMemoryPages: 8192,
  });
  try {
    await runtime.applyFsSnapshot([
      {
        kind: "file",
        path: "/usr/lib/riscv64-linux-gnu/libapt-pkg.so.7.0",
        data: largeFile,
        mode: 0o644,
      },
    ]);
    await runtime.writeFile(
      "/var/lib/apt/lists/partial/deb.debian.org_debian_dists_trixie_InRelease",
      new TextEncoder().encode("runtime-owned-file\n"),
      0o644,
    );
    const result = await spawnWithOutput(
      runtime,
      probeBytes,
      ["/work/fork-large-file"],
      ["LC_ALL=C"],
      90_000,
    );
    expect(
      result.exitCode,
      `${result.output}\n${result.debugInfo ?? ""}\n${await runtimeDebugSummary(runtime)}`,
    ).toBe(0);
    expect(result.output).toContain("parent-large-file:elf");
    expect(result.output).toContain("child-large-file:elf");
  } finally {
    runtime.destroy();
  }
});

test("dynamic fork large regular-file write preserves loader bytes", async () => {
  const wasmBytes = readKernelWasm();
  if (!wasmBytes) {
    return;
  }
  const probeBytes = buildDynamicForkLargeWriteLoaderProbe();
  if (!probeBytes) {
    return;
  }
  const loader = findLibrary("ld-linux-riscv64-lp64d.so.1");
  const libc = findLibrary("libc.so.6");
  if (!loader || !libc) {
    return logSkip("missing dynamic loader/libc root: set TIDEMARK_TEST_APT_METHOD_LIBRARY_ROOTS");
  }

  const runtime = await Runtime.create({
    wasmBytes,
    guestMemoryPolicyLimitBytes: 256 * 1024 * 1024,
    maxProcessMemoryPages: 16384,
  });
  try {
    await runtime.applyFsSnapshot([
      { kind: "dir", path: "/var/lib/apt/lists/partial", mode: 0o755 },
      {
        kind: "file",
        path: "/lib/ld-linux-riscv64-lp64d.so.1",
        data: new Uint8Array(readFileSync(loader)),
        mode: 0o755,
      },
      {
        kind: "file",
        path: "/usr/lib/riscv64-linux-gnu/ld-linux-riscv64-lp64d.so.1",
        data: new Uint8Array(readFileSync(loader)),
        mode: 0o755,
      },
      {
        kind: "file",
        path: "/usr/lib/riscv64-linux-gnu/libc.so.6",
        data: new Uint8Array(readFileSync(libc)),
        mode: 0o755,
      },
    ]);
    const result = await spawnWithOutput(
      runtime,
      probeBytes,
      ["/work/dynamic-fork-large-write-loader", "9379800"],
      ["LC_ALL=C"],
      180_000,
    );
    expect(
      result.exitCode,
      `${result.output}\n${result.debugInfo ?? ""}\n${await runtimeDebugSummary(runtime)}`,
    ).toBe(0);
    expect(result.output).toContain("parent-before-large-write:elf");
    expect(result.output).toContain("child-before-large-write:elf");
    expect(result.output).toContain("child-after-large-write:elf");
    expect(result.output).toContain("parent-after-large-write:elf");
  } finally {
    runtime.destroy();
  }
});

test("dynamic exec fanout large regular-file write preserves loader bytes", async () => {
  const wasmBytes = readKernelWasm();
  if (!wasmBytes) {
    return;
  }
  const probeBytes = buildDynamicExecFanoutLargeWriteLoaderProbe();
  if (!probeBytes) {
    return;
  }
  const loader = findLibrary("ld-linux-riscv64-lp64d.so.1");
  const libc = findLibrary("libc.so.6");
  if (!loader || !libc) {
    return logSkip("missing dynamic loader/libc root: set TIDEMARK_TEST_APT_METHOD_LIBRARY_ROOTS");
  }

  const runtime = await Runtime.create({
    wasmBytes,
    guestMemoryPolicyLimitBytes: 256 * 1024 * 1024,
    maxProcessMemoryPages: 16384,
  });
  try {
    await runtime.applyFsSnapshot([
      { kind: "dir", path: "/var/lib/apt/lists/partial", mode: 0o755 },
      {
        kind: "file",
        path: "/work/apt-fanout-loader-probe",
        data: probeBytes,
        mode: 0o755,
      },
      {
        kind: "file",
        path: "/lib/ld-linux-riscv64-lp64d.so.1",
        data: new Uint8Array(readFileSync(loader)),
        mode: 0o755,
      },
      {
        kind: "file",
        path: "/usr/lib/riscv64-linux-gnu/ld-linux-riscv64-lp64d.so.1",
        data: new Uint8Array(readFileSync(loader)),
        mode: 0o755,
      },
      {
        kind: "file",
        path: "/usr/lib/riscv64-linux-gnu/libc.so.6",
        data: new Uint8Array(readFileSync(libc)),
        mode: 0o755,
      },
    ]);
    const result = await spawnWithOutput(
      runtime,
      probeBytes,
      ["/work/apt-fanout-loader-probe", "9379800"],
      ["LC_ALL=C"],
      180_000,
    );
    expect(
      result.exitCode,
      `${result.output}\n${result.debugInfo ?? ""}\n${await runtimeDebugSummary(runtime)}`,
    ).toBe(0);
    expect(result.output).toContain("driver-before-writer:elf");
    expect(result.output).toContain("writer-before-large-write:elf");
    expect(result.output).toContain("writer-large-write:bytes=9379800");
    expect(result.output).toContain("writer-after-large-write:elf");
    expect(result.output).toContain("driver-after-writer-exit:elf");
    expect(result.output).toContain("post-writer-exec-loader-check:elf");
    expect(result.output).toContain("driver-after-loader-check:elf");
  } finally {
    runtime.destroy();
  }
});

test("dynamic plain fork preserves page-cache-backed large snapshot file reads", async () => {
  const wasmBytes = readKernelWasm();
  if (!wasmBytes) {
    return;
  }
  const probeBytes = buildDynamicForkLargeFileProbe();
  if (!probeBytes) {
    return;
  }
  const loader = findLibrary("ld-linux-riscv64-lp64d.so.1");
  if (!loader) {
    return logSkip("missing dynamic loader root: set TIDEMARK_TEST_APT_METHOD_LIBRARY_ROOTS");
  }
  const directLibs = directSonames.map((soname) => [soname, findLibrary(soname)] as const);
  const missing = directLibs.filter(([, path]) => !path).map(([soname]) => soname);
  if (missing.length > 0) {
    return logSkip(`missing dynamic library roots: ${missing.join(", ")}`);
  }

  const runtime = await Runtime.create({
    wasmBytes,
    guestMemoryPolicyLimitBytes: 256 * 1024 * 1024,
    maxProcessMemoryPages: 16384,
  });
  try {
    await runtime.applyFsSnapshot([
      {
        kind: "file",
        path: "/lib/ld-linux-riscv64-lp64d.so.1",
        data: new Uint8Array(readFileSync(loader)),
        mode: 0o755,
      },
      {
        kind: "file",
        path: "/usr/lib/riscv64-linux-gnu/ld-linux-riscv64-lp64d.so.1",
        data: new Uint8Array(readFileSync(loader)),
        mode: 0o755,
      },
      ...directLibs.map(([soname, path]) => ({
        kind: "file" as const,
        path: `/usr/lib/riscv64-linux-gnu/${soname}`,
        data: new Uint8Array(readFileSync(path!)),
        mode: 0o755,
      })),
    ]);
    await runtime.writeFile(
      "/var/lib/apt/lists/partial/deb.debian.org_debian_dists_trixie_InRelease",
      new TextEncoder().encode("runtime-owned-file\n"),
      0o644,
    );
    const result = await spawnWithOutput(
      runtime,
      probeBytes,
      ["/work/dynamic-fork-large-file"],
      ["LC_ALL=C"],
      120_000,
    );
    expect(
      result.exitCode,
      `${result.output}\n${result.debugInfo ?? ""}\n${await runtimeDebugSummary(runtime)}`,
    ).toBe(0);
    expect(result.output).toContain("parent-dynamic-large-file:elf");
    expect(result.output).toContain("child-dynamic-large-file:elf");
  } finally {
    runtime.destroy();
  }
});
