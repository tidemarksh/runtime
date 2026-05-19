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
} from "../support/runtime-workload.js";

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
      "    if (n < 0) { if (errno == EINTR) continue; perror(\"read-child\"); return 20; }",
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
    ramSize: 128 * 1024 * 1024,
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
    ramSize: 256 * 1024 * 1024,
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
