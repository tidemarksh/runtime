import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "vite-plus/test";
import {
  Runtime,
  logSkip,
  readKernelWasm,
  runtimeDebugSummary,
  spawnWithOutput,
} from "../../../support/runtime-workload.js";
import { buildSnapshotFromSources } from "../../../support/snapshot-sources.js";

const hostRiscvGcc = process.env.TIDEMARK_TEST_HOST_RISCV_GCC ?? "riscv64-linux-gnu-gcc";

function buildOpenUnlinkWriteProbe(): Uint8Array | null {
  const tmp = mkdtempSync(join(tmpdir(), "tidemark-open-unlink-write-"));
  const sourcePath = join(tmp, "open_unlink_write.c");
  const outputPath = join(tmp, "open-unlink-write");
  try {
    writeFileSync(sourcePath, [
      "#include <errno.h>",
      "#include <fcntl.h>",
      "#include <stdio.h>",
      "#include <string.h>",
      "#include <unistd.h>",
      "",
      "static int write_exact(int fd, const char *data, size_t len) {",
      "  size_t off = 0;",
      "  while (off < len) {",
      "    ssize_t n = write(fd, data + off, len - off);",
      "    if (n < 0) {",
      "      if (errno == EINTR) continue;",
      "      perror(\"write\");",
      "      return 1;",
      "    }",
      "    if (n == 0) { fputs(\"write-zero\\n\", stderr); return 2; }",
      "    off += (size_t)n;",
      "  }",
      "  return 0;",
      "}",
      "",
      "int main(void) {",
      "  const char *path = \"/work/open-unlink-write.txt\";",
      "  int fd = open(path, O_CREAT | O_RDWR | O_TRUNC, 0644);",
      "  if (fd < 0) { perror(\"open\"); return 10; }",
      "  if (write_exact(fd, \"before\", 6) != 0) return 11;",
      "  if (unlink(path) != 0) { perror(\"unlink\"); return 12; }",
      "  if (write_exact(fd, \"-after\", 6) != 0) return 13;",
      "  if (lseek(fd, 0, SEEK_SET) < 0) { perror(\"lseek\"); return 14; }",
      "  char buf[32];",
      "  ssize_t n = read(fd, buf, sizeof(buf));",
      "  if (n < 0) { perror(\"read\"); return 15; }",
      "  if (n != 12 || memcmp(buf, \"before-after\", 12) != 0) {",
      "    fprintf(stderr, \"bad-read n=%ld data=%.*s\\n\", (long)n, (int)n, buf);",
      "    return 16;",
      "  }",
      "  if (close(fd) != 0) { perror(\"close\"); return 17; }",
      "  int check = open(path, O_RDONLY);",
      "  if (check >= 0) { close(check); fputs(\"path-still-exists\\n\", stderr); return 18; }",
      "  if (errno != ENOENT) { perror(\"open-after-unlink\"); return 19; }",
      "  puts(\"open-unlink-write-ok\");",
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
    return logSkip(`failed to build open-unlink-write probe with ${hostRiscvGcc}: ${detail}`);
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
}

function buildForkOpenUnlinkWriteProbe(): Uint8Array | null {
  const tmp = mkdtempSync(join(tmpdir(), "tidemark-fork-open-unlink-write-"));
  const sourcePath = join(tmp, "fork_open_unlink_write.c");
  const outputPath = join(tmp, "fork-open-unlink-write");
  try {
    writeFileSync(sourcePath, [
      "#include <errno.h>",
      "#include <fcntl.h>",
      "#include <stdio.h>",
      "#include <string.h>",
      "#include <sys/wait.h>",
      "#include <unistd.h>",
      "",
      "static int write_exact(int fd, const char *data, size_t len) {",
      "  size_t off = 0;",
      "  while (off < len) {",
      "    ssize_t n = write(fd, data + off, len - off);",
      "    if (n < 0) {",
      "      if (errno == EINTR) continue;",
      "      perror(\"child-write\");",
      "      return 1;",
      "    }",
      "    if (n == 0) { fputs(\"child-write-zero\\n\", stderr); return 2; }",
      "    off += (size_t)n;",
      "  }",
      "  return 0;",
      "}",
      "",
      "static int child_main(void) {",
      "  const char *path = \"/work/fork-open-unlink-write.txt\";",
      "  int fd = open(path, O_CREAT | O_RDWR | O_TRUNC, 0644);",
      "  if (fd < 0) { perror(\"child-open\"); return 10; }",
      "  if (write_exact(fd, \"before\", 6) != 0) return 11;",
      "  if (unlink(path) != 0) { perror(\"child-unlink\"); return 12; }",
      "  if (write_exact(fd, \"-after\", 6) != 0) return 13;",
      "  if (lseek(fd, 0, SEEK_SET) < 0) { perror(\"child-lseek\"); return 14; }",
      "  char buf[32];",
      "  ssize_t n = read(fd, buf, sizeof(buf));",
      "  if (n < 0) { perror(\"child-read\"); return 15; }",
      "  if (n != 12 || memcmp(buf, \"before-after\", 12) != 0) {",
      "    fprintf(stderr, \"child-bad-read n=%ld data=%.*s\\n\", (long)n, (int)n, buf);",
      "    return 16;",
      "  }",
      "  if (close(fd) != 0) { perror(\"child-close\"); return 17; }",
      "  return 0;",
      "}",
      "",
      "int main(void) {",
      "  const char *path = \"/work/fork-open-unlink-write.txt\";",
      "  pid_t pid = fork();",
      "  if (pid < 0) { perror(\"fork\"); return 20; }",
      "  if (pid == 0) _exit(child_main());",
      "  int status = 0;",
      "  if (waitpid(pid, &status, 0) < 0) { perror(\"waitpid\"); return 21; }",
      "  if (!WIFEXITED(status)) { fprintf(stderr, \"child-not-exited status=%d\\n\", status); return 22; }",
      "  if (WEXITSTATUS(status) != 0) { fprintf(stderr, \"child-exit=%d\\n\", WEXITSTATUS(status)); return WEXITSTATUS(status); }",
      "  int check = open(path, O_RDONLY);",
      "  if (check >= 0) { close(check); fputs(\"path-still-exists\\n\", stderr); return 23; }",
      "  if (errno != ENOENT) { perror(\"parent-open-after-unlink\"); return 24; }",
      "  puts(\"fork-open-unlink-write-ok\");",
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
    return logSkip(`failed to build fork-open-unlink-write probe with ${hostRiscvGcc}: ${detail}`);
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
}

function buildForkExecOpenUnlinkWriteProbe(): Uint8Array | null {
  const tmp = mkdtempSync(join(tmpdir(), "tidemark-fork-exec-open-unlink-write-"));
  const sourcePath = join(tmp, "fork_exec_open_unlink_write.c");
  const outputPath = join(tmp, "fork-exec-open-unlink-write");
  try {
    writeFileSync(sourcePath, [
      "#include <errno.h>",
      "#include <fcntl.h>",
      "#include <stdio.h>",
      "#include <string.h>",
      "#include <sys/wait.h>",
      "#include <unistd.h>",
      "",
      "static int write_exact(int fd, const char *data, size_t len) {",
      "  size_t off = 0;",
      "  while (off < len) {",
      "    ssize_t n = write(fd, data + off, len - off);",
      "    if (n < 0) {",
      "      if (errno == EINTR) continue;",
      "      perror(\"exec-child-write\");",
      "      return 1;",
      "    }",
      "    if (n == 0) { fputs(\"exec-child-write-zero\\n\", stderr); return 2; }",
      "    off += (size_t)n;",
      "  }",
      "  return 0;",
      "}",
      "",
      "static int child_main(void) {",
      "  const char *path = \"/work/fork-exec-open-unlink-write.txt\";",
      "  int fd = open(path, O_CREAT | O_RDWR | O_TRUNC, 0644);",
      "  if (fd < 0) { perror(\"exec-child-open\"); return 10; }",
      "  if (write_exact(fd, \"before\", 6) != 0) return 11;",
      "  if (unlink(path) != 0) { perror(\"exec-child-unlink\"); return 12; }",
      "  if (write_exact(fd, \"-after\", 6) != 0) return 13;",
      "  if (lseek(fd, 0, SEEK_SET) < 0) { perror(\"exec-child-lseek\"); return 14; }",
      "  char buf[32];",
      "  ssize_t n = read(fd, buf, sizeof(buf));",
      "  if (n < 0) { perror(\"exec-child-read\"); return 15; }",
      "  if (n != 12 || memcmp(buf, \"before-after\", 12) != 0) {",
      "    fprintf(stderr, \"exec-child-bad-read n=%ld data=%.*s\\n\", (long)n, (int)n, buf);",
      "    return 16;",
      "  }",
      "  if (close(fd) != 0) { perror(\"exec-child-close\"); return 17; }",
      "  return 0;",
      "}",
      "",
      "int main(int argc, char **argv) {",
      "  const char *path = \"/work/fork-exec-open-unlink-write.txt\";",
      "  if (argc > 1 && strcmp(argv[1], \"child\") == 0) return child_main();",
      "  pid_t pid = fork();",
      "  if (pid < 0) { perror(\"fork\"); return 20; }",
      "  if (pid == 0) {",
      "    char *const child_argv[] = {\"/bin/fork-exec-open-unlink-write\", \"child\", NULL};",
      "    execv(child_argv[0], child_argv);",
      "    perror(\"execv\");",
      "    _exit(127);",
      "  }",
      "  int status = 0;",
      "  if (waitpid(pid, &status, 0) < 0) { perror(\"waitpid\"); return 21; }",
      "  if (!WIFEXITED(status)) { fprintf(stderr, \"exec-child-not-exited status=%d\\n\", status); return 22; }",
      "  if (WEXITSTATUS(status) != 0) { fprintf(stderr, \"exec-child-exit=%d\\n\", WEXITSTATUS(status)); return WEXITSTATUS(status); }",
      "  int check = open(path, O_RDONLY);",
      "  if (check >= 0) { close(check); fputs(\"exec-path-still-exists\\n\", stderr); return 23; }",
      "  if (errno != ENOENT) { perror(\"parent-open-after-exec-unlink\"); return 24; }",
      "  puts(\"fork-exec-open-unlink-write-ok\");",
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
    return logSkip(`failed to build fork-exec-open-unlink-write probe with ${hostRiscvGcc}: ${detail}`);
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
}

test("runtime fs keeps open regular-file fd writable after unlink", async () => {
  const wasmBytes = readKernelWasm();
  if (!wasmBytes) {
    return;
  }
  const probeBytes = buildOpenUnlinkWriteProbe();
  if (!probeBytes) {
    return;
  }

  const snapshot = buildSnapshotFromSources([
    {
      kind: "inlineFile",
      guestPath: "/bin/open-unlink-write",
      data: probeBytes,
      mode: 0o755,
    },
  ], {
    missingLabel: "open-unlink-write probe",
  });
  if (snapshot.skipReason) {
    return logSkip(snapshot.skipReason);
  }

  const runtime = await Runtime.create({ wasmBytes, guestMemoryPolicyLimitBytes: 256 * 1024 * 1024 });
  try {
    await runtime.applyFsSnapshot(snapshot.entries ?? []);
    const result = await spawnWithOutput(
      runtime,
      probeBytes,
      ["/bin/open-unlink-write"],
      ["PATH=/bin"],
      20_000,
      { cwd: "/work" },
    ).catch(async (error) => {
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error([detail, await runtimeDebugSummary(runtime)].join("\n"));
    });
    expect(
      result.exitCode,
      `${result.output}\n${result.debugInfo ?? ""}\n${await runtimeDebugSummary(runtime)}`,
    ).toBe(0);
    expect(result.output).toBe("open-unlink-write-ok\n");
  } finally {
    runtime.destroy();
  }
}, 40_000);

test("runtime fs keeps fork-exec child open regular-file fd writable after unlink", async () => {
  const wasmBytes = readKernelWasm();
  if (!wasmBytes) {
    return;
  }
  const probeBytes = buildForkExecOpenUnlinkWriteProbe();
  if (!probeBytes) {
    return;
  }

  const snapshot = buildSnapshotFromSources([
    {
      kind: "inlineFile",
      guestPath: "/bin/fork-exec-open-unlink-write",
      data: probeBytes,
      mode: 0o755,
    },
  ], {
    missingLabel: "fork-exec-open-unlink-write probe",
  });
  if (snapshot.skipReason) {
    return logSkip(snapshot.skipReason);
  }

  const runtime = await Runtime.create({ wasmBytes, guestMemoryPolicyLimitBytes: 256 * 1024 * 1024 });
  try {
    await runtime.applyFsSnapshot(snapshot.entries ?? []);
    const result = await spawnWithOutput(
      runtime,
      probeBytes,
      ["/bin/fork-exec-open-unlink-write"],
      ["PATH=/bin"],
      20_000,
      { cwd: "/work" },
    ).catch(async (error) => {
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error([detail, await runtimeDebugSummary(runtime)].join("\n"));
    });
    expect(
      result.exitCode,
      `${result.output}\n${result.debugInfo ?? ""}\n${await runtimeDebugSummary(runtime)}`,
    ).toBe(0);
    expect(result.output).toBe("fork-exec-open-unlink-write-ok\n");
  } finally {
    runtime.destroy();
  }
}, 40_000);

test("runtime fs keeps fork child open regular-file fd writable after unlink", async () => {
  const wasmBytes = readKernelWasm();
  if (!wasmBytes) {
    return;
  }
  const probeBytes = buildForkOpenUnlinkWriteProbe();
  if (!probeBytes) {
    return;
  }

  const snapshot = buildSnapshotFromSources([
    {
      kind: "inlineFile",
      guestPath: "/bin/fork-open-unlink-write",
      data: probeBytes,
      mode: 0o755,
    },
  ], {
    missingLabel: "fork-open-unlink-write probe",
  });
  if (snapshot.skipReason) {
    return logSkip(snapshot.skipReason);
  }

  const runtime = await Runtime.create({ wasmBytes, guestMemoryPolicyLimitBytes: 256 * 1024 * 1024 });
  try {
    await runtime.applyFsSnapshot(snapshot.entries ?? []);
    const result = await spawnWithOutput(
      runtime,
      probeBytes,
      ["/bin/fork-open-unlink-write"],
      ["PATH=/bin"],
      20_000,
      { cwd: "/work" },
    ).catch(async (error) => {
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error([detail, await runtimeDebugSummary(runtime)].join("\n"));
    });
    expect(
      result.exitCode,
      `${result.output}\n${result.debugInfo ?? ""}\n${await runtimeDebugSummary(runtime)}`,
    ).toBe(0);
    expect(result.output).toBe("fork-open-unlink-write-ok\n");
  } finally {
    runtime.destroy();
  }
}, 40_000);
