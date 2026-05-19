import { Buffer } from "node:buffer";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { existsSync, mkdtempSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { basename, join, resolve } from "node:path";
import { expect, test } from "vite-plus/test";
import type { RuntimeNetworkBridge, RuntimeNetworkConnection } from "../../../src/network.js";
import {
  Runtime,
  decoder,
  logSkip,
  readKernelWasm,
  runtimeDebugSummary,
} from "../../support/runtime-workload.js";
import { buildSnapshotFromSources, type SnapshotSource } from "../../support/snapshot-sources.js";

const methodRoot = process.env.TIDEMARK_TEST_APT_METHOD_ROOT ?? "";
const sqvMethodRoot = process.env.TIDEMARK_TEST_APT_SQV_ROOT
  ?? (existsSync("/tmp/tidemark-apt-deb-extract") ? "/tmp/tidemark-apt-deb-extract" : methodRoot);
const aptRoot = process.env.TIDEMARK_TEST_APT_GET_ROOT
  ?? (existsSync("/tmp/tidemark-apt-deb-extract") ? "/tmp/tidemark-apt-deb-extract" : sqvMethodRoot);
const methodPayload = process.env.TIDEMARK_TEST_APT_METHOD_PAYLOAD ?? "";
const methodResponseBody = process.env.TIDEMARK_TEST_APT_METHOD_RESPONSE_BODY
  ? new Uint8Array(readFileSync(process.env.TIDEMARK_TEST_APT_METHOD_RESPONSE_BODY))
  : new TextEncoder().encode("tidemark-method-ok\n");
const methodPackagesXzResponseBody = process.env.TIDEMARK_TEST_APT_METHOD_PACKAGES_XZ_RESPONSE_BODY
  ? new Uint8Array(readFileSync(process.env.TIDEMARK_TEST_APT_METHOD_PACKAGES_XZ_RESPONSE_BODY))
  : null;
const methodResponseHeaderLines = (process.env.TIDEMARK_TEST_APT_METHOD_RESPONSE_HEADER_LINES ?? "")
  .split("\n")
  .map((line) => line.trim())
  .filter(Boolean);
const hostRiscvGcc = process.env.TIDEMARK_TEST_HOST_RISCV_GCC ?? "riscv64-linux-gnu-gcc";
const methodArgv0 = process.env.TIDEMARK_TEST_APT_METHOD_ARGV0 ?? "/usr/lib/apt/methods/http";
const methodEnv = (process.env.TIDEMARK_TEST_APT_METHOD_ENV ?? "LC_ALL=C\nLANG=C")
  .split("\n")
  .filter(Boolean);
const asyncDoneTimeoutMs = Number(process.env.TIDEMARK_TEST_APT_METHOD_ASYNC_DONE_TIMEOUT_MS ?? "30000");
const chainChildTimeoutMs = Math.max(60_000, asyncDoneTimeoutMs);
const chainProbeDoneTimeoutMs = chainChildTimeoutMs * 4 + 15_000;
const sqvForkProbeDoneTimeoutMs = asyncDoneTimeoutMs * 2 + 10_000;
const aptGetUpdateExitTimeoutMs = Math.max(90_000, asyncDoneTimeoutMs);
const writePayloadAfterCapabilities = process.env.TIDEMARK_TEST_APT_METHOD_WRITE_AFTER_CAPS === "1";
const probeBlockingPipes = process.env.TIDEMARK_TEST_APT_METHOD_PROBE_BLOCKING_PIPES === "1";
const probeVfork = process.env.TIDEMARK_TEST_APT_METHOD_PROBE_VFORK === "1";
const failOnOutput = process.env.TIDEMARK_TEST_APT_METHOD_FAIL_ON_OUTPUT ?? "";
const stopChainAfterHttp = process.env.TIDEMARK_TEST_APT_METHOD_CHAIN_STOP_AFTER_HTTP === "1";
const chainVerifyMethod = process.env.TIDEMARK_TEST_APT_METHOD_CHAIN_VERIFY_METHOD ?? "sqv";
const libraryRoots = (process.env.TIDEMARK_TEST_APT_METHOD_LIBRARY_ROOTS ?? "")
  .split(":")
  .filter(Boolean);
const aptTraceFile = process.env.TIDEMARK_TEST_APT_TRACE_FILE ?? "";

const methodAcquireFilename = "/var/lib/apt/lists/partial/deb.debian.org_debian_dists_trixie_InRelease";

function traceApt(message: string): void {
  if (!aptTraceFile) {
    return;
  }
  writeFileSync(aptTraceFile, `${new Date().toISOString()} ${message}\n`, { flag: "a" });
}

function firstMismatchOffset(left: Uint8Array, right: Uint8Array): number {
  const len = Math.min(left.length, right.length);
  for (let offset = 0; offset < len; offset += 1) {
    if (left[offset] !== right[offset]) {
      return offset;
    }
  }
  return left.length === right.length ? -1 : len;
}

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

function firstExistingFile(candidates: readonly string[]): string | null {
  for (const candidate of candidates) {
    try {
      if (existsSync(candidate) && statSync(candidate).isFile()) {
        return candidate;
      }
    } catch {
      // Try the next candidate.
    }
  }
  return null;
}

function findLibrary(soname: string): string | null {
  for (const root of [methodRoot, ...libraryRoots]) {
    const found = firstExistingFile(candidateLibraryPaths(root, soname));
    if (found) {
      return found;
    }
  }
  return null;
}

function cStringLiteral(value: string): string {
  return JSON.stringify(value)
    .replace(/\u2028/g, "\\u2028")
    .replace(/\u2029/g, "\\u2029");
}

function cStringArray(name: string, values: readonly string[]): string {
  const entries = values.map((value) => `(char *)${cStringLiteral(value)}`).join(", ");
  return `    char *${name}[] = { ${entries}${entries ? ", " : ""}NULL };`;
}

function buildAptMethodSnapshot(): { entries?: ReturnType<typeof buildSnapshotFromSources>["entries"]; skipReason?: string } {
  const methodPath = resolve(methodRoot, "usr/lib/apt/methods/http");
  if (!existsSync(methodPath)) {
    return { skipReason: `missing apt http method: ${methodPath}` };
  }

  const loader = findLibrary("ld-linux-riscv64-lp64d.so.1");
  if (!loader) {
    return { skipReason: "missing apt method loader: ld-linux-riscv64-lp64d.so.1" };
  }

  const sources: SnapshotSource[] = [
    { kind: "dir", guestPath: "/usr/lib/apt/methods" },
    { kind: "dir", guestPath: "/etc" },
    { kind: "dir", guestPath: "/nonexistent" },
    { kind: "dir", guestPath: "/var/lib/apt/lists/partial", mode: 0o755 },
    {
      kind: "inlineFile",
      guestPath: "/etc/passwd",
      data: "root:x:0:0:root:/root:/bin/sh\n_apt:x:100:65534::/nonexistent:/usr/sbin/nologin\n",
    },
    {
      kind: "inlineFile",
      guestPath: "/etc/group",
      data: "root:x:0:\nnogroup:x:65534:\n",
    },
    { kind: "file", hostPath: methodPath, guestPath: "/usr/lib/apt/methods/http", mode: 0o755 },
    {
      kind: "fileCopies",
      hostPath: loader,
      guestPaths: [
        "/lib/ld-linux-riscv64-lp64d.so.1",
        "/usr/lib/ld-linux-riscv64-lp64d.so.1",
        "/usr/lib/riscv64-linux-gnu/ld-linux-riscv64-lp64d.so.1",
      ],
      mode: 0o755,
    },
  ];

  const missing: string[] = [];
  for (const soname of directSonames) {
    const hostPath = findLibrary(soname);
    if (!hostPath) {
      missing.push(soname);
      continue;
    }
    sources.push({
      kind: "file",
      hostPath,
      guestPath: `/usr/lib/riscv64-linux-gnu/${soname}`,
      mode: 0o755,
    });
  }
  if (missing.length > 0) {
    return { skipReason: `missing apt method libraries: ${missing.join(", ")}` };
  }

  return buildSnapshotFromSources(sources, {
    missingLabel: "apt method artifact",
  });
}

function buildAptSqvSnapshot(): { entries?: ReturnType<typeof buildSnapshotFromSources>["entries"]; skipReason?: string } {
  const methodPath = resolve(sqvMethodRoot, "usr/lib/apt/methods/sqv");
  if (!existsSync(methodPath)) {
    return { skipReason: `missing apt sqv method: ${methodPath}` };
  }

  const loader = findLibrary("ld-linux-riscv64-lp64d.so.1");
  if (!loader) {
    return { skipReason: "missing apt sqv loader: ld-linux-riscv64-lp64d.so.1" };
  }

  const sources: SnapshotSource[] = [
    { kind: "dir", guestPath: "/usr/lib/apt/methods" },
    { kind: "dir", guestPath: "/usr/share/apt" },
    { kind: "dir", guestPath: "/etc" },
    { kind: "dir", guestPath: "/etc/apt" },
    { kind: "dir", guestPath: "/etc/apt/trusted.gpg.d" },
    { kind: "dir", guestPath: "/nonexistent" },
    { kind: "dir", guestPath: "/var/lib/apt/lists/partial", mode: 0o755 },
    { kind: "dir", guestPath: "/tmp", mode: 0o777 },
    { kind: "dir", guestPath: "/work", mode: 0o755 },
    {
      kind: "inlineFile",
      guestPath: "/etc/passwd",
      data: "root:x:0:0:root:/root:/bin/sh\n_apt:x:100:65534::/nonexistent:/usr/sbin/nologin\n",
    },
    {
      kind: "inlineFile",
      guestPath: "/etc/group",
      data: "root:x:0:\nnogroup:x:65534:\n",
    },
    { kind: "inlineFile", guestPath: "/etc/nsswitch.conf", data: "passwd: files\ngroup: files\nhosts: files dns\n" },
    { kind: "file", hostPath: methodPath, guestPath: "/usr/lib/apt/methods/sqv", mode: 0o755 },
    {
      kind: "inlineFile",
      guestPath: "/etc/apt/trusted.gpg.d/tidemark-test.gpg",
      data: new Uint8Array([0x98]),
      mode: 0o644,
    },
    {
      kind: "fileCopies",
      hostPath: loader,
      guestPaths: [
        "/lib/ld-linux-riscv64-lp64d.so.1",
        "/usr/lib/ld-linux-riscv64-lp64d.so.1",
        "/usr/lib/riscv64-linux-gnu/ld-linux-riscv64-lp64d.so.1",
      ],
      mode: 0o755,
    },
  ];

  const gpgvMethodPath = resolve(sqvMethodRoot, "usr/lib/apt/methods/gpgv");
  if (existsSync(gpgvMethodPath)) {
    sources.push({ kind: "file", hostPath: gpgvMethodPath, guestPath: "/usr/lib/apt/methods/gpgv", mode: 0o755 });
  }

  const defaultPolicy = resolve(sqvMethodRoot, "usr/share/apt/default-sequoia.config");
  if (existsSync(defaultPolicy)) {
    sources.push({
      kind: "file",
      hostPath: defaultPolicy,
      guestPath: "/usr/share/apt/default-sequoia.config",
      mode: 0o644,
    });
  }

  const missing: string[] = [];
  for (const soname of directSonames) {
    const hostPath = findLibrary(soname);
    if (!hostPath) {
      missing.push(soname);
      continue;
    }
    sources.push({
      kind: "file",
      hostPath,
      guestPath: `/usr/lib/riscv64-linux-gnu/${soname}`,
      mode: 0o755,
    });
  }
  if (missing.length > 0) {
    return { skipReason: `missing apt sqv libraries: ${missing.join(", ")}` };
  }

  return buildSnapshotFromSources(sources, {
    missingLabel: "apt sqv artifact",
  });
}

function buildAptGetSnapshot(): { entries?: ReturnType<typeof buildSnapshotFromSources>["entries"]; skipReason?: string } {
  const aptGetPath = resolve(aptRoot, "usr/bin/apt-get");
  if (!existsSync(aptGetPath)) {
    return { skipReason: `missing apt-get binary: ${aptGetPath}` };
  }

  const httpMethodPath = resolve(aptRoot, "usr/lib/apt/methods/http");
  if (!existsSync(httpMethodPath)) {
    return { skipReason: `missing apt-get http method: ${httpMethodPath}` };
  }

  const loader = findLibrary("ld-linux-riscv64-lp64d.so.1");
  if (!loader) {
    return { skipReason: "missing apt-get loader: ld-linux-riscv64-lp64d.so.1" };
  }

  const sources: SnapshotSource[] = [
    { kind: "dir", guestPath: "/etc" },
    { kind: "dir", guestPath: "/etc/apt" },
    { kind: "dir", guestPath: "/etc/apt/apt.conf.d" },
    { kind: "dir", guestPath: "/etc/apt/trusted.gpg.d" },
    { kind: "dir", guestPath: "/nonexistent" },
    { kind: "dir", guestPath: "/repo" },
    { kind: "dir", guestPath: "/usr/bin" },
    { kind: "dir", guestPath: "/usr/lib/riscv64-linux-gnu" },
    { kind: "dir", guestPath: "/usr/share/dpkg" },
    { kind: "dir", guestPath: "/var/cache/apt/archives/partial", mode: 0o755 },
    { kind: "dir", guestPath: "/var/lib/apt/lists/partial", mode: 0o755 },
    { kind: "dir", guestPath: "/var/lib/dpkg", mode: 0o755 },
    { kind: "dir", guestPath: "/tmp", mode: 0o777 },
    { kind: "file", hostPath: aptGetPath, guestPath: "/usr/bin/apt-get", mode: 0o755 },
    { kind: "tree", hostPath: resolve(aptRoot, "usr/lib/apt"), guestPath: "/usr/lib/apt" },
    {
      kind: "inlineFile",
      guestPath: "/etc/passwd",
      data: "root:x:0:0:root:/root:/bin/sh\n_apt:x:100:65534::/nonexistent:/usr/sbin/nologin\n",
    },
    {
      kind: "inlineFile",
      guestPath: "/etc/group",
      data: "root:x:0:\nnogroup:x:65534:\n",
    },
    { kind: "inlineFile", guestPath: "/etc/nsswitch.conf", data: "passwd: files\ngroup: files\nhosts: files dns\n" },
    { kind: "inlineFile", guestPath: "/etc/hosts", data: "127.0.0.1 localhost\n" },
    { kind: "inlineFile", guestPath: "/etc/resolv.conf", data: "" },
    { kind: "inlineFile", guestPath: "/var/lib/dpkg/status", data: "" },
    { kind: "inlineFile", guestPath: "/var/lib/dpkg/arch", data: "" },
    {
      kind: "inlineFile",
      guestPath: "/usr/share/dpkg/cputable",
      data: [
        "# Version=1.0",
        "riscv64\triscv64\triscv64\t64\tlittle",
        "",
      ].join("\n"),
    },
    {
      kind: "inlineFile",
      guestPath: "/usr/share/dpkg/ostable",
      data: [
        "# Version=2.0",
        "base-gnu-linux\tlinux-gnu\tlinux[^-]*(-gnu.*)?",
        "",
      ].join("\n"),
    },
    {
      kind: "inlineFile",
      guestPath: "/usr/share/dpkg/tupletable",
      data: [
        "# Version=1.0",
        "base-gnu-linux-<cpu>\t<cpu>",
        "",
      ].join("\n"),
    },
    { kind: "inlineFile", guestPath: "/var/lib/apt/cdroms.list", data: "" },
    {
      kind: "inlineFile",
      guestPath: "/etc/apt/apt.conf.d/00tidemark-architecture",
      data: "APT::Architecture \"riscv64\";\nAPT::Architectures { \"riscv64\"; };\n",
    },
    {
      kind: "inlineFile",
      guestPath: "/etc/apt/apt.conf.d/00tidemark-runtime",
      data: [
        "APT::CDROM::NoMount \"true\";",
        "APT::CDROM::Fast \"true\";",
        "APT::Sandbox::User \"root\";",
        "Acquire::Languages \"none\";",
        "Dir::Cache::pkgcache \"\";",
        "Dir::Cache::srcpkgcache \"\";",
        "Dir::State::cdroms \"/var/lib/apt/cdroms.list\";",
        "Acquire::cdrom::mount \"/nonexistent\";",
        "",
      ].join("\n"),
    },
    {
      kind: "inlineFile",
      guestPath: "/etc/apt/sources.list",
      data: "deb [trusted=yes] http://deb.debian.org/debian trixie main\n",
    },
    {
      kind: "fileCopies",
      hostPath: loader,
      guestPaths: [
        "/lib/ld-linux-riscv64-lp64d.so.1",
        "/usr/lib/ld-linux-riscv64-lp64d.so.1",
        "/usr/lib/riscv64-linux-gnu/ld-linux-riscv64-lp64d.so.1",
      ],
      mode: 0o755,
    },
  ];

  const privateLib = firstExistingFile([
    resolve(aptRoot, "usr/lib/riscv64-linux-gnu/libapt-private.so.0.0.0"),
    resolve(aptRoot, "usr/lib/riscv64-linux-gnu/libapt-private.so.0.0"),
  ]);
  if (!privateLib) {
    return { skipReason: "missing apt-get library: libapt-private.so.0.0" };
  }
  sources.push({
    kind: "fileCopies",
    hostPath: privateLib,
    guestPaths: [
      "/usr/lib/riscv64-linux-gnu/libapt-private.so.0.0.0",
      "/usr/lib/riscv64-linux-gnu/libapt-private.so.0.0",
    ],
    mode: 0o644,
  });

  const missing: string[] = [];
  for (const soname of directSonames) {
    const hostPath = findLibrary(soname);
    if (!hostPath) {
      missing.push(soname);
      continue;
    }
    sources.push({
      kind: "file",
      hostPath,
      guestPath: `/usr/lib/riscv64-linux-gnu/${soname}`,
      mode: 0o755,
    });
  }
  if (missing.length > 0) {
    return { skipReason: `missing apt-get libraries: ${missing.join(", ")}` };
  }

  return buildSnapshotFromSources(sources, {
    missingLabel: "apt-get artifact",
  });
}

function buildForkExecMethodProbe(): Uint8Array | null {
  const tmp = mkdtempSync(join(tmpdir(), "tidemark-apt-method-fork-exec-"));
  const sourcePath = join(tmp, "apt_method_fork_exec.c");
  const outputPath = join(tmp, "apt-method-fork-exec-probe");
  try {
    writeFileSync(sourcePath, [
      "#include <errno.h>",
      "#include <fcntl.h>",
      "#include <poll.h>",
      "#include <signal.h>",
      "#include <stdio.h>",
      "#include <stdlib.h>",
      "#include <string.h>",
      "#include <sys/wait.h>",
      "#include <unistd.h>",
      "",
      "static int write_all(int fd, const char *data, ssize_t len) {",
      "  ssize_t off = 0;",
      "  while (off < len) {",
      "    ssize_t n = write(fd, data + off, (size_t)(len - off));",
      "    if (n < 0) {",
      "      if (errno == EINTR) continue;",
      "      return -1;",
      "    }",
      "    off += n;",
      "  }",
      "  return 0;",
      "}",
      "",
      "int main(void) {",
      "  int in_pipe[2];",
      "  int out_pipe[2];",
      "  char payload[16384];",
      "  char output[512];",
      "  int payload_fd;",
      "  ssize_t payload_len;",
      "  signal(SIGPIPE, SIG_IGN);",
      "  payload_fd = open(\"/work/apt-method-payload.txt\", O_RDONLY);",
      "  if (payload_fd < 0) { perror(\"open-payload\"); return 10; }",
      "  payload_len = read(payload_fd, payload, sizeof(payload));",
      "  close(payload_fd);",
      "  if (payload_len <= 0) { perror(\"read-payload\"); return 11; }",
      "  if (pipe(in_pipe) != 0 || pipe(out_pipe) != 0) { perror(\"pipe\"); return 12; }",
      probeBlockingPipes ? "" : "  fcntl(out_pipe[0], F_SETFL, fcntl(out_pipe[0], F_GETFL, 0) | O_NONBLOCK);",
      cStringArray("argv", [methodArgv0]),
      cStringArray("envp", methodEnv),
      probeVfork ? "  pid_t child = vfork();" : "  pid_t child = fork();",
      "  if (child < 0) { perror(\"fork\"); return 13; }",
      "  if (child == 0) {",
      "    if (dup2(in_pipe[0], 0) < 0) { perror(\"dup2-stdin\"); _exit(125); }",
      "    if (dup2(out_pipe[1], 1) < 0) { perror(\"dup2-stdout\"); _exit(126); }",
      "    close(in_pipe[0]); close(in_pipe[1]);",
      "    close(out_pipe[0]); close(out_pipe[1]);",
      "    (void)open(\"/dev/null\", O_WRONLY);",
      "    execve(\"/usr/lib/apt/methods/http\", argv, envp);",
      "    perror(\"execve-method\");",
      "    _exit(127);",
      "  }",
      "  close(in_pipe[0]);",
      "  close(out_pipe[1]);",
      writePayloadAfterCapabilities ? [
        "  int got_caps = 0;",
        "  for (int iter = 0; iter < 100 && !got_caps; iter++) {",
        "    struct pollfd pfd = { .fd = out_pipe[0], .events = POLLIN | POLLHUP, .revents = 0 };",
        "    int pr = poll(&pfd, 1, 100);",
        "    if (pr > 0 && (pfd.revents & POLLIN)) {",
        "      ssize_t n = read(out_pipe[0], output, sizeof(output) - 1);",
        "      if (n > 0) {",
        "        output[n] = '\\0';",
        "        if (strstr(output, \"100 Capabilities\") != NULL) got_caps = 1;",
        "      }",
        "    }",
        "  }",
        "  if (!got_caps) return 15;",
        "  if (write_all(in_pipe[1], payload, payload_len) != 0) { perror(\"write-payload\"); return 14; }",
      ].join("\n") : "  if (write_all(in_pipe[1], payload, payload_len) != 0) { perror(\"write-payload\"); return 14; }",
      "  /* Keep the command pipe open while the method processes the URI, as apt does. */",
      "  for (int iter = 0; iter < 600; iter++) {",
      "    struct pollfd pfd = { .fd = out_pipe[0], .events = POLLIN | POLLHUP, .revents = 0 };",
      "    int status = 0;",
      "    int pr = poll(&pfd, 1, 100);",
    "    if (pr > 0 && (pfd.revents & POLLIN)) {",
    "      ssize_t n = read(out_pipe[0], output, sizeof(output) - 1);",
    "      if (n > 0) {",
    "        output[n] = '\\0';",
    "        (void)write_all(1, output, n);",
        "        if (strstr(output, \"201 URI Done\") != NULL) { close(in_pipe[1]); return 0; }",
    "      }",
    "    }",
      "    pid_t waited = waitpid(child, &status, WNOHANG);",
      "    if (waited == child) {",
      "      return WIFEXITED(status) ? WEXITSTATUS(status) : 128 + WTERMSIG(status);",
      "    }",
      "  }",
      "  return 124;",
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
    return logSkip(`failed to build apt method fork/exec probe with ${hostRiscvGcc}: ${detail}`);
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
}

function buildForkExecMethodEofWaitProbe(): Uint8Array | null {
  const tmp = mkdtempSync(join(tmpdir(), "tidemark-apt-method-eof-wait-"));
  const sourcePath = join(tmp, "apt_method_eof_wait.c");
  const outputPath = join(tmp, "apt-method-eof-wait-probe");
  try {
    writeFileSync(sourcePath, [
      "#include <errno.h>",
      "#include <fcntl.h>",
      "#include <poll.h>",
      "#include <signal.h>",
      "#include <stdio.h>",
      "#include <stdlib.h>",
      "#include <string.h>",
      "#include <sys/wait.h>",
      "#include <unistd.h>",
      "",
      "#define OUTPUT_CAP 131072",
      "",
      "static int write_all(int fd, const char *data, ssize_t len) {",
      "  ssize_t off = 0;",
      "  while (off < len) {",
      "    ssize_t n = write(fd, data + off, (size_t)(len - off));",
      "    if (n < 0) {",
      "      if (errno == EINTR) continue;",
      "      return -1;",
      "    }",
      "    off += n;",
      "  }",
      "  return 0;",
      "}",
      "",
      "static int append_output(char *output, size_t *len, const char *chunk, ssize_t chunk_len) {",
      "  if (chunk_len <= 0) return 0;",
      "  if (*len + (size_t)chunk_len >= OUTPUT_CAP) return -1;",
      "  memcpy(output + *len, chunk, (size_t)chunk_len);",
      "  *len += (size_t)chunk_len;",
      "  output[*len] = '\\0';",
      "  return 0;",
      "}",
      "",
      "static int drain_until(int fd, const char *needle, char *output, size_t *output_len, int timeout_ms) {",
      "  char chunk[4096];",
      "  int elapsed = 0;",
      "  while (elapsed < timeout_ms) {",
      "    struct pollfd pfd = { .fd = fd, .events = POLLIN | POLLHUP, .revents = 0 };",
      "    int pr = poll(&pfd, 1, 100);",
      "    if (pr < 0) {",
      "      if (errno == EINTR) continue;",
      "      perror(\"poll-output\");",
      "      return -1;",
      "    }",
      "    elapsed += 100;",
      "    if (pr == 0) continue;",
      "    if (pfd.revents & POLLIN) {",
      "      for (;;) {",
      "        ssize_t n = read(fd, chunk, sizeof(chunk));",
      "        if (n < 0) {",
      "          if (errno == EINTR) continue;",
      "          if (errno == EAGAIN || errno == EWOULDBLOCK) break;",
      "          perror(\"read-output\");",
      "          return -1;",
      "        }",
      "        if (n == 0) break;",
      "        if (write_all(1, chunk, n) != 0) return -1;",
      "        if (append_output(output, output_len, chunk, n) != 0) return -1;",
      "        if (strstr(output, needle) != NULL) return 0;",
      "      }",
      "    }",
      "    if ((pfd.revents & POLLHUP) && strstr(output, needle) == NULL) return 1;",
      "  }",
      "  return 1;",
      "}",
      "",
      "static int drain_to_eof(int fd, int timeout_ms) {",
      "  char chunk[4096];",
      "  int elapsed = 0;",
      "  while (elapsed < timeout_ms) {",
      "    struct pollfd pfd = { .fd = fd, .events = POLLIN | POLLHUP, .revents = 0 };",
      "    int pr = poll(&pfd, 1, 100);",
      "    if (pr < 0) {",
      "      if (errno == EINTR) continue;",
      "      perror(\"poll-eof\");",
      "      return -1;",
      "    }",
      "    elapsed += 100;",
      "    if (pr == 0) continue;",
      "    if (pfd.revents & POLLIN) {",
      "      for (;;) {",
      "        ssize_t n = read(fd, chunk, sizeof(chunk));",
      "        if (n < 0) {",
      "          if (errno == EINTR) continue;",
      "          if (errno == EAGAIN || errno == EWOULDBLOCK) break;",
      "          perror(\"read-eof\");",
      "          return -1;",
      "        }",
      "        if (n == 0) return 0;",
      "        if (write_all(1, chunk, n) != 0) return -1;",
      "      }",
      "    }",
      "    if (pfd.revents & POLLHUP) {",
      "      ssize_t n = read(fd, chunk, sizeof(chunk));",
      "      if (n == 0) return 0;",
      "      if (n > 0 && write_all(1, chunk, n) != 0) return -1;",
      "      if (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) return -1;",
      "    }",
      "  }",
      "  return 1;",
      "}",
      "",
      "static int wait_child(pid_t child, int timeout_ms) {",
      "  int elapsed = 0;",
      "  while (elapsed < timeout_ms) {",
      "    int status = 0;",
      "    pid_t waited = waitpid(child, &status, WNOHANG);",
      "    if (waited == child) {",
      "      return WIFEXITED(status) ? WEXITSTATUS(status) : 128 + WTERMSIG(status);",
      "    }",
      "    if (waited < 0 && errno != EINTR) return 127;",
      "    usleep(100000);",
      "    elapsed += 100;",
      "  }",
      "  return 124;",
      "}",
      "",
      "int main(void) {",
      "  int in_pipe[2];",
      "  int out_pipe[2];",
      "  char payload[16384];",
      "  char output[OUTPUT_CAP];",
      "  size_t output_len = 0;",
      "  int payload_fd;",
      "  ssize_t payload_len;",
      "  output[0] = '\\0';",
      "  signal(SIGPIPE, SIG_IGN);",
      "  payload_fd = open(\"/work/apt-method-payload.txt\", O_RDONLY);",
      "  if (payload_fd < 0) { perror(\"open-payload\"); return 10; }",
      "  payload_len = read(payload_fd, payload, sizeof(payload));",
      "  close(payload_fd);",
      "  if (payload_len <= 0) { perror(\"read-payload\"); return 11; }",
      "  if (pipe(in_pipe) != 0 || pipe(out_pipe) != 0) { perror(\"pipe\"); return 12; }",
      "  fcntl(out_pipe[0], F_SETFL, fcntl(out_pipe[0], F_GETFL, 0) | O_NONBLOCK);",
      cStringArray("argv", [methodArgv0]),
      cStringArray("envp", methodEnv),
      "  pid_t child = fork();",
      "  if (child < 0) { perror(\"fork\"); return 13; }",
      "  if (child == 0) {",
      "    if (dup2(in_pipe[0], 0) < 0) { perror(\"dup2-stdin\"); _exit(125); }",
      "    if (dup2(out_pipe[1], 1) < 0) { perror(\"dup2-stdout\"); _exit(126); }",
      "    close(in_pipe[0]); close(in_pipe[1]);",
      "    close(out_pipe[0]); close(out_pipe[1]);",
      "    (void)open(\"/dev/null\", O_WRONLY);",
      "    execve(\"/usr/lib/apt/methods/http\", argv, envp);",
      "    perror(\"execve-method\");",
      "    _exit(127);",
      "  }",
      "  close(in_pipe[0]);",
      "  close(out_pipe[1]);",
      `  if (drain_until(out_pipe[0], "100 Capabilities", output, &output_len, ${chainChildTimeoutMs}) != 0) return 15;`,
      "  if (write_all(in_pipe[1], payload, payload_len) != 0) { perror(\"write-payload\"); return 14; }",
      `  if (drain_until(out_pipe[0], "201 URI Done", output, &output_len, ${chainChildTimeoutMs}) != 0) return 16;`,
      "  if (write_all(1, \"parent:http-done\\n\", 17) != 0) return 17;",
      "  close(in_pipe[1]);",
      `  if (drain_to_eof(out_pipe[0], ${chainChildTimeoutMs}) != 0) return 18;`,
      "  if (write_all(1, \"parent:stdout-eof\\n\", 18) != 0) return 19;",
      `  int exit_code = wait_child(child, ${chainChildTimeoutMs});`,
      "  dprintf(1, \"parent:child-exit=%d\\n\", exit_code);",
      "  close(out_pipe[0]);",
      "  return (exit_code == 0 || exit_code == 100) ? 0 : 100 + exit_code;",
      "}",
      "",
    ].join("\n"));
    execFileSync(hostRiscvGcc, [
      "-O2",
      "-static",
      "-Wl,--dynamic-linker=/lib/ld-linux-riscv64-lp64d.so.1",
      "-o",
      outputPath,
      sourcePath,
    ], { stdio: "pipe" });
    return new Uint8Array(readFileSync(outputPath));
  } catch (error) {
    const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
    return logSkip(`failed to build apt method EOF/wait probe with ${hostRiscvGcc}: ${detail}`);
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
}

function buildSqvExecProbe(): Uint8Array | null {
  const tmp = mkdtempSync(join(tmpdir(), "tidemark-apt-sqv-exec-"));
  const sourcePath = join(tmp, "apt_sqv_exec.c");
  const outputPath = join(tmp, "apt-sqv-exec-probe");
  try {
    writeFileSync(sourcePath, [
      "#include <stdio.h>",
      "#include <fcntl.h>",
      "#include <unistd.h>",
      "",
      "int main(void) {",
      "  char *argv[] = { (char *)\"/usr/lib/apt/methods/sqv\", NULL };",
      "  char *envp[] = {",
      "    (char *)\"PATH=/usr/bin:/bin\",",
      "    (char *)\"HOME=/root\",",
      "    (char *)\"TMPDIR=/tmp\",",
      "    (char *)\"LC_ALL=C\",",
      "    (char *)\"LANG=C\",",
      "    NULL",
      "  };",
      "  int lib_fd = open(\"/usr/lib/riscv64-linux-gnu/libapt-pkg.so.7.0\", O_RDONLY);",
      "  if (lib_fd >= 0) {",
      "    unsigned char magic[4];",
      "    ssize_t magic_len = read(lib_fd, magic, sizeof(magic));",
      "    close(lib_fd);",
      "    if (magic_len == 4) {",
      "      fprintf(stdout, \"direct-preflight-libapt:%02x%02x%02x%02x\\n\", magic[0], magic[1], magic[2], magic[3]);",
      "      fflush(stdout);",
      "    }",
      "  }",
      "  execve(\"/usr/lib/apt/methods/sqv\", argv, envp);",
      "  perror(\"execve-sqv\");",
      "  return 127;",
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
    return logSkip(`failed to build apt sqv exec probe with ${hostRiscvGcc}: ${detail}`);
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
}

function buildSqvForkExecProbe(): Uint8Array | null {
  const tmp = mkdtempSync(join(tmpdir(), "tidemark-apt-sqv-fork-exec-"));
  const sourcePath = join(tmp, "apt_sqv_fork_exec.c");
  const outputPath = join(tmp, "apt-sqv-fork-exec-probe");
  try {
    writeFileSync(sourcePath, [
      "#include <errno.h>",
      "#include <fcntl.h>",
      "#include <poll.h>",
      "#include <signal.h>",
      "#include <stdio.h>",
      "#include <stdlib.h>",
      "#include <string.h>",
      "#include <sys/wait.h>",
      "#include <unistd.h>",
      "",
      "#define OUTPUT_CAP 65536",
      "",
      "static int write_all(int fd, const char *data, ssize_t len) {",
      "  ssize_t off = 0;",
      "  while (off < len) {",
      "    ssize_t n = write(fd, data + off, (size_t)(len - off));",
      "    if (n < 0) {",
      "      if (errno == EINTR) continue;",
      "      return -1;",
      "    }",
      "    off += n;",
      "  }",
      "  return 0;",
      "}",
      "",
      "static int append_output(char *output, size_t *len, const char *chunk, ssize_t chunk_len) {",
      "  if (chunk_len <= 0) return 0;",
      "  if (*len + (size_t)chunk_len >= OUTPUT_CAP) return -1;",
      "  memcpy(output + *len, chunk, (size_t)chunk_len);",
      "  *len += (size_t)chunk_len;",
      "  output[*len] = '\\0';",
      "  return 0;",
      "}",
      "",
      "static int drain_until(int fd, const char *needle, char *output, size_t *output_len, int timeout_ms) {",
      "  char chunk[4096];",
      "  int elapsed = 0;",
      "  while (elapsed < timeout_ms) {",
      "    struct pollfd pfd = { .fd = fd, .events = POLLIN | POLLHUP, .revents = 0 };",
      "    int pr = poll(&pfd, 1, 100);",
      "    if (pr < 0) {",
      "      if (errno == EINTR) continue;",
      "      perror(\"poll-output\");",
      "      return -1;",
      "    }",
      "    elapsed += 100;",
      "    if (pr == 0) continue;",
      "    if (pfd.revents & POLLIN) {",
      "      for (;;) {",
      "        ssize_t n = read(fd, chunk, sizeof(chunk));",
      "        if (n < 0) {",
      "          if (errno == EINTR) continue;",
      "          if (errno == EAGAIN || errno == EWOULDBLOCK) break;",
      "          perror(\"read-output\");",
      "          return -1;",
      "        }",
      "        if (n == 0) break;",
      "        if (write_all(1, chunk, n) != 0) return -1;",
      "        if (append_output(output, output_len, chunk, n) != 0) return -1;",
      "        if (strstr(output, needle) != NULL) return 0;",
      "      }",
      "    }",
      "    if ((pfd.revents & POLLHUP) && strstr(output, needle) == NULL) return 1;",
      "  }",
      "  return 1;",
      "}",
      "",
      "int main(void) {",
      "  int in_pipe[2];",
      "  int out_pipe[2];",
      "  char output[OUTPUT_CAP];",
      "  size_t output_len = 0;",
      "  const char *command =",
      "    \"601 Configuration\\n\"",
      "    \"Config-Item: APT::Architecture=riscv64\\n\"",
      "    \"Config-Item: APT::Architectures::=riscv64\\n\"",
      "    \"Config-Item: Dir::Etc::TrustedParts=/etc/apt/trusted.gpg.d\\n\"",
      "    \"Config-Item: Dir::State::lists=/var/lib/apt/lists\\n\"",
      "    \"\\n\"",
      "    \"600 URI Acquire\\n\"",
      `    "URI: sqv:${methodAcquireFilename}\\n"`,
      `    "Filename: ${methodAcquireFilename}\\n"`,
      "    \"Signed-By: /etc/apt/trusted.gpg.d/tidemark-test.gpg\\n\"",
      "    \"\\n\"",
      "    \"\\n\";",
      "  output[0] = '\\0';",
      "  signal(SIGPIPE, SIG_IGN);",
      "  if (pipe(in_pipe) != 0 || pipe(out_pipe) != 0) { perror(\"pipe\"); return 12; }",
      "  fcntl(out_pipe[0], F_SETFL, fcntl(out_pipe[0], F_GETFL, 0) | O_NONBLOCK);",
      "  char *argv[] = { (char *)\"/usr/lib/apt/methods/sqv\", NULL };",
      "  char *envp[] = {",
      "    (char *)\"PATH=/usr/bin:/bin\",",
      "    (char *)\"HOME=/root\",",
      "    (char *)\"TMPDIR=/tmp\",",
      "    (char *)\"LC_ALL=C\",",
      "    (char *)\"LANG=C\",",
      "    NULL",
      "  };",
      "  int parent_lib_fd = open(\"/usr/lib/riscv64-linux-gnu/libapt-pkg.so.7.0\", O_RDONLY);",
      "  if (parent_lib_fd < 0) { perror(\"parent-preflight-open-libapt\"); return 121; }",
      "  unsigned char parent_magic[4];",
      "  ssize_t parent_magic_len = read(parent_lib_fd, parent_magic, sizeof(parent_magic));",
      "  close(parent_lib_fd);",
      "  if (parent_magic_len != 4 || parent_magic[0] != 0x7f || parent_magic[1] != 'E' || parent_magic[2] != 'L' || parent_magic[3] != 'F') {",
      "    char msg[64];",
      "    int msg_len = snprintf(msg, sizeof(msg), \"parent-preflight-libapt:not-elf:%02x%02x%02x%02x\\n\",",
      "      parent_magic_len > 0 ? parent_magic[0] : 0, parent_magic_len > 1 ? parent_magic[1] : 0,",
      "      parent_magic_len > 2 ? parent_magic[2] : 0, parent_magic_len > 3 ? parent_magic[3] : 0);",
      "    if (msg_len > 0) (void)write_all(1, msg, msg_len);",
      "    return 121;",
      "  }",
      "  (void)write_all(1, \"parent-preflight-libapt:elf\\n\", 27);",
      "  pid_t child = fork();",
      "  if (child < 0) { perror(\"fork\"); return 13; }",
      "  if (child == 0) {",
      "    if (dup2(in_pipe[0], 0) < 0) { perror(\"dup2-stdin\"); _exit(125); }",
      "    if (dup2(out_pipe[1], 1) < 0) { perror(\"dup2-stdout\"); _exit(126); }",
      "    close(in_pipe[0]); close(in_pipe[1]);",
      "    close(out_pipe[0]); close(out_pipe[1]);",
      "    int lib_fd = open(\"/usr/lib/riscv64-linux-gnu/libapt-pkg.so.7.0\", O_RDONLY);",
      "    if (lib_fd < 0) { perror(\"preflight-open-libapt\"); _exit(123); }",
      "    unsigned char magic[4];",
      "    ssize_t magic_len = read(lib_fd, magic, sizeof(magic));",
      "    close(lib_fd);",
      "    if (magic_len != 4 || magic[0] != 0x7f || magic[1] != 'E' || magic[2] != 'L' || magic[3] != 'F') {",
      "      char msg[64];",
      "      int msg_len = snprintf(msg, sizeof(msg), \"preflight-libapt:not-elf:%02x%02x%02x%02x\\n\",",
      "        magic_len > 0 ? magic[0] : 0, magic_len > 1 ? magic[1] : 0,",
      "        magic_len > 2 ? magic[2] : 0, magic_len > 3 ? magic[3] : 0);",
      "      if (msg_len > 0) (void)write_all(1, msg, msg_len);",
      "      (void)write_all(1, \"preflight-libapt:not-elf\\n\", 25);",
      "      _exit(122);",
      "    }",
      "    (void)write_all(1, \"preflight-libapt:elf\\n\", 21);",
      "    execve(\"/usr/lib/apt/methods/sqv\", argv, envp);",
      "    perror(\"execve-sqv\");",
      "    _exit(127);",
      "  }",
      "  close(in_pipe[0]);",
      "  close(out_pipe[1]);",
      `  if (drain_until(out_pipe[0], "100 Capabilities", output, &output_len, ${asyncDoneTimeoutMs}) != 0) return 15;`,
      "  if (write_all(in_pipe[1], command, (ssize_t)strlen(command)) != 0) { perror(\"write-command\"); return 14; }",
      `  if (drain_until(out_pipe[0], "URI Failure", output, &output_len, ${asyncDoneTimeoutMs}) != 0) return 16;`,
      "  close(in_pipe[1]);",
      "  (void)write_all(1, \"sqv-fork-probe:done\\n\", 20);",
      "  return 0;",
      "}",
      "",
    ].join("\n"));
    execFileSync(hostRiscvGcc, [
      "-O2",
      "-static",
      "-Wl,--dynamic-linker=/lib/ld-linux-riscv64-lp64d.so.1",
      "-o",
      outputPath,
      sourcePath,
    ], { stdio: "pipe" });
    return new Uint8Array(readFileSync(outputPath));
  } catch (error) {
    const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
    return logSkip(`failed to build apt sqv fork/exec probe with ${hostRiscvGcc}: ${detail}`);
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
}

function buildGpgvForkExecProbe(): Uint8Array | null {
  const tmp = mkdtempSync(join(tmpdir(), "tidemark-apt-gpgv-fork-exec-"));
  const sourcePath = join(tmp, "apt_gpgv_fork_exec.c");
  const outputPath = join(tmp, "apt-gpgv-fork-exec-probe");
  try {
    writeFileSync(sourcePath, [
      "#include <errno.h>",
      "#include <fcntl.h>",
      "#include <poll.h>",
      "#include <signal.h>",
      "#include <stdio.h>",
      "#include <stdlib.h>",
      "#include <string.h>",
      "#include <sys/wait.h>",
      "#include <unistd.h>",
      "",
      "#define OUTPUT_CAP 65536",
      "",
      "static int write_all(int fd, const char *data, ssize_t len) {",
      "  ssize_t off = 0;",
      "  while (off < len) {",
      "    ssize_t n = write(fd, data + off, (size_t)(len - off));",
      "    if (n < 0) {",
      "      if (errno == EINTR) continue;",
      "      return -1;",
      "    }",
      "    off += n;",
      "  }",
      "  return 0;",
      "}",
      "",
      "static int append_output(char *output, size_t *len, const char *chunk, ssize_t chunk_len) {",
      "  if (chunk_len <= 0) return 0;",
      "  if (*len + (size_t)chunk_len >= OUTPUT_CAP) return -1;",
      "  memcpy(output + *len, chunk, (size_t)chunk_len);",
      "  *len += (size_t)chunk_len;",
      "  output[*len] = '\\0';",
      "  return 0;",
      "}",
      "",
      "static int drain_until(int fd, const char *needle, char *output, size_t *output_len, int timeout_ms) {",
      "  char chunk[4096];",
      "  int elapsed = 0;",
      "  while (elapsed < timeout_ms) {",
      "    struct pollfd pfd = { .fd = fd, .events = POLLIN | POLLHUP, .revents = 0 };",
      "    int pr = poll(&pfd, 1, 100);",
      "    if (pr < 0) {",
      "      if (errno == EINTR) continue;",
      "      perror(\"poll-output\");",
      "      return -1;",
      "    }",
      "    elapsed += 100;",
      "    if (pr == 0) continue;",
      "    if (pfd.revents & POLLIN) {",
      "      for (;;) {",
      "        ssize_t n = read(fd, chunk, sizeof(chunk));",
      "        if (n < 0) {",
      "          if (errno == EINTR) continue;",
      "          if (errno == EAGAIN || errno == EWOULDBLOCK) break;",
      "          perror(\"read-output\");",
      "          return -1;",
      "        }",
      "        if (n == 0) break;",
      "        if (write_all(1, chunk, n) != 0) return -1;",
      "        if (append_output(output, output_len, chunk, n) != 0) return -1;",
      "        if (strstr(output, needle) != NULL) return 0;",
      "      }",
      "    }",
      "    if ((pfd.revents & POLLHUP) && strstr(output, needle) == NULL) return 1;",
      "  }",
      "  return 1;",
      "}",
      "",
      "int main(void) {",
      "  int in_pipe[2];",
      "  int out_pipe[2];",
      "  char output[OUTPUT_CAP];",
      "  size_t output_len = 0;",
      "  const char *command =",
      "    \"601 Configuration\\n\"",
      "    \"Config-Item: Acquire::Send-URI-Encoded=1\\n\"",
      "    \"Config-Item: APT::Architecture=riscv64\\n\"",
      "    \"Config-Item: APT::Architectures::=riscv64\\n\"",
      "    \"Config-Item: APT::Sandbox::User=root\\n\"",
      "    \"Config-Item: Dir=/\\n\"",
      "    \"Config-Item: Dir::State=var/lib/apt\\n\"",
      "    \"Config-Item: Dir::State::lists=lists/\\n\"",
      "    \"Config-Item: Dir::Etc=etc/apt\\n\"",
      "    \"Config-Item: Dir::Etc::trustedparts=trusted.gpg.d\\n\"",
      "    \"Config-Item: Acquire::AllowInsecureRepositories=true\\n\"",
      "    \"Config-Item: Acquire::AllowDowngradeToInsecureRepositories=true\\n\"",
      "    \"\\n\"",
      "    \"600 URI Acquire\\n\"",
      `    "URI: gpgv:${methodAcquireFilename}\\n"`,
      `    "Filename: ${methodAcquireFilename}\\n"`,
      "    \"Target-Type: index\\n\"",
      "    \"Target-Release: trixie\\n\"",
      "    \"Target-Repo-URI: http://deb.debian.org/debian/\\n\"",
      "    \"Target-Base-URI: http://deb.debian.org/debian/dists/trixie/\\n\"",
      "    \"Target-Site: http://deb.debian.org/debian\\n\"",
      "    \"Index-File: true\\n\"",
      "    \"Maximum-Size: 10000000\\n\"",
      "    \"Fail-Ignore: true\\n\"",
      "    \"\\n\"",
      "    \"\\n\";",
      "  output[0] = '\\0';",
      "  signal(SIGPIPE, SIG_IGN);",
      "  if (pipe(in_pipe) != 0 || pipe(out_pipe) != 0) { perror(\"pipe\"); return 12; }",
      "  fcntl(out_pipe[0], F_SETFL, fcntl(out_pipe[0], F_GETFL, 0) | O_NONBLOCK);",
      "  char *argv[] = { (char *)\"/usr/lib/apt/methods/gpgv\", NULL };",
      "  char *envp[] = {",
      "    (char *)\"PATH=/usr/bin:/bin\",",
      "    (char *)\"HOME=/root\",",
      "    (char *)\"TMPDIR=/tmp\",",
      "    (char *)\"LC_ALL=C\",",
      "    (char *)\"LANG=C\",",
      "    NULL",
      "  };",
      "  pid_t child = fork();",
      "  if (child < 0) { perror(\"fork\"); return 13; }",
      "  if (child == 0) {",
      "    if (dup2(in_pipe[0], 0) < 0) { perror(\"dup2-stdin\"); _exit(125); }",
      "    if (dup2(out_pipe[1], 1) < 0) { perror(\"dup2-stdout\"); _exit(126); }",
      "    close(in_pipe[0]); close(in_pipe[1]);",
      "    close(out_pipe[0]); close(out_pipe[1]);",
      "    execve(\"/usr/lib/apt/methods/gpgv\", argv, envp);",
      "    perror(\"execve-gpgv-method\");",
      "    _exit(127);",
      "  }",
      "  close(in_pipe[0]);",
      "  close(out_pipe[1]);",
      `  if (drain_until(out_pipe[0], "100 Capabilities", output, &output_len, ${asyncDoneTimeoutMs}) != 0) return 15;`,
      "  if (write_all(in_pipe[1], command, (ssize_t)strlen(command)) != 0) { perror(\"write-command\"); return 14; }",
      `  if (drain_until(out_pipe[0], "URI Failure", output, &output_len, ${asyncDoneTimeoutMs}) != 0) return 16;`,
      "  close(in_pipe[1]);",
      "  (void)write_all(1, \"gpgv-fork-probe:done\\n\", 21);",
      "  return 0;",
      "}",
      "",
    ].join("\n"));
    execFileSync(hostRiscvGcc, [
      "-O2",
      "-static",
      "-Wl,--dynamic-linker=/lib/ld-linux-riscv64-lp64d.so.1",
      "-o",
      outputPath,
      sourcePath,
    ], { stdio: "pipe" });
    return new Uint8Array(readFileSync(outputPath));
  } catch (error) {
    const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
    return logSkip(`failed to build apt gpgv fork/exec probe with ${hostRiscvGcc}: ${detail}`);
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
}

function buildSqvForkPreflightProbe(): Uint8Array | null {
  const tmp = mkdtempSync(join(tmpdir(), "tidemark-apt-sqv-fork-preflight-"));
  const sourcePath = join(tmp, "apt_sqv_fork_preflight.c");
  const outputPath = join(tmp, "apt-sqv-fork-preflight-probe");
  try {
    writeFileSync(sourcePath, [
      "#include <errno.h>",
      "#include <fcntl.h>",
      "#include <poll.h>",
      "#include <signal.h>",
      "#include <stdio.h>",
      "#include <string.h>",
      "#include <sys/wait.h>",
      "#include <unistd.h>",
      "",
      "#define OUTPUT_CAP 4096",
      "",
      "static int write_all(int fd, const char *data, ssize_t len) {",
      "  ssize_t off = 0;",
      "  while (off < len) {",
      "    ssize_t n = write(fd, data + off, (size_t)(len - off));",
      "    if (n < 0) {",
      "      if (errno == EINTR) continue;",
      "      return -1;",
      "    }",
      "    off += n;",
      "  }",
      "  return 0;",
      "}",
      "",
      "static int append_output(char *output, size_t *len, const char *chunk, ssize_t chunk_len) {",
      "  if (chunk_len <= 0) return 0;",
      "  if (*len + (size_t)chunk_len >= OUTPUT_CAP) return -1;",
      "  memcpy(output + *len, chunk, (size_t)chunk_len);",
      "  *len += (size_t)chunk_len;",
      "  output[*len] = '\\0';",
      "  return 0;",
      "}",
      "",
      "static int drain_until(int fd, const char *needle, char *output, size_t *output_len) {",
      "  char chunk[256];",
      "  for (;;) {",
      "    struct pollfd pfd = { .fd = fd, .events = POLLIN | POLLHUP, .revents = 0 };",
      "    int pr = poll(&pfd, 1, 1000);",
      "    if (pr < 0) {",
      "      if (errno == EINTR) continue;",
      "      perror(\"poll-output\");",
      "      return -1;",
      "    }",
      "    if (pr == 0) return 1;",
      "    if (pfd.revents & POLLIN) {",
      "      for (;;) {",
      "        ssize_t n = read(fd, chunk, sizeof(chunk));",
      "        if (n < 0) {",
      "          if (errno == EINTR) continue;",
      "          if (errno == EAGAIN || errno == EWOULDBLOCK) break;",
      "          perror(\"read-output\");",
      "          return -1;",
      "        }",
      "        if (n == 0) break;",
      "        if (write_all(1, chunk, n) != 0) return -1;",
      "        if (append_output(output, output_len, chunk, n) != 0) return -1;",
      "        if (strstr(output, needle) != NULL) return 0;",
      "      }",
      "    }",
      "    if (pfd.revents & POLLHUP) return strstr(output, needle) == NULL ? 1 : 0;",
      "  }",
      "}",
      "",
      "int main(void) {",
      "  int in_pipe[2];",
      "  int out_pipe[2];",
      "  char output[OUTPUT_CAP];",
      "  size_t output_len = 0;",
      "  output[0] = '\\0';",
      "  signal(SIGPIPE, SIG_IGN);",
      "  if (pipe(in_pipe) != 0 || pipe(out_pipe) != 0) { perror(\"pipe\"); return 12; }",
      "  fcntl(out_pipe[0], F_SETFL, fcntl(out_pipe[0], F_GETFL, 0) | O_NONBLOCK);",
      "  int parent_lib_fd = open(\"/usr/lib/riscv64-linux-gnu/libapt-pkg.so.7.0\", O_RDONLY);",
      "  if (parent_lib_fd < 0) { perror(\"parent-preflight-open-libapt\"); return 121; }",
      "  unsigned char parent_magic[4];",
      "  ssize_t parent_magic_len = read(parent_lib_fd, parent_magic, sizeof(parent_magic));",
      "  close(parent_lib_fd);",
      "  if (parent_magic_len != 4 || parent_magic[0] != 0x7f || parent_magic[1] != 'E' || parent_magic[2] != 'L' || parent_magic[3] != 'F') {",
      "    char msg[64];",
      "    int msg_len = snprintf(msg, sizeof(msg), \"parent-preflight-libapt:not-elf:%02x%02x%02x%02x\\n\",",
      "      parent_magic_len > 0 ? parent_magic[0] : 0, parent_magic_len > 1 ? parent_magic[1] : 0,",
      "      parent_magic_len > 2 ? parent_magic[2] : 0, parent_magic_len > 3 ? parent_magic[3] : 0);",
      "    if (msg_len > 0) (void)write_all(1, msg, msg_len);",
      "    return 121;",
      "  }",
      "  (void)write_all(1, \"parent-preflight-libapt:elf\\n\", 27);",
      "  pid_t child = fork();",
      "  if (child < 0) { perror(\"fork\"); return 13; }",
      "  if (child == 0) {",
      "    if (dup2(in_pipe[0], 0) < 0) { perror(\"dup2-stdin\"); _exit(125); }",
      "    if (dup2(out_pipe[1], 1) < 0) { perror(\"dup2-stdout\"); _exit(126); }",
      "    close(in_pipe[0]); close(in_pipe[1]);",
      "    close(out_pipe[0]); close(out_pipe[1]);",
      "    int lib_fd = open(\"/usr/lib/riscv64-linux-gnu/libapt-pkg.so.7.0\", O_RDONLY);",
      "    if (lib_fd < 0) { perror(\"preflight-open-libapt\"); _exit(123); }",
      "    unsigned char magic[4];",
      "    ssize_t magic_len = read(lib_fd, magic, sizeof(magic));",
      "    close(lib_fd);",
      "    if (magic_len != 4 || magic[0] != 0x7f || magic[1] != 'E' || magic[2] != 'L' || magic[3] != 'F') {",
      "      char msg[64];",
      "      int msg_len = snprintf(msg, sizeof(msg), \"preflight-libapt:not-elf:%02x%02x%02x%02x\\n\",",
      "        magic_len > 0 ? magic[0] : 0, magic_len > 1 ? magic[1] : 0,",
      "        magic_len > 2 ? magic[2] : 0, magic_len > 3 ? magic[3] : 0);",
      "      if (msg_len > 0) (void)write_all(1, msg, msg_len);",
      "      (void)write_all(1, \"preflight-libapt:not-elf\\n\", 25);",
      "      _exit(122);",
      "    }",
      "    (void)write_all(1, \"preflight-libapt:elf\\n\", 21);",
      "    _exit(0);",
      "  }",
      "  close(in_pipe[0]);",
      "  close(in_pipe[1]);",
      "  close(out_pipe[1]);",
      "  if (drain_until(out_pipe[0], \"preflight-libapt:elf\", output, &output_len) != 0) return 15;",
      "  close(out_pipe[0]);",
      "  int status = 0;",
      "  if (waitpid(child, &status, 0) < 0) { perror(\"waitpid\"); return 17; }",
      "  if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) return 18;",
      "  (void)write_all(1, \"sqv-preflight-probe:done\\n\", 25);",
      "  return 0;",
      "}",
      "",
    ].join("\n"));
    execFileSync(hostRiscvGcc, [
      "-O2",
      "-static",
      "-Wl,--dynamic-linker=/lib/ld-linux-riscv64-lp64d.so.1",
      "-o",
      outputPath,
      sourcePath,
    ], { stdio: "pipe" });
    return new Uint8Array(readFileSync(outputPath));
  } catch (error) {
    const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
    return logSkip(`failed to build apt sqv fork/preflight probe with ${hostRiscvGcc}: ${detail}`);
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
}

function buildHttpThenSqvForkExecProbe(): Uint8Array | null {
  const tmp = mkdtempSync(join(tmpdir(), "tidemark-apt-http-sqv-fork-exec-"));
  const sourcePath = join(tmp, "apt_http_sqv_fork_exec.c");
  const outputPath = join(tmp, "apt-http-sqv-fork-exec-probe");
  const verifyMethod = chainVerifyMethod === "gpgv" ? "gpgv" : "sqv";
  const verifyConfigLines = verifyMethod === "gpgv"
    ? [
      "    \"Config-Item: Acquire::Send-URI-Encoded=1\\n\"",
      "    \"Config-Item: APT::Architecture=riscv64\\n\"",
      "    \"Config-Item: APT::Architectures::=riscv64\\n\"",
      "    \"Config-Item: APT::Sandbox::User=root\\n\"",
      "    \"Config-Item: Dir=/\\n\"",
      "    \"Config-Item: Dir::State=var/lib/apt\\n\"",
      "    \"Config-Item: Dir::State::lists=lists/\\n\"",
      "    \"Config-Item: Dir::Etc=etc/apt\\n\"",
      "    \"Config-Item: Dir::Etc::trustedparts=trusted.gpg.d\\n\"",
      "    \"Config-Item: Acquire::AllowInsecureRepositories=true\\n\"",
      "    \"Config-Item: Acquire::AllowDowngradeToInsecureRepositories=true\\n\"",
    ]
    : [
      "    \"Config-Item: APT::Architecture=riscv64\\n\"",
      "    \"Config-Item: APT::Architectures::=riscv64\\n\"",
      "    \"Config-Item: Dir::Etc::TrustedParts=/etc/apt/trusted.gpg.d\\n\"",
      "    \"Config-Item: Dir::State::lists=/var/lib/apt/lists\\n\"",
    ];
  const verifyAcquireLines = verifyMethod === "gpgv"
    ? [
      "    \"Target-Type: index\\n\"",
      "    \"Target-Release: trixie\\n\"",
      "    \"Target-Repo-URI: http://deb.debian.org/debian/\\n\"",
      "    \"Target-Base-URI: http://deb.debian.org/debian/dists/trixie/\\n\"",
      "    \"Target-Site: http://deb.debian.org/debian\\n\"",
      "    \"Index-File: true\\n\"",
      "    \"Maximum-Size: 10000000\\n\"",
      "    \"Fail-Ignore: true\\n\"",
    ]
    : [
      "    \"Signed-By: /etc/apt/trusted.gpg.d/tidemark-test.gpg\\n\"",
    ];
  try {
    writeFileSync(sourcePath, [
      "#include <errno.h>",
      "#include <fcntl.h>",
      "#include <poll.h>",
      "#include <signal.h>",
      "#include <stdio.h>",
      "#include <stdlib.h>",
      "#include <string.h>",
      "#include <sys/wait.h>",
      "#include <unistd.h>",
      "",
      "#define OUTPUT_CAP 131072",
      "",
      "static int write_all(int fd, const char *data, ssize_t len) {",
      "  ssize_t off = 0;",
      "  while (off < len) {",
      "    ssize_t n = write(fd, data + off, (size_t)(len - off));",
      "    if (n < 0) {",
      "      if (errno == EINTR) continue;",
      "      return -1;",
      "    }",
      "    off += n;",
      "  }",
      "  return 0;",
      "}",
      "",
      "static int append_output(char *output, size_t *len, const char *chunk, ssize_t chunk_len) {",
      "  if (chunk_len <= 0) return 0;",
      "  if (*len + (size_t)chunk_len >= OUTPUT_CAP) return -1;",
      "  memcpy(output + *len, chunk, (size_t)chunk_len);",
      "  *len += (size_t)chunk_len;",
      "  output[*len] = '\\0';",
      "  return 0;",
      "}",
      "",
      "static int drain_until(int fd, const char *needle, char *output, size_t *output_len, int timeout_ms) {",
      "  char chunk[4096];",
      "  int elapsed = 0;",
      "  while (elapsed < timeout_ms) {",
      "    struct pollfd pfd = { .fd = fd, .events = POLLIN | POLLHUP, .revents = 0 };",
      "    int pr = poll(&pfd, 1, 100);",
      "    if (pr < 0) {",
      "      if (errno == EINTR) continue;",
      "      perror(\"poll-output\");",
      "      return -1;",
      "    }",
      "    elapsed += 100;",
      "    if (pr == 0) continue;",
      "    if (pfd.revents & POLLIN) {",
      "      for (;;) {",
      "        ssize_t n = read(fd, chunk, sizeof(chunk));",
      "        if (n < 0) {",
      "          if (errno == EINTR) continue;",
      "          if (errno == EAGAIN || errno == EWOULDBLOCK) break;",
      "          perror(\"read-output\");",
      "          return -1;",
      "        }",
      "        if (n == 0) break;",
      "        if (write_all(1, chunk, n) != 0) return -1;",
      "        if (append_output(output, output_len, chunk, n) != 0) return -1;",
      "        if (strstr(output, needle) != NULL) return 0;",
      "      }",
      "    }",
      "    if ((pfd.revents & POLLHUP) && strstr(output, needle) == NULL) return 1;",
      "  }",
      "  return 1;",
      "}",
      "",
      "static int wait_child(pid_t child, int timeout_ms) {",
      "  int elapsed = 0;",
      "  while (elapsed < timeout_ms) {",
      "    int status = 0;",
      "    pid_t waited = waitpid(child, &status, WNOHANG);",
      "    if (waited == child) {",
      "      return WIFEXITED(status) ? WEXITSTATUS(status) : 128 + WTERMSIG(status);",
      "    }",
      "    if (waited < 0 && errno != EINTR) return 127;",
      "    usleep(100000);",
      "    elapsed += 100;",
      "  }",
      "  return 124;",
      "}",
      "",
      "struct kept_child {",
      "  pid_t pid;",
      "  int stdin_fd;",
      "  int stdout_fd;",
      "};",
      "",
      "static int start_child_keepalive(const char *path, char **argv, char **envp, const char *command, ssize_t command_len, const char *needle, const char *done, struct kept_child *kept) {",
      "  int in_pipe[2];",
      "  int out_pipe[2];",
      "  char output[OUTPUT_CAP];",
      "  size_t output_len = 0;",
      "  output[0] = '\\0';",
      "  if (pipe(in_pipe) != 0 || pipe(out_pipe) != 0) { perror(\"pipe\"); return 12; }",
      "  fcntl(out_pipe[0], F_SETFL, fcntl(out_pipe[0], F_GETFL, 0) | O_NONBLOCK);",
      "  pid_t child = fork();",
      "  if (child < 0) { perror(\"fork\"); return 13; }",
      "  if (child == 0) {",
      "    if (dup2(in_pipe[0], 0) < 0) { perror(\"dup2-stdin\"); _exit(125); }",
      "    if (dup2(out_pipe[1], 1) < 0) { perror(\"dup2-stdout\"); _exit(126); }",
      "    close(in_pipe[0]); close(in_pipe[1]);",
      "    close(out_pipe[0]); close(out_pipe[1]);",
      "    (void)open(\"/dev/null\", O_WRONLY);",
      "    execve(path, argv, envp);",
      "    perror(\"execve-child\");",
      "    _exit(127);",
      "  }",
      "  close(in_pipe[0]);",
      "  close(out_pipe[1]);",
      `  if (drain_until(out_pipe[0], "100 Capabilities", output, &output_len, ${chainChildTimeoutMs}) != 0) return 15;`,
      "  if (command_len > 0 && write_all(in_pipe[1], command, command_len) != 0) { perror(\"write-command\"); return 14; }",
      `  if (drain_until(out_pipe[0], needle, output, &output_len, ${chainChildTimeoutMs}) != 0) return 15;`,
      "  kept->pid = child;",
      "  kept->stdin_fd = in_pipe[1];",
      "  kept->stdout_fd = out_pipe[0];",
      "  if (write_all(1, done, (ssize_t)strlen(done)) != 0) return 16;",
      "  return 0;",
      "}",
      "",
      "static int run_child(const char *path, char **argv, char **envp, const char *command, ssize_t command_len, const char *needle, const char *done) {",
      "  int in_pipe[2];",
      "  int out_pipe[2];",
      "  char output[OUTPUT_CAP];",
      "  size_t output_len = 0;",
      "  output[0] = '\\0';",
      "  if (pipe(in_pipe) != 0 || pipe(out_pipe) != 0) { perror(\"pipe\"); return 12; }",
      "  fcntl(out_pipe[0], F_SETFL, fcntl(out_pipe[0], F_GETFL, 0) | O_NONBLOCK);",
      "  pid_t child = fork();",
      "  if (child < 0) { perror(\"fork\"); return 13; }",
      "  if (child == 0) {",
      "    if (dup2(in_pipe[0], 0) < 0) { perror(\"dup2-stdin\"); _exit(125); }",
      "    if (dup2(out_pipe[1], 1) < 0) { perror(\"dup2-stdout\"); _exit(126); }",
      "    close(in_pipe[0]); close(in_pipe[1]);",
      "    close(out_pipe[0]); close(out_pipe[1]);",
      "    execve(path, argv, envp);",
      "    perror(\"execve-child\");",
      "    _exit(127);",
      "  }",
      "  close(in_pipe[0]);",
      "  close(out_pipe[1]);",
      `  if (drain_until(out_pipe[0], "100 Capabilities", output, &output_len, ${chainChildTimeoutMs}) != 0) return 15;`,
      "  if (command_len > 0 && write_all(in_pipe[1], command, command_len) != 0) { perror(\"write-command\"); return 14; }",
      `  if (drain_until(out_pipe[0], needle, output, &output_len, ${chainChildTimeoutMs}) != 0) return 15;`,
      "  close(in_pipe[1]);",
      `  int exit_code = wait_child(child, ${chainChildTimeoutMs});`,
      "  if (exit_code == 124) return 124;",
      "  if (write_all(1, done, (ssize_t)strlen(done)) != 0) return 16;",
      "  return 0;",
      "}",
      "",
      "static int report_file_head(const char *path) {",
      "  unsigned char head[32];",
      "  unsigned char tail[64];",
      "  char line[512];",
      "  off_t size = -1;",
      "  off_t marker = -1;",
      "  int fd = open(path, O_RDONLY);",
      "  if (fd < 0) {",
      "    snprintf(line, sizeof(line), \"chain:file-head-open-failed:%d\\n\", errno);",
      "    write_all(1, line, (ssize_t)strlen(line));",
      "    return 0;",
      "  }",
      "  ssize_t n = read(fd, head, sizeof(head));",
      "  if (n < 0) {",
      "    close(fd);",
      "    snprintf(line, sizeof(line), \"chain:file-head-read-failed:%d\\n\", errno);",
      "    write_all(1, line, (ssize_t)strlen(line));",
      "    return 0;",
      "  }",
      "  size = lseek(fd, 0, SEEK_END);",
      "  if (size >= 0) {",
      "    char buf[4096];",
      "    const char *needle = \"HTTP/1.1\";",
      "    size_t carry_len = 0;",
      "    off_t pos = 1;",
      "    (void)lseek(fd, 1, SEEK_SET);",
      "    while (marker < 0) {",
      "      ssize_t rn = read(fd, buf + carry_len, sizeof(buf) - carry_len);",
      "      if (rn <= 0) break;",
      "      size_t total = carry_len + (size_t)rn;",
      "      for (size_t i = 0; i + 8 <= total; i++) {",
      "        if (memcmp(buf + i, needle, 8) == 0) { marker = pos + (off_t)i - (off_t)carry_len; break; }",
      "      }",
      "      if (total >= 7) {",
      "        memmove(buf, buf + total - 7, 7);",
      "        carry_len = 7;",
      "      } else {",
      "        carry_len = total;",
      "      }",
      "      pos += rn;",
      "    }",
      "  }",
      "  ssize_t tail_len = 0;",
      "  if (size > 0) {",
      "    off_t tail_start = size > (off_t)sizeof(tail) ? size - (off_t)sizeof(tail) : 0;",
      "    if (lseek(fd, tail_start, SEEK_SET) >= 0) tail_len = read(fd, tail, sizeof(tail));",
      "  }",
      "  close(fd);",
      "  int off = snprintf(line, sizeof(line), \"chain:file-state:size=%lld:head_n=%zd:head=\", (long long)size, n);",
      "  for (ssize_t i = 0; i < n && off + 3 < (int)sizeof(line); i++) {",
      "    off += snprintf(line + off, sizeof(line) - (size_t)off, \"%02x\", head[i]);",
      "  }",
      "  off += snprintf(line + off, sizeof(line) - (size_t)off, \":tail_n=%zd:tail=\", tail_len);",
      "  for (ssize_t i = 0; i < tail_len && off + 3 < (int)sizeof(line); i++) {",
      "    off += snprintf(line + off, sizeof(line) - (size_t)off, \"%02x\", tail[i]);",
      "  }",
      "  snprintf(line + off, sizeof(line) - (size_t)off, \":http_marker=%lld:pgp=%d\\n\", (long long)marker, n >= 10 && memcmp(head, \"-----BEGIN\", 10) == 0);",
      "  write_all(1, line, (ssize_t)strlen(line));",
      "  return 0;",
      "}",
      "",
      "int main(void) {",
      "  char payload[16384];",
      "  struct kept_child http_child;",
      "  int payload_fd;",
      "  ssize_t payload_len;",
      "  signal(SIGPIPE, SIG_IGN);",
      "  payload_fd = open(\"/work/apt-method-payload.txt\", O_RDONLY);",
      "  if (payload_fd < 0) { perror(\"open-payload\"); return 10; }",
      "  payload_len = read(payload_fd, payload, sizeof(payload));",
      "  close(payload_fd);",
      "  if (payload_len <= 0) { perror(\"read-payload\"); return 11; }",
      cStringArray("http_argv", [methodArgv0]),
      cStringArray("http_envp", methodEnv),
      `  char *sqv_argv[] = { (char *)"/usr/lib/apt/methods/${verifyMethod}", NULL };`,
      "  char *sqv_envp[] = {",
      "    (char *)\"PATH=/usr/bin:/bin\",",
      "    (char *)\"HOME=/root\",",
      "    (char *)\"TMPDIR=/tmp\",",
      "    (char *)\"LC_ALL=C\",",
      "    (char *)\"LANG=C\",",
      "    NULL",
      "  };",
      "  const char *sqv_command =",
      "    \"601 Configuration\\n\"",
      ...verifyConfigLines,
      "    \"\\n\"",
      "    \"600 URI Acquire\\n\"",
      `    "URI: ${verifyMethod}:${methodAcquireFilename}\\n"`,
      `    "Filename: ${methodAcquireFilename}\\n"`,
      ...verifyAcquireLines,
      "    \"\\n\"",
      "    \"\\n\";",
      "  int rc = start_child_keepalive(\"/usr/lib/apt/methods/http\", http_argv, http_envp, payload, payload_len, \"201 URI Done\", \"chain:http-done\\n\", &http_child);",
      "  if (rc != 0) return rc;",
      `  report_file_head("${methodAcquireFilename}");`,
      ...(stopChainAfterHttp
        ? [
          "  close(http_child.stdin_fd);",
          "  close(http_child.stdout_fd);",
          `  if (wait_child(http_child.pid, ${chainChildTimeoutMs}) == 124) return 124;`,
          "  return 77;",
        ]
        : []),
      `  rc = run_child("/usr/lib/apt/methods/${verifyMethod}", sqv_argv, sqv_envp, sqv_command, (ssize_t)strlen(sqv_command), "URI Failure", "chain:sqv-done\\n");`,
      "  if (http_child.stdin_fd >= 0) close(http_child.stdin_fd);",
      "  if (http_child.stdout_fd >= 0) close(http_child.stdout_fd);",
      "  if (rc != 0) return 100 + rc;",
      "  return 0;",
      "}",
      "",
    ].join("\n"));
    execFileSync(hostRiscvGcc, [
      "-O2",
      "-static",
      "-Wl,--dynamic-linker=/lib/ld-linux-riscv64-lp64d.so.1",
      "-o",
      outputPath,
      sourcePath,
    ], { stdio: "pipe" });
    return new Uint8Array(readFileSync(outputPath));
  } catch (error) {
    const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
    return logSkip(`failed to build apt http/sqv fork/exec probe with ${hostRiscvGcc}: ${detail}`);
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
}

function buildForkExecSocketOpenFcntlProbe(): Uint8Array | null {
  const tmp = mkdtempSync(join(tmpdir(), "tidemark-fork-exec-socket-open-fcntl-"));
  const sourcePath = join(tmp, "fork_exec_socket_open_fcntl.c");
  const outputPath = join(tmp, "fork-exec-socket-open-fcntl-probe");
  try {
    writeFileSync(sourcePath, [
      "#include <arpa/inet.h>",
      "#include <errno.h>",
      "#include <fcntl.h>",
      "#include <netinet/in.h>",
      "#include <poll.h>",
      "#include <signal.h>",
      "#include <stdio.h>",
      "#include <stdlib.h>",
      "#include <string.h>",
      "#include <sys/socket.h>",
      "#include <sys/wait.h>",
      "#include <unistd.h>",
      "",
      "static int write_all(int fd, const char *data, ssize_t len) {",
      "  ssize_t off = 0;",
      "  while (off < len) {",
      "    ssize_t n = write(fd, data + off, (size_t)(len - off));",
      "    if (n < 0) {",
      "      if (errno == EINTR) continue;",
      "      return -1;",
      "    }",
      "    off += n;",
      "  }",
      "  return 0;",
      "}",
      "",
      "static int wait_child(pid_t child, int timeout_ms) {",
      "  int elapsed = 0;",
      "  while (elapsed < timeout_ms) {",
      "    int status = 0;",
      "    pid_t waited = waitpid(child, &status, WNOHANG);",
      "    if (waited == child) return WIFEXITED(status) ? WEXITSTATUS(status) : 128 + WTERMSIG(status);",
      "    if (waited < 0 && errno != EINTR) return 127;",
      "    usleep(100000);",
      "    elapsed += 100;",
      "  }",
      "  return 124;",
      "}",
      "",
      "static int child_main(void) {",
      "  int sock = socket(AF_INET, SOCK_STREAM, 0);",
      "  if (sock < 0) { perror(\"socket\"); return 20; }",
      "  struct sockaddr_in addr;",
      "  memset(&addr, 0, sizeof(addr));",
      "  addr.sin_family = AF_INET;",
      "  addr.sin_port = htons(8080);",
      "  addr.sin_addr.s_addr = htonl(0x7f000001u);",
      "  if (connect(sock, (struct sockaddr *)&addr, sizeof(addr)) != 0) { perror(\"connect\"); return 21; }",
      "  const char *request = \"GET http://deb.debian.org/debian/dists/trixie/InRelease HTTP/1.1\\r\\nHost: deb.debian.org\\r\\nConnection: close\\r\\n\\r\\n\";",
      "  if (write_all(sock, request, (ssize_t)strlen(request)) != 0) { perror(\"write-request\"); return 22; }",
      "  char buf[4096];",
      "  char header[8192];",
      "  ssize_t header_len = 0;",
      "  ssize_t body_total = 0;",
      "  ssize_t next_report = 262144;",
      "  int saw_header = 0;",
      "  int fd = -1;",
      "  for (;;) {",
      "    ssize_t n = read(sock, buf, sizeof(buf));",
      "    if (n < 0) {",
      "      if (errno == EINTR) continue;",
      "      dprintf(1, \"child:read-error:errno=%d:body=%ld\\n\", errno, (long)body_total);",
      "      perror(\"read-response\");",
      "      sleep(10);",
      "      return 23;",
      "    }",
      "    if (n == 0) break;",
      "    if (!saw_header) {",
      "      if (header_len + n >= (ssize_t)sizeof(header)) { fprintf(stderr, \"header-too-large\\n\"); return 28; }",
      "      memcpy(header + header_len, buf, (size_t)n);",
      "      header_len += n;",
      "      header[header_len] = '\\0';",
      "      char *body = strstr(header, \"\\r\\n\\r\\n\");",
      "      if (body == NULL) continue;",
      "      body += 4;",
      "      saw_header = 1;",
      `      fd = open(${cStringLiteral(methodAcquireFilename)}, O_CREAT | O_WRONLY | O_TRUNC, 0644);`,
      "      if (fd < 0) { perror(\"open-output\"); return 24; }",
      "      dprintf(1, \"child:open-fd=%d\\n\", fd);",
      "      if (fcntl(fd, F_SETFD, FD_CLOEXEC) != 0) { perror(\"fcntl-cloexec\"); return 25; }",
      "      ssize_t body_len = header_len - (body - header);",
      "      if (body_len > 0 && write_all(fd, body, body_len) != 0) { perror(\"write-output\"); return 26; }",
      "      body_total += body_len;",
      "      if (body_total >= next_report) { dprintf(1, \"child:body=%ld\\n\", (long)body_total); next_report += 262144; }",
      "      continue;",
      "    }",
    "    if (write_all(fd, buf, n) != 0) { perror(\"write-output\"); return 26; }",
    "    body_total += n;",
    "    if (body_total >= next_report) { dprintf(1, \"child:body=%ld\\n\", (long)body_total); next_report += 262144; }",
      "  }",
      "  close(sock);",
      "  if (!saw_header || fd < 0) { fprintf(stderr, \"missing-response-header\\n\"); return 29; }",
      "  close(fd);",
      `  int verify_fd = open(${cStringLiteral(methodAcquireFilename)}, O_RDONLY);`,
      "  if (verify_fd < 0) { perror(\"open-verify\"); return 30; }",
      "  ssize_t verify_total = 0;",
      "  ssize_t first_zero = -1;",
      "  ssize_t zero_count = 0;",
      "  for (;;) {",
      "    ssize_t n = read(verify_fd, buf, sizeof(buf));",
      "    if (n < 0) { if (errno == EINTR) continue; perror(\"read-verify\"); return 31; }",
      "    if (n == 0) break;",
      "    for (ssize_t i = 0; i < n; i++) {",
      "      if (buf[i] == 0) {",
      "        if (first_zero < 0) first_zero = verify_total + i;",
      "        zero_count++;",
      "      }",
      "    }",
      "    verify_total += n;",
      "  }",
      "  close(verify_fd);",
      "  dprintf(1, \"child:verify-total=%ld:first-zero=%ld:zeros=%ld\\n\", (long)verify_total, (long)first_zero, (long)zero_count);",
      "  dprintf(1, \"child:body-bytes=%ld\\n\", (long)body_total);",
      "  if (write_all(1, \"child:done\\n\", 11) != 0) return 27;",
      "  return 0;",
      "}",
      "",
      "int main(int argc, char **argv, char **envp) {",
      "  if (argc > 1 && strcmp(argv[1], \"child\") == 0) return child_main();",
      "  int in_pipe[2];",
      "  int out_pipe[2];",
      "  signal(SIGPIPE, SIG_IGN);",
      "  if (pipe(in_pipe) != 0 || pipe(out_pipe) != 0) { perror(\"pipe\"); return 12; }",
      "  fcntl(out_pipe[0], F_SETFL, fcntl(out_pipe[0], F_GETFL, 0) | O_NONBLOCK);",
      "  pid_t child = fork();",
      "  if (child < 0) { perror(\"fork\"); return 13; }",
      "  if (child == 0) {",
      "    if (dup2(in_pipe[0], 0) < 0) { perror(\"dup2-stdin\"); _exit(125); }",
      "    if (dup2(out_pipe[1], 1) < 0) { perror(\"dup2-stdout\"); _exit(126); }",
      "    close(in_pipe[0]); close(in_pipe[1]);",
      "    close(out_pipe[0]); close(out_pipe[1]);",
      "    (void)open(\"/dev/null\", O_WRONLY);",
      "    char *child_argv[] = { (char *)\"/work/fd-lifecycle-probe\", (char *)\"child\", NULL };",
      "    execve(\"/work/fd-lifecycle-probe\", child_argv, envp);",
      "    perror(\"execve-child\");",
      "    _exit(127);",
      "  }",
      "  close(in_pipe[0]); close(in_pipe[1]); close(out_pipe[1]);",
      "  char output[512];",
      "  int saw_done = 0;",
      "  for (int iter = 0; iter < 600 && !saw_done; iter++) {",
      "    struct pollfd pfd = { .fd = out_pipe[0], .events = POLLIN | POLLHUP, .revents = 0 };",
      "    int pr = poll(&pfd, 1, 100);",
      "    if (pr > 0 && (pfd.revents & POLLIN)) {",
      "      ssize_t n = read(out_pipe[0], output, sizeof(output) - 1);",
      "      if (n > 0) {",
      "        output[n] = '\\0';",
      "        (void)write_all(1, output, n);",
      "        if (strstr(output, \"child:done\") != NULL) saw_done = 1;",
      "      }",
      "    }",
      "  }",
      "  close(out_pipe[0]);",
      "  int exit_code = wait_child(child, 60000);",
      "  if (exit_code != 0) return exit_code;",
      "  return saw_done ? 0 : 124;",
      "}",
      "",
    ].join("\n"));
    execFileSync(hostRiscvGcc, [
      "-O2",
      "-static",
      "-Wl,--dynamic-linker=/lib/ld-linux-riscv64-lp64d.so.1",
      "-o",
      outputPath,
      sourcePath,
    ], { stdio: "pipe" });
    return new Uint8Array(readFileSync(outputPath));
  } catch (error) {
    const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
    return logSkip(`failed to build fork/exec socket/open/fcntl probe with ${hostRiscvGcc}: ${detail}`);
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
}

function serializeHttpResponse(body: Uint8Array): Uint8Array {
  const headers = methodResponseHeaderLines.length > 0
    ? methodResponseHeaderLines
    : [
      "Date: Fri, 01 May 2026 00:00:00 GMT",
      "Last-Modified: Fri, 01 May 2026 00:00:00 GMT",
      `Content-Length: ${body.length}`,
      "Connection: close",
    ];
  const head = new TextEncoder().encode([
    "HTTP/1.1 200 OK",
    ...headers,
    "",
    "",
  ].join("\r\n"));
  const out = new Uint8Array(head.length + body.length);
  out.set(head);
  out.set(body, head.length);
  return out;
}

function selectMethodResponseBody(request: string): Uint8Array {
  if (
    methodPackagesXzResponseBody
    && request.includes("/main/binary-riscv64/")
    && (request.includes("/by-hash/") || request.includes("Packages.xz"))
  ) {
    return methodPackagesXzResponseBody;
  }
  return methodResponseBody;
}

function createMethodNetworkBridge(requests: string[]): RuntimeNetworkBridge {
  return {
    connect({ endpoint }) {
      if (endpoint.family !== "inet" || endpoint.address !== "127.0.0.1" || endpoint.port !== 8080) {
        return { errno: 111 };
      }
      let responseSent = false;
      let sawRequest = false;
      let requestText = "";
      let responseBytes: Uint8Array | null = null;
      let responseOffset = 0;
      const connection: RuntimeNetworkConnection = {
        write(data) {
          const request = decoder.decode(data);
          requestText += request;
          requests.push(request);
          if (request.includes("GET ")) {
            sawRequest = true;
          }
        },
        read() {
          if (responseSent) {
            return null;
          }
          if (!sawRequest) {
            return undefined;
          }
          responseSent = true;
          return serializeHttpResponse(selectMethodResponseBody(requestText));
        },
        close() {},
      };
      return { connection };
    },
  };
}

function createAsyncMethodNetworkBridge(requests: string[], events: string[] = []): RuntimeNetworkBridge {
  const record = (event: string): void => {
    if (events.length < 64) {
      events.push(event);
    }
  };
  return {
    connect({ endpoint }) {
      record(`connect:${endpoint.family}:${endpoint.address}:${endpoint.port}`);
      if (endpoint.family !== "inet" || endpoint.address !== "127.0.0.1" || endpoint.port !== 8080) {
        record("connect:refused");
        return { errno: 111 };
      }
      let responseSent = false;
      let sawRequest = false;
      let requestText = "";
      let responseBytes: Uint8Array | null = null;
      let responseOffset = 0;
      const connection: RuntimeNetworkConnection = {
        write(data) {
          const request = decoder.decode(data);
          requestText += request;
          requests.push(request);
          if (request.includes("GET ")) {
            sawRequest = true;
          }
          record(`write:${data.length}`);
        },
        async read(maxBytes) {
          record(`read:start:requests=${requests.length}:local=${sawRequest ? 1 : 0}:sent=${responseSent ? 1 : 0}`);
          if (responseBytes !== null && responseOffset < responseBytes.length) {
            const chunk = responseBytes.slice(responseOffset, responseOffset + maxBytes);
            responseOffset += chunk.length;
            record(`read:chunk:${chunk.length}:${responseOffset}/${responseBytes.length}`);
            return chunk;
          }
          if (responseSent || (responseBytes !== null && responseOffset >= responseBytes.length)) {
            responseSent = true;
            record("read:eof");
            return null;
          }
          if (!sawRequest) {
            record("read:no-request");
            return undefined;
          }
          responseBytes = serializeHttpResponse(selectMethodResponseBody(requestText));
          await new Promise((resolveDone) => setTimeout(resolveDone, 1));
          const chunk = responseBytes.slice(0, maxBytes);
          responseOffset = chunk.length;
          record(`read:response:${chunk.length}:${responseOffset}/${responseBytes.length}`);
          return chunk;
        },
        close() {},
      };
      return { connection };
    },
  };
}

async function waitForBridgeRequest(
  requests: readonly string[],
  timeoutMs: number,
): Promise<void> {
  const started = Date.now();
  while (Date.now() - started < timeoutMs) {
    if (requests.some((request) => request.includes("GET http://deb.debian.org/debian/dists/trixie/InRelease"))) {
      return;
    }
    await new Promise((resolveDone) => setTimeout(resolveDone, 25));
  }
  throw new Error(`timeout waiting for apt method bridge request: ${JSON.stringify(requests)}`);
}

async function waitForOutput(
  output: () => string,
  pattern: string,
  timeoutMs: number,
): Promise<void> {
  const started = Date.now();
  while (Date.now() - started < timeoutMs) {
    const text = output();
    if (text.includes(pattern)) {
      return;
    }
    if (failOnOutput && text.includes(failOnOutput)) {
      throw new Error(`output contained fail-fast pattern ${JSON.stringify(failOnOutput)}: ${JSON.stringify(text)}`);
    }
    await new Promise((resolveDone) => setTimeout(resolveDone, 25));
  }
  throw new Error(`timeout waiting for output ${JSON.stringify(pattern)}: ${JSON.stringify(output())}`);
}

async function describeGuestFile(
  runtime: Awaited<ReturnType<typeof Runtime.create>>,
  path: string,
): Promise<string> {
  try {
    const data = await runtime.readFile(path);
    const buffer = Buffer.from(data);
    const sha256 = createHash("sha256").update(buffer).digest("hex");
    const httpMarker = buffer.indexOf("HTTP/1.1", 1, "ascii");
    return [
      `${path}:size=${data.length}`,
      `sha256=${sha256}`,
      `httpMarker=${httpMarker}`,
      `head=${JSON.stringify(decoder.decode(data.slice(0, 64)))}`,
      `tail=${JSON.stringify(decoder.decode(data.slice(Math.max(0, data.length - 96))))}`,
    ].join(":");
  } catch (error) {
    const detail = error instanceof Error ? error.message : String(error);
    return `${path}:missing:${detail}`;
  }
}

async function describeGuestSqvTmpFiles(
  runtime: Awaited<ReturnType<typeof Runtime.create>>,
): Promise<string> {
  try {
    const names = (await runtime.readdir("/tmp"))
      .filter((name) => name.startsWith("apt.sqv"))
      .sort();
    if (names.length === 0) {
      return "none";
    }
    const entries = await Promise.all(names.map(async (name) => {
      const path = `/tmp/${name}`;
      try {
        const data = await runtime.readFile(path);
        const buffer = Buffer.from(data);
        return [
          `${path}:size=${data.length}`,
          `sha256=${createHash("sha256").update(buffer).digest("hex")}`,
          `head=${JSON.stringify(decoder.decode(data.slice(0, 256)))}`,
          `tail=${JSON.stringify(decoder.decode(data.slice(Math.max(0, data.length - 256))))}`,
        ].join(":");
      } catch (error) {
        const detail = error instanceof Error ? error.message : String(error);
        return `${path}:read-failed:${detail}`;
      }
    }));
    return entries.join("|");
  } catch (error) {
    const detail = error instanceof Error ? error.message : String(error);
    return `tmp-list-failed:${detail}`;
  }
}

async function waitForBridgeRequestOrExit(
  requests: readonly string[],
  proc: Awaited<ReturnType<Awaited<ReturnType<typeof Runtime.create>>["spawn"]>>,
  timeoutMs: number,
): Promise<void> {
  let timeout: ReturnType<typeof setTimeout> | null = null;
  try {
    await Promise.race([
      waitForBridgeRequest(requests, timeoutMs),
      proc.waitForExit().then((exitCode) => {
        throw new Error(`process exited before bridge request: exit=${exitCode} debug=${proc.debugInfo?.() ?? ""}`);
      }),
      new Promise<never>((_, reject) => {
        timeout = setTimeout(() => {
          reject(new Error(`timeout waiting for bridge request or process exit after ${timeoutMs}ms`));
        }, timeoutMs + 500);
      }),
    ]);
  } finally {
    if (timeout) {
      clearTimeout(timeout);
    }
  }
}

async function prepareMethodRuntime(networkBridge: RuntimeNetworkBridge): Promise<{
  runtime: Awaited<ReturnType<typeof Runtime.create>>;
  methodElf: Uint8Array;
} | null> {
  const wasmBytes = readKernelWasm();
  if (!wasmBytes) {
    return null;
  }
  if (!methodRoot || !existsSync(methodRoot)) {
    return logSkip(`missing apt method root: ${methodRoot || "set TIDEMARK_TEST_APT_METHOD_ROOT"}`);
  }
  if (!methodPayload || !existsSync(methodPayload)) {
    return logSkip(`missing apt method payload: ${methodPayload || "set TIDEMARK_TEST_APT_METHOD_PAYLOAD"}`);
  }

  const snapshot = buildAptMethodSnapshot();
  if (snapshot.skipReason) {
    return logSkip(snapshot.skipReason);
  }

  const runtime = await Runtime.create({
    wasmBytes,
    ramSize: 512 * 1024 * 1024,
    maxProcessMemoryPages: 32768,
    networkBridge,
  });

  try {
    await runtime.applyFsSnapshot(snapshot.entries ?? []);
    return {
      runtime,
      methodElf: new Uint8Array(readFileSync(resolve(methodRoot, "usr/lib/apt/methods/http"))),
    };
  } catch (error) {
    runtime.destroy();
    throw error;
  }
}

async function prepareSqvRuntime(): Promise<{
  runtime: Awaited<ReturnType<typeof Runtime.create>>;
  methodElf: Uint8Array;
} | null> {
  const wasmBytes = readKernelWasm();
  if (!wasmBytes) {
    return null;
  }

  const snapshot = buildAptSqvSnapshot();
  if (snapshot.skipReason) {
    return logSkip(snapshot.skipReason);
  }

  const runtime = await Runtime.create({
    wasmBytes,
    ramSize: 512 * 1024 * 1024,
    maxProcessMemoryPages: 32768,
  });

  try {
    await runtime.applyFsSnapshot(snapshot.entries ?? []);
    await runtime.writeFile(methodAcquireFilename, methodResponseBody);
    return {
      runtime,
      methodElf: new Uint8Array(readFileSync(resolve(sqvMethodRoot, "usr/lib/apt/methods/sqv"))),
    };
  } catch (error) {
    runtime.destroy();
    throw error;
  }
}

async function prepareHttpThenSqvRuntime(networkBridge: RuntimeNetworkBridge): Promise<{
  runtime: Awaited<ReturnType<typeof Runtime.create>>;
} | null> {
  const wasmBytes = readKernelWasm();
  if (!wasmBytes) {
    return null;
  }
  if (!methodPayload || !existsSync(methodPayload)) {
    return logSkip(`missing apt method payload: ${methodPayload || "set TIDEMARK_TEST_APT_METHOD_PAYLOAD"}`);
  }

  const httpSnapshot = buildAptMethodSnapshot();
  if (httpSnapshot.skipReason) {
    return logSkip(httpSnapshot.skipReason);
  }
  const sqvSnapshot = buildAptSqvSnapshot();
  if (sqvSnapshot.skipReason) {
    return logSkip(sqvSnapshot.skipReason);
  }

  const runtime = await Runtime.create({
    wasmBytes,
    ramSize: 512 * 1024 * 1024,
    maxProcessMemoryPages: 32768,
    networkBridge,
  });

  try {
    await runtime.applyFsSnapshot([
      ...(httpSnapshot.entries ?? []),
      ...(sqvSnapshot.entries ?? []),
    ]);
    await runtime.writeFile("/work/apt-method-payload.txt", new Uint8Array(readFileSync(methodPayload)));
    return { runtime };
  } catch (error) {
    runtime.destroy();
    throw error;
  }
}

async function prepareAptGetRuntime(networkBridge: RuntimeNetworkBridge): Promise<{
  runtime: Awaited<ReturnType<typeof Runtime.create>>;
  aptGetElf: Uint8Array;
} | null> {
  const wasmBytes = readKernelWasm();
  if (!wasmBytes) {
    return null;
  }

  const snapshot = buildAptGetSnapshot();
  if (snapshot.skipReason) {
    return logSkip(snapshot.skipReason);
  }
  traceApt(`prepare snapshot entries=${snapshot.entries?.length ?? 0}`);

  const runtime = await Runtime.create({
    wasmBytes,
    ramSize: 512 * 1024 * 1024,
    maxProcessMemoryPages: 32768,
    networkBridge,
  });
  traceApt("prepare runtime-created");

  try {
    await runtime.applyFsSnapshot(snapshot.entries ?? []);
    traceApt("prepare snapshot-applied");
    return {
      runtime,
      aptGetElf: new Uint8Array(readFileSync(resolve(aptRoot, "usr/bin/apt-get"))),
    };
  } catch (error) {
    runtime.destroy();
    throw error;
  }
}

test("apt method http local gate: captured stdin reaches generic network bridge", async () => {
  const guestRequests: string[] = [];
  const prepared = await prepareMethodRuntime(createMethodNetworkBridge(guestRequests));
  if (!prepared) {
    return;
  }
  const { runtime, methodElf } = prepared;

  let output = "";
  let completedByCleanExit = false;
  try {
    const proc = await runtime.spawn(
      methodElf,
      [methodArgv0],
      methodEnv,
      { stdin: "pipe" },
    );
    proc.onStdout((data) => {
      output += decoder.decode(data);
    });
    proc.writeStdin(new Uint8Array(readFileSync(methodPayload)));

    await waitForBridgeRequest(
      guestRequests,
      30_000,
    ).catch(async (error) => {
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error([
        detail,
        `payload=${basename(methodPayload)}`,
        `guestRequests=${JSON.stringify(guestRequests)}`,
        `debug=${await runtimeDebugSummary(runtime)}`,
      ].join("\n"));
    });

    expect(
      guestRequests.join("\n"),
      [
        "apt http method did not write an HTTP request to the bridge",
        `output=${JSON.stringify(output)}`,
        `guestRequests=${JSON.stringify(guestRequests)}`,
      ].join("\n"),
    ).toContain("GET http://deb.debian.org/debian/dists/trixie/InRelease");
  } finally {
    runtime.destroy();
  }
}, 45_000);

test("apt method http local gate: capabilities stdout is delivered while method stays blocked", async () => {
  const guestRequests: string[] = [];
  const prepared = await prepareMethodRuntime(createMethodNetworkBridge(guestRequests));
  if (!prepared) {
    return;
  }
  const { runtime, methodElf } = prepared;

  let output = "";
  let completedByCleanExit = false;
  try {
    const proc = await runtime.spawn(
      methodElf,
      [methodArgv0],
      methodEnv,
      { stdin: "pipe" },
    );
    proc.onStdout((data) => {
      output += decoder.decode(data);
    });
    proc.writeStdin(new Uint8Array(readFileSync(methodPayload)));

    await waitForBridgeRequest(guestRequests, 30_000);

    const started = Date.now();
    while (Date.now() - started < 2_000 && !output.includes("100 Capabilities")) {
      await new Promise((resolveDone) => setTimeout(resolveDone, 25));
    }

    expect(
      output,
      [
        "apt http method stdout was not delivered to host callback after bridge request",
        `guestRequests=${JSON.stringify(guestRequests)}`,
        `debug=${await runtimeDebugSummary(runtime)}`,
      ].join("\n"),
    ).toContain("100 Capabilities");
  } finally {
    runtime.destroy();
  }
}, 45_000);

test("apt method http local gate: async bridge response is delivered to method", async () => {
  const guestRequests: string[] = [];
  const bridgeEvents: string[] = [];
  const prepared = await prepareMethodRuntime(createAsyncMethodNetworkBridge(guestRequests, bridgeEvents));
  if (!prepared) {
    return;
  }
  const { runtime, methodElf } = prepared;

  let output = "";
  try {
    const proc = await runtime.spawn(
      methodElf,
      [methodArgv0],
      methodEnv,
      { stdin: "pipe" },
    );
    proc.onStdout((data) => {
      output += decoder.decode(data);
    });
    proc.writeStdin(new Uint8Array(readFileSync(methodPayload)));

    await waitForBridgeRequest(guestRequests, 30_000);
    await waitForOutput(() => output, "201 URI Done", asyncDoneTimeoutMs).catch(async (error) => {
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error([
        detail,
        `guestRequests=${JSON.stringify(guestRequests)}`,
        `bridgeEvents=${JSON.stringify(bridgeEvents)}`,
        `guestFile=${await describeGuestFile(runtime, methodAcquireFilename)}`,
        `output=${JSON.stringify(output)}`,
        `debug=${await runtimeDebugSummary(runtime)}`,
        `lastFailure=${JSON.stringify(runtime.debugLastFailureSnapshot?.() ?? null, null, 2)}`,
      ].join("\n"));
    });

    const saved = await runtime.readFile(methodAcquireFilename);
    expect(
      saved,
      [
        "apt method did not save the response body byte-for-byte",
        `guestFile=${await describeGuestFile(runtime, methodAcquireFilename)}`,
        `output=${JSON.stringify(output)}`,
        `debug=${await runtimeDebugSummary(runtime)}`,
      ].join("\n"),
    ).toEqual(methodResponseBody);
  } finally {
    runtime.destroy();
  }
}, Math.max(60_000, asyncDoneTimeoutMs + 10_000));

test("apt method http local gate: guest fork/exec pipe payload reaches generic network bridge", async () => {
  const guestRequests: string[] = [];
  const prepared = await prepareMethodRuntime(createMethodNetworkBridge(guestRequests));
  if (!prepared) {
    return;
  }
  if (!methodPayload || !existsSync(methodPayload)) {
    prepared.runtime.destroy();
    return logSkip(`missing apt method payload: ${methodPayload || "set TIDEMARK_TEST_APT_METHOD_PAYLOAD"}`);
  }
  const probeElf = buildForkExecMethodProbe();
  if (!probeElf) {
    prepared.runtime.destroy();
    return;
  }
  const { runtime } = prepared;

  let output = "";
  let completedByCleanExit = false;
  try {
    await runtime.writeFile("/work/apt-method-payload.txt", new Uint8Array(readFileSync(methodPayload)));
    const proc = await runtime.spawn(
      probeElf,
      ["/work/apt-method-fork-exec-probe"],
      ["LC_ALL=C", "LANG=C"],
      { cwd: "/work", stdin: "closed" },
    );
    proc.onStdout((data) => {
      output += decoder.decode(data);
    });

    await waitForBridgeRequestOrExit(guestRequests, proc, 45_000).catch(async (error) => {
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error([
        detail,
        `output=${JSON.stringify(output)}`,
        `guestRequests=${JSON.stringify(guestRequests)}`,
        `debug=${await runtimeDebugSummary(runtime)}`,
      ].join("\n"));
    });

    expect(guestRequests.join("\n")).toContain("GET http://deb.debian.org/debian/dists/trixie/InRelease");
  } finally {
    runtime.destroy();
  }
}, Math.max(60_000, asyncDoneTimeoutMs + 10_000));

test("apt method http local gate: guest fork/exec pipe output reaches URI Done", async () => {
  const guestRequests: string[] = [];
  const bridgeEvents: string[] = [];
  const prepared = await prepareMethodRuntime(createAsyncMethodNetworkBridge(guestRequests, bridgeEvents));
  if (!prepared) {
    return;
  }
  if (!methodPayload || !existsSync(methodPayload)) {
    prepared.runtime.destroy();
    return logSkip(`missing apt method payload: ${methodPayload || "set TIDEMARK_TEST_APT_METHOD_PAYLOAD"}`);
  }
  const probeElf = buildForkExecMethodProbe();
  if (!probeElf) {
    prepared.runtime.destroy();
    return;
  }
  const { runtime } = prepared;

  let output = "";
  try {
    await runtime.writeFile("/work/apt-method-payload.txt", new Uint8Array(readFileSync(methodPayload)));
    const proc = await runtime.spawn(
      probeElf,
      ["/work/apt-method-fork-exec-probe"],
      ["LC_ALL=C", "LANG=C"],
      { cwd: "/work", stdin: "closed" },
    );
    proc.onStdout((data) => {
      output += decoder.decode(data);
    });

    await waitForOutput(() => output, "201 URI Done", asyncDoneTimeoutMs).catch(async (error) => {
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error([
        detail,
        `guestRequests=${JSON.stringify(guestRequests)}`,
        `bridgeEvents=${JSON.stringify(bridgeEvents)}`,
        `guestFile=${await describeGuestFile(runtime, methodAcquireFilename)}`,
        `output=${JSON.stringify(output)}`,
        `debug=${await runtimeDebugSummary(runtime)}`,
        `lastFailure=${JSON.stringify(runtime.debugLastFailureSnapshot?.() ?? null, null, 2)}`,
      ].join("\n"));
    });

    expect(output).toContain("201 URI Done");
    expect(
      await runtime.readFile(methodAcquireFilename),
      [
        "apt fork/exec HTTP method did not preserve response body byte-for-byte",
        `guestFile=${await describeGuestFile(runtime, methodAcquireFilename)}`,
        `output=${JSON.stringify(output)}`,
        `debug=${await runtimeDebugSummary(runtime)}`,
      ].join("\n"),
    ).toEqual(methodResponseBody);
  } finally {
    runtime.destroy();
  }
}, Math.max(60_000, asyncDoneTimeoutMs + 10_000));

test("apt method parent wait local gate: stdout EOF follows URI Done after stdin closes", async () => {
  const guestRequests: string[] = [];
  const bridgeEvents: string[] = [];
  const prepared = await prepareMethodRuntime(createAsyncMethodNetworkBridge(guestRequests, bridgeEvents));
  if (!prepared) {
    return;
  }
  if (!methodPayload || !existsSync(methodPayload)) {
    prepared.runtime.destroy();
    return logSkip(`missing apt method payload: ${methodPayload || "set TIDEMARK_TEST_APT_METHOD_PAYLOAD"}`);
  }
  const probeElf = buildForkExecMethodEofWaitProbe();
  if (!probeElf) {
    prepared.runtime.destroy();
    return;
  }
  const { runtime } = prepared;

  let output = "";
  try {
    await runtime.writeFile("/work/apt-method-payload.txt", new Uint8Array(readFileSync(methodPayload)));
    const proc = await runtime.spawn(
      probeElf,
      ["/work/apt-method-eof-wait-probe"],
      ["LC_ALL=C", "LANG=C"],
      { cwd: "/work", stdin: "closed" },
    );
    proc.onStdout((data) => {
      output += decoder.decode(data);
    });

    await waitForOutput(() => output, "parent:stdout-eof", asyncDoneTimeoutMs).catch(async (error) => {
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error([
        detail,
        `guestRequests=${JSON.stringify(guestRequests)}`,
        `bridgeEvents=${JSON.stringify(bridgeEvents)}`,
        `guestFile=${await describeGuestFile(runtime, methodAcquireFilename)}`,
        `output=${JSON.stringify(output)}`,
        `debug=${await runtimeDebugSummary(runtime)}`,
        `lastFailure=${JSON.stringify(runtime.debugLastFailureSnapshot?.() ?? null, null, 2)}`,
      ].join("\n"));
    });

    const exitCode = await Promise.race([
      proc.waitForExit(),
      new Promise<number>((_, reject) => {
        setTimeout(() => reject(new Error("timeout waiting for parent EOF/wait probe exit")), asyncDoneTimeoutMs);
      }),
    ]).catch(async (error) => {
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error([
        detail,
        `guestRequests=${JSON.stringify(guestRequests)}`,
        `bridgeEvents=${JSON.stringify(bridgeEvents)}`,
        `guestFile=${await describeGuestFile(runtime, methodAcquireFilename)}`,
        `output=${JSON.stringify(output)}`,
        `debug=${await runtimeDebugSummary(runtime)}`,
        `lastFailure=${JSON.stringify(runtime.debugLastFailureSnapshot?.() ?? null, null, 2)}`,
      ].join("\n"));
    });

    expect(output).toContain("201 URI Done");
    expect(output).toContain("parent:http-done");
    expect(output).toContain("parent:stdout-eof");
    expect(output).toMatch(/parent:child-exit=(0|100)/);
    expect(exitCode).toBe(0);
    expect(await runtime.readFile(methodAcquireFilename)).toEqual(methodResponseBody);
  } finally {
    runtime.destroy();
  }
}, Math.max(90_000, asyncDoneTimeoutMs + 20_000));

test("apt-get update local gate: parent acquire loop exits after in-memory HTTP response", async () => {
  const guestRequests: string[] = [];
  const bridgeEvents: string[] = [];
  const prepared = await prepareAptGetRuntime(createAsyncMethodNetworkBridge(guestRequests, bridgeEvents));
  if (!prepared) {
    return;
  }
  const { runtime, aptGetElf } = prepared;

  let output = "";
  try {
    const proc = await runtime.spawn(
      aptGetElf,
      [
        "/usr/bin/apt-get",
        "update",
        "-o", "Acquire::http::Timeout=10",
        "-o", "Acquire::http::Proxy=http://127.0.0.1:8080",
        "-o", "Acquire::AllowInsecureRepositories=true",
        "-o", "Acquire::AllowDowngradeToInsecureRepositories=true",
        "-o", "Debug::NoLocking=true",
        "-o", "APT::Get::List-Cleanup=false",
      ],
      [
        "PATH=/usr/bin:/bin",
        "HOME=/root",
        "TMPDIR=/tmp",
        "LC_ALL=C",
        "LANG=C",
        "http_proxy=http://127.0.0.1:8080",
        "https_proxy=http://127.0.0.1:8080",
        "HTTP_PROXY=http://127.0.0.1:8080",
        "HTTPS_PROXY=http://127.0.0.1:8080",
      ],
      { cwd: "/work", stdin: "closed" },
    );
    traceApt("apt-get spawned");
    proc.onStdout((data) => {
      output += decoder.decode(data);
      traceApt(`apt-get stdout bytes=${data.length} total=${output.length}`);
    });

    await waitForBridgeRequestOrExit(guestRequests, proc, asyncDoneTimeoutMs).catch(async (error) => {
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error([
        detail,
        `guestRequests=${JSON.stringify(guestRequests)}`,
        `bridgeEvents=${JSON.stringify(bridgeEvents)}`,
        `output=${JSON.stringify(output)}`,
        `partialInRelease=${await describeGuestFile(runtime, methodAcquireFilename)}`,
        `debug=${await runtimeDebugSummary(runtime)}`,
        `lastFailure=${JSON.stringify(runtime.debugLastFailureSnapshot?.() ?? null, null, 2)}`,
      ].join("\n"));
    });
    traceApt(`apt-get bridge-seen requests=${guestRequests.length}`);

    const exitCode = await Promise.race([
      proc.waitForExit(),
      new Promise<number>((_, reject) => {
        setTimeout(() => {
          traceApt(`apt-get wait-timeout output=${JSON.stringify(output)}`);
          traceApt(`apt-get wait-timeout snapshot=${runtime.debugSnapshot?.() ?? ""}`);
          traceApt(`apt-get wait-timeout lastFailure=${JSON.stringify(runtime.debugLastFailureSnapshot?.() ?? null)}`);
          reject(new Error("timeout waiting for apt-get update exit"));
        }, aptGetUpdateExitTimeoutMs);
      }),
    ]).catch(async (error) => {
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error([
        detail,
        `guestRequests=${JSON.stringify(guestRequests)}`,
        `bridgeEvents=${JSON.stringify(bridgeEvents)}`,
        `output=${JSON.stringify(output)}`,
        `partialInRelease=${await describeGuestFile(runtime, methodAcquireFilename)}`,
        `finalInRelease=${await describeGuestFile(runtime, "/var/lib/apt/lists/deb.debian.org_debian_dists_trixie_InRelease")}`,
        `debug=${await runtimeDebugSummary(runtime)}`,
        `lastFailure=${JSON.stringify(runtime.debugLastFailureSnapshot?.() ?? null, null, 2)}`,
      ].join("\n"));
    });

    expect(
      guestRequests.join("\n"),
      [
        "apt-get did not drive the http method through the runtime bridge",
        `bridgeEvents=${JSON.stringify(bridgeEvents)}`,
        `output=${JSON.stringify(output)}`,
      ].join("\n"),
    ).toContain("GET http://deb.debian.org/debian/dists/trixie/InRelease");
    expect([0, 100]).toContain(exitCode);
  } finally {
    runtime.destroy();
  }
}, Math.max(90_000, asyncDoneTimeoutMs + 20_000));

test("apt method sqv local gate: clearsigned InRelease split returns a method response", async () => {
  const prepared = await prepareSqvRuntime();
  if (!prepared) {
    return;
  }
  const useDirectSqvMethod = process.env.TIDEMARK_TEST_APT_SQV_DIRECT_METHOD === "1";
  const probeElf = useDirectSqvMethod ? null : buildSqvExecProbe();
  if (!useDirectSqvMethod && !probeElf) {
    prepared.runtime.destroy();
    return;
  }
  const { runtime, methodElf } = prepared;

  let output = "";
  try {
    const proc = await runtime.spawn(
      useDirectSqvMethod ? methodElf : probeElf!,
      [useDirectSqvMethod ? "/usr/lib/apt/methods/sqv" : "/work/apt-sqv-exec-probe"],
      ["LC_ALL=C", "LANG=C"],
      { stdin: process.env.TIDEMARK_TEST_APT_SQV_STDIN_CLOSED === "1" ? "closed" : "pipe" },
    );
    proc.onStdout((data) => {
      output += decoder.decode(data);
    });
    await waitForOutput(() => output, "100 Capabilities", asyncDoneTimeoutMs).catch(async (error) => {
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error([
        detail,
        `guestFile=${await describeGuestFile(runtime, methodAcquireFilename)}`,
        `sqvTmp=${await describeGuestSqvTmpFiles(runtime)}`,
        `output=${JSON.stringify(output)}`,
        `debug=${await runtimeDebugSummary(runtime)}`,
        `lastFailure=${JSON.stringify(runtime.debugLastFailureSnapshot?.() ?? null, null, 2)}`,
      ].join("\n"));
    });
    proc.writeStdin(new TextEncoder().encode([
      "601 Configuration",
      "Config-Item: APT::Architecture=riscv64",
      "Config-Item: APT::Architectures::=riscv64",
      "Config-Item: Dir::Etc::TrustedParts=/etc/apt/trusted.gpg.d",
      "Config-Item: Dir::State::lists=/var/lib/apt/lists",
      "",
      "600 URI Acquire",
      `URI: sqv:${methodAcquireFilename}`,
      `Filename: ${methodAcquireFilename}`,
      "Signed-By: /etc/apt/trusted.gpg.d/tidemark-test.gpg",
      "",
      "",
    ].join("\n")));

    await Promise.race([
      waitForOutput(() => output, "URI Failure", asyncDoneTimeoutMs),
      proc.waitForExit().then((exitCode) => {
        throw new Error(`sqv method exited before returning a URI response: exit=${exitCode}`);
      }),
    ]).catch(async (error) => {
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error([
        detail,
        `guestFile=${await describeGuestFile(runtime, methodAcquireFilename)}`,
        `sqvTmp=${await describeGuestSqvTmpFiles(runtime)}`,
        `output=${JSON.stringify(output)}`,
        `debug=${await runtimeDebugSummary(runtime)}`,
        `lastFailure=${JSON.stringify(runtime.debugLastFailureSnapshot?.() ?? null, null, 2)}`,
      ].join("\n"));
    });

    expect(
      output,
      [
        "apt sqv method did not return a URI response after reading the clearsigned InRelease",
        `guestFile=${await describeGuestFile(runtime, methodAcquireFilename)}`,
        `debug=${await runtimeDebugSummary(runtime)}`,
      ].join("\n"),
    ).toMatch(/400 URI Failure/);
  } finally {
    runtime.destroy();
  }
}, Math.max(60_000, asyncDoneTimeoutMs + 10_000));

test("apt method sqv local gate: guest fork/exec pipe reaches URI response", async () => {
  const prepared = await prepareSqvRuntime();
  if (!prepared) {
    return;
  }
  const probeElf = buildSqvForkExecProbe();
  if (!probeElf) {
    prepared.runtime.destroy();
    return;
  }
  const { runtime } = prepared;

  let output = "";
  try {
    const proc = await runtime.spawn(
      probeElf,
      ["/work/apt-sqv-fork-exec-probe"],
      ["LC_ALL=C", "LANG=C"],
      { cwd: "/work", stdin: "closed" },
    );
    proc.onStdout((data) => {
      output += decoder.decode(data);
    });

    await Promise.race([
      waitForOutput(() => output, "sqv-fork-probe:done", sqvForkProbeDoneTimeoutMs),
      proc.waitForExit().then(async (exitCode) => {
        await waitForOutput(() => output, "sqv-fork-probe:done", 1000).catch(() => {});
        if (!output.includes("sqv-fork-probe:done")) {
          throw new Error(`sqv fork/exec probe exited before returning a URI response: exit=${exitCode}`);
        }
      }),
    ]).catch(async (error) => {
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error([
        detail,
        `guestFile=${await describeGuestFile(runtime, methodAcquireFilename)}`,
        `sqvTmp=${await describeGuestSqvTmpFiles(runtime)}`,
        `output=${JSON.stringify(output)}`,
        `debug=${await runtimeDebugSummary(runtime)}`,
        `lastFailure=${JSON.stringify(runtime.debugLastFailureSnapshot?.() ?? null, null, 2)}`,
      ].join("\n"));
    });

    expect(
      output,
      [
        "apt sqv fork/exec probe did not observe a URI response over its child stdout pipe",
        `guestFile=${await describeGuestFile(runtime, methodAcquireFilename)}`,
        `sqvTmp=${await describeGuestSqvTmpFiles(runtime)}`,
        `debug=${await runtimeDebugSummary(runtime)}`,
      ].join("\n"),
    ).toMatch(/400 URI Failure/);
  } finally {
    runtime.destroy();
  }
}, Math.max(75_000, sqvForkProbeDoneTimeoutMs + 15_000));

test("apt method gpgv local gate: guest fork/exec pipe reaches URI response", async () => {
  const prepared = await prepareSqvRuntime();
  if (!prepared) {
    return;
  }
  const probeElf = buildGpgvForkExecProbe();
  if (!probeElf) {
    prepared.runtime.destroy();
    return;
  }
  const { runtime } = prepared;

  let output = "";
  try {
    const proc = await runtime.spawn(
      probeElf,
      ["/work/apt-gpgv-fork-exec-probe"],
      ["LC_ALL=C", "LANG=C"],
      { cwd: "/work", stdin: "closed" },
    );
    proc.onStdout((data) => {
      output += decoder.decode(data);
    });

    await Promise.race([
      waitForOutput(() => output, "gpgv-fork-probe:done", sqvForkProbeDoneTimeoutMs),
      proc.waitForExit().then(async (exitCode) => {
        await waitForOutput(() => output, "gpgv-fork-probe:done", 1000).catch(() => {});
        if (!output.includes("gpgv-fork-probe:done")) {
          throw new Error(`gpgv fork/exec probe exited before returning a URI response: exit=${exitCode}`);
        }
      }),
    ]).catch(async (error) => {
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error([
        detail,
        `guestFile=${await describeGuestFile(runtime, methodAcquireFilename)}`,
        `output=${JSON.stringify(output)}`,
        `debug=${await runtimeDebugSummary(runtime)}`,
        `lastFailure=${JSON.stringify(runtime.debugLastFailureSnapshot?.() ?? null, null, 2)}`,
      ].join("\n"));
    });

    expect(
      output,
      [
        "apt gpgv fork/exec probe did not observe a URI response over its child stdout pipe",
        `guestFile=${await describeGuestFile(runtime, methodAcquireFilename)}`,
        `debug=${await runtimeDebugSummary(runtime)}`,
      ].join("\n"),
    ).toMatch(/400 URI Failure/);
  } finally {
    runtime.destroy();
  }
}, Math.max(75_000, sqvForkProbeDoneTimeoutMs + 15_000));

test("apt method sqv local gate: guest fork pipe preserves libapt preflight", async () => {
  const prepared = await prepareSqvRuntime();
  if (!prepared) {
    return;
  }
  const probeElf = buildSqvForkPreflightProbe();
  if (!probeElf) {
    prepared.runtime.destroy();
    return;
  }
  const { runtime } = prepared;

  let output = "";
  try {
    const proc = await runtime.spawn(
      probeElf,
      ["/work/apt-sqv-fork-preflight-probe"],
      ["LC_ALL=C", "LANG=C"],
      { cwd: "/work", stdin: "closed" },
    );
    proc.onStdout((data) => {
      output += decoder.decode(data);
    });

    await Promise.race([
      waitForOutput(() => output, "sqv-preflight-probe:done", 15_000),
      proc.waitForExit().then(async (exitCode) => {
        await waitForOutput(() => output, "sqv-preflight-probe:done", 1000).catch(() => {});
        if (!output.includes("sqv-preflight-probe:done")) {
          throw new Error(`sqv fork/preflight probe exited before preserving child libapt read: exit=${exitCode}`);
        }
      }),
    ]).catch(async (error) => {
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error([
        detail,
        `guestFile=${await describeGuestFile(runtime, methodAcquireFilename)}`,
        `output=${JSON.stringify(output)}`,
        `debug=${await runtimeDebugSummary(runtime)}`,
        `lastFailure=${JSON.stringify(runtime.debugLastFailureSnapshot?.() ?? null, null, 2)}`,
      ].join("\n"));
    });

    expect(output).toContain("parent-preflight-libapt:elf");
    expect(output).toContain("preflight-libapt:elf");
    expect(output).toContain("sqv-preflight-probe:done");
  } finally {
    runtime.destroy();
  }
}, 45_000);

test("apt method fd lifecycle local gate: fork/exec socket open fcntl", async () => {
  const wasmBytes = readKernelWasm();
  if (!wasmBytes) {
    return;
  }
  const probeElf = buildForkExecSocketOpenFcntlProbe();
  if (!probeElf) {
    return;
  }

  const guestRequests: string[] = [];
  const bridgeEvents: string[] = [];
  const runtime = await Runtime.create({
    wasmBytes,
    ramSize: 256 * 1024 * 1024,
    maxProcessMemoryPages: 16384,
    networkBridge: createAsyncMethodNetworkBridge(guestRequests, bridgeEvents),
  });

  let output = "";
  try {
    await runtime.applyFsSnapshot([
      { kind: "dir", path: "/work", mode: 0o755 },
      { kind: "dir", path: "/var", mode: 0o755 },
      { kind: "dir", path: "/var/lib", mode: 0o755 },
      { kind: "dir", path: "/var/lib/apt", mode: 0o755 },
      { kind: "dir", path: "/var/lib/apt/lists", mode: 0o755 },
      { kind: "dir", path: "/var/lib/apt/lists/partial", mode: 0o755 },
    ]);
    await runtime.writeFile("/work/fd-lifecycle-probe", probeElf, 0o755);
    const proc = await runtime.spawn(
      probeElf,
      ["/work/fd-lifecycle-probe"],
      ["LC_ALL=C", "LANG=C"],
      { cwd: "/work", stdin: "closed" },
    );
    proc.onStdout((data) => {
      output += decoder.decode(data);
    });

    const exitCode = await Promise.race([
      proc.waitForExit(),
      waitForOutput(() => output, "child:read-error:", 75_000).then(() => {
        throw new Error("fd lifecycle probe reported a socket read error before exit");
      }),
      new Promise<never>((_resolve, reject) => {
        setTimeout(() => reject(new Error("timeout waiting for fd lifecycle probe")), 75_000);
      }),
    ]).catch(async (error) => {
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error([
        detail,
        `guestRequests=${JSON.stringify(guestRequests)}`,
        `bridgeEvents=${JSON.stringify(bridgeEvents)}`,
        `guestFile=${await describeGuestFile(runtime, methodAcquireFilename)}`,
        `sqvTmp=${await describeGuestSqvTmpFiles(runtime)}`,
        `output=${JSON.stringify(output)}`,
        `debug=${await runtimeDebugSummary(runtime)}`,
        `lastFailure=${JSON.stringify(runtime.debugLastFailureSnapshot?.() ?? null, null, 2)}`,
      ].join("\n"));
    });

    expect(exitCode, [
      `fd lifecycle probe failed with exit=${exitCode}`,
      `guestRequests=${JSON.stringify(guestRequests)}`,
      `bridgeEvents=${JSON.stringify(bridgeEvents)}`,
      `guestFile=${await describeGuestFile(runtime, methodAcquireFilename)}`,
      `output=${JSON.stringify(output)}`,
      `debug=${await runtimeDebugSummary(runtime)}`,
    ].join("\n")).toBe(0);
    expect(output).toContain("child:done");
    const savedBody = await runtime.readFile(methodAcquireFilename);
    expect(
      savedBody,
      [
        "fd lifecycle probe did not preserve the response body byte-for-byte",
        `firstMismatch=${firstMismatchOffset(savedBody, methodResponseBody)}`,
        `guestRequests=${JSON.stringify(guestRequests)}`,
        `bridgeEvents=${JSON.stringify(bridgeEvents)}`,
        `guestFile=${await describeGuestFile(runtime, methodAcquireFilename)}`,
        `output=${JSON.stringify(output)}`,
        `debug=${await runtimeDebugSummary(runtime)}`,
      ].join("\n"),
    ).toEqual(methodResponseBody);
  } finally {
    runtime.destroy();
  }
}, 90_000);

test("apt method chain local gate: parent runs http then sqv over fork/exec pipes", async () => {
  const guestRequests: string[] = [];
  const bridgeEvents: string[] = [];
  const prepared = await prepareHttpThenSqvRuntime(createAsyncMethodNetworkBridge(guestRequests, bridgeEvents));
  if (!prepared) {
    return;
  }
  const probeElf = buildHttpThenSqvForkExecProbe();
  if (!probeElf) {
    prepared.runtime.destroy();
    return;
  }
  const { runtime } = prepared;

  let output = "";
  try {
    const proc = await runtime.spawn(
      probeElf,
      ["/work/apt-http-sqv-fork-exec-probe"],
      ["LC_ALL=C", "LANG=C"],
      { cwd: "/work", stdin: "closed" },
    );
    proc.onStdout((data) => {
      output += decoder.decode(data);
    });

    await waitForOutput(() => output, "chain:http-done", asyncDoneTimeoutMs).catch(async (error) => {
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error([
        detail,
        `guestRequests=${JSON.stringify(guestRequests)}`,
        `bridgeEvents=${JSON.stringify(bridgeEvents)}`,
        `guestFile=${await describeGuestFile(runtime, methodAcquireFilename)}`,
        `output=${JSON.stringify(output)}`,
        `debug=${await runtimeDebugSummary(runtime)}`,
        `lastFailure=${JSON.stringify(runtime.debugLastFailureSnapshot?.() ?? null, null, 2)}`,
      ].join("\n"));
    });

    await Promise.race([
      waitForOutput(() => output, "chain:sqv-done", chainProbeDoneTimeoutMs),
      proc.waitForExit().then(async (exitCode) => {
        await waitForOutput(() => output, "chain:sqv-done", 1000).catch(() => {});
        if (exitCode === 0) {
          completedByCleanExit = true;
          return;
        }
        if (!output.includes("chain:sqv-done")) {
          throw new Error(`http/sqv chain probe exited before completion: exit=${exitCode}`);
        }
      }),
    ]).catch(async (error) => {
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error([
        detail,
        `guestRequests=${JSON.stringify(guestRequests)}`,
        `bridgeEvents=${JSON.stringify(bridgeEvents)}`,
        `guestFile=${await describeGuestFile(runtime, methodAcquireFilename)}`,
        `sqvTmp=${await describeGuestSqvTmpFiles(runtime)}`,
        `output=${JSON.stringify(output)}`,
        `debug=${await runtimeDebugSummary(runtime)}`,
        `lastFailure=${JSON.stringify(runtime.debugLastFailureSnapshot?.() ?? null, null, 2)}`,
      ].join("\n"));
    });

    expect(output).toContain("chain:http-done");
    expect(
      output.includes("chain:sqv-done") || completedByCleanExit,
      [
        "apt http/sqv chain did not observe sqv completion",
        `output=${JSON.stringify(output)}`,
        `debug=${await runtimeDebugSummary(runtime)}`,
      ].join("\n"),
    ).toBe(true);
    expect(
      await runtime.readFile(methodAcquireFilename),
      [
        "apt http/sqv chain did not preserve the fetched InRelease byte-for-byte",
        `guestFile=${await describeGuestFile(runtime, methodAcquireFilename)}`,
        `sqvTmp=${await describeGuestSqvTmpFiles(runtime)}`,
        `output=${JSON.stringify(output)}`,
        `debug=${await runtimeDebugSummary(runtime)}`,
      ].join("\n"),
    ).toEqual(methodResponseBody);
  } finally {
    runtime.destroy();
  }
}, Math.max(90_000, chainProbeDoneTimeoutMs + 15_000));
