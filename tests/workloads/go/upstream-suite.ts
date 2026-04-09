import type { ToolchainExecEntry } from "../../support/toolchain-exec-harness.js";
import { BUSYBOX_BINARY_PATH, GO_BINARY_PATH, GO_TOOLCHAIN_DIR } from "./support.js";

function goUpstreamSuiteEntry(
  id: string,
  args: readonly string[],
  timeoutMs: number,
  expectedDriverSubstrings: readonly string[] = [],
): ToolchainExecEntry {
  return {
    id,
    targetClass: "Go toolchain",
    workflowKind: "upstream suite invocation",
    hostExecutablePath: GO_BINARY_PATH,
    guestExecutablePath: "/usr/local/go/bin/go",
    argv: ["/usr/local/go/bin/go", ...args],
    env: [
      "GOROOT=/usr/local/go",
      "PATH=/usr/local/go/bin:/bin",
      "HOME=/root",
      "TMPDIR=/tmp",
      "GOCACHE=/tmp/gocache",
      "GO_TEST_SHORT=true",
      "GOMAXPROCS=1",
      "GOTELEMETRY=off",
      "GOTELEMETRYDIR=/dev/null",
    ],
    cwd: "/usr/local/go/src",
    timeoutMs,
    ramSize: 1024 * 1024 * 1024,
    snapshotSources: [
      { kind: "tree", hostPath: `${GO_TOOLCHAIN_DIR}/bin`, guestPath: "/usr/local/go/bin" },
      { kind: "tree", hostPath: `${GO_TOOLCHAIN_DIR}/pkg`, guestPath: "/usr/local/go/pkg" },
      { kind: "tree", hostPath: `${GO_TOOLCHAIN_DIR}/src`, guestPath: "/usr/local/go/src" },
      { kind: "tree", hostPath: `${GO_TOOLCHAIN_DIR}/lib`, guestPath: "/usr/local/go/lib" },
      { kind: "inlineFile", guestPath: "/usr/local/go/go.env", data: "GOTOOLCHAIN=local\n" },
      { kind: "dir", guestPath: "/tmp/gocache" },
      { kind: "dir", guestPath: "/root/.config/go/telemetry" },
      { kind: "inlineFile", guestPath: "/root/.config/go/telemetry/mode", data: "off 2000-01-01\n" },
      ...(BUSYBOX_BINARY_PATH
        ? [{ kind: "fileCopies" as const, hostPath: BUSYBOX_BINARY_PATH, guestPaths: ["/bin/busybox", "/bin/sh"], mode: 0o755 }]
        : []),
    ],
    expectedDriverSubstrings,
    artifactChecks: [],
  };
}

export const goRuntimeSuiteEntry = goUpstreamSuiteEntry(
  "go-upstream-runtime",
  ["test", "runtime/..."],
  120_000,
  ["ok\t"],
);

export const goOsSuiteEntry = goUpstreamSuiteEntry(
  "go-upstream-os",
  ["test", "os/..."],
  120_000,
  ["ok\tos"],
);

export const goOsExecSuiteEntry = goUpstreamSuiteEntry(
  "go-upstream-os-exec",
  ["test", "os/exec"],
  60_000,
  ["ok\tos/exec"],
);

export const goCmdGoSuiteEntry = goUpstreamSuiteEntry(
  "go-upstream-cmd-go",
  ["test", "cmd/go"],
  120_000,
  ["ok\tcmd/go"],
);
