import type { ToolchainExecEntry } from "../../../support/toolchain-exec-harness.js";
import {
  GO_BINARY_PATH,
  GO_BUILD_EXPERIMENT,
  GO_EXEC_CAPTURE_HELPER_PATH,
  GO_RUNTIME_GUEST_MEMORY_POLICY_LIMIT_BYTES,
  GO_TOOLCHAIN_DIR,
} from "./support.js";

function goHelperExecEntry(
  id: string,
  mode: string,
  expectedDriverSubstrings: readonly string[],
): ToolchainExecEntry {
  return {
    id,
    targetClass: "Go toolchain",
    workflowKind: "guest os/exec child tool exec",
    hostExecutablePath: GO_EXEC_CAPTURE_HELPER_PATH,
    guestExecutablePath: "/work/exec-capture-helper",
    argv: ["/work/exec-capture-helper", mode],
    env: [
      "GOROOT=/usr/local/go",
      "PATH=/usr/local/go/bin:/bin",
      "HOME=/root",
      "TMPDIR=/tmp",
    ],
    timeoutMs: 15_000,
    guestMemoryPolicyLimitBytes: 512 * 1024 * 1024,
    snapshotSources: [
      { kind: "tree", hostPath: `${GO_TOOLCHAIN_DIR}/bin`, guestPath: "/usr/local/go/bin" },
      { kind: "tree", hostPath: `${GO_TOOLCHAIN_DIR}/pkg`, guestPath: "/usr/local/go/pkg" },
      { kind: "tree", hostPath: `${GO_TOOLCHAIN_DIR}/src`, guestPath: "/usr/local/go/src" },
      { kind: "file", hostPath: GO_EXEC_CAPTURE_HELPER_PATH, guestPath: "/work/exec-capture-helper", mode: 0o755 },
    ],
    expectedDriverSubstrings,
    artifactChecks: [
      { path: "/tmp/goarch.a", minSize: 1 },
    ],
  };
}

export const goCompileVersionToolchainEntry: ToolchainExecEntry = {
  id: "go-compile-version",
  targetClass: "Go toolchain",
  workflowKind: "direct child-tool exec",
  hostExecutablePath: `${GO_TOOLCHAIN_DIR}/pkg/tool/linux_riscv64/compile`,
  guestExecutablePath: "/usr/local/go/pkg/tool/linux_riscv64/compile",
  argv: ["/usr/local/go/pkg/tool/linux_riscv64/compile", "-V=full"],
  env: [
    "GOROOT=/usr/local/go",
    "PATH=/usr/local/go/bin:/bin",
    "HOME=/root",
    "GOMAXPROCS=1",
    "GOGC=off",
    "GOTELEMETRY=off",
    "GOTELEMETRYDIR=/dev/null",
  ],
  timeoutMs: 10_000,
  guestMemoryPolicyLimitBytes: GO_RUNTIME_GUEST_MEMORY_POLICY_LIMIT_BYTES,
  snapshotSources: [
    { kind: "tree", hostPath: `${GO_TOOLCHAIN_DIR}/pkg/tool/linux_riscv64`, guestPath: "/usr/local/go/pkg/tool/linux_riscv64" },
    { kind: "inlineFile", guestPath: "/usr/local/go/go.env", data: "GOTOOLCHAIN=local\n" },
    { kind: "dir", guestPath: "/root/.config/go/telemetry" },
    { kind: "inlineFile", guestPath: "/root/.config/go/telemetry/mode", data: "off 2000-01-01\n" },
  ],
  expectedDriverSubstrings: ["compile version go1.26.1"],
  artifactChecks: [],
};

export const goBuildToolchainEntry: ToolchainExecEntry = {
  id: "go-build",
  targetClass: "Go toolchain",
  workflowKind: "build-driver + child tool exec",
  hostExecutablePath: GO_BINARY_PATH,
  guestExecutablePath: "/usr/local/go/bin/go",
  argv: ["/usr/local/go/bin/go", "build", "-o", "/work/hello", "/work/hello.go"],
  env: [
    "GOROOT=/usr/local/go",
    "GOPATH=/tmp/gopath",
    "GOCACHE=/tmp/gocache",
    "PATH=/usr/local/go/bin:/bin",
    "GOMAXPROCS=1",
    "GOTELEMETRY=off",
    "GOTELEMETRYDIR=/dev/null",
    "GONOSUMDB=*",
    "GOFLAGS=-mod=mod",
    "HOME=/root",
    "GOGC=off",
    ...(GO_BUILD_EXPERIMENT ? [`GOEXPERIMENT=${GO_BUILD_EXPERIMENT}`] : []),
  ],
  timeoutMs: 30_000,
  guestMemoryPolicyLimitBytes: GO_RUNTIME_GUEST_MEMORY_POLICY_LIMIT_BYTES,
  snapshotSources: [
    { kind: "tree", hostPath: `${GO_TOOLCHAIN_DIR}/bin`, guestPath: "/usr/local/go/bin" },
    { kind: "tree", hostPath: `${GO_TOOLCHAIN_DIR}/pkg`, guestPath: "/usr/local/go/pkg" },
    { kind: "tree", hostPath: `${GO_TOOLCHAIN_DIR}/src`, guestPath: "/usr/local/go/src" },
    { kind: "tree", hostPath: `${GO_TOOLCHAIN_DIR}/lib`, guestPath: "/usr/local/go/lib" },
    { kind: "inlineFile", guestPath: "/usr/local/go/go.env", data: "GOTOOLCHAIN=local\n" },
    { kind: "dir", guestPath: "/root/.config/go/telemetry" },
    { kind: "inlineFile", guestPath: "/root/.config/go/telemetry/mode", data: "off 2000-01-01\n" },
    { kind: "dir", guestPath: "/tmp/gopath" },
    { kind: "dir", guestPath: "/tmp/gocache" },
    {
      kind: "inlineFile",
      guestPath: "/work/hello.go",
      data: 'package main\nfunc main() { println("hello from go build") }\n',
    },
  ],
  artifactChecks: [{ path: "/work/hello" }],
  runChecks: [{
    path: "/work/hello",
    timeoutMs: 10_000,
    expectedSubstrings: ["hello from go build"],
  }],
};

export const goToolCompileVersionToolchainEntry: ToolchainExecEntry = {
  id: "go-tool-compile-version",
  targetClass: "Go toolchain",
  workflowKind: "driver-mediated child tool exec",
  hostExecutablePath: GO_BINARY_PATH,
  guestExecutablePath: "/usr/local/go/bin/go",
  argv: ["/usr/local/go/bin/go", "tool", "compile", "-V=full"],
  env: [
    "GOROOT=/usr/local/go",
    "PATH=/usr/local/go/bin:/bin",
    "HOME=/root",
    "GOTELEMETRY=off",
    "GOTELEMETRYDIR=/dev/null",
  ],
  timeoutMs: 15_000,
  guestMemoryPolicyLimitBytes: GO_RUNTIME_GUEST_MEMORY_POLICY_LIMIT_BYTES,
  snapshotSources: [
    { kind: "tree", hostPath: `${GO_TOOLCHAIN_DIR}/bin`, guestPath: "/usr/local/go/bin" },
    { kind: "tree", hostPath: `${GO_TOOLCHAIN_DIR}/pkg`, guestPath: "/usr/local/go/pkg" },
    { kind: "tree", hostPath: `${GO_TOOLCHAIN_DIR}/lib`, guestPath: "/usr/local/go/lib" },
    { kind: "inlineFile", guestPath: "/usr/local/go/go.env", data: "GOTOOLCHAIN=local\n" },
    { kind: "dir", guestPath: "/root/.config/go/telemetry" },
    { kind: "inlineFile", guestPath: "/root/.config/go/telemetry/mode", data: "off 2000-01-01\n" },
  ],
  expectedDriverSubstrings: ["compile version go1.26.1"],
  artifactChecks: [],
};

export const goToolCompileVersionSingleThreadToolchainEntry: ToolchainExecEntry = {
  ...goToolCompileVersionToolchainEntry,
  id: "go-tool-compile-version-gomaxprocs1",
  env: [
    ...goToolCompileVersionToolchainEntry.env,
    "GOMAXPROCS=1",
  ],
};

export const goHelperCombinedGoarchToolchainEntry = goHelperExecEntry(
  "go-helper-combined-goarch",
  "combined-goarch",
  [
    "mode=combined-goarch",
    "combined_output_err=<nil>",
  ],
);

export const goHelperTwoStepGoarchToolchainEntry = goHelperExecEntry(
  "go-helper-two-step-goarch",
  "two-step-goarch",
  [
    "mode=two-step-goarch",
    "compile_err=<nil>",
    "buildid_err=<nil>",
  ],
);

export const goHelperTwoStepGoarchExplicitEnvToolchainEntry = goHelperExecEntry(
  "go-helper-two-step-goarch-explicit-env",
  "two-step-goarch-explicit-env",
  [
    "mode=two-step-goarch-explicit-env",
    "compile_err=<nil>",
    "buildid_err=<nil>",
  ],
);
