import { resolve } from "node:path";
import type {
  ArchiveOrchestratorEntry,
  ArchiveOrchestratorStep,
} from "../../../support/archive-orchestrator-harness.js";

const toolsetDir = process.env.TIDEMARK_TEST_DEVTOOLS_TOOLSET_DIR ?? "";
const glibcSysrootDir = process.env.TIDEMARK_TEST_GLIBC_SYSROOT_DIR ?? "";

const DEVTOOLS_RUNTIME_LIBS = [
  "ld-linux-riscv64-lp64d.so.1",
  "libc.so.6",
  "libdl.so.2",
  "libpthread.so.0",
  "libm.so.6",
  "librt.so.1",
  "libutil.so.1",
  "libgcc_s.so.1",
] as const;

function baseEnv() {
  return ["PATH=/usr/bin:/bin", "HOME=/tmp", "LD_LIBRARY_PATH=/usr/lib/riscv64-linux-gnu:/lib"] as const;
}

function devtoolsSnapshotSources(extra: readonly import("../../../support/snapshot-sources.js").SnapshotSource[]) {
  return [
    { kind: "tree" as const, hostPath: resolve(toolsetDir, "usr/lib/riscv64-linux-gnu"), guestPath: "/usr/lib/riscv64-linux-gnu" },
    ...DEVTOOLS_RUNTIME_LIBS.map((name) => ({
      kind: "file" as const,
      hostPath: resolve(glibcSysrootDir, name),
      guestPath: `/lib/${name}`,
      mode: 0o755,
    })),
    ...extra,
  ];
}

function commandStep(input: {
  id: string;
  executableName: string;
  argv: readonly string[];
  expectedOutputSubstrings?: readonly string[];
  cwd?: string;
  timeoutMs?: number;
}): ArchiveOrchestratorStep {
  return {
    id: input.id,
    hostExecutablePath: resolve(toolsetDir, `usr/bin/${input.executableName}`),
    guestExecutablePath: `/usr/bin/${input.executableName}`,
    argv: input.argv,
    env: baseEnv(),
    cwd: input.cwd,
    timeoutMs: input.timeoutMs ?? 15_000,
    expectedOutputSubstrings: input.expectedOutputSubstrings,
  };
}

export const gzipRoundtripArchiveEntry: ArchiveOrchestratorEntry = {
  id: "gzip-roundtrip",
  targetClass: "major developer tools",
  workflowKind: "archive / compression roundtrip",
  guestMemoryPolicyLimitBytes: 512 * 1024 * 1024,
  snapshotSources: devtoolsSnapshotSources([
    { kind: "file", hostPath: resolve(toolsetDir, "usr/bin/gzip"), guestPath: "/usr/bin/gzip", mode: 0o755 },
    {
      kind: "inlineFile",
      guestPath: "/work/input.txt",
      data: "gzip-evidence\n",
    },
  ]),
  steps: [
    commandStep({
      id: "gzip-compress",
      executableName: "gzip",
      argv: ["/usr/bin/gzip", "-kf", "/work/input.txt"],
    }),
    commandStep({
      id: "gzip-decompress",
      executableName: "gzip",
      argv: ["/usr/bin/gzip", "-dc", "/work/input.txt.gz"],
      expectedOutputSubstrings: ["gzip-evidence"],
    }),
  ],
  artifactChecks: [
    {
      path: "/work/input.txt.gz",
      minSize: 1,
      expectedBinaryPrefix: [0x1f, 0x8b],
    },
  ],
};

export const xzRoundtripArchiveEntry: ArchiveOrchestratorEntry = {
  id: "xz-roundtrip",
  targetClass: "major developer tools",
  workflowKind: "archive / compression roundtrip",
  guestMemoryPolicyLimitBytes: 512 * 1024 * 1024,
  snapshotSources: devtoolsSnapshotSources([
    { kind: "file", hostPath: resolve(toolsetDir, "usr/bin/xz"), guestPath: "/usr/bin/xz", mode: 0o755 },
    {
      kind: "inlineFile",
      guestPath: "/work/input.txt",
      data: "xz-evidence\n",
    },
  ]),
  steps: [
    commandStep({
      id: "xz-compress",
      executableName: "xz",
      argv: ["/usr/bin/xz", "-kf", "/work/input.txt"],
      timeoutMs: 20_000,
    }),
    commandStep({
      id: "xz-decompress",
      executableName: "xz",
      argv: ["/usr/bin/xz", "-dc", "/work/input.txt.xz"],
      expectedOutputSubstrings: ["xz-evidence"],
      timeoutMs: 20_000,
    }),
  ],
  artifactChecks: [
    {
      path: "/work/input.txt.xz",
      minSize: 1,
      expectedBinaryPrefix: [0xfd, 0x37, 0x7a, 0x58, 0x5a, 0x00],
    },
  ],
};

export const cmakeScriptArchiveEntry: ArchiveOrchestratorEntry = {
  id: "cmake-script-orchestrator",
  targetClass: "major developer tools",
  workflowKind: "build orchestrator / generator",
  guestMemoryPolicyLimitBytes: 512 * 1024 * 1024,
  snapshotSources: devtoolsSnapshotSources([
    { kind: "file", hostPath: resolve(toolsetDir, "usr/bin/cmake"), guestPath: "/usr/bin/cmake", mode: 0o755 },
    { kind: "tree", hostPath: resolve(toolsetDir, "usr/share/cmake"), guestPath: "/usr/share/cmake" },
    { kind: "tree", hostPath: resolve(toolsetDir, "usr/share/cmake-4.3"), guestPath: "/usr/share/cmake-4.3" },
    {
      kind: "inlineFile",
      guestPath: "/work/script.cmake",
      data: [
        'message(STATUS "cmake-evidence")',
        'file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/out.txt" "cmake-out\\n")',
        "",
      ].join("\n"),
    },
  ]),
  steps: [
    commandStep({
      id: "cmake-script",
      executableName: "cmake",
      argv: ["/usr/bin/cmake", "-P", "/work/script.cmake"],
      cwd: "/work",
      expectedOutputSubstrings: ["cmake-evidence"],
      timeoutMs: 20_000,
    }),
  ],
  artifactChecks: [
    {
      path: "/work/out.txt",
      minSize: 1,
      expectedTextSubstrings: ["cmake-out"],
    },
  ],
};
