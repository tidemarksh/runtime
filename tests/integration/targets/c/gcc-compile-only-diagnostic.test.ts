import { resolve } from "node:path";
import { expect, test } from "vite-plus/test";
import {
  prepareToolchainExecRuntime,
  runToolchainExecEntry,
  scaledToolchainTimeoutMs,
  validateToolchainArtifacts,
  type ToolchainExecEntry,
} from "../../../support/toolchain-exec-harness.js";
import { runtimeDebugSummary } from "../../../support/runtime-workload.js";
import { cToolchainSnapshotSources } from "./toolchain-exec.js";

const cToolchainDir = process.env.TIDEMARK_TEST_C_TOOLCHAIN_DIR ?? "";
const helloSource = '#include <stdio.h>\nint main(void) { puts("c-evidence"); return 0; }\n';

const gccCompileEntry: ToolchainExecEntry = {
  id: "c-gcc-compile-only-diagnostic",
  targetClass: "C toolchain",
  workflowKind: "guest gcc compile only diagnostic",
  hostExecutablePath: resolve(cToolchainDir, "usr/bin/riscv64-linux-gnu-gcc-16"),
  guestExecutablePath: "/usr/bin/riscv64-linux-gnu-gcc-16",
  argv: ["/usr/bin/riscv64-linux-gnu-gcc-16", "-c", "-O2", "-o", "/work/hello.o", "/work/hello.c"],
  env: [
    "PATH=/usr/bin:/bin",
    "HOME=/tmp",
    "LD_LIBRARY_PATH=/usr/lib/riscv64-linux-gnu:/usr/lib:/lib",
  ],
  timeoutMs: 20_000,
  guestMemoryPolicyLimitBytes: 768 * 1024 * 1024,
  snapshotSources: cToolchainSnapshotSources([
    {
      kind: "inlineFile",
      guestPath: "/tmp/cc.res",
      data: "",
    },
    {
      kind: "inlineFile",
      guestPath: "/work/hello.c",
      data: helloSource,
    },
  ]),
  artifactChecks: [{ path: "/work/hello.o", minSize: 1 }],
};

test("toolchain exec local gate: c-gcc-compile-only-diagnostic", async () => {
  const prepared = await prepareToolchainExecRuntime(gccCompileEntry);
  if (!prepared) {
    return;
  }
  const { runtime, executableBytes: gccBytes } = prepared;
  try {
    const compileResult = await runToolchainExecEntry(runtime, gccCompileEntry, gccBytes);
    if (compileResult.exitCode !== 0) {
      const summary = await runtimeDebugSummary(runtime);
      throw new Error([
        "compile stage failed",
        `exitCode=${compileResult.exitCode}`,
        "output:",
        compileResult.output,
        compileResult.debugInfo ? `debug:\n${compileResult.debugInfo}` : null,
        summary ? `runtime:\n${summary}` : null,
      ].filter((part): part is string => part !== null).join("\n"));
    }

    expect(await validateToolchainArtifacts(runtime, gccCompileEntry)).toEqual([]);
    const objectBytes = await runtime.fs.readFile("/work/hello.o");
    expect(objectBytes.byteLength).toBeGreaterThan(0);
  } finally {
    runtime.destroy();
  }
}, 60_000 + scaledToolchainTimeoutMs(gccCompileEntry.timeoutMs));
