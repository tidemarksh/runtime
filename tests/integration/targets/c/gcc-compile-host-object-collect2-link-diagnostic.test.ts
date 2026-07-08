import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
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
const hostRiscvGcc = process.env.TIDEMARK_TEST_HOST_RISCV_GCC ?? "riscv64-linux-gnu-gcc";
const helloSource = '#include <stdio.h>\nint main(void) { puts("c-evidence"); return 0; }\n';

function buildHostHelloObject(): Uint8Array {
  const tmp = mkdtempSync(join(tmpdir(), "tidemark-c-gcc-compile-host-object-"));
  const sourcePath = join(tmp, "hello.c");
  const objectPath = join(tmp, "hello.o");
  try {
    writeFileSync(sourcePath, helloSource);
    execFileSync(hostRiscvGcc, ["-c", "-O2", "-o", objectPath, sourcePath], { stdio: "pipe" });
    return new Uint8Array(readFileSync(objectPath));
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
}

function equalBytes(left: Uint8Array, right: Uint8Array): boolean {
  if (left.byteLength !== right.byteLength) {
    return false;
  }
  for (let index = 0; index < left.byteLength; index += 1) {
    if (left[index] !== right[index]) {
      return false;
    }
  }
  return true;
}

const gccCompileEntry: ToolchainExecEntry = {
  id: "c-gcc-compile-stage-before-host-object-diagnostic",
  targetClass: "C toolchain",
  workflowKind: "guest gcc compile before host object diagnostic",
  hostExecutablePath: resolve(cToolchainDir, "usr/bin/riscv64-linux-gnu-gcc-16"),
  guestExecutablePath: "/usr/bin/riscv64-linux-gnu-gcc-16",
  argv: ["/usr/bin/riscv64-linux-gnu-gcc-16", "-c", "-O2", "-o", "/work/hello.o", "/work/hello.c"],
  env: [
    "PATH=/usr/bin:/bin",
    "HOME=/tmp",
    "LD_LIBRARY_PATH=/usr/lib/riscv64-linux-gnu:/usr/lib:/lib",
  ],
  timeoutMs: 60_000,
  guestMemoryPolicyLimitBytes: 768 * 1024 * 1024,
  hostFileBackingMode: "host-file",
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

const collect2LinkEntry: ToolchainExecEntry = {
  id: "c-host-object-after-gcc-compile-collect2-link-diagnostic",
  targetClass: "C toolchain",
  workflowKind: "host object after guest compile collect2 link diagnostic",
  hostExecutablePath: resolve(cToolchainDir, "usr/libexec/gcc/riscv64-linux-gnu/16/collect2"),
  guestExecutablePath: "/usr/libexec/gcc/riscv64-linux-gnu/16/collect2",
  argv: [
    "/usr/libexec/gcc/riscv64-linux-gnu/16/collect2",
    "-plugin",
    "/usr/libexec/gcc/riscv64-linux-gnu/16/liblto_plugin.so",
    "-plugin-opt=/usr/libexec/gcc/riscv64-linux-gnu/16/lto-wrapper",
    "-plugin-opt=-fresolution=/tmp/cc.res",
    "-plugin-opt=-pass-through=-lgcc",
    "-plugin-opt=-pass-through=-lgcc_s",
    "-plugin-opt=-pass-through=-lc",
    "-plugin-opt=-pass-through=-lgcc",
    "-plugin-opt=-pass-through=-lgcc_s",
    "--build-id",
    "--eh-frame-hdr",
    "--hash-style=gnu",
    "-melf64lriscv",
    "-X",
    "-dynamic-linker",
    "/lib/ld-linux-riscv64-lp64d.so.1",
    "-pie",
    "-o",
    "/work/hello",
    "/usr/lib/riscv64-linux-gnu/Scrt1.o",
    "/usr/lib/gcc/riscv64-linux-gnu/16/crti.o",
    "/usr/lib/gcc/riscv64-linux-gnu/16/crtbeginS.o",
    "-L/usr/lib/gcc/riscv64-linux-gnu/16",
    "-L/usr/lib/riscv64-linux-gnu",
    "/work/hello.o",
    "-lgcc",
    "--push-state",
    "--as-needed",
    "-lgcc_s",
    "--pop-state",
    "-lc",
    "-lgcc",
    "--push-state",
    "--as-needed",
    "-lgcc_s",
    "--pop-state",
    "/usr/lib/gcc/riscv64-linux-gnu/16/crtendS.o",
    "/usr/lib/gcc/riscv64-linux-gnu/16/crtn.o",
  ],
  env: [
    "PATH=/usr/bin:/bin",
    "HOME=/tmp",
    "LD_LIBRARY_PATH=/usr/lib/riscv64-linux-gnu:/usr/lib:/lib",
    "COLLECT_GCC=/usr/bin/riscv64-linux-gnu-gcc-16",
    "COMPILER_PATH=/usr/libexec/gcc/riscv64-linux-gnu/16/:/usr/libexec/gcc/riscv64-linux-gnu/:/usr/lib/gcc/riscv64-linux-gnu/16/:/usr/lib/gcc/riscv64-linux-gnu/:/usr/bin/",
    "LIBRARY_PATH=/usr/lib/gcc/riscv64-linux-gnu/16/:/usr/lib/riscv64-linux-gnu/:/usr/lib/",
  ],
  timeoutMs: 120_000,
  guestMemoryPolicyLimitBytes: 768 * 1024 * 1024,
  snapshotSources: [],
  artifactChecks: [{ path: "/work/hello", minSize: 1 }],
};

test("toolchain exec local gate: c-gcc-compile-host-object-collect2-link-diagnostic", async () => {
  const prepared = await prepareToolchainExecRuntime(gccCompileEntry);
  if (!prepared) {
    return;
  }
  const { runtime, executableBytes: gccBytes } = prepared;
  try {
    const compileResult = await runToolchainExecEntry(runtime, gccCompileEntry, gccBytes);
    if (compileResult.exitCode !== 0) {
      throw new Error(`compile stage failed\n${compileResult.output}\n${compileResult.debugInfo ?? ""}`);
    }
    expect(await validateToolchainArtifacts(runtime, gccCompileEntry)).toEqual([]);

    const guestObject = await runtime.fs.readFile("/work/hello.o");
    const hostObject = buildHostHelloObject();
    const guestPageCache = await runtime.debugPageCachePath("/work/hello.o");
    await runtime.fs.writeFile("/work/hello.o", hostObject);
    const replacedObject = await runtime.fs.readFile("/work/hello.o");
    const replacedPageCache = await runtime.debugPageCachePath("/work/hello.o");
    if (!equalBytes(replacedObject, hostObject)) {
      throw new Error([
        "host object replacement readback mismatch",
        `guestObjectBytes=${guestObject.byteLength}`,
        `hostObjectBytes=${hostObject.byteLength}`,
        `replacedObjectBytes=${replacedObject.byteLength}`,
        `guestPageCache=${JSON.stringify(guestPageCache)}`,
        `replacedPageCache=${JSON.stringify(replacedPageCache)}`,
      ].join("\n"));
    }

    const collect2Bytes = new Uint8Array(readFileSync(collect2LinkEntry.hostExecutablePath));
    const linkResult = await runToolchainExecEntry(runtime, collect2LinkEntry, collect2Bytes);
    if (linkResult.exitCode !== 0) {
      const summary = await runtimeDebugSummary(runtime);
      throw new Error([
        "collect2 link stage failed after host object replacement",
        `exitCode=${linkResult.exitCode}`,
        `guestObjectBytes=${guestObject.byteLength}`,
        `hostObjectBytes=${hostObject.byteLength}`,
        `replacedObjectBytes=${replacedObject.byteLength}`,
        `guestPageCache=${JSON.stringify(guestPageCache)}`,
        `replacedPageCache=${JSON.stringify(replacedPageCache)}`,
        "output:",
        linkResult.output,
        linkResult.debugInfo ? `debug:\n${linkResult.debugInfo}` : null,
        summary ? `runtime:\n${summary}` : null,
      ].filter((part): part is string => part !== null).join("\n"));
    }
    expect(await validateToolchainArtifacts(runtime, collect2LinkEntry)).toEqual([]);
  } finally {
    runtime.destroy();
  }
}, 60_000 + scaledToolchainTimeoutMs(gccCompileEntry.timeoutMs) + scaledToolchainTimeoutMs(collect2LinkEntry.timeoutMs));
