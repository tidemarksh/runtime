import { resolve } from "node:path";
import type { ScriptRuntimeEntry } from "../../../support/script-runtime-harness.js";

const busyboxBinary = process.env.TIDEMARK_TEST_BUSYBOX_BINARY ?? "";
const runtimeDir = process.env.TIDEMARK_TEST_R_RUNTIME_DIR ?? "";
const rhomeOverlayDir = process.env.TIDEMARK_TEST_R_HOME_OVERLAY_DIR ?? "";

export const rScriptRuntimeEntry: ScriptRuntimeEntry = {
  id: "r-script",
  targetClass: "R runtime",
  runnerKind: "direct-exec",
  hostExecutablePath: resolve(runtimeDir, "usr/bin/Rscript"),
  guestExecutablePath: "/usr/bin/Rscript",
  argv: ["/usr/bin/Rscript", "/work/script.R"],
  env: [
    "R_HOME=/rhome",
    "R_SHARE_DIR=/usr/share/R/share",
    "R_INCLUDE_DIR=/usr/share/R/include",
    "R_DOC_DIR=/usr/share/R/doc",
  ],
  timeoutMs: 10_000,
  guestMemoryPolicyLimitBytes: 512 * 1024 * 1024,
  snapshotSources: [
    {
      kind: "fileCopies",
      hostPath: busyboxBinary,
      guestPaths: ["/bin/busybox", "/bin/sh", "/bin/bash", "/bin/uname", "/bin/rm", "/usr/bin/which"],
      mode: 0o755,
    },
    { kind: "tree", hostPath: resolve(runtimeDir, "usr/bin"), guestPath: "/usr/bin" },
    { kind: "tree", hostPath: resolve(runtimeDir, "usr/lib/R"), guestPath: "/usr/lib/R" },
    {
      kind: "file",
      hostPath: resolve(runtimeDir, "usr/lib/libR.so"),
      guestPath: "/usr/lib/libR.so",
      mode: 0o755,
    },
    {
      kind: "file",
      hostPath: resolve(runtimeDir, "usr/lib/riscv64-linux-gnu/blas/libblas.so.3"),
      guestPath: "/usr/lib/riscv64-linux-gnu/libblas.so.3",
      mode: 0o755,
    },
    {
      kind: "file",
      hostPath: resolve(runtimeDir, "usr/lib/riscv64-linux-gnu/lapack/liblapack.so.3"),
      guestPath: "/usr/lib/riscv64-linux-gnu/liblapack.so.3",
      mode: 0o755,
    },
    { kind: "tree", hostPath: resolve(runtimeDir, "usr/lib/riscv64-linux-gnu"), guestPath: "/usr/lib/riscv64-linux-gnu" },
    { kind: "tree", hostPath: resolve(runtimeDir, "etc"), guestPath: "/etc" },
    {
      kind: "file",
      hostPath: resolve(runtimeDir, "usr/lib/ld-linux-riscv64-lp64d.so.1"),
      guestPath: "/lib/ld-linux-riscv64-lp64d.so.1",
      mode: 0o755,
    },
    { kind: "tree", hostPath: resolve(runtimeDir, "usr/share/R"), guestPath: "/usr/share/R" },
    { kind: "tree", hostPath: rhomeOverlayDir, guestPath: "/rhome" },
    {
      kind: "inlineFile",
      guestPath: "/work/script.R",
      data: 'cat("r-script-evidence\\n")\n',
    },
  ],
  expectedSubstrings: ["r-script-evidence"],
  forbiddenSubstrings: ["cannot find system Renviron"],
};
