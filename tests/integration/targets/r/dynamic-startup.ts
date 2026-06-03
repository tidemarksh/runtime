import { resolve } from "node:path";
import type { DynamicStartupEntry } from "../../../support/dynamic-startup-harness.js";

const runtimeDir = process.env.TIDEMARK_TEST_R_RUNTIME_DIR ?? "";
const rhomeOverlayDir = process.env.TIDEMARK_TEST_R_HOME_OVERLAY_DIR ?? "";

export const rDynamicStartupEntry: DynamicStartupEntry = {
  id: "r-version-clean",
  targetClass: "R runtime",
  runnerKind: "direct-exec",
  hostExecutablePath: resolve(runtimeDir, "usr/lib/R/bin/exec/R"),
  guestExecutablePath: "/usr/lib/R/bin/exec/R",
  argv: ["/usr/lib/R/bin/exec/R", "--version"],
  env: ["R_HOME=/rhome"],
  timeoutMs: 5_000,
  guestMemoryPolicyLimitBytes: 512 * 1024 * 1024,
  snapshotSources: [
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
    { kind: "tree", hostPath: rhomeOverlayDir, guestPath: "/rhome" },
  ],
  expectedSubstrings: ["R version 4.6.0 alpha"],
  forbiddenSubstrings: ["cannot find system Renviron"],
};
