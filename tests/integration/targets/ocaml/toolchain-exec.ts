import { resolve } from "node:path";
import type { SnapshotSource } from "../../../support/snapshot-sources.js";
import type { ToolchainExecEntry } from "../../../support/toolchain-exec-harness.js";

const ocamlToolchainDir = process.env.TIDEMARK_TEST_OCAML_TOOLCHAIN_DIR ?? "";

function ocamlSnapshotSources(extra: readonly SnapshotSource[]): SnapshotSource[] {
  return [
    { kind: "dir", guestPath: "/tmp", mode: 0o777 },
    { kind: "dir", guestPath: "/lib" },
    {
      kind: "file",
      hostPath: resolve(ocamlToolchainDir, "usr/lib/ld-linux-riscv64-lp64d.so.1"),
      guestPath: "/lib/ld-linux-riscv64-lp64d.so.1",
      mode: 0o755,
    },
    { kind: "tree", hostPath: resolve(ocamlToolchainDir, "usr"), guestPath: "/usr" },
    { kind: "tree", hostPath: resolve(ocamlToolchainDir, "etc"), guestPath: "/etc" },
    { kind: "tree", hostPath: resolve(ocamlToolchainDir, "var"), guestPath: "/var" },
    ...extra,
  ];
}

export const ocamlCompileToolchainEntry: ToolchainExecEntry = {
  id: "ocaml-compile",
  targetClass: "OCaml runtime / toolchain",
  workflowKind: "compiler",
  hostExecutablePath: resolve(ocamlToolchainDir, "usr/bin/ocamlc.opt"),
  guestExecutablePath: "/usr/bin/ocamlc",
  argv: ["/usr/bin/ocamlc", "-o", "/work/hello", "/work/hello.ml"],
  env: [
    "PATH=/usr/bin:/bin",
    "HOME=/tmp",
    "LD_LIBRARY_PATH=/usr/lib/riscv64-linux-gnu:/lib/riscv64-linux-gnu:/usr/lib:/lib",
  ],
  timeoutMs: 30_000,
  guestMemoryPolicyLimitBytes: 1024 * 1024 * 1024,
  artifactMaterializationBudgetBytes: 1536 * 1024 * 1024,
  hostFileBackingMode: "host-file",
  snapshotSources: ocamlSnapshotSources([
    {
      kind: "inlineFile",
      guestPath: "/work/hello.ml",
      data: "print_endline \"ocaml-evidence\"\n",
    },
  ]),
  artifactChecks: [{ path: "/work/hello" }],
};
