import { resolve } from "node:path";
import type { PackageModuleDriverEntry } from "../../support/package-module-driver-harness.js";

const runtimeDir = process.env.TIDEMARK_TEST_PERL_RUNTIME_DIR ?? "";

export const perlProvePackageModuleEntry: PackageModuleDriverEntry = {
  id: "perl-prove-basic",
  targetClass: "Perl runtime",
  workflowKind: "test driver",
  hostExecutablePath: resolve(runtimeDir, "usr/bin/perl"),
  guestExecutablePath: "/usr/bin/perl",
  argv: ["/usr/bin/perl", "/usr/bin/prove", "/work/basic.t"],
  env: [],
  timeoutMs: 10_000,
  ramSize: 256 * 1024 * 1024,
  snapshotSources: [
    { kind: "tree", hostPath: resolve(runtimeDir, "usr/bin"), guestPath: "/usr/bin" },
    { kind: "tree", hostPath: resolve(runtimeDir, "usr/lib/riscv64-linux-gnu"), guestPath: "/usr/lib/riscv64-linux-gnu" },
    { kind: "tree", hostPath: resolve(runtimeDir, "usr/lib/riscv64-linux-gnu/perl"), guestPath: "/usr/lib/riscv64-linux-gnu/perl" },
    { kind: "tree", hostPath: resolve(runtimeDir, "usr/share/perl"), guestPath: "/usr/share/perl" },
    {
      kind: "file",
      hostPath: resolve(runtimeDir, "usr/lib/ld-linux-riscv64-lp64d.so.1"),
      guestPath: "/lib/ld-linux-riscv64-lp64d.so.1",
      mode: 0o755,
    },
    {
      kind: "file",
      hostPath: resolve(process.cwd(), "../artifacts/reference-linux/perl/prove/basic.t"),
      guestPath: "/work/basic.t",
      mode: 0o644,
    },
  ],
  expectedDriverSubstrings: ["basic.t .. ok", "Result: PASS"],
};
