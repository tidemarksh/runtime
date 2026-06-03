import { resolve } from "node:path";
import type { DynamicStartupEntry } from "../../../support/dynamic-startup-harness.js";

const runtimeDir = process.env.TIDEMARK_TEST_PERL_RUNTIME_DIR ?? "";

export const perlDynamicStartupEntry: DynamicStartupEntry = {
  id: "perl-version",
  targetClass: "Perl runtime",
  runnerKind: "direct-exec",
  hostExecutablePath: resolve(runtimeDir, "usr/bin/perl"),
  guestExecutablePath: "/usr/bin/perl",
  argv: ["/usr/bin/perl", "-v"],
  env: [],
  timeoutMs: 5_000,
  guestMemoryPolicyLimitBytes: 256 * 1024 * 1024,
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
  ],
  expectedSubstrings: ["perl 5, version 40, subversion 1"],
};
