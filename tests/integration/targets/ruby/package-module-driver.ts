import { resolve } from "node:path";
import type { PackageModuleDriverEntry } from "../../../support/package-module-driver-harness.js";

const runtimeDir = process.env.TIDEMARK_TEST_RUBY_RUNTIME_DIR ?? "";

export const rubyGemPackageModuleEntry: PackageModuleDriverEntry = {
  id: "ruby-gem-version",
  targetClass: "Ruby runtime",
  workflowKind: "package driver",
  hostExecutablePath: resolve(runtimeDir, "usr/bin/ruby3.3"),
  guestExecutablePath: "/usr/bin/ruby3.3",
  argv: ["/usr/bin/ruby3.3", "/usr/bin/gem", "--version"],
  env: [],
  timeoutMs: 10_000,
  guestMemoryPolicyLimitBytes: 256 * 1024 * 1024,
  snapshotSources: [
    { kind: "tree", hostPath: resolve(runtimeDir, "usr/bin"), guestPath: "/usr/bin" },
    { kind: "tree", hostPath: resolve(runtimeDir, "usr/lib/riscv64-linux-gnu"), guestPath: "/usr/lib/riscv64-linux-gnu" },
    { kind: "tree", hostPath: resolve(runtimeDir, "usr/lib/ruby"), guestPath: "/usr/lib/ruby" },
    { kind: "tree", hostPath: resolve(runtimeDir, "usr/share/rubygems-integration"), guestPath: "/usr/share/rubygems-integration" },
    {
      kind: "file",
      hostPath: resolve(runtimeDir, "usr/lib/ld-linux-riscv64-lp64d.so.1"),
      guestPath: "/lib/ld-linux-riscv64-lp64d.so.1",
      mode: 0o755,
    },
  ],
  expectedDriverSubstrings: ["3.6.7"],
};
