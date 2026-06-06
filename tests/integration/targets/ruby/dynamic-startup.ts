import { resolve } from "node:path";
import type { DynamicStartupEntry } from "../../../support/dynamic-startup-harness.js";

const runtimeDir = process.env.TIDEMARK_TEST_RUBY_RUNTIME_DIR ?? "";

export const rubyDynamicStartupEntry: DynamicStartupEntry = {
  id: "ruby-version",
  targetClass: "Ruby runtime",
  runnerKind: "direct-exec",
  hostExecutablePath: resolve(runtimeDir, "usr/bin/ruby3.3"),
  guestExecutablePath: "/usr/bin/ruby3.3",
  argv: ["/usr/bin/ruby3.3", "--version"],
  env: [],
  timeoutMs: 60_000,
  guestMemoryPolicyLimitBytes: 256 * 1024 * 1024,
  snapshotSources: [
    { kind: "tree", hostPath: resolve(runtimeDir, "usr/bin"), guestPath: "/usr/bin" },
    { kind: "tree", hostPath: resolve(runtimeDir, "usr/lib/riscv64-linux-gnu"), guestPath: "/usr/lib/riscv64-linux-gnu" },
    { kind: "tree", hostPath: resolve(runtimeDir, "usr/lib/ruby"), guestPath: "/usr/lib/ruby" },
    {
      kind: "file",
      hostPath: resolve(runtimeDir, "usr/lib/ld-linux-riscv64-lp64d.so.1"),
      guestPath: "/lib/ld-linux-riscv64-lp64d.so.1",
      mode: 0o755,
    },
  ],
  expectedSubstrings: ["ruby 3.3.8"],
};
