import { resolve } from "node:path";
import type { DynamicStartupEntry } from "../../../support/dynamic-startup-harness.js";

const runtimeDir = process.env.TIDEMARK_TEST_PHP_RUNTIME_DIR ?? "";

export const phpDynamicStartupEntry: DynamicStartupEntry = {
  id: "php-version",
  targetClass: "PHP runtime",
  runnerKind: "direct-exec",
  hostExecutablePath: resolve(runtimeDir, "usr/bin/php8.4"),
  guestExecutablePath: "/usr/bin/php8.4",
  argv: ["/usr/bin/php8.4", "--version"],
  env: [],
  timeoutMs: 5_000,
  ramSize: 256 * 1024 * 1024,
  snapshotSources: [
    { kind: "tree", hostPath: resolve(runtimeDir, "usr/bin"), guestPath: "/usr/bin" },
    { kind: "tree", hostPath: resolve(runtimeDir, "usr/lib/php"), guestPath: "/usr/lib/php" },
    { kind: "tree", hostPath: resolve(runtimeDir, "usr/lib/riscv64-linux-gnu"), guestPath: "/usr/lib/riscv64-linux-gnu" },
    {
      kind: "file",
      hostPath: resolve(runtimeDir, "usr/lib/ld-linux-riscv64-lp64d.so.1"),
      guestPath: "/lib/ld-linux-riscv64-lp64d.so.1",
      mode: 0o755,
    },
  ],
  expectedSubstrings: ["PHP 8.4.16"],
};
