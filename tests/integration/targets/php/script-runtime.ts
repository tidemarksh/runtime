import { resolve } from "node:path";
import type { ScriptRuntimeEntry } from "../../../support/script-runtime-harness.js";

const runtimeDir = process.env.TIDEMARK_TEST_PHP_RUNTIME_DIR ?? "";

export const phpScriptRuntimeEntry: ScriptRuntimeEntry = {
  id: "php-script",
  targetClass: "PHP runtime",
  runnerKind: "direct-exec",
  hostExecutablePath: resolve(runtimeDir, "usr/bin/php8.4"),
  guestExecutablePath: "/usr/bin/php8.4",
  argv: ["/usr/bin/php8.4", "/work/script.php"],
  env: [],
  timeoutMs: 10_000,
  guestMemoryPolicyLimitBytes: 256 * 1024 * 1024,
  hostFileBackingMode: "host-file",
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
    {
      kind: "inlineFile",
      guestPath: "/work/script.php",
      data: [
        "<?php",
        'echo json_encode(["lang" => "php", "n" => 42]), PHP_EOL;',
        "",
      ].join("\n"),
    },
  ],
  expectedSubstrings: ['{"lang":"php","n":42}'],
};
