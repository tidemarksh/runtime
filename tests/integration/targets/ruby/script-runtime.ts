import { resolve } from "node:path";
import type { ScriptRuntimeEntry } from "../../../support/script-runtime-harness.js";

const runtimeDir = process.env.TIDEMARK_TEST_RUBY_RUNTIME_DIR ?? "";

export const rubyScriptRuntimeEntry: ScriptRuntimeEntry = {
  id: "ruby-script",
  targetClass: "Ruby runtime",
  runnerKind: "direct-exec",
  hostExecutablePath: resolve(runtimeDir, "usr/bin/ruby3.3"),
  guestExecutablePath: "/usr/bin/ruby3.3",
  argv: ["/usr/bin/ruby3.3", "/work/script.rb"],
  env: [],
  timeoutMs: 10_000,
  guestMemoryPolicyLimitBytes: 256 * 1024 * 1024,
  hostFileBackingMode: "host-file",
  snapshotSources: [
    { kind: "tree", hostPath: resolve(runtimeDir, "usr/bin"), guestPath: "/usr/bin" },
    { kind: "tree", hostPath: resolve(runtimeDir, "usr/lib/riscv64-linux-gnu"), guestPath: "/usr/lib/riscv64-linux-gnu" },
    { kind: "tree", hostPath: resolve(runtimeDir, "usr/lib/ruby/3.3.0"), guestPath: "/usr/lib/ruby/3.3.0" },
    { kind: "tree", hostPath: resolve(runtimeDir, "usr/lib/ruby/vendor_ruby"), guestPath: "/usr/lib/ruby/vendor_ruby" },
    {
      kind: "file",
      hostPath: resolve(runtimeDir, "usr/lib/ruby/gems/3.3.0/specifications/default/json-2.7.2.gemspec"),
      guestPath: "/usr/lib/ruby/gems/3.3.0/specifications/default/json-2.7.2.gemspec",
    },
    {
      kind: "file",
      hostPath: resolve(runtimeDir, "usr/lib/ld-linux-riscv64-lp64d.so.1"),
      guestPath: "/lib/ld-linux-riscv64-lp64d.so.1",
      mode: 0o755,
    },
    {
      kind: "inlineFile",
      guestPath: "/work/script.rb",
      data: [
        'require "json"',
        'puts JSON.generate({ "lang" => "ruby", "n" => 42 })',
        "",
      ].join("\n"),
    },
  ],
  expectedSubstrings: ['{"lang":"ruby","n":42}'],
};
