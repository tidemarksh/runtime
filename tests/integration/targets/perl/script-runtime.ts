import { resolve } from "node:path";
import type { ScriptRuntimeEntry } from "../../../support/script-runtime-harness.js";

const runtimeDir = process.env.TIDEMARK_TEST_PERL_RUNTIME_DIR ?? "";

export const perlScriptRuntimeEntry: ScriptRuntimeEntry = {
  id: "perl-script",
  targetClass: "Perl runtime",
  runnerKind: "direct-exec",
  hostExecutablePath: resolve(runtimeDir, "usr/bin/perl"),
  guestExecutablePath: "/usr/bin/perl",
  argv: ["/usr/bin/perl", "/work/script.pl"],
  env: [],
  timeoutMs: 10_000,
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
    {
      kind: "inlineFile",
      guestPath: "/work/script.pl",
      data: [
        "use strict;",
        "use warnings;",
        "use JSON::PP ();",
        'print JSON::PP->new->canonical->encode({ lang => "perl", n => 42 });',
        'print "\\n";',
        "",
      ].join("\n"),
    },
  ],
  expectedSubstrings: ['{"lang":"perl","n":42}'],
};
