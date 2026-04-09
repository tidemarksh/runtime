import { resolve } from "node:path";
import type { ScriptRuntimeEntry } from "../../support/script-runtime-harness.js";

const runtimeDir = process.env.TIDEMARK_TEST_ELIXIR_RUNTIME_DIR ?? "";
const erlangRootDir = resolve(runtimeDir, "usr/lib/erlang");
const erlexecPath = resolve(erlangRootDir, "erts-15.2.7.6/bin/erlexec");
const erlangBindir = "/usr/lib/erlang/erts-15.2.7.6/bin";

function elixirRuntimeSnapshotSources() {
  return [
    { kind: "tree" as const, hostPath: resolve(runtimeDir, "usr/bin"), guestPath: "/usr/bin" },
    { kind: "tree" as const, hostPath: resolve(runtimeDir, "usr/lib/erlang"), guestPath: "/usr/lib/erlang" },
    { kind: "tree" as const, hostPath: resolve(runtimeDir, "usr/lib/elixir"), guestPath: "/usr/lib/elixir" },
    { kind: "tree" as const, hostPath: resolve(runtimeDir, "usr/lib/riscv64-linux-gnu"), guestPath: "/usr/lib/riscv64-linux-gnu" },
    { kind: "tree" as const, hostPath: resolve(runtimeDir, "etc"), guestPath: "/etc" },
    {
      kind: "file" as const,
      hostPath: resolve(runtimeDir, "usr/lib/ld-linux-riscv64-lp64d.so.1"),
      guestPath: "/lib/ld-linux-riscv64-lp64d.so.1",
      mode: 0o755,
    },
  ];
}

export const erlEvalScriptRuntimeEntry: ScriptRuntimeEntry = {
  id: "erl-eval",
  targetClass: "Elixir / Erlang runtime",
  runnerKind: "direct-exec",
  hostExecutablePath: erlexecPath,
  guestExecutablePath: "/usr/lib/erlang/erts-15.2.7.6/bin/erlexec",
  argv: [
    "/usr/lib/erlang/erts-15.2.7.6/bin/erlexec",
    "-noshell",
    "-eval",
    'io:format("erl-evidence~n"), halt().',
  ],
  env: ["ROOTDIR=/usr/lib/erlang", `BINDIR=${erlangBindir}`, "EMU=beam", "PROGNAME=erl", "HOME=/tmp"],
  timeoutMs: 10_000,
  ramSize: 512 * 1024 * 1024,
  snapshotSources: elixirRuntimeSnapshotSources(),
  expectedSubstrings: ["erl-evidence"],
};

export const elixirEvalScriptRuntimeEntry: ScriptRuntimeEntry = {
  id: "elixir-eval",
  targetClass: "Elixir / Erlang runtime",
  runnerKind: "direct-exec",
  hostExecutablePath: erlexecPath,
  guestExecutablePath: "/usr/lib/erlang/erts-15.2.7.6/bin/erlexec",
  argv: [
    "/usr/lib/erlang/erts-15.2.7.6/bin/erlexec",
    "-noshell",
    "-elixir_root",
    "/usr/lib/elixir/lib",
    "-pa",
    "/usr/lib/elixir/lib/elixir/ebin",
    "-s",
    "elixir",
    "start_cli",
    "-extra",
    "-e",
    'IO.puts("elixir-evidence")',
  ],
  env: ["ROOTDIR=/usr/lib/erlang", `BINDIR=${erlangBindir}`, "EMU=beam", "PROGNAME=erl", "HOME=/tmp"],
  timeoutMs: 10_000,
  ramSize: 512 * 1024 * 1024,
  snapshotSources: elixirRuntimeSnapshotSources(),
  expectedSubstrings: ["elixir-evidence"],
};
