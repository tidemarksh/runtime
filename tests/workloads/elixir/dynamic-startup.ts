import { resolve } from "node:path";
import type { DynamicStartupEntry } from "../../support/dynamic-startup-harness.js";

const busyboxPath = process.env.TIDEMARK_TEST_BUSYBOX_BINARY ?? "";
const runtimeDir = process.env.TIDEMARK_TEST_ELIXIR_RUNTIME_DIR ?? "";

const ELIXIR_SHELL_APPLETS = [
  "sh",
  "basename",
  "dirname",
  "pwd",
  "readlink",
  "mkdir",
  "sed",
] as const;

export const elixirDynamicStartupEntry: DynamicStartupEntry = {
  id: "elixir-version",
  targetClass: "Elixir / Erlang runtime",
  runnerKind: "shell-script",
  hostExecutablePath: busyboxPath,
  guestExecutablePath: "/bin/sh",
  argv: ["/bin/sh", "/usr/bin/elixir", "--version"],
  env: ["PATH=/bin:/usr/bin", "HOME=/tmp"],
  timeoutMs: 5_000,
  ramSize: 512 * 1024 * 1024,
  snapshotSources: [
    {
      kind: "fileCopies",
      hostPath: busyboxPath,
      guestPaths: ["/bin/busybox", ...ELIXIR_SHELL_APPLETS.map((applet) => `/bin/${applet}`)],
      mode: 0o755,
    },
    { kind: "tree", hostPath: resolve(runtimeDir, "usr/bin"), guestPath: "/usr/bin" },
    { kind: "tree", hostPath: resolve(runtimeDir, "usr/lib/erlang"), guestPath: "/usr/lib/erlang" },
    { kind: "tree", hostPath: resolve(runtimeDir, "usr/lib/elixir"), guestPath: "/usr/lib/elixir" },
    { kind: "tree", hostPath: resolve(runtimeDir, "usr/lib/riscv64-linux-gnu"), guestPath: "/usr/lib/riscv64-linux-gnu" },
    { kind: "tree", hostPath: resolve(runtimeDir, "etc"), guestPath: "/etc" },
    {
      kind: "file",
      hostPath: resolve(runtimeDir, "usr/lib/ld-linux-riscv64-lp64d.so.1"),
      guestPath: "/lib/ld-linux-riscv64-lp64d.so.1",
      mode: 0o755,
    },
  ],
  expectedSubstrings: ["Erlang/OTP 28", "Elixir 1.18.3"],
};
