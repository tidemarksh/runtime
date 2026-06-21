import { existsSync, readdirSync } from "node:fs";
import { resolve } from "node:path";
import { expect, test } from "vite-plus/test";
import {
  type DynamicStartupEntry,
  prepareDynamicStartupRuntime,
  resolveDynamicStartupTestTimeoutMs,
  resolveDynamicStartupTimeoutMs,
  runDynamicStartupEntry,
} from "../../../support/dynamic-startup-harness.js";

const runtimeDir = process.env.TIDEMARK_TEST_ELIXIR_RUNTIME_DIR ?? "";
const erlangRootDir = resolve(runtimeDir, "usr/lib/erlang");
const ertsDirName = existsSync(erlangRootDir)
  ? (readdirSync(erlangRootDir).find((entry) => entry.startsWith("erts-")) ?? "erts-unknown")
  : "erts-unknown";
const erlexecGuestPath = `/usr/lib/erlang/${ertsDirName}/bin/erlexec`;
const erlexecPath = resolve(erlangRootDir, `${ertsDirName}/bin/erlexec`);
const erlangBindir = `/usr/lib/erlang/${ertsDirName}/bin`;

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

const elixirDynamicStartupPtyEntry: DynamicStartupEntry = {
  id: "elixir-version-pty-stdio",
  targetClass: "Elixir / Erlang runtime",
  runnerKind: "direct-exec",
  hostExecutablePath: erlexecPath,
  guestExecutablePath: erlexecGuestPath,
  argv: [
    erlexecGuestPath,
    "-noshell",
    "-elixir_root",
    "/usr/lib/elixir/lib",
    "-pa",
    "/usr/lib/elixir/lib/elixir/ebin",
    "-s",
    "elixir",
    "start_cli",
    "-extra",
    "--version",
  ],
  env: ["ROOTDIR=/usr/lib/erlang", `BINDIR=${erlangBindir}`, "EMU=beam", "PROGNAME=erl", "HOME=/tmp"],
  timeoutMs: 5_000,
  stdio: "pty",
  guestMemoryPolicyLimitBytes: 512 * 1024 * 1024,
  diagnosticArtifactPaths: [
    "/usr/lib/erlang/bin/start.boot",
    `/usr/lib/erlang/${ertsDirName}/bin/erlexec`,
    "/usr/lib/erlang/releases/28/OTP_VERSION",
  ],
  snapshotSources: elixirRuntimeSnapshotSources(),
  expectedSubstrings: ["Erlang/OTP 28", "Elixir 1.18.3"],
};

const tcgets2Request = 0x802c542a;

function errorDetail(error: unknown): string {
  return error instanceof Error ? (error.stack ?? error.message) : String(error);
}

function expectPtyTcgets2Timeout(detail: string): void {
  expect(detail).toContain(
    `timeout after ${resolveDynamicStartupTimeoutMs(elixirDynamicStartupPtyEntry)}ms`,
  );
  expect(detail).toContain("syscall_a1=802c542a");
  expect(detail).toMatch(
    new RegExp(
      `nr=29:fd=0:[^\\n]*:ty=12:[^\\n]*:a1=${tcgets2Request}:[^\\n]*:ret=0`,
    ),
  );
  expect(detail).not.toMatch(
    new RegExp(
      `nr=29:fd=0:[^\\n]*:ty=12:[^\\n]*:a1=${tcgets2Request}:[^\\n]*:ret=-25`,
    ),
  );
  expect(detail).toContain("blocked=[21@");
  expect(detail).toContain("owner=running");
}

test("elixir dynamic startup diagnostic: pty stdio reaches TCGETS2 before timeout", async () => {
  const prepared = await prepareDynamicStartupRuntime(elixirDynamicStartupPtyEntry);
  if (!prepared) {
    return;
  }

  const { runtime, elfBytes } = prepared;
  try {
    let detail = "";
    try {
      const result = await runDynamicStartupEntry(
        runtime,
        elixirDynamicStartupPtyEntry,
        elfBytes,
      );
      detail = [
        `unexpected exit=${result.exitCode}`,
        `output=${JSON.stringify(result.output)}`,
        `debug=${result.debugInfo ?? ""}`,
      ].join("\n");
    } catch (error) {
      detail = errorDetail(error);
    }

    expectPtyTcgets2Timeout(detail);
  } finally {
    runtime.destroy();
  }
}, resolveDynamicStartupTestTimeoutMs(elixirDynamicStartupPtyEntry));
