import { expect, test } from "vite-plus/test";
import { existsSync, readFileSync } from "node:fs";
import { Runtime, logSkip, readKernelWasm, runtimeDebugSummary, spawnWithOutput } from "../../../support/runtime-workload.js";
import { buildSnapshotFromSources, type SnapshotSource } from "../../../support/snapshot-sources.js";

const busyboxBinary = process.env.TIDEMARK_TEST_BUSYBOX_BINARY ?? "";
const nestedShellWrapperTimeoutMs = 20_000;
const nestedShellWrapperTestTimeoutMs = 30_000;

const snapshotSources: readonly SnapshotSource[] = [
  {
    kind: "fileCopies",
    hostPath: busyboxBinary,
    guestPaths: ["/bin/busybox", "/bin/sh", "/bin/echo", "/bin/readlink", "/bin/dirname", "/bin/basename", "/bin/pwd"],
    mode: 0o755,
  },
  {
    kind: "dir",
    guestPath: "/usr/bin",
  },
  {
    kind: "inlineFile",
    guestPath: "/work/wrapper.sh",
    data: 'exec /bin/echo wrapped-ok\n',
  },
  {
    kind: "inlineFile",
    guestPath: "/work/command-substitution-wrapper.sh",
    data: [
      "readlink_f() {",
      "  cd \"$(dirname \"$1\")\" || exit 1",
      "  target=\"$(basename \"$1\")\"",
      "  while [ -L \"$target\" ]; do",
      "    target=\"$(readlink \"$target\")\"",
      "    cd \"$(dirname \"$target\")\" || exit 1",
      "    target=\"$(basename \"$target\")\"",
      "  done",
      "  dir=\"$(pwd -P)\"",
      "  echo \"$dir/$target\"",
      "}",
      "SELF=\"$(readlink_f \"$0\")\"",
      "SCRIPT_PATH=\"$(dirname \"$SELF\")\"",
      "exec /bin/echo \"command-substitution-ok:$SCRIPT_PATH:$1\"",
      "",
    ].join("\n"),
  },
  {
    kind: "inlineFile",
    guestPath: "/usr/bin/elixir",
    mode: 0o755,
    data: [
      "#!/bin/sh",
      "set -e",
      "readlink_f () {",
      "  cd \"$(dirname \"$1\")\" > /dev/null",
      "  filename=\"$(basename \"$1\")\"",
      "  if [ -h \"$filename\" ]; then",
      "    readlink_f \"$(readlink \"$filename\")\"",
      "  else",
      "    echo \"$(pwd -P)/$filename\"",
      "  fi",
      "}",
      "SELF=$(readlink_f \"$0\")",
      "SCRIPT_PATH=$(dirname \"$SELF\")",
      "set -- \"erl\" -noshell -elixir_root \"$SCRIPT_PATH\"/../lib -pa \"$SCRIPT_PATH\"/../lib/elixir/ebin -s elixir start_cli \"$@\"",
      "if [ -n \"$ELIXIR_CLI_DRY_RUN\" ]; then",
      "  echo \"$@\"",
      "else",
      "  exec \"$@\"",
      "fi",
      "",
    ].join("\n"),
  },
];

test("generic shell-wrapper exec chain: /bin/sh wrapper -> /bin/echo", async () => {
  const wasmBytes = readKernelWasm();
  if (!wasmBytes) {
    return;
  }
  if (!busyboxBinary || !existsSync(busyboxBinary)) {
    return logSkip(`missing busybox binary: ${busyboxBinary || "set TIDEMARK_TEST_BUSYBOX_BINARY"}`);
  }

  const snapshot = buildSnapshotFromSources(snapshotSources, {
    missingLabel: "shell wrapper artifact",
  });
  if (snapshot.skipReason) {
    return logSkip(snapshot.skipReason);
  }

  const runtime = await Runtime.create({ wasmBytes });
  try {
    await runtime.applyFsSnapshot(snapshot.entries ?? []);
    const result = await spawnWithOutput(
      runtime,
      new Uint8Array(readFileSync(busyboxBinary)),
      ["/bin/sh", "/work/wrapper.sh"],
      [],
      10_000,
    );
    expect(
      result.exitCode,
      [
        "shell-wrapper exec exit mismatch",
        `exit=${result.exitCode}`,
        `output=${JSON.stringify(result.output)}`,
        `debug=${result.debugInfo ?? ""}`,
      ].join("\n"),
    ).toBe(0);
    expect(result.output).toContain("wrapped-ok");
  } finally {
    runtime.destroy();
  }
}, 20_000);

test("generic shell-wrapper elixir-style dry-run resumes parent shell", async () => {
  const wasmBytes = readKernelWasm();
  if (!wasmBytes) {
    return;
  }
  if (!busyboxBinary || !existsSync(busyboxBinary)) {
    return logSkip(`missing busybox binary: ${busyboxBinary || "set TIDEMARK_TEST_BUSYBOX_BINARY"}`);
  }

  const snapshot = buildSnapshotFromSources(snapshotSources, {
    missingLabel: "shell elixir-style wrapper artifact",
  });
  if (snapshot.skipReason) {
    return logSkip(snapshot.skipReason);
  }

  const runtime = await Runtime.create({ wasmBytes });
  try {
    await runtime.applyFsSnapshot(snapshot.entries ?? []);
    const result = await spawnWithOutput(
      runtime,
      new Uint8Array(readFileSync(busyboxBinary)),
      ["/bin/sh", "/usr/bin/elixir", "--version"],
      ["PATH=/bin:/usr/bin", "HOME=/tmp", "ELIXIR_CLI_DRY_RUN=1"],
      nestedShellWrapperTimeoutMs,
      { guestExecutablePath: "/bin/sh", stdin: "closed" },
    ).catch(async (error) => {
      const summary = await runtimeDebugSummary(runtime);
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error([detail, summary].join("\n"));
    });
    expect(
      result.exitCode,
      [
        "shell-wrapper elixir-style dry-run exit mismatch",
        `exit=${result.exitCode}`,
        `output=${JSON.stringify(result.output)}`,
        `debug=${result.debugInfo ?? ""}`,
      ].join("\n"),
    ).toBe(0);
    expect(result.output).toContain("erl -noshell");
    expect(result.output).toContain("-s elixir start_cli");
    expect(result.output).toContain("--version");
  } finally {
    runtime.destroy();
  }
}, nestedShellWrapperTestTimeoutMs);

test("generic shell-wrapper command substitution pipe EOF resumes parent shell", async () => {
  const wasmBytes = readKernelWasm();
  if (!wasmBytes) {
    return;
  }
  if (!busyboxBinary || !existsSync(busyboxBinary)) {
    return logSkip(`missing busybox binary: ${busyboxBinary || "set TIDEMARK_TEST_BUSYBOX_BINARY"}`);
  }

  const snapshot = buildSnapshotFromSources(snapshotSources, {
    missingLabel: "shell command substitution wrapper artifact",
  });
  if (snapshot.skipReason) {
    return logSkip(snapshot.skipReason);
  }

  const runtime = await Runtime.create({ wasmBytes });
  try {
    await runtime.applyFsSnapshot(snapshot.entries ?? []);
    const result = await spawnWithOutput(
      runtime,
      new Uint8Array(readFileSync(busyboxBinary)),
      ["/bin/sh", "/work/command-substitution-wrapper.sh", "arg-ok"],
      [],
      nestedShellWrapperTimeoutMs,
    );
    expect(
      result.exitCode,
      [
        "shell-wrapper command substitution exit mismatch",
        `exit=${result.exitCode}`,
        `output=${JSON.stringify(result.output)}`,
        `debug=${result.debugInfo ?? ""}`,
      ].join("\n"),
    ).toBe(0);
    expect(result.output).toContain("command-substitution-ok:");
    expect(result.output).toContain(":arg-ok");
  } finally {
    runtime.destroy();
  }
}, nestedShellWrapperTestTimeoutMs);
