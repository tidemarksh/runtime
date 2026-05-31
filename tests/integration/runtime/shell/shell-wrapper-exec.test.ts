import { expect, test } from "vite-plus/test";
import { existsSync, readFileSync } from "node:fs";
import { Runtime, logSkip, readKernelWasm, spawnWithOutput } from "../../../support/runtime-workload.js";
import { buildSnapshotFromSources, type SnapshotSource } from "../../../support/snapshot-sources.js";

const busyboxBinary = process.env.TIDEMARK_TEST_BUSYBOX_BINARY ?? "";

const snapshotSources: readonly SnapshotSource[] = [
  {
    kind: "fileCopies",
    hostPath: busyboxBinary,
    guestPaths: ["/bin/busybox", "/bin/sh", "/bin/echo"],
    mode: 0o755,
  },
  {
    kind: "inlineFile",
    guestPath: "/work/wrapper.sh",
    data: 'exec /bin/echo wrapped-ok\n',
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
