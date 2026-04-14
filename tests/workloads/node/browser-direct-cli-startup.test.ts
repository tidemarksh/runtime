import { expect, test } from "vite-plus/test";
import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";
import type { FsSnapshotEntry } from "../../../src/fs/types.js";
import { runBrowserDirectStartupEntry } from "../../support/browser-direct-harness.js";
import { prepareNodeBrowserDirectStartupEntry } from "./browser-direct-startup.js";

const BUSYBOX_BINARY_PATH = process.env.TIDEMARK_TEST_BUSYBOX_BINARY
  ?? resolve(__dirname, "../../../../images/busybox");

function shellEnv(): string[] {
  return [
    "TERM=xterm",
    "HOME=/root",
    "ENV=/root/.shrc",
    "PATH=/bin:/usr/bin:/usr/local/go/bin",
    "PS1=$ ",
    "GOROOT=/usr/local/go",
    "GOPATH=/tmp/gopath",
    "GOCACHE=/tmp/gocache",
    "GOMAXPROCS=1",
    "GOTELEMETRY=off",
    "GOTELEMETRYDIR=/dev/null",
    "GOGC=off",
    "GOEXPERIMENT=nodwarf5",
    "GOFLAGS=-mod=mod",
    "GONOSUMDB=*",
    "R_HOME=/rhome",
  ];
}

function shellEntries(busyboxBytes: Uint8Array): FsSnapshotEntry[] {
  return [
    { kind: "dir", path: "/root", mode: 0o755 },
    { kind: "file", path: "/bin/busybox", data: busyboxBytes, mode: 0o755 },
    { kind: "file", path: "/bin/sh", data: busyboxBytes, mode: 0o755 },
    { kind: "file", path: "/root/.shrc", data: new TextEncoder().encode(""), mode: 0o644 },
  ];
}

function splitNodeRuntimeEntries(entries: FsSnapshotEntry[]): {
  shellSnapshot: FsSnapshotEntry[];
  liveInstallEntries: FsSnapshotEntry[];
} {
  const liveInstallEntries = entries.filter((entry) =>
    entry.kind === "file" && (entry.path === "/bin/node" || entry.path.startsWith("/lib/")),
  );
  const shellSnapshot = entries.filter((entry) =>
    !(entry.kind === "file" && (entry.path === "/bin/node" || entry.path.startsWith("/lib/"))),
  );
  return { shellSnapshot, liveInstallEntries };
}

test("browser direct startup local gate: node --help", async () => {
  const base = prepareNodeBrowserDirectStartupEntry();
  if (!base) {
    return;
  }

  const result = await runBrowserDirectStartupEntry({
    ...base,
    id: "browser-direct-node-help",
    run: {
      ...base.run,
      argv: ["/bin/node", "--help"],
    },
    outputPattern: /Usage: node/,
    timeoutMs: 60_000,
  });
  if (!result) {
    return;
  }

  expect(result.exitCode).toBe(0);
  expect(result.output).toContain("Usage: node");
}, 90_000);

test("browser direct startup local gate: node --help after shell boot", async () => {
  const base = prepareNodeBrowserDirectStartupEntry();
  if (!base) {
    return;
  }
  if (!existsSync(BUSYBOX_BINARY_PATH)) {
    return;
  }

  const busyboxBytes = new Uint8Array(readFileSync(BUSYBOX_BINARY_PATH));
  const result = await runBrowserDirectStartupEntry({
    ...base,
    id: "browser-direct-node-help-after-shell-boot",
    snapshot: [
      ...base.snapshot,
      ...shellEntries(busyboxBytes),
    ],
    prepareShell: {
      executablePath: "/bin/sh",
      argv: ["/bin/sh", "-i"],
      env: shellEnv(),
      cwd: "/work",
      readyOutputIncludes: "$ ",
      timeoutMs: 10_000,
    },
    run: {
      executablePath: "/bin/node",
      argv: ["/bin/node", "--help"],
      env: shellEnv(),
      cwd: "/work",
    },
    outputPattern: /Usage: node/,
    timeoutMs: 60_000,
  });
  if (!result) {
    return;
  }

  expect(result.exitCode).toBe(0);
  expect(result.output).toContain("Usage: node");
}, 90_000);

test("browser direct startup local gate: node --help after shell boot and live install", async () => {
  const base = prepareNodeBrowserDirectStartupEntry();
  if (!base) {
    return;
  }
  if (!existsSync(BUSYBOX_BINARY_PATH)) {
    return;
  }

  const busyboxBytes = new Uint8Array(readFileSync(BUSYBOX_BINARY_PATH));
  const { shellSnapshot, liveInstallEntries } = splitNodeRuntimeEntries(base.snapshot);
  const result = await runBrowserDirectStartupEntry({
    ...base,
    id: "browser-direct-node-help-after-shell-boot-and-live-install",
    snapshot: [
      ...shellSnapshot,
      ...shellEntries(busyboxBytes),
    ],
    prepareShell: {
      executablePath: "/bin/sh",
      argv: ["/bin/sh", "-i"],
      env: shellEnv(),
      cwd: "/work",
      readyOutputIncludes: "$ ",
      timeoutMs: 10_000,
    },
    prepareWritesAfterShellStart: liveInstallEntries,
    run: {
      executablePath: "/bin/node",
      argv: ["/bin/node", "--help"],
      env: shellEnv(),
      cwd: "/work",
    },
    outputPattern: /Usage: node/,
    timeoutMs: 60_000,
  });
  if (!result) {
    return;
  }

  expect(result.exitCode).toBe(0);
  expect(result.output).toContain("Usage: node");
}, 90_000);
