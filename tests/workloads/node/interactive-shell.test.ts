import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";
import { expect, test } from "vite-plus/test";
import {
  Runtime,
  decoder,
  logSkip,
  readKernelWasm,
  runtimeDebugSummary,
} from "../../support/runtime-workload.js";
import type { FsSnapshotEntry } from "../../../src/fs/types.js";

const BUSYBOX_BINARY_PATH = process.env.TIDEMARK_TEST_BUSYBOX_BINARY
  ?? resolve(__dirname, "../../../../images/busybox");
const NODE_BINARY_PATH = process.env.TIDEMARK_TEST_NODE_BINARY
  ?? resolve(__dirname, "../../../../images/node");
const GO_BINARY_PATH = process.env.TIDEMARK_TEST_GO_BINARY
  ?? resolve(__dirname, "../../../../images/go");
const NODE_SYSROOT_DIR = process.env.TIDEMARK_TEST_NODE_SYSROOT_DIR
  ?? resolve(__dirname, "../../../../artifacts/node/sysroot");

const NODE_RUNTIME_LIBS = [
  ["ld-linux-riscv64-lp64d.so.1", "ld-linux-riscv64-lp64d.so.1"],
  ["libc.so.6", "libc.so.6"],
  ["libgcc_s.so.1", "libgcc_s.so.1"],
  ["libm.so.6", "libm.so.6"],
  ["libstdc++.so.6", "libstdc++.so.6.0.35"],
] as const;

function readBytes(path: string, label: string): Uint8Array | null {
  if (!existsSync(path)) {
    return logSkip(`missing ${label}: ${path}`);
  }
  return new Uint8Array(readFileSync(path));
}

function buildInteractiveShellSnapshot(
  busyboxBytes: Uint8Array,
  nodeBytes: Uint8Array,
): FsSnapshotEntry[] | null {
  if (!existsSync(NODE_SYSROOT_DIR)) {
    return logSkip(`missing Node sysroot dir: ${NODE_SYSROOT_DIR}`);
  }

  const entries: FsSnapshotEntry[] = [
    { kind: "dir", path: "/", mode: 0o755 },
    { kind: "dir", path: "/bin", mode: 0o755 },
    { kind: "dir", path: "/lib", mode: 0o755 },
    { kind: "dir", path: "/root", mode: 0o755 },
    { kind: "dir", path: "/root/.config", mode: 0o755 },
    { kind: "dir", path: "/root/.config/go", mode: 0o755 },
    { kind: "dir", path: "/root/.config/go/telemetry", mode: 0o755 },
    { kind: "dir", path: "/tmp", mode: 0o777 },
    { kind: "dir", path: "/tmp/gopath", mode: 0o755 },
    { kind: "dir", path: "/tmp/gocache", mode: 0o755 },
    { kind: "dir", path: "/usr", mode: 0o755 },
    { kind: "dir", path: "/usr/local", mode: 0o755 },
    { kind: "dir", path: "/usr/local/go", mode: 0o755 },
    { kind: "dir", path: "/usr/local/go/bin", mode: 0o755 },
    { kind: "dir", path: "/work", mode: 0o755 },
    { kind: "file", path: "/bin/busybox", data: busyboxBytes, mode: 0o755 },
    { kind: "file", path: "/bin/sh", data: busyboxBytes, mode: 0o755 },
    { kind: "file", path: "/bin/node", data: nodeBytes, mode: 0o755 },
    { kind: "file", path: "/usr/local/go/go.env", data: new TextEncoder().encode("GOTOOLCHAIN=local\n"), mode: 0o644 },
    { kind: "file", path: "/usr/local/go/VERSION", data: new TextEncoder().encode("go1.26.1\n"), mode: 0o644 },
    { kind: "file", path: "/root/.config/go/telemetry/mode", data: new TextEncoder().encode("off 2000-01-01\n"), mode: 0o644 },
  ];

  for (const [guestName, hostName] of NODE_RUNTIME_LIBS) {
    const hostPath = resolve(NODE_SYSROOT_DIR, hostName);
    if (!existsSync(hostPath)) {
      return logSkip(`missing Node runtime lib: ${hostPath}`);
    }
    entries.push({
      kind: "file",
      path: `/lib/${guestName}`,
      data: new Uint8Array(readFileSync(hostPath)),
      mode: 0o755,
    });
  }

  return entries;
}

test("Runtime interactive shell executes /bin/node -v with demo-style env", async () => {
  const wasmBytes = readKernelWasm();
  const busyboxBytes = readBytes(BUSYBOX_BINARY_PATH, "BusyBox binary");
  const nodeBytes = readBytes(NODE_BINARY_PATH, "Node binary");
  const snapshot = busyboxBytes && nodeBytes
    ? buildInteractiveShellSnapshot(busyboxBytes, nodeBytes)
    : null;
  if (!wasmBytes || !busyboxBytes || !nodeBytes || !snapshot) {
    return;
  }

  const runtime = await Runtime.create({
    wasmBytes,
    ramSize: 384 * 1024 * 1024,
  });
  try {
    await runtime.applyFsSnapshot(snapshot);
    const proc = await runtime.spawn(
      busyboxBytes,
      ["/bin/sh", "-i"],
      [
        "TERM=xterm",
        "HOME=/root",
        "PATH=/bin:/usr/local/go/bin",
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
      ],
      { cwd: "/work" },
    );

    let output = "";
    proc.onStdout((data) => {
      output += decoder.decode(data);
    });

    proc.writeStdin(new TextEncoder().encode("/bin/node -v\n"));

    await Promise.race([
      new Promise<void>((resolve, reject) => {
        const deadline = Date.now() + 10_000;
        const poll = (): void => {
          if (/\nv\d+\.\d+\.\d+\r?\n|\nv\d+\.\d+\.\d+\n/.test(output)) {
            resolve();
            return;
          }
          if (Date.now() >= deadline) {
            reject(new Error("timeout waiting for node version output"));
            return;
          }
          setTimeout(poll, 20);
        };
        poll();
      }),
      proc.waitForExit().then((exitCode) => {
        throw new Error(`interactive shell exited early: ${exitCode}`);
      }),
    ]).catch(async (error) => {
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error([
        detail,
        `output=${JSON.stringify(output)}`,
        await runtimeDebugSummary(runtime),
      ].join("\n"));
    });

    expect(output).toMatch(/v\d+\.\d+\.\d+/);

    proc.writeStdin(new TextEncoder().encode("exit\n"));
    await expect(proc.waitForExit()).resolves.toBe(0);
  } finally {
    runtime.destroy();
  }
}, 30_000);

test("Runtime interactive shell executes node -v with demo-style PATH lookup", async () => {
  const wasmBytes = readKernelWasm();
  const busyboxBytes = readBytes(BUSYBOX_BINARY_PATH, "BusyBox binary");
  const nodeBytes = readBytes(NODE_BINARY_PATH, "Node binary");
  const snapshot = busyboxBytes && nodeBytes
    ? buildInteractiveShellSnapshot(busyboxBytes, nodeBytes)
    : null;
  if (!wasmBytes || !busyboxBytes || !nodeBytes || !snapshot) {
    return;
  }

  const runtime = await Runtime.create({
    wasmBytes,
    ramSize: 384 * 1024 * 1024,
  });
  try {
    await runtime.applyFsSnapshot(snapshot);
    const proc = await runtime.spawn(
      busyboxBytes,
      ["/bin/sh", "-i"],
      [
        "TERM=xterm",
        "HOME=/root",
        "PATH=/bin:/usr/local/go/bin",
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
      ],
      { cwd: "/work" },
    );

    let output = "";
    proc.onStdout((data) => {
      output += decoder.decode(data);
    });

    proc.writeStdin(new TextEncoder().encode("node -v\n"));

    await Promise.race([
      new Promise<void>((resolve, reject) => {
        const deadline = Date.now() + 10_000;
        const poll = (): void => {
          if (/\nv\d+\.\d+\.\d+\r?\n|\nv\d+\.\d+\.\d+\n/.test(output)) {
            resolve();
            return;
          }
          if (Date.now() >= deadline) {
            reject(new Error("timeout waiting for node version output"));
            return;
          }
          setTimeout(poll, 20);
        };
        poll();
      }),
      proc.waitForExit().then((exitCode) => {
        throw new Error(`interactive shell exited early: ${exitCode}`);
      }),
    ]).catch(async (error) => {
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error([
        detail,
        `output=${JSON.stringify(output)}`,
        await runtimeDebugSummary(runtime),
      ].join("\n"));
    });

    expect(output).toMatch(/v\d+\.\d+\.\d+/);

    proc.writeStdin(new TextEncoder().encode("exit\n"));
    await expect(proc.waitForExit()).resolves.toBe(0);
  } finally {
    runtime.destroy();
  }
}, 30_000);

test("Runtime interactive shell executes node -v with demo-style PATH lookup from char-by-char stdin", async () => {
  const wasmBytes = readKernelWasm();
  const busyboxBytes = readBytes(BUSYBOX_BINARY_PATH, "BusyBox binary");
  const nodeBytes = readBytes(NODE_BINARY_PATH, "Node binary");
  const snapshot = busyboxBytes && nodeBytes
    ? buildInteractiveShellSnapshot(busyboxBytes, nodeBytes)
    : null;
  if (!wasmBytes || !busyboxBytes || !nodeBytes || !snapshot) {
    return;
  }

  const runtime = await Runtime.create({
    wasmBytes,
    ramSize: 384 * 1024 * 1024,
  });
  try {
    await runtime.applyFsSnapshot(snapshot);
    const proc = await runtime.spawn(
      busyboxBytes,
      ["/bin/sh", "-i"],
      [
        "TERM=xterm",
        "HOME=/root",
        "PATH=/bin:/usr/local/go/bin",
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
      ],
      { cwd: "/work" },
    );

    let output = "";
    proc.onStdout((data) => {
      output += decoder.decode(data);
    });

    for (const ch of "node -v\r") {
      proc.writeStdin(new TextEncoder().encode(ch));
    }

    await Promise.race([
      new Promise<void>((resolve, reject) => {
        const deadline = Date.now() + 10_000;
        const poll = (): void => {
          if (/\nv\d+\.\d+\.\d+\r?\n|\nv\d+\.\d+\.\d+\n/.test(output)) {
            resolve();
            return;
          }
          if (Date.now() >= deadline) {
            reject(new Error("timeout waiting for node version output"));
            return;
          }
          setTimeout(poll, 20);
        };
        poll();
      }),
      proc.waitForExit().then((exitCode) => {
        throw new Error(`interactive shell exited early: ${exitCode}`);
      }),
    ]).catch(async (error) => {
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error([
        detail,
        `output=${JSON.stringify(output)}`,
        await runtimeDebugSummary(runtime),
      ].join("\n"));
    });

    expect(output).toMatch(/v\d+\.\d+\.\d+/);

    proc.writeStdin(new TextEncoder().encode("exit\n"));
    await expect(proc.waitForExit()).resolves.toBe(0);
  } finally {
    runtime.destroy();
  }
}, 30_000);

test("Runtime interactive shell executes go version with demo-style PATH lookup", async () => {
  const wasmBytes = readKernelWasm();
  const busyboxBytes = readBytes(BUSYBOX_BINARY_PATH, "BusyBox binary");
  const nodeBytes = readBytes(NODE_BINARY_PATH, "Node binary");
  const snapshot = busyboxBytes && nodeBytes
    ? buildInteractiveShellSnapshot(busyboxBytes, nodeBytes)
    : null;
  if (!wasmBytes || !busyboxBytes || !nodeBytes || !snapshot) {
    return;
  }

  const runtime = await Runtime.create({
    wasmBytes,
    ramSize: 384 * 1024 * 1024,
  });
  try {
    await runtime.applyFsSnapshot(snapshot);
    const proc = await runtime.spawn(
      busyboxBytes,
      ["/bin/sh", "-i"],
      [
        "TERM=xterm",
        "HOME=/root",
        "PATH=/bin:/usr/local/go/bin",
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
      ],
      { cwd: "/work" },
    );

    let output = "";
    proc.onStdout((data) => {
      output += decoder.decode(data);
    });

    proc.writeStdin(new TextEncoder().encode("go version\n"));

    await Promise.race([
      new Promise<void>((resolve, reject) => {
        const deadline = Date.now() + 10_000;
        const poll = (): void => {
          if (output.includes("go version")) {
            resolve();
            return;
          }
          if (Date.now() >= deadline) {
            reject(new Error("timeout waiting for go version output"));
            return;
          }
          setTimeout(poll, 20);
        };
        poll();
      }),
      proc.waitForExit().then((exitCode) => {
        throw new Error(`interactive shell exited early: ${exitCode}`);
      }),
    ]).catch(async (error) => {
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error([
        detail,
        `output=${JSON.stringify(output)}`,
        await runtimeDebugSummary(runtime),
      ].join("\n"));
    });

    expect(output).toContain("go version");

    proc.writeStdin(new TextEncoder().encode("exit\n"));
    await expect(proc.waitForExit()).resolves.toBe(0);
  } finally {
    runtime.destroy();
  }
}, 30_000);

test("Runtime interactive shell executes go env GOROOT after live Go install", async () => {
  const wasmBytes = readKernelWasm();
  const busyboxBytes = readBytes(BUSYBOX_BINARY_PATH, "BusyBox binary");
  const nodeBytes = readBytes(NODE_BINARY_PATH, "Node binary");
  const goBytes = readBytes(GO_BINARY_PATH, "Go binary");
  const snapshot = busyboxBytes && nodeBytes
    ? buildInteractiveShellSnapshot(busyboxBytes, nodeBytes)
    : null;
  if (!wasmBytes || !busyboxBytes || !nodeBytes || !goBytes || !snapshot) {
    return;
  }

  const runtime = await Runtime.create({
    wasmBytes,
    ramSize: 384 * 1024 * 1024,
  });
  try {
    await runtime.applyFsSnapshot(snapshot);
    const proc = await runtime.spawn(
      busyboxBytes,
      ["/bin/sh", "-i"],
      [
        "TERM=xterm",
        "HOME=/root",
        "PATH=/bin:/usr/local/go/bin",
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
      ],
      { cwd: "/work" },
    );

    await runtime.fs.writeFile("/usr/local/go/bin/go", goBytes, 0o755);
    await runtime.fs.writeFile("/usr/local/go/go.env", new TextEncoder().encode("GOTOOLCHAIN=local\n"), 0o644);
    await runtime.fs.writeFile("/usr/local/go/VERSION", new TextEncoder().encode("go1.26.1\n"), 0o644);

    let output = "";
    proc.onStdout((data) => {
      output += decoder.decode(data);
    });

    proc.writeStdin(new TextEncoder().encode("go env GOROOT\n"));

    await Promise.race([
      new Promise<void>((resolve, reject) => {
        const deadline = Date.now() + 10_000;
        const poll = (): void => {
          if (output.includes("/usr/local/go")) {
            resolve();
            return;
          }
          if (Date.now() >= deadline) {
            reject(new Error("timeout waiting for go env GOROOT output"));
            return;
          }
          setTimeout(poll, 20);
        };
        poll();
      }),
      proc.waitForExit().then((exitCode) => {
        throw new Error(`interactive shell exited early: ${exitCode}`);
      }),
    ]).catch(async (error) => {
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error([
        detail,
        `output=${JSON.stringify(output)}`,
        await runtimeDebugSummary(runtime),
      ].join("\n"));
    });

    expect(output).toContain("/usr/local/go");
    expect(runtime.debugLastFailureSnapshot()).toBeNull();
    const workerFocus = JSON.stringify(runtime.debugWorkerLogFocus());
    expect(workerFocus).not.toContain("status_message_break");
    expect(workerFocus).not.toContain("bad g in signal handler");

    proc.writeStdin(new TextEncoder().encode("exit\n"));
    await expect(proc.waitForExit()).resolves.toBe(0);
  } finally {
    runtime.destroy();
  }
}, 30_000);
