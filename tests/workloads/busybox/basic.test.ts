import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";
import { expect, test } from "vite-plus/test";
import {
  Runtime,
  logSkip,
  readKernelWasm,
  spawnWithOutput,
} from "../../support/runtime-workload.js";

const BUSYBOX_BINARY_PATH = process.env.TIDEMARK_TEST_BUSYBOX_BINARY
  ?? resolve(__dirname, "../../../../images/busybox");

function readBusyBoxBinary(): Uint8Array | null {
  if (!existsSync(BUSYBOX_BINARY_PATH)) {
    return logSkip(`missing BusyBox binary: ${BUSYBOX_BINARY_PATH}`);
  }
  return new Uint8Array(readFileSync(BUSYBOX_BINARY_PATH));
}

const CASES = [
  {
    name: "--list",
    argv: ["/bin/busybox", "--list"],
    assert(result: { exitCode: number; output: string }): void {
      expect(result.exitCode).toBe(0);
      expect(result.output).toContain("echo\n");
      expect(result.output).toContain("sh\n");
    },
  },
  {
    name: "echo hello",
    argv: ["/bin/busybox", "echo", "hello"],
    assert(result: { exitCode: number; output: string }): void {
      expect(result.exitCode).toBe(0);
      expect(result.output).toBe("hello\n");
    },
  },
  {
    name: "sh -c echo hello",
    argv: ["/bin/busybox", "sh", "-c", "echo hello"],
    assert(result: { exitCode: number; output: string }): void {
      expect(result.exitCode).toBe(0);
      expect(result.output).toBe("hello\n");
    },
  },
];

for (const testCase of CASES) {
  test(`Runtime executes official BusyBox: ${testCase.name}`, async () => {
    const wasmBytes = readKernelWasm();
    const busybox = readBusyBoxBinary();
    if (!wasmBytes || !busybox) {
      return;
    }

    const runtime = await Runtime.create({ wasmBytes });
    try {
      await runtime.fs.mkdir("/bin", 0o755).catch(() => {});
      await runtime.fs.writeFile("/bin/busybox", busybox, 0o755);

      const result = await spawnWithOutput(
        runtime,
        busybox,
        testCase.argv,
        [],
        10_000,
      );
      testCase.assert(result);
    } finally {
      runtime.destroy();
    }
  }, 30_000);
}
