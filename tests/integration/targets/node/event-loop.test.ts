import { expect, test } from "vite-plus/test";
import { encoder } from "../../../support/runtime-workload.js";
import { prepareNodeRuntime, runNodeFixture, writeNodeFixture } from "./support.js";

const CASES = [
  {
    name: "fs-timer-thread",
    fixtureName: "event-loop-fs-timer-thread.js",
    timeoutMs: 180_000,
    async setup(
      runtime: Awaited<ReturnType<typeof prepareNodeRuntime>> extends { runtime: infer R } ? R : never,
    ): Promise<void> {
      await writeNodeFixture(runtime, "/work/input.txt", encoder.encode("node-evidence"));
    },
    assert(result: { exitCode: number; output: string }): void {
      expect(result.exitCode).toBe(0);
      expect(JSON.parse(result.output)).toEqual({
        data: "node-evidence",
        worker: 21,
        sum: 42,
      });
    },
    argv: ["/work/input.txt"],
  },
  {
    name: "fs-promises-loop",
    fixtureName: "event-loop-fs-promises-loop.js",
    timeoutMs: 180_000,
    async setup(
      runtime: Awaited<ReturnType<typeof prepareNodeRuntime>> extends { runtime: infer R } ? R : never,
    ): Promise<void> {
      await writeNodeFixture(runtime, "/work/loop/input.txt", encoder.encode("node-fs-loop-evidence"));
      for (const name of ["a.txt", "b.txt", "c.txt", "d.txt", "e.txt", "f.txt"]) {
        await writeNodeFixture(runtime, `/work/loop/${name}`, encoder.encode(name));
      }
    },
    assert(result: { exitCode: number; output: string }): void {
      expect(result.exitCode).toBe(0);
      expect(JSON.parse(result.output)).toEqual({
        data: "node-fs-loop-evidence",
        entryCount: 7,
        hasInput: true,
        immediate: true,
        microtask: true,
      });
    },
    argv: ["/work/loop/input.txt", "/work/loop"],
  },
] as const;

for (const testCase of CASES) {
  test(`Runtime executes Node event-loop local gate: ${testCase.name}`, async () => {
    const prepared = await prepareNodeRuntime();
    if (!prepared) {
      return;
    }

    const { runtime, nodeBytes } = prepared;
    try {
      await testCase.setup(runtime);
      const result = await runNodeFixture(
        runtime,
        nodeBytes,
        testCase.fixtureName,
        testCase.timeoutMs,
        testCase.argv,
      );
      if (!result) {
        return;
      }
      testCase.assert(result);
    } finally {
      runtime.destroy();
    }
  }, 210_000);
}
