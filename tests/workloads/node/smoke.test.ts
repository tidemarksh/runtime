import { expect, test } from "vite-plus/test";
import { encoder } from "../../support/runtime-workload.js";
import { prepareNodeRuntime, runNodeFixture, writeNodeFixture } from "./support.js";

const CASES = [
  {
    name: "hello",
    fixtureName: "hello.js",
    timeoutMs: 10_000,
    assert(result: { exitCode: number; output: string }): void {
      expect(result.exitCode).toBe(0);
      expect(result.output).toBe("PASS: hello world\n");
    },
  },
  {
    name: "timers",
    fixtureName: "timers.js",
    timeoutMs: 10_000,
    assert(result: { exitCode: number; output: string }): void {
      expect(result.exitCode).toBe(0);
      expect(result.output).toContain("PASS: timers ");
    },
  },
  {
    name: "fs-read",
    fixtureName: "fs-read.js",
    timeoutMs: 10_000,
    async setup(
      runtime: Awaited<ReturnType<typeof prepareNodeRuntime>> extends { runtime: infer R } ? R : never,
    ): Promise<void> {
      await writeNodeFixture(runtime, "/work/fixture.txt", encoder.encode("fixture ok"));
    },
    assert(result: { exitCode: number; output: string }): void {
      expect(result.exitCode).toBe(0);
      expect(result.output).toContain("PASS: fs-read ");
    },
  },
];

for (const testCase of CASES) {
  test(`Runtime executes Node smoke: ${testCase.name}`, async () => {
    const prepared = await prepareNodeRuntime();
    if (!prepared) {
      return;
    }

    const { runtime, nodeBytes } = prepared;
    try {
      await testCase.setup?.(runtime);
      const result = await runNodeFixture(runtime, nodeBytes, testCase.fixtureName, testCase.timeoutMs);
      if (!result) {
        return;
      }
      testCase.assert(result);
    } finally {
      runtime.destroy();
    }
  }, 30_000);
}
