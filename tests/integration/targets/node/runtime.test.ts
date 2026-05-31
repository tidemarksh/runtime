import { expect, test } from "vite-plus/test";
import { prepareNodeRuntime, runNodeFixture } from "./support.js";

const CASES = [
  {
    name: "process-info",
    fixtureName: "process-info.js",
    timeoutMs: 10_000,
    assert(result: { exitCode: number; output: string }): void {
      expect(result.exitCode).toBe(0);
      expect(result.output).toContain("PASS: process ");
    },
  },
  {
    name: "os-module",
    fixtureName: "os-module.js",
    timeoutMs: 10_000,
    assert(result: { exitCode: number; output: string }): void {
      expect(result.exitCode).toBe(0);
      expect(result.output).toContain("PASS: os-module ");
    },
  },
];

for (const testCase of CASES) {
  test(`Runtime executes Node runtime contract: ${testCase.name}`, async () => {
    const prepared = await prepareNodeRuntime();
    if (!prepared) {
      return;
    }

    const { runtime, nodeBytes } = prepared;
    try {
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
