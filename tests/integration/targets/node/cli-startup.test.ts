import { expect, test } from "vite-plus/test";
import { runtimeDebugSummary, spawnWithOutput } from "../../../support/runtime-workload.js";
import { prepareNodeRuntimeWithOptions } from "./support.js";

for (const [label, options] of [
  [
    "default runtime contract",
    {},
  ],
  [
    "browser-direct representative contract",
    {
      guestMemoryPolicyLimitBytes: 256 * 1024 * 1024,
      kernelWorkerMaxMemoryPages: 7168,
      maxProcessMemoryPages: 16384,
      maxForkMemoryPages: 16384,
    },
  ],
  [
    "demo direct contract",
    {
      guestMemoryPolicyLimitBytes: 192 * 1024 * 1024,
      kernelWorkerMaxMemoryPages: 7168,
      maxProcessMemoryPages: 20480,
      maxForkMemoryPages: 40960,
    },
  ],
] as const) {
  test(`Runtime direct exec executes /bin/node --help: ${label}`, async () => {
    const prepared = await prepareNodeRuntimeWithOptions(options);
    if (!prepared) {
      return;
    }

    const { runtime, nodeBytes } = prepared;
    try {
      const result = await spawnWithOutput(
        runtime,
        nodeBytes,
        ["/bin/node", "--help"],
        [],
        15_000,
      ).catch(async (error) => {
        const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
        throw new Error(`${detail}\n${await runtimeDebugSummary(runtime)}`);
      });

      expect(
        result.exitCode,
        [
          `node --help exit mismatch: ${label}`,
          `exit=${result.exitCode}`,
          `output=${JSON.stringify(result.output)}`,
          result.debugInfo ? `debugInfo=${result.debugInfo}` : "",
          await runtimeDebugSummary(runtime),
        ].filter(Boolean).join("\n"),
      ).toBe(0);
      expect(
        result.output,
        [
          `node --help output mismatch: ${label}`,
          `exit=${result.exitCode}`,
          `output=${JSON.stringify(result.output)}`,
          result.debugInfo ? `debugInfo=${result.debugInfo}` : "",
          await runtimeDebugSummary(runtime),
        ].filter(Boolean).join("\n"),
      ).toContain("Usage: node");
    } finally {
      runtime.destroy();
    }
  }, 30_000);
}
