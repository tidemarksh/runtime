import { expect, test } from "vite-plus/test";
import { prepareNodeRuntime, runNodeFixture } from "./support.js";

test("Runtime executes Node worker_threads baseline", async () => {
  const prepared = await prepareNodeRuntime();
  if (!prepared) {
    return;
  }

  const { runtime, nodeBytes } = prepared;
  try {
    const result = await runNodeFixture(runtime, nodeBytes, "worker-threads.js", 15_000);
    if (!result) {
      return;
    }
    expect(result.exitCode).toBe(0);
    expect(result.output).toContain("PASS: worker-threads ");
  } finally {
    runtime.destroy();
  }
}, 30_000);
