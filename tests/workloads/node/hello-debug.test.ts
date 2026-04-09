import { test } from "vite-plus/test";
import { runtimeDebugSummary } from "../../support/runtime-workload.js";
import { prepareNodeRuntime, runNodeFixture } from "./support.js";

test("debug hello fixture", async () => {
  const prepared = await prepareNodeRuntime();
  if (!prepared) {
    return;
  }

  try {
    const result = await runNodeFixture(prepared.runtime, prepared.nodeBytes, "hello.js", 10_000);
    if (!result) {
      return;
    }
    if (result.exitCode !== 0) {
      throw new Error(
        `exitCode=${result.exitCode}\n`
        + `output=${result.output}\n`
        + `debugInfo=${result.debugInfo ?? "n/a"}\n`
        + `${await runtimeDebugSummary(prepared.runtime)}`,
      );
    }
  } finally {
    prepared.runtime.destroy();
  }
}, 30_000);
