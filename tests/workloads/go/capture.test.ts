import { expect, test } from "vite-plus/test";
import {
  SAMPLE_MODULE_DIR,
  goEnv,
  prepareGoRuntime,
  readGoExecCaptureHelperBinary,
  runtimeDebugSummary,
  spawnWithOutput,
} from "../../support/runtime-workload.js";

async function prepareHelperRuntime() {
  const helperBytes = readGoExecCaptureHelperBinary();
  if (!helperBytes) {
    return null;
  }

  const prepared = await prepareGoRuntime({
    includeToolchain: true,
    includeSampleModule: true,
  });
  if (!prepared) {
    return null;
  }

  return {
    runtime: prepared.runtime,
    helperBytes,
  };
}

test("Runtime guest helper captures internal/goarch compile with CombinedOutput and go build env", async () => {
  const prepared = await prepareHelperRuntime();
  if (!prepared) {
    return;
  }

  const { runtime, helperBytes } = prepared;
  try {
    const { exitCode, output, debugInfo } = await spawnWithOutput(
      runtime,
      helperBytes,
      ["exec-capture-helper", "combined-goarch"],
      goEnv(1),
      120_000,
      { cwd: SAMPLE_MODULE_DIR },
    ).catch(async (error) => {
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error(`${detail}\n${await runtimeDebugSummary(runtime)}`);
    });
    expect(
      exitCode,
      `${output}\n${debugInfo ?? ""}\n${await runtimeDebugSummary(runtime)}`,
    ).toBe(0);
    expect(output).toContain("combined_output_err=<nil>");
  } finally {
    runtime.destroy();
  }
}, 150_000);

test("Runtime guest helper runs internal/goarch compile then go tool buildid with go build env", async () => {
  const prepared = await prepareHelperRuntime();
  if (!prepared) {
    return;
  }

  const { runtime, helperBytes } = prepared;
  try {
    const { exitCode, output, debugInfo } = await spawnWithOutput(
      runtime,
      helperBytes,
      ["exec-capture-helper", "two-step-goarch"],
      goEnv(1),
      120_000,
      { cwd: SAMPLE_MODULE_DIR },
    ).catch(async (error) => {
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error(`${detail}\n${await runtimeDebugSummary(runtime)}`);
    });
    expect(
      exitCode,
      `${output}\n${debugInfo ?? ""}\n${await runtimeDebugSummary(runtime)}`,
    ).toBe(0);
    expect(output).toContain("mode=two-step-goarch");
    expect(output).toContain("compile_err=<nil>");
    expect(output).toContain("buildid_err=<nil>");
  } finally {
    runtime.destroy();
  }
}, 150_000);

test("Runtime guest helper runs internal/goarch compile then go tool buildid with explicit env", async () => {
  const prepared = await prepareHelperRuntime();
  if (!prepared) {
    return;
  }

  const { runtime, helperBytes } = prepared;
  try {
    const { exitCode, output, debugInfo } = await spawnWithOutput(
      runtime,
      helperBytes,
      ["exec-capture-helper", "two-step-goarch-explicit-env"],
      goEnv(1),
      120_000,
      { cwd: SAMPLE_MODULE_DIR },
    ).catch(async (error) => {
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error(`${detail}\n${await runtimeDebugSummary(runtime)}`);
    });
    expect(
      exitCode,
      `${output}\n${debugInfo ?? ""}\n${await runtimeDebugSummary(runtime)}`,
    ).toBe(0);
    expect(output).toContain("mode=two-step-goarch-explicit-env");
    expect(output).toContain("compile_err=<nil>");
    expect(output).toContain("buildid_err=<nil>");
  } finally {
    runtime.destroy();
  }
}, 150_000);
