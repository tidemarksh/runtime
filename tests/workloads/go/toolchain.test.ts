import { expect, test } from "vite-plus/test";
import {
  SAMPLE_MODULE_DIR,
  goEnv,
  GO_TOOL_VERSION,
  prepareGoRuntime,
  readGoToolBinary,
} from "./support.js";
import {
  encoder,
  runtimeDebugSummary,
  spawnWithOutput,
} from "../../support/runtime-workload.js";

function goarchCompileArgs(): string[] {
  return [
    "/usr/local/go/pkg/tool/linux_riscv64/compile",
    "-o",
    "/tmp/goarch.a",
    "-trimpath",
    "/tmp=>",
    "-p",
    "internal/goarch",
    "-lang=go1.26",
    "-std",
    "-complete",
        "-buildid",
        "test-buildid",
        "-goversion",
        GO_TOOL_VERSION,
        "-nolocalimports",
        "-importcfg",
        "/tmp/importcfg",
    "-pack",
    "/usr/local/go/src/internal/goarch/goarch.go",
    "/usr/local/go/src/internal/goarch/goarch_riscv64.go",
    "/usr/local/go/src/internal/goarch/zgoarch_riscv64.go",
  ];
}

test("Runtime executes go tool compile -V=full", async () => {
  const prepared = await prepareGoRuntime({
    includeToolchain: true,
    includeSampleModule: false,
  });
  if (!prepared) {
    return;
  }

  const { runtime, goBytes } = prepared;
  try {
    const { exitCode, output, debugInfo } = await spawnWithOutput(
      runtime,
      goBytes,
      ["go", "tool", "compile", "-V=full"],
      [
        "GOROOT=/usr/local/go",
        "GOTELEMETRY=off",
        "GOTELEMETRYDIR=/dev/null",
      ],
      120_000,
    ).catch(async (error) => {
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error(`${detail}\n${await runtimeDebugSummary(runtime)}`);
    });
    expect(
      exitCode,
      `${output}\n${debugInfo ?? ""}\n${await runtimeDebugSummary(runtime)}`,
    ).toBe(0);
    expect(output).toContain("compile version");
  } finally {
    runtime.destroy();
  }
}, 150_000);

test("Runtime executes compile binary -V=full with go build env", async () => {
  const compileBytes = readGoToolBinary("pkg/tool/linux_riscv64/compile");
  if (!compileBytes) {
    return;
  }

  const prepared = await prepareGoRuntime({
    includeToolchain: true,
    includeSampleModule: true,
  });
  if (!prepared) {
    return;
  }

  const { runtime } = prepared;
  try {
    const { exitCode, output, debugInfo } = await spawnWithOutput(
      runtime,
      compileBytes,
      ["/usr/local/go/pkg/tool/linux_riscv64/compile", "-V=full"],
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
    expect(output).toContain("compile version");
  } finally {
    runtime.destroy();
  }
}, 150_000);

test("Runtime executes compile binary on sample main.go", async () => {
  const compileBytes = readGoToolBinary("pkg/tool/linux_riscv64/compile");
  if (!compileBytes) {
    return;
  }

  const prepared = await prepareGoRuntime({
    includeToolchain: true,
    includeSampleModule: true,
  });
  if (!prepared) {
    return;
  }

  const { runtime } = prepared;
  try {
    const { exitCode, output, debugInfo } = await spawnWithOutput(
      runtime,
      compileBytes,
      [
        "/usr/local/go/pkg/tool/linux_riscv64/compile",
        "-p",
        "main",
        "-o",
        "/tmp/hello.o",
        `${SAMPLE_MODULE_DIR}/main.go`,
      ],
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
    const artifact = await runtime.fs.readFile("/tmp/hello.o");
    expect(artifact.byteLength).toBeGreaterThan(0);
  } finally {
    runtime.destroy();
  }
}, 150_000);

test("Runtime executes compile binary on internal/goarch", async () => {
  const compileBytes = readGoToolBinary("pkg/tool/linux_riscv64/compile");
  if (!compileBytes) {
    return;
  }

  const prepared = await prepareGoRuntime({
    includeToolchain: true,
    includeSampleModule: true,
  });
  if (!prepared) {
    return;
  }

  const { runtime } = prepared;
  try {
    await runtime.fs.writeFile("/tmp/importcfg", encoder.encode("# import config\n"), 0o644);
    const { exitCode, output, debugInfo } = await spawnWithOutput(
      runtime,
      compileBytes,
      goarchCompileArgs(),
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
    const artifact = await runtime.fs.readFile("/tmp/goarch.a");
    expect(artifact.byteLength).toBeGreaterThan(0);
  } finally {
    runtime.destroy();
  }
}, 150_000);

test("Runtime executes go tool buildid -w on compiled internal/goarch archive", async () => {
  const compileBytes = readGoToolBinary("pkg/tool/linux_riscv64/compile");
  if (!compileBytes) {
    return;
  }

  const prepared = await prepareGoRuntime({
    includeToolchain: true,
    includeSampleModule: true,
  });
  if (!prepared) {
    return;
  }

  const { runtime, goBytes } = prepared;
  try {
    await runtime.fs.writeFile("/tmp/importcfg", encoder.encode("# import config\n"), 0o644);
    const compileResult = await spawnWithOutput(
      runtime,
      compileBytes,
      goarchCompileArgs(),
      goEnv(1),
      120_000,
      { cwd: SAMPLE_MODULE_DIR },
    ).catch(async (error) => {
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error(`compile step failed: ${detail}\n${await runtimeDebugSummary(runtime)}`);
    });
    expect(
      compileResult.exitCode,
      `${compileResult.output}\n${compileResult.debugInfo ?? ""}\n${await runtimeDebugSummary(runtime)}`,
    ).toBe(0);

    const { exitCode, output, debugInfo } = await spawnWithOutput(
      runtime,
      goBytes,
      ["go", "tool", "buildid", "-w", "/tmp/goarch.a"],
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
  } finally {
    runtime.destroy();
  }
}, 150_000);

test("Runtime executes go tool compile -V=full with go build env", async () => {
  const prepared = await prepareGoRuntime({
    includeToolchain: true,
    includeSampleModule: true,
  });
  if (!prepared) {
    return;
  }

  const { runtime, goBytes } = prepared;
  try {
    const { exitCode, output, debugInfo } = await spawnWithOutput(
      runtime,
      goBytes,
      ["go", "tool", "compile", "-V=full"],
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
    expect(output).toContain("compile version");
  } finally {
    runtime.destroy();
  }
}, 150_000);
