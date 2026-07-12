import { readdirSync } from "node:fs";
import { expect, test } from "vite-plus/test";
import {
  GO_TOOLCHAIN_DIR,
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
} from "../../../support/runtime-workload.js";

type GoRuntimeOptions = NonNullable<Parameters<typeof prepareGoRuntime>[0]>;
const GO_TOOLCHAIN_HOST_FILE_BACKING_MODE = process.env
  .TIDEMARK_ACCEPTANCE_GO_BUILD_HOST_FILE_BACKING_MODE as GoRuntimeOptions["hostFileBackingMode"] | undefined;
const GO_TOOL_DIAGNOSTIC_GODEBUG = process.env.TIDEMARK_DIAGNOSTIC_GO_TOOL_GODEBUG;
const GO_TOOL_DIAGNOSTIC_TIMEOUT_MS = Number.parseInt(
  process.env.TIDEMARK_DIAGNOSTIC_GO_TOOL_TIMEOUT_MS ?? "120000",
  10,
);

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

function runtimeSourcePaths(names: string[]): string[] {
  return names.map((name) => `/usr/local/go/src/runtime/${name}`);
}

function runtimeSourcePrefixThrough(name: string): string[] {
  if (!GO_TOOLCHAIN_DIR) {
    return [];
  }
  const runtimeDir = `${GO_TOOLCHAIN_DIR}/src/runtime`;
  const files = readdirSync(runtimeDir)
    .filter((entry) => entry.endsWith(".go") && !entry.endsWith("_test.go"))
    .sort();
  const endIndex = files.indexOf(name);
  return runtimeSourcePaths(endIndex < 0 ? [] : files.slice(0, endIndex + 1));
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
        ...(GO_TOOL_DIAGNOSTIC_GODEBUG ? [`GODEBUG=${GO_TOOL_DIAGNOSTIC_GODEBUG}`] : []),
      ],
      GO_TOOL_DIAGNOSTIC_TIMEOUT_MS,
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
      [
        ...goEnv(1),
        ...(GO_TOOL_DIAGNOSTIC_GODEBUG ? [`GODEBUG=${GO_TOOL_DIAGNOSTIC_GODEBUG}`] : []),
      ],
      GO_TOOL_DIAGNOSTIC_TIMEOUT_MS,
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

test("Runtime parses runtime arena source with compile binary", async () => {
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
    const { output, debugInfo } = await spawnWithOutput(
      runtime,
      compileBytes,
      [
        "/usr/local/go/pkg/tool/linux_riscv64/compile",
        "-e",
        "-p",
        "runtime",
        "-o",
        "/tmp/arena.o",
        "/usr/local/go/src/runtime/arena.go",
      ],
      goEnv(1),
      120_000,
      { cwd: SAMPLE_MODULE_DIR },
    ).catch(async (error) => {
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error(`${detail}\n${await runtimeDebugSummary(runtime)}`);
    });
    expect(
      output,
      `${debugInfo ?? ""}\n${await runtimeDebugSummary(runtime)}`,
    ).not.toContain("expected 'package'");
  } finally {
    runtime.destroy();
  }
}, 150_000);

test("Runtime parses runtime mgcscavenge source with compile binary", async () => {
  const compileBytes = readGoToolBinary("pkg/tool/linux_riscv64/compile");
  if (!compileBytes) {
    return;
  }

  const prepared = await prepareGoRuntime({
    includeToolchain: true,
    includeSampleModule: true,
    hostFileBackingMode: GO_TOOLCHAIN_HOST_FILE_BACKING_MODE,
  });
  if (!prepared) {
    return;
  }

  const { runtime } = prepared;
  try {
    const { output, debugInfo } = await spawnWithOutput(
      runtime,
      compileBytes,
      [
        "/usr/local/go/pkg/tool/linux_riscv64/compile",
        "-e",
        "-p",
        "runtime",
        "-o",
        "/tmp/mgcscavenge.o",
        "/usr/local/go/src/runtime/mgcscavenge.go",
      ],
      goEnv(1),
      120_000,
      { cwd: SAMPLE_MODULE_DIR },
    ).catch(async (error) => {
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error(`${detail}\n${await runtimeDebugSummary(runtime)}`);
    });
    expect(
      output,
      `${debugInfo ?? ""}\n${await runtimeDebugSummary(runtime)}`,
    ).not.toContain("expected 'package'");
  } finally {
    runtime.destroy();
  }
}, 150_000);

test("Runtime parses paired runtime sources with compile binary", async () => {
  const compileBytes = readGoToolBinary("pkg/tool/linux_riscv64/compile");
  if (!compileBytes) {
    return;
  }

  const prepared = await prepareGoRuntime({
    includeToolchain: true,
    includeSampleModule: true,
    hostFileBackingMode: GO_TOOLCHAIN_HOST_FILE_BACKING_MODE,
  });
  if (!prepared) {
    return;
  }

  const { runtime } = prepared;
  try {
    const sourceSets = [
      {
        label: "arena-then-atomic",
        sources: runtimeSourcePaths(["arena.go", "atomic_pointer.go"]),
      },
      {
        label: "atomic-then-arena",
        sources: runtimeSourcePaths(["atomic_pointer.go", "arena.go"]),
      },
      {
        label: "sorted-prefix-through-atomic",
        sources: runtimeSourcePrefixThrough("atomic_pointer.go"),
      },
      {
        label: "sorted-prefix-through-cgocall",
        sources: runtimeSourcePrefixThrough("cgocall.go"),
      },
      {
        label: "sorted-prefix-through-mgcscavenge",
        sources: runtimeSourcePrefixThrough("mgcscavenge.go"),
      },
    ];

    for (const [index, sourceSet] of sourceSets.entries()) {
      const { exitCode, output, debugInfo } = await spawnWithOutput(
        runtime,
        compileBytes,
        [
          "/usr/local/go/pkg/tool/linux_riscv64/compile",
          "-e",
          "-p",
          "runtime",
          "-o",
          `/tmp/runtime-sources-${index}.o`,
          ...sourceSet.sources,
        ],
        goEnv(1),
        180_000,
        { cwd: SAMPLE_MODULE_DIR },
      ).catch(async (error) => {
        const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
        throw new Error(`${sourceSet.label}\n${detail}\n${await runtimeDebugSummary(runtime)}`);
      });
      expect(
        output,
        `${sourceSet.label} exit=${exitCode}\n${debugInfo ?? ""}\n${await runtimeDebugSummary(runtime)}`,
      ).not.toContain("expected 'package'");
    }
  } finally {
    runtime.destroy();
  }
}, 240_000);

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

test("Runtime executes go tool link -V=full with go build env", async () => {
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
      ["go", "tool", "link", "-V=full"],
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
    expect(output).toContain("link version");
  } finally {
    runtime.destroy();
  }
}, 150_000);
