import { expect, test } from "vite-plus/test";
import {
  goEnv,
  goVersionEnv,
  prepareGoRuntime,
  runtimeDebugSummary,
  spawnWithOutput,
} from "../../support/runtime-workload.js";

async function spawnGoCommand(
  argv: string[],
  env: string[],
  options: {
    includeToolchain?: boolean;
    includeSampleModule?: boolean;
  } = {},
) {
  const prepared = await prepareGoRuntime(options);
  if (!prepared) {
    return null;
  }

  const { runtime, goBytes } = prepared;
  try {
    const result = await spawnWithOutput(
      runtime,
      goBytes,
      argv,
      env,
      120_000,
    ).catch(async (error) => {
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error(`${detail}\n${await runtimeDebugSummary(runtime)}`);
    });
    return {
      runtime,
      ...result,
    };
  } catch (error) {
    runtime.destroy();
    throw error;
  }
}

for (const gomaxprocs of [1, 2]) {
  test(`Runtime executes go version with GOMAXPROCS=${gomaxprocs}`, async () => {
    const includeToolchain = process.env.TIDEMARK_ACCEPTANCE_FULL_GOROOT === "1";
    const result = await spawnGoCommand(
      ["go", "version"],
      goVersionEnv(gomaxprocs),
      {
        includeToolchain,
        includeSampleModule: false,
      },
    );
    if (!result) {
      return;
    }

    try {
      expect(result.exitCode, `${result.output}\n${result.debugInfo ?? ""}`).toBe(0);
      expect(result.output).toContain("go version");
    } finally {
      result.runtime.destroy();
    }
  }, 150_000);
}

test("Runtime executes go env GOROOT with minimal environment", async () => {
  const result = await spawnGoCommand(
    ["go", "env", "GOROOT"],
    [
      "GOROOT=/usr/local/go",
      "GOTELEMETRY=off",
      "PATH=/usr/local/go/bin:/bin",
    ],
    {
      includeToolchain: false,
      includeSampleModule: false,
    },
  );
  if (!result) {
    return;
  }

  try {
    expect(
      result.exitCode,
      `${result.output}\n${result.debugInfo ?? ""}\n${await runtimeDebugSummary(result.runtime)}`,
    ).toBe(0);
    expect(result.output).toContain("/usr/local/go");
  } finally {
    result.runtime.destroy();
  }
}, 150_000);

test("Runtime executes go env GOROOT with telemetry dir env", async () => {
  const result = await spawnGoCommand(
    ["go", "env", "GOROOT"],
    [
      "GOROOT=/usr/local/go",
      "GOTELEMETRY=off",
      "GOTELEMETRYDIR=/dev/null",
      "PATH=/usr/local/go/bin:/bin",
    ],
    {
      includeToolchain: false,
      includeSampleModule: false,
    },
  );
  if (!result) {
    return;
  }

  try {
    expect(result.exitCode, `${result.output}\n${result.debugInfo ?? ""}`).toBe(0);
    expect(result.output).toContain("/usr/local/go");
  } finally {
    result.runtime.destroy();
  }
}, 150_000);

test("Runtime executes go env GOROOT with experiment env", async () => {
  const result = await spawnGoCommand(
    ["go", "env", "GOROOT"],
    [
      "GOROOT=/usr/local/go",
      "GOTELEMETRY=off",
      "GOEXPERIMENT=nodwarf5",
      "PATH=/usr/local/go/bin:/bin",
    ],
    {
      includeToolchain: false,
      includeSampleModule: false,
    },
  );
  if (!result) {
    return;
  }

  try {
    expect(result.exitCode, `${result.output}\n${result.debugInfo ?? ""}`).toBe(0);
    expect(result.output).toContain("/usr/local/go");
  } finally {
    result.runtime.destroy();
  }
}, 150_000);

test("Runtime executes go env GOROOT with cache env group", async () => {
  const result = await spawnGoCommand(
    ["go", "env", "GOROOT"],
    [
      "GOROOT=/usr/local/go",
      "PATH=/usr/local/go/bin:/bin",
      "GOPATH=/tmp/gopath",
      "GOCACHE=/tmp/gocache",
      "GONOSUMDB=*",
      "HOME=/root",
      "GOGC=off",
    ],
    {
      includeToolchain: false,
      includeSampleModule: false,
    },
  );
  if (!result) {
    return;
  }

  try {
    expect(result.exitCode, `${result.output}\n${result.debugInfo ?? ""}`).toBe(0);
    expect(result.output).toContain("/usr/local/go");
  } finally {
    result.runtime.destroy();
  }
}, 150_000);

test("Runtime executes go env GOROOT with HOME env", async () => {
  const result = await spawnGoCommand(
    ["go", "env", "GOROOT"],
    [
      "GOROOT=/usr/local/go",
      "GOTELEMETRY=off",
      "PATH=/usr/local/go/bin:/bin",
      "HOME=/root",
    ],
    {
      includeToolchain: false,
      includeSampleModule: false,
    },
  );
  if (!result) {
    return;
  }

  try {
    expect(
      result.exitCode,
      `${result.output}\n${result.debugInfo ?? ""}\n${await runtimeDebugSummary(result.runtime)}`,
    ).toBe(0);
    expect(result.output).toContain("/usr/local/go");
  } finally {
    result.runtime.destroy();
  }
}, 150_000);

test("Runtime propagates go build environment", async () => {
  const prepared = await prepareGoRuntime({
    includeToolchain: false,
    includeSampleModule: false,
  });
  if (!prepared) {
    return;
  }

  const { runtime, goBytes } = prepared;
  try {
    const env = goEnv(1).filter((entry) => !entry.startsWith("GOFLAGS="));
    const { exitCode, output, debugInfo } = await spawnWithOutput(
      runtime,
      goBytes,
      ["go", "env", "GOTELEMETRY", "GOROOT", "GOMAXPROCS"],
      env,
      120_000,
    ).catch(async (error) => {
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error(`${detail}\n${await runtimeDebugSummary(runtime)}`);
    });
    expect(exitCode, `${output}\n${debugInfo ?? ""}`).toBe(0);
    expect(output).toContain("off");
    expect(output).toContain("/usr/local/go");
    expect(output).toContain("1");
  } finally {
    runtime.destroy();
  }
}, 150_000);
