import { existsSync, readFileSync } from "node:fs";
import { expect, test } from "vite-plus/test";
import {
  BUSYBOX_BINARY_PATH,
  GO_TOOLCHAIN_DIR,
  SAMPLE_MODULE_DIR,
  goEnv,
  goVersionEnv,
  prepareGoRuntime,
} from "./support.js";
import {
  decoder,
  runtimeDebugSummary,
  spawnWithOutput,
} from "../../../support/runtime-workload.js";

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

test("Runtime reads go runtime source page ranges", async () => {
  if (!BUSYBOX_BINARY_PATH || !existsSync(BUSYBOX_BINARY_PATH)) {
    return;
  }
  const prepared = await prepareGoRuntime({
    includeToolchain: true,
    includeSampleModule: false,
  });
  if (!prepared) {
    return;
  }

  const { runtime } = prepared;
  try {
    const busyboxBytes = new Uint8Array(readFileSync(BUSYBOX_BINARY_PATH));
    async function runBusybox(argv: string[]) {
      return spawnWithOutput(
        runtime,
        busyboxBytes,
        ["/bin/busybox", ...argv],
        goEnv(1),
        120_000,
        { stdin: "closed" },
      ).catch(async (error) => {
        const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
        throw new Error(`${detail}\n${await runtimeDebugSummary(runtime)}`);
      });
    }
    const head = await runBusybox(["head", "-c", "32", "/usr/local/go/src/runtime/arena.go"]);
    const page1 = await runBusybox([
      "dd",
      "if=/usr/local/go/src/runtime/arena.go",
      "bs=4096",
      "skip=1",
      "count=1",
    ]);
    const page2 = await runBusybox([
      "dd",
      "if=/usr/local/go/src/runtime/arena.go",
      "bs=4096",
      "skip=2",
      "count=1",
    ]);
    const expected = new Uint8Array(readFileSync(`${GO_TOOLCHAIN_DIR}/src/runtime/arena.go`));
    expect(
      head.output,
      `${head.debugInfo ?? ""}\n${await runtimeDebugSummary(runtime)}`,
    ).toBe(decoder.decode(expected.subarray(0, 32)));
    expect(
      page1.output.slice(0, 32),
      `${page1.debugInfo ?? ""}\n${await runtimeDebugSummary(runtime)}`,
    ).toBe(decoder.decode(expected.subarray(4096, 4096 + 32)));
    expect(
      page2.output.slice(0, 32),
      `${page2.debugInfo ?? ""}\n${await runtimeDebugSummary(runtime)}`,
    ).toBe(decoder.decode(expected.subarray(8192, 8192 + 32)));
  } finally {
    runtime.destroy();
  }
}, 180_000);

test("Runtime preserves distinct go runtime source file heads", async () => {
  if (!BUSYBOX_BINARY_PATH || !existsSync(BUSYBOX_BINARY_PATH)) {
    return;
  }
  const prepared = await prepareGoRuntime({
    includeToolchain: true,
    includeSampleModule: false,
  });
  if (!prepared) {
    return;
  }

  const { runtime } = prepared;
  try {
    const busyboxBytes = new Uint8Array(readFileSync(BUSYBOX_BINARY_PATH));
    async function readHead(name: string) {
      return spawnWithOutput(
        runtime,
        busyboxBytes,
        ["/bin/busybox", "head", "-c", "48", `/usr/local/go/src/runtime/${name}`],
        goEnv(1),
        120_000,
        { stdin: "closed" },
      ).catch(async (error) => {
        const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
        throw new Error(`${detail}\n${await runtimeDebugSummary(runtime)}`);
      });
    }

    for (const name of ["asan0.go", "arena.go", "hexdump.go", "heapdump.go"]) {
      const actual = await readHead(name);
      const expected = new Uint8Array(readFileSync(`${GO_TOOLCHAIN_DIR}/src/runtime/${name}`));
      expect(
        actual.output,
        `${actual.debugInfo ?? ""}\n${await runtimeDebugSummary(runtime)}`,
      ).toBe(decoder.decode(expected.subarray(0, 48)));
    }
  } finally {
    runtime.destroy();
  }
}, 180_000);

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
    expect(output.trimEnd().split("\n")).toEqual([
      "off",
      "/usr/local/go",
    ]);
    expect(output).not.toContain("\n1\n");
  } finally {
    runtime.destroy();
  }
}, 150_000);

test("Runtime resolves go sample module cwd", async () => {
  const prepared = await prepareGoRuntime({
    includeToolchain: true,
    includeSampleModule: true,
  });
  if (!prepared) {
    return;
  }

  const { runtime, goBytes } = prepared;
  try {
    const env = goEnv(1);
    const envResult = await spawnWithOutput(
      runtime,
      goBytes,
      ["go", "env", "GOMOD", "GOWORK", "GO111MODULE"],
      env,
      120_000,
      { cwd: SAMPLE_MODULE_DIR },
    ).catch(async (error) => {
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error(`${detail}\n${await runtimeDebugSummary(runtime)}`);
    });
    expect(envResult.exitCode, `${envResult.output}\n${envResult.debugInfo ?? ""}`).toBe(0);
    expect(envResult.output).toContain(`${SAMPLE_MODULE_DIR}/go.mod`);
  } finally {
    runtime.destroy();
  }
}, 180_000);

test("Runtime lists go sample module package", async () => {
  const prepared = await prepareGoRuntime({
    includeToolchain: true,
    includeSampleModule: true,
  });
  if (!prepared) {
    return;
  }

  const { runtime, goBytes } = prepared;
  try {
    const listResult = await spawnWithOutput(
      runtime,
      goBytes,
      ["go", "list", "-json", "."],
      goEnv(1),
      180_000,
      { cwd: SAMPLE_MODULE_DIR },
    ).catch(async (error) => {
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error(`${detail}\n${await runtimeDebugSummary(runtime)}`);
    });
    expect(
      listResult.exitCode,
      `${listResult.output}\n${listResult.debugInfo ?? ""}\n${await runtimeDebugSummary(runtime)}`,
    ).toBe(0);
    expect(listResult.output).toContain('"ImportPath": "hello"');
  } finally {
    runtime.destroy();
  }
}, 240_000);
