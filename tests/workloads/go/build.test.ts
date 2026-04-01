import { expect, test } from "vite-plus/test";
import {
  SAMPLE_MODULE_DIR,
  RuntimeProcess,
  decoder,
  goEnv,
  prepareGoRuntime,
  runtimeDebugSummary,
} from "../../support/runtime-workload.js";

test("Runtime executes go build", async () => {
  const gomaxprocs = Number(process.env.TIDEMARK_ACCEPTANCE_GO_BUILD_GOMAXPROCS ?? "1");
  const timeoutMs = Number(process.env.TIDEMARK_ACCEPTANCE_GO_BUILD_TIMEOUT_MS ?? "300000");
  const prepared = await prepareGoRuntime({
    includeToolchain: true,
    includeSampleModule: true,
  });
  if (!prepared) {
    return;
  }

  const { runtime, goBytes } = prepared;
  try {
    const proc: RuntimeProcess = await runtime.spawn(
      goBytes,
      ["go", "build", "-x", "-v", "-o", `${SAMPLE_MODULE_DIR}/hello`, "."],
      goEnv(gomaxprocs),
      { cwd: SAMPLE_MODULE_DIR },
    );
    let output = "";
    let partialLine = "";
    let timeoutId: ReturnType<typeof setTimeout> | null = null;
    let timedOut = false;
    let compiledPackages = 0;
    let buildCommands = 0;

    proc.onStdout((data) => {
      const text = decoder.decode(data);
      output += text;
      partialLine += text;

      const lines = partialLine.split("\n");
      partialLine = lines.pop() ?? "";
      for (const line of lines) {
        if (line.startsWith("internal/") || line.startsWith("runtime")) {
          compiledPackages += 1;
        }
        if (
          line.startsWith("WORK=")
          || line.includes("/pkg/tool/linux_riscv64/compile ")
          || line.includes("/pkg/tool/linux_riscv64/link ")
        ) {
          buildCommands += 1;
        }
      }
    });

    const exitCode = await Promise.race([
      proc.waitForExit().catch(async (error) => {
        const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
        throw new Error(`${detail}\n${await runtimeDebugSummary(runtime)}`);
      }),
      new Promise<null>((resolve) => {
        timeoutId = setTimeout(() => {
          timedOut = true;
          resolve(null);
        }, timeoutMs);
        if (typeof timeoutId === "object" && timeoutId !== null && "unref" in timeoutId) {
          timeoutId.unref();
        }
      }),
    ]);

    if (timeoutId !== null) {
      clearTimeout(timeoutId);
    }

    expect(
      timedOut,
      `go build timed out after ${timeoutMs}ms\ncompiled_packages=${compiledPackages} build_commands=${buildCommands}\n${output}\n${proc.debugInfo?.() ?? ""}\n${await runtimeDebugSummary(runtime)}`,
    ).toBe(false);

    expect(
      exitCode,
      `go build exited with ${exitCode}\n${output}\n${proc.debugInfo?.() ?? ""}\n${await runtimeDebugSummary(runtime)}`,
    ).toBe(0);
    const artifactPath = `${SAMPLE_MODULE_DIR}/hello`;
    const artifact = await runtime.fs.readFile(artifactPath);
    expect(artifact.byteLength).toBeGreaterThan(0);
    const artifactStat = await runtime.fs.stat(artifactPath);
    expect(artifactStat.size).toBeGreaterThan(0);
  } finally {
    runtime.destroy();
  }
}, Number(process.env.TIDEMARK_ACCEPTANCE_GO_BUILD_TIMEOUT_MS ?? "300000") + 60_000);
