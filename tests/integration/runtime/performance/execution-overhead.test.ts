import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "vite-plus/test";
import {
  benchmarkAsyncCase,
  benchmarkCaseResultJson,
  type ExecutionCase,
  type ExecutionIdentity,
  type MetricContract,
} from "../../../support/performance-case.js";
import {
  decoder,
  readKernelWasm,
  RUNTIME_DIST_PATH,
  Runtime,
} from "../../../support/runtime-workload.js";

const hostRiscvGcc = process.env.TIDEMARK_TEST_HOST_RISCV_GCC ?? "riscv64-linux-gnu-gcc";
const CPU_OPERATIONS = 10_000_000;
const FS_OPERATIONS = 64;
const PIPE_OPERATIONS = 64;
const METRIC_CONTRACTS: MetricContract[] = [
  { name: "dispatched-step-elapsed", unit: "millisecond" },
  { name: "fs-resume-calls", unit: "call" },
  { name: "pipe-sync-requests", unit: "request" },
  { name: "requested-step-budget", unit: "instruction" },
  { name: "step-count", unit: "step" },
  { name: "worker-messages", unit: "message" },
];

const probeSource = [
  "#include <fcntl.h>",
  "#include <stdint.h>",
  "#include <stdio.h>",
  "#include <string.h>",
  "#include <unistd.h>",
  "",
  `#define CPU_OPERATIONS ${CPU_OPERATIONS}`,
  `#define FS_OPERATIONS ${FS_OPERATIONS}`,
  `#define PIPE_OPERATIONS ${PIPE_OPERATIONS}`,
  "",
  "static int wait_for_stop(void) {",
  "  char byte;",
  "  return read(0, &byte, 1) < 0 ? 91 : 0;",
  "}",
  "",
  "int main(int argc, char **argv) {",
  "  if (argc != 2) return 90;",
  "  if (strcmp(argv[1], \"cpu\") == 0) {",
  "    volatile uint64_t sum = 0;",
  "    for (uint64_t value = 1; value <= CPU_OPERATIONS; value++) sum += value;",
  "    dprintf(1, \"done:cpu:%llu\\n\", (unsigned long long)sum);",
  "    return wait_for_stop();",
  "  }",
  "  if (strcmp(argv[1], \"fs\") == 0) {",
  "    int fd = open(\"/tmp/execution-overhead.bin\", O_CREAT | O_TRUNC | O_WRONLY, 0600);",
  "    if (fd < 0) return 92;",
  "    char bytes[64] = {0};",
  "    for (int index = 0; index < FS_OPERATIONS; index++) {",
  "      if (write(fd, bytes, sizeof(bytes)) != (ssize_t)sizeof(bytes)) return 93;",
  "    }",
  "    if (close(fd) != 0) return 94;",
  "    dprintf(1, \"done:fs:%d\\n\", FS_OPERATIONS * (int)sizeof(bytes));",
  "    return wait_for_stop();",
  "  }",
  "  if (strcmp(argv[1], \"pipe\") == 0) {",
  "    for (int index = 0; index < PIPE_OPERATIONS; index++) {",
  "      if (write(1, \"x\", 1) != 1) return 95;",
  "    }",
  "    dprintf(1, \"done:pipe:%d\\n\", PIPE_OPERATIONS);",
  "    return wait_for_stop();",
  "  }",
  "  return 96;",
  "}",
  "",
].join("\n");

type CaseMode = "cpu" | "fs" | "pipe";

function digest(...values: Array<string | Uint8Array>): string {
  const hash = createHash("sha256");
  for (const value of values) hash.update(value);
  return hash.digest("hex");
}

function buildProbe(): Uint8Array {
  const directory = mkdtempSync(join(tmpdir(), "tidemark-execution-overhead-"));
  const sourcePath = join(directory, "execution_overhead.c");
  const outputPath = join(directory, "execution-overhead");
  try {
    writeFileSync(sourcePath, probeSource);
    execFileSync(hostRiscvGcc, ["-static", "-O2", "-o", outputPath, sourcePath], {
      stdio: "pipe",
    });
    return new Uint8Array(readFileSync(outputPath));
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
}

function usefulWork(mode: CaseMode): number {
  switch (mode) {
    case "cpu": return CPU_OPERATIONS;
    case "fs": return FS_OPERATIONS;
    case "pipe": return PIPE_OPERATIONS;
  }
}

function expectedMarker(mode: CaseMode): string {
  switch (mode) {
    case "cpu": return "done:cpu:50000005000000\n";
    case "fs": return `done:fs:${FS_OPERATIONS * 64}\n`;
    case "pipe": return `done:pipe:${PIPE_OPERATIONS}\n`;
  }
}

function definition(mode: CaseMode): ExecutionCase {
  const count = usefulWork(mode);
  return {
    technicalName: `runtime-end-to-end-${mode}`,
    class: "diagnostic",
    repositoryOwner: "runtime",
    definitionSourceDigest: digest(probeSource, mode, "definition-v1"),
    inputDigest: digest(mode, String(count), "input-v1"),
    usefulWorkUnit: mode === "cpu" ? "integer-update" : `${mode}-write`,
    usefulWorkCount: BigInt(count),
    calibratedRepetitionCount: BigInt(count),
    oracleContractDigest: digest(expectedMarker(mode), "oracle-v1"),
    counterContractDigest: digest(
      "completed-operations",
      ...METRIC_CONTRACTS.flatMap(metric => [metric.name, metric.unit]),
    ),
    caseScheduleDigest: digest("one-warmup-five-measurements-single-runtime-v1"),
  };
}

test("reports fixed end-to-end CPU, filesystem, and pipe execution cases", async () => {
  const wasmBytes = readKernelWasm();
  if (!wasmBytes) return;
  const probeBytes = buildProbe();
  const execution: ExecutionIdentity = {
    engine: "interpreter",
    configurationDigest: digest("runtime-default-memory-policy-static-probe-tmp-dir-v1"),
    artifactDigest: digest(wasmBytes, new Uint8Array(readFileSync(RUNTIME_DIST_PATH))),
  };
  for (const mode of ["cpu", "fs", "pipe"] as const) {
    const runtime = await Runtime.create({ wasmBytes });
    try {
      await runtime.fs.mkdir("/tmp", 0o777).catch(() => {});
      const executionCase = definition(mode);
      const marker = expectedMarker(mode);
      const result = await benchmarkAsyncCase({
        expectedCase: executionCase,
        observedCase: { ...executionCase },
        expectedExecution: execution,
        observedExecution: { ...execution },
        oracleExpected: marker,
        countersExpected: [{ name: "completed-operations", value: BigInt(usefulWork(mode)) }],
        metricsExpected: METRIC_CONTRACTS,
        warmupRuns: 1,
        measurementRuns: 5,
      }, async () => {
        const process = await runtime.spawn(
          new Uint8Array(probeBytes),
          ["/execution-overhead", mode],
          ["LC_ALL=C", "LANG=C"],
          { cwd: "/" },
        );
        let output = "";
        let timeoutId: ReturnType<typeof setTimeout> | undefined;
        try {
          await Promise.race([
            new Promise<void>((resolve, reject) => {
              timeoutId = setTimeout(() => reject(new Error(`marker timeout: ${mode}`)), 30_000);
              process.onStdout((bytes) => {
                output += decoder.decode(bytes);
                if (output.endsWith(marker)) resolve();
              });
            }),
            process.waitForExit().then(exitCode => {
              throw new Error(`probe exited before marker: ${mode}: exit=${exitCode}`);
            }),
          ]);
          const stats = runtime.debugExecutionStats().find(entry => entry.pid === process.pid);
          if (!stats) throw new Error(`missing execution statistics: ${mode}`);
          const stepElapsedMs = stats.workers.reduce((sum, worker) => sum + worker.totalStepWallMs, 0);
          const stepCount = stats.workers.reduce((sum, worker) => sum + worker.stepCount, 0);
          const stepBudget = stats.workers.reduce((sum, worker) => sum + worker.totalStepBudget, 0);
          return {
            output,
            metrics: [
              { name: "dispatched-step-elapsed", unit: "millisecond", value: BigInt(Math.round(stepElapsedMs)) },
              { name: "fs-resume-calls", unit: "call", value: BigInt(stats.transferDebugStats.fsResumeCalls) },
              { name: "pipe-sync-requests", unit: "request", value: BigInt(stats.transferDebugStats.pipeSyncRequests) },
              { name: "requested-step-budget", unit: "instruction", value: BigInt(stepBudget) },
              { name: "step-count", unit: "step", value: BigInt(stepCount) },
              { name: "worker-messages", unit: "message", value: BigInt(stats.workerMessageDebugStats.received) },
            ],
          };
        } finally {
          if (timeoutId !== undefined) clearTimeout(timeoutId);
          runtime.kill(process.pid);
        }
      }, observation => ({
        oracle: observation.output.slice(-marker.length),
        counters: [{ name: "completed-operations", value: BigInt(usefulWork(mode)) }],
        metrics: observation.metrics,
      }));
      console.log(`TIDEMARK_RUNTIME_BENCHMARK ${benchmarkCaseResultJson(result)}`);
      expect(result.metrics).toHaveLength(METRIC_CONTRACTS.length);
    } finally {
      runtime.destroy();
    }
  }
}, 180_000);
