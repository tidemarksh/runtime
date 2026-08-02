import { createHash } from "node:crypto";
import { expect, test } from "vite-plus/test";
import { createSupervisorResumeHostBoundary } from "../../../src/worker/host-boundary/supervisor-resume.js";
import { syncOfdStateViaKernelWorker } from "../../../src/worker/host-boundary/fd-ofd-state-sync.js";
import { applyStatusFdEntries } from "../../../src/worker/message-handler/status/fd-entries.js";
import type { ProcessHandle } from "../../../src/worker/types.js";
import {
  benchmarkAsyncCase,
  benchmarkCase,
  benchmarkCaseResultJson,
  type ExecutionCase,
  type ExecutionIdentity,
} from "../../support/performance-case.js";
import { makeHandle, makeKernel, makeStatusMessage } from "../worker/status/helpers.js";

const digest = (value: string) => createHash("sha256").update(value).digest("hex");
const identity: ExecutionIdentity = {
  engine: "runtime-native",
  configurationDigest: digest("runtime-native-single-process-v1"),
  artifactDigest: digest("runtime-source-state-v1"),
};

function definition(name: string, unit: string, usefulWorkCount: number): ExecutionCase {
  return {
    technicalName: name,
    class: "semantic",
    repositoryOwner: "runtime",
    definitionSourceDigest: digest(`${name}:definition-v1`),
    inputDigest: digest(`${name}:input-v1`),
    usefulWorkUnit: unit,
    usefulWorkCount: BigInt(usefulWorkCount),
    calibratedRepetitionCount: BigInt(usefulWorkCount),
    oracleContractDigest: digest(`${name}:oracle-v1`),
    counterContractDigest: digest(`${name}:counter-v1`),
    caseScheduleDigest: digest("one-warmup-five-measurements-v1"),
  };
}

test("benchmarks supervisor resume polling when no resume ABI is present", () => {
  const repetitions = 10_000_000;
  const executionCase = definition("supervisor-resume-empty-poll", "resume-poll", repetitions);
  const boundary = createSupervisorResumeHostBoundary({
    threadWorkerStates: new WeakMap(),
    retryDeferredResumes: () => {},
  } as never);
  const handle = { kernel: {} } as ProcessHandle;
  const result = benchmarkCase({
    expectedCase: executionCase,
    observedCase: { ...executionCase },
    expectedExecution: identity,
    observedExecution: { ...identity },
    oracleExpected: `empty=${repetitions}`,
    countersExpected: [{ name: "resume-polls", value: BigInt(repetitions) }],
  }, () => {
    let empty = 0;
    for (let index = 0; index < repetitions; index += 1) {
      if (boundary.selectSupervisorRunnableWorker(handle) === undefined) {
        empty += 1;
      }
    }
    return empty;
  }, empty => ({
    oracle: `empty=${empty}`,
    counters: [{ name: "resume-polls", value: BigInt(empty) }],
  }));
  console.log(`TIDEMARK_RUNTIME_BENCHMARK ${benchmarkCaseResultJson(result)}`);
  expect(result.measurementRuns).toBe(5);
});

test("benchmarks status processing with no descriptor changes", () => {
  const repetitions = 100_000;
  const executionCase = definition("status-empty-fd-update", "status-message", repetitions);
  const handle = makeHandle({
    kernel: makeKernel({ abi_status_fault: () => 3 }),
  }) as unknown as ProcessHandle;
  const message = makeStatusMessage({ fdEntries: [] });
  const result = benchmarkCase({
    expectedCase: executionCase,
    observedCase: { ...executionCase },
    expectedExecution: identity,
    observedExecution: { ...identity },
    oracleExpected: "changed=0",
    countersExpected: [{ name: "status-messages", value: BigInt(repetitions) }],
  }, () => {
    let changed = 0;
    for (let index = 0; index < repetitions; index += 1) {
      changed += applyStatusFdEntries(handle, message as never).length;
    }
    return changed;
  }, changed => ({
    oracle: `changed=${changed}`,
    counters: [{ name: "status-messages", value: BigInt(repetitions) }],
  }));
  console.log(`TIDEMARK_RUNTIME_BENCHMARK ${benchmarkCaseResultJson(result)}`);
  expect(result.measurementRuns).toBe(5);
});

test("benchmarks one event descriptor state synchronization", async () => {
  const repetitions = 20_000;
  const executionCase = definition("event-ofd-state-sync", "ofd-sync", repetitions);
  const kernel = makeKernel({
    kernel_ofd_fd_type: (ofdId: number) => ofdId === 7 ? 9 : 0,
  });
  const handle = makeHandle({
    kernel,
    kernelWorkerRequest: async (message: { type: string; requestId?: number; kernelState: unknown; ofdSlots?: unknown[] }) => ({
      type: "ofd-state-synced" as const,
      requestId: message.requestId ?? 0,
      kernelState: message.kernelState,
      ofdSlots: message.ofdSlots ?? [],
    }),
  }) as unknown as ProcessHandle;
  const result = await benchmarkAsyncCase({
    expectedCase: executionCase,
    observedCase: { ...executionCase },
    expectedExecution: identity,
    observedExecution: { ...identity },
    oracleExpected: `synced=${repetitions}`,
    countersExpected: [{ name: "ofd-syncs", value: BigInt(repetitions) }],
  }, async () => {
    for (let index = 0; index < repetitions; index += 1) {
      await syncOfdStateViaKernelWorker(
        handle,
        [7],
        { kind: "syscall", nr: 0n, name: "performance-measurement" },
      );
    }
    return repetitions;
  }, synced => ({
    oracle: `synced=${synced}`,
    counters: [{ name: "ofd-syncs", value: BigInt(synced) }],
  }));
  console.log(`TIDEMARK_RUNTIME_BENCHMARK ${benchmarkCaseResultJson(result)}`);
  expect(result.measurementRuns).toBe(5);
});
