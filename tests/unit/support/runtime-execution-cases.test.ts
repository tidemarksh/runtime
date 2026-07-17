import { createHash } from "node:crypto";
import { MessageChannel } from "node:worker_threads";
import { expect, test } from "vite-plus/test";
import { executeInterpreterStep } from "../../../src/execution/interpreter.js";
import { readProcessSpawnHandoff, type KernelExports } from "../../../src/abi.js";
import {
  completeSupervisorCloneSpawnHostOp,
  type SupervisorCloneSpawnHostOp,
} from "../../../src/worker/supervisor-host-op.js";
import type { ProcessHandle } from "../../../src/worker/types.js";
import {
  measureAsyncCase,
  measureCase,
  rawCaseResultJson,
  type CaseClass,
  type ExecutionCase,
  type ExecutionIdentity,
  type OperationObservation,
} from "../../support/performance-case.js";

const digest = (value: string) => createHash("sha256").update(value).digest("hex");
const identity: ExecutionIdentity = {
  engine: "interpreter",
  configurationDigest: digest("runtime-native-v1"),
  artifactDigest: digest("controlled-kernel-boundary-v1"),
};

function definition(name: string, classification: CaseClass, unit: string): ExecutionCase {
  return {
    technicalName: name,
    class: classification,
    repositoryOwner: "runtime",
    definitionSourceDigest: digest(name),
    inputDigest: digest(`${name}:input-v1`),
    usefulWorkUnit: unit,
    usefulWorkCount: 1n,
    calibratedRepetitionCount: 1n,
    oracleContractDigest: digest(`${name}:oracle-v1`),
    counterContractDigest: digest(`${name}:counter-v1`),
    caseScheduleDigest: digest("single-fixed-sample"),
  };
}

function measure(name: string, classification: CaseClass, unit: string, oracle: string, counter: string, operation: () => unknown, observe: (output: unknown) => OperationObservation) {
  const executionCase = definition(name, classification, unit);
  return measureCase({
    expectedCase: executionCase,
    observedCase: { ...executionCase },
    expectedExecution: identity,
    observedExecution: { ...identity },
    oracleExpected: oracle,
    countersExpected: [{ name: counter, value: 1n }],
  }, operation, observe);
}

test("measures the Runtime interpreter adapter around one fixed Kernel step", () => {
  let calls = 0;
  const result = measure("runtime-kernel-step", "semantic", "kernel-step", "continue:63", "kernel-steps", () => executeInterpreterStep({
    exports: { thread_step: () => { calls += 1; return 63; }, thread_status: () => 0 },
    statusCodes: { OK: 0, FAULT: 1, FS: 2, FUTEX_WAIT: 3, PIPE_WAIT: 4, EPOLL_WAIT: 5, VFORK_WAIT: 6, EXECVE_SPAWN: 7, CLONE_SPAWN: 8, SLEEP: 9, JIT_READY: 10, WAIT4: 11, CONNECT: 12, SOCKET_SEND: 13, SOCKET_RECV: 14, FLOCK_WAIT: 15 },
    vmPtr: 0x1234,
    budget: 64,
  }), output => {
    const step = output as ReturnType<typeof executeInterpreterStep>;
    return { oracle: `${step.boundary}:${step.returnedBudget}`, counters: [{ name: "kernel-steps", value: BigInt(calls) }] };
  });
  expect(JSON.parse(rawCaseResultJson(result)).correctness).toBe("pass");
});

test("measures one native worker request and response roundtrip", async () => {
  const executionCase = definition("worker-message-roundtrip", "diagnostic", "worker-roundtrip");
  const channel = new MessageChannel();
  channel.port2.once("message", value => channel.port2.postMessage(value + 1));
  const response = new Promise<number>(resolve => channel.port1.once("message", resolve));
  const result = await measureAsyncCase({ expectedCase: executionCase, observedCase: { ...executionCase }, expectedExecution: identity, observedExecution: { ...identity }, oracleExpected: "response=42", countersExpected: [{ name: "responses", value: 1n }] }, async () => {
    channel.port1.postMessage(41);
    return await response;
  }, value => ({ oracle: `response=${value}`, counters: [{ name: "responses", value: 1n }] }));
  channel.port1.close(); channel.port2.close();
  expect(JSON.parse(rawCaseResultJson(result)).correctness).toBe("pass");
});

test("measures production HostOp record transport", () => {
  const memory = new WebAssembly.Memory({ initial: 8 });
  const exports = {
    memory,
    kernel_heap_ptr: () => 0,
    supervisor_process_host_op_record_bytes: () => 88,
    supervisor_process_host_op_record_magic: () => 0x504f_4854,
    supervisor_process_host_op_kind_spawn_process_worker: () => 7,
    supervisor_abi_version: () => 1,
    supervisor_process_host_op: (_vm: number, ptr: number, len: number) => {
      const view = new DataView(memory.buffer, ptr, len);
      view.setUint32(0, 0x504f_4854, true); view.setUint32(4, 1, true); view.setUint32(8, 88, true); view.setUint32(12, 7, true);
      view.setBigUint64(16, 9n, true); view.setUint32(24, 0x1234, true); view.setUint32(28, 2, true); view.setInt32(32, 77, true);
      view.setUint32(44, 1, true); view.setBigUint64(48, 0x100000n, true); view.setUint32(56, 16, true); view.setBigUint64(64, 1n, true); view.setBigUint64(72, 2n, true); view.setBigUint64(80, 3n, true);
      return 88;
    },
  } as unknown as KernelExports;
  const result = measure("host-op-transport", "diagnostic", "host-op", "child=77", "host-ops", () => readProcessSpawnHandoff(exports, 0x1234), output => ({ oracle: `child=${(output as { childPid: number }).childPid}`, counters: [{ name: "host-ops", value: 1n }] }));
  expect(JSON.parse(rawCaseResultJson(result)).correctness).toBe("pass");
});

test("measures production HostCompletion encoding and application", () => {
  const memory = new WebAssembly.Memory({ initial: 8 });
  let applied = 0;
  const kernel = { memory, kernel_heap_ptr: () => 0, supervisor_abi_version: () => 1, supervisor_host_completion_record_bytes: () => 64, supervisor_host_completion_record_magic: () => 0x4348_5354, supervisor_host_completion_outcome_success: () => 1, supervisor_complete_host_op: (ptr: number, len: number) => { const view = new DataView(memory.buffer, ptr, len); applied += Number(view.getBigUint64(16, true) === 9n); return 0; } } as unknown as KernelExports;
  const handle = { kernel, memory } as unknown as ProcessHandle;
  const record: SupervisorCloneSpawnHostOp = { kind: 7, seq: 9n, parentVmPtr: 0x1234, childVmPtr: 0x5678, tid: 3, processPtr: 0, status: 0, flags: 0n, childTidPtr: 0n };
  const result = measure("host-completion-apply", "diagnostic", "host-completion", "status=0", "completions", () => completeSupervisorCloneSpawnHostOp(handle, record), output => ({ oracle: `status=${output}`, counters: [{ name: "completions", value: BigInt(applied) }] }));
  expect(JSON.parse(rawCaseResultJson(result)).correctness).toBe("pass");
});
