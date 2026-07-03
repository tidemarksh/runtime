import type { KernelExports } from "../abi.js";

export interface SupervisorPendingBackingFillRequest {
  ino: number;
  offset: number;
  length: number;
  expectedSize: number;
  opcode?: number;
}

export interface SupervisorBackingFillSpan {
  offset: number;
  end: number;
  length: number;
}

function supervisorU64ToSafeNumber(value: bigint | number | undefined): number | null {
  if (value === undefined) {
    return null;
  }
  const numeric = typeof value === "bigint" ? Number(value) : value;
  if (!Number.isSafeInteger(numeric) || numeric < 0) {
    return null;
  }
  return numeric;
}

function supervisorNumber(value: bigint | number | undefined, name: string): number {
  const numeric = typeof value === "bigint" ? Number(value) : value;
  if (typeof numeric !== "number" || !Number.isSafeInteger(numeric)) {
    throw new RangeError(`invalid ${name}: ${String(value)}`);
  }
  return numeric;
}

function supervisorNumericExport(
  kernel: KernelExports,
  name:
    | "supervisor_fs_backing_fill_span_offset"
    | "supervisor_fs_backing_fill_span_end"
    | "supervisor_fs_backing_fill_span_length",
  label: string,
): number {
  const read = kernel[name];
  if (typeof read !== "function") {
    throw new Error(`missing ${name} export`);
  }
  return supervisorNumber(read(), label);
}

export function readSupervisorPendingBackingFill(
  kernel: KernelExports,
  vmPtr: number,
): SupervisorPendingBackingFillRequest | null {
  if (
    typeof kernel.supervisor_fs_pending_backing_fill_ino !== "function"
    || typeof kernel.supervisor_fs_pending_backing_fill_offset !== "function"
    || typeof kernel.supervisor_fs_pending_backing_fill_length !== "function"
    || typeof kernel.supervisor_fs_pending_backing_fill_expected_size !== "function"
    || typeof kernel.supervisor_fs_pending_backing_fill_opcode !== "function"
  ) {
    throw new Error("missing supervisor pending backing-fill exports");
  }
  const ino = kernel.supervisor_fs_pending_backing_fill_ino(vmPtr);
  const offset = supervisorU64ToSafeNumber(
    kernel.supervisor_fs_pending_backing_fill_offset(vmPtr),
  );
  const length = supervisorU64ToSafeNumber(
    kernel.supervisor_fs_pending_backing_fill_length(vmPtr),
  );
  const expectedSize = supervisorU64ToSafeNumber(
    kernel.supervisor_fs_pending_backing_fill_expected_size(vmPtr),
  );
  const opcode = kernel.supervisor_fs_pending_backing_fill_opcode(vmPtr);
  if (
    ino <= 0
    || offset === null
    || length === null
    || expectedSize === null
    || length <= 0
    || expectedSize <= 0
  ) {
    return null;
  }
  return {
    ino,
    offset,
    length,
    expectedSize,
    ...(opcode > 0 ? { opcode } : {}),
  };
}

export function clearSupervisorPendingBackingFill(kernel: KernelExports, vmPtr: number): number {
  const clear = kernel.supervisor_fs_clear_pending_backing_fill;
  if (typeof clear !== "function") {
    throw new Error("missing supervisor_fs_clear_pending_backing_fill export");
  }
  const result = supervisorNumber(clear(vmPtr), "supervisor clear pending backing fill result");
  if (result < 0) {
    throw new Error(`supervisor clear pending backing fill failed: ${result}`);
  }
  return result;
}

export function supervisorBackingRetryLimit(
  kernel: KernelExports,
  request: SupervisorPendingBackingFillRequest,
): number {
  const decide = kernel.supervisor_fs_backing_retry_limit;
  if (typeof decide !== "function") {
    throw new Error("missing supervisor_fs_backing_retry_limit export");
  }
  const limit = supervisorNumber(
    decide(
      request.opcode ?? 0,
      BigInt(request.offset),
      BigInt(request.length),
      BigInt(request.expectedSize),
    ),
    "supervisor backing retry limit",
  );
  if (limit <= 0) {
    throw new Error(`invalid supervisor backing retry limit: ${limit}`);
  }
  return limit;
}

export function supervisorBackingFillSpan(
  kernel: KernelExports,
  request: SupervisorPendingBackingFillRequest,
): SupervisorBackingFillSpan | null {
  const decide = kernel.supervisor_fs_backing_fill_span_decision;
  if (typeof decide !== "function") {
    throw new Error("missing supervisor_fs_backing_fill_span_decision export");
  }
  const result = supervisorNumber(
    decide(
      request.opcode ?? 0,
      BigInt(request.offset),
      BigInt(request.length),
      BigInt(request.expectedSize),
    ),
    "supervisor backing-fill span decision",
  );
  if (result < 0) {
    return null;
  }
  const offset = supervisorNumericExport(
    kernel,
    "supervisor_fs_backing_fill_span_offset",
    "supervisor backing-fill span offset",
  );
  const end = supervisorNumericExport(
    kernel,
    "supervisor_fs_backing_fill_span_end",
    "supervisor backing-fill span end",
  );
  const length = supervisorNumericExport(
    kernel,
    "supervisor_fs_backing_fill_span_length",
    "supervisor backing-fill span length",
  );
  if (offset < 0 || end <= offset || length !== end - offset) {
    throw new Error(`invalid supervisor backing-fill span: off=${offset}:end=${end}:len=${length}`);
  }
  return { offset, end, length };
}
