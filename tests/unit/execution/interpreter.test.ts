import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";
import { expect, test, vi } from "vite-plus/test";
import {
  classifyExecutionBoundary,
  createInterpreterExecutionEngine,
  executeInterpreterStep,
} from "../../../src/execution/interpreter.js";
import type { ExecutionBoundaryStatusCodes } from "../../../src/execution/types.js";

const statusCodes: ExecutionBoundaryStatusCodes = {
  OK: 0,
  FAULT: 1,
  FS: 2,
  FUTEX_WAIT: 3,
  PIPE_WAIT: 4,
  EPOLL_WAIT: 5,
  VFORK_WAIT: 6,
  EXECVE_SPAWN: 7,
  CLONE_SPAWN: 8,
  SLEEP: 9,
  JIT_READY: 10,
  WAIT4: 11,
  CONNECT: 12,
  SOCKET_SEND: 13,
  SOCKET_RECV: 14,
  FLOCK_WAIT: 15,
};

test("interpreter execution engine wraps thread_step and returns the status boundary", () => {
  const threadStep = vi.fn(() => 17);
  const threadStatus = vi.fn(() => statusCodes.FS);

  const result = executeInterpreterStep({
    exports: {
      thread_step: threadStep,
      thread_status: threadStatus,
    },
    statusCodes,
    vmPtr: 0x1234,
    budget: 1_000,
  });

  expect(threadStep).toHaveBeenCalledWith(0x1234, 1_000);
  expect(threadStatus).toHaveBeenCalledWith(0x1234);
  expect(result).toEqual({
    engine: "interpreter",
    status: statusCodes.FS,
    boundary: "syscall",
    returnedBudget: 17,
  });
});

test("execution boundary classification covers syscall, fault, blocking, and invalidation returns", () => {
  expect(classifyExecutionBoundary(statusCodes.OK, statusCodes)).toBe("continue");
  expect(classifyExecutionBoundary(statusCodes.FS, statusCodes)).toBe("syscall");
  expect(classifyExecutionBoundary(statusCodes.EXECVE_SPAWN, statusCodes)).toBe("syscall");
  expect(classifyExecutionBoundary(statusCodes.FAULT, statusCodes)).toBe("fault");
  expect(classifyExecutionBoundary(statusCodes.FUTEX_WAIT, statusCodes)).toBe("blocking");
  expect(classifyExecutionBoundary(statusCodes.EPOLL_WAIT, statusCodes)).toBe("blocking");
  expect(classifyExecutionBoundary(statusCodes.WAIT4, statusCodes)).toBe("blocking");
  expect(classifyExecutionBoundary(statusCodes.JIT_READY, statusCodes)).toBe("invalidation");
});

test("execution exposes only interpreter engine construction for section 14", () => {
  const engine = createInterpreterExecutionEngine({
    exports: {
      thread_step: () => 0,
      thread_status: () => statusCodes.OK,
    },
    statusCodes,
  });

  expect(engine.kind).toBe("interpreter");
});

test("execution implementation does not import fd ownership or scheduler policy modules", () => {
  const here = dirname(fileURLToPath(import.meta.url));
  const sourceRoot = resolve(here, "../../../src/execution");
  const sources = [
    readFileSync(resolve(sourceRoot, "types.ts"), "utf8"),
    readFileSync(resolve(sourceRoot, "interpreter.ts"), "utf8"),
    readFileSync(resolve(sourceRoot, "index.ts"), "utf8"),
  ].join("\n");

  expect(sources).not.toMatch(/from "\.\.\/worker\//);
  expect(sources).not.toMatch(/from "\.\.\/thread-worker\//);
  expect(sources).not.toMatch(/from ".*\/(ownership|scheduler|kernel-rpc|sync|fork|io)\//);
});
