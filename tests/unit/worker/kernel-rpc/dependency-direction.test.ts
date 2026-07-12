import { readFileSync, readdirSync } from "node:fs";
import { join, relative } from "node:path";
import { expect, test } from "vite-plus/test";
import {
  assertSupervisorRuntimeAbi,
  requiredSupervisorRuntimeExportNames,
} from "../../../../src/abi.js";

const RUNTIME_ROOT = join(process.cwd(), "src");
const RUNTIME_SRC = join(RUNTIME_ROOT, "worker");

function tsFiles(root: string): string[] {
  const entries = readdirSync(root, { withFileTypes: true });
  return entries.flatMap((entry) => {
    const path = join(root, entry.name);
    if (entry.isDirectory()) {
      return tsFiles(path);
    }
    return entry.isFile() && entry.name.endsWith(".ts") ? [path] : [];
  });
}

function relativeWorkerPath(path: string): string {
  return relative(RUNTIME_SRC, path).split("\\").join("/");
}

function runtimeSource(path: string): string {
  return readFileSync(join(RUNTIME_ROOT, path), "utf8");
}

test("kernel-rpc internals do not depend on host-boundary composition", () => {
  const kernelRpcViolations = tsFiles(join(RUNTIME_SRC, "kernel-rpc"))
    .filter((path) => relativeWorkerPath(path) !== "kernel-rpc/index.ts")
    .flatMap((path) => {
      const source = readFileSync(path, "utf8");
      const matches = [...source.matchAll(/from\s+["']([^"']+)["']/g)]
        .map((match) => match[1])
        .filter((specifier) => specifier === "../host-boundary.js" || specifier.startsWith("../host-boundary/"));
      return matches.map((specifier) => `${relativeWorkerPath(path)} -> ${specifier}`);
    });
  expect(kernelRpcViolations).toEqual([]);
});

test("process creation requires the supervisor ABI before runtime ownership starts", () => {
  const source = runtimeSource("worker/create-process.ts");
  const kernelInit = source.indexOf("exports.kernel_init(wasmHeapBase);");
  const supervisorAssert = source.indexOf("assertSupervisorRuntimeAbi(exports);");
  const baseTime = source.indexOf("exports.set_base_time(");

  expect(kernelInit).toBeGreaterThanOrEqual(0);
  expect(supervisorAssert).toBeGreaterThan(kernelInit);
  expect(baseTime).toBeGreaterThan(supervisorAssert);
  expect(requiredSupervisorRuntimeExportNames()).toEqual(expect.arrayContaining([
    "supervisor_process_resume",
    "supervisor_next_resume",
    "supervisor_complete_futex_wait",
    "supervisor_blocked_status_is_host_wait",
    "supervisor_blocked_status_is_connect_wait",
    "supervisor_blocked_status_is_epoll_wait",
    "supervisor_blocked_status_is_pipe_wait",
    "supervisor_blocked_status_is_wait4",
    "supervisor_blocked_status_is_socket_wait",
    "supervisor_blocked_status_needs_network_sidecar_restore",
    "supervisor_blocked_status_syncs_socket_after_resume",
    "supervisor_blocked_epoll_wait_timeout_ms",
    "supervisor_blocked_poll_wait_has_socket",
    "supervisor_blocked_pipe_wait_has_ready_events",
    "supervisor_blocked_wait_active_child_pid",
    "supervisor_blocked_wait_retry_flag_host_progress",
    "supervisor_blocked_wait_retry_flag_active_child",
    "supervisor_blocked_wait_retry_flag_passive_wait",
    "supervisor_blocked_wait_retry_flag_process_gate",
    "supervisor_blocked_wait_retry_flag_thread_init_pending",
    "supervisor_signal_pending_for_blocked_wait",
    "supervisor_signal_boundary_check",
    "supervisor_enqueue_blocked_wait_timer",
    "supervisor_blocked_wait_timer_required",
    "supervisor_fd_control_record",
    "supervisor_fs_control_record",
    "supervisor_signal_control_record",
    "supervisor_debug_snapshot",
    "supervisor_thread_state_snapshot",
  ]));
});

test("supervisor ABI checker fails closed when an authority export is missing", () => {
  expect(() => assertSupervisorRuntimeAbi({} as never)).toThrow(
    "kernel supervisor ABI missing required export(s): thread_tid, supervisor_abi_version",
  );

  const completeExports = Object.fromEntries(
    requiredSupervisorRuntimeExportNames().map((name) => [name, () => 0]),
  );
  expect(() => assertSupervisorRuntimeAbi(completeExports as never)).not.toThrow();
});
