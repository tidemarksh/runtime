import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "vite-plus/test";
import {
  Runtime,
  logSkip,
  readKernelWasm,
  runtimeDebugSummary,
  spawnWithOutput,
} from "../../support/runtime-workload.js";

const hostRiscvGcc = process.env.TIDEMARK_TEST_HOST_RISCV_GCC ?? "riscv64-linux-gnu-gcc";

function buildEventfdEpollThreadProbe(): Uint8Array | null {
  const tmp = mkdtempSync(join(tmpdir(), "tidemark-eventfd-epoll-thread-"));
  const sourcePath = join(tmp, "eventfd_epoll_thread.c");
  const outputPath = join(tmp, "eventfd-epoll-thread");
  try {
    writeFileSync(sourcePath, [
      "#include <pthread.h>",
      "#include <stdint.h>",
      "#include <stdio.h>",
      "#include <sys/epoll.h>",
      "#include <sys/eventfd.h>",
      "#include <unistd.h>",
      "",
      "static int event_fd;",
      "static int epoll_fd;",
      "static int ready_pipe[2];",
      "",
      "static void *wait_for_event(void *arg) {",
      "  (void)arg;",
      "  char ready = 'R';",
      "  if (write(ready_pipe[1], &ready, 1) != 1) return (void *)(intptr_t)20;",
      "  struct epoll_event event = {0};",
      "  if (epoll_wait(epoll_fd, &event, 1, -1) != 1) return (void *)(intptr_t)21;",
      "  if ((event.events & EPOLLIN) == 0 || event.data.fd != event_fd) return (void *)(intptr_t)22;",
      "  eventfd_t value = 0;",
      "  if (eventfd_read(event_fd, &value) != 0 || value != 1) return (void *)(intptr_t)23;",
      "  return NULL;",
      "}",
      "",
      "int main(void) {",
      "  event_fd = eventfd(0, EFD_CLOEXEC);",
      "  epoll_fd = epoll_create1(EPOLL_CLOEXEC);",
      "  if (event_fd < 0 || epoll_fd < 0 || pipe(ready_pipe) != 0) return 10;",
      "  struct epoll_event interest = { .events = EPOLLIN, .data.fd = event_fd };",
      "  if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, event_fd, &interest) != 0) return 11;",
      "  pthread_t waiter;",
      "  if (pthread_create(&waiter, NULL, wait_for_event, NULL) != 0) return 12;",
      "  char ready = 0;",
      "  if (read(ready_pipe[0], &ready, 1) != 1 || ready != 'R') return 13;",
      "  usleep(100000);",
      "  eventfd_t value = 1;",
      "  if (eventfd_write(event_fd, value) != 0) return 14;",
      "  void *thread_result = NULL;",
      "  if (pthread_join(waiter, &thread_result) != 0) return 15;",
      "  if (thread_result != NULL) return (int)(intptr_t)thread_result;",
      "  puts(\"eventfd-epoll-thread-ok\");",
      "  return 0;",
      "}",
      "",
    ].join("\n"));
    execFileSync(hostRiscvGcc, ["-static", "-O2", "-pthread", "-o", outputPath, sourcePath], {
      stdio: "pipe",
    });
    return new Uint8Array(readFileSync(outputPath));
  } catch (error) {
    const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
    return logSkip(`failed to build eventfd epoll thread probe with ${hostRiscvGcc}: ${detail}`);
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
}

test("eventfd write wakes a thread blocked in epoll_wait", async () => {
  const wasmBytes = readKernelWasm();
  const probeBytes = buildEventfdEpollThreadProbe();
  if (!wasmBytes || !probeBytes) {
    return;
  }

  const runtime = await Runtime.create({
    wasmBytes,
    guestMemoryPolicyLimitBytes: 128 * 1024 * 1024,
  });
  try {
    const result = await spawnWithOutput(
      runtime,
      probeBytes,
      ["/eventfd-epoll-thread"],
      ["LC_ALL=C", "LANG=C"],
      5_000,
      { cwd: "/", stdin: "closed" },
    ).catch(async (error) => {
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error([detail, await runtimeDebugSummary(runtime)].join("\n"));
    });
    expect(result.exitCode, `${result.output}\n${result.debugInfo ?? ""}`).toBe(0);
    expect(result.output).toBe("eventfd-epoll-thread-ok\n");
  } finally {
    runtime.destroy();
  }
}, 20_000);
