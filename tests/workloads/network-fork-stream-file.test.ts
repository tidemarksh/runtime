import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "vite-plus/test";
import type { RuntimeNetworkBridge, RuntimeNetworkConnection } from "../../src/index.js";
import {
  Runtime,
  decoder,
  logSkip,
  readKernelWasm,
  spawnWithOutput,
} from "../support/runtime-workload.js";

const hostRiscvGcc = process.env.TIDEMARK_TEST_HOST_RISCV_GCC ?? "riscv64-linux-gnu-gcc";
const runForkStreamFileGate = process.env.TIDEMARK_TEST_FORK_STREAM_FILE_GATE === "1";
const streamBodyBytes = Number(process.env.TIDEMARK_TEST_FORK_STREAM_FILE_BYTES ?? 2 * 1024 * 1024);
const streamReadBufferBytes = Number(process.env.TIDEMARK_TEST_FORK_STREAM_FILE_READ_BUFFER_BYTES ?? 8192);
const streamTimeoutMs = Number(process.env.TIDEMARK_TEST_FORK_STREAM_FILE_TIMEOUT_MS ?? 150_000);
const streamAsyncBridge = process.env.TIDEMARK_TEST_FORK_STREAM_FILE_ASYNC_BRIDGE === "1";
const streamParentPselectTimeoutMs = Math.max(
  0,
  Math.trunc(Number(process.env.TIDEMARK_TEST_FORK_STREAM_FILE_PARENT_PSELECT_TIMEOUT_MS ?? 10)),
);

function buildForkStreamFileProbe(): { parent: Uint8Array; child: Uint8Array } | null {
  const tmp = mkdtempSync(join(tmpdir(), "tidemark-network-fork-stream-file-"));
  const parentSourcePath = join(tmp, "fork_stream_file_parent.c");
  const childSourcePath = join(tmp, "fork_stream_file_child.c");
  const parentOutputPath = join(tmp, "fork-stream-file-parent");
  const childOutputPath = join(tmp, "fork-stream-file-child");
  try {
    writeFileSync(childSourcePath, [
      "#include <arpa/inet.h>",
      "#include <errno.h>",
      "#include <fcntl.h>",
      "#include <netinet/in.h>",
      "#include <stdio.h>",
      "#include <stdlib.h>",
      "#include <string.h>",
      "#include <sys/socket.h>",
      "#include <unistd.h>",
      "",
      "static int write_all(int fd, const unsigned char *data, size_t len) {",
      "  size_t off = 0;",
      "  while (off < len) {",
      "    ssize_t n = write(fd, data + off, len - off);",
      "    if (n < 0) {",
      "      if (errno == EINTR) continue;",
      "      return -1;",
      "    }",
      "    off += (size_t)n;",
      "  }",
      "  return 0;",
      "}",
      "",
      "static void account_body(const unsigned char *data, size_t len, unsigned long long *count, unsigned long long *sum) {",
      "  for (size_t i = 0; i < len; i++) *sum += (unsigned long long)data[i];",
      "  *count += (unsigned long long)len;",
      "}",
      "",
      "int main(int argc, char **argv) {",
      "  unsigned long long expected = argc > 1 ? strtoull(argv[1], NULL, 10) : 0;",
      "  int sock = socket(AF_INET, SOCK_STREAM, 0);",
      "  if (sock < 0) { perror(\"socket\"); return 20; }",
      "  struct sockaddr_in addr;",
      "  memset(&addr, 0, sizeof(addr));",
      "  addr.sin_family = AF_INET;",
      "  addr.sin_port = htons(8080);",
      "  addr.sin_addr.s_addr = htonl(0x7f000001u);",
      "  if (connect(sock, (struct sockaddr *)&addr, sizeof(addr)) != 0) { perror(\"connect\"); return 21; }",
      "  const char *request = \"GET http://stream-file.test/payload HTTP/1.1\\r\\nHost: stream-file.test\\r\\nConnection: close\\r\\n\\r\\n\";",
      "  if (write_all(sock, (const unsigned char *)request, strlen(request)) != 0) { perror(\"write-request\"); return 22; }",
      "  int out = open(\"/tmp/fork-stream.out\", O_CREAT | O_TRUNC | O_WRONLY, 0644);",
      "  if (out < 0) { perror(\"open-output\"); return 23; }",
      `  unsigned char buf[${Math.max(1, Math.trunc(streamReadBufferBytes))}];`,
      "  unsigned long long body_count = 0;",
      "  unsigned long long checksum = 0;",
      "  unsigned long long read_calls = 0;",
      "  unsigned long long socket_bytes = 0;",
      "  for (;;) {",
      "    ssize_t n = read(sock, buf, sizeof(buf));",
      "    if (n < 0) {",
      "      if (errno == EINTR) continue;",
      "      perror(\"read-response\");",
      "      return 24;",
      "    }",
      "    if (n == 0) break;",
      "    read_calls++;",
      "    socket_bytes += (unsigned long long)n;",
      "    if (write_all(out, buf, (size_t)n) != 0) { perror(\"write-body\"); return 26; }",
      "    account_body(buf, (size_t)n, &body_count, &checksum);",
      "  }",
      "  close(out);",
      "  close(sock);",
      "  printf(\"child-stream bytes=%llu checksum=%llu read_calls=%llu socket_bytes=%llu\\n\", body_count, checksum, read_calls, socket_bytes);",
      "  if (expected != 0 && body_count != expected) return 28;",
      "  return 0;",
      "}",
      "",
    ].join("\n"));
    writeFileSync(parentSourcePath, [
      "#include <errno.h>",
      "#include <fcntl.h>",
      "#include <signal.h>",
      "#include <stdio.h>",
      "#include <stdlib.h>",
      "#include <string.h>",
      "#include <sys/select.h>",
      "#include <sys/stat.h>",
      "#include <sys/wait.h>",
      "#include <time.h>",
      "#include <unistd.h>",
      "",
      "extern char **environ;",
      "",
      "static int set_nonblock(int fd) {",
      "  int flags = fcntl(fd, F_GETFL, 0);",
      "  if (flags < 0) return -1;",
      "  return fcntl(fd, F_SETFL, flags | O_NONBLOCK);",
      "}",
      "",
      "static int verify_file(const char *path, unsigned long long *bytes, unsigned long long *checksum, unsigned long long *zero_count, long long *first_zero) {",
      "  int fd = open(path, O_RDONLY);",
      "  if (fd < 0) return -1;",
      "  unsigned char buf[16384];",
      "  for (;;) {",
      "    ssize_t n = read(fd, buf, sizeof(buf));",
      "    if (n < 0) {",
      "      if (errno == EINTR) continue;",
      "      close(fd);",
      "      return -1;",
      "    }",
      "    if (n == 0) break;",
      "    for (ssize_t i = 0; i < n; i++) {",
      "      *checksum += (unsigned long long)buf[i];",
      "      if (buf[i] == 0) {",
      "        if (*first_zero < 0) *first_zero = (long long)(*bytes + (unsigned long long)i);",
      "        (*zero_count)++;",
      "      }",
      "    }",
      "    *bytes += (unsigned long long)n;",
      "  }",
      "  close(fd);",
      "  return 0;",
      "}",
      "",
      "int main(int argc, char **argv) {",
      "  unsigned long long expected_bytes = argc > 1 ? strtoull(argv[1], NULL, 10) : 0;",
      "  unsigned long long expected_checksum = argc > 2 ? strtoull(argv[2], NULL, 10) : 0;",
      "  if (mkdir(\"/tmp\", 0777) != 0 && errno != EEXIST) { perror(\"mkdir-tmp\"); return 9; }",
      "  unlink(\"/tmp/fork-stream.out\");",
      "  int out_pipe[2];",
      "  if (pipe(out_pipe) != 0) { perror(\"pipe\"); return 10; }",
      "  if (set_nonblock(out_pipe[0]) != 0) { perror(\"nonblock\"); return 11; }",
      "  pid_t child = fork();",
      "  if (child < 0) { perror(\"fork\"); return 12; }",
      "  if (child == 0) {",
      "    if (dup2(out_pipe[1], 1) < 0) { perror(\"dup2-stdout\"); _exit(125); }",
      "    if (dup2(out_pipe[1], 2) < 0) { perror(\"dup2-stderr\"); _exit(126); }",
      "    close(out_pipe[0]);",
      "    close(out_pipe[1]);",
      "    char expected_arg[32];",
      "    snprintf(expected_arg, sizeof(expected_arg), \"%llu\", expected_bytes);",
      "    char *child_argv[] = { (char *)\"/work/socket-file-child\", expected_arg, NULL };",
      "    execve(\"/work/socket-file-child\", child_argv, environ);",
      "    perror(\"execve-child\");",
      "    _exit(127);",
      "  }",
      "  close(out_pipe[1]);",
      "  char child_output[4096];",
      "  size_t child_output_len = 0;",
      "  unsigned long long pselect_calls = 0;",
      "  unsigned long long pselect_timeouts = 0;",
      "  unsigned long long wait_retries = 0;",
      "  unsigned long long pipe_reads = 0;",
      "  unsigned long long pipe_bytes = 0;",
      "  int child_done = 0;",
      "  int pipe_eof = 0;",
      "  int status = 0;",
      "  while (!child_done || !pipe_eof) {",
      "    if (!child_done) {",
      "      pid_t waited = waitpid(child, &status, WNOHANG);",
      "      if (waited == child) child_done = 1;",
      "      else if (waited == 0) wait_retries++;",
      "      else if (errno != EINTR) { perror(\"waitpid\"); return 13; }",
      "    }",
      "    fd_set rfds;",
      "    FD_ZERO(&rfds);",
      "    if (!pipe_eof) FD_SET(out_pipe[0], &rfds);",
      "    struct timespec timeout;",
      `    timeout.tv_sec = ${Math.floor(streamParentPselectTimeoutMs / 1000)};`,
      `    timeout.tv_nsec = ${(streamParentPselectTimeoutMs % 1000) * 1000 * 1000};`,
      "    pselect_calls++;",
      "    int ready = pselect(out_pipe[0] + 1, &rfds, NULL, NULL, &timeout, NULL);",
      "    if (ready < 0) {",
      "      if (errno == EINTR) continue;",
      "      perror(\"pselect\");",
      "      return 14;",
      "    }",
      "    if (ready == 0) pselect_timeouts++;",
      "    if (!pipe_eof && ready > 0 && FD_ISSET(out_pipe[0], &rfds)) {",
      "      for (;;) {",
      "        char buf[512];",
      "        ssize_t n = read(out_pipe[0], buf, sizeof(buf));",
      "        if (n < 0) {",
      "          if (errno == EINTR) continue;",
      "          if (errno == EAGAIN || errno == EWOULDBLOCK) break;",
      "          perror(\"read-pipe\");",
      "          return 15;",
      "        }",
      "        if (n == 0) { pipe_eof = 1; break; }",
      "        pipe_reads++;",
      "        pipe_bytes += (unsigned long long)n;",
      "        size_t copy = (size_t)n;",
      "        if (copy > sizeof(child_output) - 1 - child_output_len) copy = sizeof(child_output) - 1 - child_output_len;",
      "        memcpy(child_output + child_output_len, buf, copy);",
      "        child_output_len += copy;",
      "        child_output[child_output_len] = '\\0';",
      "      }",
      "    }",
      "  }",
      "  close(out_pipe[0]);",
      "  if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {",
      "    fprintf(stderr, \"child-status=%d output=%s\\n\", status, child_output);",
      "    return 16;",
      "  }",
      "  unsigned long long file_bytes = 0;",
      "  unsigned long long file_checksum = 0;",
      "  unsigned long long file_zero_count = 0;",
      "  long long file_first_zero = -1;",
      "  if (verify_file(\"/tmp/fork-stream.out\", &file_bytes, &file_checksum, &file_zero_count, &file_first_zero) != 0) { perror(\"verify-file\"); return 17; }",
      "  printf(\"fork-stream-file-ok bytes=%llu checksum=%llu first_zero=%lld zeros=%llu pselect_calls=%llu pselect_timeouts=%llu wait_retries=%llu pipe_reads=%llu pipe_bytes=%llu child_output=%s\",",
      "    file_bytes, file_checksum, file_first_zero, file_zero_count, pselect_calls, pselect_timeouts, wait_retries, pipe_reads, pipe_bytes, child_output);",
      "  if (expected_bytes != 0 && file_bytes != expected_bytes) return 18;",
      "  if (expected_checksum != 0 && file_checksum != expected_checksum) return 19;",
      "  return 0;",
      "}",
      "",
    ].join("\n"));
    execFileSync(hostRiscvGcc, ["-static", "-O2", "-o", childOutputPath, childSourcePath], { stdio: "pipe" });
    execFileSync(hostRiscvGcc, ["-static", "-O2", "-o", parentOutputPath, parentSourcePath], { stdio: "pipe" });
    return {
      parent: new Uint8Array(readFileSync(parentOutputPath)),
      child: new Uint8Array(readFileSync(childOutputPath)),
    };
  } catch (error) {
    const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
    return logSkip(`failed to build fork stream file probe with ${hostRiscvGcc}: ${detail}`);
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
}

function buildPayload(length: number): { bytes: Uint8Array; checksum: bigint } {
  const bytes = new Uint8Array(length);
  let checksum = 0n;
  for (let i = 0; i < bytes.length; i += 1) {
    const byte = (i * 17 + 23) & 0xff;
    bytes[i] = byte;
    checksum += BigInt(byte);
  }
  return { bytes, checksum };
}

function createStreamFileBridge(body: Uint8Array, asyncBridge = false): {
  bridge: RuntimeNetworkBridge;
  requests: string[];
  readChunks: number[];
  metrics: {
    readCalls: number;
    readBytes: number;
    maxReadRequestBytes: number;
  };
} {
  const requests: string[] = [];
  const readChunks: number[] = [];
  const metrics = {
    readCalls: 0,
    readBytes: 0,
    maxReadRequestBytes: 0,
  };
  const bridge: RuntimeNetworkBridge = {
    connect({ endpoint }) {
      if (endpoint.family !== "inet" || endpoint.address !== "127.0.0.1" || endpoint.port !== 8080) {
        return { errno: 111 };
      }
      let sawRequest = false;
      let offset = 0;
      let pendingRead: {
        resolve(): void;
        timer: ReturnType<typeof setTimeout>;
      } | null = null;
      const readNext = (maxBytes: number): Uint8Array | null | undefined => {
        if (!sawRequest) {
          return undefined;
        }
        if (offset >= body.length) {
          return null;
        }
        const chunk = body.subarray(offset, Math.min(body.length, offset + maxBytes));
        offset += chunk.length;
        readChunks.push(chunk.length);
        metrics.readBytes += chunk.length;
        return chunk;
      };
      const connection: RuntimeNetworkConnection = {
        write(data) {
          const request = decoder.decode(data);
          requests.push(request);
          if (request.includes("GET ")) {
            sawRequest = true;
          }
          if (pendingRead) {
            clearTimeout(pendingRead.timer);
            pendingRead.resolve();
          }
          pendingRead = null;
        },
        read: asyncBridge
          ? async (maxBytes) => {
              metrics.readCalls += 1;
              metrics.maxReadRequestBytes = Math.max(metrics.maxReadRequestBytes, maxBytes);
              if (!sawRequest) {
                await new Promise<void>((resolveDone) => {
                  const timer = setTimeout(() => {
                    if (pendingRead?.resolve === resolveDone) {
                      pendingRead = null;
                    }
                    resolveDone();
                  }, 100);
                  pendingRead = { resolve: resolveDone, timer };
                });
              }
              return readNext(maxBytes);
            }
          : (maxBytes) => {
              metrics.readCalls += 1;
              metrics.maxReadRequestBytes = Math.max(metrics.maxReadRequestBytes, maxBytes);
              return readNext(maxBytes);
            },
        close() {},
      };
      return { connection };
    },
  };

  return { bridge, requests, readChunks, metrics };
}

test("network fork stream file local gate: exec child writes socket body while parent pselect/waits", async () => {
  if (!runForkStreamFileGate) {
    return logSkip("set TIDEMARK_TEST_FORK_STREAM_FILE_GATE=1 to run fork stream file observation gate");
  }
  const wasmBytes = readKernelWasm();
  if (!wasmBytes) {
    return;
  }
  const probeBytes = buildForkStreamFileProbe();
  if (!probeBytes) {
    return;
  }

  const payload = buildPayload(streamBodyBytes);
  const { bridge, requests, readChunks, metrics } = createStreamFileBridge(payload.bytes, streamAsyncBridge);
  const runtime = await Runtime.create({ wasmBytes, networkBridge: bridge });
  try {
    await runtime.writeFile("/work/socket-file-child", probeBytes.child, 0o755);
    const started = Date.now();
    const result = await spawnWithOutput(
      runtime,
      probeBytes.parent,
      ["/work/fork-stream-file-parent", String(payload.bytes.length), payload.checksum.toString()],
      ["LC_ALL=C", "LANG=C"],
      streamTimeoutMs,
      { cwd: "/", stdin: "closed" },
    ).catch((error) => {
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error([
        detail,
        `bridgeReadCalls=${metrics.readCalls}`,
        `bridgeReadBytes=${metrics.readBytes}`,
        `bridgeChunks=${readChunks.length}`,
        `bridgeMaxReadRequestBytes=${metrics.maxReadRequestBytes}`,
      ].join("\n"));
    });
    expect(
      result.exitCode,
      [
        `output=${JSON.stringify(result.output)}`,
        `bridgeReadCalls=${metrics.readCalls}`,
        `bridgeReadBytes=${metrics.readBytes}`,
        `bridgeChunks=${readChunks.length}`,
        `bridgeMaxReadRequestBytes=${metrics.maxReadRequestBytes}`,
      ].join("\n"),
    ).toBe(0);
    expect(requests.join("")).toContain("GET http://stream-file.test/payload HTTP/1.1");
    expect(readChunks.length).toBeGreaterThan(1);
    expect(result.output).toContain(`fork-stream-file-ok bytes=${payload.bytes.length}`);
    expect(result.output).toContain(`checksum=${payload.checksum.toString()}`);
    expect(result.output).toContain(`child-stream bytes=${payload.bytes.length}`);
    expect(metrics.readBytes).toBe(payload.bytes.length);
    console.log([
      `network fork stream file metrics durationMs=${Date.now() - started}`,
      `asyncBridge=${streamAsyncBridge ? 1 : 0}`,
      `parentPselectTimeoutMs=${streamParentPselectTimeoutMs}`,
      `childReadCalls=${result.output.match(/read_calls=([0-9]+)/)?.[1] ?? "unknown"}`,
      `childSocketBytes=${result.output.match(/socket_bytes=([0-9]+)/)?.[1] ?? "unknown"}`,
      `parentPselectCalls=${result.output.match(/pselect_calls=([0-9]+)/)?.[1] ?? "unknown"}`,
      `parentPselectTimeouts=${result.output.match(/pselect_timeouts=([0-9]+)/)?.[1] ?? "unknown"}`,
      `parentWaitRetries=${result.output.match(/wait_retries=([0-9]+)/)?.[1] ?? "unknown"}`,
      `parentPipeReads=${result.output.match(/pipe_reads=([0-9]+)/)?.[1] ?? "unknown"}`,
      `bridgeReadCalls=${metrics.readCalls}`,
      `bridgeReadBytes=${metrics.readBytes}`,
      `bridgeChunks=${readChunks.length}`,
      `bridgeMaxReadRequestBytes=${metrics.maxReadRequestBytes}`,
    ].join(" "));
  } finally {
    runtime.destroy();
  }
}, 180_000);
