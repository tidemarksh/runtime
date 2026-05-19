import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "vite-plus/test";
import type { RuntimeNetworkBridge, RuntimeNetworkConnection } from "../../src/network.js";
import {
  Runtime,
  decoder,
  logSkip,
  readKernelWasm,
  spawnWithOutput,
} from "../support/runtime-workload.js";

const hostRiscvGcc = process.env.TIDEMARK_TEST_HOST_RISCV_GCC ?? "riscv64-linux-gnu-gcc";
const largeBodyBytes = 2 * 1024 * 1024;

function buildLargeHttpClientProbe(): Uint8Array | null {
  const tmp = mkdtempSync(join(tmpdir(), "tidemark-network-large-response-"));
  const sourcePath = join(tmp, "large_http_client.c");
  const outputPath = join(tmp, "large-http-client");
  try {
    writeFileSync(sourcePath, [
      "#include <arpa/inet.h>",
      "#include <errno.h>",
      "#include <netinet/in.h>",
      "#include <stdio.h>",
      "#include <stdlib.h>",
      "#include <string.h>",
      "#include <sys/socket.h>",
      "#include <unistd.h>",
      "",
      "static int write_all(int fd, const char *data, size_t len) {",
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
      "static void consume_body(const unsigned char *data, size_t len, unsigned long long *count, unsigned long long *sum) {",
      "  for (size_t i = 0; i < len; i++) {",
      "    *sum += (unsigned long long)data[i];",
      "  }",
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
      "  const char *request = \"GET http://large.test/payload HTTP/1.1\\r\\nHost: large.test\\r\\nConnection: close\\r\\n\\r\\n\";",
      "  if (write_all(sock, request, strlen(request)) != 0) { perror(\"write-request\"); return 22; }",
      "  unsigned char buf[8192];",
      "  unsigned char header[16384];",
      "  size_t header_len = 0;",
      "  int saw_header = 0;",
      "  unsigned long long body_count = 0;",
      "  unsigned long long checksum = 0;",
      "  unsigned long long read_calls = 0;",
      "  unsigned long long socket_bytes = 0;",
      "  for (;;) {",
      "    ssize_t n = read(sock, buf, sizeof(buf));",
      "    if (n < 0) {",
      "      if (errno == EINTR) continue;",
      "      perror(\"read-response\");",
      "      return 23;",
      "    }",
      "    if (n == 0) break;",
      "    read_calls++;",
      "    socket_bytes += (unsigned long long)n;",
      "    if (!saw_header) {",
      "      if (header_len + (size_t)n > sizeof(header)) {",
      "        fprintf(stderr, \"header-too-large\\n\");",
      "        return 24;",
      "      }",
      "      memcpy(header + header_len, buf, (size_t)n);",
      "      header_len += (size_t)n;",
      "      char *body = NULL;",
      "      for (size_t i = 3; i < header_len; i++) {",
      "        if (header[i - 3] == '\\r' && header[i - 2] == '\\n' && header[i - 1] == '\\r' && header[i] == '\\n') {",
      "          body = (char *)(header + i + 1);",
      "          break;",
      "        }",
      "      }",
      "      if (body == NULL) continue;",
      "      saw_header = 1;",
      "      size_t body_len = header_len - (size_t)((unsigned char *)body - header);",
      "      consume_body((const unsigned char *)body, body_len, &body_count, &checksum);",
      "      continue;",
      "    }",
      "    consume_body(buf, (size_t)n, &body_count, &checksum);",
      "  }",
      "  close(sock);",
      "  if (!saw_header) { fprintf(stderr, \"missing-header\\n\"); return 25; }",
      "  printf(\"large-http bytes=%llu checksum=%llu read_calls=%llu socket_bytes=%llu\\n\", body_count, checksum, read_calls, socket_bytes);",
      "  if (expected != 0 && body_count != expected) return 26;",
      "  return 0;",
      "}",
      "",
    ].join("\n"));
    execFileSync(hostRiscvGcc, ["-static", "-O2", "-o", outputPath, sourcePath], {
      stdio: "pipe",
    });
    return new Uint8Array(readFileSync(outputPath));
  } catch (error) {
    const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
    return logSkip(`failed to build large HTTP client probe with ${hostRiscvGcc}: ${detail}`);
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

function createLargeResponseBridge(body: Uint8Array): {
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
  const head = new TextEncoder().encode([
    "HTTP/1.1 200 OK",
    `Content-Length: ${body.length}`,
    "Connection: close",
    "",
    "",
  ].join("\r\n"));
  const response = new Uint8Array(head.length + body.length);
  response.set(head);
  response.set(body, head.length);

  const bridge: RuntimeNetworkBridge = {
    connect({ endpoint }) {
      if (endpoint.family !== "inet" || endpoint.address !== "127.0.0.1" || endpoint.port !== 8080) {
        return { errno: 111 };
      }
      let sawRequest = false;
      let offset = 0;
      const connection: RuntimeNetworkConnection = {
        write(data) {
          const request = decoder.decode(data);
          requests.push(request);
          if (request.includes("GET ")) {
            sawRequest = true;
          }
        },
        read(maxBytes) {
          metrics.readCalls += 1;
          metrics.maxReadRequestBytes = Math.max(metrics.maxReadRequestBytes, maxBytes);
          if (!sawRequest) {
            return undefined;
          }
          if (offset >= response.length) {
            return null;
          }
          const chunk = response.subarray(offset, Math.min(response.length, offset + maxBytes));
          offset += chunk.length;
          readChunks.push(chunk.length);
          metrics.readBytes += chunk.length;
          return chunk;
        },
        close() {},
      };
      return { connection };
    },
  };

  return { bridge, requests, readChunks, metrics };
}

test("network large response local gate: guest reads multi-megabyte HTTP body byte-for-byte", async () => {
  const wasmBytes = readKernelWasm();
  if (!wasmBytes) {
    return;
  }
  const probeBytes = buildLargeHttpClientProbe();
  if (!probeBytes) {
    return;
  }

  const payload = buildPayload(largeBodyBytes);
  const { bridge, requests, readChunks, metrics } = createLargeResponseBridge(payload.bytes);
  const runtime = await Runtime.create({ wasmBytes, networkBridge: bridge });
  try {
    const started = Date.now();
    const result = await spawnWithOutput(
      runtime,
      probeBytes,
      ["/work/large-http-client", String(payload.bytes.length)],
      ["LC_ALL=C", "LANG=C"],
      120_000,
      { cwd: "/", stdin: "closed" },
    );
    expect(result.exitCode, result.output + (result.debugInfo ? `\n${result.debugInfo}` : "")).toBe(0);
    expect(requests.join("")).toContain("GET http://large.test/payload HTTP/1.1");
    expect(readChunks.length).toBeGreaterThan(1);
    expect(readChunks.some((chunk) => chunk >= 128 * 1024)).toBe(true);
    expect(result.output).toContain(`large-http bytes=${payload.bytes.length}`);
    expect(result.output).toContain(`checksum=${payload.checksum.toString()}`);
    expect(result.output).toMatch(/read_calls=\d+/);
    expect(result.output).toMatch(/socket_bytes=\d+/);
    expect(metrics.readBytes).toBeGreaterThan(payload.bytes.length);
    console.log([
      `network large response metrics durationMs=${Date.now() - started}`,
      `guestReadCalls=${result.output.match(/read_calls=([0-9]+)/)?.[1] ?? "unknown"}`,
      `guestSocketBytes=${result.output.match(/socket_bytes=([0-9]+)/)?.[1] ?? "unknown"}`,
      `bridgeReadCalls=${metrics.readCalls}`,
      `bridgeReadBytes=${metrics.readBytes}`,
      `bridgeChunks=${readChunks.length}`,
      `bridgeMaxReadRequestBytes=${metrics.maxReadRequestBytes}`,
    ].join(" "));
  } finally {
    runtime.destroy();
  }
}, 150_000);
