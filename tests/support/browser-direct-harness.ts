import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { spawn } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import { createServer as createNetServer, type AddressInfo } from "node:net";
import type { FsSnapshotEntry } from "../../src/fs/types.js";
import {
  KERNEL_WASM_PATH,
  RUNTIME_DIST_PATH,
  RUNTIME_WORKER_DIST_DIR,
  logSkip,
} from "./runtime-workload.js";

export interface BrowserDirectRunSpec {
  executablePath: string;
  argv: string[];
  env: string[];
  cwd?: string;
}

export interface BrowserDirectPrepareShellSpec {
  executablePath: string;
  argv: string[];
  env: string[];
  cwd?: string;
  readyOutputIncludes?: string;
  timeoutMs?: number;
}

export interface BrowserDirectRuntimeOptions {
  guestMemoryPolicyLimitBytes?: number;
  kernelWorkerMaxMemoryPages?: number;
  maxProcessMemoryPages?: number;
  maxForkMemoryPages?: number;
}

export interface BrowserDirectStartupEntry {
  id: string;
  snapshot: FsSnapshotEntry[];
  prepareWritesAfterShellStart?: FsSnapshotEntry[];
  run: BrowserDirectRunSpec;
  prepareShell?: BrowserDirectPrepareShellSpec;
  runtimeOptions?: BrowserDirectRuntimeOptions;
  expectedExitCode: number;
  outputPattern: RegExp;
  timeoutMs: number;
}

export interface BrowserDirectStartupResult {
  exitCode: number;
  output: string;
  error: string | null;
  failureSnapshot: unknown;
}

interface SnapshotPayloadEntryDir {
  kind: "dir";
  path: string;
  mode?: number;
}

interface SnapshotPayloadEntryFile {
  kind: "file";
  path: string;
  mode?: number;
  dataBase64: string;
}

interface SnapshotPayloadEntrySymlink {
  kind: "symlink";
  path: string;
  mode?: number;
  targetBase64: string;
}

type SnapshotPayloadEntry =
  | SnapshotPayloadEntryDir
  | SnapshotPayloadEntryFile
  | SnapshotPayloadEntrySymlink;

interface FixtureConfigPayload {
  runtimeOptions: BrowserDirectRuntimeOptions;
  snapshot: SnapshotPayloadEntry[];
  prepareWritesAfterShellStart?: SnapshotPayloadEntry[];
  prepareShell?: BrowserDirectPrepareShellSpec;
  run: BrowserDirectRunSpec;
}

class CdpClient {
  private readonly pending = new Map<number, {
    resolve: (value: unknown) => void;
    reject: (reason?: unknown) => void;
  }>();
  private readonly eventHandlers = new Map<string, Array<(params: unknown) => void>>();

  private nextId = 1;

  constructor(private readonly ws: WebSocket) {
    this.ws.onmessage = (event) => {
      const message = JSON.parse(String(event.data)) as {
        id?: number;
        method?: string;
        params?: unknown;
        error?: unknown;
        result?: unknown;
      };
      if (!message.id) {
        if (!message.method) {
          return;
        }
        const handlers = this.eventHandlers.get(message.method);
        if (!handlers) {
          return;
        }
        for (const handler of handlers) {
          handler(message.params);
        }
        return;
      }
      const pending = this.pending.get(message.id);
      if (!pending) {
        return;
      }
      this.pending.delete(message.id);
      if (message.error) {
        pending.reject(new Error(JSON.stringify(message.error)));
        return;
      }
      pending.resolve(message.result ?? {});
    };
  }

  async send<T>(method: string, params: Record<string, unknown> = {}): Promise<T> {
    return await new Promise<T>((resolve, reject) => {
      const id = this.nextId++;
      this.pending.set(id, { resolve, reject });
      this.ws.send(JSON.stringify({ id, method, params }));
    });
  }

  async eval<T>(expression: string): Promise<T> {
    const result = await this.send<{
      result?: { value?: T };
      exceptionDetails?: unknown;
    }>("Runtime.evaluate", {
      expression,
      awaitPromise: true,
      returnByValue: true,
    });
    if (result.exceptionDetails) {
      throw new Error(JSON.stringify(result.exceptionDetails));
    }
    return result.result?.value as T;
  }

  on(method: string, handler: (params: unknown) => void): void {
    const handlers = this.eventHandlers.get(method) ?? [];
    handlers.push(handler);
    this.eventHandlers.set(method, handlers);
  }

  async close(): Promise<void> {
    if (this.ws.readyState === WebSocket.OPEN) {
      this.ws.close();
    }
  }
}

async function allocateLoopbackPort(): Promise<number> {
  const server = createNetServer();
  await new Promise<void>((resolve, reject) => {
    server.listen(0, "127.0.0.1", () => resolve());
    server.on("error", reject);
  });
  const address = server.address();
  if (!address || typeof address === "string") {
    await new Promise<void>((resolve) => server.close(() => resolve()));
    throw new Error("failed to allocate browser direct CDP port");
  }
  await new Promise<void>((resolve, reject) => {
    server.close((error) => error ? reject(error) : resolve());
  });
  return address.port;
}

function encodeBytes(bytes: Uint8Array): string {
  return Buffer.from(bytes).toString("base64");
}

function serializeSnapshot(entries: readonly FsSnapshotEntry[]): SnapshotPayloadEntry[] {
  return entries.map((entry) => {
    switch (entry.kind) {
      case "dir":
        return {
          kind: "dir",
          path: entry.path,
          mode: entry.mode,
        };
      case "file":
        return {
          kind: "file",
          path: entry.path,
          mode: entry.mode,
          dataBase64: encodeBytes(entry.data),
        };
      case "symlink":
        return {
          kind: "symlink",
          path: entry.path,
          mode: entry.mode,
          targetBase64: encodeBytes(entry.target),
        };
    }
  });
}

function fixtureHtml(): string {
  return [
    "<!doctype html>",
    "<html>",
    "<head>",
    '  <meta charset="utf-8" />',
    "  <title>Tidemark Browser Direct Fixture</title>",
    "</head>",
    "<body>",
    "  <div id=\"app\">booting</div>",
    '  <script type="module" src="/fixture.mjs"></script>',
    "</body>",
    "</html>",
  ].join("\n");
}

function applyDecodedSnapshotEntries(): string[] {
  return [
    "async function applySnapshotEntries(runtime, entries) {",
    "  for (const entry of decodeSnapshot(entries)) {",
    "    if (entry.kind === 'dir') {",
    "      await runtime.fs.mkdir(entry.path, entry.mode).catch(() => {});",
    "      continue;",
    "    }",
    "    if (entry.kind === 'file') {",
    "      await runtime.fs.writeFile(entry.path, entry.data, entry.mode);",
    "      continue;",
    "    }",
    "    await runtime.fs.symlink(entry.path, entry.target, entry.mode);",
    "  }",
    "}",
  ];
}

function fixtureModuleSource(): string {
  return [
    "import { Runtime } from '/runtime/index.mjs';",
    "const decoder = new TextDecoder();",
    "function decodeBase64(value) {",
    "  const raw = atob(value);",
    "  const bytes = new Uint8Array(raw.length);",
    "  for (let index = 0; index < raw.length; index += 1) bytes[index] = raw.charCodeAt(index);",
    "  return bytes;",
    "}",
    "function decodeSnapshot(entries) {",
    "  return entries.map((entry) => {",
    "    if (entry.kind === 'dir') return { kind: 'dir', path: entry.path, mode: entry.mode };",
    "    if (entry.kind === 'file') return { kind: 'file', path: entry.path, mode: entry.mode, data: decodeBase64(entry.dataBase64) };",
    "    return { kind: 'symlink', path: entry.path, mode: entry.mode, target: decodeBase64(entry.targetBase64) };",
    "  });",
    "}",
    ...applyDecodedSnapshotEntries(),
    "async function waitForOutput(proc, needle, timeoutMs) {",
    "  if (!needle) return '';",
    "  let output = '';",
    "  proc.onStdout((data) => { output += decoder.decode(data); });",
    "  const deadline = Date.now() + timeoutMs;",
    "  while (Date.now() < deadline) {",
    "    if (output.includes(needle)) return output;",
    "    await new Promise((resolve) => setTimeout(resolve, 20));",
    "  }",
    "  throw new Error(`timeout waiting for shell output: ${needle}: ${JSON.stringify(output)}`);",
    "}",
    "async function boot() {",
    "  const [configResponse, wasmResponse] = await Promise.all([fetch('/config.json'), fetch('/kernel.wasm')]);",
    "  if (!configResponse.ok) throw new Error(`config fetch failed: ${configResponse.status}`);",
    "  if (!wasmResponse.ok) throw new Error(`kernel fetch failed: ${wasmResponse.status}`);",
    "  const config = await configResponse.json();",
    "  const wasmBytes = await wasmResponse.arrayBuffer();",
    "  const runtime = await Runtime.create({ wasmBytes, ...config.runtimeOptions });",
    "  await runtime.applyFsSnapshot(decodeSnapshot(config.snapshot));",
    "  let preparedShell = null;",
    "  window.__tidemarkBrowserDirect = {",
    "    async run() {",
    "      if (config.prepareShell && !preparedShell) {",
    "        const shellBytes = await runtime.readFile(config.prepareShell.executablePath);",
    "        preparedShell = await runtime.spawn(",
    "          shellBytes,",
    "          config.prepareShell.argv,",
    "          config.prepareShell.env,",
    "          config.prepareShell.cwd ? { cwd: config.prepareShell.cwd } : {},",
    "        );",
    "        await waitForOutput(",
    "          preparedShell,",
    "          config.prepareShell.readyOutputIncludes ?? '',",
    "          config.prepareShell.timeoutMs ?? 5000,",
    "        );",
    "        if (config.prepareWritesAfterShellStart?.length) {",
    "          await applySnapshotEntries(runtime, config.prepareWritesAfterShellStart);",
    "        }",
    "      }",
    "      const elfBytes = await runtime.readFile(config.run.executablePath);",
    "      const proc = await runtime.spawn(elfBytes, config.run.argv, config.run.env, config.run.cwd ? { cwd: config.run.cwd } : {});",
    "      let output = '';",
    "      proc.onStdout((data) => { output += decoder.decode(data); });",
    "      try {",
    "        const exitCode = await proc.waitForExit();",
    "        return { exitCode, output, error: null, failureSnapshot: runtime.debugLastFailureSnapshot() ?? null };",
    "      } catch (error) {",
    "        const detail = error instanceof Error ? error.message : String(error);",
    "        return { exitCode: -1, output, error: detail, failureSnapshot: runtime.debugLastFailureSnapshot() ?? null };",
    "      }",
    "    },",
    "    failureSnapshot() { return runtime.debugLastFailureSnapshot() ?? null; },",
    "  };",
    "  window.__tidemarkBrowserDirectReady = true;",
    "  document.getElementById('app').textContent = 'ready';",
    "}",
    "boot().catch((error) => {",
    "  window.__tidemarkBrowserDirectBootError = error instanceof Error ? (error.stack ?? error.message) : String(error);",
    "  document.getElementById('app').textContent = `boot failed: ${window.__tidemarkBrowserDirectBootError}`;",
    "});",
  ].join("\n");
}

function setCommonHeaders(response: ServerResponse, contentType: string): void {
  response.setHeader("content-type", contentType);
  response.setHeader("Cross-Origin-Opener-Policy", "same-origin");
  response.setHeader("Cross-Origin-Embedder-Policy", "require-corp");
  response.setHeader("Cross-Origin-Resource-Policy", "same-origin");
  response.setHeader("Cache-Control", "no-store");
}

function runtimeDistContentType(pathname: string): string {
  if (pathname.endsWith(".mjs")) {
    return "text/javascript; charset=utf-8";
  }
  if (pathname.endsWith(".js")) {
    return "text/javascript; charset=utf-8";
  }
  if (pathname.endsWith(".json")) {
    return "application/json; charset=utf-8";
  }
  if (pathname.endsWith(".wasm")) {
    return "application/wasm";
  }
  return "application/octet-stream";
}

function respondNotFound(response: ServerResponse): void {
  response.statusCode = 404;
  response.end("not found");
}

function createFixtureServer(config: FixtureConfigPayload) {
  const fixtureModule = fixtureModuleSource();
  const html = fixtureHtml();
  const kernelWasm = readFileSync(KERNEL_WASM_PATH);
  const configJson = Buffer.from(JSON.stringify(config));

  return createServer((request: IncomingMessage, response: ServerResponse) => {
    const url = new URL(request.url ?? "/", "http://127.0.0.1");
    switch (url.pathname) {
      case "/":
        setCommonHeaders(response, "text/html; charset=utf-8");
        response.end(html);
        return;
      case "/fixture.mjs":
        setCommonHeaders(response, "text/javascript; charset=utf-8");
        response.end(fixtureModule);
        return;
      case "/config.json":
        setCommonHeaders(response, "application/json; charset=utf-8");
        response.end(configJson);
        return;
      case "/kernel.wasm":
        setCommonHeaders(response, "application/wasm");
        response.end(kernelWasm);
        return;
      case "/thread-worker.mjs":
      case "/kernel-worker.mjs": {
        const runtimePath = resolve(RUNTIME_WORKER_DIST_DIR, url.pathname.slice(1));
        if (!existsSync(runtimePath)) {
          respondNotFound(response);
          return;
        }
        setCommonHeaders(response, runtimeDistContentType(url.pathname));
        response.end(readFileSync(runtimePath));
        return;
      }
      default:
        if (
          url.pathname.startsWith("/env-")
          || url.pathname.startsWith("/features-")
          || url.pathname.startsWith("/scope-")
          || /^[A-Za-z0-9._-]+\.mjs$/.test(url.pathname.slice(1))
        ) {
          const runtimePath = resolve(RUNTIME_WORKER_DIST_DIR, url.pathname.slice(1));
          if (!existsSync(runtimePath)) {
            respondNotFound(response);
            return;
          }
          setCommonHeaders(response, runtimeDistContentType(url.pathname));
          response.end(readFileSync(runtimePath));
          return;
        }
        if (url.pathname.startsWith("/runtime/")) {
          const relativePath = url.pathname.slice("/runtime/".length);
          if (relativePath.includes("..") || relativePath.length === 0) {
            respondNotFound(response);
            return;
          }
          const runtimePath = resolve(RUNTIME_WORKER_DIST_DIR, relativePath);
          if (!runtimePath.startsWith(`${RUNTIME_WORKER_DIST_DIR}/`) || !existsSync(runtimePath)) {
            respondNotFound(response);
            return;
          }
          setCommonHeaders(response, runtimeDistContentType(url.pathname));
          response.end(readFileSync(runtimePath));
          return;
        }
        respondNotFound(response);
    }
  });
}

async function waitForJson<T>(url: string, timeoutMs = 30_000): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  let lastError = "not-started";
  while (Date.now() < deadline) {
    try {
      const response = await fetch(url);
      if (response.ok) {
        return await response.json() as T;
      }
      lastError = `status=${response.status}`;
    } catch (error) {
      lastError = error instanceof Error ? error.message : String(error);
    }
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error(`timeout waiting for json ${url}: ${lastError}`);
}

async function connectToPage(baseUrl: string, cdpPort: number): Promise<CdpClient> {
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    const pages = await waitForJson<Array<{
      type: string;
      url: string;
      webSocketDebuggerUrl?: string;
    }>>(`http://127.0.0.1:${cdpPort}/json/list`);
    const page = pages.find((entry) => entry.type === "page" && entry.url.startsWith(baseUrl));
    if (page?.webSocketDebuggerUrl) {
      const ws = new WebSocket(page.webSocketDebuggerUrl);
      await new Promise<void>((resolve, reject) => {
        ws.onopen = () => resolve();
        ws.onerror = (event) => reject(event);
      });
      const client = new CdpClient(ws);
      await client.send("Page.enable");
      await client.send("Runtime.enable");
      return client;
    }
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error(`fixture page target not found for ${baseUrl}`);
}

async function waitForFixtureReady(client: CdpClient, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const ready = await client.eval<boolean>("Boolean(window.__tidemarkBrowserDirectReady)");
    if (ready) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  const [bootError, domText] = await Promise.all([
    client.eval<string | null>("window.__tidemarkBrowserDirectBootError ?? null").catch(() => null),
    client.eval<string | null>("document.body?.innerText ?? null").catch(() => null),
  ]);
  throw new Error(
    [
      "browser direct fixture did not become ready",
      `bootError=${bootError ?? "null"}`,
      `dom=${JSON.stringify(domText ?? "")}`,
    ].join(": "),
  );
}

export async function runBrowserDirectStartupEntry(
  entry: BrowserDirectStartupEntry,
): Promise<BrowserDirectStartupResult | null> {
  if (!KERNEL_WASM_PATH) {
    return logSkip("missing kernel wasm path: set TIDEMARK_TEST_KERNEL_WASM");
  }
  if (!existsSync(KERNEL_WASM_PATH)) {
    return logSkip(`kernel release wasm not found: ${KERNEL_WASM_PATH}`);
  }

  const chromiumBin = process.env.CHROMIUM_BIN ?? "/usr/bin/chromium";
  if (!existsSync(chromiumBin)) {
    return logSkip(`missing Chromium binary: ${chromiumBin}`);
  }

  const server = createFixtureServer({
    runtimeOptions: entry.runtimeOptions ?? {},
    snapshot: serializeSnapshot(entry.snapshot),
    prepareWritesAfterShellStart: entry.prepareWritesAfterShellStart ? serializeSnapshot(entry.prepareWritesAfterShellStart) : undefined,
    prepareShell: entry.prepareShell,
    run: entry.run,
  });
  await new Promise<void>((resolve, reject) => {
    server.listen(0, "127.0.0.1", () => resolve());
    server.on("error", reject);
  });

  const port = (server.address() as AddressInfo).port;
  const baseUrl = `http://127.0.0.1:${port}/`;
  const cdpPort = process.env.TIDEMARK_BROWSER_DIRECT_CDP_PORT
    ? Number(process.env.TIDEMARK_BROWSER_DIRECT_CDP_PORT)
    : await allocateLoopbackPort();
  const userDataDir = await mkdtemp(`${tmpdir()}/tidemark-browser-direct-`);
  const chromeProc = spawn(
    chromiumBin,
    [
      "--headless=new",
      "--disable-gpu",
      "--no-first-run",
      "--no-default-browser-check",
      "--no-sandbox",
      `--remote-debugging-port=${cdpPort}`,
      `--user-data-dir=${userDataDir}`,
      baseUrl,
    ],
    { stdio: ["ignore", "ignore", "pipe"] },
  );
  let chromeStderr = "";
  chromeProc.stderr?.on("data", (chunk) => {
    chromeStderr += chunk.toString();
  });

  let client: CdpClient | null = null;
  const browserConsole: string[] = [];
  const browserExceptions: string[] = [];
  try {
    await waitForJson(`http://127.0.0.1:${cdpPort}/json/version`);
    client = await connectToPage(baseUrl, cdpPort);
    client.on("Runtime.consoleAPICalled", (params) => {
      const payload = params as {
        type?: string;
        args?: Array<{ value?: unknown; description?: string }>;
      };
      const args = (payload.args ?? []).map((arg) => {
        if ("value" in arg && arg.value !== undefined) {
          return String(arg.value);
        }
        return arg.description ?? "<unserializable>";
      });
      browserConsole.push(`${payload.type ?? "console"}:${args.join(" ")}`);
    });
    client.on("Runtime.exceptionThrown", (params) => {
      const payload = params as {
        exceptionDetails?: { text?: string; exception?: { description?: string } };
      };
      const details = payload.exceptionDetails;
      browserExceptions.push(
        details?.exception?.description
          ?? details?.text
          ?? JSON.stringify(payload),
      );
    });
    await waitForFixtureReady(client, entry.timeoutMs);
    const result = await client.eval<BrowserDirectStartupResult>(
      "window.__tidemarkBrowserDirect.run()",
    );
    return result;
  } catch (error) {
    const domText = client
      ? await client.eval<string | null>("document.body?.innerText ?? null").catch(() => null)
      : null;
    const detail = error instanceof Error ? error.message : String(error);
    throw new Error([
      detail,
      `browserConsole=${JSON.stringify(browserConsole)}`,
      `browserExceptions=${JSON.stringify(browserExceptions)}`,
      `chromeStderr=${JSON.stringify(chromeStderr.trim())}`,
      `dom=${JSON.stringify(domText ?? "")}`,
    ].join("\n"));
  } finally {
    await client?.close().catch(() => {});
    if (!chromeProc.killed) {
      chromeProc.kill("SIGTERM");
    }
    await rm(userDataDir, { recursive: true, force: true }).catch(() => {});
    await new Promise<void>((resolve, reject) => {
      server.close((error) => (error ? reject(error) : resolve()));
    }).catch(() => {});
  }
}
