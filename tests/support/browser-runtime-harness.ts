import { spawn } from "node:child_process";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { existsSync, readFileSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { createServer as createNetServer, type AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import {
  KERNEL_WASM_PATH,
  RUNTIME_WORKER_DIST_DIR,
  logSkip,
} from "./runtime-workload.js";

export interface BrowserRuntimeScenarioOptions {
  config: unknown;
  moduleSource: string;
  timeoutMs: number;
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
    throw new Error("failed to allocate browser runtime CDP port");
  }
  await new Promise<void>((resolve, reject) => {
    server.close((error) => error ? reject(error) : resolve());
  });
  return address.port;
}

function setCommonHeaders(response: ServerResponse, contentType: string): void {
  response.setHeader("content-type", contentType);
  response.setHeader("Cross-Origin-Opener-Policy", "same-origin");
  response.setHeader("Cross-Origin-Embedder-Policy", "require-corp");
  response.setHeader("Cross-Origin-Resource-Policy", "same-origin");
  response.setHeader("Cache-Control", "no-store");
}

function runtimeDistContentType(pathname: string): string {
  if (pathname.endsWith(".mjs") || pathname.endsWith(".js")) {
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

function createFixtureServer(options: BrowserRuntimeScenarioOptions) {
  const configJson = Buffer.from(JSON.stringify(options.config));
  const kernelWasm = readFileSync(KERNEL_WASM_PATH);

  return createServer((request: IncomingMessage, response: ServerResponse) => {
    const url = new URL(request.url ?? "/", "http://127.0.0.1");
    switch (url.pathname) {
      case "/":
        setCommonHeaders(response, "text/html; charset=utf-8");
        response.end([
          "<!doctype html>",
          "<html>",
          "<head><meta charset=\"utf-8\" /><title>Tidemark Browser Runtime Fixture</title></head>",
          "<body>",
          "  <div id=\"app\">booting</div>",
          "  <script type=\"module\" src=\"/scenario.mjs\"></script>",
          "</body>",
          "</html>",
        ].join("\n"));
        return;
      case "/scenario.mjs":
        setCommonHeaders(response, "text/javascript; charset=utf-8");
        response.end(options.moduleSource);
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

async function waitForScenarioReady(client: CdpClient, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const ready = await client.eval<boolean>("Boolean(window.__tidemarkBrowserRuntimeReady)");
    if (ready) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  const [bootError, domText] = await Promise.all([
    client.eval<string | null>("window.__tidemarkBrowserRuntimeBootError ?? null").catch(() => null),
    client.eval<string | null>("document.body?.innerText ?? null").catch(() => null),
  ]);
  throw new Error([
    "browser runtime fixture did not become ready",
    `bootError=${bootError ?? "null"}`,
    `dom=${JSON.stringify(domText ?? "")}`,
  ].join(": "));
}

export async function runBrowserRuntimeScenario<T>(
  options: BrowserRuntimeScenarioOptions,
): Promise<T | null> {
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

  const server = createFixtureServer(options);
  await new Promise<void>((resolve, reject) => {
    server.listen(0, "127.0.0.1", () => resolve());
    server.on("error", reject);
  });

  const port = (server.address() as AddressInfo).port;
  const baseUrl = `http://127.0.0.1:${port}/`;
  const cdpPort = process.env.TIDEMARK_BROWSER_RUNTIME_CDP_PORT
    ? Number(process.env.TIDEMARK_BROWSER_RUNTIME_CDP_PORT)
    : await allocateLoopbackPort();
  const userDataDir = await mkdtemp(`${tmpdir()}/tidemark-browser-runtime-`);
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
    await waitForScenarioReady(client, options.timeoutMs);
    return await client.eval<T>("window.__tidemarkBrowserRuntime.run()");
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
