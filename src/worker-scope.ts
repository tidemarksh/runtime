type WorkerHandler<T> = (event: MessageEvent<T>) => void | Promise<void>;

type NodeParentPort = {
  postMessage(message: unknown): void;
  on(event: "message", listener: (message: unknown) => void): void;
};

let cachedParentPort: NodeParentPort | null | undefined;

async function getNodeParentPort() {
  if (cachedParentPort !== undefined) {
    return cachedParentPort;
  }
  try {
    const workerThreads = await import("node:worker_threads");
    cachedParentPort = workerThreads.parentPort as NodeParentPort | null;
  } catch {
    cachedParentPort = null;
  }
  return cachedParentPort;
}

export function workerPostMessage(message: unknown): void {
  if (typeof globalThis.postMessage === "function") {
    globalThis.postMessage(message);
    return;
  }
  if (cachedParentPort) {
    cachedParentPort.postMessage(message);
    return;
  }
  throw new Error("worker postMessage bridge is unavailable");
}

export async function bindWorkerMessageHandler<T>(handler: WorkerHandler<T>): Promise<void> {
  if ("onmessage" in globalThis) {
    (globalThis as typeof globalThis & {
      onmessage: WorkerHandler<T> | null;
    }).onmessage = handler;
    return;
  }

  const parentPort = await getNodeParentPort();
  if (!parentPort) {
    throw new Error("worker message bridge is unavailable");
  }
  parentPort.on("message", (data) => {
    void handler({ data } as MessageEvent<T>);
  });
}
