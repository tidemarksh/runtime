type WorkerLike<T> = Worker & {
  on?: (event: "message", listener: (data: T) => void) => void;
  off?: (event: "message", listener: (data: T) => void) => void;
};

const nodeListenerMap = new WeakMap<Worker, (data: unknown) => void>();
const workerErrorListenerMap = new WeakMap<Worker, EventListener>();
const workerMessageErrorListenerMap = new WeakMap<Worker, EventListener>();

export function addWorkerMessageListener<T>(
  worker: Worker,
  listener: (event: MessageEvent<T>) => void,
): void {
  if ("addEventListener" in worker && typeof worker.addEventListener === "function") {
    worker.addEventListener("message", listener as EventListener);
    return;
  }

  const nodeWorker = worker as WorkerLike<T>;
  if (typeof nodeWorker.on === "function") {
    const wrapped = (data: T) => {
      listener({ data } as MessageEvent<T>);
    };
    nodeListenerMap.set(worker, wrapped as (data: unknown) => void);
    nodeWorker.on("message", wrapped);
  }
}

export function removeWorkerMessageListener<T>(
  worker: Worker,
  listener: (event: MessageEvent<T>) => void,
): void {
  if ("removeEventListener" in worker && typeof worker.removeEventListener === "function") {
    worker.removeEventListener("message", listener as EventListener);
    return;
  }

  const nodeWorker = worker as WorkerLike<T>;
  const wrapped = nodeListenerMap.get(worker);
  if (wrapped && typeof nodeWorker.off === "function") {
    nodeWorker.off("message", wrapped as (data: T) => void);
  }
  nodeListenerMap.delete(worker);
}

export function setWorkerMessageHandler<T>(
  worker: Worker,
  listener: (event: MessageEvent<T>) => void,
): void {
  if ("onmessage" in worker) {
    worker.onmessage = listener as ((this: Worker, ev: MessageEvent<unknown>) => unknown);
    return;
  }

  const existing = nodeListenerMap.get(worker);
  const nodeWorker = worker as WorkerLike<T>;
  if (existing && typeof nodeWorker.off === "function") {
    nodeWorker.off("message", existing as (data: T) => void);
  }
  if (typeof nodeWorker.on === "function") {
    const wrapped = (data: T) => {
      listener({ data } as MessageEvent<T>);
    };
    nodeListenerMap.set(worker, wrapped as (data: unknown) => void);
    nodeWorker.on("message", wrapped);
  }
}

export function clearWorkerMessageHandler(worker: Worker): void {
  if ("onmessage" in worker) {
    worker.onmessage = null;
    return;
  }

  const nodeWorker = worker as WorkerLike<unknown>;
  const wrapped = nodeListenerMap.get(worker);
  if (wrapped && typeof nodeWorker.off === "function") {
    nodeWorker.off("message", wrapped);
  }
  nodeListenerMap.delete(worker);
}

export function setWorkerErrorHandler(
  worker: Worker,
  listener: ((event: ErrorEvent) => void) | null,
): void {
  if ("onerror" in worker) {
    worker.onerror = listener as Worker["onerror"];
    return;
  }
  const eventWorker = worker as Worker & EventTarget;
  const existing = workerErrorListenerMap.get(worker);
  if (existing && typeof eventWorker.removeEventListener === "function") {
    eventWorker.removeEventListener("error", existing);
  }
  if (listener && typeof eventWorker.addEventListener === "function") {
    const wrapped = listener as unknown as EventListener;
    workerErrorListenerMap.set(worker, wrapped);
    eventWorker.addEventListener("error", wrapped);
    return;
  }
  workerErrorListenerMap.delete(worker);
}

export function setWorkerMessageErrorHandler(
  worker: Worker,
  listener: ((event: MessageEvent<unknown>) => void) | null,
): void {
  if ("onmessageerror" in worker) {
    worker.onmessageerror = listener as Worker["onmessageerror"];
    return;
  }
  const eventWorker = worker as Worker & EventTarget;
  const existing = workerMessageErrorListenerMap.get(worker);
  if (existing && typeof eventWorker.removeEventListener === "function") {
    eventWorker.removeEventListener("messageerror", existing);
  }
  if (listener && typeof eventWorker.addEventListener === "function") {
    const wrapped = listener as unknown as EventListener;
    workerMessageErrorListenerMap.set(worker, wrapped);
    eventWorker.addEventListener("messageerror", wrapped);
    return;
  }
  workerMessageErrorListenerMap.delete(worker);
}
