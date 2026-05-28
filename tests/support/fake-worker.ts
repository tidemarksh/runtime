import { vi } from "vite-plus/test";

export class FakeWorker {
  onmessage: ((event: MessageEvent<unknown>) => void) | null = null;
  terminated = false;
  terminate = vi.fn(() => {
    this.terminated = true;
  });

  constructor(
    readonly _url?: string | URL,
    readonly _options?: WorkerOptions,
  ) {}

  postMessage(_message: unknown): void {}

  emit(message: unknown): void {
    this.onmessage?.({ data: message } as MessageEvent<unknown>);
  }
}

export class AutoPreparedFakeWorker extends FakeWorker {
  postMessage(message: { type?: string }): void {
    if (message.type === "prepare") {
      queueMicrotask(() => {
        this.emit({ type: "prepared", wasmFeatures: {} });
      });
    }
  }
}
