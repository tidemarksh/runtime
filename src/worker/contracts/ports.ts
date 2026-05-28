export interface RuntimeMessagePort<Inbound, Outbound> {
  postMessage(message: Outbound, transfer?: Transferable[]): void;
  addEventListener(
    type: "message",
    listener: (event: MessageEvent<Inbound>) => void,
  ): void;
  removeEventListener(
    type: "message",
    listener: (event: MessageEvent<Inbound>) => void,
  ): void;
}

export type RuntimeWorkerFactory<TWorker extends Worker = Worker> = (
  url: string | URL,
  options?: WorkerOptions,
) => TWorker;
