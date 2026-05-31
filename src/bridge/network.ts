export interface RuntimeNetworkEndpoint {
  family: "inet" | "inet6";
  address: string;
  port: number;
}

export interface RuntimeNetworkConnectRequest {
  pid: number;
  fd: number;
  endpoint: RuntimeNetworkEndpoint;
}

export interface RuntimeNetworkBridge {
  connect(
    request: RuntimeNetworkConnectRequest,
  ): Promise<RuntimeNetworkConnectResult | void> | RuntimeNetworkConnectResult | void;
}

export interface RuntimeNetworkConnection {
  write(data: Uint8Array): Promise<void> | void;
  read(maxBytes: number): Promise<Uint8Array | null | undefined> | Uint8Array | null | undefined;
  close?(): Promise<void> | void;
}

export interface RuntimeNetworkConnectResult {
  errno?: number;
  connection?: RuntimeNetworkConnection;
}
