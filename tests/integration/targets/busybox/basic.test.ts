import { existsSync, readFileSync } from "node:fs";
import { expect, test } from "vite-plus/test";
import {
  Runtime,
  encoder,
  decoder,
  logSkip,
  readKernelWasm,
  spawnWithOutput,
} from "../../../support/runtime-workload.js";

const BUSYBOX_BINARY_PATH = process.env.TIDEMARK_TEST_BUSYBOX_BINARY ?? "";

function readBusyBoxBinary(): Uint8Array | null {
  if (!BUSYBOX_BINARY_PATH) {
    return logSkip("missing BusyBox binary: set TIDEMARK_TEST_BUSYBOX_BINARY");
  }
  if (!existsSync(BUSYBOX_BINARY_PATH)) {
    return logSkip(`missing BusyBox binary: ${BUSYBOX_BINARY_PATH}`);
  }
  return new Uint8Array(readFileSync(BUSYBOX_BINARY_PATH));
}

const CASES = [
  {
    name: "--list",
    argv: ["/bin/busybox", "--list"],
    assert(result: { exitCode: number; output: string }): void {
      expect(result.exitCode).toBe(0);
      expect(result.output).toContain("echo\n");
      expect(result.output).toContain("sh\n");
    },
  },
  {
    name: "echo hello",
    argv: ["/bin/busybox", "echo", "hello"],
    assert(result: { exitCode: number; output: string }): void {
      expect(result.exitCode).toBe(0);
      expect(result.output).toBe("hello\n");
    },
  },
  {
    name: "sh -c echo hello",
    argv: ["/bin/busybox", "sh", "-c", "echo hello"],
    assert(result: { exitCode: number; output: string }): void {
      expect(result.exitCode).toBe(0);
      expect(result.output).toBe("hello\n");
    },
  },
];

for (const testCase of CASES) {
  test(`Runtime executes official BusyBox: ${testCase.name}`, async () => {
    const wasmBytes = readKernelWasm();
    const busybox = readBusyBoxBinary();
    if (!wasmBytes || !busybox) {
      return;
    }

    const runtime = await Runtime.create({ wasmBytes });
    try {
      await runtime.fs.mkdir("/bin", 0o755).catch(() => {});
      await runtime.fs.writeFile("/bin/busybox", busybox, 0o755);

      const result = await spawnWithOutput(
        runtime,
        busybox,
        testCase.argv,
        [],
        10_000,
      );
      testCase.assert(result);
    } finally {
      runtime.destroy();
    }
  }, 30_000);
}

test("Runtime executes official BusyBox: sh redirects and cats workspace output", async () => {
  const wasmBytes = readKernelWasm();
  const busybox = readBusyBoxBinary();
  if (!wasmBytes || !busybox) {
    return;
  }

  const runtime = await Runtime.create({ wasmBytes });
  try {
    await runtime.fs.mkdir("/bin", 0o755).catch(() => {});
    await runtime.fs.mkdir("/work", 0o755).catch(() => {});
    await runtime.fs.writeFile("/bin/busybox", busybox, 0o755);
    await runtime.fs.writeFile("/bin/sh", busybox, 0o755);
    await runtime.fs.writeFile("/bin/cat", busybox, 0o755);

    const result = await spawnWithOutput(
      runtime,
      busybox,
      ["/bin/sh", "-c", "echo make-evidence > out.txt; cat out.txt"],
      ["PATH=/bin"],
      10_000,
      { cwd: "/work" },
    );

    expect(result.exitCode, `${result.output}\n${result.debugInfo ?? ""}`).toBe(0);
    expect(result.output).toBe("make-evidence\n");
  } finally {
    runtime.destroy();
  }
}, 30_000);

for (const [argv, lineEnding] of [
  [["/bin/sh"], "\n"],
  [["/bin/sh", "-i"], "\n"],
  [["/bin/sh", "-i"], "\r"],
] as const) {
  test(`Runtime interactive shell stdin/stdout stays live: ${argv.join(" ")} ${JSON.stringify(lineEnding)}`, async () => {
    const wasmBytes = readKernelWasm();
    const busybox = readBusyBoxBinary();
    if (!wasmBytes || !busybox) {
      return;
    }

    const runtime = await Runtime.create({ wasmBytes });
    try {
      await runtime.fs.mkdir("/bin", 0o755).catch(() => {});
      await runtime.fs.mkdir("/work", 0o755).catch(() => {});
      await runtime.fs.writeFile("/bin/busybox", busybox, 0o755);
      await runtime.fs.writeFile("/bin/sh", busybox, 0o755);
      await runtime.fs.writeFile("/bin/ls", busybox, 0o755);
      await runtime.fs.writeFile("/work/hello.txt", new TextEncoder().encode("hello\n"), 0o644);

      const proc = await runtime.spawn(
        busybox,
        [...argv],
        ["TERM=xterm", "HOME=/root", "PATH=/bin"],
        { cwd: "/work", stdio: "pty" },
      );
      let output = "";
      proc.onStdout((data) => {
        output += decoder.decode(data);
      });

      proc.writeStdin(new TextEncoder().encode(`ls${lineEnding}`));
      await Promise.race([
        new Promise<void>((resolve, reject) => {
          const deadline = Date.now() + 2_000;
          const poll = (): void => {
            if (output.includes("hello.txt")) {
              resolve();
              return;
            }
            if (Date.now() >= deadline) {
              reject(new Error(`interactive shell did not emit ls output before exit: ${JSON.stringify({ argv, output })}`));
              return;
            }
            setTimeout(poll, 10);
          };
          poll();
        }),
        new Promise<never>((_resolve, reject) => {
          const timer = setTimeout(() => reject(new Error(`timeout waiting for ls output: ${JSON.stringify({ argv, output })}`)), 2_000);
          if (typeof timer === "object" && timer !== null && "unref" in timer) {
            timer.unref();
          }
        }),
      ]);
      proc.writeStdin(new TextEncoder().encode(`exit${lineEnding}`));

      const exitCode = await Promise.race([
        proc.waitForExit(),
        new Promise<never>((_resolve, reject) => {
          const timer = setTimeout(() => reject(new Error("timeout after 2000ms")), 2_000);
          if (typeof timer === "object" && timer !== null && "unref" in timer) {
            timer.unref();
          }
        }),
      ]);

      expect(
        { argv, exitCode, output, debug: proc.debugInfo?.() },
        "interactive shell must process stdin and emit ls output before exit",
      ).toMatchObject({
        exitCode: 0,
      });
      expect(output).toContain("hello.txt");
    } finally {
      runtime.destroy();
    }
  }, 20_000);
}

test("Runtime interactive shell ls handles multi-entry workspace without wedging", async () => {
  const wasmBytes = readKernelWasm();
  const busybox = readBusyBoxBinary();
  if (!wasmBytes || !busybox) {
    return;
  }

  const runtime = await Runtime.create({ wasmBytes });
  try {
    await runtime.fs.mkdir("/bin", 0o755).catch(() => {});
    await runtime.fs.mkdir("/work", 0o755).catch(() => {});
    await runtime.fs.mkdir("/work/go-hello", 0o755).catch(() => {});
    await runtime.fs.writeFile("/bin/busybox", busybox, 0o755);
    await runtime.fs.writeFile("/bin/sh", busybox, 0o755);
    await runtime.fs.writeFile("/bin/ls", busybox, 0o755);
    await runtime.fs.writeFile("/work/node-hello.js", new TextEncoder().encode("console.log('ok');\n"), 0o644);
    await runtime.fs.writeFile("/work/node-timers.js", new TextEncoder().encode("setTimeout(() => {}, 1);\n"), 0o644);

    const proc = await runtime.spawn(
      busybox,
      ["/bin/sh", "-i"],
      ["TERM=xterm", "HOME=/root", "PATH=/bin"],
      { cwd: "/work", stdio: "pty" },
    );
    let output = "";
    proc.onStdout((data) => {
      output += decoder.decode(data);
    });

    for (const ch of "ls\r") {
      proc.writeStdin(new TextEncoder().encode(ch));
    }

    await Promise.race([
      new Promise<void>((resolve, reject) => {
        const deadline = Date.now() + 2_000;
        const poll = (): void => {
          if (
            output.includes("go-hello")
            && output.includes("node-hello.js")
            && output.includes("node-timers.js")
          ) {
            resolve();
            return;
          }
          if (Date.now() >= deadline) {
            reject(new Error(`interactive shell wedged on multi-entry ls: ${JSON.stringify({ output, debug: proc.debugInfo?.() })}`));
            return;
          }
          setTimeout(poll, 10);
        };
        poll();
      }),
      new Promise<never>((_resolve, reject) => {
        const timer = setTimeout(() => reject(new Error(`timeout waiting for multi-entry ls output: ${JSON.stringify({ output, debug: proc.debugInfo?.() })}`)), 2_000);
        if (typeof timer === "object" && timer !== null && "unref" in timer) {
          timer.unref();
        }
      }),
    ]);

    for (const ch of "exit\r") {
      proc.writeStdin(new TextEncoder().encode(ch));
    }
    await expect(proc.waitForExit()).resolves.toBe(0);
  } finally {
    runtime.destroy();
  }
}, 20_000);

test("Runtime interactive shell echoes fast character stdin exactly once", async () => {
  const wasmBytes = readKernelWasm();
  const busybox = readBusyBoxBinary();
  if (!wasmBytes || !busybox) {
    return;
  }

  const runtime = await Runtime.create({ wasmBytes });
  try {
    await runtime.fs.mkdir("/bin", 0o755).catch(() => {});
    await runtime.fs.mkdir("/work", 0o755).catch(() => {});
    await runtime.fs.writeFile("/bin/busybox", busybox, 0o755);
    await runtime.fs.writeFile("/bin/sh", busybox, 0o755);
    await runtime.fs.writeFile("/bin/ls", busybox, 0o755);

    const proc = await runtime.spawn(
      busybox,
      ["/bin/sh", "-i"],
      ["TERM=xterm", "HOME=/root", "PATH=/bin", "PS1=$ "],
      { cwd: "/work", stdio: "pty" },
    );
    let output = "";
    proc.onStdout((data) => {
      output += decoder.decode(data);
    });

    await Promise.race([
      new Promise<void>((resolve, reject) => {
        const deadline = Date.now() + 2_000;
        const poll = (): void => {
          if (output.includes("$ ")) {
            resolve();
            return;
          }
          if (Date.now() >= deadline) {
            reject(new Error(`interactive shell did not emit prompt: ${JSON.stringify({ output, debug: proc.debugInfo?.() })}`));
            return;
          }
          setTimeout(poll, 10);
        };
        poll();
      }),
      new Promise<never>((_resolve, reject) => {
        const timer = setTimeout(() => reject(new Error(`timeout waiting for prompt: ${JSON.stringify({ output, debug: proc.debugInfo?.() })}`)), 2_000);
        if (typeof timer === "object" && timer !== null && "unref" in timer) {
          timer.unref();
        }
      }),
    ]);

    const promptEnd = output.length;
    const line = "aiueo kakikukeko sasisuseso tatituteto naninuneno";
    for (const ch of line) {
      proc.writeStdin(encoder.encode(ch));
    }

    await Promise.race([
      new Promise<void>((resolve, reject) => {
        const deadline = Date.now() + 2_000;
        let lastLength = output.length;
        let stableSince = Date.now();
        const poll = (): void => {
          if (output.length !== lastLength) {
            lastLength = output.length;
            stableSince = Date.now();
          }
          if (output.length >= promptEnd + line.length && Date.now() - stableSince >= 50) {
            resolve();
            return;
          }
          if (Date.now() >= deadline) {
            reject(new Error(`interactive shell did not echo full line: ${JSON.stringify({ output, debug: proc.debugInfo?.() })}`));
            return;
          }
          setTimeout(poll, 10);
        };
        poll();
      }),
      new Promise<never>((_resolve, reject) => {
        const timer = setTimeout(() => reject(new Error(`timeout waiting for exact echo: ${JSON.stringify({ output, debug: proc.debugInfo?.() })}`)), 2_000);
        if (typeof timer === "object" && timer !== null && "unref" in timer) {
          timer.unref();
        }
      }),
    ]);

    expect(output.slice(promptEnd)).toBe(line);
  } finally {
    runtime.destroy();
  }
}, 20_000);

test("Runtime interactive shell preserves absolute-path exec with demo-style env", async () => {
  const wasmBytes = readKernelWasm();
  const busybox = readBusyBoxBinary();
  if (!wasmBytes || !busybox) {
    return;
  }

  const runtime = await Runtime.create({ wasmBytes, guestMemoryPolicyLimitBytes: 384 * 1024 * 1024 });
  try {
    await runtime.fs.mkdir("/bin", 0o755).catch(() => {});
    await runtime.fs.mkdir("/root", 0o755).catch(() => {});
    await runtime.fs.mkdir("/tmp", 0o777).catch(() => {});
    await runtime.fs.mkdir("/tmp/gopath", 0o755).catch(() => {});
    await runtime.fs.mkdir("/tmp/gocache", 0o755).catch(() => {});
    await runtime.fs.mkdir("/usr", 0o755).catch(() => {});
    await runtime.fs.mkdir("/usr/local", 0o755).catch(() => {});
    await runtime.fs.mkdir("/usr/local/go", 0o755).catch(() => {});
    await runtime.fs.mkdir("/usr/local/go/bin", 0o755).catch(() => {});
    await runtime.fs.mkdir("/work", 0o755).catch(() => {});
    await runtime.fs.mkdir("/work/go-hello", 0o755).catch(() => {});
    await runtime.fs.writeFile("/bin/busybox", busybox, 0o755);
    await runtime.fs.writeFile("/bin/sh", busybox, 0o755);
    await runtime.fs.writeFile("/work/node-hello.js", new TextEncoder().encode("console.log('ok');\n"), 0o644);
    await runtime.fs.writeFile("/work/node-timers.js", new TextEncoder().encode("setTimeout(() => {}, 1);\n"), 0o644);

    const proc = await runtime.spawn(
      busybox,
      ["/bin/sh", "-i"],
      [
        "TERM=xterm",
        "HOME=/root",
        "PATH=/bin:/usr/local/go/bin",
        "PS1=$ ",
        "GOROOT=/usr/local/go",
        "GOPATH=/tmp/gopath",
        "GOCACHE=/tmp/gocache",
        "GOMAXPROCS=1",
        "GOTELEMETRY=off",
        "GOTELEMETRYDIR=/dev/null",
        "GOGC=off",
        "GOEXPERIMENT=nodwarf5",
        "GOFLAGS=-mod=mod",
        "GONOSUMDB=*",
      ],
      { cwd: "/work", stdio: "pty" },
    );
    let output = "";
    const debugSnapshot = (): string => {
      const runtimeDebug = (runtime as Runtime & { debugSnapshot?: () => string }).debugSnapshot?.();
      return runtimeDebug ?? proc.debugInfo?.() ?? "";
    };
    proc.onStdout((data) => {
      output += decoder.decode(data);
    });

    proc.writeStdin(encoder.encode("echo $PATH\n"));
    await Promise.race([
      new Promise<void>((resolve, reject) => {
        const deadline = Date.now() + 2_000;
        const poll = (): void => {
          if (output.includes("/bin:/usr/local/go/bin")) {
            resolve();
            return;
          }
          if (Date.now() >= deadline) {
            reject(new Error(`interactive shell did not echo demo PATH: ${JSON.stringify({ output, debug: debugSnapshot() })}`));
            return;
          }
          setTimeout(poll, 10);
        };
        poll();
      }),
      new Promise<never>((_resolve, reject) => {
        const timer = setTimeout(() => reject(new Error(`timeout waiting for demo PATH output: ${JSON.stringify({ output, debug: debugSnapshot() })}`)), 2_000);
        if (typeof timer === "object" && timer !== null && "unref" in timer) {
          timer.unref();
        }
      }),
    ]);

    proc.writeStdin(encoder.encode("/bin/busybox ls\n"));
    await Promise.race([
      new Promise<void>((resolve, reject) => {
        const deadline = Date.now() + 2_000;
        const poll = (): void => {
          if (
            output.includes("go-hello")
            && output.includes("node-hello.js")
            && output.includes("node-timers.js")
          ) {
            resolve();
            return;
          }
          if (Date.now() >= deadline) {
            reject(new Error(`interactive shell did not execute /bin/busybox ls: ${JSON.stringify({ output, debug: debugSnapshot() })}`));
            return;
          }
          setTimeout(poll, 10);
        };
        poll();
      }),
      new Promise<never>((_resolve, reject) => {
        const timer = setTimeout(() => reject(new Error(`timeout waiting for /bin/busybox ls output: ${JSON.stringify({ output, debug: debugSnapshot() })}`)), 2_000);
        if (typeof timer === "object" && timer !== null && "unref" in timer) {
          timer.unref();
        }
      }),
    ]);

    proc.writeStdin(encoder.encode("exit\n"));
    await expect(proc.waitForExit()).resolves.toBe(0);
  } finally {
    runtime.destroy();
  }
}, 20_000);

for (const [argv, lineEnding] of [
  [["/bin/sh"], "\n"],
  [["/bin/sh", "-i"], "\r"],
] as const) {
  test(`Runtime interactive shell accepts character-by-character stdin: ${argv.join(" ")} ${JSON.stringify(lineEnding)}`, async () => {
    const wasmBytes = readKernelWasm();
    const busybox = readBusyBoxBinary();
    if (!wasmBytes || !busybox) {
      return;
    }

    const runtime = await Runtime.create({ wasmBytes });
    try {
      await runtime.fs.mkdir("/bin", 0o755).catch(() => {});
      await runtime.fs.mkdir("/work", 0o755).catch(() => {});
      await runtime.fs.writeFile("/bin/busybox", busybox, 0o755);
      await runtime.fs.writeFile("/bin/sh", busybox, 0o755);
      await runtime.fs.writeFile("/bin/ls", busybox, 0o755);
      await runtime.fs.writeFile("/work/hello.txt", new TextEncoder().encode("hello\n"), 0o644);

      const proc = await runtime.spawn(
        busybox,
        [...argv],
        ["TERM=xterm", "HOME=/root", "PATH=/bin"],
        { cwd: "/work", stdio: "pty" },
      );
      let output = "";
      proc.onStdout((data) => {
        output += decoder.decode(data);
      });

      for (const ch of `ls${lineEnding}`) {
        proc.writeStdin(new TextEncoder().encode(ch));
      }

      await Promise.race([
        new Promise<void>((resolve, reject) => {
          const deadline = Date.now() + 2_000;
          const poll = (): void => {
            if (output.includes("hello.txt")) {
              resolve();
              return;
            }
            if (Date.now() >= deadline) {
              reject(new Error(`interactive shell did not emit ls output from char-by-char stdin: ${JSON.stringify({ argv, output })}`));
              return;
            }
            setTimeout(poll, 10);
          };
          poll();
        }),
        new Promise<never>((_resolve, reject) => {
          const timer = setTimeout(() => reject(new Error(`timeout waiting for char-by-char ls output: ${JSON.stringify({ argv, output })}`)), 2_000);
          if (typeof timer === "object" && timer !== null && "unref" in timer) {
            timer.unref();
          }
        }),
      ]);

      for (const ch of `exit${lineEnding}`) {
        proc.writeStdin(new TextEncoder().encode(ch));
      }

      await expect(proc.waitForExit()).resolves.toBe(0);
    } finally {
      runtime.destroy();
    }
  }, 20_000);
}
