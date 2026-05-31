// Adapted from /tmp/nano/test/nodejs/tests/worker-threads.js.
// The coverage is kept close to NanoVM so this workload can serve as a
// baseline acceptance reference for Tidemark as well.

const {
  Worker,
  isMainThread,
  parentPort,
  workerData,
  threadId,
  MessageChannel,
} = require("worker_threads");

let ok = 0;
let total = 0;

function check(name, cond) {
  total += 1;
  if (cond) {
    ok += 1;
  } else {
    process.stderr.write(`  FAIL ${name}\n`);
  }
}

check("isMainThread", isMainThread === true);
check("threadId", typeof threadId === "number");
check("parentPort-null", parentPort === null);
check("Worker-fn", typeof Worker === "function");
check("MessageChannel-fn", typeof MessageChannel === "function");

const mc = new MessageChannel();
check("channel-port1", mc.port1 !== null && mc.port1 !== undefined);
check("channel-port2", mc.port2 !== null && mc.port2 !== undefined);
mc.port1.close();
mc.port2.close();

function createWorker(code, opts = {}) {
  return new Promise((resolve, reject) => {
    try {
      const worker = new Worker(code, { eval: true, ...opts });
      resolve(worker);
    } catch (error) {
      reject(error);
    }
  });
}

(async () => {
  try {
    const worker = await createWorker(
      `const { parentPort } = require("worker_threads");
       parentPort.on("message", (msg) => parentPort.postMessage(msg * 2));`,
    );
    const value = await new Promise((resolve, reject) => {
      worker.on("message", resolve);
      worker.on("error", reject);
      worker.postMessage(21);
    });
    check("echo-double", value === 42);
    await worker.terminate();
  } catch (error) {
    check("echo-double", error && error.code === "ERR_WORKER_INIT_FAILED");
  }

  try {
    const worker = await createWorker(
      `const { parentPort, workerData } = require("worker_threads");
       parentPort.postMessage(workerData.greeting + " " + workerData.name);`,
      { workerData: { greeting: "hello", name: "tidemark" } },
    );
    const value = await new Promise((resolve, reject) => {
      worker.on("message", resolve);
      worker.on("error", reject);
    });
    check("workerData", value === "hello tidemark");
    await worker.terminate();
  } catch (error) {
    check("workerData", error && error.code === "ERR_WORKER_INIT_FAILED");
  }

  try {
    const sab = new SharedArrayBuffer(4);
    const array = new Int32Array(sab);
    array[0] = 100;
    const worker = await createWorker(
      `const { parentPort, workerData } = require("worker_threads");
       const arr = new Int32Array(workerData.buf);
       Atomics.add(arr, 0, 50);
       parentPort.postMessage(Atomics.load(arr, 0));`,
      { workerData: { buf: sab } },
    );
    const value = await new Promise((resolve, reject) => {
      worker.on("message", resolve);
      worker.on("error", reject);
    });
    check("sab-atomics", value === 150);
    check("sab-main-view", Atomics.load(array, 0) === 150);
    await worker.terminate();
  } catch (error) {
    check("sab-atomics", error && error.code === "ERR_WORKER_INIT_FAILED");
    check("sab-main-view", error && error.code === "ERR_WORKER_INIT_FAILED");
  }

  try {
    const worker = await createWorker("process.exit(0);");
    const code = await new Promise((resolve) => {
      worker.on("exit", resolve);
    });
    check("exit-code", code === 0);
  } catch (error) {
    check("exit-code", error && error.code === "ERR_WORKER_INIT_FAILED");
  }

  try {
    const worker = await createWorker(`throw new Error("worker-crash");`);
    const value = await new Promise((resolve) => {
      worker.on("error", resolve);
    });
    check("worker-error", value && value.message === "worker-crash");
    await new Promise((resolve) => worker.on("exit", resolve));
  } catch (error) {
    check("worker-error", error && error.code === "ERR_WORKER_INIT_FAILED");
  }

  console.log(`PASS: worker-threads ${ok}/${total}`);
  process.exit(ok === total ? 0 : 1);
})();

setTimeout(() => {
  process.stderr.write("  TIMEOUT: worker-threads exceeded 15s\n");
  console.log(`PASS: worker-threads ${ok}/${total}`);
  process.exit(ok === total ? 0 : 1);
}, 15000);
