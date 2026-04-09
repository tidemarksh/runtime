const fs = require("node:fs");
const { Worker } = require("node:worker_threads");

const inputPath = process.argv[2];
const data = fs.readFileSync(inputPath, "utf8").trim();

const worker = new Worker(
  `const { parentPort } = require("node:worker_threads"); parentPort.postMessage(21);`,
  { eval: true },
);

worker.once("message", (n) => {
  setTimeout(() => {
    console.log(JSON.stringify({ data, worker: n, sum: n + 21 }));
    worker.terminate().then(() => process.exit(0));
  }, 1);
});

worker.once("error", (error) => {
  console.error(error instanceof Error ? error.stack ?? error.message : String(error));
  process.exit(1);
});
