const fs = require("node:fs/promises");

async function main() {
  const inputPath = process.argv[2];
  const dirPath = process.argv[3];
  const data = (await fs.readFile(inputPath, "utf8")).trim();
  const entries = await fs.readdir(dirPath);

  let immediate = false;
  let microtask = false;

  queueMicrotask(() => {
    microtask = true;
  });

  await new Promise((resolve) => {
    setImmediate(() => {
      immediate = true;
      resolve();
    });
  });

  console.log(
    JSON.stringify({
      data,
      entryCount: entries.length,
      hasInput: entries.includes("input.txt"),
      immediate,
      microtask,
    }),
  );
}

main().catch((error) => {
  console.error(error instanceof Error ? error.stack ?? error.message : String(error));
  process.exit(1);
});
