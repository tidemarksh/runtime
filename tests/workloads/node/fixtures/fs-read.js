// Adapted from /tmp/nano/test/nodejs/tests/fs-read.js.
// The NanoVM test reads /etc/hostname. Tidemark writes a fixture into /work
// so the workload stays self-contained under the runtime harness.

const fs = require("fs");

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

try {
  const data = fs.readFileSync("/work/fixture.txt", "utf8");
  check("readFile", data.includes("fixture ok"));
} catch (error) {
  check("readFile", false);
  process.stderr.write(`  ${error instanceof Error ? error.message : String(error)}\n`);
}

try {
  check("existsSync", fs.existsSync("/work/fixture.txt"));
} catch {
  check("existsSync", false);
}

try {
  const stat = fs.statSync("/work/fixture.txt");
  check("statSync", stat.size > 0);
} catch {
  check("statSync", false);
}

console.log(`PASS: fs-read ${ok}/${total}`);
process.exit(ok === total ? 0 : 1);
