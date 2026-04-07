// Adapted from /tmp/nano/test/nodejs/tests/timers.js.

(async () => {
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

  const t1 = await new Promise((resolve) => setTimeout(() => resolve(Date.now()), 10));
  check("setTimeout", typeof t1 === "number");

  const t2 = await new Promise((resolve) => setImmediate(() => resolve(true)));
  check("setImmediate", t2 === true);

  let cleared = true;
  const id = setTimeout(() => {
    cleared = false;
  }, 10);
  clearTimeout(id);
  await new Promise((resolve) => setTimeout(resolve, 50));
  check("clearTimeout", cleared);

  let micro = false;
  queueMicrotask(() => {
    micro = true;
  });
  await new Promise((resolve) => setTimeout(resolve, 0));
  check("queueMicrotask", micro);

  console.log(`PASS: timers ${ok}/${total}`);
  process.exit(ok === total ? 0 : 1);
})();
