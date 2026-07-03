import { expect, test } from "vite-plus/test";
import {
  applyKernelRpcResumeEffects,
} from "../../../../src/worker/kernel-rpc/effects.js";
import type { ProcessHandle } from "../../../../src/worker/types.js";

test("applyKernelRpcResumeEffects runs notify-host-effect-progress effect before enqueue", async () => {
  const worker = {} as Worker;
  const handle = { pid: 1 } as ProcessHandle;
  const wakeHandle = { pid: 2 } as ProcessHandle;
  const events: string[] = [];

  const applied = await applyKernelRpcResumeEffects(
    handle,
    worker,
    0x1234,
    {
      kind: "completed",
      effects: [
        { kind: "notify-host-effect-progress", handle: wakeHandle },
        { kind: "enqueue-worker" },
      ],
    },
    {
      enqueueWorker(enqueueHandle) {
        events.push(`enqueue:${enqueueHandle.pid}`);
      },
      handleExecveSpawn: async () => {},
      notifyHostEffectProgress(progressHandle) {
        events.push(`progress:${progressHandle.pid}`);
      },
      threadWorkerStates: new WeakMap(),
    },
  );

  expect(events).toEqual(["progress:2", "enqueue:1"]);
  expect(applied).toEqual({ kind: "completed" });
});
