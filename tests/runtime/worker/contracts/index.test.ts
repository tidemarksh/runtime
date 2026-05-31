import { describe, expectTypeOf, it } from "vitest";

import type {
  KernelWorkerInbound as LegacyKernelWorkerInbound,
  ThreadWorkerOutbound as LegacyThreadWorkerOutbound,
} from "../../../../src/messages.js";
import type {
  KernelWorkerInbound,
  RuntimeMessagePort,
  RuntimeOutcome,
  RuntimeReadonlyQuery,
  RuntimeWorkerFactory,
  ThreadWorkerOutbound,
} from "../../../../src/worker/contracts/index.js";

describe("worker contracts", () => {
  it("exports worker message contracts from the owner directory", () => {
    expectTypeOf<KernelWorkerInbound>().toEqualTypeOf<LegacyKernelWorkerInbound>();
    expectTypeOf<ThreadWorkerOutbound>().toEqualTypeOf<LegacyThreadWorkerOutbound>();
  });

  it("keeps generic outcome, query, and port contracts type-only", () => {
    expectTypeOf<RuntimeOutcome<number, string>>().toMatchTypeOf<
      { ok: true; value: number } | { ok: false; error: string }
    >();
    expectTypeOf<RuntimeReadonlyQuery<"debug", { pid: number }>>().toMatchTypeOf<{
      name: "debug";
      args: { pid: number };
    }>();
    expectTypeOf<RuntimeMessagePort<ThreadWorkerOutbound, KernelWorkerInbound>>().toHaveProperty(
      "postMessage",
    );
    expectTypeOf<RuntimeWorkerFactory>().returns.toEqualTypeOf<Worker>();
  });
});
