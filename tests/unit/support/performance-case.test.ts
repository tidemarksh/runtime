import { expect, test } from "vite-plus/test";
import {
  acceptCaseResult,
  benchmarkCase,
  benchmarkCaseResultJson,
  benchmarkInterleavedAsyncCases,
  measureCase,
  rawCaseResultJson,
  type ExecutionCase,
  type ExecutionIdentity,
  type OperationObservation,
  type RawCaseResult,
} from "../../support/performance-case.js";

const digest = (character: string) => character.repeat(64);

function executionCase(): ExecutionCase {
  return {
    technicalName: "runtime/worker/roundtrip-contract",
    class: "diagnostic",
    repositoryOwner: "runtime",
    definitionSourceDigest: digest("1"),
    inputDigest: digest("2"),
    usefulWorkUnit: "worker-roundtrip",
    usefulWorkCount: 4n,
    calibratedRepetitionCount: 1n,
    oracleContractDigest: digest("3"),
    counterContractDigest: digest("4"),
    caseScheduleDigest: digest("5"),
  };
}

function identity(): ExecutionIdentity {
  return {
    engine: "interpreter",
    configurationDigest: digest("6"),
    artifactDigest: digest("7"),
  };
}

function observation(): OperationObservation {
  return {
    oracle: "responses=4",
    counters: [{ name: "responses", value: 4n }],
  };
}

function accept(overrides: Partial<Parameters<typeof acceptCaseResult>[0]> = {}) {
  const expectedCase = executionCase();
  const expectedExecution = identity();
  return acceptCaseResult({
    expectedCase,
    observedCase: { ...expectedCase },
    expectedExecution,
    observedExecution: { ...expectedExecution },
    elapsedNs: 125n,
    oracleExpected: "responses=4",
    observation: observation(),
    countersExpected: [{ name: "responses", value: 4n }],
    ...overrides,
  });
}

test("accepts an exact native case and emits its raw result", () => {
  const result = accept();
  const raw = JSON.parse(rawCaseResultJson(result));
  expect(raw).toMatchObject({
    schema: "execution-case-result-v1",
    technical_name: "runtime/worker/roundtrip-contract",
    engine: "interpreter",
    useful_work_count: "4",
    elapsed_ns: "125",
    correctness: "pass",
    oracle_expected: "responses=4",
    oracle_observed: "responses=4",
    counters: [{ name: "responses", expected: "4", observed: "4" }],
  });
});

test("retains declared diagnostic metrics with their units", () => {
  const raw = JSON.parse(rawCaseResultJson(accept({
    metricsExpected: [
      { name: "dispatched-step-elapsed", unit: "millisecond" },
      { name: "worker-messages", unit: "message" },
    ],
    observation: {
      ...observation(),
      metrics: [
        { name: "dispatched-step-elapsed", unit: "millisecond", value: 17n },
        { name: "worker-messages", unit: "message", value: 23n },
      ],
    },
  })));
  expect(raw.metrics).toEqual([
    { name: "dispatched-step-elapsed", unit: "millisecond", value: "17" },
    { name: "worker-messages", unit: "message", value: "23" },
  ]);
});

test("retains per-run diagnostic metric values in benchmark output", () => {
  const expectedCase = executionCase();
  const expectedExecution = identity();
  let value = 0n;
  const result = benchmarkCase({
    expectedCase,
    observedCase: { ...expectedCase },
    expectedExecution,
    observedExecution: { ...expectedExecution },
    oracleExpected: "responses=4",
    countersExpected: [{ name: "responses", value: 4n }],
    metricsExpected: [{ name: "worker-messages", unit: "message" }],
    warmupRuns: 0,
    measurementRuns: 2,
  }, () => {
    value += 1n;
    return value;
  }, current => ({
    ...observation(),
    metrics: [{ name: "worker-messages", unit: "message", value: current }],
  }));
  expect(JSON.parse(benchmarkCaseResultJson(result)).metrics).toEqual([
    { name: "worker-messages", unit: "message", values: ["1", "2"] },
  ]);
});

test("interleaves async cases and reverses alternate measurement cycles", async () => {
  const first = executionCase();
  const second = { ...executionCase(), technicalName: "runtime/worker/second-roundtrip-contract" };
  const order: string[] = [];
  const makeEntry = (label: string, expectedCase: ExecutionCase) => ({
    args: {
      expectedCase,
      observedCase: { ...expectedCase },
      expectedExecution: identity(),
      observedExecution: identity(),
      oracleExpected: "responses=4",
      countersExpected: [{ name: "responses", value: 4n }],
    },
    operation: async () => {
      order.push(label);
      return 4;
    },
    observe: () => observation(),
  });

  const results = await benchmarkInterleavedAsyncCases([
    makeEntry("first", first),
    makeEntry("second", second),
  ], { warmupRuns: 1, measurementRuns: 2 });

  expect(order).toEqual(["first", "second", "first", "second", "second", "first"]);
  expect(results.map(result => ({
    name: result.case.technicalName,
    runs: result.measurementRuns,
  }))).toEqual([
    { name: first.technicalName, runs: 2 },
    { name: second.technicalName, runs: 2 },
  ]);
});

test("emits the canonical cross-language raw-case golden bytes", () => {
  expect(rawCaseResultJson(accept())).toBe(
    "{\"schema\":\"execution-case-result-v1\",\"technical_name\":\"runtime/worker/roundtrip-contract\",\"class\":\"diagnostic\",\"repository_owner\":\"runtime\",\"definition_source_digest\":\"1111111111111111111111111111111111111111111111111111111111111111\",\"input_digest\":\"2222222222222222222222222222222222222222222222222222222222222222\",\"useful_work_unit\":\"worker-roundtrip\",\"useful_work_count\":\"4\",\"calibrated_repetition_count\":\"1\",\"oracle_contract_digest\":\"3333333333333333333333333333333333333333333333333333333333333333\",\"counter_contract_digest\":\"4444444444444444444444444444444444444444444444444444444444444444\",\"case_schedule_digest\":\"5555555555555555555555555555555555555555555555555555555555555555\",\"engine\":\"interpreter\",\"configuration_digest\":\"6666666666666666666666666666666666666666666666666666666666666666\",\"artifact_digest\":\"7777777777777777777777777777777777777777777777777777777777777777\",\"elapsed_ns\":\"125\",\"correctness\":\"pass\",\"oracle_expected\":\"responses=4\",\"oracle_observed\":\"responses=4\",\"counters\":[{\"name\":\"responses\",\"expected\":\"4\",\"observed\":\"4\"}]}",
  );
});

test("times only the supplied production operation", () => {
  const expectedCase = executionCase();
  const expectedExecution = identity();
  let stage = 0;
  const result = measureCase({
    expectedCase,
    observedCase: { ...expectedCase },
    expectedExecution,
    observedExecution: { ...expectedExecution },
    oracleExpected: "responses=4",
    countersExpected: [{ name: "responses", value: 4n }],
  }, () => {
    expect(stage).toBe(0);
    stage = 1;
    return 4;
  }, value => {
    expect(value).toBe(4);
    expect(stage).toBe(1);
    stage = 2;
    return observation();
  });
  expect(stage).toBe(2);
  expect(result.elapsedNs).toBeGreaterThan(0n);
});

test("rejects oracle, counter, case, and execution identity drift", () => {
  expect(() => accept({
    observation: { ...observation(), oracle: "responses=3" },
  })).toThrow("oracle mismatch");
  expect(() => accept({
    observation: { ...observation(), counters: [{ name: "responses", value: 3n }] },
  })).toThrow("counter mismatch");
  expect(() => accept({
    observedCase: { ...executionCase(), usefulWorkCount: 3n },
  })).toThrow("case identity drift");
  expect(() => accept({
    observedExecution: { ...identity(), engine: "alternate-interpreter" },
  })).toThrow("identity drift");
  expect(() => accept({
    metricsExpected: [{ name: "worker-messages", unit: "message" }],
  })).toThrow("metric count mismatch");
  expect(() => accept({
    metricsExpected: [{ name: "worker-messages", unit: "message" }],
    observation: {
      ...observation(),
      metrics: [{ name: "worker-messages", unit: "call", value: 1n }],
    },
  })).toThrow("metric contract mismatch");
});

test("rejects incomplete results before raw emission", () => {
  expect(() => accept({
    observedCase: { ...executionCase(), usefulWorkCount: 0n },
    expectedCase: { ...executionCase(), usefulWorkCount: 0n },
  })).toThrow("usefulWorkCount");
  expect(() => accept({ elapsedNs: 0n })).toThrow("elapsed duration");
  expect(() => accept({
    countersExpected: [],
    observation: { ...observation(), counters: [] },
  })).toThrow("at least one exact counter");
  expect(() => rawCaseResultJson({} as RawCaseResult)).toThrow("not accepted");
});
