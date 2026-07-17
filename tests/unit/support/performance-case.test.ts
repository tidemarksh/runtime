import { expect, test } from "vite-plus/test";
import {
  acceptCaseResult,
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

test("times only the supplied production operation", () => {
  const expectedCase = executionCase();
  const expectedExecution = identity();
  let calls = 0;
  const result = measureCase({
    expectedCase,
    observedCase: { ...expectedCase },
    expectedExecution,
    observedExecution: { ...expectedExecution },
    oracleExpected: "responses=4",
    countersExpected: [{ name: "responses", value: 4n }],
  }, () => {
    calls += 1;
    return observation();
  });
  expect(calls).toBe(1);
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
