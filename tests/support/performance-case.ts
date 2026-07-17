export const CASE_RESULT_SCHEMA = "execution-case-result-v1";
const MAX_U64 = 18_446_744_073_709_551_615n;
const acceptedResultBrand: unique symbol = Symbol("accepted-execution-case-result");

export type CaseClass = "semantic" | "diagnostic";

export interface ExecutionCase {
  technicalName: string;
  class: CaseClass;
  repositoryOwner: "kernel" | "runtime";
  definitionSourceDigest: string;
  inputDigest: string;
  usefulWorkUnit: string;
  usefulWorkCount: bigint;
  calibratedRepetitionCount: bigint;
  oracleContractDigest: string;
  counterContractDigest: string;
  caseScheduleDigest: string;
}

export interface ExecutionIdentity {
  engine: string;
  configurationDigest: string;
  artifactDigest: string;
}

export interface ExpectedCounter {
  name: string;
  value: bigint;
}

export interface ObservedCounter {
  name: string;
  value: bigint;
}

export interface OperationObservation {
  oracle: string;
  counters: ObservedCounter[];
}

export interface CounterResult {
  name: string;
  expected: bigint;
  observed: bigint;
}

export interface RawCaseResult {
  readonly [acceptedResultBrand]: true;
  case: ExecutionCase;
  execution: ExecutionIdentity;
  elapsedNs: bigint;
  oracleExpected: string;
  oracleObserved: string;
  counters: CounterResult[];
}

export function measureCase<Output>(args: {
  expectedCase: ExecutionCase;
  observedCase: ExecutionCase;
  expectedExecution: ExecutionIdentity;
  observedExecution: ExecutionIdentity;
  oracleExpected: string;
  countersExpected: ExpectedCounter[];
}, operation: () => Output, observe: (output: Output) => OperationObservation): RawCaseResult {
  const started = process.hrtime.bigint();
  const output = operation();
  const elapsedNs = process.hrtime.bigint() - started;
  const observation = observe(output);
  return acceptCaseResult({ ...args, elapsedNs, observation });
}

export function acceptCaseResult(args: {
  expectedCase: ExecutionCase;
  observedCase: ExecutionCase;
  expectedExecution: ExecutionIdentity;
  observedExecution: ExecutionIdentity;
  elapsedNs: bigint;
  oracleExpected: string;
  observation: OperationObservation;
  countersExpected: ExpectedCounter[];
}): RawCaseResult {
  validateCase(args.expectedCase);
  validateCase(args.observedCase);
  validateExecutionIdentity(args.expectedExecution);
  validateExecutionIdentity(args.observedExecution);
  if (!sameCase(args.expectedCase, args.observedCase)) {
    throw new Error("case identity drift");
  }
  if (!sameExecution(args.expectedExecution, args.observedExecution)) {
    throw new Error("engine or configuration identity drift");
  }
  if (args.oracleExpected !== args.observation.oracle) {
    throw new Error(
      `oracle mismatch: expected ${JSON.stringify(args.oracleExpected)}, observed ${JSON.stringify(args.observation.oracle)}`,
    );
  }
  validateExpectedCounters(args.countersExpected);
  if (args.countersExpected.length !== args.observation.counters.length) {
    throw new Error(
      `counter count mismatch: expected ${args.countersExpected.length}, observed ${args.observation.counters.length}`,
    );
  }
  const counters = args.countersExpected.map((expected, index) => {
    const observed = args.observation.counters[index];
    if (expected.name !== observed.name || expected.value !== observed.value) {
      throw new Error(
        `counter mismatch for ${JSON.stringify(expected.name)}: expected ${expected.name}=${expected.value}, observed ${observed.name}=${observed.value}`,
      );
    }
    return { name: expected.name, expected: expected.value, observed: observed.value };
  });
  if (args.elapsedNs <= 0n || args.elapsedNs > MAX_U64) {
    throw new Error("elapsed duration must be positive");
  }
  return {
    [acceptedResultBrand]: true,
    case: args.observedCase,
    execution: args.observedExecution,
    elapsedNs: args.elapsedNs,
    oracleExpected: args.oracleExpected,
    oracleObserved: args.observation.oracle,
    counters,
  };
}

export function rawCaseResultJson(result: RawCaseResult): string {
  if (result[acceptedResultBrand] !== true) {
    throw new Error("case result was not accepted");
  }
  return JSON.stringify({
    schema: CASE_RESULT_SCHEMA,
    technical_name: result.case.technicalName,
    class: result.case.class,
    repository_owner: result.case.repositoryOwner,
    definition_source_digest: result.case.definitionSourceDigest,
    input_digest: result.case.inputDigest,
    useful_work_unit: result.case.usefulWorkUnit,
    useful_work_count: result.case.usefulWorkCount.toString(),
    calibrated_repetition_count: result.case.calibratedRepetitionCount.toString(),
    oracle_contract_digest: result.case.oracleContractDigest,
    counter_contract_digest: result.case.counterContractDigest,
    case_schedule_digest: result.case.caseScheduleDigest,
    engine: result.execution.engine,
    configuration_digest: result.execution.configurationDigest,
    artifact_digest: result.execution.artifactDigest,
    elapsed_ns: result.elapsedNs.toString(),
    correctness: "pass",
    oracle_expected: result.oracleExpected,
    oracle_observed: result.oracleObserved,
    counters: result.counters.map(counter => ({
      name: counter.name,
      expected: counter.expected.toString(),
      observed: counter.observed.toString(),
    })),
  });
}

function validateCase(value: ExecutionCase): void {
  validateTechnicalName(value.technicalName, "technicalName");
  if (value.class !== "semantic" && value.class !== "diagnostic") {
    throw new Error("class must be semantic or diagnostic");
  }
  if (value.repositoryOwner !== "kernel" && value.repositoryOwner !== "runtime") {
    throw new Error("repositoryOwner must be kernel or runtime");
  }
  validateTechnicalName(value.usefulWorkUnit, "usefulWorkUnit");
  if (value.usefulWorkCount <= 0n || value.usefulWorkCount > MAX_U64) {
    throw new Error("usefulWorkCount must be positive");
  }
  if (value.calibratedRepetitionCount <= 0n || value.calibratedRepetitionCount > MAX_U64) {
    throw new Error("calibratedRepetitionCount must be positive");
  }
  for (const [label, digest] of [
    ["definitionSourceDigest", value.definitionSourceDigest],
    ["inputDigest", value.inputDigest],
    ["oracleContractDigest", value.oracleContractDigest],
    ["counterContractDigest", value.counterContractDigest],
    ["caseScheduleDigest", value.caseScheduleDigest],
  ] as const) {
    validateDigest(digest, label);
  }
}

function validateExecutionIdentity(value: ExecutionIdentity): void {
  validateTechnicalName(value.engine, "engine");
  validateDigest(value.configurationDigest, "configurationDigest");
  validateDigest(value.artifactDigest, "artifactDigest");
}

function validateExpectedCounters(counters: ExpectedCounter[]): void {
  if (counters.length === 0) {
    throw new Error("at least one exact counter is required");
  }
  let previous: string | undefined;
  for (const counter of counters) {
    validateTechnicalName(counter.name, "counter name");
    if (counter.value < 0n || counter.value > MAX_U64) {
      throw new Error("counter value must fit u64");
    }
    if (previous !== undefined && previous >= counter.name) {
      throw new Error("counter names must be sorted and unique");
    }
    previous = counter.name;
  }
}

function validateTechnicalName(value: string, label: string): void {
  if (!/^[a-z0-9_-]+(?:\/[a-z0-9_-]+)*$/.test(value)) {
    throw new Error(`${label} is not a stable technical name: ${JSON.stringify(value)}`);
  }
}

function validateDigest(value: string, label: string): void {
  if (!/^[0-9a-f]{64}$/.test(value)) {
    throw new Error(`${label} must be 64 lowercase hexadecimal characters`);
  }
}

function sameCase(left: ExecutionCase, right: ExecutionCase): boolean {
  return left.technicalName === right.technicalName
    && left.class === right.class
    && left.repositoryOwner === right.repositoryOwner
    && left.definitionSourceDigest === right.definitionSourceDigest
    && left.inputDigest === right.inputDigest
    && left.usefulWorkUnit === right.usefulWorkUnit
    && left.usefulWorkCount === right.usefulWorkCount
    && left.calibratedRepetitionCount === right.calibratedRepetitionCount
    && left.oracleContractDigest === right.oracleContractDigest
    && left.counterContractDigest === right.counterContractDigest
    && left.caseScheduleDigest === right.caseScheduleDigest;
}

function sameExecution(left: ExecutionIdentity, right: ExecutionIdentity): boolean {
  return left.engine === right.engine
    && left.configurationDigest === right.configurationDigest
    && left.artifactDigest === right.artifactDigest;
}
