export interface RuntimeSuccessOutcome<T = void> {
  ok: true;
  value: T;
}

export interface RuntimeFailureOutcome<E = Error> {
  ok: false;
  error: E;
}

export type RuntimeOutcome<T = void, E = Error> =
  | RuntimeSuccessOutcome<T>
  | RuntimeFailureOutcome<E>;
