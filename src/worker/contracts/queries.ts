export interface RuntimeReadonlyQuery<Name extends string = string, Args = void> {
  name: Name;
  args: Args;
}

export interface RuntimeReadonlyQueryResult<Name extends string = string, Value = unknown> {
  name: Name;
  value: Value;
}
