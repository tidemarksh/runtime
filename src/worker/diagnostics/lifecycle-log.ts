export function appendRuntimeLifecycleEvent(
  runtimeId: number,
  event: string,
  runtimeLifecycleEvents: Map<number, string[]>,
): void {
  const events = runtimeLifecycleEvents.get(runtimeId) ?? [];
  events.push(event);
  if (events.length > 5000) {
    const important = events.filter((entry) => (
      entry.includes("fork")
      || entry.includes("spawn")
      || entry.includes("child")
      || entry.includes("exit")
      || entry.includes("wait4")
      || entry.includes("status-file")
      || entry.includes("reroute")
      || entry.includes("ownership")
    ));
    const tail = events.slice(-3000);
    events.splice(0, events.length, ...[...new Set([...important.slice(-2000), ...tail])]);
  }
  runtimeLifecycleEvents.set(runtimeId, events);
}
