export function retainRuntimeLifecycleEvents(events: readonly string[]): string[] {
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
    return [...new Set([...important.slice(-2000), ...tail])];
  }
  return [...events];
}
