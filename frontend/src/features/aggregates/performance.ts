/** Display the agreed policy before a draft has a server-side measurement. */
export function executionThresholds(duration: string | number) {
  if (typeof duration === "string" && !/^[0-9]+$/.test(duration)) return null;
  const seconds = Number(duration);
  if (!Number.isSafeInteger(seconds) || seconds < 1 || seconds > 604800)
    return null;
  return { softSeconds: 5, hardSeconds: Math.min(30, (4 * seconds) / 5) };
}
