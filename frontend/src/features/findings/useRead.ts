import { useEffect, useState } from "react";

export function useRead<T>(load: ((signal: AbortSignal) => Promise<T>) | null) {
  const [attempt, setAttempt] = useState(0);
  const [result, setResult] = useState<{
    load: typeof load;
    attempt: number;
    data?: T;
    error?: unknown;
  }>();
  useEffect(() => {
    if (!load) return;
    const controller = new AbortController();
    void load(controller.signal).then(
      (data) => {
        if (!controller.signal.aborted) setResult({ load, attempt, data });
      },
      (error: unknown) => {
        if (!controller.signal.aborted) setResult({ load, attempt, error });
      },
    );
    return () => controller.abort();
  }, [load, attempt]);
  const current =
    result && result.load === load && result.attempt === attempt
      ? result
      : undefined;
  return {
    data: current?.data,
    error: current?.error,
    loading: load !== null && current === undefined,
    retry: () => setAttempt((value) => value + 1),
  };
}
