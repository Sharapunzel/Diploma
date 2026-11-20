/** One local monotonic budget. The profile's global timeout remains external. */
export class Deadline {
  readonly started: number;
  readonly expires: number;

  constructor(
    milliseconds: number,
    private readonly clock = () => performance.now(),
    parent?: Deadline,
  ) {
    this.started = clock();
    this.expires = Math.min(
      this.started + milliseconds,
      parent?.expires ?? Infinity,
    );
  }

  child(milliseconds: number) {
    return new Deadline(milliseconds, this.clock, this);
  }

  snapshot() {
    const now = this.clock();
    return {
      elapsed_ms: now - this.started,
      remaining_ms: Math.max(0, this.expires - now),
    };
  }

  remaining(maximum = Infinity) {
    const remaining = this.expires - this.clock();
    if (remaining <= 0) throw new Error("Browser phase budget exhausted");
    return Math.min(maximum, remaining);
  }

  async run<T>(work: () => Promise<T>) {
    this.remaining();
    const result = await work();
    this.remaining();
    return result;
  }
}

/** Finite test metadata at an existing phase boundary; never changes its result. */
export async function withPhaseEnd<T>(
  work: () => Promise<T>,
  end: (outcome: "complete" | "failed") => void,
  unavailable: (error: unknown) => void,
) {
  let outcome: "complete" | "failed" = "failed";
  try {
    const result = await work();
    outcome = "complete";
    return result;
  } finally {
    try {
      end(outcome);
    } catch (error) {
      try {
        unavailable(error);
      } catch {
        // Diagnostic emit failure cannot hide an assertion or prevent cleanup.
      }
    }
  }
}
