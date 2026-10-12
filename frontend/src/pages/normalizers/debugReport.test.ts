import { describe, expect, it } from "vitest";
import { safeDebugReport } from "./debugReport";
import type { RuleV1 } from "../../app/api/normalizers";

describe("safe debug report", () => {
  it("exports only known diagnostics and structural paths", () => {
    const secret = "password=private";
    const rule: RuleV1 = {
      format_version: 1,
      variants: [
        {
          key: secret,
          priority: 1,
          when: { kind: "contains", value: secret },
          blocks: [],
        },
      ],
    };
    const report = safeDebugReport(
      [
        {
          position: 2,
          result: {
            status: "failed" as const,
            variant_key: secret,
            ecs_data: { value: secret },
            fluent_bit_collected_at: null,
            diagnostics: [
              {
                code: "required_mapping_missing",
                variant: secret,
                block: null,
                field: secret,
              },
            ],
            trace: [
              {
                variant: secret,
                block: secret,
                candidate: 0,
                result: "missing",
              },
            ],
          },
        },
      ],
      rule,
    );
    const exported = JSON.stringify(report);
    expect(exported).not.toContain(secret);
    expect(exported).toContain("required_mapping_missing");
    expect(exported).toContain("вариант 1");
  });

  it("serializes only allowlisted diagnostics and never leaks user data", () => {
    const secret = "credential=do-not-export";
    const rule = {
      format_version: 1 as const,
      variants: [
        {
          key: secret,
          priority: 1,
          when: { kind: "contains" as const, value: secret },
          blocks: [],
        },
      ],
    };
    const diagnostics = ["constructor", "toString", "__proto__", secret].map(
      (code) => ({
        code,
        variant: secret,
        block: secret,
        field: secret,
      }),
    );
    const outcome = {
      position: 0,
      result: {
        status: "failed" as const,
        variant_key: secret,
        ecs_data: { [secret]: secret },
        fluent_bit_collected_at: null,
        diagnostics,
        trace: [],
      },
    };
    Object.assign(outcome, { log: secret });
    const report = safeDebugReport([outcome], rule);
    const serialized = JSON.stringify(report);
    expect(() => JSON.parse(serialized)).not.toThrow();
    expect(serialized).not.toContain(secret);
    const entries = JSON.parse(serialized).entries[0].diagnostics;
    expect(entries.map((item: { code: string }) => item.code)).toEqual([
      "unknown",
      "unknown",
      "unknown",
      "unknown",
    ]);
    expect(
      entries.every(
        (item: { explanation: string }) =>
          item.explanation === "Проверьте правило и входную запись.",
      ),
    ).toBe(true);
  });
});
