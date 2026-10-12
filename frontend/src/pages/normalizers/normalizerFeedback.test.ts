import { describe, expect, it } from "vitest";
import { ApiError } from "../../app/api/client";
import type { PreviewResult, RuleV1 } from "../../app/api/normalizers";
import { edgeSpaces, previewReasons, ruleIssues } from "./normalizerFeedback";

const rule: RuleV1 = {
  format_version: 1,
  variants: [
    {
      key: "event",
      priority: 10,
      when: { kind: "contains", value: "demo" },
      blocks: [
        {
          key: "parser",
          kind: "columns",
          input: "log",
          candidates: [{ delimiter: " ", columns: ["a", "b", "bad-name"] }],
        },
      ],
    },
  ],
};

describe("normalizer feedback", () => {
  it("locates every safe validation error through union paths", () => {
    const error = new ApiError(
      422,
      "validation_error",
      "req-1",
      "secret server message",
      {
        errors: [
          {
            loc: [
              "body",
              "rule",
              "variants",
              0,
              "blocks",
              0,
              "columns",
              "candidates",
              0,
              "columns",
              2,
            ],
            type: "string_pattern_mismatch",
            msg: "unsafe",
          },
          { loc: ["body", "name"], type: "string_too_short", msg: "unsafe" },
        ],
      },
    );
    const issues = ruleIssues(error, rule);
    expect(issues).toHaveLength(2);
    expect(issues[0].path).toBe(
      "rule.variants[0].blocks[0].candidates[0].columns[2]",
    );
    expect(issues[0]).toMatchObject({
      variant: 0,
      block: 0,
      candidate: 0,
      field: "columns",
    });
    expect(issues[0].message).toContain("3-е имя колонки");
    expect(issues[0].message).toContain("Дефис недопустим");
    expect(issues[1].path).toBe("name");
    expect(JSON.stringify(issues)).not.toContain("unsafe");
  });

  it("explains a compiler parameter without exposing server text", () => {
    const error = new ApiError(
      422,
      "normalizer_rule_invalid",
      "req-2",
      "sensitive",
      { variant: "event", block: "parser", parameter: "candidates.0.columns" },
    );
    const [issue] = ruleIssues(error, rule);
    expect(issue).toMatchObject({
      variant: 0,
      block: 0,
      candidate: 0,
      field: "columns",
    });
    expect(issue.message).toContain("имена колонок");
    expect(issue.message).not.toContain("sensitive");
  });

  it("keeps edge spaces explicit and lists every preview cause", () => {
    expect(edgeSpaces(" regex ")).toBe(true);
    expect(edgeSpaces("a b")).toBe(false);
    const result: PreviewResult = {
      status: "partial",
      variant_key: "event",
      ecs_data: {},
      fluent_bit_collected_at: null,
      diagnostics: [
        {
          code: "event_time_fallback",
          variant: "event",
          block: null,
          field: "@timestamp",
        },
        {
          code: "optional_mapping_missing",
          variant: "event",
          block: "parser",
          field: "user.name",
        },
      ],
      trace: [],
    };
    expect(previewReasons(result)).toHaveLength(2);
    expect(previewReasons(result)[0]).toContain("служебное время");
    expect(previewReasons(result)[1]).toContain("user.name");
    expect(
      previewReasons({ ...result, status: "failed", diagnostics: [] })[0],
    ).toContain("не указал причину");
  });
});
