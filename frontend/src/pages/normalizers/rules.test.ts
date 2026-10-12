import { describe, expect, it } from "vitest";
import { emptyRule, parseRule, referenceWarnings, templates } from "./rules";

describe("normalizer drafts", () => {
  it("loads four independent supported starter rules", () => {
    expect(templates.map((item) => item.id)).toEqual([
      "nginx",
      "iis",
      "syslog",
      "json",
    ]);
    for (const item of templates) {
      expect(parseRule(JSON.stringify(item.rule))).toEqual(item.rule);
      expect(item.sample).toEqual({
        timestamp: expect.any(String),
        log: expect.any(String),
      });
    }
  });
  it("rejects invalid or unsupported JSON without replacing the last rule", () => {
    const previous = emptyRule();
    expect(() => parseRule('{"format_version":1,')).toThrow();
    expect(() => parseRule('{"format_version":2,"variants":[]}')).toThrow();
    expect(previous.variants[0].blocks[0].kind).toBe("map_ecs");
  });
  it("flags references broken by a renamed extractor", () => {
    const rule = emptyRule();
    rule.variants[0].blocks = [
      {
        key: "first",
        kind: "regex",
        candidates: [{ pattern: "(?P<value>.+)" }],
      },
      {
        key: "mapped",
        kind: "map_ecs",
        target: "message",
        source: { ref: "first.value" },
        required: true,
      },
    ];
    expect(referenceWarnings(rule)).toEqual({});
    rule.variants[0].blocks[0].key = "renamed";
    expect(referenceWarnings(rule)["event/mapped"]).toContain(
      "Ссылка first.value не указывает на результат предшествующего блока.",
    );
  });
});
