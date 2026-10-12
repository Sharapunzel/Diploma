import { describe, expect, it } from "vitest";
import type { RuleV1 } from "../../app/api/normalizers";
import { buildRuleTree, possibleOutputs } from "./ruleTree";

const variant: RuleV1["variants"][number] = {
  key: "create",
  priority: 10,
  when: { kind: "contains", value: "create" },
  blocks: [
    {
      key: "parser",
      kind: "regex",
      input: "log",
      candidates: [
        { pattern: "(?P<user>.+)", set: { action: "create" } },
        { pattern: "(?P<actor>.+)" },
      ],
    },
    {
      key: "independent",
      kind: "columns",
      input: "log",
      candidates: [{ delimiter: " ", columns: ["date", "time"] }],
    },
    {
      key: "nested",
      kind: "json",
      input: "parser.user",
      candidates: [{ requires: [], extract: { name: "user.name" } }],
    },
    {
      key: "ecs",
      kind: "map_ecs",
      target: "user.name",
      source: { refs: ["nested.name", "parser.actor"], join: " " },
      required: true,
    },
    {
      key: "constant",
      kind: "map_ecs",
      target: "event.action",
      source: { literal: "create" },
      required: false,
    },
    {
      key: "broken",
      kind: "map_ecs",
      target: "message",
      source: { ref: "absent.value" },
      required: false,
    },
  ],
};

describe("rule tree projection", () => {
  it("nests only real dependencies and preserves all references", () => {
    const roots = buildRuleTree(variant);
    expect(roots.map((node) => node.block.key)).toEqual([
      "parser",
      "independent",
      "constant",
      "broken",
    ]);
    expect(roots[0].children[0].block.key).toBe("nested");
    expect(roots[0].children[0].children[0].block.key).toBe("ecs");
    expect(roots[0].children[0].children[0].inputs).toEqual([
      "nested.name",
      "parser.actor",
    ]);
    expect(roots[2].inputs).toEqual([]);
    expect(roots[3].parent).toBeNull();
  });

  it("labels candidate outputs as possible fields without evaluating input", () => {
    expect(possibleOutputs(variant.blocks[0]).names).toEqual([
      "action",
      "user",
      "match",
      "actor",
    ]);
    expect(possibleOutputs(variant.blocks[1]).names).toEqual(["date", "time"]);
    expect(possibleOutputs(variant.blocks[2]).names).toEqual(["name"]);
  });
});
