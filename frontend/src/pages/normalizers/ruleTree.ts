import type { Block, RuleV1 } from "../../app/api/normalizers";

export type TreeNode = {
  index: number;
  block: Block;
  inputs: string[];
  outputs: string[];
  unknownOutput: boolean;
  parent: number | null;
  children: TreeNode[];
};

const isObject = (value: unknown): value is Record<string, unknown> =>
  !!value && typeof value === "object" && !Array.isArray(value);

export function possibleOutputs(block: Block): {
  names: string[];
  unknown: boolean;
} {
  if (block.kind === "map_ecs") return { names: [], unknown: false };
  const names = new Set<string>();
  let unknown = false;
  for (const candidate of block.candidates) {
    if (isObject(candidate.set))
      Object.keys(candidate.set).forEach((name) => names.add(name));
    if (block.kind === "regex") {
      const pattern = candidate.pattern;
      if (typeof pattern === "string")
        for (const match of pattern.matchAll(/\(\?P<([A-Za-z][A-Za-z0-9_]*)>/g))
          names.add(match[1]);
      names.add("match");
    } else if (block.kind === "template") {
      const template = candidate.template;
      if (typeof template === "string")
        for (const match of template.matchAll(/%\{([A-Za-z][A-Za-z0-9_]*)\}/g))
          names.add(match[1]);
      names.add("match");
    } else if (block.kind === "columns") {
      if (Array.isArray(candidate.columns))
        candidate.columns.forEach((name) => {
          if (typeof name === "string") names.add(name);
        });
      else unknown = true;
    } else if (block.kind === "json") {
      if (isObject(candidate.extract))
        Object.keys(candidate.extract).forEach((name) => names.add(name));
      else unknown = true;
    }
  }
  return { names: [...names], unknown: unknown || names.size === 0 };
}

export function blockInputs(block: Block): string[] {
  if (block.kind !== "map_ecs") return [block.input || "log"];
  if ("ref" in block.source) return [block.source.ref];
  if ("refs" in block.source) return block.source.refs;
  return [];
}

export function buildRuleTree(variant: RuleV1["variants"][number]): TreeNode[] {
  const nodes: TreeNode[] = [];
  const roots: TreeNode[] = [];
  variant.blocks.forEach((block, index) => {
    const inputs = blockInputs(block);
    const output = possibleOutputs(block);
    const producers = inputs
      .filter((ref) => ref !== "log")
      .map((ref) => {
        const [key, field, extra] = ref.split(".");
        if (!field || extra) return null;
        return nodes.find(
          (node) => node.block.key === key && node.outputs.includes(field),
        );
      })
      .filter((node): node is TreeNode => !!node);
    const parentNode = producers[0] || null;
    const node: TreeNode = {
      index,
      block,
      inputs,
      outputs: output.names,
      unknownOutput: output.unknown,
      parent: parentNode?.index ?? null,
      children: [],
    };
    nodes.push(node);
    if (parentNode) parentNode.children.push(node);
    else roots.push(node);
  });
  return roots;
}
