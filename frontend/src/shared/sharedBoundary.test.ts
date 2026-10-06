import { readFileSync, readdirSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";

const sharedRoot = resolve(process.cwd(), "src/shared");
const forbiddenLayerImport =
  /from\s+["'](?:\.\.\/)+(?:app|pages|widgets)(?:\/|["'])|from\s+["']@\/(?:app|pages|widgets)(?:\/|["'])/;

function sourceFiles(directory: string): string[] {
  return readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
    const path = `${directory}/${entry.name}`;
    if (entry.isDirectory()) return sourceFiles(path);
    return /\.[cm]?[jt]sx?$/.test(entry.name) ? [path] : [];
  });
}

describe("shared layer boundaries", () => {
  it("does not import app, pages, or widgets", () => {
    const violations = sourceFiles(sharedRoot).flatMap((file) => {
      const matches = readFileSync(file, "utf8")
        .split(/\r?\n/)
        .flatMap((line, index) =>
          forbiddenLayerImport.test(line) ? [`${file}:${index + 1}`] : [],
        );
      return matches;
    });

    expect(violations).toEqual([]);
  });
});
