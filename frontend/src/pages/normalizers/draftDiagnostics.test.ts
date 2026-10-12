import { describe, expect, it } from "vitest";
import { aiFeedback, draftIssues, draftRisks } from "./draftDiagnostics";
import { aiPrompt, aiPromptVersion, parseRule } from "./rules";

const draft = {
  format_version: 1,
  variants: [
    {
      key: "nginx",
      priority: 10,
      when: { kind: "contains", value: "GET " },
      blocks: [
        {
          key: "first",
          kind: "extract",
          extractor: {
            kind: "regex",
            input: "log",
            candidates: [{ pattern: "(?P<request>.+)" }],
          },
        },
        {
          key: "second",
          kind: "extract",
          extractor: {
            kind: "regex",
            input: "log",
            candidates: [{ pattern: "(?P<date>.+)" }],
          },
        },
        { key: "time", kind: "date", target: "@timestamp", date_format: "%Y" },
        {
          key: "method",
          kind: "map_ecs",
          target: "http.request.method",
          source: { ref: "first.request" },
          required: true,
        },
        {
          key: "path",
          kind: "map_ecs",
          target: "url.path",
          source: { ref: "first.request" },
          required: false,
        },
      ],
    },
  ],
};

describe("external AI handoff", () => {
  it("includes full block forms, source alternatives, date and server limitations", () => {
    expect(aiPromptVersion).toBe(3);
    for (const section of [
      "1. Зачем нужен нормализатор",
      "2. Сущности и порядок исполнения",
      "3. Разновидности и параметры",
      "4. Ограничения и порядок работы с пользователем",
      "5. Примеры",
    ])
      expect(aiPrompt).toContain(section);
    expect(aiPrompt).toContain("ECS 9.4.0");
    expect(aiPrompt).toContain("Сначала выясните недостающие сведения");
    expect(aiPrompt).toContain(
      "Если данных недостаточно, сначала задайте конкретные вопросы",
    );
    expect(aiPrompt).toContain("Режим разбора ошибки");
    expect(aiPrompt).toContain("#Fields");
    expect(aiPrompt).toContain("log_format");
    expect(aiPrompt).toContain("event.name в этом каталоге отсутствует");
    for (const kind of ["regex", "template", "columns", "json", "map_ecs"]) {
      const block = JSON.parse(
        aiPrompt
          .split("\n")
          .find(
            (line) =>
              line.startsWith(`{"key":`) && line.includes(`"kind":"${kind}"`),
          )!,
      );
      expect(block.kind).toBe(kind);
      expect(block.key).toBeTruthy();
      if (kind !== "map_ecs") {
        expect(block.input).toBeTruthy();
        expect(block.candidates).toHaveLength(1);
      }
    }
    for (const source of [
      '{"ref":"request.method"}',
      '{"literal":"GET"}',
      '{"refs":["request.method","request.path"],"join":" "}',
      '{"refs":["request.method","request.path"],"as_array":true}',
    ])
      expect(aiPrompt).toContain(source);
    expect(aiPrompt).toContain('"target":"@timestamp"');
    expect(aiPrompt).toContain(
      '"transform":{"date_format":"%d/%b/%Y:%H:%M:%S %z"}',
    );
    expect(aiPrompt).toContain("каталог ECS приложения");
    expect(aiPrompt).toContain("priority: 100");
    expect(aiPrompt).toContain("независимо от порядка в массиве");
    expect(aiPrompt).toContain(
      "when.regex проверяют совпадение, но не создают выходов block.field",
    );
    expect(aiPrompt).not.toContain('"input":"request.path","candidates"');
    const linked = JSON.parse(
      aiPrompt
        .split("\n")
        .find((line) =>
          line.startsWith('{"format_version":1,"variants":[{"key":"access"'),
        )!,
    );
    expect(linked.variants[0].blocks[1].source.ref).toBe("access.time");
    expect(linked.variants[0].blocks[0].key).toBe("access");
    expect(aiPrompt).toContain("Ожидаемый status: complete");
    const nested = JSON.parse(
      aiPrompt
        .split("\n")
        .find((line) =>
          line.startsWith('{"format_version":1,"variants":[{"key":"nested"'),
        )!,
    );
    expect(nested.variants[0].blocks[1].input).toBe("outer.body");
    expect(aiPrompt).not.toContain("secret-token");
  });

  it("reports independent unsupported shapes and reused source without copying the draft", () => {
    const input = JSON.stringify(draft);
    const issues = draftIssues(input);
    expect(issues.map((item) => item.path)).toEqual(
      expect.arrayContaining([
        "variants[0].blocks[0].kind",
        "variants[0].blocks[1].kind",
        "variants[0].blocks[2].kind",
        "variants[0].blocks[4].source",
      ]),
    );
    expect(
      issues.find((item) => item.path.endsWith("blocks[0].kind"))?.message,
    ).toContain('kind: "regex"');
    expect(
      issues.find((item) => item.path.endsWith("blocks[2].kind"))?.message,
    ).toContain("transform");
    const feedback = aiFeedback(issues);
    expect(feedback).toContain("variants[0].blocks[4].source");
    expect(feedback).toContain("сначала запросите их");
    expect(feedback).toContain("Разберите каждый путь и причину ошибки");
    expect(feedback).not.toContain(input);
    expect(feedback).not.toContain("secret-token");
    const privateIssue = aiFeedback(
      draftIssues(
        JSON.stringify({
          format_version: 2,
          variants: [
            {
              key: "SECRET_TOKEN",
              priority: 1,
              when: { kind: "contains", value: "x" },
              blocks: [
                { key: "SECRET_BLOCK", kind: "unknown", candidates: [] },
              ],
            },
          ],
        }),
      ),
    );
    expect(privateIssue).not.toContain("SECRET_TOKEN");
    expect(privateIssue).not.toContain("SECRET_BLOCK");
    expect(() => parseRule(input)).toThrow();
  });

  it("distinguishes syntax and accepts a manually corrected shape", () => {
    expect(draftIssues('{"format_version":')).toEqual([
      expect.objectContaining({ path: "$" }),
    ]);
    const fixed = structuredClone(draft) as unknown as {
      variants: { blocks: unknown[] }[];
    };
    fixed.variants[0].blocks = [
      {
        key: "first",
        kind: "regex",
        input: "log",
        candidates: [{ pattern: "(?P<request>.+)" }],
      },
      {
        key: "method",
        kind: "map_ecs",
        target: "http.request.method",
        source: { ref: "first.request" },
        required: true,
      },
    ];
    expect(draftIssues(JSON.stringify(fixed))).toEqual([]);
  });
  it("keeps possible priority shadowing separate from proven errors", () => {
    const sample = {
      format_version: 1,
      variants: [
        {
          key: "general",
          priority: 1,
          when: { kind: "contains", value: "GET" },
          blocks: [
            {
              key: "message",
              kind: "map_ecs",
              target: "message",
              source: { ref: "log" },
              required: true,
            },
          ],
        },
        {
          key: "specific",
          priority: 2,
          when: { kind: "contains", value: "GET /admin" },
          blocks: [
            {
              key: "message",
              kind: "map_ecs",
              target: "message",
              source: { ref: "log" },
              required: true,
            },
          ],
        },
      ],
    };
    expect(draftIssues(JSON.stringify(sample))).toEqual([]);
    expect(draftRisks(JSON.stringify(sample))).toEqual([
      expect.stringContaining("смысловой риск"),
    ]);
  });
  it("removes arbitrary property names from every copied path", () => {
    const secret = "SYNTHETIC_SECRET_PROPERTY_987";
    const input = JSON.stringify({
      format_version: 1,
      [secret]: true,
      variants: [
        {
          key: "web",
          priority: 10,
          when: { kind: "contains", value: "x" },
          [secret]: true,
          blocks: [
            {
              key: "message",
              kind: "map_ecs",
              target: "message",
              source: { ref: "log" },
              required: true,
              [secret]: true,
            },
          ],
        },
      ],
    });
    const issues = draftIssues(input);
    expect(issues.map((item) => item.path)).toEqual(
      expect.arrayContaining([
        secret,
        `variants[0].${secret}`,
        `variants[0].blocks[0].${secret}`,
      ]),
    );
    const copied = aiFeedback(issues);
    expect(copied).not.toContain(secret);
    expect(copied).toContain(
      "variants[0].blocks[0].[неподдерживаемое свойство]",
    );
    expect(copied).toContain("Неподдерживаемое свойство корня");
    expect(
      aiFeedback([
        {
          path: "variants[0].blocks[2].kind",
          code: "kind_extract",
          message:
            'kind: "extract" не поддерживается; используйте kind: "regex".',
        },
      ]),
    ).toContain('kind: "regex"');
    const malformedKey = aiFeedback(
      draftIssues(
        JSON.stringify({
          format_version: 1,
          variants: [
            {
              key: `bad)${secret}`,
              priority: 1,
              when: { kind: "contains", value: "x" },
              blocks: [
                {
                  key: "message",
                  kind: "map_ecs",
                  target: "message",
                  source: { ref: "log" },
                  required: true,
                },
              ],
            },
          ],
        }),
      ),
    );
    expect(malformedKey).not.toContain(secret);
    const quotedKey = aiFeedback(
      draftIssues(
        JSON.stringify({
          format_version: 1,
          variants: [
            {
              key: "web",
              priority: 1,
              when: { kind: "contains", value: "x" },
              blocks: [
                {
                  key: `x\\"${secret}`,
                  kind: "extract",
                  extractor: { kind: "regex" },
                },
              ],
            },
          ],
        }),
      ),
    );
    expect(quotedKey).not.toContain(secret);
  });

  it("copies every known constraint with its path and without user values", () => {
    const secret = "PRIVATE_VALUE_624";
    const cases: { raw: unknown; checks: [string, string, string][] }[] = [
      {
        raw: { format_version: 2, variants: [] },
        checks: [
          ["version", "format_version", "должен равняться 1"],
          ["variants_count", "variants", "от 1 до 16"],
        ],
      },
      {
        raw: {
          format_version: 1,
          variants: [
            {
              key: "bad-key",
              priority: -1,
              when: { kind: "unknown", value: 1 },
              blocks: [],
            },
          ],
        },
        checks: [
          ["key_invalid", "variants[0].key", "латинской буквы"],
          ["priority_invalid", "variants[0].priority", "0 до 1000000"],
          ["when_invalid", "variants[0].when", "prefix, contains или regex"],
          ["blocks_count", "variants[0].blocks", "от 1 до 32"],
        ],
      },
      {
        raw: {
          format_version: 1,
          variants: [
            {
              key: "web",
              priority: 10,
              when: { kind: "contains", value: "x" },
              blocks: [
                {
                  key: "extractor",
                  kind: "extract",
                  extractor: { kind: "regex" },
                },
                { key: "date", kind: "date" },
                { key: "other", kind: secret },
                {
                  key: "columns",
                  kind: "columns",
                  input: 4,
                  candidates: [{ delimiter: secret, columns: ["a"] }, {}],
                },
                { key: "json", kind: "json", candidates: [{}] },
                { key: "regex", kind: "regex", candidates: [{}] },
                { key: "template", kind: "template", candidates: [{}] },
              ],
            },
          ],
        },
        checks: [
          ["kind_extract", "variants[0].blocks[0].kind", 'kind: "regex"'],
          [
            "extractor_wrapper",
            "variants[0].blocks[0].extractor",
            "на уровень блока",
          ],
          ["kind_date", "variants[0].blocks[1].kind", "transform"],
          [
            "kind_invalid",
            "variants[0].blocks[2].kind",
            "regex, template, columns, json или map_ecs",
          ],
          ["input_invalid", "variants[0].blocks[3].input", "block.field"],
          [
            "delimiter_invalid",
            "variants[0].blocks[3].candidates[0].delimiter",
            "пробел, запятую, ;, | или табуляцию",
          ],
          [
            "candidate_missing_columns",
            "variants[0].blocks[3].candidates[1].columns",
            "массив имён колонок",
          ],
          [
            "candidate_missing_extract",
            "variants[0].blocks[4].candidates[0].extract",
            "внутрь кандидата",
          ],
          [
            "candidate_missing_pattern",
            "variants[0].blocks[5].candidates[0].pattern",
            "строковый pattern",
          ],
          [
            "candidate_missing_template",
            "variants[0].blocks[6].candidates[0].template",
            "строковый template",
          ],
        ],
      },
      {
        raw: {
          format_version: 1,
          variants: [
            {
              key: "web",
              priority: 10,
              when: { kind: "contains", value: "x" },
              blocks: [
                {
                  key: "first",
                  kind: "regex",
                  candidates: [{ pattern: "(?P<value>.+)" }],
                },
                { key: "first", kind: "regex", candidates: [] },
                {
                  key: "mapped",
                  kind: "map_ecs",
                  target: 3,
                  source: null,
                  required: true,
                },
                {
                  key: "mapped2",
                  kind: "map_ecs",
                  target: "message",
                  source: { ref: "first.value" },
                  required: true,
                },
                {
                  key: "mapped3",
                  kind: "map_ecs",
                  target: "message",
                  source: { ref: "first.value" },
                  required: true,
                },
              ],
            },
            {
              key: "web",
              priority: 10,
              when: { kind: "contains", value: "y" },
              blocks: [
                {
                  key: "m",
                  kind: "map_ecs",
                  target: "message",
                  source: { ref: "log" },
                  required: true,
                },
              ],
            },
          ],
        },
        checks: [
          ["key_duplicate", "variants[0].blocks[1].key", "Ключ повторяется"],
          ["candidates_count", "variants[0].blocks[1].candidates", "от 1 до 8"],
          [
            "target_missing",
            "variants[0].blocks[2].target",
            "каталога приложения",
          ],
          [
            "source_missing",
            "variants[0].blocks[2].source",
            "refs+as_array:true",
          ],
          ["target_duplicate", "variants[0].blocks[4].target", "повторяется"],
          ["source_reused", "variants[0].blocks[4].source", "другой ECS-цели"],
          ["priority_duplicate", "variants[1].priority", "повторяется"],
        ],
      },
    ];
    for (const { raw, checks } of cases) {
      const issues = draftIssues(JSON.stringify(raw));
      const copied = aiFeedback(issues);
      const lines = copied.split("\n").slice(2, -1);
      expect(lines).toHaveLength(issues.length);
      for (const [code, path, hint] of checks) {
        const index = issues.findIndex(
          (issue) => issue.code === code && issue.path === path,
        );
        expect(index).toBeGreaterThanOrEqual(0);
        expect(lines[index]).toContain(`${index + 1}. ${path}: `);
        expect(lines[index]).toContain(hint);
      }
      expect(copied).not.toContain(secret);
      expect(copied).not.toContain(
        "Проверьте обязательные поля, типы и допустимую форму",
      );
    }
  });
});
