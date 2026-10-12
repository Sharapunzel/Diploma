import { aiPromptVersion, parseRule } from "./rules";

export type DraftIssue = { path: string; message: string; code?: string };
const object = (value: unknown): value is Record<string, unknown> =>
  !!value && typeof value === "object" && !Array.isArray(value);
const shown = (value: unknown) => JSON.stringify(value) ?? String(value);
const keyPattern = /^[A-Za-z][A-Za-z0-9_]{0,63}$/;

export function draftIssues(text: string): DraftIssue[] {
  let raw: unknown;
  try {
    raw = JSON.parse(text);
  } catch (error) {
    const location =
      error instanceof SyntaxError
        ? error.message.match(/position (\d+)(?: \(line (\d+) column (\d+)\))?/)
        : null;
    const message = location
      ? `Позиция ${location[1]}${location[2] ? `, строка ${location[2]}, столбец ${location[3]}` : ""}`
      : "Проверьте синтаксис";
    return [
      {
        path: "$",
        message: `Синтаксис JSON: ${message}. Исправьте текст; проверка структуры пока недоступна.`,
        code: "syntax",
      },
    ];
  }
  const issues: DraftIssue[] = [];
  const add = (path: string, message: string, code: string) =>
    issues.push({ path, message, code });
  if (!object(raw))
    return [
      { path: "$", message: "Ожидается объект RuleV1.", code: "root_object" },
    ];
  if (raw.format_version !== 1)
    add(
      "format_version",
      `Получено ${shown(raw.format_version)}; требуется 1.`,
      "version",
    );
  for (const field of Object.keys(raw))
    if (!["format_version", "variants"].includes(field))
      add(
        field,
        `Свойство ${shown(field)} не входит в корень RuleV1; допустимы format_version и variants.`,
        "extra_root",
      );
  if (!Array.isArray(raw.variants))
    return [
      ...issues,
      {
        path: "variants",
        message:
          "Ожидается массив из 1–16 вариантов; дальнейшая проверка невозможна.",
        code: "variants_array",
      },
    ];
  if (raw.variants.length < 1 || raw.variants.length > 16)
    add(
      "variants",
      `Получено ${raw.variants.length} вариантов; допустимо 1–16.`,
      "variants_count",
    );
  const variantKeys = new Set<string>();
  const priorities = new Set<number>();
  raw.variants.forEach((variant, vi) => {
    const path = `variants[${vi}]`;
    if (!object(variant)) {
      add(
        path,
        `Получено ${shown(variant)}; ожидается объект варианта.`,
        "variant_object",
      );
      return;
    }
    const name =
      typeof variant.key === "string" && keyPattern.test(variant.key)
        ? ` (${variant.key})`
        : "";
    for (const field of Object.keys(variant))
      if (!["key", "priority", "when", "blocks"].includes(field))
        add(
          `${path}.${field}`,
          `Свойство ${shown(field)} не входит в вариант; допустимы key, priority, when, blocks.`,
          "extra_variant",
        );
    if (typeof variant.key !== "string" || !keyPattern.test(variant.key))
      add(
        `${path}.key`,
        `Получено ${shown(variant.key)}; ключ: латинская буква, затем буквы/цифры/_, до 64 символов.`,
        "key_invalid",
      );
    else if (variantKeys.has(variant.key))
      add(
        `${path}.key`,
        `Ключ ${shown(variant.key)} повторяется; задайте уникальный ключ варианта.`,
        "key_duplicate",
      );
    if (typeof variant.key === "string") variantKeys.add(variant.key);
    if (
      !Number.isInteger(variant.priority) ||
      (variant.priority as number) < 0 ||
      (variant.priority as number) > 1_000_000
    )
      add(
        `${path}.priority`,
        `Получено ${shown(variant.priority)}; требуется целое 0–1000000.`,
        "priority_invalid",
      );
    else if (priorities.has(variant.priority as number))
      add(
        `${path}.priority`,
        `Приоритет ${shown(variant.priority)} повторяется; задайте уникальный priority.`,
        "priority_duplicate",
      );
    if (typeof variant.priority === "number") priorities.add(variant.priority);
    if (
      !object(variant.when) ||
      !["prefix", "contains", "regex"].includes(String(variant.when.kind)) ||
      typeof variant.when.value !== "string"
    )
      add(
        `${path}.when`,
        "Ожидается when с kind: prefix/contains/regex и строковым value.",
        "when_invalid",
      );
    if (!Array.isArray(variant.blocks)) {
      add(
        `${path}.blocks`,
        `Вариант${name}: ожидается массив блоков; дальнейшая проверка блоков невозможна.`,
        "blocks_array",
      );
      return;
    }
    if (variant.blocks.length < 1 || variant.blocks.length > 32)
      add(
        `${path}.blocks`,
        `Получено ${variant.blocks.length} блоков; допустимо 1–32.`,
        "blocks_count",
      );
    const blockKeys = new Set<string>();
    const mapped = new Set<string>();
    const targets = new Set<string>();
    variant.blocks.forEach((block, bi) => {
      const bp = `${path}.blocks[${bi}]`;
      if (!object(block)) {
        add(
          bp,
          `Получено ${shown(block)}; ожидается объект блока.`,
          "block_object",
        );
        return;
      }
      const bn = `Вариант${name}, блок ${shown(block.key)}`;
      if (typeof block.key !== "string" || !keyPattern.test(block.key))
        add(
          `${bp}.key`,
          `${bn}: ключ должен начинаться с латинской буквы; затем буквы/цифры/_, до 64 символов.`,
          "key_invalid",
        );
      else if (blockKeys.has(block.key))
        add(
          `${bp}.key`,
          `${bn}: ключ повторяется; задайте уникальный ключ блока.`,
          "key_duplicate",
        );
      if (typeof block.key === "string") blockKeys.add(block.key);
      if (block.kind === "extract")
        add(
          `${bp}.kind`,
          `${bn}: kind: "extract" не поддерживается; перенесите kind: "regex", input и candidates из extractor на уровень блока. Получено ${shown(block.kind)}.`,
          "kind_extract",
        );
      else if (block.kind === "date")
        add(
          `${bp}.kind`,
          `${bn}: kind: "date" не поддерживается; используйте map_ecs с target: "@timestamp", source и transform: {"date_format":"..."}. Получено ${shown(block.kind)}.`,
          "kind_date",
        );
      else if (
        !["regex", "template", "columns", "json", "map_ecs"].includes(
          String(block.kind),
        )
      )
        add(
          `${bp}.kind`,
          `${bn}: получено ${shown(block.kind)}; допустимы regex, template, columns, json, map_ecs.`,
          "kind_invalid",
        );
      if ("extractor" in block)
        add(
          `${bp}.extractor`,
          `${bn}: обёртка extractor не поддерживается; перенесите kind, input и candidates на уровень блока.`,
          "extractor_wrapper",
        );
      if (
        ["regex", "template", "columns", "json"].includes(String(block.kind))
      ) {
        for (const field of Object.keys(block))
          if (!["key", "kind", "input", "candidates"].includes(field))
            add(
              `${bp}.${field}`,
              `${bn}: свойство ${shown(field)} не входит в extractor-блок; используйте key, kind, input, candidates.`,
              "extra_extractor",
            );
        if (block.input !== undefined && typeof block.input !== "string")
          add(
            `${bp}.input`,
            `${bn}: input должен быть строкой log или block.field предшествующего извлекателя.`,
            "input_invalid",
          );
        if (
          !Array.isArray(block.candidates) ||
          block.candidates.length < 1 ||
          block.candidates.length > 8
        )
          add(
            `${bp}.candidates`,
            `${bn}: требуется 1–8 candidates внутри блока.`,
            "candidates_count",
          );
        if (Array.isArray(block.candidates))
          block.candidates.forEach((candidate, ci) => {
            const cp = `${bp}.candidates[${ci}]`;
            if (!object(candidate)) {
              add(cp, `${bn}: ожидается объект кандидата.`, "candidate_object");
              return;
            }
            const required =
              block.kind === "regex"
                ? "pattern"
                : block.kind === "template"
                  ? "template"
                  : block.kind === "columns"
                    ? "columns"
                    : "extract";
            if (!(required in candidate))
              add(
                `${cp}.${required}`,
                `${bn}: требуется ${required} внутри кандидата ${ci + 1}.`,
                `candidate_missing_${required}`,
              );
            if (
              block.kind === "columns" &&
              ![" ", ",", ";", "|", "\t"].includes(String(candidate.delimiter))
            )
              add(
                `${cp}.delimiter`,
                `${bn}: получено ${shown(candidate.delimiter)}; допустимы пробел, запятая, ;, | или табуляция.`,
                "delimiter_invalid",
              );
          });
      }
      if (block.kind === "map_ecs") {
        for (const field of Object.keys(block))
          if (
            ![
              "key",
              "kind",
              "target",
              "source",
              "required",
              "transform",
            ].includes(field)
          )
            add(
              `${bp}.${field}`,
              `${bn}: свойство ${shown(field)} не входит в map_ecs; допустимы key, kind, target, source, required, transform.`,
              "extra_map",
            );
        if (typeof block.target !== "string")
          add(
            `${bp}.target`,
            `${bn}: требуется строковый ECS target; сверяйте его с каталогом приложения.`,
            "target_missing",
          );
        else if (targets.has(block.target))
          add(
            `${bp}.target`,
            `${bn}: target ${shown(block.target)} повторяется в варианте.`,
            "target_duplicate",
          );
        if (typeof block.target === "string") targets.add(block.target);
        if (!object(block.source))
          add(
            `${bp}.source`,
            `${bn}: требуется одна форма source: ref, literal, refs+join либо refs+as_array:true.`,
            "source_missing",
          );
        else {
          const refs =
            typeof block.source.ref === "string"
              ? [block.source.ref]
              : Array.isArray(block.source.refs)
                ? block.source.refs
                : [];
          refs.forEach((ref) => {
            if (typeof ref === "string" && ref !== "log") {
              if (mapped.has(ref))
                add(
                  `${bp}.source`,
                  `${bn}: извлечённое поле ${shown(ref)} уже сопоставлено с ECS; используйте отдельное извлечение или пересмотрите цель.`,
                  "source_reused",
                );
              mapped.add(ref);
            }
          });
        }
      }
    });
  });
  if (!issues.length) {
    try {
      parseRule(text);
    } catch {
      add(
        "RuleV1",
        "Структура RuleV1 требует исправления. Проверка может быть неполной; окончательная компиляция выполняется сервером.",
        "unverified",
      );
    }
  } else {
    try {
      parseRule(text);
    } catch {
      add(
        "RuleV1",
        "Есть и другие проблемы структуры RuleV1. Остальные причины могут быть не обнаружены.",
        "unverified",
      );
    }
  }
  return issues;
}

export function aiFeedback(issues: DraftIssue[]): string {
  const children: Record<string, string[]> = {
    root: ["format_version", "variants"],
    variant: ["key", "priority", "when", "blocks"],
    when: ["kind", "value"],
    block: [
      "key",
      "kind",
      "extractor",
      "input",
      "candidates",
      "target",
      "source",
      "required",
      "transform",
    ],
    candidate: [
      "pattern",
      "template",
      "delimiter",
      "columns",
      "requires",
      "extract",
      "set",
    ],
    source: ["ref", "refs", "join", "as_array", "literal"],
    transform: ["date_format", "timezone", "year"],
  };
  const safePath = (path: string): { path: string; unknown: boolean } => {
    if (path === "$" || path === "RuleV1") return { path, unknown: false };
    const parts = path.split(/(?=\.)|(?=\[)/).filter(Boolean);
    const result: string[] = [];
    let level = "root";
    for (const part of parts) {
      if (
        /^\[\d+\]$/.test(part) &&
        [
          "variants",
          "blocks",
          "candidates",
          "columns",
          "requires",
          "refs",
        ].includes(level)
      ) {
        result.push(part);
        level =
          level === "variants"
            ? "variant"
            : level === "blocks"
              ? "block"
              : level === "candidates"
                ? "candidate"
                : "scalar";
        continue;
      }
      const name = part.startsWith(".") ? part.slice(1) : part;
      if (!children[level]?.includes(name))
        return {
          path: `${result.join("") || "$"}.[неподдерживаемое свойство]`,
          unknown: true,
        };
      result.push(result.length ? `.${name}` : name);
      level = [
        "variants",
        "blocks",
        "candidates",
        "when",
        "source",
        "transform",
        "columns",
        "requires",
        "refs",
      ].includes(name)
        ? name
        : "scalar";
    }
    return { path: result.join(""), unknown: false };
  };
  const safeMessages: Record<string, string> = {
    syntax:
      "JSON имеет синтаксическую ошибку. Исправьте текст; проверка структуры пока недоступна.",
    root_object:
      "Корень RuleV1 должен быть объектом с format_version: 1 и массивом variants.",
    version: "format_version должен равняться 1; исправьте версию правила.",
    extra_root:
      "Неподдерживаемое свойство корня. Оставьте только format_version и variants.",
    variants_array:
      "variants должен быть массивом из 1–16 вариантов; дальнейшая проверка пока невозможна.",
    variants_count:
      "Число вариантов должно быть от 1 до 16; добавьте или удалите варианты.",
    variant_object:
      "Вариант должен быть объектом с key, priority, when и blocks.",
    extra_variant:
      "Неподдерживаемое свойство варианта. Оставьте только key, priority, when и blocks.",
    key_invalid:
      "Ключ должен начинаться с латинской буквы; далее допустимы буквы, цифры и _, максимум 64 символа. Исправьте ключ.",
    key_duplicate: "Ключ повторяется на этом уровне; задайте уникальный key.",
    priority_invalid:
      "priority должен быть целым числом от 0 до 1000000; исправьте значение.",
    priority_duplicate:
      "priority повторяется у вариантов; задайте уникальное целое от 0 до 1000000.",
    when_invalid:
      "when должен быть объектом с kind: prefix, contains или regex и строковым value; исправьте условие.",
    blocks_array:
      "blocks должен быть массивом из 1–32 блоков; дальнейшая проверка блоков пока невозможна.",
    blocks_count:
      "Число блоков в варианте должно быть от 1 до 32; добавьте или удалите блоки.",
    block_object:
      "Блок должен быть объектом с key, допустимым kind и полями соответствующего типа.",
    kind_extract:
      'kind: "extract" не поддерживается; перенесите kind: "regex", input и candidates из extractor на уровень блока.',
    kind_date:
      'kind: "date" не поддерживается; используйте map_ecs с target: "@timestamp", source и transform: {"date_format":"..."}.',
    kind_invalid:
      "kind блока допускает только regex, template, columns, json или map_ecs; выберите один из них.",
    extractor_wrapper:
      "Обёртка extractor не поддерживается; перенесите kind, input и candidates на уровень блока.",
    extra_extractor:
      "Неподдерживаемое свойство extractor-блока. Оставьте key, kind, input и candidates.",
    input_invalid:
      "input должен быть строкой log или block.field более раннего извлекателя; исправьте ссылку.",
    candidates_count:
      "У extractor-блока должно быть от 1 до 8 candidates; добавьте или удалите кандидатов.",
    candidate_object:
      "Каждый candidate должен быть объектом с полями своего kind.",
    candidate_missing_pattern:
      "У regex-кандидата отсутствует pattern; добавьте строковый pattern внутрь кандидата.",
    candidate_missing_template:
      "У template-кандидата отсутствует template; добавьте строковый template внутрь кандидата.",
    candidate_missing_columns:
      "У columns-кандидата отсутствует columns; добавьте массив имён колонок внутрь кандидата.",
    candidate_missing_extract:
      "У json-кандидата отсутствует extract; добавьте объект extract внутрь кандидата, а не на уровень блока.",
    delimiter_invalid:
      "Недопустимый delimiter кандидата columns. Выберите пробел, запятую, ;, | или табуляцию.",
    extra_map:
      "Неподдерживаемое свойство map_ecs. Оставьте key, kind, target, source, required и необязательный transform.",
    target_missing:
      "У map_ecs требуется строковый ECS target; выберите поле из каталога приложения.",
    target_duplicate:
      "ECS target повторяется в варианте; укажите уникальную цель сопоставления.",
    source_missing:
      "source должен иметь одну форму: ref, literal, refs+join или refs+as_array:true; задайте источник.",
    source_reused:
      "Извлечённое поле уже используется для другой ECS-цели в этом варианте; выберите отдельный источник или пересмотрите сопоставление.",
    unverified:
      "Структура RuleV1 требует дальнейшей проверки. Список может быть неполным; окончательную компиляцию выполнит сервер.",
  };
  return `Замечания к RuleV1 (format_version: 1), версия промпта ${aiPromptVersion}.\nЭто только безопасные замечания: они не содержат исходный JSON, log или секреты. Используйте общую инструкцию RuleV1. Если прежнего правила и нужных образцов нет в диалоге, сначала запросите их; не исправляйте вслепую. Разберите каждый путь и причину ошибки. Когда данных достаточно и пользователь просит исправление, сохраните назначение правила и верните полный исправленный JSON без Markdown.\n${issues
    .map((issue, index) => {
      const path = safePath(issue.path);
      const unknownHint = [
        "extra_root",
        "extra_variant",
        "extra_extractor",
        "extra_map",
      ].includes(issue.code || "")
        ? safeMessages[issue.code || ""]
        : "Неподдерживаемое свойство на этом уровне. Удалите его или перенесите данные в допустимую форму RuleV1.";
      return `${index + 1}. ${path.path}: ${path.unknown ? unknownHint : safeMessages[issue.code || ""] || safeMessages.unverified}`;
    })
    .join(
      "\n",
    )}\nЭто локальные замечания; окончательную проверку выполнит сервер приложения.`;
}

export function draftRisks(text: string): string[] {
  let raw: unknown;
  try {
    raw = JSON.parse(text);
  } catch {
    return [];
  }
  if (!object(raw) || !Array.isArray(raw.variants)) return [];
  const variants = raw.variants.filter(object);
  const warnings: string[] = [];
  for (let i = 0; i < variants.length; i++)
    for (let j = 0; j < variants.length; j++) {
      if (i === j) continue;
      const first = variants[i];
      const later = variants[j];
      if (
        !object(first.when) ||
        !object(later.when) ||
        typeof first.when.value !== "string" ||
        typeof later.when.value !== "string" ||
        !first.when.value ||
        first.when.kind !== later.when.kind ||
        !["prefix", "contains"].includes(String(first.when.kind))
      )
        continue;
      if (
        typeof first.priority !== "number" ||
        typeof later.priority !== "number" ||
        first.priority >= later.priority
      )
        continue;
      const overlaps =
        first.when.kind === "prefix"
          ? later.when.value.startsWith(first.when.value)
          : later.when.value.includes(first.when.value);
      if (overlaps)
        warnings.push(
          `Проверьте порядок вариантов ${i + 1} и ${j + 1}: более раннее буквальное условие может перехватить события специфичного варианта. Это смысловой риск, а не доказанная ошибка правила.`,
        );
    }
  return warnings;
}
