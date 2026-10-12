import { ApiError } from "../../app/api/client";
import type { PreviewResult, RuleV1 } from "../../app/api/normalizers";

export type RuleIssue = {
  path: string;
  message: string;
  variant: number | null;
  block: number | null;
  candidate: number | null;
  field: string | null;
};

const object = (value: unknown): value is Record<string, unknown> =>
  !!value && typeof value === "object" && !Array.isArray(value);
const indexAfter = (path: (string | number)[], key: string) => {
  for (let position = path.length - 2; position >= 0; position--)
    if (path[position] === key && typeof path[position + 1] === "number")
      return path[position + 1] as number;
  return null;
};
const unionNames = new Set([
  "RegexBlock",
  "TemplateBlock",
  "ColumnsBlock",
  "JsonBlock",
  "MapEcsBlock",
  "regex",
  "template",
  "columns",
  "json",
  "map_ecs",
]);
const lastField = (path: (string | number)[]) =>
  [...path].reverse().find((part, reverseIndex): part is string => {
    if (
      typeof part !== "string" ||
      ["body", "rule", "variants", "blocks", "candidates"].includes(part)
    )
      return false;
    const index = path.length - reverseIndex - 1;
    return !(
      unionNames.has(part) &&
      typeof path[index - 1] === "number" &&
      path[index - 2] === "blocks"
    );
  });
const fieldLabel: Record<string, string> = {
  name: "имя нормализатора",
  description: "описание",
  key: "ключ",
  priority: "приоритет",
  value: "значение условия",
  kind: "тип",
  input: "вход",
  pattern: "шаблон RE2",
  template: "шаблон",
  columns: "имена колонок",
  delimiter: "разделитель",
  extract: "JSON-пути",
  requires: "обязательные JSON-пути",
  set: "константы set",
  target: "поле ECS",
  source: "источник значения",
  ref: "ссылка",
  refs: "ссылки",
  literal: "константа",
  join: "разделитель join",
  required: "признак обязательности",
  transform: "преобразование даты",
  date_format: "формат даты",
  year: "год",
};
const keyHint =
  "Имя должно начинаться с латинской буквы; далее допустимы латинские буквы, цифры и _. Дефис недопустим.";

function locationMessage(path: (string | number)[], type: string): string {
  const field = lastField(path);
  const column = indexAfter(path, "columns");
  const fieldName =
    column !== null
      ? `${column + 1}-е имя колонки`
      : fieldLabel[field || ""] || "параметр";
  if (
    column !== null ||
    (["key", "input", "ref", "refs", "extract", "set"].includes(field || "") &&
      type.includes("pattern"))
  )
    return `${fieldName}: ${keyHint}`;
  if (type.includes("missing"))
    return `${fieldName}: заполните обязательное значение.`;
  if (type.includes("too_long"))
    return `${fieldName}: превышена допустимая длина.`;
  if (type.includes("too_short"))
    return `${fieldName}: значение слишком короткое.`;
  if (type.includes("less_than") || type.includes("greater_than"))
    return `${fieldName}: число вне допустимого диапазона.`;
  if (type.includes("literal") || type.includes("enum"))
    return `${fieldName}: выберите допустимое значение.`;
  if (type.includes("list"))
    return `${fieldName}: проверьте число элементов и их формат.`;
  if (type.includes("string_pattern")) return `${fieldName}: ${keyHint}`;
  return `${fieldName}: проверьте формат и допустимое значение.`;
}

function pathString(path: (string | number)[]): string {
  const clean = path.filter(
    (part, index) =>
      part !== "body" &&
      !(
        unionNames.has(String(part)) &&
        typeof path[index - 1] === "number" &&
        path[index - 2] === "blocks"
      ),
  );
  return clean.reduce<string>(
    (result, part) =>
      typeof part === "number"
        ? `${result}[${part}]`
        : `${result}${result ? "." : ""}${part}`,
    "",
  );
}

function compileMessage(parameter: string): string {
  if (parameter.includes("key/priority"))
    return "Ключ или приоритет варианта повторяется. Задайте уникальные значения.";
  if (parameter.includes("source"))
    return "Проверьте источник: ссылка должна указывать на существующее поле предшествующего блока; для нескольких ссылок проверьте каждую.";
  if (parameter.includes("target"))
    return "Проверьте ECS target: поле должно быть допустимым, совместимым по типу и не повторяться.";
  if (parameter.includes("candidates") && parameter.includes("columns"))
    return `Проверьте имена колонок. ${keyHint}`;
  if (parameter.includes("candidates") && parameter.includes("template"))
    return "Проверьте имена и повторения плейсхолдеров %{field}.";
  if (parameter.includes("candidates") && parameter.includes("pattern"))
    return "Проверьте шаблон RE2 и имена групп захвата.";
  if (parameter.includes("candidates") && parameter.includes("set"))
    return "Проверьте имена и значения констант set.";
  if (parameter.includes("candidates"))
    return "Проверьте параметры кандидата и формат извлечения.";
  if (parameter.includes("transform"))
    return "Проверьте формат даты, год и допустимый часовой пояс UTC.";
  if (parameter.includes("priority"))
    return "Проверьте уникальность и допустимый диапазон приоритета.";
  if (parameter.includes("key")) return `Проверьте ключ блока. ${keyHint}`;
  if (parameter.includes("blocks"))
    return "Проверьте порядок блоков и ссылки на их выходы.";
  return "Правило не прошло серверную проверку. Проверьте указанный параметр.";
}

export function ruleIssues(error: unknown, rule: RuleV1): RuleIssue[] {
  if (!(error instanceof ApiError) || !object(error.details)) return [];
  if (error.code === "validation_error" && Array.isArray(error.details.errors))
    return error.details.errors.flatMap((entry): RuleIssue[] => {
      if (!object(entry) || !Array.isArray(entry.loc)) return [];
      const path = entry.loc.filter(
        (part): part is string | number =>
          typeof part === "string" || typeof part === "number",
      );
      const variant = indexAfter(path, "variants");
      const block = indexAfter(path, "blocks");
      const candidate = indexAfter(path, "candidates");
      const field = lastField(path) || null;
      const location = pathString(path);
      return [
        {
          path: location,
          message: locationMessage(path, String(entry.type || "")),
          variant,
          block,
          candidate,
          field,
        },
      ];
    });
  if (error.code !== "normalizer_rule_invalid") return [];
  const details = error.details;
  const variant =
    typeof details.variant === "string"
      ? rule.variants.findIndex((item) => item.key === details.variant)
      : -1;
  const block =
    variant >= 0 && typeof details.block === "string"
      ? rule.variants[variant].blocks.findIndex(
          (item) => item.key === details.block,
        )
      : -1;
  const parameter =
    typeof details.parameter === "string" ? details.parameter : "rule";
  const candidateMatch = parameter.match(/candidates\.(\d+)/);
  const path = `${variant >= 0 ? `rule.variants[${variant}]` : "rule"}${block >= 0 ? `.blocks[${block}]` : ""}.${parameter}`;
  return [
    {
      path,
      message: compileMessage(parameter),
      variant: variant >= 0 ? variant : null,
      block: block >= 0 ? block : null,
      candidate: candidateMatch ? Number(candidateMatch[1]) : null,
      field: parameter.split(".").at(-1) || null,
    },
  ];
}

export function edgeSpaces(value: string): boolean {
  return value.length > 0 && value !== value.trim();
}

export function previewReasons(result: PreviewResult): string[] {
  if (result.status === "complete")
    return ["Правило обработало пример полностью."];
  if (!result.diagnostics.length)
    return ["Сервер не указал причину. Проверьте правило и строку log."];
  return result.diagnostics.map((item) => {
    const location = [
      item.variant && `вариант ${item.variant}`,
      item.block && `блок ${item.block}`,
      item.field && `поле ${item.field}`,
    ]
      .filter(Boolean)
      .join(" / ");
    const where = location ? `${location}: ` : "";
    const message: Record<string, string> = {
      event_time_fallback:
        "Не получено @timestamp из правила; использовано служебное время проверки, поэтому результат частичный. Проверьте сопоставление даты и времени события.",
      optional_mapping_missing:
        "Не получено необязательное поле ECS. Проверьте исходную ссылку и извлечение.",
      required_mapping_missing:
        "Не получено обязательное поле ECS, поэтому проверка не завершилась. Проверьте извлечение и ссылку.",
      conversion_failed:
        "Значение не преобразовано в тип ECS. Проверьте формат и преобразование.",
      no_variant_matched:
        "Ни одно условие не подошло. Проверьте prefix/contains/regex и пробелы по краям условия.",
      extraction_missing:
        "Блок извлечения не получил значение. Проверьте вход и кандидатов.",
      empty_ecs: "Не удалось получить поля ECS. Проверьте сопоставления.",
      rule_invalid: "Правило не прошло проверку структуры и ссылок.",
      invalid_envelope: "Проверьте строку log и служебное время проверки.",
      log_too_long: "Строка log превышает допустимый размер.",
    };
    return `${where}${message[item.code] || "Сервер сообщил о проблеме без известного описания."} (${item.code})`;
  });
}
