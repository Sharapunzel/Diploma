import type { PreviewResult, RuleV1 } from "../../app/api/normalizers";

export type DebugOutcome = {
  position: number;
  result?: PreviewResult;
  error?: "transport" | "session" | "rule" | "invalid_envelope";
};

const explanations: Record<string, string> = {
  rule_invalid: "Структура правила не прошла проверку.",
  invalid_envelope: "Входная запись не соответствует формату.",
  log_too_long: "Строка лога превышает лимит.",
  no_variant_matched: "Ни один вариант правила не подошёл.",
  required_mapping_missing: "Обязательное поле не получено.",
  conversion_failed: "Не удалось преобразовать значение.",
  optional_mapping_missing: "Необязательное поле не получено.",
  event_time_fallback: "Время события взято из времени сбора.",
  empty_ecs: "Ни одно поле ECS не получено.",
  extraction_missing: "Блок извлечения не нашёл значение.",
};

function path(
  rule: RuleV1,
  variant: string | null,
  block: string | null,
  parameter = false,
) {
  const vi = rule.variants.findIndex((item) => item.key === variant);
  if (vi < 0) return "правило";
  const bi = rule.variants[vi].blocks.findIndex((item) => item.key === block);
  const location =
    bi < 0 ? `вариант ${vi + 1}` : `вариант ${vi + 1} / блок ${bi + 1}`;
  return parameter ? `${location} / параметр` : location;
}

export function safeDebugReport(outcomes: DebugOutcome[], rule: RuleV1) {
  return {
    format: "diploma-normalizer-debug-v1",
    total: outcomes.length,
    complete: outcomes.filter((item) => item.result?.status === "complete")
      .length,
    partial: outcomes.filter((item) => item.result?.status === "partial")
      .length,
    failed: outcomes.filter((item) => item.result?.status === "failed").length,
    infrastructure_errors: outcomes.filter((item) => item.error).length,
    entries: outcomes.map((item) => ({
      position: item.position,
      status: item.result?.status ?? "infrastructure_error",
      error: item.error ?? null,
      diagnostics: (item.result?.diagnostics ?? []).map((diagnostic) => {
        const allowed = Object.hasOwn(explanations, diagnostic.code);
        return {
          code: allowed ? diagnostic.code : "unknown",
          explanation: allowed
            ? explanations[diagnostic.code]
            : "Проверьте правило и входную запись.",
          path: path(
            rule,
            diagnostic.variant,
            diagnostic.block,
            diagnostic.field !== null,
          ),
        };
      }),
      trace: (item.result?.trace ?? []).map((step) => ({
        path: path(rule, step.variant, step.block),
        candidate: step.candidate === null ? null : step.candidate + 1,
        result: ["matched", "missing", "mapped", "conversion_failed"].includes(
          step.result,
        )
          ? step.result
          : "unknown",
      })),
    })),
  };
}
