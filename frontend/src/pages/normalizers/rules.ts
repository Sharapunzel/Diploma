import type { Block, RuleV1 } from "../../app/api/normalizers";
import nginx from "./templates/nginx.json";
import iis from "./templates/iis.json";
import syslog from "./templates/syslog.json";
import json from "./templates/json.json";

export const templates = [
  {
    id: "nginx",
    name: "Nginx combined",
    description: "Стандартная строка combined; проверьте формат access log.",
    sample: {
      timestamp: "2026-01-01T12:00:00Z",
      log: '192.0.2.10 - alice [01/Jan/2026:12:00:00 +0000] "GET /health HTTP/1.1" 200 12 "-" "Demo/1.0"',
    },
    rule: nginx,
  },
  {
    id: "iis",
    name: "IIS W3C",
    description:
      "Порядок колонок условный. Сверьте его со своей строкой #Fields и измените вручную.",
    sample: {
      timestamp: "2026-01-01T12:00:00Z",
      log: "2026-01-01 12:00:00 192.0.2.1 GET /health - 443 - 198.51.100.4 Demo - 200 0 0 3",
    },
    rule: iis,
  },
  {
    id: "syslog",
    name: "Syslog",
    description:
      "Пример для строки RFC 3164 с process: message; другие формы настройте отдельно.",
    sample: {
      timestamp: "2026-01-01T12:00:00Z",
      log: "Jan  1 12:00:00 host-a demo: Service started",
    },
    rule: syslog,
  },
  {
    id: "json",
    name: "JSON",
    description:
      "Объект JSON с event.action и необязательным message; вложенные пути меняются вручную.",
    sample: {
      timestamp: "2026-01-01T12:00:00Z",
      log: '{"event":{"action":"login"},"message":"Demo event"}',
    },
    rule: json,
  },
] as const;

export const emptyRule = (): RuleV1 => ({
  format_version: 1,
  variants: [
    {
      key: "event",
      priority: 10,
      when: { kind: "contains", value: "" },
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
});

const isObject = (value: unknown): value is Record<string, unknown> =>
  !!value && typeof value === "object" && !Array.isArray(value);
const keyPattern = /^[A-Za-z][A-Za-z0-9_]{0,63}$/;
const onlyKeys = (value: Record<string, unknown>, allowed: string[]) =>
  Object.keys(value).every((key) => allowed.includes(key));
export function parseRule(text: string): RuleV1 {
  const value: unknown = JSON.parse(text);
  if (
    !isObject(value) ||
    value.format_version !== 1 ||
    !Array.isArray(value.variants) ||
    !value.variants.length ||
    value.variants.length > 16
  )
    throw new Error("Требуется RuleV1 с format_version: 1 и 1–16 вариантами.");
  if (!onlyKeys(value, ["format_version", "variants"]))
    throw new Error("Правило содержит неподдерживаемые поля.");
  for (const variant of value.variants) {
    if (
      !isObject(variant) ||
      typeof variant.key !== "string" ||
      !keyPattern.test(variant.key) ||
      !Number.isInteger(variant.priority) ||
      !isObject(variant.when) ||
      !["prefix", "contains", "regex"].includes(String(variant.when.kind)) ||
      typeof variant.when.value !== "string" ||
      !Array.isArray(variant.blocks) ||
      !variant.blocks.length ||
      variant.blocks.length > 32
    )
      throw new Error("Проверьте структуру варианта, условие и блоки.");
    if (
      !onlyKeys(variant, ["key", "priority", "when", "blocks"]) ||
      !onlyKeys(variant.when, ["kind", "value"])
    )
      throw new Error(`Вариант ${variant.key} содержит неподдерживаемые поля.`);
    for (const block of variant.blocks) {
      if (
        !isObject(block) ||
        typeof block.key !== "string" ||
        !keyPattern.test(block.key) ||
        !["regex", "template", "columns", "json", "map_ecs"].includes(
          String(block.kind),
        )
      )
        throw new Error(`Некорректный блок в варианте ${variant.key}.`);
      if (
        block.kind === "map_ecs"
          ? typeof block.target !== "string" ||
            !isObject(block.source) ||
            typeof block.required !== "boolean"
          : !Array.isArray(block.candidates) ||
            !block.candidates.length ||
            block.candidates.length > 8
      )
        throw new Error(`Проверьте параметры блока ${block.key}.`);
      const allowed =
        block.kind === "map_ecs"
          ? ["key", "kind", "target", "source", "required", "transform"]
          : ["key", "kind", "input", "candidates"];
      if (!onlyKeys(block, allowed))
        throw new Error(`Блок ${block.key} содержит неподдерживаемые поля.`);
      if (block.kind === "map_ecs") {
        const source = block.source as Record<string, unknown>;
        const modes = ["ref", "literal", "refs"].filter((key) => key in source);
        if (
          modes.length !== 1 ||
          !onlyKeys(
            source,
            modes[0] === "refs" ? ["refs", "join", "as_array"] : modes,
          )
        )
          throw new Error(`Проверьте источник блока ${block.key}.`);
        if (
          (modes[0] === "ref" && typeof source.ref !== "string") ||
          (modes[0] === "refs" &&
            (!Array.isArray(source.refs) ||
              source.refs.length === 0 ||
              !source.refs.every((ref) => typeof ref === "string") ||
              (typeof source.join === "string") === (source.as_array === true)))
        )
          throw new Error(`Проверьте источник блока ${block.key}.`);
      } else
        for (const candidate of block.candidates as unknown[]) {
          if (!isObject(candidate))
            throw new Error(`Проверьте кандидатов блока ${block.key}.`);
          const fields =
            block.kind === "regex"
              ? ["pattern", "set"]
              : block.kind === "template"
                ? ["template", "set"]
                : block.kind === "columns"
                  ? ["delimiter", "columns", "set"]
                  : ["requires", "extract", "set"];
          if (
            !onlyKeys(candidate, fields) ||
            (block.kind === "regex" && typeof candidate.pattern !== "string") ||
            (block.kind === "template" &&
              typeof candidate.template !== "string") ||
            (block.kind === "columns" &&
              (!Array.isArray(candidate.columns) ||
                !candidate.columns.length ||
                !candidate.columns.every(
                  (name: unknown) => typeof name === "string",
                ) ||
                typeof candidate.delimiter !== "string")) ||
            (block.kind === "json" &&
              (!isObject(candidate.extract) ||
                Object.values(candidate.extract).some(
                  (path) => typeof path !== "string",
                ) ||
                (candidate.requires !== undefined &&
                  (!Array.isArray(candidate.requires) ||
                    !candidate.requires.every(
                      (path: unknown) => typeof path === "string",
                    ))))) ||
            (candidate.set !== undefined && !isObject(candidate.set))
          )
            throw new Error(`Проверьте кандидатов блока ${block.key}.`);
        }
    }
  }
  return value as RuleV1;
}

export function referenceWarnings(rule: RuleV1): Record<string, string[]> {
  const result: Record<string, string[]> = {};
  for (const variant of rule.variants) {
    const outputs = new Map<string, Set<string>>();
    const targets = new Set<string>();
    const usedRefs = new Set<string>();
    const blockKeys = new Set<string>();
    for (const block of variant.blocks) {
      const warnings: string[] = [];
      if (blockKeys.has(block.key))
        warnings.push(`Ключ блока ${block.key} повторяется в варианте.`);
      blockKeys.add(block.key);
      const refs =
        block.kind === "map_ecs"
          ? "ref" in block.source
            ? [block.source.ref]
            : "refs" in block.source
              ? block.source.refs
              : []
          : [block.input || "log"];
      for (const ref of refs)
        if (ref !== "log") {
          const [key, name, extra] = ref.split(".");
          if (extra || !name || !outputs.get(key)?.has(name))
            warnings.push(
              `Ссылка ${ref} не указывает на результат предшествующего блока.`,
            );
          if (block.kind === "map_ecs") {
            if (usedRefs.has(ref))
              warnings.push(`Извлечённое поле ${ref} уже сопоставлено с ECS.`);
            usedRefs.add(ref);
          }
        }
      if (block.kind === "map_ecs") {
        if (targets.has(block.target))
          warnings.push(
            `Поле ECS ${block.target} уже сопоставлено в варианте.`,
          );
        targets.add(block.target);
      }
      if (warnings.length) result[`${variant.key}/${block.key}`] = warnings;
      if (block.kind !== "map_ecs") {
        const names = new Set<string>();
        for (const candidate of block.candidates) {
          if (isObject(candidate.set))
            Object.keys(candidate.set).forEach((name) => names.add(name));
          if (block.kind === "regex" && typeof candidate.pattern === "string")
            for (const match of candidate.pattern.matchAll(
              /\(\?P<([A-Za-z][A-Za-z0-9_]*)>/g,
            ))
              names.add(match[1]);
          if (
            block.kind === "template" &&
            typeof candidate.template === "string"
          )
            for (const match of candidate.template.matchAll(
              /%\{([A-Za-z][A-Za-z0-9_]*)\}/g,
            ))
              names.add(match[1]);
          if (block.kind === "columns" && Array.isArray(candidate.columns))
            candidate.columns.forEach((name) => {
              if (typeof name === "string") names.add(name);
            });
          if (block.kind === "json" && isObject(candidate.extract))
            Object.keys(candidate.extract).forEach((name) => names.add(name));
        }
        if (block.kind === "regex" || block.kind === "template")
          names.add("match");
        outputs.set(block.key, names);
      }
    }
  }
  return result;
}

export function defaultBlock(kind: Block["kind"], index: number): Block {
  const key = `block${index}`;
  switch (kind) {
    case "regex":
      return {
        key,
        kind,
        input: "log",
        candidates: [{ pattern: "(?P<value>.+)" }],
      };
    case "template":
      return {
        key,
        kind,
        input: "log",
        candidates: [{ template: "%{value}" }],
      };
    case "columns":
      return {
        key,
        kind,
        input: "log",
        candidates: [{ delimiter: " ", columns: ["value"] }],
      };
    case "json":
      return {
        key,
        kind,
        input: "log",
        candidates: [{ requires: [], extract: { value: "value" } }],
      };
    case "map_ecs":
      return {
        key,
        kind,
        target: "message",
        source: { ref: "log" },
        required: true,
      };
  }
}

export const aiPromptVersion = 3;
export const aiPrompt = `Инструкция для внешнего ИИ · движок RuleV1 · версия промпта ${aiPromptVersion}

Используйте эту инструкцию в двух режимах: создание правила и разбор/исправление существующего. Не начинайте генерацию JSON сразу после просьбы «сделай правило для nginx/IIS/другого сервиса». Сначала выясните недостающие сведения по разделу 4. Если пользователь уже дал достаточные образцы и настройки, не задавайте повторных вопросов. Приложение не отправляет вам данные автоматически; пользователь сам решает, какие обезличенные образцы, ошибки и JSON передать.

1. Зачем нужен нормализатор
На вход поступает один атомарный Kafka-конверт {timestamp, log}. timestamp — время сбора Fluent Bit, log — сырая строка одного события. Нормализатор применяет своё JSON-правило RuleV1 только к log: распознаёт разновидность события, извлекает значения и сопоставляет их полям ECS. На выходе одного log бывает не более одного ECS-документа. Локальный каталог приложения основан на ECS 9.4.0. Не смешивайте format_version правила (всегда 1), версию ECS (9.4.0) и версию сохранённой сущности нормализатора.
Движок сам записывает event.original = log и ecs.version = 9.4.0. Эти два поля не назначают через map_ecs. Время события @timestamp желательно извлекать из log. Если его не удалось получить и оно не объявлено обязательным, движок подставит timestamp Fluent Bit с причиной и статусом partial; если обязательное @timestamp не получено — failed. Служебное время ручного Preview не заменяет время события. complete означает, что все сопоставления выбранного варианта получены и преобразованы; отсутствие необязательного даёт partial; отсутствие обязательного, пустой результат или отсутствие подходящего варианта даёт failed.

2. Сущности и порядок исполнения
Корень правила содержит только format_version и variants. variants — от 1 до 16 разновидностей строк одного источника: например access, error и известный JSON-формат. Каждый вариант содержит только key, priority, when и blocks. when решает, подходит ли ему вся строка log; меньший числовой priority проверяется раньше независимо от порядка в массиве. Выполняется только первый совпавший вариант, даже если его разбор завершится failed: к следующему варианту движок не переходит. Правило с одним вариантом может иметь priority: 100 — это не ошибка и не показатель важности события. Условия должны различать форматы достаточно точно: общий вариант не должен затенять частный.
В выбранном варианте blocks исполняются по порядку (1–32). Извлекатель (regex, template, columns или json) получает строку input: log либо поле результата более раннего извлекателя block.field; так задаётся вложенность без вложенного JSON-массива blocks. У извлекателя есть 1–8 candidates: альтернативы проверяются по порядку, применяется первая подошедшая. Извлечённые имена становятся ссылками вида block.field только после этого блока; match у regex/template содержит совпавший фрагмент. Само условие when, даже regex с группами, ничего не извлекает: when.regex проверяют совпадение, но не создают выходов block.field. Блок map_ecs берёт значение по source, проверяет тип target и записывает одно поле ECS. Нужен хотя бы один map_ecs в каждом варианте. Обязательность required относится к сопоставлению данного варианта, а не к аналогичному полю других вариантов. priority не обозначает тип или важность события.

3. Разновидности и параметры
Условие when содержит только kind и непустое value. prefix — буквальное начало log; contains — буквальная подстрока; regex — поиск по RE2, не по произвольному PCRE. Пробелы в value значимы. Например contains "GET" и contains "GET /admin": второму, более точному условию назначьте меньший priority, иначе первый перехватит строку. Не используйте contains со строкой регулярного выражения: для шаблона нужен kind: "regex".

Каждый извлекатель содержит key, kind, необязательный input (по умолчанию log) и candidates; никаких полей extractor-обёртки. В каждом candidate необязательный set задаёт до 32 констант с именами выходов (строка, число или boolean), не конфликтующих с извлечёнными именами.
regex: candidate.pattern — RE2-шаблон, поиск внутри input, именованные группы (?P<name>...) дают поля block.name. RE2 не поддерживает все возможности PCRE; ограничение pattern — 1024 символа. Для полного формата закрепляйте начало/конец ^ и $, если это действительно требуется.
template: candidate.template — целая строка с уникальными плейсхолдерами %{name}; буквальные части должны совпасть, извлечённые значения доступны как block.name. Шаблон сопоставляется со всей input-строкой.
columns: candidate.delimiter — один из: пробел, запятая, ;, |, табуляция; candidate.columns — упорядоченные уникальные имена 1–64. Число реально разобранных колонок должно точно совпасть со списком, иначе candidate не подходит. Для IIS W3C список и порядок перепишите из реального #Fields, не угадывайте по названию сервиса. Разные наборы колонок можно задать несколькими candidates, но сопоставления должны ссылаться на имена, реально доступные в подходящем наборе.
json: input должна быть строкой JSON-объекта. candidate.requires — необязательные пути, которые должны содержать значение; candidate.extract — обязательный объект из 1–32 пар {выходное_имя: "путь.в.объекте"}. Путь проходит по вложенным объектам через точку, не является JSONPath и не индексирует массивы. Отсутствующие или null-поля дают отсутствующее значение. Для JSON-фрагмента внутри обычного log сначала извлеките его текст предшествующим блоком, затем задайте input: "block.field".
Примеры полных блоков; каждый JSON-объект ниже — отдельный блок внутри blocks, не самостоятельное правило:
{"key":"request","kind":"regex","input":"log","candidates":[{"pattern":"(?P<method>GET|POST) (?P<path>[^ ]+) (?P<protocol>HTTP/[^ ]+)"}]}
{"key":"templated","kind":"template","input":"log","candidates":[{"template":"%{method} %{path} %{protocol}"}]}
{"key":"columns","kind":"columns","input":"log","candidates":[{"delimiter":" ","columns":["date","time","status_code"]}]}
{"key":"json_fields","kind":"json","input":"log","candidates":[{"requires":["event.action"],"extract":{"action":"event.action"}}]}

map_ecs содержит только key, kind, target, source, required и необязательный transform. target — точное допустимое поле каталога ECS приложения. source имеет ровно одну форму: {"ref":"request.method"} для одного значения, {"literal":"GET"} для константы, {"refs":["request.method","request.path"],"join":" "} для объединения строк, {"refs":["request.method","request.path"],"as_array":true} для поля ECS типа array. Ссылка ref может быть log только для строкового target; прочие ссылки — block.field более раннего извлекателя. Все refs должны присутствовать, иначе сопоставление отсутствует. Внутри одного варианта target уникален, и одно извлечённое поле не используют повторно для другой ECS-цели. Для разных значений в одну строковую цель используйте один refs+join; для массива — refs+as_array и только если target массивный. required — явный boolean; отмечайте обязательными действительно необходимые для этого формата поля, не все поля подряд.
{"key":"method","kind":"map_ecs","target":"http.request.method","source":{"ref":"request.method"},"required":true}

Тип target важен: движок принимает совместимые строковые значения; числовую строку автоматически приводит к допустимому integer/float, строку true/false — к boolean, IP проверяет, дату преобразует. Это не произвольный язык преобразований. Например строку всего HTTP request нельзя положить в http.request.method, её сначала разбивают на метод/путь/версию. transform поддерживает только дату и только у ECS date target: date_format описывает исходную строку, например %Y-%m-%d %H:%M:%S %z; допустимы %Y, %m, %b/%B, %d, %H, %M, %S, %z и литерал %%. Требуются полные дата и время. Если года нет в строке, задайте явный year; если зона есть — разберите %z, если нет — timezone:"UTC" допустимо лишь когда UTC подтверждён. Произвольные локальные зоны не поддерживаются. Если исходная дата уже ISO с зоной, transform обычно не нужен.
{"key":"event_time","kind":"map_ecs","target":"@timestamp","source":{"ref":"access.time"},"required":true,"transform":{"date_format":"%d/%b/%Y:%H:%M:%S %z"}}

Проверенные примеры target из локального ECS 9.4.0: @timestamp (date), message (string), event.action (string), event.outcome (string), event.category (array string), source.ip (ip), http.request.method (string), http.response.status_code (long), http.response.body.bytes (long), http.version (string), url.path (string), log.level (string), process.pid (long), user.name (string), host.name (string), user_agent.original (string), tags (array string). Это не полный каталог ECS приложения. Для остальных target пользователь должен предоставить запись из поиска полей приложения с типом; не выдумывайте ECS-поля. event.original и ecs.version заполняются движком; event.name в этом каталоге отсутствует; labels как объект здесь не является mappable target. Значения, не имеющие достоверного соответствия ECS, не притворяйте ECS-полями.

4. Ограничения и порядок работы с пользователем
Сначала сгруппированно спросите то, без чего нельзя составить достоверные варианты: версию сервиса и настроенные форматы/шаблоны; по одному обезличенному примеру каждого вида log и, желательно, примеры изменчивых/отсутствующих полей; для IIS — буквальную строку #Fields и порядок колонок; для nginx — конфигурацию log_format и образцы access/error/JSON, если эти потоки используются; формат и временную зону события; желаемые ECS-поля и какие из них обязательны. Уточните, объединяет ли Fluent Bit многострочные события в один атомарный log: RuleV1 сам строки не склеивает. Не просите пароли, токены, персональные или конфиденциальные логи; предложите обезличить образцы, сохранив структуру и экранирование. Не задавайте вопросы, ответы на которые уже есть в сообщении.
Если пользователь просит «все логи сервиса», выясните реально используемые форматы. Невозможно гарантировать покрытие произвольного custom log_format, JSON-схемы, всех версий ПО и будущих сообщений по одному названию сервиса. Если данных недостаточно, сначала задайте конкретные вопросы и НЕ выдавайте якобы полное правило. Когда вводных достаточно, верните только один полный валидный JSON RuleV1 без Markdown/комментариев/пояснений. Включите варианты для всех предоставленных различимых форматов, соблюдая лимит 16; если он недостаточен, сообщите об ограничении до генерации. Не добавляйте общий catch-all, который скрывает неизвестные форматы под успешным результатом.
Корень: только format_version:1 и variants. Вариант: только key, priority, when, blocks. key блока и варианта: латинская буква, затем латинские буквы/цифры/_, не более 64 символов; key блока уникален внутри варианта, key и priority вариантов уникальны. priority — целое 0–1000000; 10/20/30 — удобные интервалы, а не требование. Правило до 65536 байт UTF-8, вложенность JSON до 16 уровней; не добавляйте неизвестные свойства. Кандидаты проверяются слева направо, блоки — сверху вниз. Нельзя использовать kind:"extract", kind:"date", обёртку extractor, вычисляемые условия, арифметику, lower/upper, split, rename/remove или новые transform-операции: движок их не реализует.
Перед ответом мысленно проверьте JSON-синтаксис, допустимые поля каждого блока, уникальность ключей/приоритетов/target, ссылки только на предыдущие выходы, типы target, точность условий, порядок колонок, дату и временную зону. После вставки в приложение пользователь должен выполнить серверную компиляцию и Preview на каждом образце, проверить status, diagnostics, trace и ECS-результат. Внешняя модель не видит полный каталог ECS и не может сама гарантировать компиляцию; при ошибке в target попросите тип/описание поля из каталога приложения. Старые события не пересчитываются при исправлении правила.

Режим разбора ошибки: если пользователь прислал правило, сообщение сервера и/или пример log, сначала определите стадию проблемы: невалидный JSON, схема RuleV1, компиляция (RE2, ссылки, ECS target/тип), выбор when, выбор candidate, пропущенное поле, преобразование даты/типа или partial из-за fallback времени. Назовите вариант, блок, параметр и причину, используя путь/код ошибки сервера; не объявляйте ошибку исправленной без Preview. Если недостаёт самого правила, образца log, #Fields, зоны или каталога target, задайте только необходимые уточнения. Если причина ясна и пользователь просит исправить, верните полный исправленный JSON, не отдельный фрагмент; сохраняйте прочие варианты и смысл правила. Не подменяйте реальную причину правдоподобной догадкой. Отсутствующий необязательный mapping даёт partial; отсутствующий обязательный — failed; отсутствие @timestamp при необязательном сопоставлении даёт partial с fallback Fluent Bit.

5. Примеры
Связный пример RuleV1 (полный JSON)
{"format_version":1,"variants":[{"key":"access","priority":100,"when":{"kind":"contains","value":"GET /"},"blocks":[{"key":"access","kind":"regex","input":"log","candidates":[{"pattern":"(?P<time>[0-9]{2}/[A-Za-z]{3}/[0-9]{4}:[0-9]{2}:[0-9]{2}:[0-9]{2} [+-][0-9]{4}) (?P<method>GET|POST) (?P<path>[^ ]+) (?P<protocol>HTTP/[^ ]+)"}]},{"key":"event_time","kind":"map_ecs","target":"@timestamp","source":{"ref":"access.time"},"required":true,"transform":{"date_format":"%d/%b/%Y:%H:%M:%S %z"}},{"key":"method","kind":"map_ecs","target":"http.request.method","source":{"ref":"access.method"},"required":true},{"key":"path","kind":"map_ecs","target":"url.path","source":{"ref":"access.path"},"required":true}]}]}
Синтетический log: 01/Jan/2026:12:00:00 +0000 GET /health HTTP/1.1
Ожидаемый status: complete; ECS @timestamp = 2026-01-01T12:00:00Z, http.request.method = GET, url.path = /health. Это пример одного известного формата, НЕ универсальный парсер nginx.

Пример вложенности: regex-блок outer извлекает строку body из log, затем json-блок inner читает outer.body и выдаёт inner.action; map_ecs сопоставляет её event.action. Это не вложенный массив blocks и не обращение к JSON-объекту напрямую из ref.
{"format_version":1,"variants":[{"key":"nested","priority":10,"when":{"kind":"contains","value":"payload="},"blocks":[{"key":"outer","kind":"regex","input":"log","candidates":[{"pattern":"payload=(?P<body>.+)$"}]},{"key":"inner","kind":"json","input":"outer.body","candidates":[{"requires":["event.action"],"extract":{"action":"event.action"}}]},{"key":"action","kind":"map_ecs","target":"event.action","source":{"ref":"inner.action"},"required":true}]}]}
Синтетический log: payload={"event":{"action":"login"}}. Ожидаемый status: partial, потому что @timestamp берётся из времени Fluent Bit; ECS event.action = login.

Если пользователь после этой инструкции просит создать правило — сначала примените вопросы раздела 4. Если пользователь просит разобрать ошибку — используйте диагностический порядок раздела 4. Только после достаточных вводных возвращайте полный JSON.`;
