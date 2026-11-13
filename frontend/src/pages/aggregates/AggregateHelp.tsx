import { InfoCircleOutlined } from "@ant-design/icons";
import { Button, Popover, Select } from "antd";
import { useState, type ReactNode } from "react";
import styles from "./Aggregates.module.css";

export function Help({
  text,
  name = "Описание",
}: {
  text: string;
  name?: string;
}) {
  const [open, setOpen] = useState(false);
  return (
    <Popover
      open={open}
      onOpenChange={setOpen}
      title={name}
      content={<div className={styles.helpContent}>{text}</div>}
      trigger="click"
    >
      <Button
        type="text"
        size="small"
        className={styles.help}
        aria-label={`Справка: ${name}`}
        aria-expanded={open}
        icon={<InfoCircleOutlined />}
        onKeyDownCapture={(event) => {
          if (open && event.key === "Escape") {
            event.preventDefault();
            event.stopPropagation();
            setOpen(false);
          }
        }}
      />
    </Popover>
  );
}

export function FieldLabel({
  children,
  help,
  required = false,
}: {
  children: ReactNode;
  help: string;
  required?: boolean;
}) {
  return (
    <span
      className={styles.fieldLabel}
      onClick={(event) => {
        if ((event.target as HTMLElement).closest("button,a")) return;
        event.currentTarget.parentElement
          ?.querySelector<HTMLElement>("input,textarea,select")
          ?.focus();
      }}
    >
      {children}{" "}
      {required && (
        <span className={styles.required} aria-label="Обязательное поле">
          *
        </span>
      )}{" "}
      <Help
        text={help}
        name={typeof children === "string" ? children : "Поле"}
      />
    </span>
  );
}

const operationLabels: Record<string, string> = {
  count: "Количество событий",
  distinct_count: "Количество уникальных значений",
  present_count: "Количество заполненных значений",
  missing_count: "Количество отсутствующих значений",
  true_count: "Количество истинных значений",
  false_count: "Количество ложных значений",
  sum: "Сумма",
  avg: "Среднее значение",
  min: "Минимум",
  max: "Максимум",
  range: "Размах",
  variance: "Дисперсия",
  stddev: "Стандартное отклонение",
  median: "Медиана",
  percentile: "Перцентиль",
  mode_count: "Частота самого частого значения",
  mode_share: "Доля самого частого значения",
  formula: "Формула над итогами окна",
  derive: "Формула по именованным признакам",
  add: "Сложение",
  subtract: "Вычитание",
  multiply: "Умножение",
  divide: "Деление",
  ratio: "Отношение",
  percent: "Процент",
  per_second: "Частота за секунду",
  abs: "Модуль",
  clamp: "Ограничение диапазоном",
  min_of: "Минимум из результатов",
  max_of: "Максимум из результатов",
  all: "Все условия",
  any: "Любое условие",
  not: "Отрицание условия",
  eq: "Равно",
  neq: "Не равно",
  in: "Входит в список",
  not_in: "Не входит в список",
  exists: "Поле присутствует",
  not_exists: "Поле отсутствует",
  gt: "Больше",
  gte: "Больше или равно",
  lt: "Меньше",
  lte: "Меньше или равно",
  between: "В диапазоне",
  contains: "Содержит текст",
  starts_with: "Начинается с текста",
  in_cidr: "Входит в подсеть",
  has_any: "Содержит любой элемент",
  has_all: "Содержит все элементы",
  has_none: "Не содержит указанные элементы",
};

export const operationLabel = (name: string) => operationLabels[name] ?? name;

const descriptions: Record<string, string> = {
  count: "Количество отобранных событий в окне. Поле ECS не требуется.",
  distinct_count: "Количество различных значений выбранного поля за окно.",
  present_count:
    "Количество событий с присутствующим значением поля подходящего типа.",
  missing_count: "Количество событий без значения выбранного поля.",
  true_count: "Количество событий, где логическое поле равно true.",
  false_count: "Количество событий, где логическое поле равно false.",
  sum: "Сумма значений числового поля в отобранных событиях окна.",
  avg: "Среднее арифметическое значений числового поля за окно.",
  min: "Наименьшее значение числового поля за окно.",
  max: "Наибольшее значение числового поля за окно.",
  range: "Разность максимального и минимального значений числового поля.",
  variance: "Дисперсия значений за окно с делением на число значений N.",
  stddev: "Стандартное отклонение значений числового поля за окно.",
  median: "Медиана числовых значений: перцентиль 0.5.",
  percentile:
    "Непрерывный перцентиль числовых значений. Задайте долю от 0 до 1.",
  mode_count: "Число появлений самого частого значения поля за окно.",
  mode_share: "Доля самого частого значения среди подходящих значений поля.",
  formula:
    "Формула над итогами окна: операции, константы, ссылки и собственные агрегирующие входы.",
  derive:
    "Формула над именованными признаками и константами, без собственных ECS-входов.",
  add: "Сложение двух итогов окна: a + b.",
  subtract: "Вычитание второго итога из первого: a − b.",
  multiply: "Умножение двух итогов окна: a × b.",
  divide:
    "Деление первого итога на второй: a / b. Нулевой знаменатель даёт пропуск.",
  ratio: "Отношение двух итогов окна: a / b. Нулевой знаменатель даёт пропуск.",
  percent: "Перевод доли в проценты: 100 × x.",
  per_second: "Средняя частота в секунду: итог / длительность окна в секундах.",
  abs: "Абсолютное значение итога окна.",
  clamp: "Ограничение значения нижней и верхней границами: clamp(x, min, max).",
  min_of: "Наименьший из двух или более итогов окна.",
  max_of: "Наибольший из двух или более итогов окна.",
  all: "Событие проходит, если выполняются все вложенные условия (И).",
  any: "Событие проходит, если выполняется хотя бы одно вложенное условие (ИЛИ).",
  not: "Отрицание одного вложенного условия (НЕ).",
  eq: "Значение поля равно указанному значению.",
  neq: "Поле присутствует, и его значение не равно указанному.",
  in: "Значение поля входит в указанный список одного типа.",
  not_in: "Поле присутствует, и его значение не входит в указанный список.",
  exists:
    "Значение поля присутствует в событии. Значение для сравнения не требуется.",
  not_exists:
    "Значение поля отсутствует в событии. Значение для сравнения не требуется.",
  gt: "Значение числового поля или даты строго больше заданного.",
  gte: "Значение числового поля или даты больше либо равно заданному.",
  lt: "Значение числового поля или даты строго меньше заданного.",
  lte: "Значение числового поля или даты меньше либо равно заданному.",
  between: "Число или дата находится между двумя границами включительно.",
  contains:
    "Строка содержит указанный буквальный текст, без регулярных выражений.",
  starts_with: "Строка начинается с указанного буквального текста.",
  in_cidr: "IP-адрес входит в указанную сеть CIDR.",
  has_any: "Массив поля содержит хотя бы одно из указанных значений.",
  has_all: "Массив поля содержит все указанные значения.",
  has_none: "Массив поля не содержит ни одного из указанных значений.",
  $ref: "Готовый итог именованного признака. Его собственные условия отбора сохраняются.",
  $value: "Точная числовая константа в формуле, без чтения событий.",
  $aggregate:
    "Итог операции над событиями одного окна с собственным полем и отбором.",
};

export function OperationSelect({
  label,
  name = label,
  value,
  options,
  onChange,
}: {
  label: string;
  name?: string;
  value: string;
  options: { name: string; label?: string }[];
  onChange: (value: string) => void;
}) {
  const description = (operation: string) =>
    descriptions[operation] ??
    "Операция из серверного каталога. После выбора выполните проверку правила и предварительный расчёт.";
  return (
    <div className={styles.field}>
      <FieldLabel help={description(value)}>{label}</FieldLabel>
      <Select
        aria-label={name}
        className={styles.fullWidth}
        classNames={{ popup: { root: styles.optionPopup } }}
        value={value}
        optionLabelProp="title"
        virtual={false}
        options={options.map((option) => ({
          value: option.name,
          title: option.label ?? operationLabel(option.name),
          label: (
            <span className={styles.optionDescription}>
              <span>{option.label ?? operationLabel(option.name)}</span>
              <small>{description(option.name)}</small>
            </span>
          ),
        }))}
        onChange={onChange}
      />
    </div>
  );
}
