import { ArrowRightOutlined } from "@ant-design/icons";
import { Button, Modal, Spin, Table, Tag } from "antd";
import { Link } from "react-router-dom";
import type {
  FindingCard,
  FindingFeature,
  FindingRow,
} from "../../app/api/findings";
import { AppRequestError } from "../../app/ui/AppRequestError";
import { findingEventsPath } from "../../features/events/findingLink";
import { timeBasisHelp, timeBasisLabel } from "../../features/findings/labels";
import { displayZone, exactLocalTime } from "../../shared/time/utc";
import tableStyles from "../../shared/ui/WorkspaceTable.module.css";
import { ColumnLabel } from "./ColumnLabel";
import styles from "./Findings.module.css";

const featureColors = [
  "#2f80d0",
  "#d48f27",
  "#8b62d1",
  "#299578",
  "#c85170",
  "#6477a1",
];
const percent = new Intl.NumberFormat("ru-RU", {
  style: "percent",
  maximumSignificantDigits: 4,
});

export function FindingFlags({ finding }: { finding: FindingRow }) {
  if (!finding.late && !finding.accelerated_used) return null;
  return (
    <div className={styles.flags}>
      {finding.late && <Tag>Запоздалая оценка</Tag>}
      {finding.accelerated_used && (
        <Tag>
          <span aria-hidden="true">🔥</span> Использован ускоренный прогрев
        </Tag>
      )}
    </div>
  );
}

function Attribution({ features }: { features: FindingFeature[] }) {
  if (
    !features.length ||
    features.some((feature) => feature.attribution === null)
  )
    return <p>В снимке нет полного набора вкладов RCF.</p>;
  const maximum = features.reduce(
    (max, feature) => Math.max(max, feature.attribution!),
    0,
  );
  if (!maximum) return <p>Все сохранённые вклады равны нулю.</p>;

  const total = features.reduce(
    (sum, feature) => sum + feature.attribution! / maximum,
    0,
  );
  let accumulated = 0;
  const entries = features.map((feature, index) => {
    const share = feature.attribution! / maximum / total;
    const start = accumulated;
    accumulated += share;
    return {
      ...feature,
      share,
      start,
      end: accumulated,
      color: featureColors[index % featureColors.length],
    };
  });
  const segments = entries
    .filter(({ share }) => share > 0)
    .map(({ color, start, end }) => `${color} ${start * 100}% ${end * 100}%`);
  const background =
    segments.length === 1
      ? entries.find(({ share }) => share > 0)!.color
      : `conic-gradient(${segments.join(", ")})`;
  return (
    <div className={styles.attribution}>
      <div className={styles.pie} style={{ background }} aria-hidden="true" />
      <ul
        className={styles.legend}
        aria-label="Доли вкладов признаков"
        tabIndex={0}
      >
        {entries.map(({ key, name, color, share }) => (
          <li key={key}>
            <span
              className={styles.swatch}
              style={{ background: color }}
              aria-hidden="true"
            />
            <span>
              <strong>{name}</strong> · {percent.format(share)}
            </span>
          </li>
        ))}
      </ul>
    </div>
  );
}

function RangeLink({ finding }: { finding: FindingCard }) {
  const { source_id: sourceId, event_range: range } = finding;
  if (!sourceId || !range) {
    const explanation = {
      empty_window: "В сохранённом окне не было событий.",
      unavailable: "В снимке отсутствует источник или точный диапазон событий.",
      unrepresentable:
        "Верхняя граница диапазона выходит за пределы поддерживаемых дат.",
    };
    return <p>{explanation[finding.event_range_reason ?? "unavailable"]}</p>;
  }
  return (
    <>
      <p>
        Поиск покажет, что происходило в этом источнике в диапазоне находки по
        времени события (ECS @timestamp). В «Событиях» можно добавить фильтры.
      </p>
      <p>
        {exactLocalTime(range.timestamp_from)} —{" "}
        {exactLocalTime(range.timestamp_to)} (до, исключительно)
      </p>
      <Link
        className={styles.eventLink}
        to={findingEventsPath({
          sourceId,
          from: range.timestamp_from,
          to: range.timestamp_to,
        })}
      >
        Перейти к событиям
      </Link>
    </>
  );
}

function Details({ finding }: { finding: FindingCard }) {
  const expected =
    finding.expected_origin === "aws_rcf"
      ? "Ожидаемые значения — ориентир по оценке AWS RCF."
      : finding.expected_origin === "preceding_median"
        ? "Ожидаемые значения — ориентир по медиане предшествующих входов."
        : "Происхождение ожидаемых значений отсутствует в снимке.";
  const flow = [
    ["Источник", finding.source_name ?? "Нет данных"],
    ["Агрегат", finding.aggregate_name ?? "Нет данных"],
    ["Детектор", finding.detector_name || "Имя не сохранено"],
  ];
  const information = [
    ["Начало окна", exactLocalTime(finding.window_start)],
    ["Конец окна", exactLocalTime(finding.window_end)],
    ["Расчёт начат", exactLocalTime(finding.aggregation_started_at)],
    ["Расчёт завершён", exactLocalTime(finding.aggregation_completed_at)],
    ["Оценка начата", exactLocalTime(finding.evaluation_started_at)],
    ["Оценка завершена", exactLocalTime(finding.evaluation_completed_at)],
  ];
  return (
    <div className={styles.details}>
      <div className={styles.overview}>
        <div className={styles.metrics}>
          <section className={styles.tile}>
            <h3>Степень аномальности</h3>
            <p className={styles.description}>
              Насколько сильно окно отклонилось по шкале RCF от 0 до 1.
            </p>
            <strong>{finding.grade ?? "Нет данных"}</strong>
          </section>
          <section className={styles.tile}>
            <h3>Уверенность оценки</h3>
            <p className={styles.description}>
              Насколько данных было достаточно для оценки модели по шкале от 0
              до 1.
            </p>
            <strong>{finding.confidence ?? "Нет данных"}</strong>
          </section>
        </div>
        <FindingFlags finding={finding} />
        <section className={styles.tile}>
          <h3>Вклад признаков</h3>
          <p className={styles.description}>
            Доля каждого признака в сумме вкладов RCF для этой находки.
          </p>
          <Attribution features={finding.features} />
        </section>
      </div>
      <section className={styles.tile}>
        <h3>Значения признаков</h3>
        <p className={styles.description}>{expected}</p>
        <Table<FindingFeature>
          rowKey="key"
          className={tableStyles.table}
          tableLayout="auto"
          pagination={false}
          scroll={{ x: 470 }}
          dataSource={finding.features}
          locale={{ emptyText: "В снимке нет сохранённых признаков" }}
          columns={[
            {
              title: (
                <ColumnLabel
                  name="Признак"
                  help="Вход модели в порядке сохранённого вектора."
                />
              ),
              dataIndex: "name",
            },
            {
              title: (
                <ColumnLabel
                  name="Фактическое"
                  help="Точное значение признака, которое детектор получил из агрегата."
                />
              ),
              dataIndex: "actual",
              render: (value: string | null) => value ?? "Нет данных",
            },
            {
              title: (
                <ColumnLabel
                  name="Ожидаемое"
                  help="Сохранённое значение, с которым можно сопоставить результат окна."
                />
              ),
              dataIndex: "expected",
              render: (value: string | null) => value ?? "Нет данных",
            },
          ]}
        />
      </section>
      <section className={styles.tile}>
        <h3>Как получена находка · {displayZone}</h3>
        <p className={styles.description}>
          Источник поставляет события, агрегат рассчитывает признаки окна,
          детектор оценивает их на аномальность.
        </p>
        <ol
          className={styles.processFlow}
          aria-label="Поток данных"
          role="list"
        >
          {flow.map(([label, value], index) => (
            <li key={label}>
              <div className={styles.processTile}>
                <span>{label}</span>
                <strong>{value}</strong>
              </div>
              {index < flow.length - 1 && (
                <span className={styles.flowArrow} aria-hidden="true">
                  <ArrowRightOutlined />
                </span>
              )}
            </li>
          ))}
        </ol>
        <dl className={`${styles.processTile} ${styles.timeBasis}`}>
          <dt>Основание времени</dt>
          <dd>{timeBasisLabel[finding.time_basis]}</dd>
          <dd className={styles.description}>
            {timeBasisHelp[finding.time_basis]}
          </dd>
        </dl>
        <dl className={styles.information}>
          {information.map(([label, value]) => (
            <div key={label}>
              <dt>{label}</dt>
              <dd>{value}</dd>
            </div>
          ))}
        </dl>
      </section>
      <section className={styles.tile}>
        <h3>Исходные события · {displayZone}</h3>
        <RangeLink finding={finding} />
      </section>
    </div>
  );
}

export function FindingDetails({
  open,
  finding,
  loading,
  error,
  onRetry,
  onClose,
}: {
  open: boolean;
  finding?: FindingCard;
  loading: boolean;
  error: unknown;
  onRetry: () => void;
  onClose: () => void;
}) {
  return (
    <Modal
      open={open}
      title="Подробности находки"
      wrapClassName={styles.detailWrap}
      width="min(1100px, 100%)"
      style={{ top: 0, margin: 0, maxWidth: "100%", paddingBottom: 0 }}
      onCancel={onClose}
      footer={<Button onClick={onClose}>Закрыть</Button>}
    >
      {loading && <Spin aria-label="Загрузка находки" />}
      {!!error && <AppRequestError error={error} onRetry={onRetry} />}
      {finding && <Details finding={finding} />}
    </Modal>
  );
}
