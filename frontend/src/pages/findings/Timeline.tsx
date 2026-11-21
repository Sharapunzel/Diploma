import type { CSSProperties } from "react";
import { Empty, Tooltip } from "antd";
import type {
  FindingDetector,
  FindingRow,
  TimeBasis,
} from "../../app/api/findings";
import { timeBasisHelp, timeBasisLabel } from "../../features/findings/labels";
import { laneHeight, layoutWindows } from "../../features/findings/windows";
import { displayZone, localTime } from "../../shared/time/utc";
import { ColumnLabel } from "./ColumnLabel";
import styles from "./Findings.module.css";

const colors = [
  "#2f80d0",
  "#8b62d1",
  "#d48f27",
  "#299578",
  "#c85170",
  "#6477a1",
];
const timeBasisOffset: Record<TimeBasis, number> = {
  event_time: 0,
  availability_at: 1,
  backend_processed_at_historical: 2,
};
const patterns = [
  "none",
  "repeating-linear-gradient(45deg, transparent 0 3px, #0007 3px 5px)",
  "repeating-linear-gradient(90deg, transparent 0 4px, #0007 4px 6px)",
  "repeating-linear-gradient(0deg, transparent 0 3px, #0007 3px 5px)",
  "repeating-linear-gradient(-45deg, transparent 0 4px, #0007 4px 6px)",
  "repeating-radial-gradient(circle at center, transparent 0 2px, #0007 2px 3px)",
];

function WindowTooltip({ finding }: { finding: FindingRow }) {
  return (
    <dl className={styles.windowTooltipDetails}>
      <div>
        <dt>Начало окна</dt>
        <dd>{localTime(finding.window_start)}</dd>
      </div>
      <div>
        <dt>Конец окна</dt>
        <dd>{localTime(finding.window_end)}</dd>
      </div>
      <div>
        <dt>Степень аномальности</dt>
        <dd>{finding.grade ?? "Нет данных"}</dd>
      </div>
      <div>
        <dt>Уверенность оценки</dt>
        <dd>{finding.confidence ?? "Нет данных"}</dd>
      </div>
    </dl>
  );
}

export function Timeline({
  items,
  detectors,
  highlighted,
  onSelect,
}: {
  items: FindingRow[];
  detectors: FindingDetector[];
  highlighted: string | null;
  onSelect: (id: string) => void;
}) {
  const layout = layoutWindows(items);
  if (!layout)
    return (
      <Empty
        image={Empty.PRESENTED_IMAGE_SIMPLE}
        description={
          items.length
            ? "Границы окон не сохранены"
            : "За этот период находок нет"
        }
      />
    );

  const catalog = new Map(detectors.map((item) => [item.detector_id, item]));
  const present = new Map<string, Set<TimeBasis>>();
  for (const { finding } of layout.marks) {
    const id = finding.detector_id;
    if (!catalog.has(id)) catalog.set(id, finding);
    if (!present.has(id)) present.set(id, new Set());
    present.get(id)!.add(finding.time_basis);
  }
  const indexes = new Map(
    [...catalog.keys()].sort().map((id, index) => [id, index]),
  );
  const nameOf = (id: string) =>
    catalog.get(id)?.detector_name || "Имя не сохранено";

  function swatch(detectorId: string, basis: TimeBasis): CSSProperties {
    const index = indexes.get(detectorId)!;
    return {
      backgroundColor: colors[index % colors.length],
      backgroundImage:
        patterns[
          (index + Math.floor(index / colors.length) + timeBasisOffset[basis]) %
            patterns.length
        ],
    };
  }

  const span = layout.end - layout.start;
  const divisions = Math.min(4, Math.max(1, Math.floor(span / 1000)));
  const ticks = Array.from({ length: divisions + 1 }, (_, index) => ({
    position: (index / divisions) * 100,
    label: localTime(
      new Date(layout.start + (span * index) / divisions).toISOString(),
    ),
  }));
  return (
    <>
      <div>Окна находок · {displayZone}</div>
      <div className={styles.timelineScroll}>
        <div style={{ minWidth: layout.minWidth }}>
          <div className={styles.axis} aria-hidden="true">
            {ticks.map(({ position, label }) => (
              <span key={position} style={{ left: `${position}%` }}>
                {label}
              </span>
            ))}
          </div>
          <div
            className={styles.windowPlot}
            style={{ height: Math.max(3, layout.lanes) * laneHeight }}
          >
            {ticks.map(({ position }) => (
              <span
                key={position}
                className={styles.gridLine}
                aria-hidden="true"
                style={{ left: `${position}%` }}
              />
            ))}
            {layout.marks.map(({ finding, lane, left, width }) => (
              <Tooltip
                key={finding.id}
                title={<WindowTooltip finding={finding} />}
                trigger={["hover", "focus"]}
                arrow={false}
                classNames={{ root: styles.windowTooltip }}
              >
                <button
                  id={`finding-window-${finding.id}`}
                  type="button"
                  className={styles.windowMark}
                  data-highlighted={highlighted === finding.id || undefined}
                  aria-label={`Находка: ${nameOf(finding.detector_id)} · ${localTime(finding.window_start)} — ${localTime(finding.window_end)} · К строке таблицы`}
                  style={{
                    left: `${left}%`,
                    width: `${width}%`,
                    top: lane * laneHeight + 6,
                    ...swatch(finding.detector_id, finding.time_basis),
                  }}
                  onClick={() => onSelect(finding.id)}
                />
              </Tooltip>
            ))}
          </div>
        </div>
      </div>
      {layout.marks.length < items.length && (
        <p className={styles.description}>
          Находок без сохранённых границ окна:{" "}
          {items.length - layout.marks.length}.
        </p>
      )}
      <h2 className={styles.timelineLegendTitle}>Детекторы</h2>
      <ul
        className={styles.timelineLegend}
        aria-label="Детекторы и основание времени"
      >
        {[...present.keys()].sort().map((id) => (
          <li key={id}>
            <strong>{nameOf(id)}</strong>
            <ul className={styles.timelineLegendEntries}>
              {[...present.get(id)!]
                .sort((a, b) => timeBasisOffset[a] - timeBasisOffset[b])
                .map((basis) => (
                  <li key={basis}>
                    <span
                      className={styles.swatch}
                      style={swatch(id, basis)}
                      aria-hidden="true"
                    />
                    <ColumnLabel
                      name={timeBasisLabel[basis]}
                      help={timeBasisHelp[basis]}
                    />
                  </li>
                ))}
            </ul>
          </li>
        ))}
      </ul>
    </>
  );
}
