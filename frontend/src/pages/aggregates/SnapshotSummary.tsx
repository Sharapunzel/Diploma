import { exact, object, type JsonObject } from "../../app/api/aggregateJson";
import { reasonText } from "../../features/aggregates/labels";
import { displayTime } from "../../features/aggregates/time";

export function SnapshotSummary({
  metadata,
  zone,
}: {
  metadata: JsonObject;
  zone: string;
}) {
  const facts = object(metadata.facts) ? metadata.facts : null;
  const exclusions =
    facts && object(facts.exclusions) ? facts.exclusions : null;
  return (
    <div>
      <p>
        Подтверждение доставки:{" "}
        {metadata.delivery_coverage === "not_attested"
          ? "доставка всех журналов не подтверждена"
          : "сведения отсутствуют"}
        .
      </p>
      <p>
        Качество описывает полученный снимок. Complete означает отсутствие
        известных нарушений в нём; это не гарантия получения всех событий хоста.
        Unknown и interrupted не являются нормальным нулём для модели.
      </p>
      {exclusions && Object.keys(exclusions).length > 0 && (
        <ul aria-label="Исключения времени снимка">
          {Object.entries(exclusions).map(([reason, count]) => (
            <li key={reason}>
              {reasonText(reason)}: {exact(count)}
            </li>
          ))}
        </ul>
      )}
      {(typeof metadata.load_start === "string" ||
        typeof metadata.load_end === "string") && (
        <details>
          <summary>Технический период загрузки снимка</summary>
          {displayTime(
            typeof metadata.load_start === "string"
              ? metadata.load_start
              : null,
            zone,
          )}{" "}
          —{" "}
          {displayTime(
            typeof metadata.load_end === "string" ? metadata.load_end : null,
            zone,
          )}
        </details>
      )}
    </div>
  );
}
