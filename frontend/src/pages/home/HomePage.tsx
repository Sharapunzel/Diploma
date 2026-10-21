import { useEffect, useMemo } from "react";
import { Button, Spin } from "antd";
import { observer } from "mobx-react-lite";
import { Link } from "react-router-dom";
import { useStores } from "../../app/providers/StoresProvider";
import { AppRequestError } from "../../app/ui/AppRequestError";
import { HomeStore } from "../../features/home/HomeStore";
import styles from "./HomePage.module.css";

const processing = [
  ["Агенты", "/agents", "agents.read", "Учётные записи и назначения топиков."],
  [
    "Подключения",
    "/connections",
    "connections.read",
    "Kafka и внешние индексеры.",
  ],
  ["Источники", "/sources", "sources.read", "Наборы данных и их состояния."],
  [
    "Нормализаторы",
    "/normalizers",
    "normalizers.read",
    "Правила приведения собственных событий к ECS.",
  ],
  [
    "Диагностика",
    "/diagnostics",
    "events.read",
    "Снимок и история собственного конвейера.",
  ],
] as const;

function Count({
  label,
  value,
  tone,
}: {
  label: string;
  value: number;
  tone?: "enabled" | "disabled" | "archived";
}) {
  const labelClass =
    tone === "enabled"
      ? styles.enabledLabel
      : tone === "disabled"
        ? styles.disabledLabel
        : tone === "archived"
          ? styles.archivedLabel
          : undefined;
  return (
    <div className={styles.count}>
      <strong>{value}</strong>
      <span className={labelClass}>{label}</span>
    </div>
  );
}

export const HomePage = observer(() => {
  const { api, session } = useStores();
  const accessKey = `${session.version}:${[...(session.data?.permissions ?? [])].sort().join(",")}`;
  const home = useMemo(() => new HomeStore(api, accessKey), [api, accessKey]);
  const { overview, loading, error } = home;
  const refresh = () => void home.load();

  useEffect(() => {
    void home.load();
    return () => home.cancel();
  }, [home]);

  const permitted = processing.filter(([, , permission]) =>
    session.has(permission),
  );
  const own = overview?.sources;
  const ownTotal = own
    ? own.own_enabled + own.own_disabled + own.own_archived
    : 0;
  const sourceSegments =
    own && ownTotal > 0
      ? (
          [
            {
              key: "enabled",
              value: own.own_enabled,
              className: styles.enabled,
            },
            {
              key: "disabled",
              value: own.own_disabled,
              className: styles.disabled,
            },
            {
              key: "archived",
              value: own.own_archived,
              className: styles.archived,
            },
          ] as const
        ).filter((segment) => segment.value > 0)
      : [];

  return (
    <div className={styles.page}>
      <header className={styles.intro}>
        <p className={styles.eyebrow}>ОБЗОР ПРИЛОЖЕНИЯ</p>
        <h2>От источника до анализа событий</h2>
        <p>
          Собственные события проходят путь агент → Kafka → нормализация →
          PostgreSQL. Внешние ECS-источники читаются напрямую из индексера.
        </p>
      </header>
      <div className={styles.modules}>
        <section className={styles.module} aria-labelledby="processing-title">
          <h3 id="processing-title">Обработка данных</h3>
          {permitted.length ? (
            <ul className={styles.links}>
              {permitted.map(([label, path, , description]) => (
                <li key={path}>
                  <Link to={path}>
                    {label} <span aria-hidden="true">↗</span>
                  </Link>
                  <span>{description}</span>
                </li>
              ))}
            </ul>
          ) : (
            <p className={styles.muted}>
              Разделы обработки данных недоступны с вашими правами.
            </p>
          )}
        </section>
        <section className={styles.module} aria-labelledby="analysis-title">
          <h3 id="analysis-title">Анализ данных</h3>
          {session.has("events.read") ? (
            <div className={styles.analysisLink}>
              <Link to="/events">
                События <span aria-hidden="true">↗</span>
              </Link>
              <p>
                Поиск и фильтрация событий выбранного собственного или внешнего
                источника.
              </p>
            </div>
          ) : (
            <p className={styles.muted}>
              Просмотр событий недоступен с вашими правами.
            </p>
          )}
        </section>
      </div>
      <section className={styles.inventory} aria-labelledby="inventory-title">
        <div className={styles.inventoryHeader}>
          <div>
            <h3 id="inventory-title">Текущий инвентарь</h3>
            <p className={styles.muted}>
              Сохранённая конфигурация, не показатель доставки или доступности.
            </p>
          </div>
          <Button onClick={refresh} loading={loading}>
            Обновить показатели
          </Button>
        </div>
        {loading && !overview && <Spin aria-label="Загрузка показателей" />}
        {error !== null && <AppRequestError error={error} onRetry={refresh} />}
        {overview && (
          <>
            <p className={styles.timestamp}>
              Снимок:{" "}
              <time dateTime={overview.captured_at}>
                {new Date(overview.captured_at).toLocaleString("ru-RU")}
              </time>
              {error !== null && " · данные могут устареть"}
            </p>
            <div className={styles.cards}>
              {overview.connections && (
                <article className={styles.card}>
                  <h4>Подключения</h4>
                  <div className={styles.counts}>
                    <Count
                      label="собственных"
                      value={overview.connections.own}
                    />
                    <Count
                      label="внешних"
                      value={overview.connections.external}
                    />
                  </div>
                </article>
              )}
              {own && (
                <article className={styles.card}>
                  <h4>Собственные источники</h4>
                  {ownTotal > 0 ? (
                    <svg
                      className={styles.bar}
                      role="img"
                      aria-label={`Собственные источники: включённые ${own.own_enabled}, выключенные ${own.own_disabled}, архивные ${own.own_archived}`}
                      viewBox="0 0 100 10"
                      preserveAspectRatio="none"
                    >
                      {sourceSegments.map((segment, index) => (
                        <rect
                          key={segment.key}
                          x={sourceSegments
                            .slice(0, index)
                            .reduce(
                              (offset, previous) =>
                                offset + (previous.value / ownTotal) * 100,
                              0,
                            )}
                          y="0"
                          width={(segment.value / ownTotal) * 100}
                          height="10"
                          className={segment.className}
                        />
                      ))}
                    </svg>
                  ) : (
                    <p className={styles.muted}>Источников пока нет.</p>
                  )}
                  <div className={styles.counts}>
                    <Count
                      label="включённых"
                      value={own.own_enabled}
                      tone="enabled"
                    />
                    <Count
                      label="выключенных"
                      value={own.own_disabled}
                      tone="disabled"
                    />
                    <Count
                      label="архивных"
                      value={own.own_archived}
                      tone="archived"
                    />
                  </div>
                </article>
              )}
              {own && (
                <article className={styles.card}>
                  <h4>Внешние источники</h4>
                  <div className={styles.counts}>
                    <Count label="включённых" value={own.external_enabled} />
                    <Count label="выключенных" value={own.external_disabled} />
                  </div>
                </article>
              )}
              {overview.agents && (
                <article className={styles.card}>
                  <h4>Доступ агентов</h4>
                  <div className={styles.counts}>
                    <Count
                      label="готовых учётных записей"
                      value={overview.agents.ready}
                    />
                    <Count
                      label="обеспечение"
                      value={overview.agents.provisioning}
                    />
                    <Count label="ошибка" value={overview.agents.error} />
                    <Count
                      label="отзыв ожидается"
                      value={overview.agents.revocation_pending}
                    />
                    <Count label="отозванных" value={overview.agents.revoked} />
                  </div>
                  <p className={styles.muted}>
                    Готовая учётная запись не подтверждает работу Fluent Bit.
                  </p>
                </article>
              )}
              {overview.normalizers != null && (
                <article className={styles.card}>
                  <h4>Нормализаторы</h4>
                  <div className={styles.counts}>
                    <Count label="правил" value={overview.normalizers} />
                  </div>
                </article>
              )}
            </div>
            {!overview.connections &&
              !own &&
              !overview.agents &&
              overview.normalizers == null && (
                <p className={styles.muted}>
                  Инвентарные показатели недоступны с вашими правами.
                </p>
              )}
          </>
        )}
      </section>
    </div>
  );
});
