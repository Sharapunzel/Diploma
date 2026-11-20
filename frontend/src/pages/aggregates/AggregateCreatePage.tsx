import { useEffect, useMemo, useState } from "react";
import { Button, Input, Radio, Spin } from "antd";
import { observer } from "mobx-react-lite";
import { Link, useNavigate, useSearchParams } from "react-router-dom";
import { DraftStore } from "../../features/aggregates/DraftStore";
import { useAggregateScope } from "../../features/aggregates/useAggregateScope";
import { useDraftGuard } from "../../features/aggregates/useDraftGuard";
import { browserZone, selectedWindow } from "../../features/aggregates/time";
import {
  timezoneError,
  validInstant,
} from "../../features/aggregates/processValidation";
import { ResourcesApi } from "../../app/api/resources";
import { useStores } from "../../app/providers/StoresProvider";
import { ConfirmationDialog } from "../../shared/ui/ConfirmationDialog";
import { AggregateError } from "./AggregateError";
import { CalendarTime } from "./CalendarTime";
import { SourcePicker } from "./SourcePicker";
import { RuleExchange } from "./RuleExchange";
import { RuleEditor } from "./RuleEditor";
import { ResultView, Time } from "./ResultView";
import { FieldLabel } from "./AggregateHelp";
import { BudgetView } from "./BudgetView";
import { AggregateNotice } from "./AggregateNotice";
import styles from "./Aggregates.module.css";
export const AggregateCreatePage = observer(() => {
  const [params] = useSearchParams(),
    from = params.get("copy") || "";
  const navigate = useNavigate(),
    { api } = useStores();
  const { store, service, session } = useAggregateScope(
    (service) => new DraftStore(service, from),
    from,
  );
  const resources = useMemo(() => new ResourcesApi(api), [api]);
  const { leaving, cancelLeave, confirmLeave } = useDraftGuard(store);
  const [zone, setZone] = useState(browserZone),
    [catalogError, setCatalogError] = useState<unknown>(null),
    [retry, setRetry] = useState(0);
  useEffect(() => {
    const c = new AbortController();
    setCatalogError(null);
    void service
      .operators(c.signal, true)
      .then((ops) => {
        if (!c.signal.aborted) store.setOperators(ops);
      })
      .catch((e) => {
        if (!c.signal.aborted) setCatalogError(e);
      });
    if (from) void store.loadCopy();
    return () => c.abort();
  }, [store, service, from, retry]);
  const creating = store.lifecycle.isPending("create");
  const zoneError = timezoneError(zone);
  const processReady = store.processValid && !zoneError;
  const processHint = [
    ...new Set([
      ...Object.values(store.processErrors),
      ...(zoneError ? [zoneError] : []),
    ]),
  ].join(" ");
  const budget = store.preview?.budget ?? store.validation?.budget;
  let windowInfo = null;
  let windowError = "";
  if (store.window && store.boundary) {
    try {
      windowInfo = selectedWindow(
        store.window,
        store.validation?.anchor_utc ?? store.boundary,
        Number(store.duration),
      );
    } catch (error) {
      windowError = error instanceof Error ? error.message : "Проверьте окно.";
    }
  }
  const create = () =>
    void store.create().then((aggregate) => {
      if (aggregate)
        navigate(`/aggregates/${aggregate.id}`, {
          state: { aggregateCreated: aggregate.id },
        });
    });
  return (
    <div
      className={styles.page}
      key={`${session.version}|${session.data?.permissions.join("|")}|${from}`}
    >
      <Link to="/aggregates">← К агрегатам</Link>

      {catalogError !== null && (
        <AggregateError
          error={catalogError}
          onRetry={() => setRetry((n) => n + 1)}
        />
      )}
      {!store.operators.length && catalogError === null && (
        <Spin aria-label="Загрузка каталога операций" />
      )}
      {!store.editable ? (
        <section
          aria-label="Загрузка копии"
          aria-busy={store.copyState === "loading"}
        >
          {store.copyState === "loading" ? (
            <Spin aria-label="Загрузка исходного агрегата" />
          ) : (
            <AggregateError
              error={store.copyError}
              onRetry={() => void store.loadCopy()}
            />
          )}
        </section>
      ) : (
        <>
          <fieldset
            disabled={creating}
            className={styles.block}
            aria-label="Параметры процесса"
          >
            <h3 aria-label="1. Параметры процесса">1. Параметры процесса </h3>
            <p className={styles.notice}>
              {from &&
                "Копия правила: будет создан новый агрегат с отдельным UUID. "}
              Имя, источник и расписание будущего агрегата. После создания
              настройки неизменяемы; изменения выполняются через копию. Поля со
              знаком * обязательны. Заполните их, чтобы перейти к признакам и
              проверке.
            </p>
            <div className={`${styles.grid} ${styles.processGrid}`}>
              <div className={styles.field}>
                <FieldLabel
                  required
                  help="Название нового неизменяемого агрегата, до 200 символов."
                >
                  Имя агрегата
                </FieldLabel>
                <Input
                  aria-label="Имя агрегата"
                  aria-required
                  aria-invalid={!!store.name && !!store.processErrors.name}
                  status={
                    store.name && store.processErrors.name ? "error" : undefined
                  }
                  maxLength={200}
                  value={store.name}
                  onChange={(e) => store.setField("name", e.target.value)}
                />
              </div>
              <SourcePicker
                api={resources}
                value={store.source}
                disabled={creating}
                required
                onChange={(v) => store.setField("source", v)}
              />
              <div className={styles.field}>
                <FieldLabel
                  required
                  help="Целое число секунд: длина окна и период появления новых точек."
                >
                  Длительность, секунды (1–604800)
                </FieldLabel>
                <Input
                  aria-label="Длительность, секунды"
                  aria-required
                  aria-invalid={!!store.processErrors.duration}
                  status={store.processErrors.duration ? "error" : undefined}
                  inputMode="numeric"
                  value={store.duration}
                  onChange={(e) => store.setField("duration", e.target.value)}
                />
              </div>
              <div className={styles.field}>
                <FieldLabel
                  required
                  help="UTC или IANA, например Asia/Yekaterinburg. Смена пояса меняет представление существующих моментов, но не расписание."
                >
                  Пояс отображения / ввода новой даты
                </FieldLabel>
                <Input
                  aria-label="Пояс отображения"
                  aria-required
                  aria-invalid={!!zoneError}
                  status={zoneError ? "error" : undefined}
                  value={zone}
                  onChange={(e) => setZone(e.target.value)}
                />
              </div>
              <CalendarTime
                label="Начало истории и сетка окон"
                zoned
                required
                value={store.boundary}
                zone={zone}
                disabled={creating}
                onChange={(v) => {
                  store.setField("boundary", v);
                  store.setField("timezone", zone);
                }}
              />
            </div>
            <p className={styles.notice}>
              Исходный пояс расписания: {store.timezone}.
            </p>
            <p className={styles.notice}>
              События относятся к окну по исходному ECS @timestamp. Дата
              загрузки не переносит их на другую дату. Граница задаёт начало
              автоматической истории. Текущее окно закрывается после конца плюс
              min(длительность, 300 секунд); завершённая старая история
              рассчитывается сразу.
            </p>
          </fieldset>
          {!processReady && (
            <p className={styles.notice} role="status">
              Признаки и диагностика станут доступны после заполнения параметров
              процесса. {processHint}
            </p>
          )}
          {store.unknownCreate && (
            <div
              className={styles.block}
              role="group"
              aria-label="Неподтверждённое создание"
            >
              <AggregateNotice
                type="warning"
                message="Результат создания неизвестен. Явный повтор отправит исходные тело и ключ даже после редактирования. Изменённый черновик не считается сохранённым этим повтором."
              />
              {store.error !== null && <AggregateError error={store.error} />}
              <div className={styles.toolbar}>
                <Button
                  type="primary"
                  aria-label="Повторить создание с тем же ключом"
                  loading={creating}
                  disabled={
                    !store.canCreate || !session.has("aggregates.write")
                  }
                  onClick={create}
                >
                  Повторить создание с тем же ключом
                </Button>
              </div>
            </div>
          )}
          <fieldset
            disabled={creating || !processReady}
            inert={creating || !processReady}
            aria-disabled={creating || !processReady}
            className={`${styles.block} ${!processReady ? styles.disabledBlock : ""}`}
            aria-label="Выходной вектор и полное правило"
          >
            <h3 aria-label="2. Выходной вектор и полное правило">
              2. Выходной вектор и полное правило{" "}
            </h3>
            <p className={styles.notice}>
              Включённые признаки образуют выходной вектор. Вспомогательные
              признаки остаются доступны как зависимости формул.
            </p>
            <Radio.Group
              aria-label="Режим правила"
              value={store.mode}
              disabled={creating || !processReady}
              onChange={(e) => store.switchMode(e.target.value)}
              options={[
                { value: "visual", label: "Визуальный" },
                { value: "json", label: "JSON" },
              ]}
            />
            <RuleExchange
              key={store.source}
              api={service}
              source={store.source}
              operators={store.operators}
              text={store.text}
              disabled={creating || !processReady}
              onImport={store.importText}
            />
            {store.mode === "json" ? (
              <div className={styles.field}>
                <FieldLabel help="Полное правило: выходные и вспомогательные признаки, условия отбора. Имя, источник и расписание задаются отдельно.">
                  Полное правило JSON
                </FieldLabel>
                <Input.TextArea
                  aria-label="Полное правило JSON"
                  rows={18}
                  value={store.text}
                  onChange={(e) => store.setText(e.target.value)}
                />
              </div>
            ) : (
              <RuleEditor
                rule={store.rule}
                operators={store.operators}
                api={service}
                source={store.source}
                onChange={store.setRule}
                onInvalid={store.setInvalid}
              />
            )}
          </fieldset>
          <fieldset
            className={`${styles.block} ${!processReady ? styles.disabledBlock : ""}`}
            disabled={creating || !processReady}
            inert={creating || !processReady}
            aria-disabled={creating || !processReady}
            aria-label="Проверка и диагностика"
          >
            <h3 aria-label="3. Проверка и диагностика">
              3. Проверка и диагностика{" "}
            </h3>
            <p>
              Исходная граница и длительность из параметров процесса задают
              сетку последовательных окон. Выберите начало одного уже
              завершённого окна той же длительности: сначала проверьте правило,
              затем выполните пробный расчёт. Он не создаёт агрегат и не
              сохраняет точку истории. Корректное правило само по себе не
              гарантирует полноту данных и пригодность значений для модели.
            </p>
            <div className={styles.grid}>
              <CalendarTime
                label="Начало окна для расчёта"
                value={store.window}
                scheduleAnchor={store.validation?.anchor_utc ?? store.boundary}
                zone={zone}
                disabled={creating || !processReady}
                onChange={(v) => store.setField("window", v)}
              />
            </div>
            {windowInfo && (
              <p aria-label="Выбранное окно предварительного расчёта">
                [<Time value={windowInfo.start} zone={zone} />,{" "}
                <Time value={windowInfo.end} zone={zone} />
                ).{" "}
                {windowInfo.inProcess
                  ? "Окно процесса."
                  : "Диагностика вне процесса: до начала автоматической истории."}{" "}
                {!windowInfo.completed && "Окно ещё не завершено."}
              </p>
            )}
            {windowError && <p role="alert">{windowError}</p>}
            {store.localError && (
              <AggregateNotice
                type="error"
                role="alert"
                message={store.localError}
              />
            )}
            {store.errors.map((e) => (
              <AggregateNotice type="error" role="alert" key={e} message={e} />
            ))}
            {!store.unknownCreate && store.error !== null && (
              <AggregateError error={store.error} />
            )}
            {store.success && (
              <AggregateNotice
                type="success"
                role="status"
                message={store.success}
              />
            )}
            <div className={styles.toolbar}>
              <Button
                aria-label="Проверить правило"
                loading={store.lifecycle.isPending("validate")}
                disabled={
                  store.busy ||
                  !processReady ||
                  !store.operators.length ||
                  !!store.localError ||
                  !!store.invalidNodes.size ||
                  !!store.errors.length
                }
                onClick={() => void store.validate()}
              >
                Проверить правило
              </Button>
              <Button
                aria-label="Предварительный расчёт"
                loading={store.lifecycle.isPending("preview")}
                disabled={
                  !processReady ||
                  !store.validation ||
                  !validInstant(store.window) ||
                  !!windowError ||
                  !windowInfo?.completed ||
                  store.busy
                }
                onClick={() => void store.runPreview()}
              >
                Предварительный расчёт
              </Button>
              {!store.unknownCreate && (
                <Button
                  type="primary"
                  aria-label="Создать агрегат"
                  loading={creating}
                  disabled={
                    !processReady ||
                    !store.canCreate ||
                    store.busy ||
                    !session.has("aggregates.write")
                  }
                  onClick={create}
                >
                  Создать агрегат
                </Button>
              )}
            </div>
            {store.validation && (
              <>
                <p>
                  Проверенная исходная граница расписания:{" "}
                  <Time value={store.validation.anchor_utc} zone={zone} />;
                  длительность {store.validation.duration_seconds} с.
                </p>
              </>
            )}
            {store.preview && (
              <ResultView
                result={store.preview}
                zone={zone}
                showBudget={false}
              />
            )}
            <BudgetView
              budget={budget}
              duration={store.duration}
              execution={store.lastExecution}
            />
          </fieldset>
        </>
      )}
      <ConfirmationDialog
        open={!!leaving}
        title="Покинуть черновик?"
        message="Несохранённое правило и параметры будут потеряны. Уход не отменяет серверный запрос; при неизвестном исходе создание могло состояться."
        confirmText="Покинуть"
        onCancel={cancelLeave}
        onConfirm={confirmLeave}
      />
    </div>
  );
});
