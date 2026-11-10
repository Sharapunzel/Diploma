import { useEffect, useRef } from "react";
import { Alert, Button, Checkbox, Collapse, Input, Select } from "antd";
import { observer } from "mobx-react-lite";
import { Link, useNavigate } from "react-router-dom";
import { useStores } from "../../app/providers/StoresProvider";
import { useDraftGuard } from "../../features/aggregates/useDraftGuard";
import { ConfirmationDialog } from "../../shared/ui/ConfirmationDialog";
import { CommandFeedback, DetectorError, FieldHelp, FieldLabel } from "./Parts";
import styles from "./Detectors.module.css";
export const DetectorCreatePage = observer(() => {
  const { detectors, session } = useStores();
  const s = detectors.draft;
  const navigate = useNavigate();
  const previousReceipt = useRef(s.command.receipt);
  const catalogOpen = useRef(false);
  const guard = useDraftGuard(s);
  useEffect(() => {
    if (session.has("detectors.write")) {
      detectors.newDraft();
    }
  }, [detectors, session, s]);
  const canWrite = session.has("detectors.write");
  useEffect(() => {
    if (!canWrite || s.frozen) return;
    const timer = setTimeout(() => void s.catalog(), s.query ? 250 : 0);
    return () => {
      clearTimeout(timer);
      s.lifecycle.cancel("catalog-read");
      s.lifecycle.cancel("catalog");
    };
  }, [s, s.query, s.frozen, canWrite]);
  useEffect(() => {
    if (s.command.receipt && s.command.receipt !== previousReceipt.current)
      navigate(`/detectors/${s.command.receipt.detector_id}`, {
        replace: true,
      });
  }, [s.command.receipt, navigate]);
  if (!session.has("detectors.write"))
    return (
      <Alert type="warning" message="Нет разрешения на создание детектора" />
    );
  const field = (
    key: "name" | "step" | "trees" | "sample" | "output" | "seed",
    label: string,
    hint?: string,
  ) => (
    <label className={styles.field}>
      <FieldLabel
        label={label}
        help={
          hint ??
          "Название от 1 до 200 символов после удаления пробелов по краям. Помогает различать независимые модели одного агрегата."
        }
      />
      <Input
        aria-label={label}
        value={s[key]}
        disabled={s.frozen}
        aria-invalid={!!s.errors[key]}
        aria-describedby={s.errors[key] ? `error-${key}` : undefined}
        onChange={(e) => s.change(key, e.target.value)}
      />
      {hint && <span className={styles.hint}>{hint}</span>}
      {s.errors[key] && (
        <span id={`error-${key}`} role="alert" className={styles.error}>
          {s.errors[key]}
        </span>
      )}
    </label>
  );
  return (
    <div className={styles.page}>
      <Link to="/detectors">К списку детекторов</Link>
      <h2>Создание детектора</h2>
      <CommandFeedback store={s.command} />
      <form
        className={`${styles.form} ${styles.panel}`}
        onSubmit={(e) => {
          e.preventDefault();
          void s.submit();
        }}
      >
        <label className={styles.field}>
          <FieldLabel
            label="Активный агрегат"
            help="Сохранённое правило собственного источника задаёт длительность окна и порядок признаков. Показаны первые 10 активных агрегатов; введите часть имени для поиска по всему каталогу."
          />
          <Select
            aria-label="Активный агрегат"
            virtual={false}
            classNames={{ popup: { root: styles.catalogPopup } }}
            showSearch
            filterOption={false}
            searchValue={s.query}
            onOpenChange={(open) => {
              catalogOpen.current = open;
            }}
            onSearch={(value) => {
              // Select emits an empty search when its popup closes. Only a
              // user's clear while open should replace the requested query.
              if (value || catalogOpen.current) s.search(value);
            }}
            loading={
              !s.frozen &&
              (s.lifecycle.isPending("catalog") || s.catalogQuery !== s.query)
            }
            disabled={s.frozen}
            value={s.aggregateId || undefined}
            placeholder="Выберите сохранённый агрегат"
            options={s.aggregates
              .filter((a) => a.state === "active")
              .map((a) => ({
                value: a.id,
                title: a.name,
                label: `${a.name} — ${a.source_name}`,
                sourceName: a.source_name,
              }))}
            labelRender={(option) =>
              s.selected?.id === option.value ? s.selected.name : option.label
            }
            optionRender={(option) => (
              <div className={styles.cell}>
                <span>{option.data.title}</span>
                <small>Источник: {option.data.sourceName}</small>
              </div>
            )}
            onChange={(v) => {
              s.change("aggregateId", v);
              s.search("");
            }}
            notFoundContent={
              s.lifecycle.isPending("catalog") || s.catalogQuery !== s.query
                ? "Поиск…"
                : s.catalogError
                  ? "Поиск недоступен. Повторите запрос."
                  : "Активных агрегатов не найдено"
            }
            aria-invalid={!!s.errors.aggregateId}
          />
          {s.errors.aggregateId && (
            <span role="alert" className={styles.error}>
              {s.errors.aggregateId}
            </span>
          )}
        </label>
        {s.catalogError !== null && (
          <DetectorError
            error={s.catalogError}
            onRetry={() => void s.catalog()}
          />
        )}
        {s.total > 10 && (
          <p className={styles.hint}>
            Найдено {s.total}. Показаны первые 10 — уточните имя в поле
            агрегата.
          </p>
        )}
        <p>
          Поиск охватывает весь каталог; архивные и удалённые агрегаты
          недоступны для выбора. Если активных нет,{" "}
          <Link to="/aggregates">создайте или возобновите агрегат</Link>.
        </p>
        {s.selected && (
          <section className={styles.panel} aria-label="Выбранный агрегат">
            <p>Агрегат: {s.selected.name}</p>
            <p>Источник: {s.selected.source_name}</p>
            <p>Длительность: {s.selected.duration_seconds} с</p>
            <p>Закреплённые признаки:</p>
            <ol>
              {Array.isArray(s.selected.aggregate_rule.features) &&
                s.selected.aggregate_rule.features.map((f, i) => (
                  <li key={i}>
                    {typeof f === "object" && f !== null && "name" in f
                      ? String(f.name)
                      : `Признак ${i + 1}`}
                  </li>
                ))}
            </ol>
          </section>
        )}
        {field("name", "Имя детектора")}
        <div className={styles.inline}>
          <Checkbox
            checked={s.accelerated}
            disabled={s.frozen}
            onChange={(e) => s.mode(e.target.checked)}
          >
            Ускоренный прогрев
          </Checkbox>
          <FieldHelp
            label="Ускоренный прогрев"
            text="По умолчанию выключен. Исторические окна могут перекрываться, но рабочие окна после прогрева остаются последовательными. Фактический охват и успешное использование отмечаются отдельно."
          />
        </div>
        {s.accelerated &&
          field(
            "step",
            "Шаг прогрева, секунды",
            "Целое число от 1 секунды до длительности окна; начальное значение 1 с. При меньшем шаге исторические окна перекрываются, но не становятся независимыми периодами.",
          )}
        <p>
          Историю выбирает система. Обучающая история не создаёт находок.
          Перекрывающиеся точки не равны независимым периодам; фактический охват
          появится в карточке. Готовность модели не гарантирует точность
          обнаружения.
        </p>
        <Collapse
          items={[
            {
              key: "rcf",
              label: "Дополнительные настройки RCF",
              children: (
                <div className={styles.formFields}>
                  {field(
                    "trees",
                    "Число деревьев",
                    "Положительное целое; по умолчанию 100 деревьев. Большее число повышает стоимость памяти и расчёта; ресурсную пригодность проверяет сервер.",
                  )}
                  {field(
                    "sample",
                    "Размер выборки",
                    "Целое не менее 2; по умолчанию 256 входов в выборке дерева. Влияет на память модели и необходимое число исторических окон.",
                  )}
                  {field(
                    "output",
                    "Порог начала оценки",
                    "Не меньше размера выборки; по умолчанию 256.",
                  )}
                  {field(
                    "seed",
                    "Начальное значение генератора",
                    "Точное целое int64; по умолчанию 42. Ресурсную пригодность проверяет сервер.",
                  )}
                </div>
              ),
            },
          ]}
        />
        <p>Создание сохраняет конфигурацию без запуска модели.</p>
        <Button
          type="primary"
          htmlType="submit"
          loading={s.command.busy}
          disabled={s.frozen}
        >
          Создать без запуска
        </Button>
      </form>
      <ConfirmationDialog
        open={!!guard.leaving}
        title="Покинуть форму?"
        message="Несекретный черновик сохранится в текущей сессии. Незавершённая команда останется доступна для явного разрешения."
        confirmText="Перейти"
        onCancel={guard.cancelLeave}
        onConfirm={guard.confirmLeave}
      />
    </div>
  );
});
