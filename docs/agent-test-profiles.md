# Проверочные профили агентов

Эта инструкция описывает команды единого изолированного контура. Распределение проверок между Исполнителем, Этапным архитектором и Главным архитектором, статусы ревью/приёмки и условия коммита задаёт обязательная [политика тестирования](../harness/testing-policy.md).

## Состав полного прогона

Ниже перечислен полный регрессионный контур для приёмки этапа/релиза. Для ревью и коммита задачи это каталог профилей: выбираются только затронутые крупные компоненты и необходимые связи по политике тестирования. Неизменённый frontend не включается из-за серверной задачи или участия его сервиса в стенде.

| Область | Команда из корня проекта или способ проверки |
| --- | --- |
| Frontend: типы, lint, форматирование, весь Vitest и сборка | `python -B scripts/agent_test_profiles.py frontend` |
| Весь обычный backend, PostgreSQL и реальная Kafka | `python -B scripts/agent_test_profiles.py kafka-integration` без `--pytest` |
| Целевой Linux/JVM RCF, сервис, транзакции, миграции и числовые контракты; Alembic upgrade/check | `python -B scripts/agent_test_profiles.py backend-db --rcf` без `--pytest` |
| Постоянные тесты подготовки стенда и очистки | `python -B -m pytest scripts/test_prepare_dev_kafka.py scripts/test_agent_test_profiles_cleanup.py` с временными путями в каталоге прогона |
| Общий pgoutput gate: протокол, replay и потеря слота | `python -B scripts/check_pgoutput_gate.py` |
| Собственный сквозной путь через UI | `python -B scripts/agent_test_profiles.py end-to-end` |
| Серверные event-time агрегаты, RCF, пауза и очистка | `smoke-development --aggregate-only`, `smoke-production --aggregate-only`; также `end-to-end --aggregate-only` |
| Постоянный detector-worker, обе поставки, 5-секундная цепочка и сохранение состояния | Включено в полный собственный E2E и оба smoke; адресно `smoke-development --detector-only` / `smoke-production --detector-only` |
| Внешний индексер, API и UI | `python -B scripts/agent_test_profiles.py external-end-to-end` |
| Постоянные браузерные сценарии | Playwright на подготовленном изолированном development/production-контуре; ограничения текущих команд описаны ниже |
| Запуск, повторный запуск, HTTP(S), сессия, OIDC и TLS | `python -B scripts/agent_test_profiles.py smoke-development` и `python -B scripts/agent_test_profiles.py smoke-production` |

Не повторяйте обычный полный `backend-db` после полного `kafka-integration`: его тесты уже выполнены. Проверки Alembic сохраняются в `backend-db --rcf`. Отдельный `backend-db --wal` не требуется, если прошли оба smoke: они уже выполняют тот же WAL bootstrap-сценарий. Он не заменяет общий `check_pgoutput_gate.py`. Исторический исследовательский `check_commit_timestamp_retention.py` не является обязательной ежедневной регрессией.

Профиль `frontend` не запускает Playwright. Стандартный браузерный набор выбирается веткой существующих smoke-профилей:

```powershell
python -B scripts/agent_test_profiles.py smoke-development --browser-only
python -B scripts/agent_test_profiles.py smoke-development --browser-only --browser-spec detectors.ui.spec.ts detectors.live.spec.ts
python -B scripts/agent_test_profiles.py smoke-production --browser-only
python -B scripts/agent_test_profiles.py smoke-production --browser-only --browser-spec detectors.live.spec.ts
```

Development содержит **19 specs**: полный `auth.spec.ts`, `events.spec.ts`, `resources.spec.ts`, `resources.confirm.spec.ts`, `resources.policy.spec.ts`, `resources.targets.spec.ts`, `normalizers.spec.ts`, `normalizers.ai.spec.ts`, `normalizers.blocks.spec.ts`, `normalizers.feedback.spec.ts`, `normalizers.fix.spec.ts`, `normalizers.source_debug.spec.ts`, `normalizers.tree.spec.ts`, `aggregates.live.spec.ts`, `aggregates.ui.spec.ts`, `detectors.ui.spec.ts`, `detectors.live.spec.ts`, `findings.ui.spec.ts` и `findings.live.spec.ts`. Семейства `resources.*` и `normalizers.*` включаются автоматически. Production содержит **4 specs**: `detectors.live.spec.ts`, `aggregates.live.spec.ts`, `findings.live.spec.ts` и `proxy.session.spec.ts`. Другие специальные live-пути не включаются discovery всего каталога.

`--browser-spec` принимает одно или несколько точных имён только своего набора и только вместе с `--browser-only`. Другой профиль, неизвестное имя, дубликаты и сочетания с `--detector-only`, `--aggregate-only`, `--rcf`, `--wal`, `--pytest` отклоняются до создания каталога и Docker-ресурсов. Обычные smoke-команды сохраняют прежние проверки. Браузерная ветка не выполняет общий aggregate/detector runtime smoke, WAL gate, lifecycle up/down/up, TLS-агента или внешний E2E.

Браузерная ветка использует те же Compose-файлы, изолированные project/порты/тома и отдельный Buildx. API получает `KAFKA_CONSUMERS_ENABLED=false`: независимый синтетический обработчик/probe пишет через `ProcessingRepository.store_event` и подписывает наблюдаемые интервалы; Kafka consumer не конкурирует за тестовые идентичности. WAL decoder и оба worker настоящие. История формируется по реальным ECS `@timestamp` с двухсекундными окнами и изменчивым count. Создание и preview явно передают `event_time`. Подготовка ждёт 16 последних последовательных canonical-точек (`kind=working`, соседние `window_index`, `time_basis=event_time`, `payload_state=present`, `completeness=complete`), а не выбирает пригодные строки через разрыв и не предполагает готовность по задержке. Ускоренный live-прогрев использует шаг 1 секунду. Готовность и пригодность определяет сервер; complete-векторы и время доступности вручную не назначаются. Trees=20/sample=8 относятся только к синтетическим детекторам теста, defaults продукта не меняются. Source/probe/пользователи/история существуют только в своей `diploma_test`.

Подготовка агрегатов переиспользует начальный writer/WAL-шаг `aggregate_runtime_smoke(prepare_only=True)` и прежний `AGGREGATE_BROWSER_CONFIG`, без полного runtime smoke. Fixture включает два разных источника, их поля/категории и отдельный источник с назначенным нормализатором без событий. Событийный anchor и preview совпадают с исходным `@timestamp=2000-01-01T00:00:00Z`: загрузка и WAL commit происходят сейчас и не переносят точку в текущую дату. Writer сохраняет атомарные event/processed-record/marker и реальную materialization; полнота не назначается вручную. Отсутствующая обязательная fixture — ошибка профиля. Development auth подготавливает настоящий управляемый OIDC и оставляет его активным для полного auth-набора. Группы выполняются последовательно с `--workers=1`; HMR восстанавливает исходный CSS в finally, runner дополнительно сверяет точные байты. Production выполняет aggregate/detector-маршруты, прямые URL/copy/reload и session через HTTPS в закреплённом временном Chromium с импортированным тестовым CA, без исключений TLS. HMR/mock-набор в production не запускаются.

Фоновая detector fixture публикует атомарный статус процесса/фазы/последовательности.
Профиль проверяет её PID, продвижение и подтверждённую материализацию настоящих
event/barrier-маркеров при подготовке,
установке browser-зависимостей и выполнении тестов. Завершение/зависание fixture
или потеря capture прерывают собственную команду с безопасными кодами диагностики.
Для ошибки БД статус сохраняет только имя класса и ограниченный SQLSTATE;
фаза `gating_snapshot` отличает вход в gate от записи событий и materialization.
Начальная подготовка публикует фазы после реально завершённых групп импортов,
хеширования паролей, commit источника и загрузки каталога. Это не фоновый
heartbeat: отсутствие продвижения одного шага по-прежнему даёт stale через10с,
весь startup ограничен прежними60с. До настоящей materialization состояние
остаётся `fixture_capture_starting`, без допуска к browser-тестам. Только
соединения синтетической fixture ограничены connect_timeout5с,
statement_timeout3с и lock_timeout1с; настройки рабочей БД не меняются.
Лёгкий status-probe использует только стандартную библиотеку: он не запускает
новый ORM/пул БД на каждый опрос. Writer обновляет отметку capture только после
материализации своих маркеров, с прежним двухсекундным пределом ожидания.

Перед очисткой detector browser выполняется ограниченная read-only диагностика
истории: UUID попытки/cutoff, происхождение входов, барьеры и время закрытия
непригодных working points, интервалы/sequence/received_at относящихся к ним
аттестаций. Она показывает до 12 попыток, до трёх проблемных окон каждой и до
восьми отчётов окна; сырые события, подписи и credentials не выводятся.

Подготовка fixture, установка зависимостей, свежая готовность и тесты делят один
общий бюджет1200с с входа в `run_browsers`; development/production получают его
остаток, не новый deadline. Ограниченная диагностика и обязательная очистка
выполняются также после исчерпания этого бюджета.
После установки зависимостей и непосредственно перед тестами профиль повторно
ждёт 16 последних полных рабочих окон, включая новое `origin=online` окно после завершения
browser-подготовки; прежняя готовность не заменяет эту проверку. Это не повтор
неудавшегося start: все команды детектора остаются явными действиями теста.
Для auto-live existing browser finally до удаления стенда сохраняет последние10
подтверждённых online closure/worker/model шагов с фазами, queue/admission и
RSS/cgroup/CPU, независимо от исхода теста. Отдельный bounded READ ONLY snapshot
показывает next_index/turn/history_remaining, GC pending/check/backoff и курсоры
до10 моделей; checkpoint/inputs/raw и SQL text не выгружаются. Clock читается
только для своего UUID,10 stat-наблюдений не меняют cache или семантику времени.
Недоступная телеметрия остаётся null/unavailable, не заменяется нулевым временем.

Селекция, обязательность fixtures, свежая готовность и cleanup браузерного контейнера проверяются `scripts/test_browser_test_scenarios.py`, `scripts/test_browser_profile_health.py` и существующим `scripts/test_agent_test_profiles_cleanup.py`. Все файлы, npm-кэш, вывод Playwright и credentials находятся внутри одного каталога прогона; общий finally удаляет Compose, образы, builder/cache и каталог, затем сверяет Docker inventory. Это стандартный браузерный набор frontend, а не полная регрессия всего приложения.

Production копирует выбранные specs вместе с деревом относительных локальных TypeScript-импортов внутри `browser/`. Агрегатная fixture передаётся отдельным `AGGREGATE_BROWSER_CONFIG`, её result-path переводится в `/results`, owned-ID экспортируются обратно в каталог своего прогона до общей очистки. Отсутствующая fixture — отказ. TLS проверяется также у `page.request`; detector config и session-сценарии сохраняются.

Адресные aggregate-команды:
```powershell
python -B scripts/agent_test_profiles.py smoke-development --browser-only --browser-spec aggregates.ui.spec.ts aggregates.live.spec.ts
python -B scripts/agent_test_profiles.py smoke-production --browser-only --browser-spec aggregates.live.spec.ts proxy.session.spec.ts
```

Live-сценарии проверяют расширенный вектор, validate/preview/create/copy/history, точные числа, импорт/экспорт, отдельные prompt/catalog и прямые production URL. Репрезентативные непустые экраны и модалки снимаются в light/dark на 320/2560 px и при масштабе 200%; mock UI сохраняет проверки отказов, guest/scope и подтверждений.

Профили находок запускаются отдельно:

```powershell
python -B scripts/agent_test_profiles.py smoke-development --browser-only --browser-spec findings.ui.spec.ts findings.live.spec.ts
python -B scripts/agent_test_profiles.py smoke-production --browser-only --browser-spec findings.live.spec.ts proxy.session.spec.ts
```

`findings.live.spec.ts` использует подготовленные снимки находок, в том числе с очищенным payload окна, и синтетические ECS-события в изолированной PostgreSQL. Сценарий проверяет обе роли через реальную сессию, прямой маршрут и перезагрузку, фильтры, переходы между окном на таймлайне и строкой таблицы, поиск по точному полуоткрытому диапазону. При отдельном выборе сценариев находок профиль запускает API с его штатными зависимостями и готовит фикстуру одной командой внутри API-контейнера. Подготовка и браузерные проверки входят в общий бюджет 1200 с. Профиль удаляет фикстуры вместе с изолированным стендом.
Helper `aggregates.visual.ts` дополнительно снимает каждую такую форму/модалку
в обеих темах при высоте360px и отдельно при масштабе200%, проверяя доступность
footer и отсутствие горизонтального переполнения. Непустые тестовые ряды проверяются на
320/1920/2560px; снимки лежат только в каталоге своего запуска до общего cleanup.

`detectors.live.spec.ts` также проверяет агрегатное управление через UI: паузу,
reload с сохранённой позицией, продолжение и реальное восполнение worker;
protection plan со всеми моделями одного агрегата и отказ очистки при работающей
модели; явную остановку всех, законный минимум сохранения, принятие и завершение
очистки. Успех подтверждается уменьшением `points/total`, `410` для очищенной
identity, отсутствием повторного payload после reload и сохранностью последних
detector-проекций. Продолжение агрегата не запускает остановленные модели.
Mock retired-карточка в aggregate UI проверяет только чтение прежнего basis и
копию в новый черновик; legacy-объект через удалённый create API не создаётся.

`browser_detector_fixture.working_closure_gate` ограничивает публикацию тестовых
свидетельств транзакцией и сохраняет прежние `lock_timeout=1с` / statement limit3с.
Её `SHARE ROW EXCLUSIVE` на `source_interruptions` совместима с `ACCESS SHARE`
настоящего RR-чтения event-time снимка. Прежняя `ACCESS EXCLUSIVE` конфликтовала
с таким чтением до завершения всей snapshot/overlap-транзакции: детерминированная
DB-регрессия воспроизводит `OperationalError` / SQLSTATE55P03 на прежнем режиме и
проверяет успешный новый gate с освобождением блокировки при успехе и исключении.
Это локализованный класс отказа R39-7-2; точная причинная связь прежнего
удалённого production-журнала не реконструируется задним числом. Полноту
по-прежнему определяет серверный classifier полученного снимка; gate не ждёт
будущего отчёта для улучшения snapshot и не назначает complete вручную.
Адресная регрессия helper на том же контуре:

```powershell
python -B scripts/agent_test_profiles.py backend-db --pytest tests/test_browser_fixture.py
```

Историческая готовность и настоящий WAL проверяются прежними сервисными
регрессиями без этого gate: `backend-db --rcf --pytest tests/ml/rcf_service.py`
и `backend-db --pytest tests/test_availability_materializer.py`.

В production тот же тестовый CA передаётся Node-клиенту `page.request`
через `NODE_EXTRA_CA_CERTS=/tmp/proxy-ca.pem` внутри временного браузерного
контейнера. Проверка TLS остаётся включённой и у Chromium, и у API-клиента.

Прямой запуск Playwright требует уже подготовленного изолированного стенда; предпочтительны команды `--browser-only` выше. Конфигурация Playwright сама не запускает web server. Запуск тестов по порту уже завершающегося smoke-профиля не является воспроизводимой проверкой.

## Контролируемая нагрузка и автоматическая очистка

Селекция `--aggregate-performance` использует существующие smoke-профили,
их Compose, builder, ограничения CPU/RAM и общий finally:

```powershell
python -B scripts/agent_test_profiles.py smoke-development --aggregate-performance
python -B scripts/agent_test_profiles.py smoke-production --aggregate-performance
# Полная согласованная матрица тремя последовательными изолированными группами:
python -B scripts/agent_test_profiles.py smoke-development --aggregate-performance --aggregate-performance-group baseline-through-20
python -B scripts/agent_test_profiles.py smoke-development --aggregate-performance --aggregate-performance-group shared-30
python -B scripts/agent_test_profiles.py smoke-development --aggregate-performance --aggregate-performance-group distributed-30
```

Она несовместима с другими селекторами. Это отдельная проверка контролируемой
нагрузки внутри прежнего стенда; она не заменяет `--aggregate-only` с D5,
перезапуском, моделями и lifecycle. Эквивалентный тяжёлый замер не требуется
дублировать между режимами при том же runtime/коде/ресурсах; выбранный режим
и пределы доказательств явно указываются в отчёте.

Уточнение Этапного архитектора17.11.2026: пять неизменных случаев разрешено
выполнить тремя последовательными изолированными запусками существующего
профиля: `baseline-1 + shared-10-sparse + distributed-20`, затем
`shared-30-dense`, затем `distributed-30-dense`. Общий реальный бюджет
**каждого запуска —1200с**: отсчёт начинается однократно при входе в нагрузочный
helper ДО подготовки данных и заканчивается после всех выбранных измерений.
Первая группа делит одни1200с между
тремя случаями, не получает по1200с на каждый. Сумма активных бюджетов трёх
групп —не более3600с; это не разрешение продлевать конкретный расчёт окна.

`--aggregate-performance-group` выбирает только эти три фиксированные группы;
без него сохранена прежняя матрица всех пяти случаев. Группа без
`--aggregate-performance`, неизвестное имя или сочетание с чужой селекцией
отклоняются до подготовки ресурсов. Каждая команда выполняет только выбранную
группу и не запускает следующую автоматически. До seed печатаются group/case IDs,
host monotonic origin/deadline и его UTC-отметка; это не часы контейнера.
Все группы проверяются на одном варианте кода/зависимостей/данных и с прежними
подготовительными условиями каждого случая, cold/warm, GC и ограничениями.
Число целевых агрегатов, источников,520 canonical окон, плотности, правила и
три online-волны не уменьшаются. Для каждой группы — собственные ресурсы и
finally cleanup. Матрица завершена только после успешных результатов всех
пяти случаев; результат каждой группы и неполный прежний прогон учитываются
отдельно, старый запуск не переименовывается в PASS.

Команды, HTTP-запросы и вложенные ожидания ограничены остатком1200с своей группы
и применимым локальным таймаутом; прогресс/retry/новый case не обновляют deadline.
Поздний положительный результат не принимается. После исчерпания бюджета
остаются ограниченная диагностика и finally cleanup. Если одна30-группа не
помещается, передать причину и фазовые измерения, не продлевать срок автоматически.
Browser1200 и остальные browser/runtime бюджеты этим решением не изменяются.

Матрица включает baseline1, затем10/20/30 агрегатов, плотности2/1000/5000
событий **каждого источника** за300с, общий источник и распределение между
тремя источниками. Правила смешивают count/distinct PID, scalar/array where,
auxiliary/formula с общими листьями, exact median/percentile/mode и статистики.
Три последовательные online-волны проверяют одновременные и сдвинутые сроки.
Данные проходят настоящий ProcessingRepository/record/event/marker, WAL и
те же worker/snapshot/classifier. Сохранённые complete-векторы не вставляются.

Test-only `backend/tests/aggregate_clock_worker.py` запускает обычные main
обоих workers и задаёт логические UTC-часы только для явно перечисленных UUID
(не более30). Карта находится в собственном каталоге прогона и доступна workers
только для чтения. HTTP API и production composition эти часы не используют.
Подготовительный helper вызывает те же typed validate/preview/create и сервисный
UoW для настоящего тестового администратора. Общий bootstrap фиксируется до
начала seed с запасом существующего1200с бюджета, anchor=bootstrap−520×300с:
длительная запись не сдвигает online_start_index. Первое scheduling auto-state
при этом использует исходные физические часы БД. Вызов всего original maintain
в test-entrypoint также заключён в отдельный ContextVar maintenance scope с
finally-reset: checked/next/retry/clear остаются физическими, как и due SQL.
Период60с не ускоряется виртуальными окнами и pending не уходит в будущее.
Scope изолирован между потоками и не меняет production methods.
Во время создания worker остановлен; полная UUID-карта записывается перед
запуском того же сервиса. Между ступенями получается cold restart, последующие
волны внутри ступени используют тот же прогретый процесс. Сдвинутые сетки имеют
anchor−0/1/2с; исходные события одинаково принадлежат своим300с окнам, плотности
и данные не уменьшаются. Каждая следующая волна записывается реальным writer
до собственного release; более поздние timestamps заранее не отравляют ранний
снимок причиной event_time_future.
Монотонные budgets, SQL/lock timeouts, CPU и сохранение измеряются реальными
часами. Перед атомарной записью карты профиль берёт monotonic-отметку внутри
самого aggregate-worker; collector отмечает уже подтверждённый commit теми же
часами контейнера. Их разность — консервативная верхняя граница задержки,
включающая управляющую доставку и наблюдение, а не точный момент commit.
Host/VM UTC и будущие виртуальные даты для этой разности не используются.
Отчёт содержит
p50/p95/max для всех целевых точек, очередь, фазы snapshot/interpret/save/commit,
RSS/cgroup memory, connections, запись и API. По согласованной политике TASK-41
от17.11.2026 мягкий ориентир5с диагностический, бюджет исполнения окна
`T(D)=min(30с,0,8D)` считается отдельно от очереди. Прежние p95≤5с/max≤10с
не универсальные pass/fail-границы; значения и старые неуспехи сохраняются.
Положительные профили требуют точных computed и завершения до следующего
close deadline без растущего backlog; массовый uncomputed не считается PASS.
Для каждой волны проверяются точные ожидаемые значения всех признаков реального
writer, порядок выходов, статус computed и политика исполнения. Координаты
записи сохраняются до её выполнения: oracle независимо проверяет count/distinct,
сумму/ratio/категориальный count и exact median/percentile/mode/variance.
Wait online-волны ограничен остатком до следующего close deadline от первого
release и общим deadline группы; ingest/polling не выдают новый срок.
ВсеN×3 computed подтверждены до перехода к следующей волне, online pending
возвращается к нулю; фактический прогресс и оставшаяся initial history приводятся
отдельно и не подменяются числом seed.

Test-only sidecar присваивает заданиям job_id, не меняя production job/carrier
или бюджет. По адресному разрешению Этапного архитектора17.11.2026 допускается
одноразовый test-only release заранее подготовленной online-волны сразу после
реального commit ACK первого очищения выбранной цели вместо host REST-poll.
Hook заранее привязан к run/case/wave/closure, входные данные/markers готовы;
rollback/чужой/повторный/пропущенный до arm сигнал не запускает release.
Реальный monotonic marker того же worker снимается до смены тестовых часов,
задним числом времена не переписываются. Искусственно задерживать GC/foreground
или менять production файлы/движок ради перекрытия нельзя. Это согласованное
изменение только синхронизации подготовки case; все данные, три волны,
одновременные/сдвинутые сроки, ресурсы, порции и group1200 сохраняются.
REST200 до удаления и410 после, физическое освобождение всех целей и ID-пары
проверяются как прежде. Хотя бы одна настоящая тяжёлая порция прежнего размера
целиком удовлетворяет `release <= gc_start < gc_commit_ack <= online_commit_start`;
частичное пересечение и marker/no-op не дают PASS. Hook scoped/finally-reset,
положительная и отрицательные границы покрываются постоянными тестами.
Предыдущий отказ вложенности остаётся неуспехом, а не PASS после смены подготовки.

Trace sidecar сохраняет поведение ниже, не меняя production job/carrier
или бюджет. Он сохраняет начало/конец существующих discovery-вызовов,
интервалы до/после реальных append/acquire и исходные execute start/end worker.
Прежняя queue_seconds начинается при входе в discovery iteration и включает SQL;
она не называется чистым FIFO; discovery_to_execute_seconds сохраняет ту же
семантику отдельным явным именем. Отдельно публикуются discovery duration,
нижняя/верхняя границы FIFO/dispatch, actual whole task и commit ACK/observer.
Release может быть позже начала уже поставленного задания: знаковые дельты
сохраняются, очередь не вычитается из core для выдуманного pre-enqueue времени.
Sidecar ограничен64 ready+2 active; завершённая metadata использует существующие
bounded steps/closed maps, ошибки/переполнение видимы и не дают нагрузочный PASS.
Для причинного разбора GC `maintenance_jobs` в том же test-only JSON сохраняет
хвост до256 завершённых maintenance-заданий и до192KiB метаданных: job_id,
aggregate_id, kind и девять существующих monotonic timestamps. GC-порции и
ошибки связываются с заданием по job_id. Отдельные observed/evicted/invalid
счётчики и null показывают границы сохранённых данных; усечение не превращается
в вымышленное ожидание очереди или доказанный backoff. Прежние live counters
и нагрузочные критерии остаются прежними.
Новых SQL, опросов, потоков или периодических публикаций ради trace нет.
При отказе подготовки W1 `distributed-30-dense` отдельный best-effort capture
сохраняет до30 уже полученных HTTP status/canonical ID, до15 уже измеренных
batch durations, восемь возможных assert-site и безопасные function/line.
Он ограничен16KiB UTF-8 вместе с префиксом и CRLF; response body, exception repr,
исходные события, checkpoint и секреты не публикуются. Host markers используют
существующие проверки времени, не вымышленные driver ACK или моменты arm.
Сбой любой операции capture не меняет успешный результат и исходное исключение;
сам capture не добавляет clock calls, SQL, GET, опросов или потоков.
В заключительной browser-диагностике необязательный `job_capture` проецирует
только уже загруженную worker metadata выбранного auto-history aggregate:
до256 maintenance jobs и по10 online/step/span/error связей, строгие IDs,
девять существующих timestamps и счётчики retained/evicted/invalid.
Дополнительное поле ограничено192KiB UTF-8 вместе с envelope/CRLF; старые
записи вытесняются с учётом потерь, недоступные/невалидные значения остаются
null. ID связи может сохраниться без уже вытесненной записи задания.
Исходные события, checkpoint, response body и секреты не включаются.
Проекция, сериализация и emit нефатальны, новых чтений/обращений/clock calls
нет. Поздний хвост сам по себе не гарантирует охват всей фазы generation и не
доказывает причину прежнего timeout. Для TASK-41 §13.3 отдельный test-only
`generation_phase` сохраняется независимо от вытеснения этих хвостов.
Begin передаётся атомарно с прежним единственным clock jump, end — в `finally`
generation до protection/resume, с тем же clock value. Scope включает run,
aggregate, новый nonce и phase; чужой/replayed marker не перезапускает capture.
Worker наблюдает границы только при прежнем чтении clock и берёт по одному
monotonic timestamp на принятую границу; задержка доставки неизвестна (`null`).
Накопитель до64KiB внутри прежних192KiB содержит полные счётчики наблюдений
discovered/offered/admitted/completed по foreground/maintenance, суммы доступных
интервалов discovery/append/queue/execution/ACK, bounded pending/reason metadata,
не более10 последних и10 самых медленных связанных jobs, по10 span/error links.
Повторные обнаружения считаются наблюдениями, не уникальными jobs. Интервалы jobs
могут пересекать границы, суммы перекрываются и не являются wall time фазы.
Preexisting jobs, observed/retained/dropped, missing links и ошибки показаны явно.
Нет end — active/incomplete; нет begin — fragment с неизвестными счётчиками;
исход failed, потерянная связь или ошибка capture не получают `complete=true`.
Замороженная end-сводка переживает длительный resume. Новых SQL/HTTP/процессов,
потоков и периодических опросов нет; ошибки capture/end marker не заменяют
исходное исключение и не продлевают assertion deadline. Накладные расходы
диагностики измеряются отдельно, вне runtime-порогов.
Runner публикует timeline из существующих отсчётов подготовки, canonical,
production dependencies/readiness и точный остаток1200 при передаче Playwright
команды. Локальные auto/generation/resume snapshots и framework durations в
afterEach имеют отдельный Node clock domain; afterEach duration ещё не включает
оставшийся teardown, финальные длительности остаются в штатном reporter.
Их нельзя суммировать как wall time или вычитать из Python monotonic. Точный
остаток общего бюджета при входе в auto и launch-to-entry lag неизвестны
(`null`): существующего межпроцессного entry transport нет. Новые часы/опросы
для подмены этого неизвестного не добавляются. Диагностика не меняет данные и бюджеты.
Существующий test-only inspect всех случаев дополнительно отдаёт
`old_cleared_at` из уже загруженной ORM-строки, без нового SQL; недоступное
диагностическое значение остаётся null. Новая диагностика не повторяет волну,
не продлевает GC120/group1200 и не превращает прежний отказ в PASS.
Сохранённая execution-диагностика computed относится к before_commit и проверяется
в пределах T; фактический поздний commit ACK показан отдельно и не превращается
в новый timeout после подтверждённого вычисленного результата.
Пиковая память контейнера читается из kernel high-water counter: cgroup v2
memory.peak либо cgroup v1 memory/memory.max_usage_in_bytes. Отдельно сохраняются
версия, путь счётчика и memory.max/memory.limit_in_bytes. Неизвестный счётчик или
лимит не заменяется process RSS и не проходит проверку768MiB; ресурсы повторно
проверяются по итоговому readback после GC. После истечения применимого срока
online-волны (не позже следующего close deadline и остатка group1200) готовность
не ждут повторно: один bounded diagnostic read выводит последние30 подтверждённых фаз,
сводку GC/памяти и позиции до3 pending карточек, без raw/векторов/секретов.

Для настоящего GC writer сеет520 исторических интервалов, а worker действительно
строит canonical точки порциями; фактические closed/history_remaining отдельно
показаны в отчёте и не подменяются числом seed. Окно8 имеет10001 реальную
membership-запись. При любом L≤519 оно входит в резерв512; первая online-точка L520
делает его безопасным кандидатом. Проверяются реальные удаление payload/snapshot
и зануление тяжёлых membership details при сохранении всех10001 ID-пар. До и после
читаются отдельные физические факты, поэтому исчезновение строки из списка не
подменяет доказательство компактизации. Для membership проверяются обе удаляемые
колонки (fingerprint ИЛИ event_time), постоянные ID-пары остаются10001.
Период политики60с не сокращается.
Число10001 возвращается setup и проверяется точными counts до/после: это две
полные порции по5000 и остаток1, при неизменных SQL budgets и online-плотностях.
Для нагрузочных ступеней10–30 данные второй online-волны готовятся заранее;
существующий REST lookup должен показать present200 выбранного старого окна.
Затем заранее scoped hook выпускает волну после настоящего commit ACK первого
очищения этой цели, а verified automatic410 той же identity независимо
подтверждает удаление. Worker продолжает работать без искусственных задержек,
барьеров или нового daemon. Уже очищенная до arm цель не выпускает волну
задним числом; если present или нужный ACK не наблюдался, ступень завершается отказом.
Baseline1 выполняет три волны без этого conditioning, сохраняя все физические GC
и resource-проверки и фактический отчёт пересечений.

Test-entrypoint дополнительно оборачивает исходные compact/save/maintain и
читает rowcount уже выполняемых UPDATE без дополнительных SQL. В отчёт попадают
только подтверждённые commit порции: closure UUID, фаза, число изменённых строк,
монотонные начало SQL-фазы/её завершение/получение commit ACK. Rollback и
неподтверждённый commit не считаются прогрессом. Общий ring ограничен256 spans;
отдельный ring256 для порций больше3 строк сохраняет тяжёлое доказательство при
потоке малых порций; оба показывают вытеснения и общие счётчики фаз/строк.
У online commit отдельно записываются отметки до вызова commit и после ACK;
это границы подтверждения, а не точная серверная дата commit. Для ступеней10–30
обязателен хотя бы один GC-span больше3 изменённых строк конкретной старой10001-row точки, полностью
попавший в интервал от release до начала online commit: release≤GC start<GC ACK≤
online commit start. Обычный GC, все порции target_history и тяжёлые порции target_history
учитываются отдельно: остаток1 строки не подтверждает тяжёлую нагрузку. Частичные
пересечения интервалов ACK показаны только как наблюдения. Baseline1 сохраняет
те же факты без обязательного пересечения. После физической проверки GC прежнее
ожидание10с, ограниченное общим остатком, требует ACK всех10001 membership-строк
каждой целевой точки в worker metrics. Сводка ограничена30 тяжёлыми closures,
учитывает последнюю порцию1 и явно сообщает переполнение; старый metrics-файл
не подтверждает завершение. Дополнительных SQL, потоков или публикаций нет.
Новый ACK-counter регистрируется первой полной5000-row порцией фиксированной
10001-row цели, затем учитывает все её подтверждённые порции, включая остаток1.
Это сохраняет30 мест для30 целей: наблюдавшиеся incidental7-row closures
не занимают registry. Все spans и исходное определение heavy>3 сохраняются;
это профилактика allocation-ловушки, а не заявление о доказанном отказе30.
Connections проверяются одним снимком после GC; это наблюдение, не измерение
пикового числа соединений за всю online-волну.
Все online-интервалы сохраняются в отчёте обычным списком: aggregate UUID,
window_index, point UUID, release и monotonic commit-start/ACK/observer. Для
каждого case остаются всеN×3 интервалов, максимум90 и128KiB без raw/векторов.

Только при выборе `detectors.live.spec.ts` browser fixture дополнительно создаёт
отдельный источник и сеет520 интервалов; canonical initial-history окна строит
настоящий aggregate-worker. Порции записи по40 окон
публикуют завершённые milestones и ждут настоящую materialization с прежними2с;
startup60с, stale10с и обычный16-point fresh-online gate сохраняются.
Перед browser-тестами runner сначала фиксирует old0 present DTO и наличие его
физического payload/snapshot. Только новый auto-сценарий получает отдельный
предел 300 с, запущенный один раз непосредственно перед командой запуска настоящего
aggregate-worker. Запуск worker, early before-proof и ожидание точно520 canonical
headers0…519 с origin=initial_history/time_basis=event_time делят этот предел,
включая cleared и любые фактические качества. Readiness-only режим тестового
read-only helper читает только этот count; прежний old0 before-proof не
переписывается после auto-GC. Unknown/invalid count, превышение520 и deadline
дают отказ с bounded диагностикой, даже если520 появились позже. Эти 300 с входят
в общий1200с; early before-proof не связан с прежним startup60 и не означает
готовность всех520. Он фиксируется сразу при появлении и сохраняется неизменно.
Auto-case имеет предел720с: bulk-закрытие520 окон — до360с, оба resume
ожидания моделей делят один хвост300с от первого start, включая navigation/request/UI
и его прежний максимум45с. Generation360 начинается до единственного clock-jump
и ограничивается остатком auto-case.
Локальные monotonic deadlines вложены в body720, а последний dev/prod запуск
Playwright после dependencies/fresh16 получает `--global-timeout` с остатком
неизменного browser1200. Между host/container передаётся длительность, не чужой
monotonic origin; предыдущие specs расходуют тот же framework timeout. После
исчерпания срока выполняются только ограниченная диагностика и очистка.
Canonical/generation/resume start, completion/failure и before-proof печатают
ограниченные count/elapsed/remaining; progress не обновляет начало отсчёта.
Первая auto-GC проверка допускает до80с
(обычный период60с плюс20с на работу), без sleep или изменения policy cadence.
Прежние180с других live-case, start45с, короткий online30с, fresh16/60с,
PID/marker supervision и production budgets не меняются. First-history
и последние10 шагов показывают origin/numeric timings ещё до online/GC;
это телеметрия, не назначение complete. После тестов read-only проверка подтверждает
их1→0 и сохранённую cleared identity. Для stopped unread-хвоста длиннее резерва
тот же UUID-scoped тестовый clock позволяет настоящим workers закрыть и потребить
более512 online-окон. Часы и данные не переводят unknown/gap в complete.
Файл карты передаётся тесту через `auto_cleanup.clock_file`; запись выполняется
атомарно внутри своего каталога. Auth/resources и один detector UI mock-набор
этот расширенный seed не получают.

AggregateWorker допускает максимум две задачи при обычном256MiB бюджете:
foreground и maintenance делят512MiB, оставляя256MiB прежнего768MiB контейнера
для Python/каталога/транспорта. Очередь содержит не более64 идентичностей,
executor получает только допущенные задачи. Не более одной maintenance future
на процесс и одной задачи на aggregate; долговечная работа сверх очереди
повторно обнаруживается в БД. Более крупный настроенный per-operation budget
снижает concurrency, а бюджет свыше512MiB даёт явный отказ запуска.
Ожидающая допустимая maintenance получает допуск после одного foreground;
отдельная admission-квота не меняет durable online/scan/history-квоту3.
Регрессии проверяют обе ёмкости (одна/две задачи), FIFO, blocked waiter и
продвижение обоих видов; нагрузка и обе D5-поставки измеряют возможное
увеличение foreground-очереди при прежних GC120/group1200 и ресурсах.
Ограниченная диагностика worker показывает admission/очередь и реальные фазы,
не содержит событий, features, checkpoint или credentials.

Постоянные регрессии: `backend/tests/test_aggregate_worker.py`,
`scripts/test_aggregate_performance.py` и изменённые полные browser/health/selection
модули. Фактические команды, результаты и ограничения текущего прогона отдельно
фиксируются в отчёте; наличие сценария не означает, что он уже прошёл.

## Проверки затронутых областей

На всех фазах, включая предкоммитную, выбирайте тесты по влиянию и роли. Исполнитель использует адресную селекцию; Этапный архитектор выполняет полный набор выбранного компонента. Для backend без изменений Kafka/ingest достаточно полного `backend-db`; для backend вместе с Kafka — полного `kafka-integration` без дублирования backend. Для одной Kafka допускается селекция всего её интеграционного набора, без остальных backend-тестов. Полные уже успешные независимые проверки неизменившегося компонента повторно перед коммитом не запускаются. Для backend поддерживаются `backend-db --pytest <test nodes>`, `kafka-integration --pytest <test nodes>` и `backend-db --rcf --pytest <test nodes>`. Для frontend из его каталога можно выполнить `npm exec -- vitest run <test files> --maxWorkers=1` и выбранные specs через `npm run test:browser -- <specs>`, соблюдая общие правила временных путей и очистки. Аргумент `--pytest` не ограничивает профиль `frontend`: его текущая реализация всегда запускает весь frontend-профиль.

## Запуск, окружение и очистка

Перед обычным `down` smoke сохраняет в каталоге прогона точные привязки томов к контейнерам своего Compose-проекта. При финальной очистке учитываются и оставшиеся после `down` анонимные тома образов Kafka. Удаление разрешается только для записанной привязки, если том отсутствовал в исходном инвентаре, имеет ожидаемые метки и больше не подключён к контейнеру. Посторонние и исходные тома сохраняются; неопределённая принадлежность приводит к явной ошибке очистки.

`python -B scripts/agent_test_profiles.py backend-db --wal` запускает Linux/WAL gate в штатных образах bootstrap/decoder на изолированной PostgreSQL: повторные проверки при живом декодере и непрерывном writer, неизменность границ и credentials, продвижение durable LSN, отказы при неверном пароле, правах и публикации. Он не подменяет общий pgoutput gate. Оба smoke-профиля выполняют тот же gate, а после записи синтетических событий и точек — полный `up --build -d` при работающем приложении и цикл `down` без `-v` → `up --build -d`. Сверяются exit codes migrate/bootstrap, readiness/UI, идентичность томов/слота, границы capture, монотонность LSN, прежние события/точки и хэши/права секретов. Очистка выполняется только после проверки сохранности. Контракты prepare проверяются `python -B -m pytest scripts/test_prepare_dev_kafka.py` с временными путями внутри каталога прогона.

Все профили запускаются из корня репозитория одной командой:

```powershell
python scripts/agent_test_profiles.py frontend
python scripts/agent_test_profiles.py backend-db
python scripts/agent_test_profiles.py backend-db --rcf
python scripts/agent_test_profiles.py kafka-integration
python scripts/agent_test_profiles.py end-to-end
python scripts/agent_test_profiles.py end-to-end --aggregate-only
python scripts/agent_test_profiles.py external-end-to-end
python scripts/agent_test_profiles.py smoke-development
python scripts/agent_test_profiles.py smoke-production
```

`backend-db --rcf` собирает закреплённый Linux ML-target и запускает одноразовый
verification service на сети той же PostgreSQL. JVM heap — 512 MiB,
контейнер — 1 GiB/2 CPU; внутренний TEST_DATABASE_URL использует postgres:5432.
Тесты mapper replay, реального сервиса/транзакций и обратимости миграции
выполняются без skip при отсутствующих Java/JAR. Обычные host-наборы их
не собирают автоматически. Зависимости, ограничения и точная селекция —
в [контракте детекторов](detectors.md). Lifecycle inventory/build/run/cleanup
общий с остальными профилями. Постоянный detector-worker входит в оба режима,
полную сборку/ожидание/cleanup и lifecycle up/down/up. Полный ML-набор включает
`tests/ml/rcf_worker.py` вместе с модельными, сервисными и миграционными тестами.
Он также включает `tests/test_model_runtime.py` с проверками IPC.

В worker переиспользуется один JVM-runtime владельца, а каждый кандидат
создаёт новую модель из checkpoint БД. Lifetime, framing, deadline и отмена —
в [контракте детекторов](detectors.md#lifetime-jvm-и-модели).
Адресный Linux-прогон:

```powershell
python -B scripts/agent_test_profiles.py backend-db --rcf --pytest tests/ml/rcf_worker.py tests/test_model_runtime.py tests/ml/rcf_service.py::test_worker_discards_candidate_archived_during_real_model_and_resumes
```

Он печатает безопасную сводку cold/warm init/restore/process/checkpoint, PID и RSS.
Cold reference запускается отдельным свежим процессом на каждый запрос.
Проверяются чередование конфигураций, повтор неподтверждённого кандидата,
точное совпадение результатов и checkpoint; реальные IPC faults, kill/reap
и отсутствие оставшихся I/O-потоков. Оба `--aggregate-only` smoke сохраняют
прежние D5, три модели/два источника и шесть проверок точных индексов.

`--detector-only` выбирает поставку/worker/lifecycle и доступен обеим ролям.
Он может быть достаточным smoke перед коммитом серверной worker-задачи, если
UI/OIDC/TLS-agent и их контракты не затронуты; их сценарии автоматически не добавляются.
Полные стандартные smoke остаются для полной приёмки этапа либо фактического
влияния на соответствующие пути. Текущая реализация `--detector-only` пропускает
браузерные проверки, но пока сначала делает общий `up --build -d` и запускает
frontend/oidc-idp. Это не минимальный запуск одних worker-сервисов. Для устранения
лишних сборок и сервисов Этапный архитектор уточняет существующий профиль в активной
задаче; наличие этих сервисов на стенде не требует запуска их полных тестов.

Команды одинаковы для Windows PowerShell и Linux shell. Добавьте `--check-only`, чтобы проверить resolved Compose без запуска сервисов; если внешний профиль при этом скачал Wazuh-образ, он удалит только новый образ. Docker-профиль печатает имя изолированного Compose-проекта и адреса без паролей; успешный запуск завершается кодом 0 после проверки очистки. При ошибке скрипт сообщает точные идентификаторы остатка и путь временного прогона: проверьте принадлежность ресурсов по Compose label и не удаляйте чужие тома. Постоянная проверка раннего отказа и `--check-only` запускается командой `python -m pytest -q scripts/test_agent_test_profiles_cleanup.py`. Не запускайте несколько профилей на одной общей тестовой БД одновременно.

Нужны Python-зависимости backend, Node-зависимости frontend, Docker Compose и доступный Chromium для Playwright. Используются образы из проектных Compose-файлов: PostgreSQL 18, Kafka 4.2.0, Fluent Bit 4.2.3, Nginx 1.30.5 Alpine. Внешний профиль дополнительно использует `wazuh/wazuh-indexer:5.0.0-beta5` по digest `sha256:70d7c42518e4e295a73947ef916fc26e67f81658dd0c892d7032018b296607ce`. Скрипт создаёт уникальный Compose project `diploma_t21_*`, порты на loopback, временные секреты и БД `diploma_test` только в собственном временном томе. Все временные файлы находятся в `.test_runs/runs/<run-id>/`. Даже при ошибке скрипт останавливает и удаляет только ресурсы своего проекта и проверяет остатки. Если Docker недоступен до начала очистки, сохраните путь из ошибки и выполните точечную очистку этого проекта после восстановления Docker; не используйте общую очистку Docker.

Временный BuildKit builder и шаги сборки Kafka используют сеть хоста только при сборке образов, чтобы разрешение имён пакетных репозиториев совпадало с обычным Docker-контейнером. Сервисы Compose продолжают работать в изолированной сети проекта; builder удаляется вместе со стендом.

`frontend` запускает typecheck, lint, format:check, тесты и сборку. `backend-db` поднимает тестовую PostgreSQL и по умолчанию выполняет полный набор без трёх файлов, требующих брокер: `test_kafka_integration.py`, `test_agent_recovery.py`, `test_window_kafka_evidence.py`. Используется штатный `backend/scripts/run_tests.py --temporary`; `--pytest` выбирает профильные тесты. Эти три файла проверяются в `kafka-integration`, который поднимает PostgreSQL и Kafka с SCRAM/ACL, сверяет metadata внешнего и внутреннего listener и без `--pytest` запускает весь backend-набор через тот же runner. Для адреса и credentials тестов скрипт задаёт `KAFKA_TEST_BOOTSTRAP_SERVERS`, `KAFKA_TEST_SECURITY_PROTOCOL`, `KAFKA_TEST_READER_PASSWORD_FILE`, `KAFKA_TEST_ADMIN_PASSWORD_FILE`, `KAFKA_TEST_DEV_ALLOWED_BOOTSTRAP_SERVERS` и `TEST_DATABASE_URL`.

`--aggregate-only` в существующих `smoke-development`, `smoke-production` и
`end-to-end` выбирает серверный event-time сценарий без браузера. Несовместимые
селекторы (`--rcf`, `--wal`, `--pytest`, `--detector-only`, `--browser-only`)
отклоняются до создания ресурсов. Это не новый профиль. API, proxy, pgoutput,
aggregate/detector workers настоящие; development использует frontend только
для проверки proxy. OIDC/provisioner в этой ветке не собираются и не запускаются.
API consumers отключены в изолированном overlay: writer пишет атомарные
event/processed-record/marker. Два источника включены, отсутствие необязательного
probe не подменяется искусственным complete.

Готовность архива проверяет canonical0…15 по by-index:409 означает ожидание,
200 — сохранённый payload,410 принимается только с той же проверенной identity,
origin=initial_history и automatic/model_history_v1. Автоматическая очистка
не превращает уже закрытое окно в not-ready. Позднее событие в archive0 остаётся
в сценарии; доступный прежний payload сравнивается целиком. Независимо от200/410
дополнительно берётся present online-точка внутри резерва, в неё записывается
ровно одно позднее событие и проверяются точный прирост late_count на1 и полное
равенство DTO по by-index и by-ID. Модели получают16 последних последовательных
present complete online-точек, без выбора удобных строк через пропуск.

Проверяются старый архив до bootstrap, его последнее окно, exact membership,
циклическое обнаружение late, неизменность сохранённой точки, D=5 и три настоящих
AWS RCF модели sample_size=8: обычная и overlap на одном агрегате, независимая
на втором. Замер штатных оценок начинается с окон, открытых после готовности
всех лесов: evaluation_completed_at <= close_not_before_at+5 с. Далее — находки,
pause/unpause, restart workers, catch-up, replay прежней квитанции, остановка всех
связанных моделей, отказ N<M, сохранение M, физическое освобождение payload,
перезапуск aggregate-worker после частичного подтверждённого удаления без потери
прогресса и границы операции, независимость Findings/checkpoints и running
up --build/down/up. После этих циклов сравниваются также cursor, paused,
generation, постоянные closure identity и завершённые операции очистки. В полных
smoke/E2E используется тот же серверный сценарий; потребители API восстанавливаются
перед дальнейшей проверкой приёма агентов. `--detector-only` — совместимый
серверный селектор этого сценария.

Runtime smoke печатает UTC-моменты готовности моделей, исходную границу замера,
идентичности источников/агрегатов/детекторов и результаты каждого контрольного
индекса. Timestamp читаются из сохранённых outcomes именно этого индекса, даже
если карточка уже показывает следующий. При отказе до cleanup выводятся позиции,
backlog, Docker stats, последние 48 шагов coordinator и до 16 шагов запуска из его heartbeat: длительность
state/read и всего шага, начало и исход. Для оценки отдельно видны чтение,
модельный кандидат, финальный lock и commit. Raw, credentials и checkpoint не выводятся.

Browser fixture использует event-time контракт TASK-40, как и новый UI41.
`--aggregate-only` остаётся отдельной серверной селекцией и к этой fixture
не обращается; успех server smoke не подменяет UI/browser-проверки.

```powershell
python -B scripts/agent_test_profiles.py backend-db --pytest tests/test_event_time_contracts.py tests/test_event_time_pipeline.py tests/test_event_time_migration.py
python -B scripts/agent_test_profiles.py backend-db --rcf --pytest tests/ml/event_time.py
python -B scripts/agent_test_profiles.py smoke-development --aggregate-only
python -B scripts/agent_test_profiles.py smoke-production --aggregate-only
```

Профили на одном Docker daemon запускаются последовательно: параллельная сборка
мешает сопоставлению исходного и итогового инвентаря общей службы Docker.

`smoke-development` собирает и запускает PostgreSQL, Kafka, migration, provisioner, API, frontend и proxy, затем проверяет `/agents` через proxy. `end-to-end` использует development UI: регистрирует агента и два топика, однократно получает конфигурацию, отправляет тестовую запись временным Fluent Bit, проверяет Kafka, включает source с нормализатором и сверяет PostgreSQL и страницу «События». Он также проверяет сохранение сырого лога и Kafka-происхождения, перезапускает API и убеждается, что сохранённая запись не продублирована. Перед UI-сценарием профиль извлекает встроенный Lua Code filter `systemd` непосредственно из генератора конфига и запускает его на синтетической записи с Fluent Bit 4.2.3. Вход задан с timestamp `2000-01-01T00:00:00Z`; проверяется точный конверт `{timestamp, log}`, сохранение старого времени в строке `log` и timestamp времени обработки. Проверка использует синтетический dummy input, не читает живой journald. Fluent Bit использует неизменённый скачанный конфиг и внешний Kafka listener; тестовый контейнер разделяет сетевое пространство брокера, чтобы тот же loopback-адрес и динамический внешний порт были доступны без публикации тестового порта наружу. Проверка установки на отдельном хосте и живое чтение Windows Event Log остаются вне этого теста.

В отчёте о каждом прогоне указывайте профиль, команду, выведенный скриптом Compose project, фактические свободные порты, результат и отсутствие оставшихся ресурсов. Пароли и содержимое конфигурации Fluent Bit в отчёт не переносите.

`smoke-production` собирает production proxy с React и запускает PostgreSQL, Kafka, migration, provisioner и API на изолированном Compose-проекте. Он проверяет HTTPS с доверенным тестовым CA и именем `localhost`, API readiness, прямые UI-маршруты, JS/CSS, отказы для неизвестного API, отсутствующего asset и закрытой документации. Дополнительно проверяется доставка события тестовым Fluent Bit через внешний TLS listener. Интерактивные вход, сессия и выход проверяются отдельно в браузере через тот же HTTPS proxy.

`smoke-development` также создаёт синтетического локального администратора и выполняет тот же браузерный сценарий входа, обновления страницы и выхода через HTTP proxy разработки.

`end-to-end` проверяет реальный локальный путь Fluent Bit → Kafka → PostgreSQL → UI. Через UI он создаёт и проверяет Kafka-подключение, регистрирует агента, получает конфигурацию, создаёт и проверяет нормализатор на записи из Kafka, назначает его источнику и включает источник. Профиль создаёт только синтетическую выключенную запись внешнего источника для главной страницы.

`external-end-to-end` использует тот же изолированный Compose-контур в production-режиме и добавляет закреплённый Wazuh Indexer 5.0.0-beta5 с синтетическими ECS-ориентированными документами. Сертификаты и пароль читателя создаются в `.test_runs/runs/<run-id>/`; индексер доступен на отдельном loopback-порту хоста и по `indexer:9200` из API. Профиль проверяет HTTPS/CA, чтение данных реальным индексером, live backend-контракт, подключение и поиск через production API, отсутствие копирования в `parsed_logs` и неизменность списка Kafka-топиков. В браузере через production HTTPS proxy он создаёт подключение и источник, загружает CA, проверяет подключение, включает источник, ищет, фильтрует и открывает карточку. Затем профиль выбирает синтетический локальный источник в той же странице событий, проверяет его фильтр, карточку и доступность только локального удаления. Локальная запись создаётся непосредственно в изолированной БД для проверки переключения; полный путь Fluent Bit → Kafka → PostgreSQL проверяет `end-to-end`. Браузер доверяет только тестовому CA в своём временном контейнере. Синтетические документы имеют форму Wazuh 5, но не созданы Wazuh Manager.

Профили `smoke-development` и `smoke-production` дополнительно запускают в рамках своего прогона
синтетический OIDC IdP. Development использует HTTP issuer
`http://localhost:<свободный порт>/realms/master`, проверяет маршрут API через
`host.docker.internal` и отказ для другого HTTP-адреса. Это имя в тестовом
контуре назначено только синтетическому IdP в отдельной сети проекта;
production использует HTTPS с отдельным тестовым CA, проверяет отказ без CA и
при неверном имени сертификата, затем загружает CA. Через тот же proxy и изолированную БД они
сохраняют черновик,
проверяют discovery/JWKS, выполняют пробный Authorization Code Flow в Chromium, активируют провайдера через UI, завершают в браузере активный OIDC-вход, затем проверяют локальный резервный вход, отключают OIDC и проверяют
сохранение отключения после перезапуска API. IdP и его сертификаты удаляются вместе с ресурсами
прогона; рабочие IdP и секреты не используются. Для пробного входа Chromium запускается во временном
контейнере в сетевом пространстве proxy. В production тестовый CA импортируется только в NSS-базу этого
контейнера: браузер проверяет TLS без исключений и подтверждает сохранение локальной сессии
администратора. Результат находится в каталоге текущего прогона, образ удаляется при очистке.

Синтетический IdP собирается как временный контейнер `oidc-idp` в том же Compose-проекте. Его
порт в development публикуется только на динамическом loopback-порту хоста для локального браузера;
в production он не публикуется. Для API сервис подключён к отдельной сети проекта с параметром
`internal: true`; в development вторая временная сеть обеспечивает loopback-доступ локального браузера.
Перед smoke скрипт проверяет resolved Compose, затем после запуска проверяет список сетей и точные
host bindings через `docker inspect`. Development выполняет OIDC браузерный сценарий локальным Chrome,
production запускает временный браузерный контейнер в сетевом пространстве proxy.

Для production OIDC smoke профиль запускает временный контейнер из `mcr.microsoft.com/playwright:v1.63.0-noble`, где Chromium 153.0.8010.12 соответствует Playwright 1.63.0 из зафиксированного `package-lock.json`. Внутри устанавливаются `libnss3-tools` для тестового CA и `socat` для доступа Chromium к proxy; по минимальному lock-файлу ставится Playwright. Контейнер запускается в сетевом пространстве proxy и удаляется после сценария. Образ удаляется при очистке, если профиль скачал его; при запуске с уже установленным образом он считается исходным ресурсом машины. Тестовый CA монтируется только в контейнер, добавляется в его системный trust store и проверяется Chromium без TLS-исключений. Development OIDC smoke использует локальный Chrome и loopback-порт синтетического IdP. Результаты находятся в каталоге текущего прогона; доверие хоста не меняется.
