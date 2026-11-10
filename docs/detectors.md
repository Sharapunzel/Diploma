# Детекторы AWS RCF и постоянный worker

Сервис создаёт конфигурацию, сохраняет входы прогрева, обрабатывает одну
следующую working point и атомарно сохраняет checkpoint, курсор, outcome и
находку. Постоянный `detector-worker` выполняет явно разрешённую работу в обоих
Compose-режимах. HTTP управления и диагностики и интерфейс TASK-37 описаны ниже.
Техническая готовность модели не гарантирует качество обнаружения.

## Интерфейс детекторов

Раздел «Аномалии → Детекторы» (`/detectors`) доступен с `detectors.read`.
Список использует серверные страницы, UUID агрегата и состояние исполнения;
обновляется вручную. Фильтры редактируются в диалоге: отмена сохраняет текущую
выборку, применение начинает с первой страницы, ошибка сохраняет черновик без
показа старых строк как результата нового запроса. Создание (`/detectors/new`) и кнопки управления требуют
также `detectors.write`. Гость при прямом входе не читает detector/aggregate API.

В форме выбирается активный агрегат в одном поле с серверным поиском по имени
по всему каталогу. Показываются первые 10 совпадений; поиск уточняется после
паузы 250 мс. Источник различает одинаковые имена. Выбранный агрегат сохраняется
при новом поиске и ошибке загрузки. Сохранение
создаёт конфигурацию без запуска. Обычный прогрев выбран по умолчанию;
ускоренный задаёт целый шаг от 1 секунды до длительности окна. Дополнительные
настройки раскрываются отдельно; seed передаётся точным JSON-числом int64.
Историю, размерность и порядок признаков определяет сервер.

Карточка (`/detectors/:id`) разделяет разрешение исполнения и готовность модели,
текущую причину и предыдущую ошибку, потреблённое и оценённое окно. Показываются
фактическое покрытие прогрева, UTC-расписание в выбранном поясе, backlog/lag
и происхождение восстановления. Неизвестные значения не заменяются нулями.
Постоянная 🔥 означает только подтверждённый ускоренный прогрев.

Запуск — отдельное действие; остановка требует подтверждения и сохраняет
модель/курсор для последующего восполнения. Подтверждение содержит имя и UUID;
смена карточки, пользователя или права управления аннулирует диалог.
После квитанции выполняется GET. Новое намерение очищает квитанцию предыдущей
команды; сообщение о подтверждении относится только к текущей команде.
Опрос видимой карточки ограничен интервалом 3 секунды и одним GET одновременно,
приостанавливается в скрытой вкладке и после ошибки, прекращается при уходе.
Он никогда не повторяет POST. Смена пояса не меняет серверное расписание.

Неизвестный исход сохраняет исходное намерение (тело/target/action/key) в памяти
текущей сессии. Создание замораживает форму до явного повтора прежнего POST;
для известного UUID доступны чтение текущего состояния и явный replay.
Подтверждённая квитанция с ошибкой чтения требует только GET. Старое состояние
не разрешает новую команду до успешного чтения. Черновик и неразрешённое
намерение переживают обычную навигацию; смена пользователя/выход очищают их.
Перезагрузка всего приложения не является долговечным хранилищем черновика.

Повторяемые UI/live-сценарии запускаются селекцией `--browser-only` штатных
smoke-профилей; команды, подготовка истории и очистка — в
[инструкции проверок](agent-test-profiles.md). Находки и их интерфейс остаются
отдельным пользовательским сценарием этапа.

## HTTP управления и диагностики

Все маршруты имеют префикс `/api/v1/detectors`. Они используют серверную
сессию; POST также требуют `X-CSRF-Token`. Миграция `0020_detector_commands`
добавляет `detectors.read` и `detectors.write` существующей системной роли
администратора. Гость этих прав не получает. Проверка прав выполняется при
каждом запросе, в том числе при повторе ранее принятой команды. API работает
без JRE/JPype/JAR и не вызывает модель, подготовку истории или worker по HTTP.

| Метод | Успех / DTO | Право |
| --- | --- | --- |
| `POST /detectors` | 201 `DetectorCommandReceipt` | `detectors.write` |
| `GET /detectors` | 200 `DetectorPage` | `detectors.read` |
| `GET /detectors/{id}` | 200 `DetectorCard` | `detectors.read` |
| `POST /detectors/{id}/start` | 202 `DetectorCommandReceipt` | `detectors.write` |
| `POST /detectors/{id}/stop` | 200 `DetectorCommandReceipt` | `detectors.write` |

Примеры JSON создания (подставьте UUID сохранённого активного агрегата):

```json
{"aggregate_id":"00000000-0000-4000-8000-000000000100","name":"Число событий"}
```

```json
{"aggregate_id":"00000000-0000-4000-8000-000000000100","name":"Ускоренный прогрев","accelerated":true,"step_seconds":60,"config":{"trees":100,"sample_size":256,"output_after":256,"seed":42}}
```

Во втором примере длительность агрегата должна быть не меньше 60 секунд.
Config можно передать частично; defaults: trees=100, sample_size=256,
output_after=256, seed=42. Размерность/ключи/порядок признаков определяет сервер.
Все целые строгие: bool, дроби и числовые строки не подходят; неизвестные поля
отвергаются, включая dimensions и параметры runtime. Имя обрезается по внешним
пробелам, затем имеет длину 1–200. В обычном режиме step отсутствует/null;
любое заданное значение отвергается. В ускоренном режиме нужен целый шаг
1..duration включительно. Принятые ограничения ModelConfig/ресурсной оценки
действуют и здесь. Создание не проверяет историю и оставляет execution=created.
Разные намерения создания одного агрегата дают независимые модели.

### Ключи, квитанции и потерянный ответ

Для каждого POST нужен `Idempotency-Key`: строка до 200 символов, содержащая
непробельный символ. На одно намерение клиент создаёт один ключ и сохраняет
его вместе с телом до подтверждения результата. Scope — actor UUID из сессии,
action и точное значение ключа. SHA-256 включает target и нормализованный body:
UUID в стандартном виде, обрезанное имя, все defaults config, явные
accelerated=false/step=null при их отсутствии. Порядок JSON-полей не влияет.
Ключи не обрезаются; их содержимое клиент не должен менять при повторе.

Квитанция имеет только `operation_id`, `detector_id`, `action` (create/start/stop),
`recorded_at` (aware UTC), `execution_generation` (целое) и `start_request_id`
(UUID/null). recorded_at — часы БД внутри транзакции регистрации, а не WAL-время
commit. Квитанция выдаётся после подтверждённого commit. Поколение и start UUID
относятся к принятой операции; текущую карточку нужно читать отдельным GET.

Тот же actor/action/key с теми же параметрами возвращает первоначальные
квитанцию и код успеха. Другой target/body даёт 409 idempotency_conflict.
Повтор старого start после stop не запускает модель; старый stop после нового
start не останавливает её. Старое создание остаётся повторяемым после start
или удаления агрегата. Повтор failed-start с прежним ключом не создаёт новую
попытку: новый ручной запуск требует нового ключа. Авторизация всегда текущая.

Command-key lock предшествует schedule-lock и detector-lock. Журнал с unique
constraint `(actor_id,action,key)` и предметное изменение фиксируются одним
сервисным UoW/commit. Репозитории не коммитят. CLI переиспользует те же внутренние
операции без HTTP-журнала и сохраняет прежние результаты. Разные команды
сериализуются lifecycle-блокировками; порядок определяется транзакциями,
а не временем клика. Незавершённого processing-key после rollback нет.
Журнал долговечный, без TTL; downgrade 0020 сознательно удаляет его и новые
права, сохраняя существующие модели, историю и остальные права/сессии.

При потере ответа нельзя автоматически создавать новый ключ или обещать,
что изменение не произошло. `503 detector_command_unconfirmed` содержит
`details.outcome=unknown` и `resolution=read_card_and_retry_same_key`.
Прочитайте GET карточки (для создания UUID может ещё быть неизвестен), затем
явно повторите исходный POST с теми же body/key для получения квитанции.
Потеря acknowledgement после commit не создаёт вторую модель/попытку.

Start/stop принимают пустое тело либо `{}`. `202 start` подтверждает короткую
команду, после которой GET показывает requested. Историю/модель готовит worker:
starting, затем running либо failed с причиной. GET и новые события не повторяют
неудавшийся запуск. При checkpoint start продолжает прежний курсор и backlog.
Start уже requested/starting/running не создаёт новую попытку. Stop не ждёт JVM;
повтор для stopped/terminated не повышает generation. Удалённый/архивный
агрегат не допускает новый start; удаление сохраняет доступную карточку истории.

### DTO чтения и nullable

`GET /detectors?limit=50&offset=0&aggregate_id=UUID&execution_state=running`
принимает limit=1..100, offset=0..100000; оба фильтра необязательны.
`DetectorPage` содержит items, total, limit, offset. Фильтры у total и items
одинаковы, сортировка created_at DESC, id DESC. Возвращаются все execution
states, включая created/stopped/failed/terminated, без исключения удалённой истории.

`DetectorSummary` содержит:

| Поля | Смысл / null |
| --- | --- |
| id/name, aggregate_id/aggregate_name/aggregate_state, source_id/source_name, created_at | UUID и сохранённые имена; aggregate_state=active/archived/deleted |
| execution_state, model_state | Раздельные перечисления из таблицы состояний; running не подтверждает health процесса |
| accelerated, accelerated_used | Настройка и provenance успешного ускоренного прогрева; used означает 🔥, без оценки опасности |
| reason | `{code,message,factor}` либо null; factor только известный ресурсный фактор/null, русское сообщение по allowlist |
| last_consumed, last_evaluated | Отдельные `DetectorWindow` либо null до соответствующего перехода |
| backlog, lag_seconds | Число наступивших окон от следующей границы / задержка этой границы; null до подтверждённой границы оценки, даже при начатом прогреве |

DetectorWindow: точный целый index, kind=evaluated/skipped, safe reason/null,
start_utc/end_utc/scheduled_at, aggregation_started_at/aggregation_completed_at,
evaluation_started_at/evaluation_completed_at (null для skipped), late.
Skipped продвигает только consumed, поэтому last_evaluated может относиться
к более раннему окну либо отсутствовать. Точные индексы/config не приводятся к float.

`DetectorCard` добавляет:

| Поля | Смысл / null |
| --- | --- |
| config | dimensions/trees/sample_size/output_after/seed, целые неизменяемые значения |
| feature_keys/feature_names | Полные закреплённые списки в серверном порядке |
| anchor_utc/duration_seconds/step_seconds | Неизменяемое UTC-расписание и шаг прогрева |
| revision/execution_generation/updated_at | Сохранённые revision, поколение и время |
| start_request_id/start_requested_at/execution_started_at/stopped_at | Текущая идентичность запроса и сохранённые даты; null до появления соответствующего факта |
| last_failure | `{reason,at,start_request_id}` либо null; прошлый failure не подменяет reason текущего запуска |
| warmup | Сводка активной сохранённой попытки либо null, когда попытки ещё нет или её batch пуст |
| recovery | Последний диапазон `{reason,first_index,last_index,started_at}` либо null; reason=crash_recovery/manual_resume |
| next_scheduled_at/waiting_for_point | Следующий срок с учётом intentional archive / отсутствие готовой точки; null до границы оценки |

Warmup содержит cutoff, step_seconds, required_windows (минимум ModelConfig),
planned_windows, materialized_windows, feature_valid_windows, eligible_windows,
ineligible_windows, materialized_coverage_seconds, eligible_coverage_seconds,
readiness (model state), reason/null и status=preparing/prepared/waiting/ready.
Числа и охват выводятся общей warmup_summary только из сохранённых фактов.
Пустая/ещё не материализованная попытка даёт null. Покрытие —
объединение интервалов; 512 перекрывающихся точек не означают 512 независимых
периодов или 100% готовности. Решение ready подтверждает worker, не процент.

Каждый GET читает один read-only REPEATABLE READ snapshot без изменения
revision, модели, курсора или ключей. Все даты — aware UTC; TimeZone БД не
меняет значения/расчёты. Список ограничен 100 строками. На строку читаются только
последние consumed/evaluated outcomes и одна следующая граница; карточка читает
один последний recovery и максимум 4096 лёгких записей metadata прогрева.
Имена в публичных ответах ограничены 200 символами. reason/message берутся из
фиксированного словаря; factor только из allowlist. Feature keys/config сохраняют
пределы принятых агрегаторного и модельного бюджетов, без нового порога числа признаков.
SQL-проекции не материализуют checkpoint, inputs-векторы, outcomes/findings payload,
события, runtime paths, PID, токены и полный массив recovery/archive истории.
Подготовка новых точек ради GET не выполняется.

Ошибки сохраняют envelope `{code,message,request_id,details}` и X-Request-ID:
401 — нет/истекла сессия; 403 — право/CSRF; 404 — неизвестный UUID ресурса;
422 — DTO/config/step/key; 409 — lifecycle или конфликт ключа; 503 — известная
временная недоступность/таймаут либо неизвестный commit с явным outcome.
Непредвиденные ошибки остаются безопасными 500 общего механизма.
Детали драйвера/SQL/исходный DetectionError в ответы не включаются.

Адресная проверка HTTP и команд:

```powershell
python -B scripts/agent_test_profiles.py backend-db --pytest tests/test_detector_api.py
```

Тесты используют настоящий app/router/session/CSRF и PostgreSQL, независимые
соединения, rollback/потерю acknowledgement, lifecycle/replay, bounded projections,
UTC, миграцию 0019→0020 и downgrade/upgrade. Полные проверки ревью выбирает
Этапный архитектор по влиянию; новый API не добавляет frontend-потребителя.

## Воспроизведение

Из корня проекта:

```powershell
python -B scripts/agent_test_profiles.py backend-db --rcf
```

Штатный runner создаёт изолированные PostgreSQL, сеть, том и Buildx builder,
проверяет resolved Compose, собирает `backend/Dockerfile.ml`, затем запускает
одноразовый `ml-verification` на той же сети. Внутри вызывается
`backend/scripts/run_tests.py --temporary --pytest tests/ml/rcf_gate.py tests/ml/rcf_service.py tests/test_detector_numeric.py tests/test_detector_rules.py tests/ml/rcf_worker.py tests/ml/rcf_migrations.py`.
`TEST_DATABASE_URL` указывает на `postgres:5432/diploma_test`; runner создаёт
отдельную временную БД и удаляет её. После проверки, в том числе неуспешной,
удаляются контейнеры, сеть, том, собственные образы, builder/cache и каталог
запуска; Docker-инвентарь сравнивается с исходным. Рабочая БД не используется.
`--rcf` допустим только с `backend-db`. Отсутствующая JVM/JAR — ошибка, не skip.

Gate расположен в `backend/tests/ml/rcf_gate.py`: имя намеренно не соответствует
автоматическому `test_*.py`, поэтому обычный host-набор не требует Java.
Явная Linux-селекция запускает его полностью. Каждый сегмент получает новый
Python/JVM-процесс; JVM не наследуется через fork и не перезапускается в процессе.
Heap ограничен 512 MiB, контейнер — 1 GiB/2 CPU, каждый дочерний вызов — 120 с,
весь verification-вызов — 600 с. Нет пользовательского Java HTTP-сервиса.

## Зависимости и состояние

Точные URL и SHA256 всех ML-артефактов находятся в
[`backend/ml/manifest.json`](../backend/ml/manifest.json).
Загрузка происходит только при сборке с проверкой SHA256. Runtime ничего
не загружает. Существующие backend-зависимости остаются под прежними
ограничениями `pyproject.toml`; фиксация ML не является полным lockfile backend.

| Компонент | Версия / назначение |
| --- | --- |
| AWS core, parkservices | 4.4.0, официальный тег `4.4.0-java` |
| AWS testutils | 4.4.0, опубликованная compile-зависимость parkservices |
| Eclipse Temurin JRE | 17.0.17+10, Linux amd64, digest в manifest и Dockerfile |
| JPype1 | 1.6.0, CPython 3.12 manylinux2014 x86_64 wheel |
| packaging | 25.0, зависимость JPype |
| Jackson annotations/core/databind | 2.18.3, структурированный JSON mapper-state |

AWS, JPype и Jackson поставляются под Apache-2.0; packaging — Apache-2.0
или BSD-2-Clause; OpenJDK — GPL-2.0 с Classpath Exception. Содержимое JAR,
включая встроенные license/notices, не перепаковывается; каталог `legal` JRE
сохраняется при копировании runtime. Lombok имеет provided scope, JUnit/Mockito
и протокольные сериализаторы из test scope не нужны runtime.

Probe вызывает именно `ThresholdedRandomCutForestMapper`, включая forest,
preprocessor, predictor-corrector и last descriptor. Jackson читает конкретный
класс state без default typing; pickle и Java object serialization не используются.
Продуктовый адаптер дополнительно проверяет envelope с SHA256, runtime identity,
config/settings, порядком ключей, rule hash, affine-параметрами, preceding history,
update count и подтверждённой границей входа. Проверка предшествует загрузке
Java-state. Повреждение/несовместимость дают отказ без создания нового леса.
Checksum использует канонические JSON-числа: целочисленная экспоненциальная
запись и её развёрнутая JSONB-форма имеют один digest. После чтения сохранённые
ML-координаты возвращаются в float перед сравнением float32-представлений;
исходные точные признаки от этого преобразования не зависят.

Конфигурация probe: STANDARD, shingleSize=1, TransformMethod.NONE,
100 деревьев, sampleSize=256, outputAfter=256, seed=42,
timeDecay=1/256, последовательное исполнение. Остальные настройки остаются
defaults официального 4.4.0. Продуктовый адаптер закрепляет дополнительные
настройки через ModelConfig.settings (см. ниже). Тест обрабатывает 512 входов прогрева и 160
последующих входов в одной и трёх координатах, с повторениями и контролируемыми
отклонениями; три восстановления делят продолжение на 40/60/60 входов.
Отдельный тест использует полностью постоянные векторы.

Сравниваются score, grade, confidence, наличие и значения expected/attribution,
счётчики обновлений и конечное state. Допуск вычислительных чисел:
rtol=1e-6, atol=1e-8. Положительный grade должен иметь официальный attribution.
Probe не реализует findings или медианный fallback; успешный probe сам по себе
не завершает требования к объяснению и предметному пути TASK-34.

## Числовая подготовка

Независимый модуль `app/detection/numeric.py` использует фиксированное affine
преобразование: center=min, scale=max-min, scale=1 для постоянной координаты.
Вычитание и деление выполняются в Decimal до binary64/float32, исходные
значения не изменяются. Проверяется фактическое float32-представление:
конечность, запрет ненулевого значения, превратившегося в ноль, и
`abs(z-y)<=1e-6*max(1,abs(y))`. Различные точные значения не должны совпасть
после преобразования. В истории fit проверяется вся попытка; при рабочем
вызове caller обязан передать предыдущие sampleSize принятых входов.
Bounded history сохраняется в checkpoint и восстанавливается на каждом переходе.

Примеры: соседние `2**60`, `2**60+1`, `2**60+2` преобразуются в 0/0.5/1
без потери разницы. История 0/`2**60`/`2**60+1` отклоняется как
`float32_collision`: последние два значения сливаются. При center=0/scale=1
`1e-100` даёт `float32_underflow`, `1e100` — `float32_overflow`; значение 2
допускается без клиппирования. Отдельная постоянная координата сохраняется.
Полностью постоянная история даёт
`constant_history`, а не обещание качества обнаружения.

## Конфигурация, качество и прогрев

Config фиксирует dimension, trees, sampleSize, outputAfter и seed; sampleSize>=2,
outputAfter>=sampleSize. Ресурсная оценка dimensions*trees*sampleSize*256 байт
ограничена 256 MiB с запасом для JVM/Python. Фиксированного порога в пять нет.
ModelConfig.settings закрепляет FLOAT_32, internal shingling, unit weights,
centerOfMass=false, sequence indexes=false, boundingBoxCacheFraction=1,
initialAcceptFraction=1, anomalyRate=.01, autoAdjust=false, zFactor=3,
scoreDifferencing=.5, alertOnce=false, absoluteThreshold=.8,
initialThreshold=1.5, startNormalization=min(10,outputAfter-1),
stopNormalization=2147483647, transformDecay=1/sampleSize,
EXPECTED_INVERSE_DEPTH. Settings проверяются при восстановлении;
внутренние состояния порогов/predictor-corrector сохраняет официальный mapper.

Вектор содержит весь закреплённый список ключей в исходном порядке. Bool,
null, nonfinite, invalid_count и reason отвергают весь вектор. Только complete
допускает обучение/оценку; unknown/interrupted/unverified не повышаются до complete.
Достоверно пустое окно с пригодным count=0 допускается; статистический null
не заменяется нулём.

Cutoff — последняя завершённая UTC-граница до начала попытки.
N_model=max(2*sampleSize,outputAfter+1), по умолчанию 512. Обычный шаг=duration;
ускоренный — целые секунды 0<step<=duration. Ускоренный план имеет
max(N_model,1+ceil(7*duration/step)) окон и охват >=8*duration; перекрытия
не объявляются независимыми периодами. План не пересекает anchor и не зависит
от TimeZone. Недостаточная история даёт waiting, превышение бюджета — причину
без усечения. Бюджеты: 4096 окон, 30 с подготовки всего batch,
64 MiB inputs и 64 MiB checkpoint.

Обычная попытка может целиком использовать сохранённую working-последовательность,
если все её окна имеют `complete`. Наличие всех строк с `unknown/interrupted`
не подтверждает пригодность batch: тогда он целиком пересчитывается по барьеру
новой попытки. Закрытые working points не меняются; отчёт, полученный после их
барьера, может подтвердить только новый исторический снимок, если он получен до
барьера этой попытки. Реальный gap и поздний относительно неё отчёт сохраняют `unknown`.
Иначе весь batch, включая перекрывающиеся окна, рассчитывается через агрегатный
компилятор и сохранённые coverage evidence. Барьер закреплён за попыткой:
`uuid5(attempt_id, "warmup")`; чужие более новые барьеры его не заменяют.
Короткие чтения с rollback между опросами ждут материализации этого барьера
и свежего post-barrier checkpoint без lifecycle-lock и открытого RR.
Ожидание, SQL/lock wait и расчёт входят в общий бюджет 30 с. Затем новый
read-only REPEATABLE READ UoW заново проверяет барьер и все доказательства,
читает весь batch из одного снимка. Свежесть checkpoint — прежние 10 с
от одной опорной даты БД этого снимка, не плавающего времени каждого окна.
При гонке готовности выбрасывается весь batch; повтор ограничен тем же
deadline/attempt/cutoff. Истечение бюджета завершает старт с resource_limit
и требует нового ручного действия. Старые точки
не подмешиваются к пересчёту. Legacy без доказательств остаётся непригодной.
Короткий write-UoW повторно проверяет lifecycle/revision/active_attempt_id,
сохраняет план и весь batch. Inputs содержат точные FeatureResult, quality/evidence,
границы/основание, affine и ML-вход. DB-trigger запрещает менять inputs после
подготовки. Прогрев читает только snapshots: удаление событий не меняет обучение.

Во время прогрева findings не создаются. Успех требует пригодной изменчивой
истории и готовности официального леса. Evaluation начинается с end>cutoff;
consumed cursor не подделывается. Accelerated_used ставится только при успешном
ускоренном прогреве и переносится в находки.

## Состояния и транзакции

State читает детектор и активную попытку из одного read-only snapshot. Числа
пригодных/непригодных окон и фактический охват объединения интервалов вычисляются
только из сохранённых неизменяемых inputs; новые события и их удаление на эти
показатели не влияют. Раздельно видны числовая пригодность и решение готовности.
Outcome также сохраняет фактические моменты начала/завершения своего перехода.

Waiting означает отсутствие пригодного прогрева с причиной, warming — подготовку
входов/прогрев, ready — подтверждённый checkpoint, stopped — окончательное
удаление агрегата. Ошибка модели/снимка возвращается CLI без продвижения
курсора; отдельный короткий UoW фиксирует error с повторной проверкой revision,
не затирая более новый успех или удаление. Новая попытка имеет
собственные UUID/cutoff/inputs hash и не переписывает прежнюю.

Порядок блокировок: schedule агрегата → Source только для барьера → Detector
по UUID. Ожидание SQL-блокировки ограничено 5 с, statement timeout — 30 с.
Долгий ML выполняется без lifecycle-lock в новом Python/JVM-процессе
на каждый кандидат (120 с, heap 512 MiB). Финальная транзакция сверяет lifecycle,
config/revision, active attempt и consumed cursor. Одна mutation сохраняет
checkpoint, consumed/evaluated cursor, outcome и optional finding. Unique
по detector/input и CAS исключают дубль. DB-trigger защищает определение
детектора и увеличение revision. Репозитории не выполняют commit; один UoW
разделяется с агрегатным lifecycle. Autoflush выключен внутри mutation, чтобы
revision и данные дошли до БД одним UPDATE.

Skip продвигает consumed; evaluated и лес сохраняются. Неизвестная дыра
working indices даёт ожидание; прыжок разрешён только по сохранённому intentional
archive range. Архив запрещает переходы, resume сохраняет модель. Удаление
атомарно останавливает всех связанных детекторов, сохраняя историю.
Ошибка/rollback/конфликт/неизвестный исход commit отбрасывают кандидата;
ручной повтор начинает с нового чтения БД и подтверждённого checkpoint.

Raw score, anomaly grade и data confidence сохраняются отдельно. Finding
создаётся только при grade>0 после прогрева. Confidence не является вероятностью
инцидента. Expected берётся у AWS с обратным affine; fallback — медиана
предшествующих sampleSize пригодных входов, expected_origin=preceding_median.
Attribution всегда официальный score-attribution, не процент/значение признака;
affine к нему не применяется. Finding сохраняет source UUID, имена, точные факты,
окно, ECS @timestamp range; пустое окно не получает выдуманный диапазон.
`late` определяется долговечными recovery-диапазонами, описанными ниже.

## Административная CLI и проверки

В ML-образе с административно подготовленным DATABASE_URL в окружении:

```bash
python -m app.detector_cli create AGGREGATE_UUID --actor ADMIN_UUID --name RCF
python -m app.detector_cli create AGGREGATE_UUID --actor ADMIN_UUID --name RCF --accelerated --step 60
python -m app.detector_cli start DETECTOR_UUID
python -m app.detector_cli stop DETECTOR_UUID
python -m app.detector_cli state DETECTOR_UUID
```

Start подтверждает постановку запроса, а не завершение прогрева. Прежние
prepare-warmup/warmup/advance-one возвращают `managed_worker_required` с
указанием start/state. Сервисные операции требуют проверенного ExecutionOwner.
Пароли не передаются аргументами.
При неизвестном исходе сначала прочитайте state; автоматического повторного
обучения нет. HTTP использует отдельные права и долговечные квитанции, описанные ниже.

Полная регрессия прежнего backend: `python -B scripts/agent_test_profiles.py kafka-integration`.
ML/DB и обратимость миграции: `python -B scripts/agent_test_profiles.py backend-db --rcf`.
Совместимость обычной поставки: `smoke-development`, `smoke-production`,
`end-to-end --aggregate-only` того же runner. ML-набор отдельно проверяет
mapper replay, сбой до commit/потерю ack после commit, stale candidates независимых
sessions, lifecycle, signed coverage через настоящий агрегатный сервис и один
REPEATABLE READ batch при конкурентном удалении событий. Времена и данные
синтетические; эти проверки не являются нагрузочным тестом.

## Разрешение исполнения

Готовность модели (`waiting/warming/ready/error`, окончательный `stopped` после
удаления агрегата) отделена от `execution_state`:

| Значение | Смысл |
| --- | --- |
| `created` | Создан без разрешения; worker не готовит историю и не обучает |
| `requested` | Долговечный запрос пользователя, ещё не принятый worker |
| `starting` | Worker принял запрос; готовит модель или проверяет checkpoint |
| `running` | Запуск подтверждён атомарно с checkpoint |
| `stopped` | Ручная остановка; checkpoint, курсор и история сохранены |
| `failed` | Подтверждённый неуспех/прерванный запуск; нужен ручной start |
| `terminated` | Агрегат удалён; start запрещён окончательно |

Миграция 0019 сохраняет старые checkpoints, inputs, outcomes и findings, но не
выдаёт прежним детекторам разрешение. Start фиксирует UUID запроса,
start_requested_at по часам БД и новое поколение. Повтор для
requested/starting/running сохраняет идентичность. Долгих SQL/ML в start нет.
Stop повышает поколение и revision до возврата, отменяя право прежних кандидатов
на запись, включая ошибку/inputs. Он не ждёт JVM и не останавливает агрегат или
соседние детекторы. Повтор stop безопасен.

Неудачный прогрев не повторяется после polling, новых данных или рестарта.
Pending WAL-барьер — ожидание той же попытки, а не новый cutoff. Начатый
первоначальный запуск после потери координатора получает start_interrupted и
ждёт ручного start. Ещё не принятый requested допустимо выполнить после рестарта.
Потеря acknowledgement разрешается чтением подтверждённой БД.

При checkpoint start проверяет envelope и продолжает прежние модель, cutoff и
курсор; повреждение даёт checkpoint_invalid, новый лес не создаётся. Все
накопившиеся доступные точки обрабатываются по порядку. Ручная пауза не создаёт
пропущенный диапазон агрегата. Неизвестные дыры ждут точку; только признанный
intentional_archive позволяет обойти отсутствующие индексы. Новый start
архивного/недоступного агрегата отклоняется. Уже running может ждать его возврата.
Если агрегат архивирован во время оценки, кандидат отбрасывается без ошибки
модели и без отзыва разрешения; после resume работа продолжается с checkpoint.

## Владение и recovery

Один координатор на БД удерживает session advisory lock `(1380205362, 1)` на
отдельном autocommit-соединении. Standby ждёт без recovery. Запись
logs.detector_coordinator хранит монотонное поколение, UUID владельца, PID
lock-сессии и время БД. Получение поколения предшествует recovery/вычислениям.
Общая composition root собирает репозиторий, UnitOfWork и модельную фабрику.
Сервисный сценарий проверяет физический lock и фиксирует поколение отдельной
короткой транзакцией до recovery; при ошибке выполняется rollback и переизбрание.
Публикация takeover и финальный save сериализованы блокировкой этой записи.
Порядок: coordinator → schedule → Source только для барьера → Detector.
Start/stop/delete используют schedule → Detector и не ждут ML.

Перед mutation и непосредственно перед commit проверяются session lock в
pg_locks, UUID/PID/поколение координатора, разрешение/поколение запроса,
revision/config hash/active attempt/входной курсор. Coordinator row удерживается
до commit, сериализуя финальную транзакцию и публикацию нового владения.
Отложенные constraint triggers проверяют LOCAL fencing token на commit:
потеря физического lock-соединения после flush также откатывает транзакцию.
После потери lock новое планирование прекращается, незавершённый JVM-кандидат
отменяется. Rollback/неизвестный commit требуют нового чтения БД; изменённая
JVM-модель не переиспользуется.

До восполнения running-моделей сохраняются detector_recoveries: первый и
последний уже наступившие непотреблённые индексы, время БД, поколение и причина
crash_recovery. Граница выводится из UTC anchor/duration, evaluation_start,
consumed и scheduled_at, без поиска materialized points. Повторный takeover не
стирает диапазоны; точка, появившаяся позже, сохраняет происхождение. Ручное
возобновление сохраняет manual_resume. Outcome и находка содержат применённые
диапазоны; finding получает late=true. Обычная задержка polling late не создаёт.
При сроках 10:00/10:05 восстановление в 10:01 уже помечает находку за 10:00.

State читает один REPEATABLE READ snapshot: модель, execution state, запрос,
безопасную причину, inputs/summary/🔥, recovery, последнее consumed/evaluated окно,
последнюю неудачу с UTC-временем и идентичностью её запроса (она сохраняется при
новом start/stop), фактические моменты агрегирования/оценки, следующий scheduled_at, backlog/lag и
ожидание точки. Отсутствие первой оценки даёт null. Стабильные коды и русские
объяснения находятся в detection/execution.py: history_insufficient,
history_quality, resource_limit, checkpoint_invalid, start_interrupted,
working_point_pending, aggregate_inactive, aggregate_deleted, manual_stop,
temporary_unavailable. Неподтверждённый отказ БД не объявляется сохранённым.
Для ресурсной ошибки last_failure.details.factor сохраняет только известный
фактор бюджета (например model_seconds, checkpoint_bytes, operator_cost или
total_memory); SQL timeout подготовки даёт preparation_seconds. Подтверждённый
отказ завершает попытку и требует нового ручного start; произвольный
текст драйвера в структурированные детали не переносится.

## Поставка и проверки worker

Detector-worker использует принятый Linux ML-target без отдельного Java HTTP
сервиса. Один кандидат, контейнер 1 GiB/2 CPU, JVM heap 512 MiB, ML timeout
120 с, подготовка 30 с, shutdown grace 40 с. Образ содержит проверенные JRE/JAR;
обычный API Java не получает. Worker не публикует порты и получает только URL
PostgreSQL (production: DATABASE_URL_FILE), без Kafka/OIDC/agent secrets и
Docker socket. Он ждёт миграций и здорового WAL-декодера.

SIGTERM/SIGINT прекращают планирование, освобождают lock и отменяют дочерний
Python/JVM-процесс; SQL ограничен timeout. Healthcheck app.detector_health читает
heartbeat, фазу/deadline и проверяет БД, не запускает JVM. Допустимый долгий шаг
не считается зависанием; просроченная фаза, умерший цикл и недоступная БД дают
unhealthy. Standby/отсутствие моделей допустимы. Выбор по UUID с циклическим
продолжением и ограниченной выборкой обслуживает модели после первых N.

Полный backend-db --rcf включает tests/ml/rcf_worker.py (CLI, health/deadline,
отмена процесса) и расширенные rcf_service.py (управление, fencing,
standby/takeover, recovery/late), прежние модельные и миграционные тесты.
Адресная проверка изменённой поставки:

```powershell
python -B scripts/agent_test_profiles.py smoke-development --detector-only
python -B scripts/agent_test_profiles.py smoke-production --detector-only
```

Профиль доступен Исполнителю и Этапному архитектору и может покрывать smoke
затронутой поставки worker перед коммитом. UI/OIDC/TLS-agent не добавляются,
если их пути не затронуты; ограничения запуска лишних сервисов описаны в
[инструкции профилей](agent-test-profiles.md). Полные smoke и собственный E2E
выбираются по влиянию либо при полной приёмке этапа; они также включают сценарий: два источника, synthetic saved history,
обычный/ускоренный режим (здесь шаг=duration; перекрытие проверяет ML/DB),
реальные WAL/aggregate/RCF worker, обычная/аномальная последовательность,
сохранённое объяснение и завершение до следующей 5-секундной границы.
API временно остановлен только на время синтетического обработчика/probe,
чтобы реальный Kafka-consumer не присваивал его тестовые topic identities.
Источники включены во время этого входа; после сценария они выключаются и API
возвращается. Признаки остановки не удаляются и не обходятся.
Синтетические доказательства синхронизируются тестовой блокировкой первой
проверки покрытия до готовности реальных WAL-барьеров и свежих checkpoint;
правила полноты не меняются. Блокировка снята до RCF и входит в замер от
scheduled_at до подтверждённой видимости обоих outcomes в БД.
Прежний секундный aggregate smoke сохраняет порог. Running up и down/up проверяют
сохранность ручной остановки, checkpoint/inputs/history. Предел произвольной
нагрузки этим сценарием не измеряется.
