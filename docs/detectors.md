# Серверное ядро детекторов AWS RCF

Сервис создаёт конфигурацию, сохраняет входы прогрева, обрабатывает одну
следующую working point и атомарно сохраняет checkpoint, курсор, outcome и
находку. Постоянные detector-worker, HTTP и UI относятся к последующим задачам.
Техническая готовность модели не гарантирует качество обнаружения.

## Воспроизведение

Из корня проекта:

```powershell
python -B scripts/agent_test_profiles.py backend-db --rcf
```

Штатный runner создаёт изолированные PostgreSQL, сеть, том и Buildx builder,
проверяет resolved Compose, собирает `backend/Dockerfile.ml`, затем запускает
одноразовый `ml-verification` на той же сети. Внутри вызывается
`backend/scripts/run_tests.py --temporary --pytest tests/ml/rcf_gate.py tests/ml/rcf_service.py tests/test_detector_numeric.py tests/test_detector_rules.py tests/ml/rcf_migrations.py`.
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

Обычная попытка может целиком использовать сохранённую working-последовательность.
Иначе весь batch, включая перекрывающиеся окна, рассчитывается через агрегатный
компилятор и сохранённые coverage evidence. После готовности WAL-барьера отдельный
read-only REPEATABLE READ UoW читает весь batch из одного снимка. Старые точки
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
Late=false: recovery-маркировка относится к следующей задаче.

## Административная CLI и проверки

В ML-образе с административно подготовленным DATABASE_URL в окружении:

```bash
python -m app.detector_cli create AGGREGATE_UUID --actor ADMIN_UUID --name RCF
python -m app.detector_cli create AGGREGATE_UUID --actor ADMIN_UUID --name RCF --accelerated --step 60
python -m app.detector_cli prepare-warmup DETECTOR_UUID
python -m app.detector_cli warmup DETECTOR_UUID
python -m app.detector_cli advance-one DETECTOR_UUID
python -m app.detector_cli state DETECTOR_UUID
```

Команды bounded, без daemon-loop. Пароли не передаются аргументами.
При неизвестном исходе сначала прочитайте state; автоматического повторного
обучения нет. Клиентский HTTP/API и права интерфейса здесь не вводятся.

Полная регрессия прежнего backend: `python -B scripts/agent_test_profiles.py kafka-integration`.
ML/DB и обратимость миграции: `python -B scripts/agent_test_profiles.py backend-db --rcf`.
Совместимость обычной поставки: `smoke-development`, `smoke-production`,
`end-to-end --aggregate-only` того же runner. ML-набор отдельно проверяет
mapper replay, сбой до commit/потерю ack после commit, stale candidates независимых
sessions, lifecycle, signed coverage через настоящий агрегатный сервис и один
REPEATABLE READ batch при конкурентном удалении событий. Времена и данные
синтетические; эти проверки не являются нагрузочным тестом.
