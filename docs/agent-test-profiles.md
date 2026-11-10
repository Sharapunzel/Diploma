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
| Дополнительные live/UI-сценарии агрегатов | `python -B scripts/agent_test_profiles.py end-to-end --aggregate-only` |
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

Development содержит **17 specs**: полный `auth.spec.ts`, `events.spec.ts`, `resources.spec.ts`, `resources.confirm.spec.ts`, `resources.policy.spec.ts`, `resources.targets.spec.ts`, `normalizers.spec.ts`, `normalizers.ai.spec.ts`, `normalizers.blocks.spec.ts`, `normalizers.feedback.spec.ts`, `normalizers.fix.spec.ts`, `normalizers.source_debug.spec.ts`, `normalizers.tree.spec.ts`, `aggregates.live.spec.ts`, `aggregates.ui.spec.ts`, `detectors.ui.spec.ts`, `detectors.live.spec.ts`. Семейства `resources.*` и `normalizers.*` включаются автоматически. Production содержит **2 specs**: `detectors.live.spec.ts` и `proxy.session.spec.ts`. Другие специальные live-пути не включаются discovery всего каталога.

`--browser-spec` принимает одно или несколько точных имён только своего набора и только вместе с `--browser-only`. Другой профиль, неизвестное имя, дубликаты и сочетания с `--detector-only`, `--aggregate-only`, `--rcf`, `--wal`, `--pytest` отклоняются до создания каталога и Docker-ресурсов. Обычные smoke-команды сохраняют прежние проверки. Браузерная ветка не выполняет общий aggregate/detector runtime smoke, WAL gate, lifecycle up/down/up, TLS-агента или внешний E2E.

Браузерная ветка использует те же Compose-файлы, изолированные project/порты/тома и отдельный Buildx. API получает `KAFKA_CONSUMERS_ENABLED=false`: независимый синтетический обработчик/probe пишет через `ProcessingRepository.store_event` и подписывает наблюдаемые интервалы; Kafka consumer не конкурирует за тестовые идентичности. WAL decoder и оба worker настоящие. История формируется по реальным часам с двухсекундными окнами и изменчивым count; подготовка ждёт 16 последних подтверждённо полных рабочих окон, а не предполагает готовность по задержке. Ускоренный live-прогрев использует шаг 1 секунду. Готовность и пригодность определяет сервер; complete-векторы и время доступности вручную не назначаются. Trees=20/sample=8 относятся только к синтетическим детекторам теста, defaults продукта не меняются. Source/probe/пользователи/история существуют только в своей `diploma_test`.

Подготовка агрегатов переиспользует начальный writer/WAL-шаг `aggregate_runtime_smoke(prepare_only=True)` и прежний `AGGREGATE_BROWSER_CONFIG`, без полного runtime smoke. Отсутствующая обязательная fixture — ошибка профиля. Development auth подготавливает настоящий управляемый OIDC и оставляет его активным для полного auth-набора. Группы выполняются последовательно с `--workers=1`; HMR восстанавливает исходный CSS в finally, runner дополнительно сверяет точные байты. Production выполняет новые detector-маршруты и session/reload через HTTPS в закреплённом временном Chromium с импортированным тестовым CA, без исключений TLS. HMR/mock-набор в production не запускаются.

Фоновая detector fixture публикует атомарный статус процесса/фазы/последовательности.
Профиль проверяет её PID, продвижение и подтверждённую материализацию настоящих
event/barrier-маркеров при подготовке,
установке browser-зависимостей и выполнении тестов. Завершение/зависание fixture
или потеря capture прерывают собственную команду с безопасными кодами диагностики.
Лёгкий status-probe использует только стандартную библиотеку: он не запускает
новый ORM/пул БД на каждый опрос. Writer обновляет отметку capture только после
материализации своих маркеров, с прежним двухсекундным пределом ожидания.

Перед очисткой detector browser выполняется ограниченная read-only диагностика
истории: UUID попытки/cutoff, происхождение входов, барьеры и время закрытия
непригодных working points, интервалы/sequence/received_at относящихся к ним
аттестаций. Она показывает до 12 попыток, до трёх проблемных окон каждой и до
восьми отчётов окна; сырые события, подписи и credentials не выводятся.

Установка зависимостей, свежая готовность и тесты делят общий бюджет 1200 с.
После установки зависимостей и непосредственно перед тестами профиль повторно
ждёт 16 последних полных рабочих окон, включая новое окно после завершения
browser-подготовки; прежняя готовность не заменяет эту проверку. Это не повтор
неудавшегося start: все команды детектора остаются явными действиями теста.

Селекция, обязательность fixtures, свежая готовность и cleanup браузерного контейнера проверяются `scripts/test_browser_test_scenarios.py`, `scripts/test_browser_profile_health.py` и существующим `scripts/test_agent_test_profiles_cleanup.py`. Все файлы, npm-кэш, вывод Playwright и credentials находятся внутри одного каталога прогона; общий finally удаляет Compose, образы, builder/cache и каталог, затем сверяет Docker inventory. Это стандартный браузерный набор frontend, а не полная регрессия всего приложения.

`browser_detector_fixture.working_closure_gate` ограниченно координирует только
одноразовое чтение evidence при закрытии рабочей точки: блокировка
`source_interruptions` освобождается транзакцией после реальных отчётов,
материализации барьеров и checkpoint, включая исключения. Она не исправляет
готовность исторического batch: сервисные DB-регрессии проверяют поздний
checkpoint и закреплённый барьер без этой блокировки. Адресный серверный набор:
`backend-db --rcf --pytest tests/ml/rcf_service.py` и
`backend-db --pytest tests/test_availability_materializer.py`.

В production тот же тестовый CA передаётся Node-клиенту `page.request`
через `NODE_EXTRA_CA_CERTS=/tmp/proxy-ca.pem` внутри временного браузерного
контейнера. Проверка TLS остаётся включённой и у Chromium, и у API-клиента.

Прямой запуск Playwright требует уже подготовленного изолированного стенда; предпочтительны команды `--browser-only` выше. Конфигурация Playwright сама не запускает web server. Запуск тестов по порту уже завершающегося smoke-профиля не является воспроизводимой проверкой.

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

`end-to-end --aggregate-only` использует тот же development Compose, но выполняет только серверный сценарий агрегатов через proxy. Он проверяет два источника, реальные pgoutput/worker, отдельный исторический барьер, обработку секундных окон, восстановление после остановки, два worker с разными TZ, смену пояса БД и архивирование/удаление. Сценарий также входит в обычные `end-to-end`, `smoke-development` и `smoke-production`. Это ограниченный замер своевременности, а не предельный нагрузочный тест.

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
