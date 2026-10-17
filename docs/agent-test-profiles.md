# Проверочные профили агентов

Все профили запускаются из корня репозитория одной командой:

```powershell
python scripts/agent_test_profiles.py frontend
python scripts/agent_test_profiles.py backend-db
python scripts/agent_test_profiles.py kafka-integration
python scripts/agent_test_profiles.py end-to-end
python scripts/agent_test_profiles.py smoke-development
python scripts/agent_test_profiles.py smoke-production
```

Нужны Python-зависимости backend, Node-зависимости frontend, Docker Compose и доступный Chromium для Playwright. Используются образы из проектных Compose-файлов: PostgreSQL 18, Kafka 4.2.0, Fluent Bit 4.2.3, Nginx 1.30.5 Alpine. Скрипт создаёт уникальный Compose project `diploma_t21_*`, порты на loopback, синтетические секреты и БД `diploma_test` только в собственном временном томе. Все временные файлы находятся в `.harness/runs/<run-id>/`. Даже при ошибке скрипт останавливает и удаляет только ресурсы своего проекта и проверяет остатки. Если Docker недоступен до начала очистки, сохраните путь из ошибки и выполните точечную очистку этого проекта после восстановления Docker; не используйте общую очистку Docker.

`frontend` запускает typecheck, lint, format:check, тесты и сборку. `backend-db` поднимает тестовую PostgreSQL и выполняет профильные backend-тесты. `kafka-integration` поднимает PostgreSQL и Kafka с SCRAM/ACL, сверяет metadata внешнего и внутреннего listener и запускает полный backend-набор через `backend/scripts/run_tests.py --temporary`. Для адреса и credentials тестов скрипт задаёт `KAFKA_TEST_BOOTSTRAP_SERVERS`, `KAFKA_TEST_SECURITY_PROTOCOL`, `KAFKA_TEST_READER_PASSWORD_FILE`, `KAFKA_TEST_ADMIN_PASSWORD_FILE`, `KAFKA_TEST_DEV_ALLOWED_BOOTSTRAP_SERVERS` и `TEST_DATABASE_URL`; можно указать отдельные узлы pytest через `--pytest`.

`smoke-development` собирает и запускает PostgreSQL, Kafka, migration, provisioner, API, frontend и proxy, затем проверяет `/agents` через proxy. `smoke-production` запускает текущий production-состав с одноразовыми сертификатами CA, Kafka и HTTPS и проверяет TLS/hostname и API-маршрут. По решению человека от 17.10.2026 включение React в production-поставку остаётся завершающей задачей. Дополнительно production smoke через API регистрирует агента, получает транспорт и одноразовый секрет, собирает конфиг тем же frontend-шаблоном и проверяет доставку записи Fluent Bit по внешнему TLS listener с выданным CA и проверкой hostname. Поэтому `smoke-production` подтверждает серверный/TLS путь и агентский конфиг, не весь пользовательский интерфейс. Оба smoke-профиля сверяют Kafka cluster ID с хоста, из API и тестового агента. `end-to-end` использует development UI: регистрирует агента и два топика, однократно получает конфигурацию, отправляет тестовую запись временным Fluent Bit, проверяет Kafka, включает source с нормализатором и сверяет PostgreSQL и страницу «События». Fluent Bit использует неизменённый скачанный конфиг и внешний Kafka listener; тестовый контейнер разделяет сетевое пространство брокера, чтобы тот же loopback-адрес и динамический внешний порт были доступны без публикации тестового порта наружу. Проверка установки на отдельном хосте остаётся вне этого теста.

В отчёте о каждом прогоне указывайте профиль, команду, выведенный скриптом Compose project, фактические свободные порты, результат и отсутствие оставшихся ресурсов. Пароли и содержимое конфигурации Fluent Bit в отчёт не переносите.
