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

`smoke-development` собирает и запускает PostgreSQL, Kafka, migration, provisioner, API, frontend и proxy, затем проверяет `/agents` через proxy. `end-to-end` использует development UI: регистрирует агента и два топика, однократно получает конфигурацию, отправляет тестовую запись временным Fluent Bit, проверяет Kafka, включает source с нормализатором и сверяет PostgreSQL и страницу «События». Перед UI-сценарием профиль также извлекает встроенный Lua Code filter `systemd` непосредственно из генератора конфига и запускает его на синтетической записи с Fluent Bit 4.2.3. Вход задан с timestamp `2000-01-01T00:00:00Z`; проверяется точный конверт `{timestamp, log}`, сохранение старого времени в строке `log` и timestamp времени обработки. Проверка использует синтетический dummy input, не читает живой journald. Fluent Bit использует неизменённый скачанный конфиг и внешний Kafka listener; тестовый контейнер разделяет сетевое пространство брокера, чтобы тот же loopback-адрес и динамический внешний порт были доступны без публикации тестового порта наружу. Проверка установки на отдельном хосте и живое чтение Windows Event Log остаются вне этого теста.

В отчёте о каждом прогоне указывайте профиль, команду, выведенный скриптом Compose project, фактические свободные порты, результат и отсутствие оставшихся ресурсов. Пароли и содержимое конфигурации Fluent Bit в отчёт не переносите.

`smoke-production` собирает production proxy с React и запускает PostgreSQL, Kafka, migration, provisioner и API на изолированном Compose-проекте. Он проверяет HTTPS с доверенным тестовым CA и именем `localhost`, API readiness, прямые UI-маршруты, JS/CSS, отказы для неизвестного API, отсутствующего asset и закрытой документации. Дополнительно проверяется доставка события тестовым Fluent Bit через внешний TLS listener. Интерактивные вход, сессия и выход проверяются отдельно в браузере через тот же HTTPS proxy.

`smoke-development` также создаёт синтетического локального администратора и выполняет тот же браузерный сценарий входа, обновления страницы и выхода через HTTP proxy разработки.

Для production браузерной части профиль собирает временный образ на основе `node:20.19.5-bookworm-slim` и запускает его в сетевом пространстве proxy. По зафиксированному `package-lock.json` устанавливается Playwright 1.63.0 и только Chromium headless shell соответствующей версии через `playwright install --only-shell chromium`; полный Chromium через `apk` не устанавливается. При сборке добавляются системные библиотеки headless Chromium и `libnss3-tools` для импорта тестового CA. Сборка ограничена 600 секундами, запуск браузерного сценария — 180 секундами. Тестовый CA монтируется только в этот контейнер, добавляется в его системный trust store и проверяется Chromium без TLS-исключений. Контейнер и образ браузера временные, результаты находятся в каталоге текущего прогона; доверие хоста не меняется. Проверяются `Secure`, `HttpOnly`, `SameSite` session cookie, same-origin HTTPS выход и наличие CSRF-заголовка; значения cookie и токена в вывод не попадают.
