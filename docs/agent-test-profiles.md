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

Временный BuildKit builder и шаги сборки Kafka используют сеть хоста только при сборке образов, чтобы разрешение имён пакетных репозиториев совпадало с обычным Docker-контейнером. Сервисы Compose продолжают работать в изолированной сети проекта; builder удаляется вместе со стендом.

`frontend` запускает typecheck, lint, format:check, тесты и сборку. `backend-db` поднимает тестовую PostgreSQL и выполняет профильные backend-тесты. `kafka-integration` поднимает PostgreSQL и Kafka с SCRAM/ACL, сверяет metadata внешнего и внутреннего listener и запускает полный backend-набор через `backend/scripts/run_tests.py --temporary`. Для адреса и credentials тестов скрипт задаёт `KAFKA_TEST_BOOTSTRAP_SERVERS`, `KAFKA_TEST_SECURITY_PROTOCOL`, `KAFKA_TEST_READER_PASSWORD_FILE`, `KAFKA_TEST_ADMIN_PASSWORD_FILE`, `KAFKA_TEST_DEV_ALLOWED_BOOTSTRAP_SERVERS` и `TEST_DATABASE_URL`; можно указать отдельные узлы pytest через `--pytest`.

`smoke-development` собирает и запускает PostgreSQL, Kafka, migration, provisioner, API, frontend и proxy, затем проверяет `/agents` через proxy. `end-to-end` использует development UI: регистрирует агента и два топика, однократно получает конфигурацию, отправляет тестовую запись временным Fluent Bit, проверяет Kafka, включает source с нормализатором и сверяет PostgreSQL и страницу «События». Перед UI-сценарием профиль также извлекает встроенный Lua Code filter `systemd` непосредственно из генератора конфига и запускает его на синтетической записи с Fluent Bit 4.2.3. Вход задан с timestamp `2000-01-01T00:00:00Z`; проверяется точный конверт `{timestamp, log}`, сохранение старого времени в строке `log` и timestamp времени обработки. Проверка использует синтетический dummy input, не читает живой journald. Fluent Bit использует неизменённый скачанный конфиг и внешний Kafka listener; тестовый контейнер разделяет сетевое пространство брокера, чтобы тот же loopback-адрес и динамический внешний порт были доступны без публикации тестового порта наружу. Проверка установки на отдельном хосте и живое чтение Windows Event Log остаются вне этого теста.

В отчёте о каждом прогоне указывайте профиль, команду, выведенный скриптом Compose project, фактические свободные порты, результат и отсутствие оставшихся ресурсов. Пароли и содержимое конфигурации Fluent Bit в отчёт не переносите.

`smoke-production` собирает production proxy с React и запускает PostgreSQL, Kafka, migration, provisioner и API на изолированном Compose-проекте. Он проверяет HTTPS с доверенным тестовым CA и именем `localhost`, API readiness, прямые UI-маршруты, JS/CSS, отказы для неизвестного API, отсутствующего asset и закрытой документации. Дополнительно проверяется доставка события тестовым Fluent Bit через внешний TLS listener. Интерактивные вход, сессия и выход проверяются отдельно в браузере через тот же HTTPS proxy.

`smoke-development` также создаёт синтетического локального администратора и выполняет тот же браузерный сценарий входа, обновления страницы и выхода через HTTP proxy разработки.

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
