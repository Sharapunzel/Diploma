# Развёртывание и эксплуатация

Эта инструкция нужна оператору при подготовке development или production, проверке сервисов и восстановлении. Начните с [краткого маршрута](../README.md); здесь приведены подробности и требования к секретам.

Перейти к разделу: [сети](#сервисы-и-сетевые-границы) · [development](#требования-и-запуск-разработки) · [production](#подготовка-production) · [эксплуатация и восстановление](#эксплуатация).

Система принимает локальные логи через Kafka и читает внешние ECS-ориентированные события из OpenSearch-совместимого индексера. React предоставляет вход, управление агентами, подключениями, источниками и нормализаторами, поиск событий, диагностику и администрирование по правам пользователя.

## Сервисы и сетевые границы

```text
Браузер ── HTTP (разработка) / HTTPS (production) ──> Nginx ──> FastAPI
                                                       └──> Vite (только разработка)
Fluent Bit ── SASL/SCRAM (локальный dev) / SASL_SSL (production) ──> Kafka
                                                   FastAPI ──> PostgreSQL, Kafka
```

Compose размещает PostgreSQL и Kafka в закрытой bridge-сети с постоянными именованными томами. Порт FastAPI не публикуется на хосте. Nginx — единственная опубликованная веб-служба. Controller-порт Kafka остаётся внутренним; внутренний и внешний клиентские listener требуют SCRAM-SHA-512 и ACL. В production оба используют TLS, Kafka не проходит через Nginx. Учётные записи и управление агентами Fluent Bit описаны в [инструкции](agent-lifecycle.md); runtime principal имеет только права чтения.

## Требования и запуск разработки

Установите Docker Engine с плагином Compose. Перед первым запуском создайте отдельные тестовые Kafka credentials командой `python scripts/prepare_dev_kafka.py`. Скрипт сохраняет их в неотслеживаемой `.dev/` и не перезаписывает существующие файлы. Несекретный шаблон пароля PostgreSQL находится в `.dev/postgres_password.example`. Compose использует одинаковый `POSTGRES_PASSWORD` для PostgreSQL, миграций и FastAPI. Для собственного локального пароля создайте неотслеживаемый `.env` в корне репозитория со строкой `POSTGRES_PASSWORD=<пароль, безопасный для URI>`. `.env` и локальные файлы `.dev/` игнорируются Git. Используйте только `A-Z`, `a-z`, цифры, `_`, `-`, `.`: пароль входит в development URL SQLAlchemy. Пароль по умолчанию намеренно слабый; не используйте его в production.

Перед созданием внешних OpenSearch-подключений сгенерируйте **один постоянный** ключ Fernet командой ниже и сохраните строку `EXTERNAL_SECRET_KEY=<полученный ключ>` в корневом `.env`. При последующих пересозданиях контейнера используйте тот же файл и ключ: иначе сохранённые пароли нельзя расшифровать. Без ключа остальной dev-стек запускается, но создание внешнего подключения возвращает `503 external_key_unavailable`. Compose читает корневой `.env` для подстановки и явно передаёт ключ в `api`; файл `backend/.env` используется только при прямом запуске backend и не попадает в образ. Не добавляйте `.env` в Git и не публикуйте вывод `docker compose config` или `docker inspect`: они могут содержать значение ключа и другие dev-секреты.

```shell
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Для OIDC через dev proxy дополните **корневой** `.env` значениями `OIDC_ENABLED=true`, `OIDC_ISSUER_URL`, `OIDC_CLIENT_ID`, `OIDC_CLIENT_SECRET`, `OIDC_REDIRECT_URI`, `OIDC_SCOPES` и при необходимости `OIDC_SUCCESS_REDIRECT_URL`, `OIDC_ERROR_REDIRECT_URL`, `OIDC_STATE_SECRET`. Секрет IdP не храните в отслеживаемых файлах. Зарегистрируйте у IdP callback публичного адреса proxy, по умолчанию `http://localhost:8080/api/v1/auth/oidc/callback`; при другом порту `PROXY_BIND` задайте соответствующий `OIDC_REDIRECT_URI` и `HMR_CLIENT_PORT` равный внешнему порту proxy. Успешное и ошибочное перенаправления по умолчанию относительные (`/` и `/login?error=oidc`) и остаются на том же адресе proxy. OIDC по умолчанию выключен, локальный вход в dev Compose остаётся включённым и при активации OIDC. Production требует HTTPS и отдельные защищённые файлы секретов, описанные ниже.

Из корня репозитория запустите:

```shell
docker compose -f docker-compose.yml -f docker-compose.dev.yml up --build -d
```

Dev proxy доступен по `http://localhost:8080`; frontend, глубокие маршруты SPA и HMR идут через этот адрес без второго публичного порта. Этот HTTP адрес предназначен только для локальной отладки: реальные пароли нельзя передавать по недоверенной сети без HTTPS. Решение о сохранении учётных данных принимает браузер или установленный менеджер паролей; приложение не хранит историю логинов и пароли. Маршруты API имеют префикс `/api/v1`, Swagger — `/docs`, OpenAPI — `/openapi.json`. PostgreSQL публикуется только на loopback-порту 5432. Kafka `SASL_PLAINTEXT` с тестовыми credentials публикуется только на `127.0.0.1:9092`; при попытке опубликовать dev Kafka на другом интерфейсе брокер откажет в запуске. `backend/.env.example` настроен на браузерный адрес proxy. Для прямого запуска FastAPI без Compose задайте `DATABASE_URL` с `localhost:5432`, `CORS_ORIGINS=http://localhost:5173`, OIDC callback `http://localhost:8000/api/v1/auth/oidc/callback` и перенаправления `http://localhost:5173/` и `/login?error=oidc`. Для Compose используйте callback через proxy. Kafka-профиль для прямого запуска backend описан в [инструкции по безопасности](kafka-security.md). При создании Kafka-подключения приложения указывайте внутренний адрес `kafka:9092`; `localhost` внутри backend-контейнера указывает на него самого.

Для локальной работы с фронтендом нужна Node.js 20.19.5 (эта же версия используется в `frontend/Dockerfile.dev`). В `frontend/` выполните `npm ci`, затем `npm run typecheck`, `npm run lint`, `npm run format:check`, `npm run test`, `npm run build`. `npm run dev` запускает Vite; штатный браузерный адрес при Compose — proxy на порту 8080. Файлы `node_modules` на хосте и в контейнере разделены. При каждом запуске frontend-контейнер сравнивает хэш `package-lock.json` с установленными зависимостями в своём volume и запускает `npm ci`, если lockfile изменился. После изменения зависимостей выполните `docker compose -f docker-compose.yml -f docker-compose.dev.yml up --build -d frontend`; общие тома PostgreSQL/Kafka удалять не требуется. Правки исходников подхватываются через HMR без пересборки.

Полную браузерную приёмку на Windows запускайте из корня командой `powershell -File backend/tests/run_frontend_smoke.ps1` после установки зависимостей через `npm ci` в `frontend/`. Скрипт проверяет отдельные имя Compose-проекта, БД, тома, подсеть и свободные порты до `up`, поднимает синтетический IdP с подписанным ID token, создаёт тестовый аккаунт с длинным именем и переименованной ролью, проверяет чистый и повторный запуск frontend-volume при изменении lockfile, затем выполняет Playwright через proxy. В `finally` он восстанавливает lockfile, останавливает свой IdP и удаляет только собственные smoke-контейнеры и тома. По умолчанию заняты порты 18094/15494/19094/19095 и подсеть `172.31.94.0/24`; если они уже используются, передайте параметры `-ProxyPort`, `-PostgresPort`, `-KafkaPort`, `-IdpPort`, `-Subnet`, `-ProxyIp` и при необходимости `-ChromePath`. Скрипт не использует `diploma_db`.

При ручном запуске Playwright необходимы `PLAYWRIGHT_BASE_URL`, `PLAYWRIGHT_USERNAME`, `PLAYWRIGHT_PASSWORD`, `PLAYWRIGHT_OIDC=1` и Chromium (`npx playwright install chromium`) либо `PLAYWRIGHT_CHROME_PATH` к установленному Chrome. Критические сценарии local/OIDC/HMR не пропускаются при отсутствии настройки, а завершаются понятной ошибкой.

Повторяемые изолированные проверки управления агентами, Kafka и Fluent Bit запускаются через [проверочные профили](agent-test-profiles.md). Они создают собственный Compose-проект и не используют рабочие тома.

На `/login` сервер сообщает доступные способы входа. Локальная форма и OIDC могут отображаться одновременно; OIDC требует настройки IdP и `OIDC_REDIRECT_URI` с фактическим портом proxy. После входа серверная сессия хранится в HttpOnly cookie. Тема по умолчанию следует системе; ручной выбор светлой или тёмной сохраняется в localStorage. На `/connections` доступны Kafka и внешние подключения, на `/sources` — их источники. Просмотр сохранённых внешних подключений требует `connections.read`, но их проверка и обнаружение индексов/потоков требуют `connections.write`; изменения источников требуют `sources.write`. Для внешнего подключения нужен постоянный `EXTERNAL_SECRET_KEY` на backend. Корпоративный публичный CA загружается отдельной операцией после сохранения подключения; CA общедоверенного издателя обычно загружать не нужно. Изменение внешнего подключения или CA выключает связанные источники, их включают повторно вручную. Состав доступных разделов определяется серверными правами пользователя.

Проверьте `http://localhost:8080/api/v1/health/live` и `http://localhost:8080/api/v1/health/ready`. Одноразовый сервис `migrate` применяет миграции Alembic до запуска API; при ошибке API не стартует. Первого администратора создавайте интерактивно:

```shell
docker compose -f docker-compose.yml -f docker-compose.dev.yml exec api python -m app.cli create-admin --username admin --display-name Administrator
```

Ожидается приглашение ввести пароль, затем подтверждение создания пользователя. Если `api` не запущен, проверьте readiness и журналы `migrate` и `api`, прежде чем повторять команду. Не передавайте пароль аргументом команды. Локальный вход создаёт серверную сессию; изменяющие запросы требуют CSRF-токен, описанный в [инструкции по аутентификации](authentication.md).

## Подготовка production

Для приёмки Compose используйте [постоянные изолированные профили](agent-test-profiles.md) `smoke-development` и `smoke-production`. Они проверяют resolved Compose, выделяют собственные проект, порты, БД, сеть, тома и синтетические секреты, затем удаляют свои ресурсы. Не запускайте проверку с именем рабочего проекта `diploma` и не используйте его тома или `.env.production`.

Используйте Linux-хост и закрытый каталог секретов и сертификатов вне репозитория. Для команд ниже нужны `install`, `setfacl` и `getfacl` (пакет `acl` на большинстве дистрибутивов); выполняйте их с правами администратора хоста. Сначала создайте каталоги:

```shell
install -d -o root -g root -m 0700 /etc/diploma /etc/diploma/secrets /etc/diploma/tls/web /etc/diploma/tls/kafka
```

До настройки ACL разместите в этих каталогах файлы из таблицы. Секреты переносите из защищённого хранилища непосредственно в указанные пути с владельцем `root` и режимом `0600`; сертификаты и ключи — из процедуры выпуска вашей организации. Не помещайте содержимое секретов в `.env.production`, историю команд или репозиторий. Команда для генерации **нового** ключа агента не перезапишет существующий файл (`noclobber`); сохраните его в резервной копии вместе с БД. Она требует установленный Python-пакет `cryptography`:

```shell
( set -C; umask 077; python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())' > /etc/diploma/secrets/agent_credentials_key )
```

| Файл | Что подготовить до ACL |
| --- | --- |
| `secrets/postgres_password`, `secrets/database_url` | Один пароль PostgreSQL и SQLAlchemy URL с тем же URL-кодированным паролем. |
| `secrets/oidc_state_secret`, `secrets/oidc_client_secret` | Уникальный state secret не короче 32 случайных байт и client secret IdP; при выключенном OIDC второй файл всё равно требуется Compose. |
| `secrets/external_secret_key`, `secrets/agent_credentials_key` | Два независимых постоянных ключа Fernet; первый можно сгенерировать командой ниже, второй — командой выше. |
| `secrets/kafka_admin_password`, `secrets/kafka_reader_password` | Два независимых пароля SCRAM. |
| `tls/web/fullchain.pem`, `tls/web/privkey.pem` | Цепочка сертификата и приватный ключ для `PUBLIC_HOST`. |
| `tls/kafka/broker.pem`, `tls/kafka/ca.pem` | Цепочка сертификата брокера и незашифрованный ключ PKCS#8 в одном PEM; доверенный CA. SAN брокера включает `kafka` и `KAFKA_PUBLIC_HOST`. |

После размещения всех файлов проверьте их наличие и задайте узкие POSIX ACL. Блок остановится при отсутствующем или пустом файле. Backend, миграция и provisioner работают с UID/GID `10001`; Kafka и `kafka-acls` — с UID `1000`. Nginx читает только смонтированные ему веб-сертификат и ключ как root, PostgreSQL — только свой пароль при начальной инициализации. Compose secrets с `file:` являются bind mounts: `uid/gid/mode` в Compose не отменяет запреты на исходном пути.

```shell
set -eu
for file in /etc/diploma/secrets/postgres_password /etc/diploma/secrets/database_url /etc/diploma/secrets/oidc_state_secret /etc/diploma/secrets/oidc_client_secret /etc/diploma/secrets/external_secret_key /etc/diploma/secrets/agent_credentials_key /etc/diploma/secrets/kafka_admin_password /etc/diploma/secrets/kafka_reader_password /etc/diploma/tls/web/fullchain.pem /etc/diploma/tls/web/privkey.pem /etc/diploma/tls/kafka/broker.pem /etc/diploma/tls/kafka/ca.pem; do test -s "$file"; done
setfacl -m u:10001:--x /etc/diploma /etc/diploma/secrets
setfacl -m u:10001:--x /etc/diploma/tls /etc/diploma/tls/kafka
setfacl -m u:10001:r-- /etc/diploma/secrets/database_url /etc/diploma/secrets/oidc_state_secret /etc/diploma/secrets/oidc_client_secret /etc/diploma/secrets/external_secret_key /etc/diploma/secrets/agent_credentials_key /etc/diploma/secrets/kafka_admin_password
setfacl -m u:1000:--x /etc/diploma /etc/diploma/tls /etc/diploma/tls/kafka
setfacl -m u:1000:--x /etc/diploma/secrets
setfacl -m u:1000:r-- /etc/diploma/tls/kafka/broker.pem /etc/diploma/tls/kafka/ca.pem
setfacl -m u:1000:r-- /etc/diploma/secrets/kafka_admin_password /etc/diploma/secrets/kafka_reader_password
setfacl -m u:10001:r-- /etc/diploma/secrets/kafka_reader_password /etc/diploma/tls/kafka/ca.pem
getfacl -p /etc/diploma/secrets/agent_credentials_key /etc/diploma/secrets/kafka_admin_password /etc/diploma/tls/kafka/broker.pem
```

Ожидаются ACL чтения ключа агента для `10001`, Kafka admin password для `1000` и `10001`, broker PEM только для `1000`. Убедитесь от имени этих UID, что назначенные файлы читаются, а чужие нет; Nginx и PostgreSQL получают только свои файлы через список `secrets` в Compose. При отказе в чтении проверьте право прохода по всем родительским каталогам и ACL нужного файла; не открывайте доступ всем пользователям.

Образ Apache Kafka работает с UID/GID 1000; только ему предоставьте чтение broker PEM. Генерация Fernet-ключа для **новой** установки внешних подключений:

```shell
python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())'
```

Создайте закрытый `.env.production` **только с путями и несекретными настройками хоста**. Следующий блок — шаблон: замените `events.example.org`, `kafka.example.org` и пути `/etc/diploma/...` на проверенные значения своей Linux-установки. Не запускайте Compose с этим примером без замены.

```dotenv
PUBLIC_HOST=events.example.org
KAFKA_PUBLIC_HOST=kafka.example.org
KAFKA_BIND_ADDRESS=0.0.0.0
POSTGRES_PASSWORD_FILE=/etc/diploma/secrets/postgres_password
DATABASE_URL_FILE=/etc/diploma/secrets/database_url
OIDC_STATE_SECRET_FILE=/etc/diploma/secrets/oidc_state_secret
OIDC_CLIENT_SECRET_FILE=/etc/diploma/secrets/oidc_client_secret
EXTERNAL_SECRET_KEY_FILE=/etc/diploma/secrets/external_secret_key
AGENT_CREDENTIALS_KEY_FILE=/etc/diploma/secrets/agent_credentials_key
WEB_CERTIFICATE_FILE=/etc/diploma/tls/web/fullchain.pem
WEB_PRIVATE_KEY_FILE=/etc/diploma/tls/web/privkey.pem
KAFKA_BROKER_PEM_FILE=/etc/diploma/tls/kafka/broker.pem
KAFKA_CA_FILE=/etc/diploma/tls/kafka/ca.pem
KAFKA_ADMIN_PASSWORD_FILE=/etc/diploma/secrets/kafka_admin_password
KAFKA_READER_PASSWORD_FILE=/etc/diploma/secrets/kafka_reader_password
OIDC_ENABLED=false
# При включении OIDC задайте OIDC_ISSUER_URL, OIDC_CLIENT_ID и:
# OIDC_REDIRECT_URI=https://events.example.org/api/v1/auth/oidc/callback
```

Пароль PostgreSQL монтируется только в PostgreSQL; URL БД — в `migrate` и `api` и содержит тот же пароль. Для OIDC задайте `OIDC_ENABLED=true`, issuer, client ID, scopes и URL перенаправления в несекретном окружении. Зарегистрируйте у IdP точный публичный callback `https://events.example.org/api/v1/auth/oidc/callback`. `docker compose config` показывает пути к секретам, но не их содержимое. Bridge-сеть по умолчанию — `172.30.0.0/24`, Nginx — `172.30.0.10`; при пересечении с сетью хоста задайте свободные `COMPOSE_SUBNET` и `PROXY_IP`. Uvicorn доверяет forwarded-заголовкам только от этого Nginx.

Проверьте и запустите production:

```shell
docker compose --env-file .env.production -f docker-compose.yml -f docker-compose.prod.yml config
docker compose --env-file .env.production -f docker-compose.yml -f docker-compose.prod.yml up --build -d
```

Production proxy собирает React по `frontend/package-lock.json` и обслуживает готовые файлы через HTTPS. Для обновления фронтенда пересоберите и пересоздайте только proxy: `docker compose --env-file .env.production -f docker-compose.yml -f docker-compose.prod.yml up --build -d proxy`. Проверьте публичный `/login`, локальный вход, обновление защищённой страницы и `/api/v1/health/ready` на том же адресе. Сертификат и ключ берутся из защищённых файлов `WEB_CERTIFICATE_FILE` и `WEB_PRIVATE_KEY_FILE`; остальные секреты также передаются файлами хоста. Development по-прежнему доступен через `http://localhost:8080` и Vite за proxy. Настройка OIDC через интерфейс доступна администратору в разделе «Администрирование».

Наружу публикуются только Nginx 80/443 и TLS-порт Kafka 9094. Порт 80 перенаправляет на HTTPS. Ограничьте 9094 сетями агентов. FastAPI, PostgreSQL и внутренний `SASL_SSL` listener Kafka не публикуются. Веб- и Kafka-сертификаты имеют независимые имена и процедуры продления. После обновления сертификатов перезапустите затронутые службы командой ниже из корня репозитория на Linux-хосте; тома сохраняются:

```shell
docker compose --env-file .env.production -f docker-compose.yml -f docker-compose.prod.yml restart proxy kafka
```

Ожидается восстановление HTTPS и Kafka TLS. При ошибке проверьте SAN, цепочку CA, права файлов и журналы служб. Ротация секретов описана в [инструкции по TLS](tls.md). Kafka-подключение приложения использует внутренний `kafka:9092`; внешние агенты используют TLS-адрес и 9094.

Учётные записи агентов и индивидуальные ACL создаёт provisioner. Runtime principal приложения читает существующие топики; он не может создавать топики или записывать сообщения. Готовую конфигурацию администратор однократно получает в разделе «Агенты».

## Эксплуатация

Из корня репозитория на Linux-хосте с подготовленным `.env.production` проверяйте состояние и последние журналы:

```shell
docker compose --env-file .env.production -f docker-compose.yml -f docker-compose.prod.yml ps
docker compose --env-file .env.production -f docker-compose.yml -f docker-compose.prod.yml logs --tail=200 api proxy kafka
```

Ожидаются работающие `postgres`, `kafka`, `api`, `provisioner` и `proxy`; одноразовый `migrate` должен завершиться успешно. При ошибке readiness сначала проверьте журналы `migrate`, `postgres` и `api`, затем [миграции и БД](database.md). При сбое доставки проверьте [Kafka и обработку](kafka.md) и [состояние агента](agent-lifecycle.md). Не публикуйте журналы с секретами.

Обычная остановка сохраняет тома:

```shell
docker compose --env-file .env.production -f docker-compose.yml -f docker-compose.prod.yml stop
```

Для возобновления выполните `up -d` из раздела подготовки production. `down -v` удаляет накопленные данные PostgreSQL и Kafka; для обычной остановки используйте `stop`. Перед восстановлением из копии сохраните ключи внешних подключений и агентов вместе с согласованным снимком БД и Kafka.

### Типовые неисправности

| Признак | Проверка и безопасное действие |
| --- | --- |
| `/api/v1/health/ready` недоступен | Проверьте `ps` и журналы `migrate`, `postgres`, `api`. Исправьте причину миграции или подключения к БД; не меняйте Alembic revision вручную. |
| Агент подключается, но событие не появляется | Проверьте статус агента и источника, назначенный топик, нормализатор, страницу «Диагностика» и Kafka metadata. Не создавайте вручную вторую учётную запись с тем же именем. |
| Kafka возвращает ошибку TLS, SCRAM или ACL | Сверьте внешний listener, CA и SAN сертификата, актуальный одноразово выданный конфиг, затем состояние provisioner и права топика. См. [безопасность Kafka](kafka-security.md). |
| Внешний источник не читается | Проверьте достижимость адреса из API-контейнера, сертификат и CA, права чтения индексера и состояние источника. После изменения подключения повторно проверьте и включите источник. |
| OIDC-вход не завершился | Сверьте issuer, публичный HTTPS callback и точное имя хоста, затем проверьте discovery и пробный вход в «Администрировании». Используйте резервный локальный вход администратора. |

### Восстановление

Восстанавливайте PostgreSQL и Kafka из согласованных резервных копий по принятой процедуре вашей установки. Сохраняйте связанные секреты: ключ шифрования внешних подключений, ключ невыданных учётных данных агентов, пароли Kafka и OIDC state secret. Потеря ключа внешних подключений делает сохранённые пароли нечитаемыми; простая замена переменной не является ротацией. Процедура перешифрования — в [инструкции внешних источников](external-sources.md). После восстановления проверьте миграции, readiness, Kafka metadata, состояния источников и чтение одного тестового события каждого пути. Не удаляйте рабочие тома при диагностике.
- Проверки API: `/api/v1/health/live` и `/api/v1/health/ready`.
- Корень API: `/api/v1`; Swagger/OpenAPI доступны только в разработке и тестах, в production отключены (`/docs`, `/redoc`, `/openapi.json`).
- Тестовая БД и миграции: [инструкция по БД](database.md).
- Kafka offsets и обработка: [инструкция по Kafka](kafka.md).
- SCRAM, ACL и переход тома: [безопасность Kafka](kafka-security.md).
- События ECS и нормализация: [инструкция по событиям](events.md).
- Сессии и CSRF: [инструкция по аутентификации](authentication.md).
- Административный API: [инструкция по управлению](administration.md).
- Внешние источники: [инструкция по OpenSearch](external-sources.md).

Удаляйте именованные тома только при отдельно согласованной операции удаления данных. `diploma_test` — временная тестовая БД; `diploma_db` — база разработки, на которой запрещены тесты и откаты миграций.
