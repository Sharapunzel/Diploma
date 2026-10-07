# Система анализа событий

Система принимает логи через Kafka, нормализует их в ECS, сохраняет результаты в PostgreSQL и предоставляет API через FastAPI. Внешние ECS-ориентированные события в OpenSearch-совместимом индексере читаются отдельным адаптером. React предоставляет вход, темы, управление подключениями и источниками. Остальные предметные страницы пока показывают заглушки.

## Сервисы и сетевые границы

```text
Браузер ── HTTP (разработка) / HTTPS (production) ──> Nginx ──> FastAPI
                                                       └──> Vite (только разработка)
Fluent Bit ── PLAINTEXT (разработка) / TLS (production) ──> Kafka
                                                   FastAPI ──> PostgreSQL, Kafka
```

Compose размещает PostgreSQL и Kafka в закрытой bridge-сети с постоянными именованными томами. Порт FastAPI не публикуется на хосте. Nginx — единственная опубликованная веб-служба. Controller-порт Kafka остаётся внутренним; брокер предоставляет FastAPI отдельный внутренний listener. Внешний TLS-трафик Kafka не проходит через Nginx. Ограничьте доступ к порту брокера сетями известных агентов Fluent Bit: TLS подтверждает брокер и шифрует канал, но эта версия не аутентифицирует агентов (без SASL, клиентских сертификатов и ACL).

## Требования и запуск разработки

Установите Docker Engine с плагином Compose. Несекретный шаблон пароля для разработки находится в `.dev/postgres_password.example`, поэтому чистая копия запускается без подготовки секретов. Compose использует одинаковый `POSTGRES_PASSWORD` для PostgreSQL, миграций и FastAPI. Для собственного локального пароля создайте неотслеживаемый `.env` в корне репозитория со строкой `POSTGRES_PASSWORD=<пароль, безопасный для URI>`. `.env` и локальные файлы `.dev/` игнорируются Git. Используйте только `A-Z`, `a-z`, цифры, `_`, `-`, `.`: пароль входит в development URL SQLAlchemy. Пароль по умолчанию намеренно слабый; не используйте его в production.

Перед созданием внешних OpenSearch-подключений сгенерируйте **один постоянный** ключ Fernet командой ниже и сохраните строку `EXTERNAL_SECRET_KEY=<полученный ключ>` в корневом `.env`. При последующих пересозданиях контейнера используйте тот же файл и ключ: иначе сохранённые пароли нельзя расшифровать. Без ключа остальной dev-стек запускается, но создание внешнего подключения возвращает `503 external_key_unavailable`. Compose читает корневой `.env` для подстановки и явно передаёт ключ в `api`; файл `backend/.env` используется только при прямом запуске backend и не попадает в образ. Не добавляйте `.env` в Git и не публикуйте вывод `docker compose config` или `docker inspect`: они могут содержать значение ключа и другие dev-секреты.

```shell
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Для OIDC через dev proxy дополните **корневой** `.env` значениями `OIDC_ENABLED=true`, `OIDC_ISSUER_URL`, `OIDC_CLIENT_ID`, `OIDC_CLIENT_SECRET`, `OIDC_REDIRECT_URI`, `OIDC_SCOPES` и при необходимости `OIDC_SUCCESS_REDIRECT_URL`, `OIDC_ERROR_REDIRECT_URL`, `OIDC_STATE_SECRET`. Секрет IdP не храните в отслеживаемых файлах. Зарегистрируйте у IdP callback публичного адреса proxy, по умолчанию `http://localhost:8080/api/v1/auth/oidc/callback`; при другом порту `PROXY_BIND` задайте соответствующий `OIDC_REDIRECT_URI` и `HMR_CLIENT_PORT` равный внешнему порту proxy. Успешное и ошибочное перенаправления по умолчанию относительные (`/` и `/login?error=oidc`) и остаются на том же адресе proxy. OIDC по умолчанию выключен, локальный вход в dev Compose остаётся включённым и при активации OIDC. Production требует HTTPS и отдельные защищённые файлы секретов, описанные ниже.

Из корня репозитория запустите:

```shell
docker compose -f docker-compose.yml -f docker-compose.dev.yml up --build -d
```

Dev proxy доступен по `http://localhost:8080`; frontend, глубокие маршруты SPA и HMR идут через этот адрес без второго публичного порта. Этот HTTP адрес предназначен только для локальной отладки: реальные пароли нельзя передавать по недоверенной сети без HTTPS. Решение о сохранении учётных данных принимает браузер или установленный менеджер паролей; приложение не хранит историю логинов и пароли. Маршруты API имеют префикс `/api/v1`, Swagger — `/docs`, OpenAPI — `/openapi.json`. PostgreSQL публикуется только на loopback-порту 5432. PLAINTEXT listener Kafka для агентов публикуется на порту 9092 для локальной отладки. `backend/.env.example` настроен на браузерный адрес proxy. Для прямого запуска FastAPI без Compose задайте `DATABASE_URL` с `localhost:5432`, `CORS_ORIGINS=http://localhost:5173`, OIDC callback `http://localhost:8000/api/v1/auth/oidc/callback` и перенаправления `http://localhost:5173/` и `/login?error=oidc`. Для Compose используйте callback через proxy. При создании Kafka-подключения приложения указывайте внутренний адрес `kafka:9092`; `localhost` внутри backend-контейнера указывает на него самого.

Для локальной работы с фронтендом нужна Node.js 20.19.5 (эта же версия используется в `frontend/Dockerfile.dev`). В `frontend/` выполните `npm ci`, затем `npm run typecheck`, `npm run lint`, `npm run format:check`, `npm run test`, `npm run build`. `npm run dev` запускает Vite; штатный браузерный адрес при Compose — proxy на порту 8080. Файлы `node_modules` на хосте и в контейнере разделены. При каждом запуске frontend-контейнер сравнивает хэш `package-lock.json` с установленными зависимостями в своём volume и запускает `npm ci`, если lockfile изменился. После изменения зависимостей выполните `docker compose -f docker-compose.yml -f docker-compose.dev.yml up --build -d frontend`; общие тома PostgreSQL/Kafka удалять не требуется. Правки исходников подхватываются через HMR без пересборки.

Полную браузерную приёмку на Windows запускайте из корня командой `powershell -File backend/tests/run_frontend_smoke.ps1` после установки зависимостей через `npm ci` в `frontend/`. Скрипт проверяет отдельные имя Compose-проекта, БД, тома, подсеть и свободные порты до `up`, поднимает синтетический IdP с подписанным ID token, создаёт тестовый аккаунт с длинным именем и переименованной ролью, проверяет чистый и повторный запуск frontend-volume при изменении lockfile, затем выполняет Playwright через proxy. В `finally` он восстанавливает lockfile, останавливает свой IdP и удаляет только собственные smoke-контейнеры и тома. По умолчанию заняты порты 18094/15494/19094/19095 и подсеть `172.31.94.0/24`; если они уже используются, передайте параметры `-ProxyPort`, `-PostgresPort`, `-KafkaPort`, `-IdpPort`, `-Subnet`, `-ProxyIp` и при необходимости `-ChromePath`. Скрипт не использует `diploma_db`.

При ручном запуске Playwright необходимы `PLAYWRIGHT_BASE_URL`, `PLAYWRIGHT_USERNAME`, `PLAYWRIGHT_PASSWORD`, `PLAYWRIGHT_OIDC=1` и Chromium (`npx playwright install chromium`) либо `PLAYWRIGHT_CHROME_PATH` к установленному Chrome. Критические сценарии local/OIDC/HMR не пропускаются при отсутствии настройки, а завершаются понятной ошибкой.

На `/login` сервер сообщает доступные способы входа. Локальная форма и OIDC могут отображаться одновременно; OIDC требует настройки IdP и `OIDC_REDIRECT_URI` с фактическим портом proxy. После входа серверная сессия хранится в HttpOnly cookie. Тема по умолчанию следует системе; ручной выбор светлой или тёмной сохраняется в localStorage. На `/connections` доступны Kafka и внешние подключения, на `/sources` — их источники. Просмотр сохранённых внешних подключений требует `connections.read`, но их проверка и обнаружение индексов/потоков требуют `connections.write`; изменения источников требуют `sources.write`. Для внешнего подключения нужен постоянный `EXTERNAL_SECRET_KEY` на backend. Корпоративный публичный CA загружается отдельной операцией после сохранения подключения; CA общедоверенного издателя обычно загружать не нужно. Изменение внешнего подключения или CA выключает связанные источники, их включают повторно вручную. Разделы нормализаторов, событий, диагностики, администрирования и настроек пока являются заглушками. Production-поставка собранного frontend относится к TASK-19.

Проверьте `http://localhost:8080/api/v1/health/live` и `http://localhost:8080/api/v1/health/ready`. Одноразовый сервис `migrate` применяет миграции Alembic до запуска API; при ошибке API не стартует. Первого администратора создавайте интерактивно:

```shell
docker compose -f docker-compose.yml -f docker-compose.dev.yml exec api \
  python -m app.cli create-admin --username admin --display-name "Administrator"
```

Не передавайте пароль аргументом команды. Локальный вход создаёт серверную сессию; изменяющие запросы требуют CSRF-токен, описанный в [инструкции по аутентификации](docs/authentication.md).

## Изолированная приёмочная проверка Compose

Рабочий Compose-проект по умолчанию называется `diploma`. **Не запускайте приёмочный smoke с этим именем и с рабочими томами `diploma_postgres_data` и `diploma_kafka_data`.** Сервис `migrate` применяет `upgrade head` к БД выбранного проекта, поэтому имя и ресурсы проверяют до `up`, даже из отдельной копии репозитория. Не выполняйте `down -v` для рабочего проекта.

Выберите новое уникальное имя проекта, свободные порты и отдельную подсеть. Пример для PowerShell (проверьте адреса под свою сеть):

```powershell
$smokeProject = "diploma-smoke-$(Get-Date -Format yyyyMMddHHmmss)"
$env:COMPOSE_SUBNET = "172.31.81.0/24"
$env:PROXY_IP = "172.31.81.10"
$env:FORWARDED_ALLOW_IPS = $env:PROXY_IP
$env:POSTGRES_DB = "diploma_smoke_db"
$env:PROXY_BIND = "127.0.0.1:18080:8080"
$env:POSTGRES_BIND = "127.0.0.1:15432:5432"
$env:KAFKA_HOST_PORT = "19092"
docker compose -p $smokeProject -f docker-compose.yml -f docker-compose.dev.yml config
```

В выводе `config` **до запуска** проверьте `name: $smokeProject`, тома с префиксом `$smokeProject` вместо `diploma_postgres_data` и `diploma_kafka_data`, выбранные свободные порты, непересекающуюся `COMPOSE_SUBNET` и URL сервиса `migrate`, направленный в PostgreSQL именно нового проекта. `docker compose -p $smokeProject -f docker-compose.yml -f docker-compose.dev.yml ps -a` и `docker volume ls` помогут убедиться, что у нового проекта ещё нет контейнеров и томов с данными. Затем запустите:

```powershell
docker compose -p $smokeProject -f docker-compose.yml -f docker-compose.dev.yml up --build -d
docker compose -p $smokeProject -f docker-compose.yml -f docker-compose.dev.yml ps
docker compose -p $smokeProject -f docker-compose.yml -f docker-compose.dev.yml down
```

`down` сохраняет тома. Удаляйте только отдельно проверенные ненужные тестовые тома. Для production-smoke используйте собственный закрытый `.env.smoke.production` с тестовыми секретами и сертификатами, отдельной сетью (`COMPOSE_SUBNET`, `PROXY_IP`) и свободными `HTTP_BIND`, `HTTPS_BIND`, `KAFKA_HOST_PORT`. Файл `DATABASE_URL_FILE` в нём должен указывать на БД тестового PostgreSQL-контейнера, а имя БД в URL должно совпадать с `POSTGRES_DB`. Перед запуском проверьте `name`, тома, сеть, порты и URL миграций в итоговом `config`:

```powershell
$prodSmokeProject = "diploma-prod-smoke-$(Get-Date -Format yyyyMMddHHmmss)"
docker compose -p $prodSmokeProject --env-file .env.smoke.production -f docker-compose.yml -f docker-compose.prod.yml config
docker compose -p $prodSmokeProject --env-file .env.smoke.production -f docker-compose.yml -f docker-compose.prod.yml ps -a
docker volume ls
# Только после сверки конфигурации и отсутствия ресурсов с этим именем:
docker compose -p $prodSmokeProject --env-file .env.smoke.production -f docker-compose.yml -f docker-compose.prod.yml up --build -d
docker compose -p $prodSmokeProject --env-file .env.smoke.production -f docker-compose.yml -f docker-compose.prod.yml down
```

Не запускайте production-smoke с рабочим `.env.production`: он содержит пути к рабочим секретам и URL БД. Backend suite использует только тестовую БД согласно [инструкции по БД](docs/database.md).

## Подготовка production

Используйте Linux-хост и закрытый каталог секретов и сертификатов вне репозитория. Backend и одноразовый сервис миграции работают с UID/GID `10001`; дайте этому UID доступ на чтение `database_url`, `oidc_state_secret`, `oidc_client_secret`, `external_secret_key` и право прохода (`--x`) по каждому родительскому каталогу. Пример узких POSIX ACL:

```shell
install -d -o root -g root -m 0700 /etc/diploma /etc/diploma/secrets /etc/diploma/tls/web /etc/diploma/tls/kafka
setfacl -m u:10001:--x /etc/diploma /etc/diploma/secrets
setfacl -m u:10001:r-- /etc/diploma/secrets/database_url /etc/diploma/secrets/oidc_state_secret /etc/diploma/secrets/oidc_client_secret /etc/diploma/secrets/external_secret_key
setfacl -m u:1000:--x /etc/diploma /etc/diploma/tls /etc/diploma/tls/kafka
setfacl -m u:1000:r-- /etc/diploma/tls/kafka/broker.pem
```

Главному процессу Nginx дайте доступ только к веб-сертификату и ключу и право прохода по родительским каталогам. PostgreSQL читает файл пароля при начальной инициализации под root; UID backend не должен читать его. До запуска проверьте права через `getfacl`. Не открывайте файлы и каталоги всем пользователям. Compose secrets с `file:` — bind mounts: `uid/gid/mode` не отменяет запреты на исходном пути.

Подготовьте защищённые файлы: пароль PostgreSQL, SQLAlchemy `DATABASE_URL` (с URL-кодированным паролем), уникальный OIDC state secret (минимум 32 случайных байта), OIDC client secret, Fernet `EXTERNAL_SECRET_KEY`, цепочку веб-сертификата, веб-ключ и Kafka PEM bundle из цепочки сертификата брокера и незашифрованного приватного ключа PKCS#8. Образ Apache Kafka работает с UID/GID 1000; только ему предоставьте чтение bundle (например, UID 1000, `0400` или узкий ACL). Генерация Fernet-ключа:

```shell
python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())'
```

Создайте закрытый `.env.production` **только с путями и несекретными настройками хоста**:

```dotenv
PUBLIC_HOST=events.example.org
KAFKA_PUBLIC_HOST=kafka.example.org
KAFKA_BIND_ADDRESS=0.0.0.0
POSTGRES_PASSWORD_FILE=/etc/diploma/secrets/postgres_password
DATABASE_URL_FILE=/etc/diploma/secrets/database_url
OIDC_STATE_SECRET_FILE=/etc/diploma/secrets/oidc_state_secret
OIDC_CLIENT_SECRET_FILE=/etc/diploma/secrets/oidc_client_secret
EXTERNAL_SECRET_KEY_FILE=/etc/diploma/secrets/external_secret_key
WEB_CERTIFICATE_FILE=/etc/diploma/tls/web/fullchain.pem
WEB_PRIVATE_KEY_FILE=/etc/diploma/tls/web/privkey.pem
KAFKA_BROKER_PEM_FILE=/etc/diploma/tls/kafka/broker.pem
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

Наружу публикуются только Nginx 80/443 и TLS-порт Kafka 9094. Порт 80 перенаправляет на HTTPS. Ограничьте 9094 сетями агентов. FastAPI, PostgreSQL и внутренний PLAINTEXT listener Kafka не публикуются. Веб- и Kafka-сертификаты имеют независимые имена и процедуры продления. Обновите файлы на месте и выполните `docker compose ... restart proxy kafka`, сохранив тома. Ротация секретов описана в [инструкции по TLS](docs/tls.md). Не используйте `down -v` для обычной остановки. Kafka-подключение приложения использует внутренний `kafka:9092`; внешние агенты используют TLS-адрес и 9094.

Fluent Bit подключается напрямую к `kafka.example.org:9094` с проверкой сертификата по CA издателя. Пример — в [инструкции по TLS](docs/tls.md). Автосоздание топика включено: после первой записи агента новый топик появляется как обнаруженный и регистрируется как источник только по явному действию пользователя.

## Эксплуатация

- Запуск: `docker compose ... up -d`.
- Остановка без удаления данных: `docker compose ... stop`.
- Перезапуск: `docker compose ... restart`.
- Состояние и журналы: `docker compose ... ps` и `docker compose ... logs --tail=200 api proxy kafka`.
- Проверки API: `/api/v1/health/live` и `/api/v1/health/ready`.
- Корень API: `/api/v1`; Swagger/OpenAPI доступны только в разработке и тестах, в production отключены (`/docs`, `/redoc`, `/openapi.json`).
- Тестовая БД и миграции: [инструкция по БД](docs/database.md).
- Kafka offsets и обработка: [инструкция по Kafka](docs/kafka.md).
- События ECS и нормализация: [инструкция по событиям](docs/events.md).
- Сессии и CSRF: [инструкция по аутентификации](docs/authentication.md).
- Административный API: [инструкция по управлению](docs/administration.md).
- Внешние источники: [инструкция по OpenSearch](docs/external-sources.md).

Удаляйте именованные тома только при отдельно согласованной операции удаления данных. `diploma_test` — временная тестовая БД; `diploma_db` — база разработки, на которой запрещены тесты и откаты миграций.
