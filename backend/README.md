# Backend database foundation

Требуется Python 3.11+, Docker и Docker Compose. PostgreSQL запускается существующим
Compose-файлом из корня проекта:

```powershell
docker compose up -d postgres
cd backend
python -m pip install -e ".[test]"
```

Для проверок создайте отдельную базу без удаления development volume. Команды ниже
направляют и Pytest, и ручные команды Alembic именно в `diploma_test`:

```powershell
docker exec diploma-postgres createdb -U diploma_user diploma_test
$env:TEST_DATABASE_URL = "postgresql+psycopg://diploma_user:diploma_password@localhost:5432/diploma_test"
$env:DATABASE_URL = $env:TEST_DATABASE_URL
python -m pytest -q
python -m alembic upgrade head
python -m alembic check
python -m alembic downgrade base
```

После проверок тестовую базу можно безопасно удалить, не затрагивая `diploma_db`
или именованный Compose volume:

```powershell
docker exec diploma-postgres dropdb -U diploma_user --if-exists --force diploma_test
```

Тестовая fixture разбирает URL и аварийно завершает запуск, если имя базы равно
`diploma_db`, в том числе при наличии query-параметров в URL.

Схема `app` содержит пользователей, роли, настройки и конфигурацию источников.
Схема `logs` содержит успешно нормализованные события. Все изменения схемы выполняются
только Alembic; `create_all()` для инициализации не используется.

Credentials в Compose предназначены только для локальной разработки и не являются
рабочими секретами.

## FastAPI и аутентификация

Скопируйте `.env.example` в `.env` и задайте значения окружения без добавления `.env`
в Git. Миграции запускаются вручную и не выполняются при старте API:

```powershell
python -m pip install -e ".[test]"
$env:DATABASE_URL = "postgresql+psycopg://diploma_user:diploma_password@localhost:5432/diploma_db"
python -m alembic upgrade head
python -m uvicorn app.main:app --reload
```

Проверки доступны по `/api/v1/health/live` и `/api/v1/health/ready`. Фабрика
приложения создаёт engine из `DATABASE_URL` конкретного экземпляра; миграции при
старте API не выполняются. Для создания первого администратора используется явная
интерактивная команда:

```powershell
python -m app.cli create-admin --username admin --display-name "Administrator"
```

Локальный и OIDC-вход можно включать одновременно:

```dotenv
LOCAL_AUTH_ENABLED=true
OIDC_ENABLED=true
OIDC_ISSUER_URL=https://identity.example.test
OIDC_CLIENT_ID=diploma
OIDC_CLIENT_SECRET=replace-me
OIDC_REDIRECT_URI=https://api.example.test/api/v1/auth/oidc/callback
OIDC_SCOPES=openid,profile,email
OIDC_SUCCESS_REDIRECT_URL=https://ui.example.test/
OIDC_ERROR_REDIRECT_URL=https://ui.example.test/login?error=oidc
```

`OIDC_REDIRECT_URI` нужно без изменений зарегистрировать у Identity Provider. OIDC
включается только при заполненных issuer, client ID, client secret, redirect URI и
scope `openid`; redirect URL берутся только из конфигурации. State и nonce хранятся
в отдельной краткоживущей cookie (`OIDC_HANDSHAKE_TTL_SECONDS=600`).

Локальный сценарий выглядит так:

1. `POST /api/v1/auth/local/login` с JSON `username`/`password` создаёт HttpOnly
   session cookie и readable CSRF cookie; сырой session token и CSRF token в БД не
   записываются.
2. `GET /api/v1/auth/session` возвращает пользователя, актуальную роль, permissions и
   CSRF token только после его сверки с hash серверной сессии.
3. Любой `POST`/`PUT`/`PATCH`/`DELETE` под `/api/v1`, включая повторный login и logout,
   передаёт CSRF token в `X-CSRF-Token`. Первый login без действующей сессии и OIDC
   callback являются явными исключениями.
4. `POST /api/v1/auth/logout` отзывает серверную сессию и очищает обе cookies.

Defaults перечислены в `.env.example`: absolute TTL — 28800 секунд, idle TTL — 1800,
touch interval — 60, SameSite — `lax`, Secure — `false` только для разработки. CORS
при credentials использует точный список origins; wildcard запрещён. Для запуска
полного набора на отдельной БД используется `TEST_DATABASE_URL`, как показано выше.

Для production используйте HTTPS, `Secure` cookies, точные CORS origins и случайные
секреты длиной не менее 32 байт. Известный development OIDC secret отклоняется;
`SameSite=None` разрешён только вместе с `Secure=true`.

## Административный API

Все административные маршруты находятся под `/api/v1`. Guest может читать реестры,
Administrator может изменять их. Изменяющие запросы всегда передают CSRF token из
`GET /api/v1/auth/session` в заголовке `X-CSRF-Token`.

| Область | Чтение | Изменение |
| --- | --- | --- |
| Users и roles | `users.read` | `users.write` |
| OIDC role mappings | `users.read` | `users.write` |
| App settings | `settings.read` | `settings.write` |
| Normalizers | `normalizers.read` | `normalizers.write` |

Guest — это аутентифицированный пользователь с read-only ролью. После local login он
может, например, получить список нормализаторов через ту же cookie-сессию:

```powershell
$web = New-Object Microsoft.PowerShell.Commands.WebRequestSession
Invoke-RestMethod http://localhost:8000/api/v1/auth/local/login -Method Post -WebSession $web `
  -ContentType application/json -Body '{"username":"guest","password":"guest-password"}'
Invoke-RestMethod http://localhost:8000/api/v1/normalizers -WebSession $web
```

После local login Administrator получает CSRF token и может создать local user. Пароль
при этом никогда не входит в ответ API:

```powershell
$session = Invoke-RestMethod http://localhost:8000/api/v1/auth/session -WebSession $web
$headers = @{ "X-CSRF-Token" = $session.csrf_token }
Invoke-RestMethod http://localhost:8000/api/v1/users -Method Post -WebSession $web -Headers $headers `
  -ContentType application/json -Body '{"username":"analyst","password":"long-enough-password","display_name":"Analyst","role_id":"00000000-0000-4000-8000-000000000002"}'
```

`PUT /api/v1/app-settings/{key}` и `PATCH /api/v1/normalizers/{id}` требуют актуальное
поле `version`. При устаревшей версии API возвращает `409 version_conflict` с
`details.current_version`; повторите запрос после чтения текущей записи. Settings
создаются только кодом/миграциями: HTTP API позволяет читать и менять лишь уже
зарегистрированные keys. Поле `rule` normalizer является JSONB-объектом v1;
его структура и ECS-совместимость проверяются перед созданием или заменой.

Полный набор проверок запускается на отдельной БД:

```powershell
$env:TEST_DATABASE_URL = "postgresql+psycopg://diploma_user:diploma_password@localhost:5432/diploma_test"
$env:DATABASE_URL = $env:TEST_DATABASE_URL
python -m pytest -q
python -m alembic upgrade head
python -m alembic check
```

## Kafka connections and sources

Kafka management is available under `/api/v1` and uses the existing permission pairs
`connections.read`/`connections.write` and `sources.read`/`sources.write`. The API accepts
only `PLAINTEXT` connections; credentials, URLs, arbitrary Kafka options and secrets are
rejected. Metadata requests use `KAFKA_METADATA_TIMEOUT_SECONDS` (1–30 seconds).

The normal workflow is:

1. Create a connection with one or more validated `host:port` bootstrap servers.
2. Call `POST /api/v1/kafka-connections/{id}/test` and optionally list topics with
   `GET /api/v1/kafka-connections/{id}/topics`.
3. Create a disabled source for an existing topic.
4. Assign a normalizer with `PUT /api/v1/sources/{id}/normalizer`, then enable the source.
5. Disable or delete the source when it is no longer needed.

Creating a source and enabling it perform a live metadata check. Topic listing filters
internal topics beginning with `__` unless `include_internal=true`; it never creates a
source or starts a Kafka consumer. Deleting a connection cascades its sources while
preserving parsed logs with nullable foreign keys.

## Parsed logs and ECS catalog

The API vendors the generated Elastic Common Schema artifact from the immutable
[`v9.4.0` release](https://github.com/elastic/ecs/tree/v9.4.0). The original YAML,
upstream license, retrieval date, source URL and SHA-256 are stored under
`app/resources/ecs/`. Runtime catalog loading is local/offline; the same catalog API can
later power normalizer field suggestions and validation. The source is
`generated/ecs/ecs_flat.yml` at tag `v9.4.0` (`ecs_flat_v9.4.0.yml` in this repository),
retrieved 2026-09-23; SHA-256:
`f2c78b7c68503d42f5713ebea63fbfafbbeb7056f0d3ec7208c9cc2202ba7d8f`.

All endpoints below require an authenticated session. Catalog, search and detail require
`events.read`; single and bulk deletion require `events.delete`. Every POST (including the
read-only search) and DELETE request must send the session-bound token in
`X-CSRF-Token` as returned by `GET /api/v1/auth/session`.

| Endpoint | Purpose |
| --- | --- |
| `GET /api/v1/ecs/schema` | ECS version, field count and safe artifact provenance |
| `GET /api/v1/ecs/fields?q=&type=&level=&filterable=&limit=50&offset=0` | Search/browse the sorted field catalog |
| `GET /api/v1/ecs/fields/{field_name}` | Read one dotted ECS field (including `@timestamp`) |
| `POST /api/v1/parsed-logs/search` | Search normalized logs |
| `GET /api/v1/parsed-logs/{log_id}` | Read full raw and ECS payload for one event |
| `DELETE /api/v1/parsed-logs/{log_id}` | Delete one event |
| `POST /api/v1/parsed-logs/actions/delete` | Delete by source, time interval, or both |

Search combines ID/topic/partition groups (OR within a group, AND between groups),
half-open time bounds `[from,to)`, literal raw substring search and up to 20 ECS field
filters. `%`, `_` and backslash in `raw_query` are treated literally. Example:

```json
{
  "raw_query": "authentication failure",
  "processed_from": "2026-01-01T00:00:00Z",
  "processed_to": "2026-02-01T00:00:00Z",
  "ecs_filters": [
    {"field": "event.action", "operator": "eq", "value": "login"},
    {"field": "source.ip", "operator": "eq", "value": "192.0.2.10"}
  ],
  "limit": 50,
  "cursor": null
}
```

Available ECS operators are published with each field. String-like fields support
`eq`, `neq`, case-insensitive substring `contains`, `exists` and `not_exists`; numbers
support equality and ordered comparisons; dates support timezone-aware equality and ordered
comparisons; booleans support equality; IP addresses support equality. Array-capable leaf
fields additionally support `contains`; on every array, `eq` tests exact typed membership
and `neq` requires an existing array without that element. For string-like arrays,
`contains` performs a case-insensitive substring search in string elements; for numeric,
boolean, date and IP arrays it is typed membership. Invalid IPs and dates are rejected for
every operator; integer ECS fields accept JSON integers only. `exists` means the JSON path is
present, including an explicit JSON `null`; `not_exists` means the path is absent (a present
JSON `null` is therefore considered to exist). Malformed stored values do not match value
comparisons (`eq`, `neq` or `contains`).
Unsupported/object ECS types remain discoverable but are not filterable. Field names are
looked up in the trusted catalog before being translated to parameterized JSONB queries;
clients cannot submit SQL, casts or JSON paths.

Results are ordered by `backend_processed_at DESC, id DESC`. The first page captures a
database-time `created_at` cutoff, and every continuation applies that same cutoff. This
excludes ordinary later inserts and backfills whose `created_at` is after the cutoff, but it
is not a strict database snapshot: a transaction that commits later with `created_at` at or
before the cutoff may appear on a later page, and deletions between pages can remove rows.
The signed opaque cursor carries the cutoff and sort position, and binds both to the normalized
filter fingerprint (but not page size); changing filters, tampering with the cursor or
malformed/legacy cursor data returns `422 invalid_cursor`. A new search captures a new cutoff.
The result
has `items`, `has_more` and `next_cursor`, with no expensive total count. Search summaries
contain at most 500 characters of raw text and the ECS version, never the full raw payload,
ECS object or deduplication key. The detail endpoint returns those full event fields.

Delete one returns `204` or `404 parsed_log_not_found`. Bulk deletion requires at least a
source UUID or a complete non-empty `[processed_from,processed_to)` range; the only supported
scopes are source, range, or their intersection. An empty/global delete is rejected with
`422 deletion_scope_required`. Bulk deletion is one set-based database delete in one
transaction. Migration `0003` installs `pg_trgm`, backfills immutable normalizer-name
snapshots (using `legacy-unknown` only when the old relation cannot provide a name), and
adds a GIN trigram index for raw substring search. Apply it with the normal
`python -m alembic upgrade head` workflow.

```http
DELETE /api/v1/parsed-logs/7ca4e014-175a-4bfe-8e3f-5de190868647
X-CSRF-Token: <session csrf token>
```

```http
POST /api/v1/parsed-logs/actions/delete
X-CSRF-Token: <session csrf token>
Content-Type: application/json

{"source_id":"7ca4e014-175a-4bfe-8e3f-5de190868647"}
```

```json
{
  "source_id": null,
  "processed_from": "2026-01-01T00:00:00Z",
  "processed_to": "2026-02-01T00:00:00Z"
}
```

## JSON-нормализация (TASK-6)

Миграция `0004` сохраняет прежний произвольный текст как
`{"format_version":0,"legacy_rule_text":"..."}`. Такой normalizer доступен для
чтения и редактирования, но новые активации источника с ним получают
`409 normalizer_legacy_incompatible`. PATCH описания не делает правило
исполняемым: администратор должен заменить `rule` валидным объектом v1.
Downgrade восстанавливает исходный текст legacy-правила, а v1 — как JSON-текст.
Миграция не меняет `sources.is_enabled`.

`GET /api/v1/normalizers/block-types` публикует режимы и параметры блоков
(`normalizers.read`). `GET /api/v1/ecs/fields` и detail существующего каталога
дают `mappable` и `mappable_reason`; поиск и пагинация сохраняются, можно
передать `mappable=true`. `filterable` относится только к поиску событий и не
подменяет `mappable`. `POST /api/v1/normalizers/preview` (`normalizers.write`,
session-bound CSRF) принимает несохранённые `rule` и `sample`, не меняя БД:

```json
{
  "rule": {
    "format_version": 1,
    "variants": [{
      "key": "web", "priority": 10,
      "when": {"kind": "prefix", "value": "2025-"},
      "blocks": [
        {"key": "columns", "kind": "columns", "candidates": [{
          "delimiter": " ", "columns": ["date", "time", "ip", "status"]
        }]},
        {"key": "source_ip", "kind": "map_ecs", "target": "source.ip",
         "source": {"ref": "columns.ip"}, "required": true},
        {"key": "http_status", "kind": "map_ecs", "target": "http.response.status_code",
         "source": {"ref": "columns.status"}, "required": true},
        {"key": "event_time", "kind": "map_ecs", "target": "@timestamp",
         "source": {"refs": ["columns.date", "columns.time"], "join": " "},
         "transform": {"date_format": "%Y-%m-%d %H:%M:%S", "timezone": "UTC"},
         "required": true}
      ]
    }]
  },
  "sample": {"timestamp": "2025-06-01T12:01:00Z", "log": "2025-06-01 12:00:00 192.0.2.9 404"}
}
```

Варианты сортируются по уникальному числовому `priority`; первый совпавший
`when` (`prefix`, `contains`, RE2 `regex`) исполняется без перехода к следующему
при ошибке. Блоки идут по порядку массива. `regex` использует RE2 именованные
группы, `template` — литералы и `%{name}`, `columns` — ручной порядок колонок,
`json` — разрешённые dotted paths с альтернативами `requires`/`extract`.
Каждый extractor может иметь до 8 кандидатов, первый подходящий выигрывает;
`set` задаёт константные выходы кандидата. У `map_ecs` источник — `ref`,
`literal` или `refs` с `join`/`as_array`. Ключи блоков стабильны и применяются
для ссылок и диагностики, например `columns.ip`. Вложенный разбор выглядит так:

```json
[
  {"key":"outer","kind":"regex","candidates":[{"pattern":"payload=(?P<body>\\{.*\\})"}]},
  {"key":"inner","kind":"json","input":"outer.body","candidates":[
    {"requires":["event.action"],"extract":{"action":"event.action"}},
    {"requires":["action"],"extract":{"action":"action"}}
  ]},
  {"key":"action","kind":"map_ecs","target":"event.action",
   "source":{"ref":"inner.action"},"required":true}
]
```

Результат содержит `status` (`complete`/`partial`/`failed`), `variant_key`,
`diagnostics`, компактный `trace` и отдельный `fluent_bit_collected_at`.
`ecs_data` выдаётся только при `complete`/`partial`; `event.original` и
`ecs.version` задаёт система. Если событие не содержит времени и
`@timestamp` не объявлен обязательным, используется время сбора Fluent Bit,
статус `partial` с `event_time_fallback`. Обязательное невалидное время даёт
`failed`. Текущие лимиты: правило и `log` по 64 KiB, вложенность JSON 16,
варианты 16, блоки/вариант 32, кандидаты/блок 8, паттерн 1024 символа,
кэш компиляции 128 записей. Неизвестные параметры и поля, небезопасный для RE2
синтаксис, битые/будущие ссылки и несовместимые типы отвергаются до сохранения
с адресом варианта/блока/параметра. Движок не читает Kafka и не записывает
`parsed_logs`: подключение consumer и хранение результатов относятся к
последующим задачам.
