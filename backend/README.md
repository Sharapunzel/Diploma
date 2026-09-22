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
