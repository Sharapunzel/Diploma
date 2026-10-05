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
OIDC_REDIRECT_URI=https://events.example.org/api/v1/auth/oidc/callback
OIDC_SCOPES=openid,profile,email
OIDC_SUCCESS_REDIRECT_URL=/
OIDC_ERROR_REDIRECT_URL=/login?error=oidc
```

`OIDC_REDIRECT_URI` нужно без изменений зарегистрировать у провайдера идентификации. В production
используйте публичный HTTPS-адрес; URL успешного и ошибочного перенаправления остаются на нём. OIDC
включается только при заполненных адресе провайдера, ID клиента, секрете клиента, URI перенаправления и
scope `openid`; URL перенаправления берутся только из конфигурации. Параметры state и nonce хранятся
в отдельной краткоживущей cookie (`OIDC_HANDSHAKE_TTL_SECONDS=600`).

Локальный сценарий выглядит так:

1. `POST /api/v1/auth/local/login` с JSON `username`/`password` создаёт HttpOnly
   cookie сессии и доступную для чтения CSRF cookie; сырые токены сессии и CSRF в БД не
   записываются.
2. `GET /api/v1/auth/session` возвращает пользователя, актуальную роль, разрешения и
   CSRF-токен только после его сверки с хэшем серверной сессии.
3. Любой `POST`/`PUT`/`PATCH`/`DELETE` под `/api/v1`, включая повторный вход и выход,
   передаёт CSRF-токен в `X-CSRF-Token`. Первый вход без действующей сессии и OIDC
   callback являются явными исключениями.
4. `POST /api/v1/auth/logout` отзывает серверную сессию и очищает обе cookies.

Значения по умолчанию перечислены в `.env.example`: абсолютный TTL — 28800 секунд, TTL бездействия — 1800,
интервал обновления — 60, SameSite — `lax`, Secure — `false` только для разработки. CORS
при передаче учётных данных использует точный список адресов; шаблон `*` запрещён. Для запуска
полного набора на отдельной БД используется `TEST_DATABASE_URL`, как описано в [инструкции по БД](database.md).

Для production используйте HTTPS, `Secure` cookies, точные CORS origins и случайные
секреты длиной не менее 32 байт. Известный секрет OIDC для разработки отклоняется;
`SameSite=None` разрешён только вместе с `Secure=true`.
