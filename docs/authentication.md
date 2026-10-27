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

### Управление OIDC через интерфейс

#### Отдельный Docker Keycloak в локальной разработке

1. Проверьте discovery Keycloak по
   `http://localhost:8081/realms/master/.well-known/openid-configuration`:
   значение `issuer` должно быть `http://localhost:8081/realms/master`.
   В корневом dev `.env` задайте
   `OIDC_DEV_HTTP_ISSUER=http://localhost:8081/realms/master` и
   `PUBLIC_ORIGIN=http://localhost:8080`. API внутри dev обращается к IdP через
   `host.docker.internal` на том же порту, но в интерфейсе issuer вводится
   только как `http://localhost:8081/realms/master`.
2. В том же realm создайте OIDC-клиент с Authorization Code Flow. В Keycloak
   укажите **Valid redirect URIs**:
   `http://localhost:8080/api/v1/auth/oidc/callback`, **Web origins**:
   `http://localhost:8080`. Для этого клиента оставьте поле **PKCE method / Proof
   Key for Code Exchange Code Challenge Method** пустым: текущая версия приложения
   использует серверный Authorization Code Flow с client secret, но не отправляет
   PKCE `code_challenge`. Обязательные `S256` и `plain` приводят к отказу входа.
   Это ограничение не относится к другим клиентам IdP. Root URL и Home URL не
   заменяют issuer и callback.
   Client ID и Client secret возьмите из настроек этого клиента; секрет не
   записывайте в документацию.
3. В «Администрирование» → «Способы входа» укажите issuer realm, Client ID,
   точный Callback URL, scopes с `openid` и Client secret. Сохраните черновик,
   проверьте discovery, выполните пробный вход и активируйте подтверждённую
   версию. Для повторного включения после отключения повторите эти шаги.
   `OIDC_ENABLED=true` для управления через интерфейс не требуется.

Текущий отдельный тестовый Keycloak опубликован на всех интерфейсах хоста
(`0.0.0.0:8081`, возможно также `[::]:8081`). Используйте его только в
доверенной локальной сети. Для ограничения доступа можно привязать порт к
loopback хоста, **проверив после этого доступность из API по
`host.docker.internal`**, либо перейти на TLS с доверенным CA. Изменение
публикации порта требует отдельного изменения пользовательского контейнера.
Если API отвечает `oidc_key_unavailable`, восстановите прежний постоянный
`EXTERNAL_SECRET_KEY` в защищённом файле или корневом dev `.env`, затем
обновите состояние. Не заменяйте ключ случайным новым значением.

Администратор открывает «Администрирование» → «Способы входа». Локальный вход остаётся резервным.
Отдельного маршрута `/settings` больше нет; API `app-settings` сохраняется для следующей задачи.
Для одного OIDC-провайдера задаются issuer, client ID, точный callback URL и scopes. Client secret
вводится только при установке или замене; после сохранения показывается лишь признак его наличия.
Пустое поле оставляет сохранённый секрет. Публичный корпоративный CA можно загрузить в PEM для
этого провайдера; при его отсутствии используется системное доверие. Сертификат сервера и имя
хоста проверяются всегда.

Порядок переключения: сохранить черновик → проверить discovery, endpoint, JWKS и TLS → выполнить
пробный вход в браузере → активировать подтверждённую версию. Проверка discovery сама по себе не
доказывает успешный вход. Пробный вход не создаёт пользователя, не меняет роли и не заменяет
локальную сессию администратора. При неуспешной проверке действующий вход остаётся прежним.
Отключение OIDC и замена активного провайдера требуют подтверждения. После явного отключения
OIDC остаётся отключённым и после рестарта, даже если `OIDC_ENABLED=true` в окружении.

До первой явной активации через интерфейс прежняя конфигурация `OIDC_*` из окружения и файлов
хоста остаётся действующей. Сохранение черновика не меняет приоритет. После активации приоритет
имеет сохранённая конфигурация; она применяется без перезапуска API. При потере ключа расшифровки
управляемый OIDC недоступен, а локальный администратор может войти и восстановить файл ключа.
Нельзя заменять ключ новым без процедуры восстановления зашифрованных данных.

В production на защищённом Linux-хосте остаются `DATABASE_URL_FILE`, `OIDC_STATE_SECRET_FILE`,
`EXTERNAL_SECRET_KEY_FILE`, файлы Kafka, TLS-ключ proxy и другие инфраструктурные секреты.
`EXTERNAL_SECRET_KEY_FILE` содержит существующий Fernet-ключ для шифрования client secret в БД.
`OIDC_STATE_SECRET_FILE` служит только для подписи краткоживущего handshake и не заменяет ключ
шифрования. Старый `OIDC_CLIENT_SECRET_FILE` используется только действующим env-провайдером;
текущий production Compose по-прежнему требует указать этот файл при запуске.
Права доступа к этим файлам ограничиваются на хосте; их содержимое не вводится в браузере.

Callback у IdP регистрируется в точности как публичный URL
`https://<домен>/api/v1/auth/oidc/callback` за HTTPS proxy. В локальной разработке HTTP callback
допустим только на loopback. Для восстановления доступа войдите локальным администратором,
исправьте черновик и снова пройдите проверку и пробный вход либо отключите OIDC.

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
