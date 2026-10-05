## Административный API

Все административные маршруты находятся под `/api/v1`. Гостевая роль может читать реестры,
административная — изменять их. Изменяющие запросы всегда передают CSRF-токен из
`GET /api/v1/auth/session` в заголовке `X-CSRF-Token`.

| Область | Чтение | Изменение |
| --- | --- | --- |
| Пользователи и роли | `users.read` | `users.write` |
| Соответствия ролей OIDC | `users.read` | `users.write` |
| Настройки приложения | `settings.read` | `settings.write` |
| Нормализаторы | `normalizers.read` | `normalizers.write` |

Гость — это аутентифицированный пользователь с ролью только для чтения. После локального входа он
может, например, получить список нормализаторов через ту же cookie-сессию:

```powershell
$web = New-Object Microsoft.PowerShell.Commands.WebRequestSession
Invoke-RestMethod http://localhost:8080/api/v1/auth/local/login -Method Post -WebSession $web `
  -ContentType application/json -Body '{"username":"guest","password":"guest-password"}'
Invoke-RestMethod http://localhost:8080/api/v1/normalizers -WebSession $web
```

После локального входа администратор получает CSRF-токен и может создать локального пользователя. Пароль
при этом никогда не входит в ответ API:

```powershell
$session = Invoke-RestMethod http://localhost:8080/api/v1/auth/session -WebSession $web
$headers = @{ "X-CSRF-Token" = $session.csrf_token }
Invoke-RestMethod http://localhost:8080/api/v1/users -Method Post -WebSession $web -Headers $headers `
  -ContentType application/json -Body '{"username":"analyst","password":"long-enough-password","display_name":"Analyst","role_id":"00000000-0000-4000-8000-000000000002"}'
```

`PUT /api/v1/app-settings/{key}` и `PATCH /api/v1/normalizers/{id}` требуют актуальное
поле `version`. При устаревшей версии API возвращает `409 version_conflict` с
`details.current_version`; повторите запрос после чтения текущей записи. Настройки
создаются только кодом/миграциями: HTTP API позволяет читать и менять лишь уже
зарегистрированные ключи. Поле `rule` нормализатора является JSONB-объектом v1;
его структура и ECS-совместимость проверяются перед созданием или заменой.
