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
