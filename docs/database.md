# Тестовая PostgreSQL и миграции

`diploma_db` — база разработки. Не запускайте на ней тесты, сброс с удалением данных или
откаты миграций. Полный набор тестов бэкенда использует только `diploma_test`; данные этой БД
временные. Скрипт проверяет имя и сбрасывает только эту тестовую базу, а после
прогона оставляет пустую схему на текущей ревизии Alembic. Не запускайте два набора одновременно
на общей `diploma_test`.

```powershell
$env:TEST_DATABASE_URL = "postgresql+psycopg://diploma_user:<пароль>@localhost:5432/diploma_test"
python backend/scripts/run_tests.py --shared
```

Для независимого ревью используйте `python backend/scripts/run_tests.py --temporary`:
скрипт создаёт уникальную временную БД и удаляет её после завершения. Если отмеченная
ревизия БД структурно не соответствует миграциям, не исправляйте ревизию вручную и
не запускайте набор поверх неё: штатный скрипт пересоздаёт только `diploma_test`.

При развёртывании миграции запускает одноразовый Compose-сервис `migrate` до API. Схему
меняют только Alembic-миграции; `create_all()` не используется. Для ручного запуска
задайте `DATABASE_URL` именно целевой БД и выполните из `backend/`:

```shell
python -m alembic upgrade head
python -m alembic current
```

Никогда не запускайте `alembic downgrade` на `diploma_db`. Для локальной разработки
используйте корневой [README](../README.md) и dev override Compose.
