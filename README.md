# Система анализа событий

Система принимает события от управляемых агентов Fluent Bit через Kafka, нормализует их в ECS и хранит в PostgreSQL. Внешние ECS-документы читаются из OpenSearch-совместимого индексера. Интерфейс построен на React и FastAPI.

## Быстрый запуск development

Нужны Docker Engine с Compose и Python с зависимостями backend. Из корня репозитория подготовьте локальные Kafka credentials (PowerShell или Linux shell):

```shell
python scripts/prepare_dev_kafka.py
```

Ожидается появление файлов в неотслеживаемой `.dev/`. Перед регистрацией агентов создайте постоянный ключ `.dev/agent_credentials_key` по [инструкции агента](docs/agent-lifecycle.md). Для внешнего индексера нужен отдельный постоянный `EXTERNAL_SECRET_KEY` в корневом `.env`; его подготовка и границы хранения описаны в [развёртывании](docs/deployment.md).

После подготовки из корня репозитория выполните:

```shell
docker compose -f docker-compose.yml -f docker-compose.dev.yml up --build -d
```

Откройте `http://localhost:8080/api/v1/health/ready`: ожидается успешный ответ. При ошибке проверьте состояние и журналы по [инструкции эксплуатации](docs/deployment.md#эксплуатация). Интерфейс доступен на `http://localhost:8080`. HTTP предназначен только для локальной разработки.

Первого администратора создайте интерактивно из корня репозитория:

```shell
docker compose -f docker-compose.yml -f docker-compose.dev.yml exec api python -m app.cli create-admin --username admin --display-name Administrator
```

Команда запрашивает пароль без передачи его аргументом. После неё войдите на `/login`. При настроенном OIDC локальный вход остаётся резервным. Подробности: [аутентификация и OIDC](docs/authentication.md).

## Дальнейшие действия

| Задача | Инструкция |
| --- | --- |
| Подготовить production, TLS, секреты и запустить сервисы | [Развёртывание](docs/deployment.md#подготовка-production), [TLS](docs/tls.md) |
| Зарегистрировать агента, выдать конфигурацию и отправить логи | [Агенты](docs/agent-lifecycle.md), [локальный путь событий](docs/events.md) |
| Подключить внешний индексер и источник | [Внешние источники](docs/external-sources.md) |
| Настроить OIDC и пользователей | [Аутентификация](docs/authentication.md), [администрирование](docs/administration.md) |
| Проверить состояние, диагностировать сбой или восстановить работу | [Эксплуатация](docs/deployment.md#эксплуатация), [Kafka](docs/kafka.md), [БД](docs/database.md) |
| Запустить тестовые профили | [Проверочные профили](docs/agent-test-profiles.md) |
| Найти все тематические документы | [Указатель документации](docs/README.md) |

Не запускайте тесты на `diploma_db` и не удаляйте рабочие тома командой `down -v`. Изолированные проверки и безопасная очистка описаны в [проверочных профилях](docs/agent-test-profiles.md).
