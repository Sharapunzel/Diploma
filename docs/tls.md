# TLS-сертификаты, агенты Kafka и ротация секретов

Веб-proxy и Kafka — отдельные TLS-узлы. Nginx использует публичный веб-сертификат и ключ,
которые монтируются только в `proxy`; сертификат и ключ брокера монтируются только в `kafka`.
Публичное DNS-имя веб-сертификата должно совпадать с `PUBLIC_HOST`, имя сертификата брокера —
с `KAFKA_PUBLIC_HOST`. Внутренние контейнеры используют имена служб `api`, `postgres`, `kafka`
в закрытой сети Compose. Браузеры доверяют веб-CA, а хосты Fluent Bit — CA издателя сертификата
брокера. Не передавайте приватный ключ брокера агентам.

Production предоставляет шифрование TLS и проверку брокера, но не аутентифицирует агенты
Fluent Bit. Ограничьте доступ к порту 9094 сетями известных агентов на хостовом и сетевом
firewall: доступный listener без аутентификации остаётся риском. SASL, клиентские
сертификаты и управление ACL не настроены.

Пример Kafka output Fluent Bit с внутренним корпоративным CA:

```ini
[INPUT]
    Name   dummy
    Tag    task13.tls-smoke
    Dummy  {"timestamp":"2026-10-01T12:00:00Z","log":"TASK-13 TLS smoke"}

[OUTPUT]
    Name                                      kafka
    Match                                     task13.*
    Brokers                                   kafka.example.org:9094
    Topics                                    task13-tls-smoke-unique-id
    Format                                    json
    Timestamp_Key                             timestamp
    Timestamp_Format                          iso8601
    rdkafka.security.protocol                 ssl
    rdkafka.enable.ssl.certificate.verification true
    rdkafka.ssl.endpoint.identification.algorithm https
    rdkafka.ssl.ca.location                   /etc/fluent-bit/certs/corporate-ca.pem
```

Используйте официальный [образ `fluent/fluent-bit:5.1.2`](https://hub.docker.com/r/fluent/fluent-bit/tags)
на Linux-хосте; смонтируйте конфигурацию и корпоративный CA только для чтения и выполните
`docker run --rm --network host -v "$PWD/docs/fluent-bit-task13.conf:/fluent-bit/etc/fluent-bit.conf:ro" -v "/etc/fluent-bit/certs/corporate-ca.pem:/etc/fluent-bit/certs/corporate-ca.pem:ro" fluent/fluent-bit:5.1.2`.
Перед запуском задайте в `Topics` ранее не использованное имя. Убедитесь, что автосоздание
топиков включено, затем проверьте топик через `kafka-topics.sh` в контейнере брокера. При
настроенном Kafka-подключении приложения выполните `GET /api/v1/kafka-connections/{id}/topics`:
топик должен быть обнаружен, но оставаться незарегистрированным до явного вызова API создания
источника. Заведомо неверный CA или hostname брокера должен приводить к ошибке Fluent Bit;
не отключайте проверку. Пробная запись — JSON в неизменном конверте
`{ "timestamp": ..., "log": ... }`. `Timestamp_Key` оставляет ровно эти два ключа вместо
добавления стандартного `@timestamp` Fluent Bit; `Timestamp_Format` выдаёт строку ISO 8601,
когда время события задаёт Fluent Bit. Параметры output соответствуют [официальной документации
Kafka output](https://docs.fluentbit.io/manual/data-pipeline/outputs/kafka).

Для продления поместите новые веб-сертификат и ключ, а также Kafka PEM bundle по настроенным
путям, проверьте владельца, права и SAN, затем перезапустите только `proxy` и `kafka`.
Данные Kafka остаются в именованном томе, поэтому перезапуск listener не удаляет топики
и offsets. Сохраните DNS и правила firewall на время продления. Автоматического выпуска
сертификатов нет.

Для шифрования учётных данных внешнего индексера храните `EXTERNAL_SECRET_KEY` неизменным
и резервируйте отдельно от PostgreSQL. Для ротации остановите API, сохраните копию БД и
старого ключа, выполните `backend/scripts/rotate_external_key.py`, передав `DATABASE_URL`,
`EXTERNAL_OLD_KEY` и `EXTERNAL_NEW_KEY` через закрытые файлы/окружение, затем обновите
`EXTERNAL_SECRET_KEY_FILE` и запустите API. Храните старый ключ до проверки всех подключений.
При потере ключа сохранённые пароли нельзя прочитать. API подключений и загрузка CA описаны
в [инструкции по внешним источникам](external-sources.md).
