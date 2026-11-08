# Лабораторная работа №3 — Streaming processing с Apache Flink

## Что реализовано

Пайплайн соответствует заданию из исходного `README.md`:

1. 10 CSV-файлов, по 1000 строк в каждом.
2. Python Producer читает каждую строку CSV и отправляет отдельное JSON-сообщение в Kafka topic `mock_data`.
3. Apache Flink читает Kafka в streaming-режиме.
4. Flink преобразует сообщения в модель «звезда».
5. Flink записывает измерения и факты в PostgreSQL через JDBC.

Состав модели:
- `fact_sales`
- `dim_customer`
- `dim_seller`
- `dim_product`
- `dim_store`
- `dim_supplier`
- `dim_date`

`event_id` создаётся Producer из номера файла и номера строки. Исходный `id` повторяется между файлами, поэтому он сохраняется отдельно как `original_id`, но не используется как глобальный первичный ключ факта.

## Требования

- Docker Desktop с Docker Compose.
- Доступ к интернету для загрузки Docker-образов и Maven-зависимостей при первой сборке.

Java и Maven на Windows устанавливать не требуется: Maven-сборка Flink-приложения выполняется в Docker multi-stage build.

## Запуск

Открой PowerShell в корне `BigDataFlink-main`.

Проверь конфигурацию:

```powershell
docker compose config -q
```

Собери образы и запусти инфраструктуру:

```powershell
docker compose up --build -d
```

Посмотри состояние контейнеров:

```powershell
docker compose ps -a
```

Проверь отправку сообщений Producer:

```powershell
docker compose logs producer
```

В конце лог Producer должен содержать:

```text
SUCCESS: produced 10000 JSON messages to topic 'mock_data'
```

Проверь отправку Flink-джобы:

```powershell
docker compose logs flink-submit
```

Важно: сообщение Producer `SUCCESS` означает, что 10 000 JSON-сообщений отправлены в Kafka. Flink обрабатывает их асинхронно. Посмотри прогресс Sink:

```powershell
docker compose logs -f flink-taskmanager
```

Дождись строки `Flink sink processed 10000 events`, затем останови просмотр логов сочетанием `Ctrl+C` (контейнеры продолжат работать).

Flink Web UI:

- http://localhost:8083

В интерфейсе должна быть запущенная задача `BigDataFlink: Kafka -> Flink -> PostgreSQL Star Schema`.

## Проверка PostgreSQL

Подключение из DBeaver:
- Host: `localhost`
- Port: `5435`
- Database: `bigdataflink`
- User: `student`
- Password: `student123`

Проверка из PowerShell:

```powershell
Get-Content -Raw -Encoding UTF8 .\check_lab3.sql | docker compose exec -T postgres psql -U student -d bigdataflink
```

Основная контрольная величина — 10 000 строк в `fact_sales`. Количество строк в измерениях определяется уникальностью естественных ключей исходных записей; `dim_date` содержит даты продаж.

Можно дождаться заполнения `fact_sales` автоматически из PowerShell:

```powershell
do {
    $countText = docker compose exec -T postgres psql -U student -d bigdataflink -tAc "SELECT COUNT(*) FROM fact_sales;"
    $count = [int]$countText.Trim()
    Write-Host "Flink wrote $count / 10000 rows"
    if ($count -lt 10000) { Start-Sleep -Seconds 2 }
} while ($count -lt 10000)
```

## Kafka

Внутренний адрес для контейнеров: `kafka:9092`.
Внешний адрес для инструментов на Windows: `localhost:9094`.
Топик: `mock_data`.

Каждое сообщение содержит все исходные поля CSV, а также:
- `event_id` — уникальный ключ события;
- `source_file` — имя исходного CSV;
- `source_row` — номер строки внутри файла.

## Повторный запуск с чистыми данными

ВНИМАНИЕ: следующая команда удаляет volumes PostgreSQL и Kafka, то есть очищает данные этой лабораторной.

```powershell
docker compose down -v
docker compose up --build -d
```

Не выполняй `down -v`, если нужно сохранить текущие результаты.

## Остановка без удаления данных

```powershell
docker compose down
```

## Структура проекта

```text
BigDataFlink-main/
├── docker-compose.yml
├── RUN_LAB3.md
├── check_lab3.sql
├── исходные данные/
├── postgres/
│   └── init/
│       └── 01_schema.sql
├── producer/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── producer.py
└── flink-job/
    ├── Dockerfile
    ├── pom.xml
    └── src/main/java/ru/mai/bigdataflink/
        ├── StarStreamingJob.java
        └── PostgresStarSink.java
```

## Принцип обработки

- Producer отправляет JSON-сообщения с ключом `event_id`.
- Flink Kafka Source читает топик с `earliest` offset.
- Flink преобразует JSON в структуру полей и рассчитывает ключи измерений на основе естественных идентификаторов.
- JDBC Sink сначала выполняет UPSERT измерений, затем записывает строку факта в той же транзакции.
- Первичный ключ `event_id` и `ON CONFLICT DO NOTHING` защищают факт от повторной вставки при повторной доставке Kafka-сообщения.
