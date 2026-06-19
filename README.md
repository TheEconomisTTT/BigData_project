# Потоковая обработка IoT-данных

Учебный проект по обработке потоковых данных с использованием Kafka, PostgreSQL и Apache Flink.

## Что делает проект

Генератор раз в секунду отправляет данные с IoT-устройств (температура, влажность) в Kafka. Flink читает эти события, обогащает их названиями типов устройств из PostgreSQL и агрегирует в минутные окна. Для каждого окна считается средняя температура и медиана влажности по каждому типу устройства. Результаты пишутся в другой Kafka топик.

## Технологии

- Python 3.11+
- Apache Kafka 3.9.1 (KRaft режим)
- PostgreSQL 16
- Apache Flink 1.20
- Docker Compose

## Структура проекта

```
project_2/
├── docker-compose.yml      # Инфраструктура (Kafka, Postgres, Flink)
├── .env.example           # Пример конфигурации
├── requirements.txt       # Python зависимости
├── generator/             # Генератор событий
│   ├── config.py         # Настройки генератора
│   └── producer.py       # Код генератора
├── flink/
│   ├── Dockerfile        # Образ Flink с Python и коннекторами
│   ├── config.py         # Настройки Flink job
│   └── job.py            # Код обработки данных
├── postgres/
│   ├── ddl.sql           # Создание таблицы справочника
│   └── dml.sql           # Заполнение справочника
├── scripts/              # Скрипты для запуска
└── tests/                # Unit-тесты
```

## Быстрый старт

### 1. Подготовка окружения

```bash
cd project_2

# Копируем конфиг
cp .env.example .env

# Создаем виртуальное окружение
python -m venv .venv

# Активируем (Windows)
.venv\Scripts\activate

# Или (Linux/Mac)
# source .venv/bin/activate

# Устанавливаем зависимости
pip install -r requirements.txt
```

### 2. Запуск инфраструктуры

```bash
docker compose up -d --build
```

Первая сборка займет несколько минут (Flink скачивает коннекторы).

Проверяем что все запустилось:
```bash
docker compose ps
```

Все сервисы должны быть в статусе `healthy`.

### 3. Создание Kafka топиков

```bash
bash scripts/create_topics.sh
```

Должны создаться два топика: `iot_events` и `iot_metrics`.

### 4. Запуск Flink job

```bash
bash scripts/run_flink_job.sh
```

Открываем Flink UI: http://localhost:8081
Job должен быть в статусе `RUNNING`.

### 5. Запуск генератора

Открываем новый терминал:
```bash
cd project_2
.venv\Scripts\activate
bash scripts/run_generator.sh
```

Генератор начнет отправлять события в Kafka.

### 6. Просмотр результатов

Открываем третий терминал:
```bash
bash scripts/consume_results.sh
```

Первые результаты появятся через 60-65 секунд (минутное окно + 5 секунд watermark).

## Формат данных

### Входные данные (iot_events)

```json
{
  "device_type_id": 1,
  "event_time": "2026-06-13T12:30:15.123Z",
  "temperature": 23.7,
  "humidity": 58.2
}
```

| Поле | Тип | Описание |
|------|-----|----------|
| device_type_id | int | ID типа устройства (1, 2, 3) |
| event_time | string | Время события (ISO 8601, UTC) |
| temperature | float | Температура в °C |
| humidity | float | Влажность в % |

### Выходные данные (iot_metrics)

```json
{
  "minute": "2026-06-13T12:30:00.000Z",
  "device_type": "temperature_sensor",
  "avg_temperature": 23.45,
  "median_humidity": 57.8
}
```

| Поле | Тип | Описание |
|------|-----|----------|
| minute | string | Начало минутного окна |
| device_type | string | Название типа устройства |
| avg_temperature | float | Средняя температура |
| median_humidity | float | Медиана влажности |

## Как работает обработка

1. **Чтение из Kafka** - Flink читает события из топика `iot_events`
2. **Watermark** - Устанавливается watermark = event_time - 5 секунд для обработки опоздавших событий
3. **Lookup Join** - Каждое событие обогащается названием типа устройства из PostgreSQL
4. **Фильтрация** - Отбрасываются значения вне диапазонов (температура -100..100, влажность 0..100)
5. **Окна** - События группируются в минутные tumbling окна по event time
6. **Агрегация** - Для каждого окна считается AVG(temperature) и медиана(humidity)
7. **Запись в Kafka** - Результаты пишутся в топик `iot_metrics`

## Проверка работы

### Посмотреть события в Kafka
```bash
bash scripts/consume_events.sh
```

### Проверить справочник в PostgreSQL
```bash
docker exec iot-postgres psql -U iot_user -d iot -c "SELECT * FROM device_types ORDER BY id;"
```

### Посмотреть задачи Flink
```bash
docker exec iot-flink-jobmanager flink list
```

### Запустить тесты
```bash
.venv\Scripts\activate
pytest -v
```

## Конфигурация

Основные переменные в `.env`:

| Переменная | По умолчанию | Описание |
|------------|--------------|----------|
| KAFKA_BOOTSTRAP_SERVERS | localhost:9092 | Адрес Kafka |
| KAFKA_SOURCE_TOPIC | iot_events | Входной топик |
| KAFKA_SINK_TOPIC | iot_metrics | Выходной топик |
| EVENT_INTERVAL_SECONDS | 1 | Интервал генерации событий |
| FLINK_WATERMARK_SECONDS | 5 | Задержка watermark |
| FLINK_PARALLELISM | 1 | Параллелизм Flink |

## Остановка

### Сохраняя данные
```bash
docker compose down
```

### С удалением всех данных
```bash
docker compose down -v
```

## Типичные проблемы

### Flink job не запускается
Ошибка `Could not find any factory for identifier` - пересобрать образы:
```bash
docker compose build --no-cache jobmanager taskmanager
docker compose up -d
```

### Генератор не подключается к Kafka
Проверить статус контейнеров:
```bash
docker compose ps
```
Дождаться статуса `healthy` у Kafka.

### Нет результатов в output топике
- Проверить что Flink job в статусе `RUNNING` (UI на порту 8081)
- Генератор должен отправлять события
- Подождать 65 секунд (минутное окно + watermark)

## Лицензия

Учебный проект для демонстрации работы с потоковой обработкой данных.
