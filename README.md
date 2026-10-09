# Real-Time Fraud Detection System

Репозиторий содержит решение домашней работы 1: систему скоринга фродовых транзакций с использованием Kafka, CatBoost и PostgreSQL.

## Архитектура

Поток проходит через `interface`, топик `transactions`, `fraud-detector`, топик `scores`, `scores-consumer` и PostgreSQL. Результаты читает `interface`.

Компоненты системы:

1. **`interface`** - Streamlit UI:
   - загружает `.csv` с колонками `test.csv`;
   - генерирует UUID для каждой транзакции;
   - отправляет строки отдельными JSON-сообщениями в `transactions`;
   - выводит результаты из PostgreSQL.

2. **`fraud-detector`** - ML-сервис:
   - читает `transactions`;
   - выполняет препроцессинг;
   - применяет сохраненную CatBoost-модель `artifacts/model.cbm`;
   - записывает `transaction_id`, `score` и `fraud_flag` в `scores`.

3. **`scores-consumer`** - сервис записи результатов:
   - читает `scores`;
   - проверяет контракт сообщения;
   - сохраняет результат в PostgreSQL;
   - подтверждает Kafka offset после успешной записи.

4. **Kafka infrastructure**:
   - один Kafka брокер в режиме KRaft, без Zookeeper;
   - `kafka-init` создает `transactions` и `scores`;
   - Kafka UI доступен на порту 8080.

5. **PostgreSQL**:
   - хранит идентификаторы транзакций, скоры и флаги;
   - инициализирует таблицу при первом запуске.

Модель выполняет только инференс на CPU.

## Быстрый старт

### Требования

- Docker Engine 24+;
- Docker Compose v2.

### Запуск

Из корня репозитория:

```bash
cp .env.example .env
docker compose up --build -d
docker compose ps -a
```

`kafka-init` должен завершиться со статусом `Exited (0)`. Остальные сервисы должны быть в состоянии `Up`; Kafka и PostgreSQL дополнительно получают статус `healthy`.

После запуска доступны:

- Streamlit UI: <http://localhost:8501>
- Kafka UI: <http://localhost:8080>

## Использование

### 1. Загрузка данных

1. Откройте Streamlit UI.
2. На вкладке `Загрузить данные` выберите `data/test_sample.csv` или другой `.csv`
   с колонками соревнования https://www.kaggle.com/competitions/teta-ml-1-2025.
3. Укажите число строк и нажмите `Отправить на скоринг`.

Каждая строка отправляется в Kafka отдельным сообщением. Поля исходной транзакции:

```text
transaction_time, merch, cat_id, amount, name_1, name_2, gender, street,
one_city, us_state, post_code, lat, lon, population_city, jobs,
merchant_lat, merchant_lon
```

### 2. Мониторинг

Kafka UI показывает топики `transactions`, `scores`, сообщения и consumer groups.
Логи сервисов доступны через Docker Compose:

```bash
docker compose logs --tail=100 fraud-detector
docker compose logs --tail=100 scores-consumer
docker compose logs --tail=100 interface
```

### 3. Результаты

На вкладке `Результаты` кнопка `Посмотреть результаты` выводит:

- 10 последних транзакций с `fraud_flag = 1`;
- гистограмму по выборке `ORDER BY created_at DESC LIMIT 100`;
- количество записей, средний и максимальный скор, число превышений порога.

## Формат Kafka-сообщений

Сообщение в `transactions`:

```json
{
  "transaction_id": "4f15286a-84f0-4db6-a7bb-f4737c769a12",
  "data": {
    "transaction_time": "2019-09-14 02:46",
    "merch": "fraud_Stokes, Christiansen and Sipes",
    "cat_id": "grocery_net",
    "amount": 25.79
  }
}
```

Поле `data` содержит все перечисленные в разделе загрузки колонки.

Сообщение в `scores` содержит ровно три поля:

```json
{
  "transaction_id": "4f15286a-84f0-4db6-a7bb-f4737c769a12",
  "score": 0.995,
  "fraud_flag": 1
}
```

По умолчанию `fraud_flag = 1` при `score > 0.98`. Значение порога задается через
`FRAUD_THRESHOLD` в `.env`.

## Preprocessing и модель

В решении с семинара использовался CatBoost и следующие преобразования:

- час, год, месяц, день месяца и день недели;
- объединение редких категорий в `cat_50+`;
- mean encoding категориальных и временных признаков;
- great-circle distance между клиентом и продавцом;
- заполнение числовых пропусков средним;
- преобразование `log(x + 1)` для `amount`, `population_city` и `distance`;
- порог классификации `0.98`.

Данная логика сохранена. Здесь mappings, mean encodings и средние значения заранее записаны в `fraud_detector/artifacts/preprocessing.json`.

Для пересборки:

```bash
python scripts/build_preprocessing_artifacts.py \
  /path/to/teta-ml-1-2025.zip \
  fraud_detector/artifacts/preprocessing.json
```

## PostgreSQL

Таблица создается файлом `database/init.sql`:

```text
transaction_scores
transaction_id UUID PRIMARY KEY
score DOUBLE PRECISION
fraud_flag SMALLINT
created_at TIMESTAMPTZ
```

Повторная доставка сообщения обновляет существующую запись по `transaction_id`.

## Проверка компонентов

Список топиков:

```bash
docker compose exec kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:29092 --list
```

Первые пять сообщений из `scores`:

```bash
docker compose exec kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:29092 \
  --topic scores --from-beginning --max-messages 5
```

Последние результаты в PostgreSQL:

```bash
docker compose exec postgres psql \
  -U fraud_user -d fraud_detection \
  -c "SELECT * FROM transaction_scores ORDER BY created_at DESC LIMIT 10;"
```

## Тесты

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
pytest -q
```

Тесты проверяют контракты Kafka, preprocessing, неизвестные категории и инференс сохраненной модели.

## Структура проекта

```text
data/test_sample.csv
database/Dockerfile
database/init.sql
fraud_detector/app/
fraud_detector/artifacts/model.cbm
fraud_detector/artifacts/preprocessing.json
fraud_detector/Dockerfile
fraud_detector/requirements.txt
interface/app.py
interface/Dockerfile
interface/requirements.txt
scores_consumer/app/
scores_consumer/Dockerfile
scores_consumer/requirements.txt
scripts/build_preprocessing_artifacts.py
tests/
docker-compose.yml
requirements-dev.txt
README.md
```

## Настройки Kafka

```yaml
topics:
  transactions:
    partitions: 3
    replication_factor: 1
  scores:
    partitions: 3
    replication_factor: 1
```

## Остановка

Остановить контейнеры с сохранением данных:

```bash
docker compose down
```

Удалить контейнеры и volumes:

```bash
docker compose down -v
```

## Примечание

Для запуска должны быть свободны порты 8080, 8501, 9092 и 5432. Модель и
preprocessing-артефакт уже включены в репозиторий.
