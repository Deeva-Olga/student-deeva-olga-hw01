# ДЗ 1: Перевод открытой выгрузки в воспроизводимую Lakehouse-таблицу

Репозиторий содержит решение домашнего задания по построению локального Lakehouse-стенда с использованием **PySpark, MinIO (S3), Parquet и Apache Iceberg**.

##  Источник данных и исследовательский вопрос
- **Датасет:** Divvy Bike Share (Chicago), январь 2023 г.
- **Ссылка:** [Официальный S3-бакет](https://divvy-tripdata.s3.amazonaws.com/202301-divvy-tripdata.zip)
- **Лицензия:** Открытые данные (Public Domain / CC0), не содержит PII (идентификаторы анонимизированы).
- **Исследовательский вопрос:** *Как отличается средняя длительность поездки между годовыми подписчиками (`member`) и разовыми пользователями (`casual`)?*

---

##  Как воспроизвести работу с чистого стенда

### 1. Требования
- ОС: Windows (WSL2 Ubuntu) или macOS.
- Установленные пакеты: `docker`, `python3`, `python3-venv`, `wget`, `unzip`.

### 2. Подготовка окружения
Клонирование репозитория
git clone https://github.com/ТВОЙ_НИК/student-01-hw01.git
cd student-01-hw01
Создание и активация виртуального окружения
python3 -m venv venv
source venv/bin/activate
pip install pyspark==3.5.1


### 3. Запуск хранилища (MinIO)
Запускаем локальный S3-совместимый сервер. Порты привязаны строго к `127.0.0.1` (без доступа из интернета).
```
docker run -d \
  --name minio-lakehouse \
  -p 127.0.0.1:9000:9000 \
  -p 127.0.0.1:9001:9001 \
  -e "MINIO_ROOT_USER=admin" \
  -e "MINIO_ROOT_PASSWORD=supersecret123" \
  quay.io/minio/minio server /data --console-address ":9001"
```

Далее откройте в браузере http://localhost:9001, войдите с указанными логином/паролем и создайте бакет с именем lakehouse.

### 4. Загрузка данных и запуск пайплайна

## Скачивание и распаковка датасета
mkdir -p data
wget https://divvy-tripdata.s3.amazonaws.com/202301-divvy-tripdata.zip -O data/bikes.zip
unzip -o data/bikes.zip -d data/

## Запуск основного ETL-пайплайна (Raw -> DQ -> Parquet -> Iceberg)
python pipeline.py

## Запуск проверочных SQL-запросов (аналог Trino CLI через Spark SQL)
python run_queries.py


###  Структура репозитория
student-01-h01/

├── README.md            # Этот файл

├── report.md            # Паспорт источника, таблица замеров, архитектурные выводы

├── schema.md            # Описание полей, типов данных и правил nullable

├── pipeline.py          # Основной скрипт: загрузка, DQ, Parquet, 2x Iceberg INSERT

├── run_queries.py       # Скрипт валидации SQL-запросов (эмуляция Trino)

├── queries.sql          # SQL-код аналитического и федеративного (LEFT JOIN) запросов

├── .gitignore           # Правила исключения мусора и секретов из Git

└── evidence/            # Текстовые доказательства выполнения (логи, листинги)

    ├── compose-ps.txt
    
    ├── object-listing.txt
    
    ├── spark-result.txt
    
    └── trino-result.txt
