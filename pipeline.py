import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType
from pyspark.sql.functions import col, year, month, unix_timestamp, lit

spark = SparkSession.builder \
    .appName("LakehouseHW1_Divvy_Final") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.1,org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.minio", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.minio.type", "hadoop") \
    .config("spark.sql.catalog.minio.warehouse", "s3a://lakehouse/iceberg_warehouse") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://127.0.0.1:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "supersecret123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

csv_file = "data/202301-divvy-tripdata.csv"

print("1. Читаем raw-данные с ПОЛНОЙ явной схемой...")
schema = StructType([
    StructField("ride_id", StringType(), True), StructField("rideable_type", StringType(), True),
    StructField("started_at", TimestampType(), True), StructField("ended_at", TimestampType(), True),
    StructField("start_station_name", StringType(), True), StructField("start_station_id", StringType(), True),
    StructField("end_station_name", StringType(), True), StructField("end_station_id", StringType(), True),
    StructField("start_lat", DoubleType(), True), StructField("start_lng", DoubleType(), True),
    StructField("end_lat", DoubleType(), True), StructField("end_lng", DoubleType(), True),
    StructField("member_casual", StringType(), True)
])

df_raw = spark.read.schema(schema).csv(csv_file)
total_rows = df_raw.count()

# 2. DQ и Reject-логика (Критерий: raw = accepted + rejected)
df_with_duration = df_raw.withColumn("duration_sec", unix_timestamp(col("ended_at")) - unix_timestamp(col("started_at")))

# Условия отбраковки
reject_condition = (col("duration_sec") < 0) | (col("duration_sec") > 86400) | col("started_at").isNull()

df_rejected = df_with_duration.filter(reject_condition).withColumn("reject_reason", lit("invalid_duration_or_null_date"))
rejected_count = df_rejected.count()

df_clean = df_with_duration.filter(~reject_condition)
accepted_count = df_clean.count()

print(f"Всего: {total_rows}, Принято: {accepted_count}, Отклонено: {rejected_count}. Сумма совпадает: {accepted_count + rejected_count == total_rows}")

# Сохраняем reject-файл (Критерий: отдельный reject-путь)
df_rejected.select("ride_id", "started_at", "ended_at", "duration_sec", "reject_reason") \
           .write.mode("overwrite").csv("s3a://lakehouse/reject/divvy_rejected/")

# 3. Raw в S3
df_raw.write.mode("overwrite").csv("s3a://lakehouse/raw/student_01/divvy/ingestion_date=2026-09-16/source.csv")

# 4. Parquet и Бенчмарк (Критерий: измерение времени)
df_clean_parquet = df_clean.withColumn("year", year(col("started_at"))).withColumn("month", month(col("started_at")))
df_clean_parquet.write.mode("overwrite").partitionBy("year", "month").parquet("s3a://lakehouse/datalake/student_01/divvy/parquet/")

print("5. Замер производительности (GroupBy count)...")
# Замер CSV
start_csv = time.time()
spark.read.schema(schema).csv(csv_file).groupBy("member_casual").count().count() # .count() в конце форсирует выполнение
time_csv = time.time() - start_csv

# Замер Parquet (сбрасываем кэш для честности, хотя Spark и так читает с диска)
start_parquet = time.time()
spark.read.parquet("s3a://lakehouse/datalake/student_01/divvy/parquet/").groupBy("member_casual").count().count()
time_parquet = time.time() - start_parquet

print(f"Время агрегации CSV:  {time_csv:.4f} сек")
print(f"Время агрегации Parquet: {time_parquet:.4f} сек")

# 6. Iceberg: ДВЕ непересекающиеся порции (Критерий: два снапшота из разных данных)
spark.sql("""
    CREATE TABLE IF NOT EXISTS minio.lakehouse.divvy_iceberg (
        ride_id STRING, rideable_type STRING, started_at TIMESTAMP, ended_at TIMESTAMP,
        start_station_name STRING, end_station_name STRING, member_casual STRING,
        duration_sec INT, year INT, month INT
    ) USING iceberg PARTITIONED BY (year, month)
""")

df_clean_parquet.createOrReplaceTempView("clean_divvy_data")

# Порция 1: Первые 100 000 строк
print("6a. Загрузка первой порции (100k строк)...")
spark.sql("""
    INSERT INTO minio.lakehouse.divvy_iceberg 
    SELECT * FROM (SELECT * FROM clean_divvy_data LIMIT 100000)
""")

# Порция 2: Оставшиеся строки (используем EXCEPT ALL для гарантии непересечения)
print("6b. Загрузка второй непересекающейся порции...")
spark.sql("""
    INSERT INTO minio.lakehouse.divvy_iceberg 
    SELECT * FROM clean_divvy_data 
    EXCEPT ALL 
    SELECT * FROM (SELECT * FROM clean_divvy_data LIMIT 100000)
""")

print("7. История снимков Iceberg:")
spark.sql("SELECT snapshot_id, committed_at, summary FROM minio.lakehouse.divvy_iceberg.snapshots").show(truncate=False)

print("Пайплайн успешно завершен.")