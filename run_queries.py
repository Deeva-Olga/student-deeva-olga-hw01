from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("TrinoValidation") \
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

print("=" * 60)
print("1. АНАЛИТИЧЕСКИЙ ЗАПРОС (Trino/Spark эквивалент с DECIMAL)")
print("=" * 60)
spark.sql("""
    SELECT 
        member_casual, 
        ROUND(AVG(CAST(duration_sec AS DECIMAL(18,2))) / 60.0, 2) as avg_duration_minutes, 
        COUNT(*) as trip_count
    FROM minio.lakehouse.divvy_iceberg
    WHERE member_casual IS NOT NULL
    GROUP BY member_casual
    ORDER BY trip_count DESC
""").show(truncate=False)

print("=" * 60)
print("2. ПРОВЕРКА ИСТОРИИ SNAPSHOTS (синтаксис Trino)")
print("=" * 60)
# В Trino это выглядит как lakehouse.student_01."divvy_iceberg$snapshots"
spark.sql("""
    SELECT snapshot_id, committed_at 
    FROM minio.lakehouse.divvy_iceberg.snapshots 
    ORDER BY committed_at DESC
""").show(truncate=False)

print("=" * 60)
print("3. ФЕДЕРАТИВНЫЙ JOIN (эмуляция каталога memory через TempView)")
print("=" * 60)
# Эмуляция справочника, который в Trino лежал бы в каталоге memory.default
spark.sql("""
    CREATE OR REPLACE TEMP VIEW member_types AS
    SELECT 'member' AS type_code, 'Годовой подписчик Divvy' AS description UNION ALL
    SELECT 'casual' AS type_code, 'Разовый пользователь (24h/3h pass)' AS description
""")

spark.sql("""
    SELECT 
        COALESCE(mt.description, 'Не сопоставлен') AS client_type_description,
        d.member_casual AS raw_code,
        COUNT(d.ride_id) AS total_trips
    FROM minio.lakehouse.divvy_iceberg d
    LEFT JOIN member_types mt 
        ON d.member_casual = mt.type_code
    GROUP BY COALESCE(mt.description, 'Не сопоставлен'), d.member_casual
    ORDER BY total_trips DESC
""").show(truncate=False)

print("Все SQL-запросы успешно выполнены.")
