from multiprocessing import freeze_support
from gdelt import gdelt
from datetime import datetime, timedelta, timezone
from pyspark.sql import SparkSession
import time
import warnings

def get_interval():
    now = datetime.now(timezone.utc) - timedelta(hours = 6)
    rounded_minute = (now.minute // 5) * 5

    interval = now.replace(
        minute=rounded_minute,
        second=0,
        microsecond=0
    )

    return interval


def get_gdelt_data(gd2, date_string):
    data = []

    tables = ["mentions"]

    for table in tables:
        try:
            results = gd2.Search(
                date_string,
                table=table,
                output="json"
            )

            for row in results:
                data.append({
                    "source_table": table,
                    "interval_time": date_string,
                    "collected_at": datetime.now(timezone.utc).isoformat(),
                    "raw_data": row
                })

            print(f"{table}: {len(results)} registros")

        except Exception as e:
            print(f"Error consultando {table}: {e}")

    return data


def process_with_spark(spark, data):
    if not data:
        print("No hay datos para procesar.")
        return

    df = spark.createDataFrame(data)

    df.printSchema()
    df.show(5, truncate=False)

    # Aquí haces tus transformaciones reales
    processed_df = df.select(
        "source_table",
        "interval_time",
        "collected_at",
        "raw_data"
    )

    processed_df.write \
        .format("mongodb") \
        .option("connection.uri", "mongodb://localhost:27017/") \
        .option("database", "local") \
        .option("collection", "processed_results") \
        .mode("append") \
        .save()


def main():
    warnings.filterwarnings("ignore")

    spark = SparkSession.builder \
        .remote("sc://localhost:15002") \
        .appName("GDELTAnalyzer") \
        .config(
            "spark.jars.packages",
            "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0"
        ) \
        .config(
            "spark.mongodb.write.connection.uri",
            "mongodb://mongodb:27017/"
        ) \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.maxResultSize", "4g") \
        .getOrCreate()

    gd2 = gdelt(version=2)

    while True:
        interval = get_interval()
        date_string = interval.strftime("%Y %b %d %H:%M")

        print(f"\nConsultando GDELT para: {date_string}")

        data = get_gdelt_data(gd2, date_string)

        process_with_spark(spark, data)

        print("Esperando 15 minutos...")
        time.sleep(15 * 60)


if __name__ == "__main__":
    freeze_support()
    main()