from multiprocessing import freeze_support
from pyspark.sql import SparkSession
import warnings
import json

def clean_data_for_spark(data):
    if not data:
        return data

    # Obtener todas las columnas posibles
    all_keys = set()
    for row in data:
        all_keys.update(row.keys())

    # Quitar columnas donde todos los valores son None
    valid_keys = []
    for key in all_keys:
        if any(row.get(key) is not None for row in data):
            valid_keys.append(key)

    # Reconstruir filas solo con columnas válidas
    cleaned = []
    for row in data:
        cleaned_row = {}
        for key in valid_keys:
            cleaned_row[key] = row.get(key)
        cleaned.append(cleaned_row)

    return cleaned

def process_with_spark(spark, data, database, collection):
    data = clean_data_for_spark(data)

    if not data:
        print(f"No hay datos válidos para {collection}")
        return

    df = spark.createDataFrame(data)

    df.write \
        .format("mongodb") \
        .option("connection.uri", "mongodb://mongodb:27017/") \
        .option("database", database) \
        .option("collection", collection) \
        .mode("append") \
        .save()

def main():
    warnings.filterwarnings("ignore")

    spark = SparkSession.builder \
        .appName("GDELTAnalyzer") \
        .config(
            "spark.jars.packages",
            "org.mongodb.spark:mongo-spark-connector_2.13:11.1.0"
        ) \
        .config(
            "spark.mongodb.write.connection.uri",
            "mongodb://mongodb:27017/"
        ) \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.maxResultSize", "4g") \
        .getOrCreate()
    
    tables = ["events", "mentions", "gkg"]
    
    while True:
        for table in tables:
            try:
                with open('shared/' + table + '.json', 'r') as file:
                    dataString = file.read()
                    dataJSON = json.loads(dataString)
                    process_with_spark(spark, dataJSON, 'local', table)
    
            except Exception as e:
                print(f"Error consultando {table}: {e}")

if __name__ == "__main__":
    freeze_support()
    main()