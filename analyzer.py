import os
import warnings
import json
from multiprocessing import freeze_support
from pyspark.sql import SparkSession, DataFrame

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
JSONS_DIR_PATH: str = os.path.join(BASE_PATH, "shared")
WAREHOUSE_PATH: str = os.path.join(BASE_PATH, "parquets")
JSON_EXTENSION: str = ".json"

def cleanData(data: list[dict]):
    if not data:
        return data

    # Obtener todas las columnas posibles
    allKeys: set = set()
    for row in data:
        allKeys.update(row.keys())

    # Quitar columnas donde todos los valores son None
    validKeys: list = []
    for key in allKeys:
        if any(row.get(key) is not None for row in data):
            validKeys.append(key)

    # Reconstruir filas solo con columnas válidas
    cleaned: list[dict] = []
    for row in data:
        cleanedRow: dict = {}
        for key in validKeys:
            cleanedRow[key] = row.get(key)
        cleaned.append(cleanedRow)

    return cleaned

def createSparkSession() -> SparkSession:
    sparkSession = SparkSession.builder \
        .appName("GDELTAnalyzer") \
        .config(
            "spark.jars.packages",
            "org.mongodb.spark:mongo-spark-connector_2.13:11.1.0"
        ) \
        .config(
            "spark.mongodb.write.connection.uri",
            "mongodb://mongodb:27017/"
        ) \
        .config("spark.sql.warehouse.dir", WAREHOUSE_PATH) \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.maxResultSize", "4g") \
        .getOrCreate()
    
    return sparkSession

def createTables(sparkSession: SparkSession, filePath: str, tableName: str) -> None:
    tables: list[str] = [t.name for t in sparkSession.catalog.listTables()]
    if tableName not in tables:
        df: DataFrame = sparkSession.read.json(filePath, multiLine = True)
        df.write.mode("overwrite").format("parquet").saveAsTable(tableName)

def main():
    warnings.filterwarnings("ignore")

    sparkSession = createSparkSession()
    
    tables = ["events", "mentions", "gkg"]
    for table in tables:
        try:
            filePath: str = os.path.join(JSONS_DIR_PATH, table + JSON_EXTENSION)
            createTables(sparkSession, filePath, table)

        except Exception as e:
            print(f"Error consultando {table}: {e}")

if __name__ == "__main__":
    freeze_support()
    main()