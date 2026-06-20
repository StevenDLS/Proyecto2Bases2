import os
import shutil
import pandas
import warnings
import queries
from multiprocessing import freeze_support
from pyspark.sql import SparkSession, DataFrame

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
JSONS_DIR_PATH: str = os.path.join(BASE_PATH, "raw_jsons")
PARQUET_DIR_PATH: str = os.path.join(BASE_PATH, "parquets")
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
        .config("spark.sql.warehouse.dir", PARQUET_DIR_PATH) \
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
            shutil.rmtree(os.path.join(PARQUET_DIR_PATH, table))
            filePath: str = os.path.join(JSONS_DIR_PATH, table + JSON_EXTENSION)
            createTables(sparkSession, filePath, table)

        except FileNotFoundError:
            print("Este directorio no existe.")

        except Exception as e:
            print(f"Error consultando {table}: {e}")

    queries.cargar_parquets(sparkSession)
    
    # Tests
    print(pandas.DataFrame(queries.mapa_calor_intensidad_conflictos(sparkSession)))
    print(pandas.DataFrame(queries.top_10_paises_eventos_por_dia(sparkSession)))
    print(pandas.DataFrame(queries.correlacion_avg_tone_fuentes(sparkSession)))
    print(pandas.DataFrame(queries.distribucion_cameo_por_region(sparkSession)))
    print(pandas.DataFrame(queries.matriz_interaccion_actores(sparkSession)))
    print(pandas.DataFrame(queries.paises_mayor_cobertura_mediatica(sparkSession)))
    print(pandas.DataFrame(queries.tendencia_sentimiento_pais(sparkSession)))
    print(pandas.DataFrame(queries.conflictos_pares_paises(sparkSession)))
    print(pandas.DataFrame(queries.escalada_eventos_menciones_24h(sparkSession))) # Retorna lista vacía por los momentos
    print(pandas.DataFrame(queries.conflictos_religion_region(sparkSession))) # Retorna una sola fila
    print(pandas.DataFrame(queries.temas_gkg_continente_anio(sparkSession)))
    print(pandas.DataFrame(queries.organizaciones_mas_mencionadas_por_dia(sparkSession)))
    print(pandas.DataFrame(queries.analisis_rezago_tono_conflicto(sparkSession)))
    print(pandas.DataFrame(queries.grafo_diplomacia_vs_conflicto(sparkSession)))
    print(pandas.DataFrame(queries.indice_diversidad_fuentes_pais(sparkSession)))
    print(pandas.DataFrame(queries.frecuencia_conflictos_por_etnia(sparkSession))) # Retorna una sola fila
    print(pandas.DataFrame(queries.noticias_ultima_hora(sparkSession))) # Retorna lista vacía

if __name__ == "__main__":
    freeze_support()
    main()