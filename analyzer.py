import time
import warnings
import queries
import database
from pathlib import Path
from multiprocessing import freeze_support
from pyspark.sql import SparkSession, DataFrame

RAW_JSONS_DIR_PATH: Path = Path("raw_jsons")
PARQUETS_DIR_PATH: Path = Path("parquets")

def cleanData(data: list[dict]) -> list[dict]:
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
        .config("spark.sql.warehouse.dir", PARQUETS_DIR_PATH.name) \
        .config("spark.sql.debug.maxToStringFields", "500") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.maxResultSize", "4g") \
        .getOrCreate()
    
    return sparkSession

def createTable(sparkSession: SparkSession, tableName: str) -> None:
    tables: list[str] = [t.name for t in sparkSession.catalog.listTables()]
    if tableName not in tables:
        mainDataFrame: DataFrame = sparkSession.read.json("raw_jsons/" + tableName + ".json", multiLine = True)
        mainDataFrame.write.mode("overwrite").format("parquet").saveAsTable(tableName)

def createQueries(sparkSession: SparkSession) -> None:
    # Tests
    queries.mapa_calor_intensidad_conflictos(sparkSession)
    queries.top_10_paises_eventos_por_dia(sparkSession)
    queries.correlacion_avg_tone_fuentes(sparkSession)
    queries.distribucion_cameo_por_region(sparkSession) # Está teniendo errores hacer la consulta
    queries.matriz_interaccion_actores(sparkSession)
    queries.paises_mayor_cobertura_mediatica(sparkSession)
    queries.tendencia_sentimiento_pais(sparkSession)
    queries.conflictos_pares_paises(sparkSession)
    queries.escalada_eventos_menciones_24h(sparkSession) # Retorna lista vacía por los momentos
    queries.conflictos_religion_region(sparkSession) # Retorna una sola fila
    queries.temas_gkg_continente_anio(sparkSession)
    queries.organizaciones_mas_mencionadas_por_dia(sparkSession)
    queries.analisis_rezago_tono_conflicto(sparkSession)
    queries.grafo_diplomacia_vs_conflicto(sparkSession)
    queries.indice_diversidad_fuentes_pais(sparkSession)
    queries.frecuencia_conflictos_por_etnia(sparkSession) # Retorna una sola fila
    queries.noticias_ultima_hora(sparkSession) # Retorna lista vacía

def main():
    warnings.filterwarnings("ignore")
    sparkSession = createSparkSession()
    tables = ["events", "mentions", "gkg"]

    for table in tables:
        for file in PARQUETS_DIR_PATH.joinpath(table).iterdir():
            if file.is_file():
                file.unlink()
        createTable(sparkSession, table)
    queries.cargar_parquets(sparkSession)
    createQueries(sparkSession)
    #time.sleep(4 * 60)
    database.uploadData()

if __name__ == "__main__":
    freeze_support()
    main()