import time
import warnings
import queries
from pathlib import Path
from multiprocessing import freeze_support
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

RAW_JSONS_DIR_PATH: Path = Path("raw_jsons")
PARQUETS_DIR_PATH: Path = Path("parquets")

def region_from_geo(country, lat, lon):
    """
    Región aproximada usando primero código de país y luego lat/lon como fallback.
    """
    region_by_code = {
        "US": "Norteamérica", "CA": "Norteamérica", "GL": "Norteamérica",

        "MX": "Latinoamérica y Caribe", "CR": "Latinoamérica y Caribe",
        "CS": "Latinoamérica y Caribe", "GT": "Latinoamérica y Caribe",
        "HO": "Latinoamérica y Caribe", "HN": "Latinoamérica y Caribe",
        "ES": "Latinoamérica y Caribe", "SV": "Latinoamérica y Caribe",
        "NU": "Latinoamérica y Caribe", "NI": "Latinoamérica y Caribe",
        "PM": "Latinoamérica y Caribe", "PA": "Latinoamérica y Caribe",
        "CU": "Latinoamérica y Caribe", "DR": "Latinoamérica y Caribe",
        "HA": "Latinoamérica y Caribe", "JM": "Latinoamérica y Caribe",
        "RQ": "Latinoamérica y Caribe", "PR": "Latinoamérica y Caribe",

        "BR": "Sudamérica", "AR": "Sudamérica", "CO": "Sudamérica",
        "VE": "Sudamérica", "PE": "Sudamérica", "EC": "Sudamérica",
        "CI": "Sudamérica", "CHL": "Sudamérica", "UY": "Sudamérica",
        "BL": "Sudamérica", "BO": "Sudamérica", "GY": "Sudamérica",

        "UK": "Europa", "GB": "Europa", "FR": "Europa", "GM": "Europa",
        "DE": "Europa", "SP": "Europa", "ES2": "Europa", "IT": "Europa",
        "RS": "Europa", "RU": "Europa", "UP": "Europa", "UA": "Europa",
        "PL": "Europa", "PO": "Europa", "NL": "Europa", "BE": "Europa",
        "SZ": "Europa", "SW": "Europa", "NO": "Europa", "FI": "Europa",

        "CH": "Asia", "CN": "Asia", "JA": "Asia", "JP": "Asia",
        "KS": "Asia", "KR": "Asia", "KN": "Asia", "IN": "Asia",
        "PK": "Asia", "BG": "Asia", "TH": "Asia", "VM": "Asia",
        "RP": "Asia", "PH": "Asia", "ID": "Asia", "MY": "Asia",
        "IR": "Asia", "IZ": "Asia", "IQ": "Asia", "IS": "Asia",
        "SA": "Asia", "TU": "Asia", "AF": "Asia",

        "EG": "África", "NI": "África", "NG": "África", "SF": "África",
        "ZA": "África", "KE": "África", "ET": "África", "SU": "África",
        "SD": "África", "MO": "África", "MA": "África", "AG": "África",
        "DZ": "África", "LY": "África", "TZ": "África",

        "AS": "Oceanía", "AU": "Oceanía", "NZ": "Oceanía", "PP": "Oceanía",
        "FJ": "Oceanía"
    }

    if country is not None:
        c = str(country).upper().strip()
        if c in region_by_code:
            return region_by_code[c]

    try:
        lat = float(lat)
        lon = float(lon)
    except Exception:
        return "Sin región"

    if -170 <= lon <= -50 and lat >= 32:
        return "Norteamérica"
    if -120 <= lon <= -30 and lat < 32:
        return "Latinoamérica y Caribe"
    if -90 <= lon <= -30 and lat < 15:
        return "Sudamérica"
    if -25 <= lon <= 60 and 35 <= lat <= 75:
        return "Europa"
    if -20 <= lon <= 55 and -35 <= lat <= 38:
        return "África"
    if 25 <= lon <= 180 and -10 <= lat <= 80:
        return "Asia"
    if 110 <= lon <= 180 and -50 <= lat <= 0:
        return "Oceanía"

    return "Sin región"

def normalizar_numberlong(df):
    """
    Convierte columnas tipo {"$numberLong": "..."} a long.
    Sirve si el JSON fue convertido a parquet conservando estructuras Mongo-like.
    """
    for field in df.schema.fields:
        if isinstance(field.dataType, StructType) and "$numberLong" in field.dataType.fieldNames():
            df = df.withColumn(
                field.name,
                F.col(f"`{field.name}`.`$numberLong`").cast("long")
            )
    return df

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
        .config("spark.sql.shuffle.partitions", "500") \
        .config("spark.default.parallelism", "8") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.maxResultSize", "4g") \
        .config("spark.sql.ansi.enabled", "false") \
        .getOrCreate()
    
    return sparkSession

def createTables(sparkSession: SparkSession) -> None:
    tables = ["events", "mentions", "gkg"]
    for table in tables:
        for file in PARQUETS_DIR_PATH.joinpath(table).iterdir():
            if file.is_file():
                file.unlink()
        dataFrame: DataFrame = sparkSession.read.json("raw_jsons/" + table + ".json", multiLine = True)
        dataFrame = normalizar_numberlong(dataFrame)
        dataFrame.write.mode("overwrite").format("parquet").saveAsTable(table)

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
    queries.actores_mas_asociados_eventos_negativos(sparkSession)
    queries.eventos_positivos_mas_cubiertos_por_pais(sparkSession)

def main():
    warnings.filterwarnings("ignore")
    sparkSession = createSparkSession()
    sparkSession.udf.register("region_from_geo", region_from_geo, "string")

    try:
        while True:
            print("Esperando 1 minuto antes de crear las tablas")
            time.sleep(60)
            print("Creando tablas")
            createTables(sparkSession)
            print("Obteniendo consultas")
            createQueries(sparkSession)
            print("Esperando 15 minutos para la siguiente consulta a GDELT")
            time.sleep(15 * 60)
    except Exception:
        print("Error: Ocurrió un problema con el analyzer")

if __name__ == "__main__":
    freeze_support()
    main()