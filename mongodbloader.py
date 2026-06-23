import time
import json
from datetime import datetime, timezone
from pathlib import Path
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure

PROCESSED_JSONS_DIR_PATH: Path = Path("processed_jsons")
EXPIRATION_TIME: int = 10800
MONGO_URI: str = "mongodb://mongodb:27017/"
DATABASE: str = "local"
COLLECTION_NAMES = list[str] = [
    "mapa_calor_intensidad_conflictos"
    "top_10_paises_eventos_por_dia"
    "correlacion_avg_tone_fuentes"
    "distribucion_cameo_por_region"
    "matriz_interaccion_actores"
    "paises_mayor_cobertura_mediatica"
    "tendencia_sentimiento_pais"
    "conflictos_pares_paises"
    "escalada_eventos_menciones_24h"
    "conflictos_religion_region"
    "temas_gkg_continente_anio"
    "organizaciones_mas_mencionadas_por_dia"
    "analisis_rezago_tono_conflicto"
    "grafo_diplomacia_vs_conflicto"
    "indice_diversidad_fuentes_pais"
    "frecuencia_conflictos_por_etnia"
    "noticias_ultima_hora"
    "actores_mas_asociados_eventos_negativos"
    "eventos_positivos_mas_cubiertos_por_pais"
]

def connectionTest() -> None:
    print("Conectando...")
    client: MongoClient = MongoClient(MONGO_URI)
    try:
        client.admin.command('ping')
        print("Conexión establecida")
    except ConnectionFailure:
        print("Error: Fallo de conexión")
    except OperationFailure:
        print("Error: Credenciales inválidas")
    client.close()

def createTimeExpirationIndexes() -> None:
    client: MongoClient = MongoClient(MONGO_URI)
    database = client[DATABASE]
    print("Creando índices de tiempo de expiración para las colecciones")
    for collectionName in COLLECTION_NAMES:
        collection = database[collectionName]
        collection.create_index("createdAt", expireAfterSeconds = EXPIRATION_TIME) # Borrar por cada 3 horas
    client.close()

def uploadData() -> None:
    client: MongoClient = MongoClient(MONGO_URI)
    database = client[DATABASE]
    for collectionName in COLLECTION_NAMES:
        jsonFilePathString = str(PROCESSED_JSONS_DIR_PATH.joinpath(collectionName + ".json"))
        with open(jsonFilePathString ,'r', encoding = "utf-8") as jsonFile:
            unformattedData: str = jsonFile.read()
            dictionaryList = json.loads(unformattedData)
            if dictionaryList != []:
                for dictionary in dictionaryList:
                    dictionary["createdAt"] = datetime.now(timezone.utc)
                collection = database[collectionName]
                collection.insert_many(dictionary)
    client.close()

def main() -> None:
    connectionTest()
    createTimeExpirationIndexes()
    while True:
        try:
            print("Esperando 2 minutos antes de guardar los datos a la base de datos")
            time.sleep(2 * 60)
            print("Subiendo datos...")
            uploadData()
            print("Esperando 15 minutos para la siguiente consulta a GDELT")
            time.sleep(15 * 60)
        except Exception:
            print("Error: Ocurrió un problema con la base de datos")

if __name__ == "__main__":
    main()