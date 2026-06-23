import time
import json
from pathlib import Path
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure

PROCESSED_JSONS_DIR_PATH: Path = Path("processed_jsons")
MONGO_URI: str = "mongodb://mongodb:27017/"
DATABASE: str = "local"

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

def uploadData() -> None:
    client: MongoClient = MongoClient(MONGO_URI)
    database = client[DATABASE]
    for jsonFilePath in PROCESSED_JSONS_DIR_PATH.iterdir():
        if jsonFilePath.exists() and jsonFilePath.is_file():
            with open(str(jsonFilePath.absolute()) ,'r', encoding = "utf-8") as jsonFile:
                rawData: str = jsonFile.read()
                parsedData = json.loads(rawData)
                if parsedData != []:
                    collection = database[jsonFilePath.name[:-5]]
                    collection.insert_many(parsedData)
    client.close()

def clearData() -> None:
    client: MongoClient = MongoClient(MONGO_URI)
    database = client[DATABASE]
    for jsonFilePath in PROCESSED_JSONS_DIR_PATH.iterdir():
        if jsonFilePath.exists() and jsonFilePath.is_file():
            collection = database[jsonFilePath.name[:-5]]
            collection.delete_many({})
    client.close()

def main() -> None:
    connectionTest()
    while True:
        try:
            for _ in range(47):
                print("Esperando 1 minutos antes de guardar los datos a la base de datos")
                time.sleep(1 * 60)
                print("Subiendo datos")
                uploadData()
                print("Esperando 15 minutos para la siguiente consulta a GDELT")
                time.sleep(15 * 60)
            print("Limpiando base de datos")
            clearData()
        except Exception:
            print("Error: Ocurrió un problema con la base de datos")

if __name__ == "__main__":
    main()