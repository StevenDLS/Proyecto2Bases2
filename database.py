import time
import json
from pathlib import Path
from pymongo import MongoClient

PROCESSED_JSONS_DIR_PATH: Path = Path("processed_jsons")

def uploadData() -> None:
    client: MongoClient = MongoClient("mongodb://localhost:27017/")
    database = client["local"]
    for jsonFilePath in PROCESSED_JSONS_DIR_PATH.iterdir():
        if jsonFilePath.exists() and jsonFilePath.is_file():
            with open(str(jsonFilePath.absolute()) ,'r', encoding = "utf-8") as jsonFile:
                rawData: str = jsonFile.read()
                parsedData = json.loads(rawData)
                if parsedData != []:
                    collection = database[jsonFilePath.name[:-5]]
                    collection.insert_many(parsedData)

def clearData() -> None:
    client: MongoClient = MongoClient("mongodb://localhost:27017/")
    database = client["local"]
    for jsonFilePath in PROCESSED_JSONS_DIR_PATH.iterdir():
        if jsonFilePath.exists() and jsonFilePath.is_file():
            collection = database[jsonFilePath.name[:-5]]
            collection.delete_many({})

def main() -> None:
    while True:
        for _ in range(47):
            print("Esperando 2 minutos antes de guardar los datos a la base de datos")
            time.sleep(2 * 60)
            print("Subiendo datos")
            uploadData()
            print("Esperando 15 minutos para la siguiente consulta a GDELT")
            time.sleep(15 * 60)
        print("Limpiando base de datos")
        clearData()

if __name__ == "__main__":
    main()