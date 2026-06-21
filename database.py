import json
from pathlib import Path
from pymongo import MongoClient

PROCESSED_JSONS_DIR_PATH: Path = Path("processed_jsons")
CLIENT: MongoClient = MongoClient("mongodb://localhost:27017/")
DATABASE = CLIENT["local"]

def uploadData() -> None:
    for jsonFile in PROCESSED_JSONS_DIR_PATH.iterdir():
        if jsonFile.exists() and jsonFile.is_file():
            with open(str(jsonFile.absolute()) ,'r', encoding = "utf-8") as jsonFile:
                rawData: str = jsonFile.read()
                parsedData = json.loads(rawData)
                if parsedData != []:
                    collection = DATABASE[jsonFile.name[:-5]]
                    collection.insert_many(parsedData)