import os
import time
import warnings
import json
from multiprocessing import freeze_support
from gdelt import gdelt
from datetime import datetime, timedelta, timezone

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
JSONS_DIR_PATH: str = os.path.join(BASE_PATH, "shared")
JSON_EXTENSION: str = ".json"

def getDatetimeIntervalFromNow() -> datetime:
    now: datetime = datetime.now(timezone.utc) - timedelta(hours = 6)
    rounded_minute: int = (now.minute // 15) * 15

    interval: datetime = now.replace(
        minute = rounded_minute,
        second = 0,
        microsecond = 0
    )

    return interval


def getGdeltData(gd2, dateString: str):
    data: list[dict] = []

    tables: list[str] = ["events", "mentions", "gkg"]

    for table in tables:
        try:
            results = gd2.Search(
                date = dateString,
                table = table,
                output = "json"
            )

            with open(os.path.join(JSONS_DIR_PATH, table + JSON_EXTENSION), 'w') as file:
                parsedData = json.loads(results)
                formattedJSON = json.dumps(parsedData, indent = 4)
                file.write(formattedJSON)

        except Exception as e:
            print(f"Error consultando {table}: {e}")

    return data

def main():
    warnings.filterwarnings("ignore")

    gd2 = gdelt(version=2)

    while True:
        interval = getDatetimeIntervalFromNow()
        dateString = interval.strftime("%Y %b %d %H:%M")

        print(f"\nConsultando GDELT para: {dateString}")

        getGdeltData(gd2, dateString)

        print("Esperando 15 minutos...")
        time.sleep(15 * 60)

if __name__ == "__main__":
    freeze_support()
    main()