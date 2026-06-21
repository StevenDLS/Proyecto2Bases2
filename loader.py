from pathlib import Path
import time
import warnings
import json
from multiprocessing import freeze_support
from gdelt import gdelt
from datetime import datetime, timedelta, timezone

JSONS_DIR_PATH: Path = Path("raw_jsons")

def getDatetimeIntervalFromNow() -> datetime:
    now: datetime = datetime.now(timezone.utc) - timedelta(hours = 6)
    rounded_minute: int = (now.minute // 15) * 15

    interval: datetime = now.replace(
        minute = rounded_minute,
        second = 0,
        microsecond = 0
    )

    return interval


def getGdeltData(gd2, dateString: str) -> None:
    tables: list[str] = ["events", "mentions", "gkg"]
    for table in tables:
        results = gd2.Search(
            date = dateString,
            table = table,
            output = "json"
        )

        now: datetime = datetime.now(timezone.utc) - timedelta(hours = 6)
        jsonFilePath: Path = JSONS_DIR_PATH.joinpath(table + ".json")

        with open(str(jsonFilePath), 'w') as jsonFile:
            parsedData = json.loads(results)
            formattedJSON = json.dumps(parsedData, indent = 4)
            jsonFile.write(formattedJSON)

def main():
    warnings.filterwarnings("ignore")

    gd2 = gdelt(version = 2)

    while True:
        interval = getDatetimeIntervalFromNow()
        dateString = interval.strftime("%Y %b %d %H:%M")

        print(f"\nConsultando GDELT para: {dateString}")

        getGdeltData(gd2, dateString)

        print("Esperando 15 minutos...")
        time.sleep(5 * 60)

if __name__ == "__main__":
    freeze_support()
    main()