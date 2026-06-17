from multiprocessing import freeze_support
from gdelt import gdelt
from datetime import datetime, timedelta, timezone
import time
import warnings

def get_interval():
    now = datetime.now(timezone.utc) - timedelta(hours = 6)
    rounded_minute = (now.minute // 15) * 15

    interval = now.replace(
        minute=rounded_minute,
        second = 0,
        microsecond = 0
    )

    return interval


def get_gdelt_data(gd2, date_string):
    data = []

    tables = ["events", "mentions", "gkg"]

    for table in tables:
        try:
            results = gd2.Search(
                date = date_string,
                table = table,
                output = 'json'
            )

            #for row in results:
            #    data.append({
            #        "source_table": table,
            #        "interval_time": date_string,
            #        "collected_at": datetime.now(timezone.utc).isoformat(),
            #        "raw_data": row
            #    })

            with open('shared/' + table + '.json', 'w') as file:
                file.write(results)

        except Exception as e:
            print(f"Error consultando {table}: {e}")

    return data

def main():
    warnings.filterwarnings("ignore")

    gd2 = gdelt(version=2)

    while True:
        interval = get_interval()
        date_string = interval.strftime("%Y %b %d %H:%M")

        print(f"\nConsultando GDELT para: {date_string}")

        get_gdelt_data(gd2, date_string)

        print("Esperando 15 minutos...")
        time.sleep(15 * 60)

if __name__ == "__main__":
    freeze_support()
    main()