from multiprocessing import freeze_support
import warnings
import json
import pandas

def main():
    warnings.filterwarnings("ignore")

    try:
        with open('shared/events' + '.json', 'r') as file:
            dataString = file.read()
            dataJSON = json.loads(dataString)
            dataPandas = pandas.DataFrame(dataJSON)
            print(dataPandas)

    except Exception as e:
        print(f"Error: {e}")
            

if __name__ == "__main__":
    freeze_support()
    main()