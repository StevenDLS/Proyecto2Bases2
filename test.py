from multiprocessing import freeze_support 
import gdelt

def main():
    # Version 2 queries
    gd2 = gdelt.gdelt(version=2)

    # Single 15 minute interval pull, output to json format with mentions table
    results = gd2.Search('2024 Nov 1',table='gkg',output='json')
    print(results)

    # Full day pull, output to pandas dataframe, events table
    results = gd2.Search(['2024 11 01'],table='gkg',coverage=True)
    print(results)

if __name__ == "__main__":
    freeze_support()
    main()