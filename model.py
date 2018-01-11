"""
Example
python model.py test_project.db KHOU EAST
"""

import pandas as pd
from sklearn import ensemble
import argparse
import sqlite3
from datetime import datetime
import numpy as np
import time

# import matplotlib.pyplot as plt


def read_results_table_from_db_path(path):
    """
    Return a DataFrame corresponding to the table 'results' in the database of the given path
    :param path: a path to a SQLite db
    :return: a DataFrame corresponding to the table 'results' in the database of the given path
    """
    conn = sqlite3.connect(path)
    # Assumes table in database is called 'results'
    return pd.read_sql("SELECT * FROM results", conn)


def convert_unicode_string_to_datetime(string):
    return datetime.strptime(string, "%Y-%m-%d %H:%M:%S")


def collect_axis_data_from_database(database_path, year, station, region):

    # Load db into df
    df = read_results_table_from_db_path(database_path)

    # Filter df by year
    year_start = datetime(day=1, month=1, year=year)
    year_end = datetime(day=31, month=12, year=year)

    # Convert current dates in string form to comparable datetime objects
    df["Date"] = df["Date"].apply(convert_unicode_string_to_datetime)

    # Bound dates by beginning and end of year
    df = df[df["Date"] > year_start]
    df = df[df["Date"] < year_end]


    # Select the appropriate temperatures and loads from the dataframe and round each observation to two decimal places
    temperatures = np.array(df[station + "_TemperatureF"].apply(lambda number: round(number, 2)).tolist())
    loads = np.array(df[region].apply(lambda number: round(number, 2)).tolist())

    # Combine the discrete lists into tuples, each representing a data point
    points = zip(temperatures, loads)

    # Remove any points for which the temperature is nan - which happens when there was no recording
    # at a specific time for a given station
    points = filter(lambda point: not np.isnan(point[0]),points)

    # Invert zip
    temperatures, loads = zip(*points)

    # Return selection of station and region columns
    return temperatures, loads


def train_model(db_path="test_project.db", year=2014, station="KHOU",region="EAST"):
    """
    Output a Random Forest model correspodning to the given inputs
    :param db_path: a path to the SQLite DB
    :param year: the year on which to train the model
    :param station: the weather station on which to train the model
    :param region: the region on which to train the model
    :return: a regression object, which is ready to be used to make predictions
    """

    # temperatures, loads = collect_axis_data_from_database(db_path, year, station, region)
    temperatures, loads = collect_axis_data_from_database(db_path, year, station, region)

    # Format
    temperatures = map(lambda temp: [temp], temperatures)
    loads = np.array(map(lambda load: [load], loads))

    # Plot code
    # plt.plot(temperatures, loads, 'ro')
    # plt.ylabel("East Zonal Load")
    # plt.xlabel("Houston Temperature F")
    # plt.axis([0, 100,100,4000])
    # plt.show()

    reg = ensemble.RandomForestRegressor()
    reg.fit(temperatures, loads.ravel())

    return reg

def make_predictions(model,model_inputs):
    """
    Given a model and a list of inputs, make a prediction of the output for each input
    :param model: a sklearn model
    :param model_inputs: a list of inputs appropriate to the model (in this case, temperature in F)
    :return: a string summary specific to the temperature/load example, predicting zonal loads given temperatures
    """
    output = ""
    for model_input in model_inputs:
        output += "Predicted load at temperature = " + str(model_input) + " : " + str(round(model.predict(model_input)[0], 2)) + "\n"

    return output


def main():
    # Create parser object to parse the command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("db_path", help="the path of the database containing load/weather data from importer.py")
    parser.add_argument("station", help="the weather station name (i.e. KHOU, KDAL, SAT)")
    parser.add_argument("region",
                        help="the zonal region which to model (i.e. COAST, EAST, FAR_WEST, NORTH, ... WEST, TOTAL")

    args = parser.parse_args()

    # Train model based on command line arguments with 2014 data
    model = train_model(db_path=args.db_path, year=2014, station=args.station, region=args.region)

    # Create a list of sample temperatures from 20-100, skipping 5
    sample_temperatures = np.arange(20, 100, 5)

    # Output a set of load predictions for each input temperature
    print make_predictions(model, sample_temperatures)


start_time = time.time()
main()
print("Execution Time: %s seconds" % (time.time() - start_time))










