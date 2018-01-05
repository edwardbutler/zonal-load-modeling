"""
The goal of the importer is to structure the data that has been provided and insert it into a
SQLite database so it can easily be queried. The importer should import the temperature and load data
into a normalized form in the database.
1. The importer will be named importer.py and placed at the root of the project folder.
2. importer.py will be run from the command line and take a single argument: the path of the
SQLite file to create and write into 3. The importer can assume finding the data relative to the root of the project folder.

Example
    python importer.py test_project.db
"""

import pandas as pd
from datetime import datetime
import os
import sqlite3
import argparse


def convert_date_time_tuple_into_date_time_object(date_time_tuple):
    """
    Given a tuple in the form (date_string, time_string), return a DateTime object
    :param date_time_tuple: tuple of form (date_string, time_string)
    :return: DateTime object (see datetime module)
    """
    # Extract the two strings from the tuple
    date_string = date_time_tuple[0]
    time_string = date_time_tuple[1]

    # Convert the date string into a intermediate date object
    date_object = datetime.strptime(date_string, "%m/%d/%Y").date()

    # Handle edge case
    if time_string == "24:00":
        time_string = "23:00"
        time_object = datetime.strptime(time_string, "%H:%M").time()

    else:
        time_object = datetime.strptime(time_string, "%H:%M").time()
        time_object = time_object.replace(hour=time_object.hour - 1)

    return str(datetime.combine(date_object, time_object))


def combine_date_and_hour_columns(data_frame):
    """
    Given a DF with columns 'Date' and 'HourEnding', return an updated DF with a single DateTime column,
     in which every element is a DateTime object (see datetime module)
    :param data_frame: a Pandas DF to convert
    :return: an updated DF with single DateTime column
    """

    dates = data_frame["Date"].tolist()
    hours = data_frame["HourEnding"].tolist()
    date_hour_pairs = zip(dates, hours)

    date_times = map(convert_date_time_tuple_into_date_time_object, date_hour_pairs)
    data_frame["Date"] = date_times
    return data_frame.drop("HourEnding", axis=1)


def read_load_data_from_csv(csv_path):
    """
    Given the path to a csv file containing load data, return a structured Pandas DataFrame
    :param csv_path: a string representing the path of the DataFrame
    :return: a pandas DataFrame object, normalized and ready to merge
    """
    # Load the original DataFrame, use easier-to-read column names, and drop unnecessary column
    original_df = pd.read_csv(csv_path).rename(columns={"OperDay" : "Date"}).drop(["TOTAL", "DSTFlag"],axis=1)

    original_df.name = csv_path.split("_")[1]

    # Combine the originally separate date and hour columns into a single DateTime column
    return combine_date_and_hour_columns(original_df)


def round_utc_hour_up(dateString):
    dateObject = datetime.strptime(dateString, "%Y-%m-%d %H:%M:%S")
    newHour = (dateObject.hour + 1) % 24
    dateObject = dateObject.replace(hour=newHour)
    return dateObject.strftime("%Y-%m-%d %H:00:00")


def read_weather_data_from_csv(csv_path):
    """
    Given the path to a csv file containing load data, return a structured Pandas DataFrame
    :param csv_path: a string representing the path of the DataFrame
    :return: a pandas DataFrame object, normalized and ready to merge
    """

    # Read the original DataFrame and select the relevant columns
    original_df = pd.read_csv(csv_path)[["DateUTC","TemperatureF"]]

    # Round up the hour of each Date to the nearest whole hour
    original_df["Date"] = original_df["DateUTC"].apply(round_utc_hour_up)

    # Rename Temperature field to include city name
    city = csv_path.split("_")[1].split("/")[1]
    original_df[city + "_TemperatureF"] = original_df["TemperatureF"]
    original_df = original_df.drop(["TemperatureF", "DateUTC"], axis=1)

    return original_df


def merge_data_frames_by_column(df1, df2, column_name):
    return pd.merge(df1, df2, on=column_name)


def compute_aggregate_load_data():

    # get a list of all the csv file names in the 'system_load_by_region' directory
    files = filter(lambda x: x[-4:] == ".csv", os.listdir('system_load_by_region'))

    # convert the list of csv file names to a list of corresponding DataFrames
    dfs = map(lambda file_name: read_load_data_from_csv("./system_load_by_region/" + file_name), files)

    # fold the list of data frames into a single data frame
    aggregate_df = pd.concat(dfs).reset_index().drop(["index"],axis=1)
    # aggregate_df = reduce(lambda df1, df2: pd.concat([df1, df2]), dfs)

    return aggregate_df


def compute_aggregate_weather_data():

    # get a list of all the csv files names in the 'weather_data' directory
    files = filter(lambda x: x[-4:] == ".csv", os.listdir('weather_data'))
    print len(files), " weather csv files"

    # convert the list of csv file names to a list of corresponding DataFrames
    # Make adjustments for the *specific* city - for example, label column "Dallas Tempeature", for Dallas
    dallas_files = filter(lambda file_name : "KDAL" in file_name, files)
    houston_files = filter(lambda file_name : "KHOU" in file_name, files)
    san_antonio_files = filter(lambda file_name : "KSAT" in file_name, files)

    print "\t# of Dallas FILES: ", len(dallas_files)
    print "\t# of Houston FILES: ", len(houston_files)
    print "\t# of SAT FILES: ", len(san_antonio_files)

    dallas_dfs = map(lambda file_name: read_weather_data_from_csv("./weather_data/" + file_name), dallas_files)
    houston_dfs = map(lambda file_name: read_weather_data_from_csv("./weather_data/" + file_name), houston_files)
    san_antonio_dfs = map(lambda file_name: read_weather_data_from_csv("./weather_data/" + file_name), san_antonio_files)

    dallas_df = pd.concat(dallas_dfs)
    houston_df = pd.concat(houston_dfs)
    san_antonio_df = pd.concat(san_antonio_dfs)

    # fold the list of data frames into a single data frame
    aggregate_df = reduce(lambda df1, df2: pd.merge(df1, df2, on="Date", how="outer"), [dallas_df, houston_df, san_antonio_df]).sort_values("Date")

    return aggregate_df


def get_total_data():
    return pd.merge(compute_aggregate_load_data(), compute_aggregate_weather_data(),on="Date")


def write_df_to_db(df, db_name):
    """
    Given a db
    :param db_name:
    :return:
    """
    conn = sqlite3.connect(db_name)
    df.to_sql("results", con=conn)


# Create parser object to parse the command line arguments
parser = argparse.ArgumentParser()
parser.add_argument("db_name", help="the name of the database to which the load/weather data should be inputted")
args = parser.parse_args()

# Compute the grand dataframe
total = get_total_data()

# Write the grand DF to a SQLite DB
write_df_to_db(total, args.db_name)







