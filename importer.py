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

    # Combine the originally separate date and hour columns into a single DateTime column
    return combine_date_and_hour_columns(original_df)


def roundUTCHourUp(dateString):
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
    original_df["Date"] = original_df["DateUTC"].apply(roundUTCHourUp)

    return original_df

def merge_data_frames_by_column(df1, df2, column_name):
    return pd.merge(df1, df2, on=column_name)


# Testing
df1 = read_load_data_from_csv("./system_load_by_region/cdr.00013101.0000000000000000.20140102.055001.ACTUALSYSLOADWZNP6345.csv")
df2 = read_weather_data_from_csv("./weather_data/KDAL_20140101.csv")


print df1.head()
print "-"*50
print df2.head()

print "\nMERGING\n"
print merge_data_frames_by_column(df1, df2, "Date")




