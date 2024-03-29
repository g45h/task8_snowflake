import csv
import os

import numpy as np
from airflow.hooks.filesystem import FSHook
import pandas as pd
# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


def print_hi(name):
    filepath = '/home/user/PycharmProjects/task8_snowflake/data/763K_plus_IOS_Apps_Info.csv'
    filepath_parsed = '/home/user/PycharmProjects/task8_snowflake/data/333.csv'

    df = pd.read_csv(filepath, sep=',', dtype='unicode')
    df = df.drop('_id', axis=1)
    # delete the index column

    cols_to_str = ['IOS_App_Id', 'Title', 'Developer_Name', 'Developer_IOS_Id', 'IOS_Store_Url',
                   'Seller_Official_Website', 'Age_Rating', 'Total_Average_Rating', 'Total_Number_of_Ratings',
                   'Average_Rating_For_Version', 'Number_of_Ratings_For_Version', 'Original_Release_Date',
                   'Current_Version_Release_Date', 'Price_USD', 'Primary_Genre', 'All_Genres', 'Languages',
                   'Description']
    df[cols_to_str] = df[cols_to_str].astype(str)
    # EVERYTHING to prevent misreading the delimiter char (comma)

    cols_to_modify = ['Original_Release_Date', 'Current_Version_Release_Date']
    for col in cols_to_modify:
        df[col] = pd.to_datetime(df[col], errors='coerce')
        df[col] = df[col].apply(
            lambda x: x.strftime('%Y-%m-%dT%H:%M:%S') if not pd.isnull(x) else '1970-01-01T00:00:00')
    df[cols_to_modify] = df[cols_to_modify].replace("0000-00-00T00:00:00Z", "")
    # datetime modification from 0000-00-00T00:00:00Z to 0000-00-00T00:00:00 & setting null value as an empty string

    df.replace({'"': r'\"'}, regex=True, inplace=True)
    # still trying to prevent comma misreading

    max_description_size = 16_777_210
    df['Description_part1'] = df['Description'].str.slice(0, max_description_size)
    df['Description_part2'] = df['Description'].str.slice(max_description_size, 2 * max_description_size)
    df['Description_part3'] = df['Description'].str.slice(2 * max_description_size, 3 * max_description_size)
    df = df.drop(columns=['Description'])
    # we do have big a$$ string 3x times larger than the max Snowflake STRING capacity. we do address it

    my_columns2 = ['IOS_App_Id', 'Title', 'Developer_Name', 'Developer_IOS_Id', 'IOS_Store_Url',
                   'Seller_Official_Website', 'Age_Rating', 'Total_Average_Rating', 'Total_Number_of_Ratings',
                   'Average_Rating_For_Version', 'Number_of_Ratings_For_Version', 'Original_Release_Date',
                   'Current_Version_Release_Date', 'Price_USD', 'Primary_Genre', 'All_Genres', 'Languages',
                   'Description_part1', 'Description_part2', 'Description_part3']
    df = df[my_columns2]
    # since Snowflake likes to assume we have 21 columns and not 18 I need to explicitly address it

    if os.path.exists(filepath_parsed):
        os.remove(filepath_parsed)
    # reusability moment

    df.to_csv(filepath_parsed, sep=',', index=False, quoting=csv.QUOTE_ALL)

    df = pd.read_csv(filepath_parsed, sep=',', dtype='unicode', encoding='utf-8')
    columns = df.columns.tolist()
    select_query = ""
    copy_query = ""
    awoo = 1
    #  $1 AS IOS_App_Id,
    for i in columns:
        copy_query = copy_query + ", " + str(i)
        select_query = select_query + f'${awoo} AS {i}, '
        awoo = awoo + 1
    select_query = select_query[:-2]
    copy_query = copy_query[2:]
    staged_to_raw = f"COPY INTO RAW_TABLE ({copy_query}) FROM (SELECT {select_query} FROM @~/staged) " \
                    f"FILE_FORMAT = (FORMAT_NAME = 'my_csv_format') " \
                    f"ON_ERROR = 'CONTINUE';"
    print(staged_to_raw)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
