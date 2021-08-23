#!/usr/bin/python

import re
import requests
import numpy as np
import pandas as pd
import time
import sqlite3 as sl

# Function to create all tables in the database (before any Collector instance is created/function is called)
def create_all_tables(db='vault.db'):
    con = sl.connect(db)
    cur = con.cursor()
    table_names = ['temperature', 'rainfall', 'relative_humidity', 'wind_direction', 'wind_speed']
    print()
    for table in table_names:
        try:
            create_blank_table = f"""
                    CREATE TABLE {table} (
                        entry_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                        date_time TEXT ) ;
                    """
            cur.execute(create_blank_table)
            con.commit()
            print(f"Table '{table}' created with 'entry_id' and 'date_time' as headers! No station headers created!")
        except sl.OperationalError:
            print(f"Table '{table}' already exists in vault.db!")
            #sys.exit()
    con.close()
    print()


"""

Collector object is used to interface with the API, collect information/data, and transport it/interface to the database.

There are 5 types of data provided by the NEA API, as follows. Note that the key is passed into the API URL, and the value is the key to accessed the extracted data variable.

all_data_types = {
    'air-temperature': 'temperature', 
    'rainfall': 'rainfall', 
    'relative-humidity': 'relative_humidity',
    'wind-direction': 'wind_direction',
    'wind-speed': 'wind_speed'
    }

"""

class Collector:
    # Define regex patterns to match to look_for
    temp_pattern, rain_pattern, humid_pattern, wind_d_pattern, wind_s_pattern = r"temp", r"^rain", r"humid", r"direction", r"speed"

    # Initialize an instance of the data Collector
    def __init__(self, look_for, build_format, ping_interval):
        # Store the requested build_format as a Collector attribute
        self.build_format = build_format

        # Store the ping_interval as a Collector attribute
        self.ping_interval = ping_interval

        # Initialize a previous_collection attribute
        # self.previous_collection = None

        # Air temperature: Check if the look_for contains 'temp'
        match_temp = re.search(self.temp_pattern, look_for, re.IGNORECASE)

        # Rainfall: Check if the look_for starts with 'rain' (no character before rain - i.e. no 'train', 'brain' etc.)
        match_rain = re.search(self.rain_pattern, look_for, re.IGNORECASE)

        # Relative humidity: Check if the look_for contains 'humid'
        match_humid = re.search(self.humid_pattern, look_for, re.IGNORECASE)

        # Wind direction: Check if the look_for contains 'direction'
        match_wind_d = re.search(self.wind_d_pattern, look_for, re.IGNORECASE)

        # Wind speed: Check if the look_for contains 'speed'
        match_wind_s = re.search(self.wind_s_pattern, look_for, re.IGNORECASE)

        # Match data_type (the argument passed into the API URL) to data_name (the key under which the value is stored)
        if match_temp:
            self.data_type, self.data_name = 'air-temperature', 'temperature'
        elif match_rain:
            self.data_type, self.data_name = 'rainfall', 'rainfall'
        elif match_humid:
            self.data_type, self.data_name = 'relative-humidity', 'relative_humidity'
        elif match_wind_d:
            self.data_type, self.data_name = 'wind-direction', 'wind_direction'
        elif match_wind_s:
            self.data_type, self.data_name = 'wind-speed', 'wind_speed'
        else:
            raise NameError("Data type not found! Please check if keyword exists!")

    ## Connect to API, saving reading_time, reading_unit, and raw_reading as an (temporary) attribute of the Collector instance
    def _connect_to_api(self):
        # Connect to API for an instance of the Collector object to obtain raw_data of the data type of the Collector instance from the server
        response = requests.get(f"https://api.data.gov.sg/v1/environment/{self.data_type}")
        if response.status_code == 200:
            self.raw_reading = response.json()
            self.reading_time = self.raw_reading['items'][0]['timestamp']         # Type: str. Format: 'YYYY-MM-DDThh:mm:ss+hh:mm' (e.g., '2021-07-07T15:10:00+08:00')
            self.reading_unit = self.raw_reading['metadata']['reading_unit']      # Type: str. 
        else:
            error_msg = "Connection to API failed - Returned status code is not 200!"
            raise requests.exceptions.HTTPError(error_msg)

    def _get_reading(self):
        # Store reading_time, reading_unit, stations (data), and readings as attributes of a Collector instance (these attributes are updated everytime extract_data is called successfully)
        stations = self.raw_reading['metadata']['stations']                                                          # Type: list. Format: [{'id': ..., 'device_id': ..., 'name': ..., 'location': {'latitude': ..., 'longitude': ...}}, {~~~}, ...]. Note that stations is modified on the fly in the subsequent lines
        readings = self.raw_reading['items'][0]['readings']                                                          # Type: list. Format: [{'station_id': ..., 'value': ...}, {~~~}, ...]
        self.num_stations = len(stations)                                                                            # Type: int. Get number of stations
        self.station_ids = sorted(list(stations[i]['id'] for i in range(len(stations))), key=lambda x: int(x[1:]))  # Type: list. Get list of station ids (sorted by value after S prefix)
        
        # Format the raw_data into an appropriate pandas DataFrame (as requested by the user when calling extract_data function) for storage purposes
        if re.search(r"v1", self.build_format, re.IGNORECASE):
            # Modify the json data to the necessary structure
            station_i, reading_i = 0, 0
            while station_i <= self.num_stations - 1:
                if stations[station_i]['id'] == readings[reading_i]['station_id']:
                    stations[station_i][f'{self.data_name}'] = readings[reading_i]['value']       # Add f'{data_name}' reading into processed dictionary
                    station_i += 1
                    reading_i = 0
                else:
                    reading_i += 1
            processed_reading = {'reading_time': self.reading_time, 'reading_unit': self.reading_unit, 'stations': stations}

            # Prepare dataframe for requested data_type
            column_header = list()
            entry = list()
            for station in processed_reading['stations']:
                column_header.append(station['id'].strip())
                entry.append(float(station[f'{self.data_name}']))             
            df_entry = pd.DataFrame(np.array([entry]), columns=column_header)
            df_entry.rename(index={0: processed_reading['reading_time']}, inplace=True)                       # Use 'reading_time' as index (row_label)
            df_entry.sort_index(axis=1, inplace=True, key=lambda x: x.to_series().str[1:].astype(int))        # Sort by column headers (by value after S prefix)

            return df_entry

    ## Build DataFrame (by calling _get_reading function after a _connect_to_api call)
    def build_df(self, db_table_name, limit, path_to_db='vault.db', send_to_db=False):
        # Try to build the dataframe as far as possible, barring KeyboardInterrupt
        try:
            for i in range(limit): #tqdm.tqdm(range(limit), desc=f"{db_table_name}", position=0, leave=True):
                if i == 0:
                    # Get first entry
                    self._connect_to_api()
                    df = self._get_reading()
                    print(f"First entry of DataFrame for (dbTable: {db_table_name}) built!")
                else:
                    # For subsequent entries, check if API is returning an updated timestamp
                    print(f"Building {i+1}/{limit} of DataFrame for (dbTable: {db_table_name}).")
                    while True:
                        self._connect_to_api()
                        if self.reading_time != df.index[-1]:
                            new_df_entry = self._get_reading()
                            df = df.append(new_df_entry)
                            #print(df)
                            break
                        else:
                            #print(f"({db_table_name}) Latest ping returned the same reading as latest entry in built DataFrame. Waiting for next ping in {self.ping_interval} seconds...")
                            time.sleep(self.ping_interval)
                yield i+1
        except KeyboardInterrupt:
            pass                                    # df will be retained in memory up to the previous successful while True iteration.

        # If send_to_db == True:
            # 1) Make connection with database
            # 2) Prune top row of dataframe if its datetime is the same as the latest row in the database target table
            # 3) Make the dataframe and target table in the database compatible with each other (i.e. ensure the stations in the database should be equal to or larger than the number of incoming stations)
            # 4) Pass modified dataframe to database
        if send_to_db == True:
            # 1) Make connection with database
            self.con = sl.connect(path_to_db)
            self.cur = self.con.cursor()
            
            # 2) Prune top row of dataframe if its datetime is the same as the latest row in the database target table
            # Since in the dataframe building process, each entry added is unique, and time only increases in one direction, checking the top row of the incoming dataframe against the last row of the existing table would do in ensuring a chronological order of entries
            try:
                last_row_datetime_in_dbTable = self.cur.execute(f"SELECT * FROM {db_table_name}").fetchall()[-1][1]
                earliest_entry_in_df = df.index[0]
                if earliest_entry_in_df == last_row_datetime_in_dbTable:
                    df = df.drop([f'{earliest_entry_in_df}'])
                dbTable_empty = False
            except IndexError:          # An index error could be raised to show that there are no rows in the target table in the database.
                dbTable_empty = True


            # 3) Make the dataframe and target table in the database compatible with each other (i.e. ensure the stations in the database should be equal to or larger than the number of incoming stations)
            incoming_stations = list(df.keys())
            if dbTable_empty:
                for i in range(len(incoming_stations)):
                    # Add the incoming station name as a column into the existing database. The horizontal location where the incoming station name is inserted does not matter.
                    sql_add_station = """ALTER TABLE {}
                                        ADD COLUMN {} TEXT ;
                                    """.format(db_table_name, incoming_stations[i])
                    self.cur.execute(sql_add_station)
                    print(f"Station {incoming_stations[i]} added to {db_table_name} table in database as a column. Table was empty previously.")
            else:
                colsInDb_descriptions = self.cur.execute("SELECT * FROM {}".format(db_table_name)).description
                colsInDb_names = [colsInDb_descriptions[x][0] for x in range(len(colsInDb_descriptions))]
                stationsInDb = colsInDb_names[2:]           # First two columns from left are entry_id and date_time
                # Check if df has columns (stations) not present in database yet
                for i in range(len(incoming_stations)):
                    if incoming_stations[i] not in stationsInDb:
                        # Add the incoming station name as a column into the existing database. The horizontal location where the incoming station name is inserted does not matter.
                        sql_add_station = """ALTER TABLE {}
                                            ADD COLUMN {} TEXT ;
                                        """.format(db_table_name, incoming_stations[i])
                        self.cur.execute(sql_add_station)
                        print(f"Incoming station {incoming_stations[i]} added to {db_table_name} table in database as a column. Previously not present.")

            # 4) Pass modified dataframe to database
            # Now the stations in the database should be equal to or larger than the number of incoming stations
            # In other words, the incoming stations should all be represented in the database now
            # It is possible however, that the dataframe is empty at this stage, if only one entry was made and that entry pruned in step 2 above due to matching with the latest entry in the target table in the database.
            if df.shape[0] != 0:
                df.to_sql(db_table_name, self.con, if_exists='append', index_label='date_time')
                print(f"\nAdded DataFrame to (dbTable: {db_table_name}): \n")
                print(df, "\n")
            else:
                print("DataFrame not added due to it being empty from pruning!")
            self.con.close()
            
        else:
            print("DataFrame construction completed! Constructed DataFrame not passed into database.")
            return df
