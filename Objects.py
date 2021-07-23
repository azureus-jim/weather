#!/usr/bin/python

import re
import requests
import numpy as np
import pandas as pd
import time

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
    # Define regex patterns to match to data_query
    temp_pattern, rain_pattern, humid_pattern, wind_d_pattern, wind_s_pattern = r"temp", r"^rain", r"humid", r"direction", r"speed"

    # Initialize an instance of the data Collector
    def __init__(self, data_query, entry_format, ping_interval):
        # Store the requested entry_format as a Collector attribute
        self.entry_format = entry_format

        # Store the ping_interval as a Collector attribute
        self.ping_interval = ping_interval

        # Initialize a previous_collection attribute
        # self.previous_collection = None

        # Air temperature: Check if the data_query contains 'temp'
        self.match_temp = re.search(self.temp_pattern, data_query, re.IGNORECASE)

        # Rainfall: Check if the data_query starts with 'rain' (no character before rain - i.e. no 'train', 'brain' etc.)
        self.match_rain = re.search(self.rain_pattern, data_query, re.IGNORECASE)

        # Relative humidity: Check if the data_query contains 'humid'
        self.match_humid = re.search(self.humid_pattern, data_query, re.IGNORECASE)

        # Wind direction: Check if the data_query contains 'direction'
        self.match_wind_d = re.search(self.wind_d_pattern, data_query, re.IGNORECASE)

        # Wind speed: Check if the data_query contains 'speed'
        self.match_wind_s = re.search(self.wind_s_pattern, data_query, re.IGNORECASE)

        # Match data_type (the argument passed into the API URL) to data_name (the key under which the value is stored)
        if self.match_temp:
            self.data_type, self.data_name = 'air-temperature', 'temperature'
        elif self.match_rain:
            self.data_type, self.data_name = 'rainfall', 'rainfall'
        elif self.match_humid:
            self.data_type, self.data_name = 'relative-humidity', 'relative_humidity'
        elif self.match_wind_d:
            self.data_type, self.data_name = 'wind-direction', 'wind_direction'
        elif self.match_wind_s:
            self.data_type, self.data_name = 'wind-speed', 'wind_speed'
        else:
            raise NameError("Data type not found! Please check if keyword exists!")

    ## Connect to API, saving reading_time, reading_unit, and raw_reading as an (temporary) attribute of the Collector instance
    def connect_to_api(self):
        # Connect to API for an instance of the Collector object to obtain raw_data of the data type of the Collector instance from the server
        self.response = requests.get(f"https://api.data.gov.sg/v1/environment/{self.data_type}")
        if self.response.status_code == 200:
            self.raw_reading = self.response.json()
            self.reading_time = self.raw_reading['items'][0]['timestamp']         # Type: str. Format: 'YYYY-MM-DDThh:mm:ss+hh:mm' (e.g., '2021-07-07T15:10:00+08:00')
            self.reading_unit = self.raw_reading['metadata']['reading_unit']      # Type: str. 
        else:
            error_msg = "Connection to API failed - Returned status code is not 200!"
            raise requests.exceptions.HTTPError(error_msg)

    def get_reading(self):
        # Store reading_time, reading_unit, stations (data), and readings as attributes of a Collector instance (these attributes are updated everytime extract_data is called successfully)
        _stations = self.raw_reading['metadata']['stations']                   # Type: list. Format: [{'id': ..., 'device_id': ..., 'name': ..., 'location': {'latitude': ..., 'longitude': ...}}, {~~~}, ...]. Note that stations is modified on the fly in the subsequent lines
        _data = self.raw_reading['items'][0]['readings']                 # Type: list. Format: [{'station_id': ..., 'value': ...}, {~~~}, ...]
        
        # Format the raw_data into an appropriate pandas DataFrame (as requested by the user when calling extract_data function) for storage purposes
        if re.search(r"v1", self.entry_format, re.IGNORECASE):
            # Modify the json data to the necessary structure
            _station_i, _data_i = 0, 0
            self.num_stations = len(_stations)
            while _station_i <= self.num_stations - 1:
                if _stations[_station_i]['id'] == _data[_data_i]['station_id']:
                    _stations[_station_i][f'{self.data_name}'] = _data[_data_i]['value']       # Add f'{data_name}' reading into processed dictionary
                    _station_i += 1
                    _data_i = 0
                else:
                    _data_i += 1
            processed_reading = {'reading_time': self.reading_time, 'reading_unit': self.reading_unit, 'stations': _stations}

            # Prepare dataframe for requested data_type
            column_header = list()
            self.entry = list()
            for station in processed_reading['stations']:
                column_header.append(station['id'].strip())
                self.entry.append(float(station[f'{self.data_name}']))             
            self.df_entry = pd.DataFrame(np.array([self.entry]), columns=column_header)
            self.df_entry.rename(index={0: processed_reading['reading_time']}, inplace=True)                       # Use 'reading_time' as index (row_label)
            self.df_entry.sort_index(axis=1, inplace=True, key=lambda x: x.to_series().str[1:].astype(int))        # Sort by column headers

            return self.df_entry

    ## Build DataFrame (by repeatedly calling get_reading function above if connect_to_api function passes)
    def build_df(self, limit=None):
        # Get first reading to start off the DataFrame construction
        self.connect_to_api()
        df = self.get_reading()
        print(df)

        # If no build limit is set, the Collector will keep pinging the API at the preset ping_interval. 
        # If the reading_time of the response is different from the latest entry in the DataFrame, the new_df_entry is appended to the existing DataFrame.
        if limit == None:
            while True:
                try:
                    self.connect_to_api()
                    if self.reading_time != df.index[-1]:
                        new_df_entry = self.get_reading()
                        df = df.append(new_df_entry)
                        print(df)
                    print(f"Latest ping returned the same reading as latest entry in built DataFrame. Waiting for next ping in {self.ping_interval} seconds...")
                    time.sleep(self.ping_interval)
                except KeyboardInterrupt:
                    return df

        # Else if a build limit (int type) is set, the Collector will ping the API at the preset ping_interval.
        # It will check if the reading_time of the response is different from the latest entry in the DataFrame, in which the new_df_entry is appended to the existing DataFrame. This repeats until the build limit is reached.
        else:
            entry_cnt = 1
            while entry_cnt <= limit:
                self.connect_to_api()
                if self.reading_time != df.index[-1]:
                    new_df_entry = self.get_reading()
                    df = df.append(new_df_entry)
                    entry_cnt += 1
                    print(df)
                time.sleep(self.ping_interval)
            return df
