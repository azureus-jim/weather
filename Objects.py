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

    ## Build DataFrame (by repeatedly calling _get_reading function above if _connect_to_api function passes)
    def build_df(self, limit=None):
        # Connect to API and get first reading to start off the DataFrame construction
        self._connect_to_api()
        df = self._get_reading()
        print(df)

        # If no build limit is set, the Collector will keep pinging the API at the preset ping_interval. 
        # If the reading_time of the response is different from the latest entry in the DataFrame, the new_df_entry is appended to the existing DataFrame.
        if limit == None:
            while True:
                if df.shape[0] >= 12:
                    break
                try:
                    self._connect_to_api()
                    # Add entry to dataframe only if reading_time is not the same as the latest row in the growing dataframe
                    if self.reading_time != df.index[-1]:
                        new_df_entry = self._get_reading()
                        df = df.append(new_df_entry)
                        print(df)
                    print(f"Latest ping returned the same reading as latest entry in built DataFrame. Waiting for next ping in {self.ping_interval} seconds...")
                    time.sleep(self.ping_interval)
                except KeyboardInterrupt:
                    return df
            return df

        # Else if a build limit (int type) is set, the Collector will ping the API at the preset ping_interval.
        # It will check if the reading_time of the response is different from the latest entry in the DataFrame, in which the new_df_entry is appended to the existing DataFrame. This repeats until the build limit is reached.
        else:
            entry_cnt = 1
            while entry_cnt <= limit:
                self._connect_to_api()
                if self.reading_time != df.index[-1]:
                    new_df_entry = self._get_reading()
                    df = df.append(new_df_entry)
                    entry_cnt += 1
                    print(df)
                time.sleep(self.ping_interval)
            return df
