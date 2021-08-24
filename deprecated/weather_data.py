#!/usr/bin/python

import requests
import re
import numpy as np
import pandas as pd

def extract_data(user_input):
    # Extracted data structure
    """
    Data source: f"https://api.data.gov.sg/v1/environment/{data_type}"
    Refresh rate: x/hr (x = 60 for relative humidity, temperature, wind direction, wind speed; x = 12 for rainfall)
        Note that despite the claim of 1 min refresh, in reality reading_time is reported in 5 min intervals (for all data types), in line with the refresh rate for rainfall data type

    data = 
    {
        'reading_time': reading_time, 
        'reading_unit': str, 
        'stations': [
                    {'id': '...', 'name': '...', 'location': {'latitude': ..., 'longitude': ...}, f'{data_name}': ...},
                    {~~~},
                    ...
                ]
    }

    """

    # Air temperature: Check if the user_input contains 'temp'
    temp_pattern = r"temp"
    match_temp = re.search(temp_pattern, user_input, re.IGNORECASE)

    # Rainfall: Check if the user_input starts with 'rain' (no character before rain - i.e. no 'train', 'brain' etc.)
    rain_pattern = r"^rain"
    match_rain = re.search(rain_pattern, user_input, re.IGNORECASE)

    # Relative humidity: Check if the user_input contains 'humid'
    humid_pattern = r"humid"
    match_humid = re.search(humid_pattern, user_input, re.IGNORECASE)

    # Wind direction: Check if the user_input contains 'direction'
    wind_d_pattern = r"direction"
    match_wind_d = re.search(wind_d_pattern, user_input, re.IGNORECASE)

    # Wind speed: Check if the user_input contains 'speed'
    wind_s_pattern = r"speed"
    match_wind_s = re.search(wind_s_pattern, user_input, re.IGNORECASE)

    # Match data_type (the argument passed into the API URL) to data_name (the key under which the value is stored)
    if match_temp:
        data_type, data_name = 'air-temperature', 'temperature'
    elif match_rain:
        data_type, data_name = 'rainfall', 'rainfall'
    elif match_humid:
        data_type, data_name = 'relative-humidity', 'relative_humidity'
    elif match_wind_d:
        data_type, data_name = 'wind-direction', 'wind_direction'
    elif match_wind_s:
        data_type, data_name = 'wind-speed', 'wind_speed'
    else:
        raise NameError("Data type not found! Please check if keyword exists!")

    # Get relevant data from NEA API
    response = requests.get(f"https://api.data.gov.sg/v1/environment/{data_type}")
    _raw_data = response.json()
    reading_time = _raw_data['items'][0]['timestamp']         # Type: str. Format: 'YYYY-MM-DDThh:mm:ss+hh:mm' (e.g., '2021-07-07T15:10:00+08:00')
    reading_unit = _raw_data['metadata']['reading_unit']      # Type: str. 
    stations = _raw_data['metadata']['stations']              # Type: list. Format: [{'id': ..., 'device_id': ..., 'name': ..., 'location': {'latitude': ..., 'longitude': ...}}, {~~~}, ...]
    _data = _raw_data['items'][0]['readings']                 # Type: list. Format: [{'station_id': ..., 'value': ...}, {~~~}, ...]

    # Modify the json data to the necessary structure
    _station_i, _data_i = 0, 0
    num_stations = len(stations)
    while _station_i <= num_stations - 1:
        if stations[_station_i]['id'] == _data[_data_i]['station_id']:
            stations[_station_i][f'{data_name}'] = _data[_data_i]['value']       # Add f'{data_name}' reading into processed dictionary
            _station_i += 1
            _data_i = 0
        else:
            _data_i += 1
    data = {'reading_time': reading_time, 'reading_unit': reading_unit, 'stations': stations}

    # Prepare dataframe for requested data_type
    column_header = list()
    entry = list()
    for station in data['stations']:
        column_header.append(station['id'].strip())
        entry.append(float(station[f'{data_name}']))             
    df_single = pd.DataFrame(np.array([entry]), columns=column_header)
    df_single.rename(index={0: data['reading_time']}, inplace=True)                                    # Use 'reading_time' as index (row_label)
    df_single.sort_index(axis=1, inplace=True, key=lambda x: x.to_series().str[1:].astype(int))        # Sort by column headers

    return df_single

# Test output data (this will be like main program main.py)
if __name__ == "__main__":
    # Prompt user for data type to extract
    while True:
        #df_single = extract_data(user_input=input("What weather data would you like to see?:\t").strip())
        try:
            df_single = extract_data(user_input='rain')
            print(df_single)
        except KeyboardInterrupt:
            df_single = extract_data(user_input='direction')
            break
    print(df_single)
    