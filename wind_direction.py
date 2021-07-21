#!/usr/bin/python

import requests

# Wind direction data
"""
Data source: "https://api.data.gov.sg/v1/environment/wind-direction"
Refresh rate: 60/hr

wind_direction_data = 
{
    'reading_time': reading_time, 
    'reading_unit': 'degrees', 
    'stations': [
                {'id': '...', 'name': '...', 'location': {'latitude': ..., 'longitude': ...}, 'wind_direction': ...},
                {~~~},
                ...
            ]
}
"""
def extract_wind_direction_data():
    response = requests.get("https://api.data.gov.sg/v1/environment/wind-direction")
    _raw_data = response.json()
    reading_time = _raw_data['items'][0]['timestamp']         # Type: str. Format: 'YYYY-MM-DDThh:mm:ss+hh:mm' (e.g., '2021-07-07T15:10:00+08:00')
    reading_unit = _raw_data['metadata']['reading_unit']      # Type: str. 
    stations = _raw_data['metadata']['stations']              # Type: list. Format: [{'id': ..., 'device_id': ..., 'name': ..., 'location': {'latitude': ..., 'longitude': ...}}, {~~~}, ...]
    _data = _raw_data['items'][0]['readings']                 # Type: list. Format: [{'station_id': ..., 'value': ...}, {~~~}, ...]

    _station_i, _data_i = 0, 0
    num_stations = len(stations)
    while _station_i <= num_stations - 1:
        if stations[_station_i]['id'] == _data[_data_i]['station_id']:
            stations[_station_i]['wind_direction'] = _data[_data_i]['value']       # Add wind direction (in degrees) reading into processed dictionary
            _station_i += 1
            _data_i = 0
        else:
            _data_i += 1

    wind_direction_data = {'reading_time': reading_time, 'reading_unit': reading_unit, 'stations': stations}
    return wind_direction_data, num_stations

# Output wind direction at each location
if __name__ == "__main__":
    wind_direction_data, num_stations = extract_wind_direction_data()
    print(wind_direction_data)