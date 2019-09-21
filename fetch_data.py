"""
Script for fetching and saving wmata data from api.
"""

import os
from csv import DictWriter
from pprint import pprint
from datetime import datetime

# project modules
from config import Config
from wmata_api import WmataApi

BUS_POS_FIELD_NAMES = ['BlockNumber', 'DateTime', 'Deviation', 'DirectionNum',
                       'DirectionText', 'Lat', 'Lon', 'RouteID', 'TripEndTime',
                       'TripHeadsign', 'TripID', 'TripStartTime', 'VehicleID']
BUS_API_KEY = Config.wmata_api_key(bus=True)
time_stamp = datetime.now()
data = WmataApi.get_bus_position(api_key=BUS_API_KEY)

# save data as csv
path = 'data/bus_position_{}.csv'.format(time_stamp)
with open(path, mode='w', newline='') as csv:
    pos_data = data['BusPositions']
    writer = DictWriter(csv, fieldnames=BUS_POS_FIELD_NAMES)
    writer.writeheader()
    writer.writerows(pos_data)
    print('[{}]: saved!'.format(path))
