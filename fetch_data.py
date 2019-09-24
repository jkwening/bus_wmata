"""
fetch_data.py
-------------

Script for fetching and saving WMATA data from api.
"""

import os
import argparse
import time
from csv import DictWriter, DictReader
from datetime import datetime

# project modules
from config import Config
from wmata_api import WmataApi

# module constants
DATE = datetime.today().strftime('%m-%d-%Y')
BUS_API_KEY = Config.wmata_api_key(bus=True)
DATA_CHOICES = ['position', 'route', 'schedule', 'incident']
BUS_POS_FIELD_NAMES = ['BlockNumber', 'DateTime', 'Deviation', 'DirectionNum',
                       'DirectionText', 'Lat', 'Lon', 'RouteID', 'TripEndTime',
                       'TripHeadsign', 'TripID', 'TripStartTime', 'VehicleID']
BUS_ROUTES_FIELD_NAMES = ['Name', 'RouteID', 'LineDescription']
BUS_SCHED_FIELD_NAMES = ['Name', 'DirectionNum', 'EndTime', 'RouteID',
                         'StartTime', 'StopID', 'StopName', 'StopSeq', 'Time',
                         'TripDirectionText', 'TripHeadsign', 'TripID']
BUS_INCIDENTS_FIELD_NAMES = ['DateUpdated', 'Description', 'IncidentID',
                             'IncidentType', 'RoutesAffected']
SAVE_PATH_BUS_POS = os.path.join('data', 'bus_positions', DATE)
SAVE_PATH_ROUTES = os.path.join('data', 'routes', DATE)
SAVE_PATH_SCHEDULES = os.path.join('data', 'schedules', DATE)
SAVE_PATH_INCIDENTS = os.path.join('data', 'incidents', DATE)


# helper functions
def save_csv(data, path, field_names):
    """Save fetched data as csv."""
    with open(path, mode='w', newline='') as csv:
        writer = DictWriter(csv, fieldnames=field_names)
        writer.writeheader()
        writer.writerows(data)
        print('\t[{}]: saved!'.format(path))


def bus_position(iterations=1):
    """Fetch current bus positions"""
    os.makedirs(SAVE_PATH_BUS_POS, exist_ok=True)
    for i in range(iterations):
        print('[bus_positions] Fetching iteration: {} of {}'.format(
            i+1, iterations))
        time_stamp = datetime.now()
        data = WmataApi.get_bus_position(api_key=BUS_API_KEY)
        positions = data['BusPositions']

        # save data as csv
        f_name = 'bus_position_{}.csv'.format(time_stamp)
        path = os.path.join(SAVE_PATH_BUS_POS, f_name)
        save_csv(data=positions, path=path, field_names=BUS_POS_FIELD_NAMES)
        if i + 1 < iterations:
            time.sleep(15)  # wait for until next fetch.


def bus_routes(rtn_csv_path=False):
    """Fetch bus routes"""
    os.makedirs(SAVE_PATH_ROUTES, exist_ok=True)
    print('[bus_routes] Fetching routes...')
    time_stamp = datetime.now()
    data = WmataApi.get_bus_routes(api_key=BUS_API_KEY)
    routes = data['Routes']

    # save data as csv
    f_name = 'bus_routes_{}.csv'.format(time_stamp)
    path = os.path.join(SAVE_PATH_ROUTES, f_name)
    save_csv(data=routes, path=path, field_names=BUS_ROUTES_FIELD_NAMES)

    # return routes file path if requested
    if rtn_csv_path:
        return path


def flatten_schedule_data(response):
    """
    Helper function to reformat bus schedule response data into flat format.
    """
    # Remove and store name
    name = response['Name']
    del response['Name']

    # Process direction, trip, and stop information
    data = list()
    for direction, trip_elem in response.items():  # list of dicts
        for trip in trip_elem:  # dict
            for stop in trip['StopTimes']:  # list of dicts
                row = {'Name': name}
                row.update(trip)  # add trip data for respective stop
                del row['StopTimes']  # remove StopTimes dict object
                row.update(stop)  # store specific stop_time as row data
                data.append(row)
    return data


def bus_schedules(route_id, date, variants):
    """Fetch bus schedule for given route_id and date."""
    os.makedirs(SAVE_PATH_SCHEDULES, exist_ok=True)
    print('[bus_schedule] Fetching route {} schedule...'.format(route_id))
    time_stamp = datetime.now()
    data = WmataApi.get_bus_schedule(api_key=BUS_API_KEY,
                                     route_id=route_id, date=date,
                                     variation=variants)
    schedules = flatten_schedule_data(response=data)

    # save data as csv
    route_id = route_id.replace('/', '_slash')  # handle '/' in routeIDs
    route_id = route_id.replace('\\', '_slash')  # handle '\' in routeIDs
    f_name = '{}_bus_schedule_{}.csv'.format(route_id, time_stamp)
    path = os.path.join(SAVE_PATH_SCHEDULES, f_name)
    save_csv(data=schedules, path=path, field_names=BUS_SCHED_FIELD_NAMES)


def multi_bus_schedules(csv_path, date, variants):
    """Fetch bus schedule for route_ids in given csv file."""
    with open(csv_path, mode='r') as csv:
        reader = DictReader(csv)
        for row in reader:
            bus_schedules(route_id=row['RouteID'], date=date,
                          variants=variants)
            time.sleep(10)  # per API specs sleep wait min 10 secs


def route_then_schedules(date, variants):
    """Fetch routes data and then fetch schedules."""
    routes_csv_path = bus_routes(rtn_csv_path=True)
    multi_bus_schedules(csv_path=routes_csv_path, date=date, variants=variants)


def bus_incidents(route_id):
    """Fetch bus incidents/delays."""
    route = None if route_id == 'all' else route_id
    os.makedirs(SAVE_PATH_INCIDENTS, exist_ok=True)
    print('[bus_incidents] Fetching incidents...')
    time_stamp = datetime.now()
    data = WmataApi.get_incidents(api_key=BUS_API_KEY, route_id=route)
    incidents = data['BusIncidents']

    # save data as csv
    f_name = 'bus_incidents_{}.csv'.format(time_stamp)
    path = os.path.join(SAVE_PATH_INCIDENTS, f_name)
    save_csv(data=incidents, path=path, field_names=BUS_INCIDENTS_FIELD_NAMES)


# main script function
def main(data, iters, date, route, variants, csv):
    if data == 'position':
        bus_position(iterations=iters)

    if data == 'route':
        bus_routes()

    if data == 'schedule':
        if csv:
            multi_bus_schedules(csv_path=csv, date=date, variants=variants)
        elif route == 'all':
            route_then_schedules(date=date, variants=variants)
        else:
            bus_schedules(route_id=route, date=date, variants=variants)

    if data == 'incident':
        bus_incidents(route_id=route)


if __name__ == '__main__':
    # configure argument parser
    parser = argparse.ArgumentParser()
    parser.add_argument('data', choices=DATA_CHOICES,
                        help='The data to be fetched and saved.')
    parser.add_argument('--iters', '-i', type=int, default=1,
                        help='The number of fetch iterations for positions data; default: 4800')
    parser.add_argument('--route', '-r', default='all',
                        help='Bus route id for schedules data, e.g. 70, 10A, 10Av1, etc. '
                             'Default behaviour is to fetch routes and process all.')
    parser.add_argument('--csv', '-c',
                        help='Load routes for schedules data from csv file.')
    parser.add_argument('--variants', '-v', type=bool, default=False,
                        help='Whether to include variations if a base route is specified.; Default: False')
    parser.add_argument('--date', '-d',
                        default=datetime.today().strftime('%Y-%m-%d'),
                        help='Date in YYYY-MM-DD format.')

    # parse arguments
    arguments = vars(parser.parse_args())
    main(**arguments)
