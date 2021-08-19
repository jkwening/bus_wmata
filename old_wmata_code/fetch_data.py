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
OTHER_API_KEY = Config.wmata_api_key(bus=False)
DATA_CHOICES = ['position', 'route', 'schedule', 'incident', 'stop']
BUS_POS_FIELD_NAMES = ['BlockNumber', 'DateTime', 'Deviation', 'DirectionNum',
                       'DirectionText', 'Lat', 'Lon', 'RouteID', 'TripEndTime',
                       'TripHeadsign', 'TripID', 'TripStartTime', 'VehicleID']
BUS_ROUTES_FIELD_NAMES = ['Name', 'RouteID', 'LineDescription']
BUS_SCHED_FIELD_NAMES = ['Name', 'DirectionNum', 'EndTime', 'RouteID',
                         'StartTime', 'StopID', 'StopName', 'StopSeq', 'Time',
                         'TripDirectionText', 'TripHeadsign', 'TripID']
BUS_INCIDENTS_FIELD_NAMES = ['DateUpdated', 'Description', 'IncidentID',
                             'IncidentType', 'RoutesAffected']
BUS_STOP_FIELD_NAMES = ['StopID', 'Name', 'Lat', 'Lon', 'Routes']
SAVE_PATH_BUS_POS = os.path.join('data', 'bus_positions', DATE)
SAVE_PATH_ROUTES = os.path.join('data', 'routes', DATE)
SAVE_PATH_SCHEDULES = os.path.join('data', 'schedules', DATE)
SAVE_PATH_INCIDENTS = os.path.join('data', 'incidents', DATE)
SAVE_PATH_STOPS = os.path.join('data', 'stops', DATE)


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
    time_stamp = datetime.now().strftime('%m-%d-%Y_%H-%M-%S')
    data = WmataApi.get_incidents(api_key=BUS_API_KEY, route_id=route)
    incidents = data['BusIncidents']

    # save data as csv
    f_name = 'bus_incidents_{}.csv'.format(time_stamp)
    path = os.path.join(SAVE_PATH_INCIDENTS, f_name)
    save_csv(data=incidents, path=path, field_names=BUS_INCIDENTS_FIELD_NAMES)


def __bus_stop(stop_id, date, api_key=BUS_API_KEY):
    """Returns bus stop information."""
    print('\t[__bus_stop] Fetching stop {} information...'.format(stop_id))
    time_stamp = datetime.now()
    data = WmataApi.get_bus_scheduled_stop(api_key=api_key,
                                           stop_id=stop_id, date=date)

    return data['Stop']


def bus_stops(stop_ids: list, date=DATE):
    """Fetch bus stop information for list of stops."""
    os.makedirs(SAVE_PATH_STOPS, exist_ok=True)
    print('[bus_stops] Fetching stops...')
    time_stamp = datetime.now()
    stops_data = list()

    # iterate through stop ids
    for stop in stop_ids:
        data = __bus_stop(stop_id=stop, date=date)
        stops_data.append(data)

    # save data as csv
    f_name = 'bus_stops_{}.csv'.format(time_stamp)
    path = os.path.join(SAVE_PATH_STOPS, f_name)
    save_csv(data=stops_data, path=path, field_names=BUS_STOP_FIELD_NAMES)


def sched_bus_stops(date):
    """Fetch bus stops information using schedules data for given date."""
    save_path = os.path.abspath(os.path.join(SAVE_PATH_STOPS, os.pardir, date))
    os.makedirs(save_path, exist_ok=True)
    sched_path = os.path.abspath(os.path.join(SAVE_PATH_SCHEDULES, os.pardir, DATE))
    print('[sched_bus_stops] Parsing stop ids from schedules in {}'.format(
        sched_path
    ))

    # walk schedule folder
    sched_csvs = list()
    for r, _, f in os.walk(sched_path):
        for file in f:
            path = os.path.join(r, file)
            print('\t[walk] Found {}...'.format(path))
            sched_csvs.append(path)

    # get unique stop ids
    stop_ids = set()
    for sched_csv in sched_csvs:
        with open(sched_csv, mode='r') as csv:
            print('\tGetting stop ids form {}...'.format(sched_csv))
            reader = DictReader(csv)
            for row in reader:
                stop_ids.add(row['StopID'])

    # iterate through stop ids
    time_stamp = datetime.now()
    stops_data = list()
    for stop in list(stop_ids):
        if int(stop) > 0:
            data = __bus_stop(stop_id=stop, date=date, api_key=OTHER_API_KEY)
            stops_data.append(data)

    # save data as csv
    f_name = 'bus_stops_{}.csv'.format(time_stamp)
    path = os.path.join(save_path, f_name)
    save_csv(data=stops_data, path=path, field_names=BUS_STOP_FIELD_NAMES)


# main script function
def main(data, iters, date, route, variants, csv, stops):
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

    if data == 'stop':
        if stops[0] == 'all':
            sched_bus_stops(date=date)
        elif stops:
            bus_stops(stop_ids=stops, date=date)
        else:
            raise ValueError('Stop data requires including stop ids!')


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
    parser.add_argument('--variants', '-v', type=bool, default=True,
                        help='Whether to include variations if a base route is specified.; Default: False')
    parser.add_argument('--date', '-d',
                        default=datetime.today().strftime('%Y-%m-%d'),
                        help='Date in YYYY-MM-DD format.')
    parser.add_argument('--stops', '-s', default=list(), nargs='*',
                        help='List of stop ids to fetch stop information.')

    # parse arguments
    arguments = vars(parser.parse_args())
    main(**arguments)
