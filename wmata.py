# libraries
import requests

# project modules
from utils import config

API_BASE_URL = 'https://api.wmata.com/Bus.svc/json'
API_END_POINTS = {
    'bus_position': f'{API_BASE_URL}/jBusPositions',
    'path_details': f'{API_BASE_URL}/jRouteDetails',
    'routes': f'{API_BASE_URL}/jRoutes',
    'schedule': f'{API_BASE_URL}/jRouteSchedule',
    'stop_schedule': f'{API_BASE_URL}/jStopSchedule',
    'stops': f'{API_BASE_URL}/jStops',
    'validate_key': 'https://api.wmata.com/Misc/Validate'
}
API_KEYS = config('wmata')


def validate_key(api_key: str) -> bool:
    header = {
        'api_key': api_key
    }
    r = requests.get(
        url=API_END_POINTS['validate_key'],
        headers=header
    )
    if r.status_code == requests.codes.ok:
        return True
    return False


def get_bus_position(route_id=None, lat=None, lon=None, radius=None):
    """
    Returns bus positions for the given route, with an optional search radius.

    If no parameters are specified, all bus positions are returned. Note that
    the RouteID parameter accepts only base route names and no variations,
    i.e.: use 10A instead of 10Av1 or 10Av2.

    Bus positions are refreshed approximately every 7 to 10 seconds.

    Args:
        route_id (str): Optional; Base bus route, e.g.: 70, 10A.
        lat (float): Optional; Center point Latitude, required if Longitude
            and Radius are specified.
        lon (float): Optional; Center point Longitude, required if Latitude
            and Radius are specified.
        radius (float): Optional; Radius (meters) to include in the search
            area, required if Latitude and Longitude are specified.

    Returns:
        Request response object where json() method provides the following elements:
            BusPositions - array containing bus position information.
    """
    headers = {
        'api_key': API_KEYS['bus_pos_key']
    }
    # configure api parameters
    params = dict()
    if route_id:
        params['RouteID'] = route_id
    if lat:
        params['Lat'] = lat
    if lon:
        params['Lon'] = lon
    if radius:
        params['Radius'] = radius

    r = requests.get(
        url=API_END_POINTS['bus_position'],
        headers=headers, params=params
    )
    return r


def get_path_details(route_id: str, date=None):
    """
    For a given date, returns the set of ordered latitude/longitude points
    along a route variant along with the list of stops served.

    Args:
        route_id (str): Bus route variant, e.g.: 70, 10A, 10Av1.
        date (str): Optional; Date in YYYY-MM-DD format for which to retrieve
            route and stop information. Defaults to today's date unless specified.

    Returns:
        Request response object where json() method provides the following elements:
            Direction0 - structures describing path/stop information for the route.
            Direction1 - structures describing path/stop information for the route.
            Name - descriptive name for the route.
            RouteID - bus ute variant (e.g.: 10A, 10Av1, ect.)
    """
    headers = {
        'api_key': API_KEYS['default']
    }
    # configure api parameters
    params = dict()
    if route_id:
        params['RouteID'] = route_id
    if date:
        params['Date'] = date

    r = requests.get(
        url=API_END_POINTS['path_details'],
        headers=headers, params=params
    )
    return r


def get_routes():
    """
    Returns a list of all bus route variants (patterns).

    For example, the 10A and 10Av1 are the same route, but may stop at slightly
    different locations.

    Returns:
        Request response object where json() method provides the following elements:
            Routes - array containing route variant information.
    """
    headers = {
        'api_key': API_KEYS['default']
    }

    r = requests.get(
        url=API_END_POINTS['routes'],
        headers=headers
    )
    return r


def get_schedule(route_id: str, date=None, including_variations=None):
    """Returns schedules for a given route variant for a given date.

    Args:
        route_id (str): Bus route variant, e.g.: 70, 10A, 10Av1.
        date (str): Optional; Date in YYYY-MM-DD format for which to retrieve
            schedule. Defaults to today's date unless specified.
        including_variations (bool): Optional; Whether or not to include
            variation

    Returns:
        Request response object where json() method provides the following elements:
            Direction0 - structures describing path/stop information for the route.
            Direction1 - structures describing path/stop information for the route.
            Name - descriptive name for the route.
    """
    headers = {
        'api_key': API_KEYS['bus_route_sched_key']
    }
    # configure api parameters
    params = dict()
    if route_id:
        params['RouteID'] = route_id
    if date:
        params['Date'] = date
    if including_variations:
        params['IncludingVariations'] = including_variations

    r = requests.get(
        url=API_END_POINTS['schedule'],
        headers=headers, params=params
    )
    return r


def get_stop_schedule(stop_id: str, date=None):
    """Returns a set of buses scheduled at a stop for a given date.

        Args:
            stop_id (str): 7-digit regional stop ID.
            date (str): Optional; Date in YYYY-MM-DD format for which to retrieve
                schedule. Defaults to today's date unless specified.

        Returns:
            Request response object where json() method provides the following elements:
                ScheduleArrivals - array containing scheduled arrival information.
                Stop - structures describing stop information.
        """
    headers = {
        'api_key': API_KEYS['default']
    }
    # configure api parameters
    params = dict()
    if stop_id:
        params['StopID'] = stop_id
    if date:
        params['Date'] = date

    r = requests.get(
        url=API_END_POINTS['stop_schedule'],
        headers=headers, params=params
    )
    return r


def get_stops(lat=None, lon=None, radius=None):
    """
    Returns a list of nearby bus stops based on latitude, longitude, and radius.

    Omit all parameters to retrieve a list of all stops.

    Args:
        lat (float): Optional; Center point Latitude, required if Longitude
            and Radius are specified.
        lon (float): Optional; Center point Longitude, required if Latitude
            and Radius are specified.
        radius (float): Optional; Radius (meters) to include in the search
            area, required if Latitude and Longitude are specified.

    Returns:
        Request response object where json() method provides the following elements:
            Stops - array containing stop information.
    """
    headers = {
        'api_key': API_KEYS['bus_pos_key']
    }
    # configure api parameters
    params = dict()
    if lat:
        params['Lat'] = lat
    if lon:
        params['Lon'] = lon
    if radius:
        params['Radius'] = radius

    r = requests.get(
        url=API_END_POINTS['stops'],
        headers=headers, params=params
    )
    return r


def get_route_ids(routes_data: dict) -> list:
    """Return list of route ids from routes resp.json() data."""
    route_ids = list()
    for route in routes_data['Routes']:
        route_ids.append(route['RouteID'])
    return route_ids


def get_stop_ids(stops_data: dict) -> list:
    """Return list of stop ids from stops resp.json() data."""
    stop_ids = list()
    for stop in stops_data['Stops']:
        stop_ids.append(stop['StopID'])
    return stop_ids
