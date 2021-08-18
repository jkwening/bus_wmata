import os
from configparser import ConfigParser
from datetime import datetime

# libraries
import boto3

# dir path constants
ROOT_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__))
)
CONFIG_FILE = os.path.join(ROOT_DIR, 'config.ini')
SAVE_PATH_BUS_POS = os.path.join('data', 'bus_positions')
SAVE_PATH_ROUTES = os.path.join('data', 'routes')
SAVE_PATH_SCHEDULES = os.path.join('data', 'route_scheds')
SAVE_PATH_INCIDENTS = os.path.join('data', 'incidents')
SAVE_PATH_STOPS = os.path.join('data', 'stops')
DATA_PATH_MAP = {
    'bus_positions': SAVE_PATH_BUS_POS,
    'routes': SAVE_PATH_ROUTES,
    'route_scheds': SAVE_PATH_SCHEDULES,
    'incidents': SAVE_PATH_INCIDENTS,
    'stops': SAVE_PATH_STOPS
}

# aws firehose stream name constants
POS_STREAM_NAME = 'wmata-api-bus-positions-stream'
ROUTES_STREAM_NAME = 'wmata-api-routes-stream'
ROUTES_SCHED_STREAM_NAME = 'wmata-api-route-scheds-stream'
STOPS_STREAM_NAME = 'wmata-api-stops-stream'
STOPS_SCHED_STREAM_NAME = 'wmata-api-stop-scheds-stream'

# API data fieldnames
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
DATA_FIELDNAMES_MAP = {
    'bus_positions': BUS_POS_FIELD_NAMES,
    'routes': BUS_ROUTES_FIELD_NAMES,
    'route_scheds': BUS_SCHED_FIELD_NAMES,
    'incidents': BUS_INCIDENTS_FIELD_NAMES,
    'stops': BUS_STOP_FIELD_NAMES
}


def config(section: str) -> dict:
    """Returns parameters for given section of the config.in file."""
    parser = ConfigParser()
    parser.read(CONFIG_FILE)

    # get section parameters
    opts = dict()
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            opts[param[0]] = param[1]
    else:
        raise ValueError(
            f'Section {section} not found in {CONFIG_FILE}')
    return opts


def get_aws_session() -> boto3.Session:
    """Return aws session object."""
    aws_config = config(section='aws')
    return boto3.Session(**aws_config)


def firehose_batch(
        client, data_name: str, records: list,
        stream_name: str, verbose=False
) -> dict:
    # TODO: handle exceptions, likely via storing locally and pushing
    # TODO: subsequently via batch process.
    resp = client.put_record_batch(
        DeliveryStreamName=stream_name,
        Records=records
    )
    if verbose:
        print(f'[{data_name}] Firehose response: {resp}')
    return resp


def firehose_put(
        client, data_name: str, record: dict, stream_name: str,
        verbose=False
) -> dict:
    # TODO: handle exceptions, likely via storing locally and pushing
    # TODO: subsequently via batch process.
    resp = client.put_record(
        DeliveryStreamName=stream_name,
        Record=record
    )
    if verbose:
        print(f'[{data_name}] Firehose response: {resp}')
    return resp


def add_name_timestamp(resp_data: dict, data_name: str) -> dict:
    """Add data_name and EVENT_TIME timestamp to response data."""
    resp_data['EVENT_TIME'] = datetime.now().isoformat()
    resp_data['data_name'] = data_name
    return resp_data


def mkdir_timestamp(data_type: str, level: int = 3) -> str:
    """Expand data directory to include current timestamp subfolders.

        Args:
            data_type (str): type of api data.
            level (int): represents year, month, day, and hour level to
                include in timestamp, respectively.

        Returns:
            The path for the timestamp directory.
        """
    dir_path = DATA_PATH_MAP[data_type]
    levels = datetime.now().strftime('%Y-%m-%d-%H').split('-')
    for i in range(level):
        dir_path = os.path.join(dir_path, levels[i])
    else:
        os.makedirs(name=dir_path, exist_ok=True)

    return dir_path
