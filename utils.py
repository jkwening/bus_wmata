import os
from configparser import ConfigParser
from datetime import datetime

# libraries
import boto3

ROOT_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__))
)
CONFIG_FILE = os.path.join(ROOT_DIR, 'config.ini')
POS_STREAM_NAME = 'wmata-api-bus-positions-stream'
ROUTES_STREAM_NAME = 'wmata-api-routes-stream'
ROUTES_SCHED_STREAM_NAME = 'wmata-api-route-scheds-stream'
STOPS_STREAM_NAME = 'wmata-api-stops-stream'
STOPS_SCHED_STREAM_NAME = 'wmata-api-stop-scheds-stream'
S3_DATA_DIR = os.path.join(ROOT_DIR, 's3_data')
ROUTES_DATA_DIR = os.path.join(S3_DATA_DIR, 'routes')


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


def make_data_dir(base_data_dir: str) -> str:
    """Expand data directory to include current timestamp subfolders.

    Args:
        base_data_dir: The data directory to expand from.

    Returns:
        The path for the newly created timestamp directory.
    """
    year, month, day = datetime.now().strftime('%Y/%m/%d').split('/')
    data_dir = os.path.join(
        base_data_dir, year, month, day
    )
    os.makedirs(name=data_dir, exist_ok=True)
    return data_dir
