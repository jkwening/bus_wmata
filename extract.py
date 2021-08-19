# built-in modules
import os
import argparse
from csv import DictWriter
from datetime import datetime
from time import sleep

# libraries
import json

# project modules
from utils import (
    get_aws_session, add_name_timestamp,
    firehose_put, firehose_batch, POS_STREAM_NAME,
    ROUTES_STREAM_NAME, ROUTES_SCHED_STREAM_NAME,
    STOPS_STREAM_NAME, STOPS_SCHED_STREAM_NAME,
    mkdir_timestamp, DATA_FIELDNAMES_MAP
)
from wmata import (
    get_bus_position, get_routes, get_schedule,
    get_stops, get_stop_schedule, get_route_ids,
    get_stop_ids, flatten_route_sched_data,
    get_path_details, flatten_path_details_data
)
AWS_FIREHOSE_CLIENT = get_aws_session().client('firehose')


def _send_to_firehose(json_data: str, data_name: str, stream_name: str, verbose=False):
    if len(json_data) > 1024000:
        # send json_data in chunks of 1000000 bytes or less
        start = 0
        end = 1000000
        chunk_batch = list()
        while True:
            chunk_batch.append({'Data': json_data[start:end]})
            start = end
            end += 1000000
            if end >= len(json_data):
                end = len(json_data) + 1
                chunk_batch.append({'Data': json_data[start:end] + '\n'})
                firehose_batch(
                    client=AWS_FIREHOSE_CLIENT, data_name=data_name,
                    stream_name=stream_name, records=chunk_batch, verbose=verbose
                )
                break
    else:
        record = {'Data': json_data + '\n'}
        firehose_put(
            client=AWS_FIREHOSE_CLIENT, data_name=data_name,
            stream_name=stream_name, record=record, verbose=verbose
        )


def _save_csv(data, api_type: str, path_level=2, custom: str = ''):
    if custom:
        custom = ''.join(e for e in custom if e.isalnum())  # rm spec chars
        file_name = '_'.join([
            api_type, custom, datetime.now().strftime('%m-%d-%Y_%H-%M-%S')
        ]) + '.csv'
    else:
        file_name = '_'.join([
            api_type, datetime.now().strftime('%m-%d-%Y_%H-%M-%S')
        ]) + '.csv'
    path = mkdir_timestamp(
        data_type=api_type, level=path_level
    )
    path = os.path.join(path, file_name)
    with open(path, mode='w', newline='') as csv:
        writer = DictWriter(
            csv, fieldnames=DATA_FIELDNAMES_MAP[api_type]
        )
        writer.writeheader()
        writer.writerows(data)
        print('\t[{}]: saved!'.format(file_name))


def fetch_bus_positions(verbose=False) -> None:
    """Extract bus_position data and load to S3 via firehose.

    Args:
        verbose (bool): if True, print firehose response element.
    """
    data_name = 'bus_positions'
    resp = get_bus_position()

    # iterate BusPositions elements and stream to firehose
    # TODO: add to_csv option
    records = list()
    for bus_pos in resp.json()['BusPositions']:
        records.append({'Data': json.dumps(bus_pos) + '\n'})
        if len(records) == 400:
            firehose_batch(
                client=AWS_FIREHOSE_CLIENT, data_name=data_name,
                stream_name=POS_STREAM_NAME, records=records, verbose=verbose
            )
            records = list()  # reset records for next batch
    else:
        firehose_batch(
            client=AWS_FIREHOSE_CLIENT, data_name=data_name,
            stream_name=POS_STREAM_NAME, records=records, verbose=verbose
        )


def fetch_routes(
        to_csv=True, to_firehose=False, get_sched=True, get_path=True, verbose=False
) -> None:
    """Fetch routes data.

    Args:
        to_csv (bool): save local as csv.
        to_firehose (bool): send data to aws_firehose.
        get_sched (bool): fetch bus stop schedules.
        get_path (bool): fetch path details for each route.
        verbose (bool): if True, print firehose response element.
    """
    data_name = 'routes'
    resp = get_routes()
    if to_csv:
        _save_csv(data=resp.json()['Routes'], api_type=data_name)

    if to_firehose:  # TODO: see bus_positions to complete
        data = add_name_timestamp(resp_data=resp.json(), data_name=data_name)
        raise NotImplementedError

    if get_sched:
        route_ids = get_route_ids(resp.json())
        fetch_route_sched(
            route_ids=route_ids, to_csv=to_csv,
            to_firehose=to_firehose, verbose=verbose
        )

    if get_path:
        route_ids = get_route_ids(resp.json())
        fetch_path_details(
            route_ids=route_ids, to_csv=to_csv,
            to_firehose=to_firehose, verbose=verbose
        )


def fetch_route_sched(
        route_ids: list, to_csv=True, to_firehose=False, verbose=False
) -> None:
    """Fetch route schedules data.

        Args:
            route_ids (list): route ids to fetch.
            to_csv (bool): save local as csv.
            to_firehose (bool): send data to aws_firehose.
            verbose (bool): if True, print firehose response element.
        """
    data_name = 'route_scheds'
    for i, route_id in enumerate(route_ids):
        resp = get_schedule(route_id)
        print(f'Route id: {route_id}, size: {len(resp.content)}')
        data = flatten_route_sched_data(resp_json=resp.json())
        if to_csv:
            _save_csv(
                data=data, api_type=data_name, path_level=3, custom=route_id
            )

        if to_firehose:
            raise NotImplementedError
        sleep(1/10)  # Per API specs 10 calls/second limit


def fetch_stops(
        to_csv=True, to_firehose=False, get_sched=False, verbose=False
) -> None:
    """Fetch stops data.

    Args:
        to_csv (bool): save local as csv.
        to_firehose (bool): send data to aws_firehose.
        get_sched (bool): fetch bus stop schedule.
        verbose (bool): if True, print firehose response element.
    """
    data_name = 'stops'
    resp = get_stops()
    if to_csv:
        _save_csv(data=resp.json()['Stops'], api_type=data_name)

    if to_firehose:  # TODO: see bus_positions to complete
        data = add_name_timestamp(resp_data=resp.json(), data_name=data_name)
        raise NotImplementedError

    if get_sched:
        stop_ids = get_stop_ids(resp.json())
        fetch_stop_scheds(
            stop_ids=stop_ids, to_csv=to_csv,
            to_firehose=to_firehose, verbose=verbose
        )


# TODO: Not fully implemented
def fetch_stop_scheds(
        stop_ids: list, to_csv=True, to_firehose=False, verbose=False
) -> None:
    """Fetch stop schedules data.

        Args:
            stop_ids (list): stop ids to fetch.
            to_csv (bool): save local as csv.
            to_firehose (bool): send data to aws_firehose.
            verbose (bool): if True, print firehose response element.
        """
    data_name = 'stop_schedules'
    for i, stop_id in enumerate(stop_ids):
        resp = get_stop_schedule(stop_id)
        print(f'Stop id: {stop_id}, size: {len(resp.content)}')
        if to_csv:
            raise NotImplementedError

        if to_firehose:  # TODO: see bus_positions to complete
            sched_data = add_name_timestamp(
                resp_data=resp.json(), data_name=data_name
            )
            raise NotImplementedError
        sleep(1/10)  # Per API specs 10 calls/second limit


def fetch_path_details(
        route_ids: list, date='', to_csv=True, to_firehose=False, verbose=False
) -> None:
    """Fetch path details data for specified routes.

        Args:
            route_ids (list): route ids to fetch.
            date (str): Date in YYYY-MM-DD format for which to retrieve
                path details. Defaults to today's date unless specified.
            to_csv (bool): save local as csv.
            to_firehose (bool): send data to aws_firehose.
            verbose (bool): if True, print firehose response element.
        """
    for i, route_id in enumerate(route_ids):
        resp = get_path_details(route_id, date)
        print(f'Route id: {route_id}, size: {len(resp.content)}')
        flat_data = flatten_path_details_data(resp_json=resp.json())
        for data_name, data in flat_data.items():
            if to_csv:
                _save_csv(
                    data=data, api_type=data_name, path_level=3, custom=route_id
                )

            if to_firehose:
                raise NotImplementedError
        sleep(1/10)  # Per API specs 10 calls/second limit


def extract(data, sched, nocsv, date, firehose, verbose, path):
    if data == 'positions':
        fetch_bus_positions(verbose)

    if data == 'routes':
        fetch_routes(
            to_csv=nocsv, to_firehose=firehose,
            get_sched=sched, get_path=path,
            verbose=verbose
        )

    if data == 'stops':
        fetch_stops(
            to_csv=nocsv, to_firehose=firehose,
            get_sched=sched, verbose=verbose
        )


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser(
        description='Fetch data from WMATA API.'
    )
    arg_parser.add_argument(
        'data', choices=['position', 'routes', 'stops'],
        help='The data to be fetched and saved.'
    )
    arg_parser.add_argument(
        '--sched', action='store_true',
        help='Get schedule data, if routes or stops data fetched.'
    )
    arg_parser.add_argument(
        '--path', action='store_true',
        help='Get path details, if routes data fetched.'
    )
    arg_parser.add_argument(
        '--nocsv', action='store_false',
        help='Don\'t save data to csv file.'
    )
    arg_parser.add_argument(
        '--date', '-d',
        default=datetime.today().strftime('%Y-%m-%d'),
        help='Date in YYYY-MM-DD format, for fetching historical schedules data.'
    )
    arg_parser.add_argument(
        '--firehose', action='store_true',
        help='Send fetched data to AWS Firehose.'
    )
    arg_parser.add_argument(
        '-v', '--verbose', action='store_true',
        help='if True print firehose response.',
        dest='verbose'
    )
    extract(**vars(arg_parser.parse_args()))
