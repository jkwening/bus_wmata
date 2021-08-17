# built-in modules
import argparse
import datetime
import json
import os
from csv import DictWriter

# libraries
import schedule
import threading

# project modules
from utils import (
    get_aws_session, add_name_timestamp,
    firehose_put, firehose_batch, POS_STREAM_NAME,
    ROUTES_STREAM_NAME, ROUTES_SCHED_STREAM_NAME,
    STOPS_STREAM_NAME, STOPS_SCHED_STREAM_NAME,
    make_data_dir, ROUTES_DATA_DIR
)
from wmata import (
    get_bus_position, get_routes, get_schedule,
    get_stops, get_stop_schedule, get_route_ids,
    get_stop_ids, BUS_ROUTES_FIELDNAMES
)
AWS_FIREHOSE_CLIENT = get_aws_session().client('firehose')


def send_to_firehose(json_data: str, data_name: str, stream_name: str, verbose=False):
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


def fetch_bus_positions(verbose=False) -> None:
    """Extract bus_position data and load to S3 via firehose.

    Args:
        verbose (bool): if True, print firehose response element.
    """
    data_name = 'bus_positions'
    resp = get_bus_position()

    # iterate BusPositions elements and stream to firehose
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


def _write_csv(data_name: str, file_path: str, data: dict, field_names: list):
    save_to = os.path.join(
        file_path, data_name + datetime.datetime.now().isoformat() + '.csv'
    )
    with open(save_to, 'w') as f:
        writer = DictWriter(f, field_names)
        writer.writeheader()
        for d in data:
            writer.writerow(d)


def fetch_routes() -> None:
    """Extract routes and route schedules data and save to csv.

    Args:
        verbose (bool): if True, print firehose response element.
    """
    data_name = 'bus_routes'
    file_path = make_data_dir(base_data_dir=ROUTES_DATA_DIR)
    resp = get_routes()
    data = resp.json()['Routes']
    _write_csv(
        data_name, file_path, data, BUS_ROUTES_FIELDNAMES
    )

    # process get_schedule()
    data_name = 'bus_route_schedule'
    route_ids = get_route_ids(resp.json())
    for i, route_id in enumerate(route_ids):
        sched_resp = get_schedule(route_id)
        routes = list()
        data = resp.json()
        name = data['Name']


def fetch_stops(
        verbose=False
) -> None:
    """Extract stops and stop schedules data and load to S3 via firehose.

    Args:
        verbose (bool): if True, print firehose response element.
    """
    data_name = 'bus_stops'
    resp = get_stops()
    data = add_name_timestamp(resp_data=resp.json(), data_name=data_name)
    send_to_firehose(
        json_data=json.dumps(data), data_name=data_name,
        stream_name=STOPS_STREAM_NAME, verbose=verbose
    )

    # process get_stop_schedule()
    data_name = 'bus_stop_schedule'
    stop_ids = get_stop_ids(resp.json())
    for i, stop_id in enumerate(stop_ids):
        sched_resp = get_stop_schedule(stop_id)
        print(f'Stop id: {stop_id}, size: {len(sched_resp.content)}')
        sched_data = add_name_timestamp(
            resp_data=sched_resp.json(), data_name=data_name
        )
        send_to_firehose(
            json_data=json.dumps(sched_data), data_name=data_name,
            stream_name=STOPS_SCHED_STREAM_NAME, verbose=verbose
        )


def _run_threaded(job_func, verbose):
    thread_args = verbose,
    job_thread = threading.Thread(target=job_func, args=thread_args)
    job_thread.start()


def extract(verbose=False):
    schedule.every(10).seconds.do(
        _run_threaded, fetch_bus_positions, verbose=verbose
    )
    # # extract routes data 3 times a day
    # schedule.every().day.at('04:30').do(
    #     _run_threaded, fetch_routes, verbose=verbose
    # )
    # schedule.every().day.at('12:30').do(
    #     _run_threaded, fetch_routes, verbose=verbose
    # )
    # schedule.every().day.at('00:30').do(
    #     _run_threaded, fetch_routes, verbose=verbose
    # )
    # # extract stops data 3 times a day
    # schedule.every().day.at('05:00').do(
    #     _run_threaded, fetch_stops, verbose=verbose
    # )
    # schedule.every().day.at('13:00').do(
    #     _run_threaded, fetch_stops, verbose=verbose
    # )
    # schedule.every().day.at('01:00').do(
    #     _run_threaded, fetch_stops, verbose=verbose
    # )

    while True:
        schedule.run_pending()


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser(
        description='Fetch data for WMATA api and load into AWS S3.'
    )
    arg_parser.add_argument(
        '-v', '--verbose', help='if True print firehose response.',
        type=bool, default=False, dest='verbose'
    )
    args = arg_parser.parse_args()
    extract(verbose=args.verbose)
