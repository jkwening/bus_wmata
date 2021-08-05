# built-in modules
import argparse

# libraries
import json
import schedule
import threading

# project modules
from utils import (
    STREAM_NAME, get_aws_session,
    firehose_put, firehose_batch,
    add_name_timestamp
)
from wmata import (
    get_bus_position, get_routes, get_schedule,
    get_stops, get_stop_schedule, get_route_ids,
    get_stop_ids
)
AWS_FIREHOSE_CLIENT = get_aws_session().client('firehose')


# def fetch_bus_positions(
#         data_name='bus_positions', interval=10, limit=-1, verbose=False
# ) -> None:
#     """Extract bus_position data and load to S3 via firehose.
#
#     Args:
#         data_name (str): name tag for posted data extract.
#         interval (int): delay in seconds before next fetch request.
#         limit (int): set to value greater than 0 to limit number of fetches.
#         verbose (bool): if True, print firehose response element.
#     """
#     counter = 1
#     while True:
#         resp = get_bus_position()
#         data = add_name_timestamp(resp_data=resp.json(), data_name=data_name)
#
#         # TODO: handle exceptions, likely via storing locally and pushing
#         # TODO: subsequently via batch process.
#         # send to firehose for loading
#         f_resp = AWS_FIREHOSE_CLIENT.put_record(
#             DeliveryStreamName=STREAM_NAME,
#             Record={'Data': json.dumps(data)}
#         )
#         if verbose:
#             print(f'Iteration {counter}: {f_resp}')
#
#         # sleep for interval else stop fetching data if specified limit reached
#         if counter == limit:
#             break
#         else:
#             counter += 1
#             sleep(interval)

def fetch_bus_positions(verbose=False) -> None:
    """Extract bus_position data and load to S3 via firehose.

    Args:
        verbose (bool): if True, print firehose response element.
    """
    data_name = 'bus_positions'
    resp = get_bus_position()
    data = add_name_timestamp(resp_data=resp.json(), data_name=data_name)

    # TODO: handle exceptions, likely via storing locally and pushing
    # TODO: subsequently via batch process.
    # send to firehose for loading
    record = {'Data': json.dumps(data)}
    firehose_put(
        client=AWS_FIREHOSE_CLIENT, data_name=data_name,
        record=record, verbose=verbose
    )


def fetch_routes(
        verbose=False
) -> None:
    """Extract routes and route schedules data and load to S3 via firehose.

    Args:
        verbose (bool): if True, print firehose response element.
    """
    data_name = 'bus_routes'
    resp = get_routes()
    data = add_name_timestamp(resp_data=resp.json(), data_name=data_name)
    record = {'Data': json.dumps(data)}
    firehose_put(
        client=AWS_FIREHOSE_CLIENT, data_name=data_name,
        record=record, verbose=verbose
    )

    # process get_schedule()
    data_batch = list()
    data_name = 'bus_route_schedule'
    route_ids = get_route_ids(resp.json())
    for route_id in route_ids:
        sched_resp = get_schedule(route_id)
        sched_data = add_name_timestamp(
            resp_data=sched_resp.json(), data_name=data_name
        )
        data_batch.append({'Data': json.dumps(sched_data)})
        if len(data_batch) == 250:
            firehose_batch(
                client=AWS_FIREHOSE_CLIENT, data_name=data_name,
                records=data_batch, stream_name=STREAM_NAME, verbose=verbose
            )
            data_batch = list()  # reset batch list
    else:  # sending remaining data
        firehose_batch(
            client=AWS_FIREHOSE_CLIENT, data_name=data_name,
            records=data_batch, stream_name=STREAM_NAME, verbose=verbose
        )


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
    record = {'Data': json.dumps(data)}
    firehose_put(
        client=AWS_FIREHOSE_CLIENT, data_name=data_name,
        record=record, verbose=verbose
    )

    # process get_stop_schedule()
    data_batch = list()
    data_name = 'bus_stop_schedule'
    stop_ids = get_stop_ids(resp.json())
    for stop_id in stop_ids:
        sched_resp = get_stop_schedule(stop_id)
        sched_data = add_name_timestamp(
            resp_data=sched_resp.json(), data_name=data_name
        )
        data_batch.append({'Data': json.dumps(sched_data)})
        if len(data_batch) == 250:
            firehose_batch(
                client=AWS_FIREHOSE_CLIENT, data_name=data_name,
                records=data_batch, stream_name=STREAM_NAME, verbose=verbose
            )
            data_batch = list()  # reset batch list
    else:  # sending remaining data
        firehose_batch(
            client=AWS_FIREHOSE_CLIENT, data_name=data_name,
            records=data_batch, stream_name=STREAM_NAME, verbose=verbose
        )


def _run_threaded(job_func, verbose):
    thread_args = verbose,
    job_thread = threading.Thread(target=job_func, args=thread_args)
    job_thread.start()


def extract(verbose=False):
    schedule.every(10).seconds.do(
        _run_threaded, fetch_bus_positions, verbose=verbose
    )
    # extract routes data 3 times a day
    schedule.every().day.at('04:30').do(
        _run_threaded, fetch_routes, verbose=verbose
    )
    schedule.every().day.at('12:30').do(
        _run_threaded, fetch_routes, verbose=verbose
    )
    schedule.every().day.at('00:30').do(
        _run_threaded, fetch_routes, verbose=verbose
    )
    # extract stops data 3 times a day
    schedule.every().day.at('05:00').do(
        _run_threaded, fetch_stops, verbose=verbose
    )
    schedule.every().day.at('13:00').do(
        _run_threaded, fetch_stops, verbose=verbose
    )
    schedule.every().day.at('01:00').do(
        _run_threaded, fetch_stops, verbose=verbose
    )

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
