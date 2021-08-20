import os
from datetime import datetime

# libraries import
import pandas as pd

# project imports
from utils import (
    STOPS_ANALYSIS_DIR, SAVE_PATH_STOPS, SAVE_PATH_DET_STOPS
)


def create_stops_nodes(csv_file: str, save_to: str = STOPS_ANALYSIS_DIR) -> pd.DataFrame:
    stops_df = pd.read_csv(csv_file)
    # add Id column as first column and transform 0s
    stops_df.insert(0, 'Id', stops_df.StopID)
    for n, idx in enumerate(stops_df[stops_df.StopID == 0].index, 1):
        node_id = 10 * n
        stops_df.loc[idx, 'Id'] = node_id

    if save_to:
        file_name = '_'.join([
            'stops_nodes', datetime.now().strftime('%m-%d-%Y_%H-%M-%S')
        ]) + '.csv'
        path = os.path.join(
            STOPS_ANALYSIS_DIR, 'network_data', file_name
        )
        stops_df.to_csv(path, index=False)
        print('\t[{}]: saved!'.format(file_name))
    return stops_df


def _get_node_id(node_df: pd.DataFrame, lat: float, lon: float) -> list:
    """Get Id from nodes dataframe given lat and lon."""
    return node_df.loc[(node_df.Lat == lat) & (node_df.Lon == lon), 'Id'].array


def create_stops_edges(
        details_stops_dir: str, nodes_df: pd.DataFrame, save_to: str = STOPS_ANALYSIS_DIR
) -> None:
    # process files in details_stops_dir
    edges = list()
    print('Finding edges...')
    for csv_file in os.listdir(details_stops_dir):
        print(f'\tProcessing {csv_file}...')
        paths_df = pd.read_csv(
            os.path.join(details_stops_dir, csv_file)
        )
        # iterate rows in paths df to generate edges data
        for i in range(1, len(paths_df)):
            cur_dir, cur_stop_id, cur_lat, cur_lon = paths_df.loc[
                i, ['DirectionText', 'StopID', 'Lat', 'Lon']
            ]
            source_row = paths_df.iloc[i - 1]

            # skip to next row, if current and previous rows are diff paths
            if cur_dir != source_row.DirectionText:
                continue

            # remap stopIDs with 0s to appropriate ID node
            if source_row.StopID == 0:
                source = _get_node_id(
                    node_df=nodes_df, lat=source_row.Lat, lon=source_row.Lon
                )[0]
            else:
                source = source_row.StopID

            if cur_stop_id == 0:
                target = _get_node_id(
                    node_df=nodes_df, lat=cur_lat, lon=cur_lon
                )[0]
            else:
                target = cur_stop_id

            # define edge node for path
            edges.append({
                'Source': source, 'Target': target, 'Type': 'Directed', 'Weight': 1,
                'RouteId': source_row.RouteID,
                'DirectionText': source_row.DirectionText,
                'TripHeadSign': source_row.TripHeadsign,
                'PathSeq': source_row.StopNum
            })
        print('\t\t\tcomplete!')
    else:
        edges_df = pd.DataFrame.from_records(edges)

    if save_to:
        file_name = '_'.join([
            'stops_edges', datetime.now().strftime('%m-%d-%Y_%H-%M-%S')
        ]) + '.csv'
        path = os.path.join(
            STOPS_ANALYSIS_DIR, 'network_data', file_name
        )
        edges_df.to_csv(path, index=False)
        print('\t[{}]: saved!'.format(file_name))


def _find_file_by_timestamp(date: str, list_files: list) -> str:
    """Return the most recent file name that has timestamp."""
    results = list()
    for f in list_files:
        if date in f:
            results.append(f)
    else:
        results.sort(reverse=True)
    return results[0] if len(results) > 0 else ''


def generate_network_files(date: str):
    month, day, year = date.split('-')
    # get the most recent stops csv file from specified data
    stops_files = os.listdir(
        os.path.join(SAVE_PATH_STOPS, year, month)
    )
    stops_csv = _find_file_by_timestamp(date, stops_files)
    csv_file = os.path.join(SAVE_PATH_STOPS, year, month, stops_csv)
    stops_df = create_stops_nodes(csv_file=csv_file)
    details_stops_dir = os.path.join(
        SAVE_PATH_DET_STOPS, year, month, day
    )
    create_stops_edges(
        details_stops_dir=details_stops_dir, nodes_df=stops_df
    )


generate_network_files(date='08-19-2021')
