import os
import psycopg2
import pandas as pd
from datetime import datetime

# project modules
from config import Config


class WmataDB:
    """WMATA Database class."""
    def __init__(self):
        self.__cred = Config.postgres()
        self.__conn = psycopg2.connect(
            user=self.__cred['user'],
            dbname=self.__cred['dbname'],
            password=self.__cred['password']
        )
        self.__DATE = datetime.today().strftime('%m-%d-%Y')
        self.__TEMP_PATH = os.path.join('data', 'temp', self.__DATE)
        os.makedirs(self.__TEMP_PATH, exist_ok=True)

        # constant table names
        self.__STOPS_TABLE_NAME = 'bus_stops'
        self.__ROUTES_TABLE_NAME = 'bus_routes'
        self.__SCHEDULES_TABLE_NAME = 'bus_scheds'

    def __rename_cols(self, table, source_path):
        """Helper function for remapping columns names for db loading."""
        mapper = {
            self.__STOPS_TABLE_NAME: {
                'StopID': 'stop_id',
                'Name': 'stop_name',
                'Lon': 'stop_lon',
                'Lat': 'stop_lat',
                'Routes': 'routes'
            },
            self.__ROUTES_TABLE_NAME: {
                'Name': 'route_name',
                'RouteID': 'route_id',
                'LineDescription': 'line_desc'
            },
            self.__SCHEDULES_TABLE_NAME: {
                'Name': 'route_name',
                'EndTime': 'trip_end_time',
                'RouteID': 'route_id',
                'StartTime': 'trip_start_time',
                'StopID': 'stop_id',
                'StopName': 'stop_name',
                'StopSeq': 'stop_seq',
                'Time': 'stop_depart_time',
                'TripDirectionText': 'direction_text',
                'TripHeadsign': 'trip_head_sign',
                'TripID': 'trip_id'
            }
        }

        # load as dataframe and rename columns
        df = pd.read_csv(filepath_or_buffer=source_path)
        df = df.rename(columns=mapper[table])
        df = df.loc[:, mapper[table].values()]

        #
        save_as = os.path.join(self.__TEMP_PATH,
                             'TEMP_{}'.format(os.path.split(
                                 source_path)[1]))
        df.to_csv(path_or_buf=save_as, index=False, header=True,
                  sep=',')
        return save_as

    def bus_stops_reload(self, csv_path):
        """Deletes bus_stops table entries and updates with new data."""
        with self.__conn:
            with self.__conn.cursor() as curs:
                curs.execute("""DELETE FROM {};""".format(self.__STOPS_TABLE_NAME))
                temp_path = self.__rename_cols(table=self.__STOPS_TABLE_NAME,
                                               source_path=csv_path)
                with open(file=temp_path, mode='r', newline='') as temp:
                    sql = """COPY {} (stop_id, stop_name, stop_lat, stop_lon, routes)
                    FROM STDIN WITH (FORMAT CSV, HEADER TRUE)
                    """.format(self.__STOPS_TABLE_NAME)
                    curs.copy_expert(sql, file=temp)

    def bus_routes_reload(self, csv_path):
        """Deletes bus_routes table entries and updates with new data."""
        with self.__conn:
            with self.__conn.cursor() as curs:
                curs.execute("""DELETE FROM {};""".format(self.__ROUTES_TABLE_NAME))
                temp_path = self.__rename_cols(table=self.__ROUTES_TABLE_NAME,
                                               source_path=csv_path)
                with open(file=temp_path, mode='r', newline='') as temp:
                    sql = """COPY {} (route_name, route_id, line_desc)
                    FROM STDIN WITH (FORMAT CSV, HEADER TRUE)
                    """.format(self.__ROUTES_TABLE_NAME)
                    curs.copy_expert(sql, file=temp)

    def bus_sched_load(self, csv_date_path, delete=False):
        """Deletes bus_schedules table entries and updates with new data."""
        with self.__conn:
            with self.__conn.cursor() as curs:
                if delete:
                    curs.execute("""DELETE FROM {};""".format(self.__SCHEDULES_TABLE_NAME))

                for r, d, f in os.walk(csv_date_path):
                    for name in f:
                        path = os.path.join(csv_date_path, name)
                        temp_path = self.__rename_cols(table=self.__SCHEDULES_TABLE_NAME,
                                                       source_path=path)
                        try:
                            with open(file=temp_path, mode='r', newline='') as temp:
                                sql = """COPY {} (route_name, trip_end_time, route_id,
                                trip_start_time, stop_id, stop_name, stop_seq,
                                stop_depart_time, direction_text, trip_head_sign, trip_id)
                                FROM STDIN WITH (FORMAT CSV, HEADER TRUE)
                                """.format(self.__SCHEDULES_TABLE_NAME)
                                curs.copy_expert(sql, file=temp)
                                self.__conn.commit()
                        except Exception as err:
                            print(err)
                            print('\t[TEMP FILE]', name)
                            self.__conn.rollback()


db = WmataDB()
# db.bus_stops_reload(csv_path='data/stops/2019-09-28/bus_stops_2019-10-06 13:52:02.162077.csv')
# db.bus_routes_reload(csv_path='data/routes/09-23-2019/bus_routes_2019-09-23 22:06:31.524003.csv')
db.bus_sched_load(csv_date_path='data/schedules/09-23-2019')
