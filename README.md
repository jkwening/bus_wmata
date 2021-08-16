TODO

1. refactor data folder structure: data/data_type_sub/date_stamp/csvs [Done]
    a. move current files into appropriate folders [Done]
2. update fetch_data.py to reflect new data folder structure [Done]
3. add fetch functions for the following:
    a. routes: returns list of all routes variants; essentially master list of routes. Once daily, AM [DONE]
    b. schedule: returns schedules for a given route (variant) for a given date. Once daily, AM.
        Note: TripID is useful for tracking unique bus.  [DONE]
    c. path details: provides lat/lon of stops on given RouteID. Once daily, AM

# wmata_rt_data
Access WMATA API endpoints and store real-time data via AWS Data Lake infrastructure for data science applications
