import requests


class WmataApi:
    """WMATA API class."""
    bus_routes_url = 'https://api.wmata.com/Bus.svc/json/jRoutes'
    bus_pos_url = 'https://api.wmata.com/Bus.svc/json/jBusPositions'
    bus_path_url = 'https://api.wmata.com/Bus.svc/json/jRouteDetails'
    bus_schedule_url = 'https://api.wmata.com/Bus.svc/json/jRouteSchedule'
    bus_scheduled_stop_url = 'https://api.wmata.com/Bus.svc/json/jStopSchedule'
    bus_nearby_stop_url = 'https://api.wmata.com/Bus.svc/json/jStops'
    bus_incidents_url = 'https://api.wmata.com/Incidents.svc/json/BusIncidents'

    @classmethod
    def get_bus_routes(cls, api_key: str):
        """Returns list of all bus stop info, route, and schedule data.

        Returns:
            - Routes: Array containing route variant information: Name,
                RouteID, and LineDescription.
        """
        headers = {
            'api_key': api_key
        }
        r = requests.get(cls.bus_routes_url, headers=headers)
        return r.json()

    @classmethod
    def get_bus_position(cls, api_key: str, route_id: str = None,
                         lat: int = None, lon: int = None,
                         radius: int = None):
        """
        Returns bus positions for the given route, with an optional
        search radius. If no parameters are specified, all bus positions
        are returned. Note, RouteId pars only accepts base route names
        and no variations. i.e. 10A not 10Av1 or 10Av2.

        Request parameters:
            - RouteId (opt.): Base bus route, e.g. 70, 10A.
            - Lat (opt.): Latitude, required if Longitude and Radius specified.
            - Lon (opt.): Longitude, required if Latitude and Radius specified.
            - Radius (opt.): Radius (m) to include in search area, required if
                Latitude and Longitude are specified.

        Returns:
            - BusPositions: Array containing bus position information:
                DateTime, Deviation, DirectionText, Lat, Lon, RouteID,
                TripEndTime, TripHeadsign, TripID, TripStartTime, and
                VehicleID.
        """
        headers = {
            'api_key': api_key
        }

        # configure optional parameters
        params = dict()
        if route_id is not None:
            params['RouteID'] = route_id

        if lat is not None:
            params['Lat'] = lat

        if lon is not None:
            params['Lon'] = lon

        if radius is not None:
            params['Radius'] = radius

        r = requests.get(cls.bus_pos_url, params=params, headers=headers)
        return r.json()

    @classmethod
    def get_bus_path_details(cls, api_key: str, route_id: str,
                             date: str = None):
        """
        Returns the set of ordered latitude/longitude points along
        along a route variant along with the list of stops served.

        Request parameters:
            - RouteId: Base bus route, e.g. 70, 10A.
            - Date (opt.): Date in YYYY-MM-DD format for which to retrieve
                route and stop information. Defaults to today's date unless
                specified.

        Returns:
            - Direction0/Direction1: Structures describing path/stop
            information, which most will return both but a few that run in
            a loop will return only for Direction0 and NULL content for
            Direction1: DirectionText, Shape, Stops, and TripHeadsign.
            - Name: Descriptive name for the route
            - RouteID: Bus route variant (e.g.: 10A, 10Av1, etc.)
        """
        headers = {
            'api_key': api_key
        }

        # configure parameters
        params = dict()
        params['RouteID'] = route_id

        if date is not None:
            params['Date'] = date

        r = requests.get(cls.bus_path_url, params=params, headers=headers)
        return r.json()

    @classmethod
    def get_bus_schedule(cls, api_key: str, route_id: str,
                         date: str = None, variation: bool = None):
        """
        Returns schedule for a given route variant for a given route.

        Request parameters:
            - RouteId: Base bus route, e.g. 70, 10A.
            - Date (opt.): Date in YYYY-MM-DD format for which to retrieve
                route and stop information. Defaults to today's date unless
                specified.
            - IncludingVariations (opt.): Whether or not to include variations
                if a base route is specified in RouteId. For example, if B30
                is specified and IncludingVariations is set to true, data for
                all variations of B30 such as B30v1, B30v2, etc. will be
                returned.

        Returns:
            - Direction0/Direction1: Arrays containing trip direction
            information, which most will return both but a few that run in
            a loop will return only for Direction0 and NULL content for
            Direction1: EndTime, RouteID, StartTime, StopTimes,
            TripDirectionText, TripHeadsign, and TripID.
            - Name: Descriptive name for the route
        """
        headers = {
            'api_key': api_key
        }

        # configure parameters
        params = dict()
        params['RouteID'] = route_id

        if date is not None:
            params['Date'] = date
        if variation is not None:
            params['IncludingVariations'] = variation

        r = requests.get(cls.bus_schedule_url, params=params, headers=headers)
        return r.json()

    @classmethod
    def get_bus_scheduled_stop(cls, api_key, stop_id: str, date: str = None):
        """
        Returns a set of buses scheduled at a stop for a given date.

        Request parameters:
            - StopID: 7-digit regional stop ID.
            - Date (opt.): Date in YYYY-MM-DD format for which to retrieve
                schedule. Defaults to today's date unless specified.

        Returns:
            - ScheduleArrivals: Array containing scheduled arrival information:
                DirectionNum, EndTime, RouteID, ScheduleTime,
                TripDirectionText,TripHeadsign, and TripID
            - Stop: Dict describing stop information: Lat, Lon, Name, Routes,
                and StopID
        """
        headers = {
            'api_key': api_key
        }

        # configure parameters
        params = dict()
        params['StopID'] = stop_id

        if date is not None:
            params['Date'] = date

        r = requests.get(cls.bus_scheduled_stop_url, params=params, headers=headers)
        return r.json()

    @classmethod
    def get_bus_nearby_stops(cls, api_key: str, lat: int = None,
                             lon: int = None, radius: int = None):
        """
        Returns a list of nearby bus stops based on latitude, longitude,
        and radius. Omit all parameters to retrieve a list of all stops.

        Request parameters:
            - Lat (opt.): Latitude, required if Longitude and Radius specified.
            - Lon (opt.): Longitude, required if Latitude and Radius specified.
            - Radius (opt.): Radius (m) to include in search area, required if
                Latitude and Longitude are specified.

        Returns:
            - Stops: Array containing stop information: Lat, Lon, Name,
                Routes, and StopID.
        """
        headers = {
            'api_key': api_key
        }

        # configure optional parameters
        params = dict()

        if lat is not None:
            params['Lat'] = lat

        if lon is not None:
            params['Lon'] = lon

        if radius is not None:
            params['Radius'] = radius

        r = requests.get(cls.bus_nearby_stop_url, params=params,
                         headers=headers)
        return r.json()

    @classmethod
    def get_incidents(cls, api_key, route_id=None):
        """Returns a set of reported bus incidents/delays for a given Route.

        Omit the Route to return all reported items. Note that the Route
        parameters accepts only base route names and no variants, i.e.:
        use 10A instead of 10Av1 and 10Av2.

        Returns:
            BusIncidents: Array containing bus incidents information:
                DateUpdated, Description, IncidentId, IncidentType, and
                RoutesAffected.
        """
        headers = {
            'api_key': api_key
        }

        # configure optional parameters
        params = dict()
        if route_id is not None:
            params['RouteID'] = route_id

        r = requests.get(cls.bus_incidents_url, params=params, headers=headers)
        return r.json()
