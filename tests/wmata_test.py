import unittest
from requests import codes

# project modules
import wmata


class MyTestCase(unittest.TestCase):

    def test_validate_key(self):
        for sub, key in wmata.API_KEYS.items():
            self.assertTrue(
                wmata.validate_key(api_key=key),
                msg=f'Failed validating {sub}'
            )

    def test_get_bus_position(self):
        # case route_id = 70
        resp = wmata.get_bus_position(route_id='70')
        self.assertTrue(
            resp.status_code == codes.ok,
            msg=f'[Route_ID-70] Failed with status code: {resp.status_code}'
        )
        self.assertTrue(resp.json(), msg=f'[Route_ID-70] JSON is empty: {resp.json}')
        r = resp.json()
        self.assertTrue(
            'BusPositions' in r.keys(),
            msg=f'[Route_ID-70] BusPositions not in: {resp.json()}'
        )

        # case lat=39.191525, lon=-76.672821, radius=1000 (meters)
        resp = wmata.get_bus_position(lat=39.191525, lon=-76.672821, radius=1000)
        self.assertTrue(
            resp.status_code == codes.ok,
            msg=f'[Lat,Lon,Radius] Failed with status code: {resp.status_code}'
        )
        self.assertTrue(resp.json(), msg=f'[Lat,Lon,Radius] JSON is empty: {resp.json}')
        r = resp.json()
        self.assertTrue(
            'BusPositions' in r.keys(),
            msg=f'[Lat,Lon,Radius] BusPositions not in: {resp.json()}'
        )

    def test_get_path_details(self):
        # case route_id = 70
        resp = wmata.get_path_details(route_id='70')
        self.assertTrue(
            resp.status_code == codes.ok,
            msg=f'[Route_ID-70] Failed with status code: {resp.status_code}'
        )
        self.assertTrue(resp.json(), msg=f'[Route_ID-70] JSON is empty: {resp.json}')
        r = resp.json()
        # validate response elements
        expected_elements = [
            'Direction0', 'Direction1', 'Name', 'RouteID'
        ]
        for e in expected_elements:
            self.assertTrue(
                e in r.keys(),
                msg=f'[Route_ID-70] response element {e} not in: {resp.json()}'
            )

    def test_get_routes(self):
        resp = wmata.get_routes()
        self.assertTrue(
            resp.status_code == codes.ok,
            msg=f'Failed with status code: {resp.status_code}'
        )
        self.assertTrue(resp.json(), msg=f'JSON is empty: {resp.json}')
        r = resp.json()
        # validate response elements
        expected_elements = [
            'Routes'
        ]
        for e in expected_elements:
            self.assertTrue(
                e in r.keys(),
                msg=f'[Route_ID-70] response element {e} not in: {resp.json()}'
            )

    def test_get_schedule(self):
        # case route_id = 10A
        resp = wmata.get_schedule(route_id='10A', including_variations=True)
        self.assertTrue(
            resp.status_code == codes.ok,
            msg=f'[Route_ID-10A] Failed with status code: {resp.status_code}'
        )
        self.assertTrue(resp.json(), msg=f'[Route_ID-10A] JSON is empty: {resp.json}')
        r = resp.json()
        # validate response elements
        expected_elements = [
            'Direction0', 'Direction1', 'Name'
        ]
        for e in expected_elements:
            self.assertTrue(
                e in r.keys(),
                msg=f'[Route_ID-10A] response element {e} not in: {resp.json()}'
            )

    def test_get_stop_schedule(self):
        # case stop_id = 1001195
        resp = wmata.get_stop_schedule(stop_id='1001195')
        self.assertTrue(
            resp.status_code == codes.ok,
            msg=f'[StopID-1001195] Failed with status code: {resp.status_code}'
        )
        self.assertTrue(resp.json(), msg=f'[StopID-1001195] JSON is empty: {resp.json}')
        r = resp.json()
        # validate response elements
        expected_elements = [
            'ScheduleArrivals', 'Stop'
        ]
        for e in expected_elements:
            self.assertTrue(
                e in r.keys(),
                msg=f'[StopID-1001195] response element {e} not in: {resp.json()}'
            )

    def test_get_stops(self):
        # case lat=39.191525, lon=-76.672821, radius=1000 (meters)
        resp = wmata.get_stops(lat=39.191525, lon=-76.672821, radius=1000)
        self.assertTrue(
            resp.status_code == codes.ok,
            msg=f'[Lat,Lon,Radius] Failed with status code: {resp.status_code}'
        )
        self.assertTrue(resp.json(), msg=f'[Lat,Lon,Radius] JSON is empty: {resp.json}')
        r = resp.json()
        self.assertTrue(
            'Stops' in r.keys(),
            msg=f'[Lat,Lon,Radius] BusPositions not in: {resp.json()}'
        )


if __name__ == '__main__':
    unittest.main()
