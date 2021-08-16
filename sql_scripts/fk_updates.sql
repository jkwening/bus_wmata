ALTER TABLE bus_positions ADD FOREIGN KEY (route_id) REFERENCES bus_routes(route_id);

ALTER TABLE bus_scheds ADD FOREIGN KEY (route_id) REFERENCES bus_routes(route_id);

ALTER TABLE bus_scheds ADD FOREIGN KEY (stop_id) REFERENCES bus_stops(stop_id);