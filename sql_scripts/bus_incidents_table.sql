CREATE TABLE IF NOT EXISTS bus_incidents(
	incident_id			varchar,
	last_update			timestamptz,
	incident_desc		varchar,
	incident_type		varchar,
	routes_affected		varchar[],
	PRIMARY KEY(incident_id)
);