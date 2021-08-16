DROP TABLE IF EXISTS bus_positions;

CREATE TYPE direction AS ENUM (
	'NORTH',
	'SOUTH',
	'EAST',
	'WEST',
	'LOOP'
);

CREATE TABLE bus_positions (
	fetch_date			timestamptz,
	route_id			varchar,
	vehicle_id			varchar,
	last_pos_update		timestamptz,
	deviation			int,
	direction_text		direction,
	lon					decimal,
	lat					decimal,
	trip_start_time		varchar,
	trip_end_time		timestamptz,
	trip_head_sign		varchar,
	trip_id				varchar,
	block_num			varchar,
	PRIMARY KEY (fetch_date, vehicle_id)
);