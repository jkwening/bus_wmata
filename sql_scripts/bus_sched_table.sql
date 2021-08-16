CREATE TABLE bus_scheds(
	fetch_date			timestamptz,
	route_name			varchar,
	trip_end_time		timestamptz,
	route_id			varchar,
	trip_start_time		timestamptz,
	stop_id				int,
	stop_name			varchar,
	stop_seq			int,
	stop_depart_time	timestamptz,
	direction_text		varchar,
	trip_head_sign		varchar,
	trip_id				varchar,
	PRIMARY KEY(route_id, stop_id, stop_depart_time)
);