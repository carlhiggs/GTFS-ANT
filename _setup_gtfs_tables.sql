DROP DOMAIN IF EXISTS wgs84_lat CASCADE;
CREATE DOMAIN wgs84_lat AS DOUBLE PRECISION CHECK(VALUE >= -90 AND VALUE <= 90);

DROP DOMAIN IF EXISTS wgs84_lon CASCADE;
CREATE DOMAIN wgs84_lon AS DOUBLE PRECISION CHECK(VALUE >= -180 AND VALUE <= 180);

DROP DOMAIN IF EXISTS gtfstime CASCADE;
CREATE DOMAIN gtfstime AS text CHECK(VALUE ~ '^[0-9]?[0-9]:[0-5][0-9]:[0-5][0-9]$');

CREATE TABLE agency
(
  agency_id         text UNIQUE NULL,
  agency_name       text NOT NULL,
  agency_url        text NOT NULL,
  agency_timezone   text NOT NULL,
  agency_lang       text NULL
);

CREATE TABLE calendar
(
  service_id        text PRIMARY KEY,
  monday            boolean NOT NULL,
  tuesday           boolean NOT NULL,
  wednesday         boolean NOT NULL,
  thursday          boolean NOT NULL,
  friday            boolean NOT NULL,
  saturday          boolean NOT NULL,
  sunday            boolean NOT NULL,
  start_date        numeric(8) NOT NULL,
  end_date          numeric(8) NOT NULL
);

CREATE TABLE calendar_dates
(
  service_id text NOT NULL,
  date numeric(8) NOT NULL,
  exception_type integer NOT NULL
);

CREATE TABLE routes
(
  route_id          text PRIMARY KEY,
  agency_id         text NULL,
  route_short_name  text NULL,
  route_long_name   text NOT NULL,
  route_desc        text NULL,
  route_type        integer NULL,
  route_url         text NULL,
  route_color       text NULL,
  route_text_color  text NULL
);

CREATE TABLE shapes
(
  shape_id          text,
  shape_pt_lat      wgs84_lat NOT NULL,
  shape_pt_lon      wgs84_lon NOT NULL,
  shape_pt_sequence integer NOT NULL,
  shape_dist_traveled double precision NULL
);

-- Note: we later cast shape_dist_travelled to double precision after loading data
-- '' is treated as nulls 
CREATE TABLE stop_times
(
  trip_id           text NOT NULL,
  arrival_time      interval NOT NULL,
  departure_time    interval NOT NULL,
  stop_id           text NOT NULL,
  stop_sequence     integer NOT NULL,
  stop_headsign     text NULL,
  pickup_type       integer NULL CHECK(pickup_type >= 0 and pickup_type <=3),
  drop_off_type     integer NULL CHECK(drop_off_type >= 0 and drop_off_type <=3),
  timepoint         integer NULL,
  shape_dist_traveled text NULL
);
CREATE TABLE stops
(
  stop_id           text PRIMARY KEY,
  stop_name         text NOT NULL,
  stop_lat          wgs84_lat NOT NULL,
  stop_lon          wgs84_lon NOT NULL,
  zone_id           text NULL,
  stop_code         text NULL,
  stop_desc     	text NULL,
  stop_url          text NULL,
  location_type     text NULL,
  parent_station    text NULL,
  district          text NULL
);

CREATE TABLE trips
(
  route_id          text NOT NULL,
  service_id        text NOT NULL,
  trip_id           text NOT NULL PRIMARY KEY,
  shape_id          text NULL,
  trip_headsign     text NULL,
  block_id          text NULL,
  direction_id      boolean NULL
);