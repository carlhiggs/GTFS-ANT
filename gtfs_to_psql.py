## SQL cript to import GTFS data (for Victoria) to Postgresql
## Carl Higgs 20190713

import psycopg2         ,\
       time             ,\
       getpass          ,\
       os               ,\
       sys              ,\
       datetime         ,\
       argparse         ,\
       subprocess as sp
from sqlalchemy import create_engine
from StringIO import StringIO
from zipfile import ZipFile
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

cwd = os.path.dirname(sys.argv[0])
print(cwd)

def valid_path(arg):
    if not os.path.exists(arg):
        msg = "The path %s does not exist!" % arg
        raise argparse.ArgumentTypeError(msg)
    else:
        return arg
  
task = 'Import GTFS feeds from zip files, creating databases as per filename'
# Parse input arguments
parser = argparse.ArgumentParser(description=task)
parser.add_argument('-db',
                    help='Admin database',
                    default='postgres',
                    type=str)
parser.add_argument('-U',
                    help='SQL user',
                    default='postgres',
                    type=str)
parser.add_argument('-w',
                    help='SQL password',
                    type=str)
parser.add_argument('-dir',
                    help='parent directory',
                    default=cwd,
                    type=valid_path)
args = parser.parse_args()


# connect to database

# engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = args.U,
                                                                      # pwd  = args.w,
                                                                      # host = 'local_host',
                                                                      # db   = args.db))
print(task)
print('''
It is recommended that you ensure your GTFS file names are
  - lower case
  - no spaces
  - begin with a character not number
  - descriptive and clear as to what they representative

For example: "gtfs_vic_ptv_20180413" follows a schema, 'gtfs_state_agency_yyyymmdd' 
This will allow for easy storage and retrieval of multiple gtfs feed databases.
''')
required_files = ['agency.txt', 'calendar.txt', 'calendar_dates.txt', 'routes.txt', 'shapes.txt', 'stop_times.txt', 'stops.txt', 'trips.txt'] 

required_tables_sql = '''
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
  route_type        integer NULL,
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

-- CREATE TABLE stop_times
-- (
  -- trip_id           text NOT NULL,
  -- arrival_time      interval NOT NULL,
  -- departure_time    interval NOT NULL,
  -- stop_id           text NOT NULL,
  -- stop_sequence     integer NOT NULL,
  -- stop_headsign     text NULL,
  -- pickup_type       integer NULL CHECK(pickup_type >= 0 and pickup_type <=3),
  -- drop_off_type     integer NULL CHECK(drop_off_type >= 0 and drop_off_type <=3),
  -- shape_dist_traveled double precision NULL
-- );
-- Using a work around due to empty value in shape_dist_traveled field; can't be interpreted as numeric directly
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
  shape_dist_traveled text NULL
);

CREATE TABLE stops
(
  stop_id           text PRIMARY KEY,
  stop_name         text NOT NULL,
  stop_lat          wgs84_lat NOT NULL,
  stop_lon          wgs84_lon NOT NULL
);

CREATE TABLE trips
(
  route_id          text NOT NULL,
  service_id        text NOT NULL,
  trip_id           text NOT NULL PRIMARY KEY,
  shape_id          text NULL,
  trip_headsign     text NULL,
  direction_id      boolean NULL
);
'''

create_30_mins_stops_function = '''
CREATE OR REPLACE FUNCTION bus_30_min_stops(date) RETURNS SETOF text AS $$
DECLARE
  service_date ALIAS FOR $1;
  -- for now commented out; i think a second agency_id arg would be good alternative to route colour
  -- agency_id ALIAS FOR $2;
BEGIN

DROP TABLE IF EXISTS stop_departure_bus CASCADE;
CREATE TABLE stop_departure_bus AS
SELECT DISTINCT
  routes.route_id,
  agency_id,
  route_type,
  route_color,
  trips.trip_id,
  stops.stop_id,
  stop_sequence,
  departure_time
FROM  
  routes,
  trips,
  stop_times,
  stops,
  calendar_series
WHERE
  routes.route_id = trips.route_id AND
  trips.service_id = calendar_series.service_id AND
  stop_times.trip_id = trips.trip_id AND
  stop_times.stop_id = stops.stop_id AND
  -- daytime bus services
  stop_times.departure_time BETWEEN '07:00:00' AND '19:00:00' AND
  routes.route_type = '3' AND
  -- exclude coach services
  --  NOTE!!!!! CH 20190715 This will not work on old GTFS data which does not have this field
  routes.route_color != 'A57FB2' AND
  -- offered on service_date
  calendar_series.date = to_number(to_char(service_date, 'YYYYMMDD'), '99999999')
ORDER BY
  trip_id,
  stop_sequence;

DROP MATERIALIZED VIEW IF EXISTS stop_departure_intervals_bus CASCADE;
CREATE MATERIALIZED VIEW stop_departure_intervals_bus AS
SELECT
  stop_id,
  departure_time,
  lag(departure_time) OVER (PARTITION BY stop_id ORDER BY departure_time DESC) as next_departure_time
FROM
  stop_departure_bus
ORDER BY
  stop_id,
  departure_time;

-- Find earliest stop after 7.00am
CREATE OR REPLACE VIEW stop_first_peak_service_bus AS
SELECT 
  stop_id,
  MIN (departure_time)
FROM 
  stop_departure_intervals_bus
GROUP BY
  stop_id
ORDER BY
  stop_id;

-- Find stops with a peak service commencement before 7.30am
CREATE OR REPLACE VIEW stop_first_service_before_0730_bus AS
SELECT
  stop_id
FROM
  stop_first_peak_service_bus
WHERE
  min <= '07:30:00';

-- Find latest stop before 7.00pm
CREATE OR REPLACE VIEW stop_last_peak_service_bus AS
SELECT 
  stop_id,
  MAX (departure_time)
FROM 
  stop_departure_intervals_bus
GROUP BY
  stop_id
ORDER BY
  stop_id;

-- Find stops with a peak service after 6.30pm
CREATE OR REPLACE VIEW stop_last_service_after_1830_bus AS
SELECT
  stop_id
FROM
  stop_last_peak_service_bus
WHERE
  max >= '18:30:00';

-- Find maximum interval between services for stops with a service before 7.30am and after 6.30pm 
CREATE OR REPLACE VIEW stop_max_interval_bus AS
SELECT 
  s.stop_id, 
  MAX (s.next_departure_time - s.departure_time) AS max_interval
FROM 
  stop_departure_intervals_bus AS s,
  stop_first_service_before_0730_bus AS f,
  stop_last_service_after_1830_bus AS l
WHERE
  s.next_departure_time IS NOT NULL AND
  s.stop_id = f.stop_id AND
  s.stop_id = l.stop_id
GROUP BY
  s.stop_id
ORDER BY
  s.stop_id;

DROP TABLE IF EXISTS stop_30_mins_bus;
CREATE TABLE stop_30_mins_bus AS
SELECT 
  s.agency_id,
  s.stop_id,
  s.stop_name,
  s.stop_lat, 
  s.stop_lon,
  s.route_color,
  s.geom,
  (SELECT EXTRACT(epoch FROM i.max_interval)/60) AS max_interval
FROM
  stop_bus AS s,
  stop_max_interval_bus AS i 
WHERE 
  s.stop_id = i.stop_id AND
  i.max_interval <= '00:30:00';

RETURN QUERY SELECT stop_id FROM stop_30_mins_bus;
  
END;
$$ LANGUAGE plpgsql;
'''


for root, dirs, files in os.walk(args.dir):
  for file in files:
    if file.endswith(".zip"):
      path = '{}/{}'.format(root,file)
      print('\n{}'.format(path))
      name = os.path.splitext(file)[0]            
      with ZipFile(path) as myzip:
        file_list = ZipFile.namelist(myzip)
        # print("Zip contents: {}".format(file_list))
        test_contents = [x for x in required_files if x not in file_list]
        if (len(test_contents)!=0):
          print("The zip file appears to be missing the following required files: {}".format(test_contents))
          continue
        else:
            print("\t- Required files are present.")
            conn = psycopg2.connect(dbname=args.db, user=args.U, password=args.w)
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT) # <-- ADD THIS LINE
            curs = conn.cursor()

            # check if sql database exists
            sql =  '''SELECT 1 FROM pg_catalog.pg_database WHERE datname='{}';'''.format(name)
            curs.execute(sql)
            if len(curs.fetchall())!=0:
              print("\t- Database already exists!")
              conn.close()
              continue
            else:
              # SQL queries
              sql =  '''
                CREATE DATABASE {};
               '''.format(name.lower())  
              curs.execute(sql)
              conn.commit()
              print("\t- Created database: {}".format(name.lower()))
              conn.close()
              # connect to new database
              conn = psycopg2.connect(dbname=name, user=args.U, password=args.w)
              curs = conn.cursor()
              print("\t- Connected to {}".format(name.lower()))
              curs.execute(required_tables_sql)
              conn.commit()
              print("\t- Created required tables.")
              for table in ['agency','calendar','calendar_dates','routes','shapes','stop_times','stops','trips']:
                with myzip.open('{}.txt'.format(table)) as myfile:
                  try:
                    sql = '''COPY {table} FROM STDIN WITH CSV HEADER;'''.format(table = table)
                    curs.copy_expert(sql, myfile)
                  except:
                    print("\t\t- Issue found with {}... ".format(table))
                    if table=='routes':
                      conn = psycopg2.connect(dbname=name, user=args.U, password=args.w)
                      curs = conn.cursor()
                      # known likely issue with legacy feeds, not having route colour variable
                      # so we try again and specify specific fields in hope it works
                      sql = '''
                      COPY {table} 
                           (route_id,agency_id,route_short_name,route_long_name,route_type) 
                      FROM STDIN WITH CSV HEADER;
                      '''.format(table = table)
                      curs.copy_expert(sql, myfile)
                    else:
                        print("  failed.")
                        conn.close()
                        raise
              print("\t- Updated required tables with data.")
              sql = '''
                ALTER TABLE stop_times 
                    ALTER COLUMN shape_dist_traveled TYPE double precision USING NULLIF(shape_dist_traveled, '')::double precision'''
              curs.execute(sql)
              conn.commit()
              sql = '''
              CREATE EXTENSION postgis;
              CREATE EXTENSION tablefunc;
              '''
              curs.execute(sql)
              conn.commit()
              print("\t- Created postgis extension.")
              conn.close()

amended_sql_from_ga_editing = '''
--------------------
-- Calendar setup -- 
--------------------

-- needed for crosstab calculations later
CREATE EXTENSION tablefunc;

-- this generates the series of dates (and dow) for all dates covered by the calendar
DROP TABLE IF EXISTS calendar_extent;
CREATE TABLE calendar_extent AS
SELECT
  to_number(to_char(date, 'YYYYMMDD'), '99999999') AS date_numeric,
  extract(dow from date)::int AS dow
FROM
  generate_series (
    (SELECT to_date(to_char(MIN(start_date), '99999999'), 'YYYYMMDD') FROM calendar),
    (SELECT to_date(to_char(MAX(end_date), '99999999'), 'YYYYMMDD') FROM calendar),
	interval '1 day'
  ) date;

-- this generates a table that summarises the maximum number of days a stop_id could be a 30 minute stop by dow
DROP TABLE IF EXISTS calendar_maximum;
CREATE TABLE calendar_maximum AS
SELECT * FROM crosstab (
  'SELECT DISTINCT
    stop_id,
    dow,
    count(date_numeric) dow_count
  FROM
    stops,
    calendar_extent
  GROUP BY
    stop_id,
    dow
  ORDER BY
    stop_id ASC,
    dow ASC',
  'SELECT * FROM generate_series(0, 6) ORDER BY 1'
)
  AS (
    stop_id text,
	sunday int,
	monday int,
	tuesday int,
	wednesday int,
	thursday int,
	friday int,
	saturday int
)
;

-- this function generates a series of dates between two days  
CREATE OR REPLACE FUNCTION dayseries (date, date)
  RETURNS SETOF timestamp with time zone AS
$$
  SELECT * FROM generate_series($1, $2, interval '1d') d
$$ LANGUAGE SQL IMMUTABLE;

-- this generates a service_id and a series of dates corresponding to the calendar
DROP TABLE IF EXISTS calendar_series;
CREATE TABLE calendar_series AS
SELECT
  service_id,
  to_number(to_char(dayseries(to_date(to_char(start_date, '99999999'), 'YYYYMMDD'), to_date(to_char(end_date, '99999999'), 'YYYYMMDD'))::date, 'YYYYMMDD'), '99999999') AS date
FROM
  calendar
ORDER BY
  service_id,
  date;

  
ALTER TABLE calendar_series ADD COLUMN dow integer;
UPDATE calendar_series SET dow = extract(dow from to_date(to_char(date, '99999999'), 'YYYYMMDD'))::int;

-- this deletes from the series dates on which the named day is "false"
DELETE FROM calendar_series
WHERE (service_id, dow) IN (
  SELECT
    service_id,
    dow
  FROM (
    SELECT
      service_id,
      unnest(
        array[1, 2, 3, 4, 5, 6, 0]
      ) AS dow,
      unnest(
        array[monday, tuesday, wednesday, thursday, friday, saturday, sunday]
      ) AS operates
    FROM calendar) s
  WHERE
    operates = 'F');
  
-- this deletes services as required based on the calendar_dates table
DELETE FROM calendar_series
WHERE (service_id, date) IN (SELECT service_id, date FROM calendar_dates WHERE exception_type = '2');

-- this adds services as required based on the calendar_dates table
INSERT INTO calendar_series SELECT service_id, date, extract(dow from to_date(to_char(date, '99999999'), 'YYYYMMDD'))::int FROM calendar_dates WHERE exception_type = '1';

--------------------------------------------------
-- Create train, tram and bus (and coach) stops --
--------------------------------------------------

SELECT
  agency_id,
  route_type
FROM
  routes
GROUP BY
  agency_id,
  route_type
ORDER BY
  route_type;

-- JA code assumes stops has geom; let's create
ALTER TABLE stops add column geom geometry(Point, 4326);
UPDATE stops set geom=st_SetSrid(st_MakePoint(stop_lon, stop_lat), 4326);

DROP TABLE IF EXISTS stop_train;
CREATE TABLE stop_train AS
SELECT
  routes.agency_id,
  stops.stop_id,
  stops.stop_name,
  stops.stop_lat,
  stops.stop_lon,
  stops.geom
FROM
  public.stops,
  public.stop_times,
  public.trips,
  public.routes
WHERE 
  stops.stop_id = stop_times.stop_id AND
  stop_times.trip_id = trips.trip_id AND
  trips.route_id = routes.route_id AND
  -- allow for consideration of both metropolitan and regional trains
  routes.route_type IN ('1','2') AND
  routes.agency_id IN('1','2')
GROUP BY
  routes.agency_id,
  stops.stop_id,
  stops.stop_name,
  stops.stop_lat,
  stops.stop_lon,
  stops.geom
ORDER BY
  stops.stop_id;

DROP TABLE IF EXISTS stop_tram;
CREATE TABLE stop_tram AS
SELECT
  routes.agency_id,
  stops.stop_id,
  stops.stop_name,
  stops.stop_lat,
  stops.stop_lon,
  stops.geom
FROM
  public.stops,
  public.stop_times,
  public.trips,
  public.routes
WHERE 
  stops.stop_id = stop_times.stop_id AND
  stop_times.trip_id = trips.trip_id AND
  trips.route_id = routes.route_id AND
  routes.route_type = 0 AND
  routes.agency_id = '3'
GROUP BY
  routes.agency_id,
  stops.stop_id,
  stops.stop_name,
  stops.stop_lat,
  stops.stop_lon,
  routes.route_color,
  stops.geom
ORDER BY
  stops.stop_id;
  
DROP TABLE IF EXISTS stop_bus;
CREATE TABLE stop_bus AS
SELECT
  routes.agency_id,
  stops.stop_id,
  stops.stop_name,
  stops.stop_lat,
  stops.stop_lon,
  routes.route_color,
  stops.geom
FROM
  public.stops,
  public.stop_times,
  public.trips,
  public.routes
WHERE 
  stops.stop_id = stop_times.stop_id AND
  stop_times.trip_id = trips.trip_id AND
  trips.route_id = routes.route_id AND
  routes.route_type = 3 AND
  -- allow for consideration of both metropolitan and regional buses
  routes.agency_id IN ('4','6')
GROUP BY
  routes.agency_id,
  stops.stop_id,
  stops.stop_name,
  stops.stop_lat,
  stops.stop_lon,
  routes.route_color,
  stops.geom
ORDER BY
  stops.stop_id;
  
DROP TABLE IF EXISTS stop_coach;
CREATE TABLE stop_coach AS
SELECT
  routes.agency_id,
  stops.stop_id,
  stops.stop_name,
  stops.stop_lat,
  stops.stop_lon,
  routes.route_color,
  stops.geom
FROM
  public.stops,
  public.stop_times,
  public.trips,
  public.routes
WHERE 
  stops.stop_id = stop_times.stop_id AND
  stop_times.trip_id = trips.trip_id AND
  trips.route_id = routes.route_id AND
  routes.route_type = 3 AND
  routes.agency_id IN ('5')
GROUP BY
  routes.agency_id,
  stops.stop_id,
  stops.stop_name,
  stops.stop_lat,
  stops.stop_lon,
  routes.route_color,
  stops.geom
ORDER BY
  stops.stop_id;

-----------------------------
-- TRAIN interval analysis --
-----------------------------

CREATE OR REPLACE FUNCTION train_30_min_stops(date) RETURNS SETOF text AS $$
DECLARE
  service_date ALIAS FOR $1;
BEGIN

DROP TABLE IF EXISTS stop_departure_train CASCADE;
CREATE TABLE stop_departure_train AS
SELECT DISTINCT
  routes.route_id,
  agency_id,
  route_type,
  route_color,
  trips.trip_id,
  stops.stop_id,
  stop_sequence,
  departure_time
FROM  
  routes,
  trips,
  stop_times,
  stops,
  calendar_series
WHERE
  routes.route_id = trips.route_id AND
  trips.service_id = calendar_series.service_id AND
  stop_times.trip_id = trips.trip_id AND
  stop_times.stop_id = stops.stop_id AND
  -- daytime train services
  stop_times.departure_time BETWEEN '07:00:00' AND '19:00:00' AND
  routes.route_type = 2  AND
  routes.agency_id IN('1','2')
  -- offered on service_date
  calendar_series.date = to_number(to_char(service_date, 'YYYYMMDD'), '99999999')
ORDER BY
  trip_id,
  stop_sequence;

DROP MATERIALIZED VIEW IF EXISTS stop_departure_intervals_train CASCADE;
CREATE MATERIALIZED VIEW stop_departure_intervals_train AS
SELECT
  stop_id,
  departure_time,
  lag(departure_time) OVER (PARTITION BY stop_id ORDER BY departure_time DESC) as next_departure_time
FROM
  stop_departure_train
ORDER BY
  stop_id,
  departure_time;

-- Find earliest stop after 7.00am
CREATE OR REPLACE VIEW stop_first_peak_service_train AS
SELECT 
  stop_id,
  MIN (departure_time)
FROM 
  stop_departure_intervals_train
GROUP BY
  stop_id
ORDER BY
  stop_id;

-- Find stops with a peak service commencement before 7.30am
CREATE OR REPLACE VIEW stop_first_service_before_0730_train AS
SELECT
  stop_id
FROM
  stop_first_peak_service_train
WHERE
  min <= '07:30:00';

-- Find latest stop before 7.00pm
CREATE OR REPLACE VIEW stop_last_peak_service_train AS
SELECT 
  stop_id,
  MAX (departure_time)
FROM 
  stop_departure_intervals_train
GROUP BY
  stop_id
ORDER BY
  stop_id;

-- Find stops with a peak service after 6.30pm
CREATE OR REPLACE VIEW stop_last_service_after_1830_train AS
SELECT
  stop_id
FROM
  stop_last_peak_service_train
WHERE
  max >= '18:30:00';

-- Find maximum interval between services for stops with a service before 7.30am and after 6.30pm 
CREATE OR REPLACE VIEW stop_max_interval_train AS
SELECT 
  s.stop_id, 
  MAX (s.next_departure_time - s.departure_time) AS max_interval
FROM 
  stop_departure_intervals_train AS s,
  stop_first_service_before_0730_train AS f,
  stop_last_service_after_1830_train AS l
WHERE
  s.next_departure_time IS NOT NULL AND
  s.stop_id = f.stop_id AND
  s.stop_id = l.stop_id
GROUP BY
  s.stop_id
ORDER BY
  s.stop_id;

DROP TABLE IF EXISTS stop_30_mins_train;
CREATE TABLE stop_30_mins_train AS
SELECT 
  s.agency_id,
  s.stop_id,
  s.stop_name,
  s.stop_lat, 
  s.stop_lon,
  s.route_color,
  s.geom,
  (SELECT EXTRACT(epoch FROM i.max_interval)/60) AS max_interval
FROM
  stop_train AS s,
  stop_max_interval_train AS i 
WHERE 
  s.stop_id = i.stop_id AND
  i.max_interval <= '00:30:00';

RETURN QUERY SELECT stop_id FROM stop_30_mins_train;
  
END;
$$ LANGUAGE plpgsql;



-- after the stored procedures have been defined, this generates the dates for each stop_id where that stop_id provides a 30 minute frequency service
DROP TABLE IF EXISTS train_30_min_stops_by_date;
CREATE TABLE train_30_min_stops_by_date AS
SELECT DISTINCT
  date_numeric,
  train_30_min_stops(to_date(to_char(date_numeric, '99999999'), 'YYYYMMDD')) stop_id
FROM
  calendar_extent
ORDER BY
  date_numeric;
  
-- view the results
SELECT
  date_numeric,
  count(stop_id)
FROM
  (SELECT DISTINCT * FROM train_30_min_stops_by_date) t
GROUP BY
  date_numeric
ORDER BY
  date_numeric;

-- create the crosstab query
DROP TABLE IF EXISTS train_stop_dow;
CREATE TABLE train_stop_dow AS
SELECT * FROM crosstab (
  'SELECT DISTINCT
    stop_id,
    extract(dow from to_date(to_char(date_numeric, ''99999999''), ''YYYYMMDD''))::int AS dow,
	count(date_numeric) dow_count
  FROM
    (SELECT DISTINCT * FROM train_30_min_stops_by_date) t
  -- exclude school holiday periods and data issues whereby core rail services are not in the timetable from ?
  WHERE
    date_numeric BETWEEN 20181008 AND 20181205
  GROUP BY
    stop_id,
    dow
  ORDER BY
    stop_id,
    dow',
  'SELECT * FROM generate_series(0, 6) ORDER BY 1'
)
  AS (
    stop_id text,
	sunday int,
	monday int,
	tuesday int,
	wednesday int,
	thursday int,
	friday int,
	saturday int
)
;

-- this generates a table that summarises the maximum number of days a stop_id could be a 30 minute stop by dow
DROP TABLE IF EXISTS calendar_train_maximum;
CREATE TABLE calendar_train_maximum AS
SELECT * FROM crosstab (
  'SELECT DISTINCT
    stop_id,
    dow,
    count(date_numeric) dow_count
  FROM
    stops,
    calendar_extent
  WHERE
    date_numeric BETWEEN 20181008 AND 20181205
  GROUP BY
    stop_id,
    dow
  ORDER BY
    stop_id ASC,
    dow ASC',
  'SELECT * FROM generate_series(0, 6) ORDER BY 1'
)
  AS (
    stop_id text,
	sunday int,
	monday int,
	tuesday int,
	wednesday int,
	thursday int,
	friday int,
	saturday int
)
;

DROP TABLE IF EXISTS train_stop_pcent;
CREATE TABLE train_stop_pcent AS
SELECT
  b.stop_id,
  100*(coalesce(b.monday,0) + coalesce(b.tuesday,0) + coalesce(b.wednesday,0) + coalesce(b.thursday,0) + coalesce(b.friday,0))/(m.monday + m.tuesday + m.wednesday + m.thursday + m.friday)::decimal weekday_pcent
FROM
  train_stop_dow b INNER JOIN calendar_train_maximum m ON b.stop_id = m.stop_id;

DROP TABLE IF EXISTS stop_30_mins_train_final;
CREATE TABLE stop_30_mins_train_final AS
SELECT DISTINCT
  s.agency_id,
  s.stop_id,
  s.stop_name,
  s.stop_lat, 
  s.stop_lon,
  s.route_color,
  s.geom
FROM
  stop_train s,
  train_stop_pcent p
WHERE 
  s.stop_id = p.stop_id AND
  p.weekday_pcent > 90;

---------------------------
-- BUS interval analysis --
---------------------------

-- after the stored procedures have been defined, this generates the dates for each stop_id where that stop_id provides a 30 minute frequency service
DROP TABLE IF EXISTS bus_30_min_stops_by_date;
CREATE TABLE bus_30_min_stops_by_date AS
SELECT DISTINCT
  date_numeric,
  bus_30_min_stops(to_date(to_char(date_numeric, '99999999'), 'YYYYMMDD')) stop_id
FROM
  calendar_extent
ORDER BY
  date_numeric;
  
-- view the results
SELECT
  date_numeric,
  count(stop_id)
FROM
  (SELECT DISTINCT * FROM bus_30_min_stops_by_date) t
GROUP BY
  date_numeric
ORDER BY
  date_numeric;

-- create the crosstab query
DROP TABLE IF EXISTS bus_stop_dow;
CREATE TABLE bus_stop_dow AS
SELECT * FROM crosstab (
  'SELECT DISTINCT
    stop_id,
    extract(dow from to_date(to_char(date_numeric, ''99999999''), ''YYYYMMDD''))::int AS dow,
	count(date_numeric) dow_count
  FROM
    (SELECT DISTINCT * FROM bus_30_min_stops_by_date) t
	-- exclude school holiday periods
  WHERE
    date_numeric BETWEEN 20181008 AND 20181205
  GROUP BY
    stop_id,
    dow
  ORDER BY
    stop_id,
    dow',
  'SELECT * FROM generate_series(0, 6) ORDER BY 1'
)
  AS (
    stop_id text,
	sunday int,
	monday int,
	tuesday int,
	wednesday int,
	thursday int,
	friday int,
	saturday int
)
;

-- this generates a table that summarises the maximum number of days a stop_id could be a 30 minute stop by dow
DROP TABLE IF EXISTS calendar_bus_maximum;
CREATE TABLE calendar_bus_maximum AS
SELECT * FROM crosstab (
  'SELECT DISTINCT
    stop_id,
    dow,
    count(date_numeric) dow_count
  FROM
    stops,
    calendar_extent
  WHERE
    date_numeric BETWEEN 20181008 AND 20181205
  GROUP BY
    stop_id,
    dow
  ORDER BY
    stop_id ASC,
    dow ASC',
  'SELECT * FROM generate_series(0, 6) ORDER BY 1'
)
  AS (
    stop_id text,
	sunday int,
	monday int,
	tuesday int,
	wednesday int,
	thursday int,
	friday int,
	saturday int
)
;

DROP TABLE IF EXISTS bus_stop_pcent;
CREATE TABLE bus_stop_pcent AS
SELECT
  b.stop_id,
  100*(coalesce(b.monday,0) + coalesce(b.tuesday,0) + coalesce(b.wednesday,0) + coalesce(b.thursday,0) + coalesce(b.friday,0))/(m.monday + m.tuesday + m.wednesday + m.thursday + m.friday)::decimal weekday_pcent
FROM
  bus_stop_dow b INNER JOIN calendar_bus_maximum m ON b.stop_id = m.stop_id;

DROP TABLE IF EXISTS stop_30_mins_bus_final;
CREATE TABLE stop_30_mins_bus_final AS
SELECT DISTINCT
  s.agency_id,
  s.stop_id,
  s.stop_name,
  s.stop_lat, 
  s.stop_lon,
  s.route_color,
  s.geom
FROM
  stop_bus s,
  bus_stop_pcent p
WHERE 
  s.stop_id = p.stop_id AND
  p.weekday_pcent > 90;
  
---------------------------
-- TRAM interval analysis --
---------------------------

-- after the stored procedures have been defined, this generates the dates for each stop_id where that stop_id provides a 30 minute frequency service
DROP TABLE IF EXISTS tram_30_min_stops_by_date;
CREATE TABLE tram_30_min_stops_by_date AS
SELECT DISTINCT
  date_numeric,
  tram_30_min_stops(to_date(to_char(date_numeric, '99999999'), 'YYYYMMDD')) stop_id
FROM
  calendar_extent
ORDER BY
  date_numeric;
  
-- view the results
SELECT
  date_numeric,
  count(stop_id)
FROM
  (SELECT DISTINCT * FROM tram_30_min_stops_by_date) t
GROUP BY
  date_numeric
ORDER BY
  date_numeric;

-- create the crosstab query
DROP TABLE IF EXISTS tram_stop_dow;
CREATE TABLE tram_stop_dow AS
SELECT * FROM crosstab (
  'SELECT DISTINCT
    stop_id,
    extract(dow from to_date(to_char(date_numeric, ''99999999''), ''YYYYMMDD''))::int AS dow,
	count(date_numeric) dow_count
  FROM
    (SELECT DISTINCT * FROM tram_30_min_stops_by_date) t
	-- exclude school holiday periods
  WHERE
    date_numeric BETWEEN 20181008 AND 20181205
  GROUP BY
    stop_id,
    dow
  ORDER BY
    stop_id,
    dow',
  'SELECT * FROM generate_series(0, 6) ORDER BY 1'
)
  AS (
    stop_id text,
	sunday int,
	monday int,
	tuesday int,
	wednesday int,
	thursday int,
	friday int,
	saturday int
)
;

-- this generates a table that summarises the maximum number of days a stop_id could be a 30 minute stop by dow
DROP TABLE IF EXISTS calendar_tram_maximum;
CREATE TABLE calendar_tram_maximum AS
SELECT * FROM crosstab (
  'SELECT DISTINCT
    stop_id,
    dow,
    count(date_numeric) dow_count
  FROM
    stops,
    calendar_extent
  WHERE
    date_numeric BETWEEN 20181008 AND 20181205
  GROUP BY
    stop_id,
    dow
  ORDER BY
    stop_id ASC,
    dow ASC',
  'SELECT * FROM generate_series(0, 6) ORDER BY 1'
)
  AS (
    stop_id text,
	sunday int,
	monday int,
	tuesday int,
	wednesday int,
	thursday int,
	friday int,
	saturday int
)
;

DROP TABLE IF EXISTS tram_stop_pcent;
CREATE TABLE tram_stop_pcent AS
SELECT
  b.stop_id,
  100*(coalesce(b.monday,0) + coalesce(b.tuesday,0) + coalesce(b.wednesday,0) + coalesce(b.thursday,0) + coalesce(b.friday,0))/(m.monday + m.tuesday + m.wednesday + m.thursday + m.friday)::decimal weekday_pcent
FROM
  tram_stop_dow b INNER JOIN calendar_tram_maximum m ON b.stop_id = m.stop_id;

DROP TABLE IF EXISTS stop_30_mins_tram_final;
CREATE TABLE stop_30_mins_tram_final AS
SELECT DISTINCT
  s.agency_id,
  s.stop_id,
  s.stop_name,
  s.stop_lat, 
  s.stop_lon,
  s.route_color,
  s.geom
FROM
  stop_tram s,
  tram_stop_pcent p
WHERE 
  s.stop_id = p.stop_id AND
  p.weekday_pcent > 90;
   
------------------------------
-- Combine 30 minutes stops -- 
------------------------------
DROP TABLE IF EXISTS stop_30_mins_final;
CREATE TABLE stop_30_mins_final AS
SELECT
  *
FROM (
  SELECT * FROM stop_30_mins_train_final
  UNION ALL
  SELECT * FROM stop_30_mins_bus_final
  UNION ALL
  SELECT * FROM stop_30_mins_tram_final
) s
;
'''