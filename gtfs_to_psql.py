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
conn = psycopg2.connect(dbname=args.db, user=args.U, password=args.w)
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT) # <-- ADD THIS LINE
curs = conn.cursor()

engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = args.U,
                                                                      pwd  = args.w,
                                                                      host = 'local_host',
                                                                      db   = args.db))
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
            # check if sql database exists
            sql =  '''SELECT 1 FROM pg_catalog.pg_database WHERE datname='{}';'''.format(name)
            curs.execute(sql)
            if len(curs.fetchall())!=0:
              print("\t- Database already exists!")
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
                        raise
              print("\t- Updated required tables with data.")
              sql = '''
                ALTER TABLE stop_times 
                    ALTER COLUMN shape_dist_traveled TYPE double precision USING NULLIF(shape_dist_traveled, '')::double precision'''
              curs.execute(sql)
              conn.commit()
              sql = '''CREATE EXTENSION postgis;SELECT postgis_full_version();'''
              curs.execute(sql)
              conn.commit()
              print("\t - Created postgis extension.")
              conn.close()
conn.close()
