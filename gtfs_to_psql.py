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
from urllib import urlopen

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
for root, dirs, files in os.walk(args.dir):
    for file in files:
        if file.endswith(".zip"):
            path = '{}/{}'.format(root,file)
            print('\n{}'.format(path))
            name = os.path.splitext(file)[0]            
            with ZipFile(path) as myzip:
                file_list = ZipFile.namelist(myzip)
                print("Zip contents: {}".format(file_list))
                test_contents = [x for x in ['agency.txt', 'calendar.txt', 'calendar_dates.txt', 'routes.txt', 'shapes.txt', 'stop_times.txt', 'stops.txt', 'trips.txt'] if x in file_list]
                if (len(test_contents)!=len(file_list)):
                    wrong_contents = [x for x in ['agency.txt', 'calendar.txt', 'calendar_dates.txt', 'routes.txt', 'shapes.txt', 'stop_times.txt', 'stops.txt', 'trips.txt'] if x not in file_list]
                    print("The zip file appears to be missing the following required files: {}".format(wrong_content))
                # # SQL queries
                # sql =  = '''
                  # CREATE DATABASE IF NOT EXISTS {}
                 # '''.format(name)  
                # curs.execute(sql)
                # conn.commit()
# with myzip.open('eggs.txt') as myfile:
    # print(myfile.read())
        # zipfile = ZipFile(StringIO(path))
        # for name in myzipfile.namelist():
          # print(name)
     
conn.close()

# -- 1. aquire GTFS feeds 
# --- for the case of PTV (Victoria's public transport agency), these are said to be located at data.vic.gov.au
# --- in practice, a more convenient and uptodate source is https://transitfeeds.com/
# --- This not only has the current feeds (presumably pushed by PTV to this repository) but also a comprehensive
# --- archive of previous feeds.
# --- This particular script was run using the 13 April 2018 feed at https://transitfeeds.com/p/ptv/497/20180413

# -- 2. summarise GTFS calendar data to get sense of time coverage
# ---- the aim here is to identify a representative week we might use for analysis.
# ---- A python script was prepared for this purpose: GTFS_calendarSummary.py, stored in the same directory as 
# ---- this sql script.
# ---- Locate this script in the parent folder within which other GTFS folders are located; 
# ---- the script will then iterate over all descendent folders and summarise any calendar.txt files found.
# ---- The output for the present case is as follows:
# ----
# ------ feed        D:\regional\data\vic_trains_coaches\gtfs_vic_ptv_20180413\calendar.txt
# ------ upload_date                                           20180413
# ------ entries                                                    278
# ------ min_start                                             20180412
# ------ max_start                                             20180528
# ------ min_end                                               20180412
# ------ max_end                                               20180610
# ------ non_overlap                                                  1
# ------ dailyFreq                                                 TRUE
# ------ Summary output to D:\regional\data\vic_trains_coaches\gtfs_calendar_summary_20180419.csv.
# ----
# ---- So we see that the calendar 
# ------ has coverage from 12 April to 10 June 2018
# ------ however some routes are only relevant on 12 April (ie. max_end), while others may not commence until 28 May.

# -- 3. Create the postgresql database for this particular transit feed
# -- psql -U postgres -c   "DROP DATABASE gtfs_vic_ptv_20180413;"
# -- psql -U postgres -c   "CREATE DATABASE gtfs_vic_ptv_20180413;"
# -- Run the following from within psql
# ---- this was sourced from https://github.com/chroman/df-gtfs
# ---- some points, e.g. the creation of a specific wgs84 numeric type may be redundant; but the data
# ---- checking rule has value (e.g. sometimes coords are swapped, or outright wrong).  This guards
# ---- against these instances.
# ---- If applying this script to another feed, the table field names may need to be updated to reflect
# ---- the headers and datatypes of the supplied data (not always consistent across feeds).
# DROP DOMAIN IF EXISTS wgs84_lat CASCADE;
# CREATE DOMAIN wgs84_lat AS DOUBLE PRECISION CHECK(VALUE >= -90 AND VALUE <= 90);

# DROP DOMAIN IF EXISTS wgs84_lon CASCADE;
# CREATE DOMAIN wgs84_lon AS DOUBLE PRECISION CHECK(VALUE >= -180 AND VALUE <= 180);

# DROP DOMAIN IF EXISTS gtfstime CASCADE;
# CREATE DOMAIN gtfstime AS text CHECK(VALUE ~ '^[0-9]?[0-9]:[0-5][0-9]:[0-5][0-9]$');

# CREATE TABLE agency
# (
  # agency_id         text UNIQUE NULL,
  # agency_name       text NOT NULL,
  # agency_url        text NOT NULL,
  # agency_timezone   text NOT NULL,
  # agency_lang       text NULL
# );

# CREATE TABLE calendar
# (
  # service_id        text PRIMARY KEY,
  # monday            boolean NOT NULL,
  # tuesday           boolean NOT NULL,
  # wednesday         boolean NOT NULL,
  # thursday          boolean NOT NULL,
  # friday            boolean NOT NULL,
  # saturday          boolean NOT NULL,
  # sunday            boolean NOT NULL,
  # start_date        numeric(8) NOT NULL,
  # end_date          numeric(8) NOT NULL
# );

# CREATE TABLE calendar_dates
# (
  # service_id text NOT NULL,
  # date numeric(8) NOT NULL,
  # exception_type integer NOT NULL
# );

# CREATE TABLE routes
# (
  # route_id          text PRIMARY KEY,
  # agency_id         text NULL,
  # route_short_name  text NULL,
  # route_long_name   text NOT NULL,
  # route_type        integer NULL,
  # route_color       text NULL,
  # route_text_color  text NULL
# );

# CREATE TABLE shapes
# (
  # shape_id          text,
  # shape_pt_lat      wgs84_lat NOT NULL,
  # shape_pt_lon      wgs84_lon NOT NULL,
  # shape_pt_sequence integer NOT NULL,
  # shape_dist_traveled double precision NULL
# );

# -- CREATE TABLE stop_times
# -- (
  # -- trip_id           text NOT NULL,
  # -- arrival_time      interval NOT NULL,
  # -- departure_time    interval NOT NULL,
  # -- stop_id           text NOT NULL,
  # -- stop_sequence     integer NOT NULL,
  # -- stop_headsign     text NULL,
  # -- pickup_type       integer NULL CHECK(pickup_type >= 0 and pickup_type <=3),
  # -- drop_off_type     integer NULL CHECK(drop_off_type >= 0 and drop_off_type <=3),
  # -- shape_dist_traveled double precision NULL
# -- );
# -- Using a work around due to empty value in shape_dist_traveled field; can't be interpreted as numeric directly
# CREATE TABLE stop_times_empty_shape_dist
# (
  # trip_id           text NOT NULL,
  # arrival_time      interval NOT NULL,
  # departure_time    interval NOT NULL,
  # stop_id           text NOT NULL,
  # stop_sequence     integer NOT NULL,
  # stop_headsign     text NULL,
  # pickup_type       integer NULL CHECK(pickup_type >= 0 and pickup_type <=3),
  # drop_off_type     integer NULL CHECK(drop_off_type >= 0 and drop_off_type <=3),
  # shape_dist_traveled text NULL
# );

# CREATE TABLE stops
# (
  # stop_id           text PRIMARY KEY,
  # stop_name         text NOT NULL,
  # stop_lat          wgs84_lat NOT NULL,
  # stop_lon          wgs84_lon NOT NULL
# );

# CREATE TABLE trips
# (
  # route_id          text NOT NULL,
  # service_id        text NOT NULL,
  # trip_id           text NOT NULL PRIMARY KEY,
  # shape_id          text NULL,
  # trip_headsign     text NULL,
  # direction_id      boolean NULL
# );

# -- Copy data into created tables
# \copy agency from 'D:/regional/data/vic_trains_coaches/gtfs_vic_ptv_20180413/agency.txt' with csv header
# \copy calendar from 'D:/regional/data/vic_trains_coaches/gtfs_vic_ptv_20180413/calendar.txt' with csv header
# \copy calendar_dates from 'D:/regional/data/vic_trains_coaches/gtfs_vic_ptv_20180413/calendar_dates.txt' with csv header
# \copy routes from 'D:/regional/data/vic_trains_coaches/gtfs_vic_ptv_20180413/routes.txt' with csv header
# \copy shapes from 'D:/regional/data/vic_trains_coaches/gtfs_vic_ptv_20180413/shapes.txt' with csv header
# \copy stop_times_empty_shape_dist from 'D:/regional/data/vic_trains_coaches/gtfs_vic_ptv_20180413/stop_times.txt' with csv header
# \copy stops from 'D:/regional/data/vic_trains_coaches/gtfs_vic_ptv_20180413/stops.txt' with csv header
# \copy trips from 'D:/regional/data/vic_trains_coaches/gtfs_vic_ptv_20180413/trips.txt' with csv header

# CREATE TABLE stop_times AS
 # SELECT trip_id            ,
        # arrival_time       ,
        # departure_time     ,
        # stop_id            ,
        # stop_sequence      ,
        # stop_headsign      ,
        # pickup_type        ,
        # drop_off_type      ,
        # NULLIF(shape_dist_traveled, '')::double precision::int
  # FROM stop_times_empty_shape_dist;
# -- drop the temporary work around table 
 # DROP TABLE stop_times_empty_shape_dist;  
  
# -- enable postgis extension
# CREATE EXTENSION postgis;SELECT postgis_full_version();  