## SQL cript to import GTFS data (for Victoria) to Postgresql
## Carl Higgs 20190713

import psycopg2         ,\
       time             ,\
       getpass          ,\
       os               ,\
       sys              ,\
       datetime         ,\
       argparse         ,\
       itertools        ,\
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
  
description = 'Import GTFS feeds from zip files, creating databases as per filename'
# Parse input arguments
parser = argparse.ArgumentParser(description=description)
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

sys.path.append(args.dir)
from _setup_modes import *

print(description)
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

with open('_setup_gtfs_tables.sql', 'r') as myfile:
  required_tables_sql = myfile.read()

with open('_parameterised_mode_freq_query.py.sql', 'r') as myfile:
  freq_analysis_function = myfile.read()

with open('_analyse_frequent_stops.py.sql', 'r') as myfile:
  freq_analysis = myfile.read()
  
# define variables based on above parameters, including permutations of start, end and time buffers (e.g. 

time_format  ='%H:%M:%S'
short_time   = '%H%M'



                
print("Import GTFS feeds... ")
for root, dirs, files in os.walk(args.dir):
  for file in files:
    if file.endswith(".zip"):
      path = '{}/{}'.format(root,file)
      print('\n  - {}'.format(path))
      name = os.path.splitext(file)[0]    
      print("\t - Define analysis functions... ")
      # deriving year, based on assumption suffix is _yyyymmdd
      year = name.split('_')[-1][0:4]
      analysis_start_date = '{}{}'.format(year,start_date_mmdd)
      analysis_end_date   = '{}{}'.format(year,end_date_mmdd)
      create_gtfs_analysis_functions = ''
      gtfs_analysis = ''
      print("\t\t- Analysis period: {} to {}".format(analysis_start_date,analysis_end_date))
      for mode in modes:
          print("\t\t- {}".format(mode))
          durations = list(itertools.product(modes[mode]['start_times'], modes[mode]['end_times']))
          for duration in durations:
              start_time = duration[0]
              end_time   = duration[1]
              for interval in modes[mode]['intervals']:
                  interval  = datetime.datetime.strptime(interval,time_format)
                  interval_time  = datetime.timedelta(hours=interval.hour, minutes=interval.minute, seconds=interval.second)
                  buffer_start = (datetime.datetime.strptime(start_time,time_format) + interval_time)
                  buffer_end   = (datetime.datetime.strptime(end_time,  time_format) - interval_time)
                  # use the above as buffer_start.strftime(time_format) or buffer_start.strftime(short_time) , for example
                  print("\t\t  - Analysis time: {} to {}".format(buffer_start.strftime(short_time) ,
                                                                                   buffer_end.strftime(short_time)))
                  print("\t\t  - Analysis inverval: {} (HHMM) freq".format(interval.strftime(short_time)))
                  create_gtfs_analysis_functions = '{}\n{}'.format(create_gtfs_analysis_functions,
                                                                  freq_analysis_function.format(
                     mode               = mode                                     ,
                     start_time         = start_time                               ,
                     end_time           = end_time                                 ,
                     buffer_start       = buffer_start.strftime(time_format)       ,
                     buffer_end         = buffer_end.strftime(time_format)         ,
                     buffer_start_short = buffer_start.strftime(short_time)        ,
                     buffer_end_short   = buffer_end.strftime(short_time)          ,
                     interval           = interval.strftime(time_format)            ,
                     interval_short     = interval.strftime(short_time)            ,
                     route_types        = ",".join(['{}'.format(x) for x in modes[mode]['route_types']]),
                     agency_ids         = "','".join(['{}'.format(x) for x in modes[mode]['agency_ids']])
                  ))
                  gtfs_analysis = '{}\n{}'.format(gtfs_analysis,
                                                  freq_analysis.format(
                     mode               = mode                                     ,
                     start_date         = analysis_start_date                      ,
                     end_date           = analysis_end_date                        ,
                     start_time         = start_time                               ,
                     end_time           = end_time                                 ,
                     buffer_start       = buffer_start.strftime(time_format)       ,
                     buffer_end         = buffer_end.strftime(time_format)         ,
                     buffer_start_short = buffer_start.strftime(short_time)        ,
                     buffer_end_short   = buffer_end.strftime(short_time)          ,
                     interval           = interval.strftime(time_format)            ,
                     interval_short     = interval.strftime(short_time)            ,
                     route_types        = ",".join(['{}'.format(x) for x in modes[mode]['route_types']]),
                     agency_ids         = "','".join(['{}'.format(x) for x in modes[mode]['agency_ids']])
                  ))
                  print("\t\t  - SQL function: {mode}_{interval_short}_stops(date)".format( mode = mode,
                      interval_short = interval.strftime(short_time)))
      with ZipFile(path) as myzip:
        file_list = ZipFile.namelist(myzip)
        # print("Zip contents: {}".format(file_list))
        print("\t- Checking required files are present... "),
        test_contents = [x for x in required_files if x not in file_list]
        if (len(test_contents)!=0):
          print("No.  The zip file appears to be missing the following required files: {}".format(test_contents))
          continue
        else:
            print("Done.")
            conn = psycopg2.connect(dbname=args.db, user=args.U, password=args.w)
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT) # <-- ADD THIS LINE
            curs = conn.cursor()

            print("\t- Check if sql database exists... "),
            sql =  '''SELECT 1 FROM pg_catalog.pg_database WHERE datname='{}';'''.format(name)
            curs.execute(sql)
            if len(curs.fetchall())!=0:
              print("Database already exists; skipping (assumed processed).")
              print("\t- Creating frequent transport analysis functions... "),
              # curs.execute(create_gtfs_analysis_functions)
              print(create_gtfs_analysis_functions)
              # conn.commit()
              print("Done.")
              # print("\t- Performing GTFS analysis... "),
              # curs.execute(gtfs_analysis)
              # conn.commit()
              print("Done.")
              conn.close()
              continue
            else:
              # SQL queries
              sql =  '''
                CREATE DATABASE {};
               '''.format(name.lower())  
              print("Created database: {}".format(name.lower()))
              curs.execute(sql)
              conn.commit()
              conn.close()
              # connect to new database
              conn = psycopg2.connect(dbname=name, user=args.U, password=args.w)
              curs = conn.cursor()
              print("\t- Connected to {}".format(name.lower()))
              print("\t- Creating required tables... "),
              curs.execute(required_tables_sql)
              conn.commit()
              print("Done.")
              print("\t- Updating tables with data... ")
              conn.close()
              for table in ['agency','calendar','calendar_dates','routes','shapes','stop_times','stops','trips']:
                print("\t\t{}... ".format(table)),
                with myzip.open('{}.txt'.format(table)) as myfile:
                  try:
                    conn = psycopg2.connect(dbname=name, user=args.U, password=args.w)
                    curs = conn.cursor()
                    sql = '''COPY {table} FROM STDIN WITH CSV HEADER;'''.format(table = table)
                    curs.copy_expert(sql, myfile)
                    conn.commit()
                    curs.close()
                    conn.close()
                  except:
                    print("\n\t\t- Issue found... ".format(table))
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
                      print("\t\t- Re-attempting to import data for routes table with query {}... ".format(sql)),
                      with myzip.open('{}.txt'.format(table)) as myfile:
                        curs.copy_expert(sql, myfile)
                        conn.commit()
                        curs.close()
                        conn.close()
                    else:
                        print("  failed.")
                        conn.close()
                        raise
                print("Done.")
              conn = psycopg2.connect(dbname=name, user=args.U, password=args.w)
              curs = conn.cursor()
              print("\t- Amending datatype of 'shape_dist_traveled' variable in stop_times...")
              sql = '''
                ALTER TABLE stop_times 
                    ALTER COLUMN shape_dist_traveled TYPE double precision USING NULLIF(shape_dist_traveled, '')::double precision'''
              curs.execute(sql)
              conn.commit()
              print("\t- Creating required / recommended extensions... ")
              sql = '''
              CREATE EXTENSION postgis;
              CREATE EXTENSION tablefunc;
              '''
              curs.execute(sql)
              conn.commit()
              print("\t- Creating frequent transport analysis functions... "),
              curs.execute(create_gtfs_analysis_functions)
              conn.commit()
              print("Done.")
              print("\t- Performing GTFS analysis... "),
              curs.execute(gtfs_analysis)
              conn.commit()
              print("Done.")
              conn.close()

create_combined_analysis_results = '''
------------------------------
-- Combine 30 minutes stops -- 
------------------------------
DROP TABLE IF EXISTS stop_{interval_short}_final;
CREATE TABLE stop_{interval_short}_final AS
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