## SQL cript to import GTFS data (for Victoria) to Postgresql
## Carl Higgs 20190713
## Building on work of Jonathan Arundel (2017-2018)

import psycopg2         ,\
       time             ,\
       getpass          ,\
       os               ,\
       sys              ,\
       datetime         ,\
       argparse         ,\
       itertools        ,\
       pandas as pd     ,\
       csv              ,\
       subprocess as sp
from sqlalchemy import create_engine
from StringIO import StringIO
from zipfile import ZipFile
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import create_engine

cwd = os.path.dirname(sys.argv[0])
print(cwd)

def valid_path(arg):
    if not os.path.exists(arg):
        msg = "The path %s does not exist!" % arg
        raise argparse.ArgumentTypeError(msg)
    else:
        return arg
  
description = 'Import GTFS feeds from zip files, creating databases as per filename, set up functions for analysis, and analyse for frequent transport'
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
                    help='Folder containing zipped GTFS data',
                    default=cwd,
                    type=valid_path)
parser.add_argument('-reprocess',
                    help='Re-analyse GTFS feed if database already exists (default is False)',
                    default=False, 
                    action='store_true')
parser.add_argument('-debug',
                    help='Print additional data useful for debugging purposes',
                    default=False, 
                    action='store_true')
args = parser.parse_args()

sys.path.append(args.dir)
from _setup_modes import *

print(description)
print('''
GTFS ANalysis Tool (gtfs_ant)
Carl Higgs 2019

This tool generalises General Transit Feed Specification (GTFS) analysis developed by the Healthy Liveable Cities group in work lead by Jonathan Arundel in 2017 through 2019 for Australian state transit agency GTFS feeds.

usage: gtfs_ant.py [-h] [-db DB] [-U U] [-w W] [-dir DIR] [-reprocess]

Import GTFS feeds from zip files, creating databases as per filename, set up
functions for analysis, and analyse for frequent transport

optional arguments:
  -h, --help  show this help message and exit
  -db DB      Admin database
  -U U        SQL user
  -w W        SQL password
  -dir DIR    Folder containing zipped GTFS data
  -reprocess  Re-analyse GTFS feed if database already exists (default is
              False)

It is recommended that you ensure your GTFS file names are
  - lower case
  - no spaces
  - begin with a character not number
  - descriptive and clear as to what they representative

For example: "gtfs_vic_ptv_20180413" follows a schema, 'gtfs_state_agency_yyyymmdd' 
This will allow for easy storage and retrieval of multiple gtfs feed databases.

In order to set up analysis functions and run analyses, parameters for your GTFS feeds must be stored in the specified folder in a file called '_setup_modes.py'.  If this file is opened in a text editor it appears like the below:

________________________________________________________________________________________________________________________________________

# define modes for GTFS feed(s) as per agency_id codes in agency.txt below
modes = {
         'tram' :{'route_types':[0], 'custom_mode':"routes.agency_id  IN ('3')" , 'start_times':['07:00:00'],'end_times':['19:00:00'],'intervals':['00:30:00']},
         'train':{'route_types':[1,2], 'custom_mode':"routes.agency_id  IN ('1','2')" , 'start_times':['07:00:00'],'end_times':['19:00:00'],'intervals':['00:30:00']},
         'bus'  :{'route_types':[3], 'custom_mode':"routes.agency_id  IN ('4','6')" , 'start_times':['07:00:00'],'end_times':['19:00:00'],'intervals':['00:30:00']}
         }
________________________________________________________________________________________________________________________________________

The above parameters are all comma seperated lists, allowing for flexible definitions:

Define 'route_types' as per https://developers.google.com/transit/gtfs/reference/#routestxt

Define agency_ids as per the coding used by your transit agency in agency.txt

Define start and end times as per your times of interest (e.g. 7 am to 7 pm as per example above)

Define intervals as per example (for 30 minute interval); for example to also analyse at 15 minute intervals, change this to ['00:15:00','00:30:00']

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
                  if modes[mode]['custom_mode'] != '':
                    modes[mode]['custom_mode'] = 'AND {}'.format(modes[mode]['custom_mode']).replace('AND AND','AND')
                    
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
                     custom_mode        = modes[mode]['custom_mode']
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
                     custom_mode        = modes[mode]['custom_mode']
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
              conn.close()
              print("Database already exists; skipping (assumed processed).")
              if args.reprocess:
                  print("\t- Creating frequent transport analysis functions... "),
                  # connect to new database
                  conn = psycopg2.connect(dbname=name, user=args.U, password=args.w)
                  curs = conn.cursor()
                  # print(create_gtfs_analysis_functions)
                  curs.execute(create_gtfs_analysis_functions)
                  conn.commit()
                  print("Done.")
                  print("\t- Performing GTFS analysis"),
                  for query in gtfs_analysis.split(';'):
                    if len(query.replace(' ','')) > 2 :
                        curs.execute(query)
                        conn.commit()
                        print("."),
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
                conn = psycopg2.connect(dbname=name, user=args.U, password=args.w)
                curs = conn.cursor()
                # check expected columns in table, as exists in database
                sql = '''SELECT column_name FROM information_schema.columns WHERE table_name   = '{}';'''.format(table)
                curs.execute(sql)
                columns_expected = [x[0] for x in curs.fetchall()]
                with myzip.open('{}.txt'.format(table)) as myfile:
                  # retrieve column headers from file
                  csv_data = csv.reader(myfile)
                  columns_observed = [x.decode("utf-8-sig").encode('utf-8') for x in csv_data.next()]
                  columns_available = [x for x in columns_observed if x in columns_expected]
                  if args.debug:
                      print('\nExpected: {}'.format(columns_expected)),
                      print('\nObserved: {}'.format(columns_observed)),
                      print('\nAvailable: {}'.format(columns_available))
                  try:
                    sql = '''
                    COPY {table} ({columns}) FROM STDIN WITH CSV HEADER DELIMITER ',' QUOTE '"';
                    '''.format(table = table,columns = ','.join(columns_available))
                    curs.copy_expert(sql, myfile)
                    conn.commit()
                    curs.close()
                    conn.close()
                  except:
                    print("\n\t\t- Import failed.")
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
              # print(create_gtfs_analysis_functions)
              curs.execute(create_gtfs_analysis_functions)
              conn.commit()
              print("Done.")
              print("\t- Performing GTFS analysis"),
              for query in gtfs_analysis.split(';'):
                if len(query.replace(' ','')) > 2 :
                    curs.execute(query)
                    conn.commit()
                    print("."),
              print(" Done.")
              conn.close()
              
print("\n Create or update final combined analyses... ")


# parameterised construction of by mode-interval sql queries
for interval in modes[mode]['intervals']:
    interval  = interval.replace(':','')[:4]
    print("\nFrequency: {}".format(interval))
    union_tables_by_mode = []
    summarise_tables_by_mode = []
    from_tables_by_mode = []
    for mode in modes:
        text = '''SELECT * FROM {mode}_{interval}_stop_final'''.format(mode = mode,interval = interval)
        union_tables_by_mode.append(text)
        text = '''
        {mode}_freq, 
        {mode},
        round(100*({mode}_freq /NULLIF({mode}::float,0))::numeric,2) AS {mode}_freq_pct'''.format(mode = mode)
        summarise_tables_by_mode.append(text)
        text = '''
        (SELECT COUNT(*) FROM {mode}_{interval}_stop_final) AS {mode}_freq,
        (SELECT COUNT(*) FROM stop_{mode}) AS {mode}'''.format(mode = mode,interval = interval)
        from_tables_by_mode.append(text)
        
    union_tables_by_mode = '\nUNION ALL\n'.join(union_tables_by_mode)
    summarise_tables_by_mode = ','.join(summarise_tables_by_mode)
    from_tables_by_mode = ','.join(from_tables_by_mode)

    for root, dirs, files in os.walk(args.dir):
      for file in files:
        if file.endswith(".zip"):
          name = os.path.splitext(file)[0]    
          print(" - {}".format(name))
          conn = psycopg2.connect(dbname=name, user=args.U, password=args.w)
          curs = conn.cursor()
          engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = args.U,
                                                                                pwd  = args.w,
                                                                                host = 'localhost',
                                                                                db   = name))
          create_combined_analysis_results = '''
            ------------------------------
            -- Combine {interval} minute stops -- 
            ------------------------------
            DROP TABLE IF EXISTS stop_{interval}_final;
            CREATE TABLE stop_{interval}_final AS
            SELECT
              *
            FROM (
              {union_tables_by_mode}
            ) s
            ;
            DROP TABLE IF EXISTS mode_{interval}_freq_comparison;
            CREATE TABLE mode_{interval}_freq_comparison AS
            SELECT {summarise_tables_by_mode}
            FROM (SELECT 
                  {from_tables_by_mode}
                  ) t;
            '''.format(interval = interval,
                       union_tables_by_mode = union_tables_by_mode,
                       summarise_tables_by_mode = summarise_tables_by_mode,
                       from_tables_by_mode = from_tables_by_mode)
          curs.execute(create_combined_analysis_results)
          conn.commit()
          df = pd.read_sql_table('mode_{}_freq_comparison'.format(interval),con=engine)
          print(df.to_string(index=False))
          conn.close()

