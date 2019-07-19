# GTFS ANalysis Tool (gtfs_ant)
Carl Higgs and Jonathan Arundel, Healthy Liveable Cities group 2019

This tool generalises General Transit Feed Specification (GTFS) analysis developed by the Healthy Liveable Cities group in work led by Jonathan Arundel in 2017 through 2019 for Australian state transit agency GTFS feeds.

```
usage: gtfs_ant.py [-h] [-db DB] [-U U] [-w W] [-dir DIR] [-reprocess]

Import GTFS feeds from zip files, creating databases as per filename, set up functions for analysis, and analyse for frequent transport

optional arguments:
  -h, --help  show this help message and exit
  -db DB      Admin database
  -U U        SQL user
  -w W        SQL password
  -dir DIR    Folder containing zipped GTFS data
  -reprocess  Re-analyse GTFS feed if database already exists (default is False)
```

It is recommended that you ensure your GTFS file names are
* lower case
* no spaces
* begin with a character not number
* descriptive and clear as to what they representative

For example: "gtfs_vic_ptv_20180413" follows a schema, 'gtfs_state_agency_yyyymmdd' 
This will allow for easy storage and retrieval of multiple gtfs feed databases.

In order to set up analysis functions and run analyses, parameters for your GTFS feeds must be stored in the specified folder in a file called '_setup_modes.py'.  If this file is opened in a text editor it appears like the below:

________________________________________________________________________________________________________________________________________
```python
# define modes for GTFS feed(s) as per agency_id codes in agency.txt below
# Note that the Victorian PTV agency codes metro and regional trains as type 2 (intercity and long distance), not 1 (metro / subway)
modes = {
         'tram' :{'route_types':[0], 'agency_ids':[3]  , 'start_times':['07:00:00'],'end_times':['19:00:00'],'intervals':['00:30:00']},
         'train':{'route_types':[1,2], 'agency_ids':[1,2], 'start_times':['07:00:00'],'end_times':['19:00:00'],'intervals':['00:30:00']},
         'bus'  :{'route_types':[3], 'agency_ids':[4,6], 'start_times':['07:00:00'],'end_times':['19:00:00'],'intervals':['00:30:00']}
         }

# define month and day for "representative period" ie. not in school time; here example is July 15 to August 15
# start_date_mmdd = 0715
# end_date_mmdd = 0815
start_date_mmdd = '0715'
end_date_mmdd = '0815'
```
_______________________________________________________________________________________________________________________________________

The above parameters are all comma seperated lists, allowing for flexible definitions:

Define 'route_types' as per https://developers.google.com/transit/gtfs/reference/#routestxt

Define agency_ids as per the coding used by your transit agency in agency.txt

Define start and end times as per your times of interest (e.g. 7 am to 7 pm as per example above)

Define intervals as per example (for 30 minute interval); for example to also analyse at 15 minute intervals, change this to ``['00:15:00','00:30:00']``
