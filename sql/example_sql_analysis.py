'''
A small bit of analysis to explore the F1 2022 season using SQL queries.

Here I use queries on the SQL database generated using the 
make_tables.py script.  The database used here was generated using the
following line:

> python make_tables.py -u postgres -p password -y "[2022]" -cdb True -db formula1_2022
'''
from matplotlib import pyplot as plt
import numpy as np
import os

import psycopg2 as pg2
import pandas as pd
import pandas.io.sql as psql
from tabulate import tabulate

import fastf1
import fastf1.plotting
fastf1.Cache.enable_cache(os.environ['f1-cache'])  
fastf1.plotting.setup_mpl()

import warnings
warnings.filterwarnings("ignore")
warnings.simplefilter(action='ignore', category=FutureWarning)

plt.ion()

if __name__ == '__main__':
    plt.close('all')
    sql_user = 'postgres'
    sql_password = 'password'
    db_name = 'formula1_2022'

    connection = pg2.connect(database=db_name, host='localhost', port='5432', user=sql_user,password=sql_password) # Connection for SQL calls
    
    print("Let's explore the 2022 Formula 1 Season!\n")

    '''
    SELECT driver_code AS driver, SUM(race_points) AS championship_points FROM results 
        GROUP BY driver_code 
        ORDER BY SUM(race_points) DESC;
    '''
    championship_standings_df = psql.read_sql('SELECT driver_code AS driver, SUM(race_points) AS championship_points FROM results GROUP BY driver_code ORDER BY SUM(race_points) DESC;', connection)
    
    drivers_df = psql.read_sql('SELECT * FROM drivers;', connection)
    
    champ_df = drivers_df.query('driver_code == "{}"'.format(championship_standings_df['driver'][0]))
    
    print('The 2022 world champion was {} {} with a total of {} points.'.format(champ_df['first_name'].squeeze(),champ_df['last_name'].squeeze(), championship_standings_df['championship_points'].squeeze()[0]))
    print('The full standings are as follows:')
    print(tabulate(championship_standings_df, headers='keys', tablefmt='psql'))
    print('\n')


    # Determine champions best weekend, get information.
    '''
    SELECT results.event_id, results.driver_code, SUM(results.race_points) AS weekend_points, events.* FROM results
        INNER JOIN events ON results.event_id = events.event_id
        WHERE results.driver_code LIKE %(driver_code)s
        GROUP BY results.event_id, events.event_id, results.driver_code
        ORDER BY SUM(results.race_points) DESC
        LIMIT 1;
    '''
    best_weekend_simple = psql.read_sql('SELECT results.event_id, results.driver_code, SUM(results.race_points) AS weekend_points, events.* FROM results INNER JOIN events ON results.event_id = events.event_id WHERE results.driver_code LIKE %(driver_code)s GROUP BY results.event_id, events.event_id, results.driver_code ORDER BY SUM(results.race_points) DESC LIMIT 1;', connection, params={'driver_code':championship_standings_df['driver'][0]})
    print('{}\'s best weekend was at {}, where he obtained {} points.\nThis was a {} weekend and the GP was held on {} as the "{}"'.format(champ_df['first_name'].squeeze(), best_weekend_simple['location'][0], best_weekend_simple['weekend_points'][0], best_weekend_simple['event_format'][0], best_weekend_simple['race_date'][0], best_weekend_simple['event_name'][0]))

    #winners_df = psql.read_sql("SELECT * from drivers WHERE driver_id IN (SELECT DISTINCT driver_id FROM results WHERE race_finish_position = 1);", connection)
    #Load in drivers table of just race winners, listing their results.

    
    # Below are a list of questions that COULD be answered with SQL queries that I need to understand, interpret, and make happen.  I do not currently remember how to do these things with SQL.

    # Return table of race winners with new column containing a list of each of their result finishes.  

    # Return table of all drivers showing their total season points, sorted by this as well. 

    # Return the race where each TEAM earned the most cumalitive points accross drivers.

    # Return information about unfinished race results. 

    connection.close()