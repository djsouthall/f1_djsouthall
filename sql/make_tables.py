'''
These functions are designed to generate SQL databases and tables regarding.


Initial set of sample code for working with SQL databases using python.
Specifically this uses PostgreSQL using Psycopg2 (  https://www.psycopg.org/docs/ ).
'''
import os
import sys
import pdb
import numpy as np
import pandas as pd
import warnings
warnings.filterwarnings("ignore")

import psycopg2 as pg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

import fastf1
import fastf1.plotting
fastf1.Cache.enable_cache(os.environ['f1-cache'])  
fastf1.plotting.setup_mpl()


sql_user = 'postgres'
sql_password = 'password'

known_country_alias = {"USA":"United States"}

def createDatabase(name, owner=sql_user, template='template1'):
    '''
    Given a name, this will generate a new database if one of that name does
    not already exist.  Returns 1 if a new database has been succesfully
    generated and 0 if that database already existed or if there was an error.

    Parameters
    ----------
    name : str
        The name of the database to be generated.

    Returns
    ----------
        0 if no new database generated.
        1 if new database generated succesfully.
    '''
    if name != name.lower():
        print('Renaming %s to %s'%(name, name.lower()))
        name = name.lower()
    try:
        connection = pg2.connect(user='postgres',password='password') # Create a connection with PostgreSQL
        print("Database connected")
    except Exception as e:
        print("Database failed to connect")
    if connection is not None:
        connection.autocommit = True
        cursor = connection.cursor()
        cursor.execute("SELECT datname FROM pg_database;")
        list_database = cursor.fetchall()
        if (name,) in list_database:
            print("'{}' Database already exists, not generated.".format(name))
            connection.close()
            return 0
        else:
            print("'{}' Database does not exist.".format(name))
            sqlCreateDatabase = "CREATE DATABASE {} WITH TEMPLATE {} OWNER {};".format(name, template, owner)
            cursor.execute(sqlCreateDatabase)
            print("'{}' Database Generated.".format(name))
            connection.close()
            return 1

def initiateTable(db_name, table_name, creation_command_str):
    try:
        connection = pg2.connect(database = db_name, user = sql_user, password = sql_password)
    except:
        print("Unable to connect to the database") 

    cursor = connection.cursor()
    try:
        cursor.execute(creation_command_str)
        failed = False
    except Exception as e:
        print("Error adding table:")
        print(e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)

        print("'%s' table NOT generated."%table_name)
        failed = True

    connection.commit() # Makes sure the change is shown in the database
    connection.close()
    cursor.close()
    if failed == False:
        print("'%s' table generated."%table_name)

def insertIntoTable(db_name, table_name, insert_command_str):
    try:
        connection = pg2.connect(database = db_name, user = sql_user, password = sql_password)
    except:
        print("Unable to connect to the database") 

    cursor = connection.cursor()
    try:
        cursor.execute(insert_command_str)
        failed = False
    except Exception as e:
        print("\nError inserting into table:")
        print(e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)

        print("'%s' row NOT inserted.\n"%table_name)
        failed = True

    connection.commit() # Makes sure the change is shown in the database
    connection.close()
    cursor.close()

def initiateDriversTable(db_name):
    '''
    This will generate a table called drivers.
    '''
    create_table_str = '''
        CREATE TABLE drivers (
        driver_id SERIAL PRIMARY KEY NOT NULL,
        driver_code VARCHAR(4),
        broadcast_name VARCHAR(80),
        first_name VARCHAR(80),
        last_name VARCHAR(80),
        driver_number INT,
        most_recent_team_id INT REFERENCES teams(team_id) NOT NULL,
        team_color VARCHAR(6),
        headshot_url VARCHAR(1000),
        country_code VARCHAR(10),
        driver_reference VARCHAR(30) UNIQUE,
        UNIQUE(driver_code, first_name, last_name)
        );
        '''
    initiateTable(db_name, 'drivers', create_table_str)

def initiateTeamsTable(db_name):
    '''
    This will generate a table called teams.
    '''
    create_table_str = '''
        CREATE TABLE teams (
        team_id SERIAL PRIMARY KEY NOT NULL,
        team_name VARCHAR(500) UNIQUE,
        team_color VARCHAR(6)
        );
        '''
    initiateTable(db_name, 'teams', create_table_str)


def initiateCountriesTable(db_name):
    '''
    This will generate a table called countries.
    '''
    create_table_str = '''
        CREATE TABLE countries (
        country_id SERIAL PRIMARY KEY NOT NULL,
        country VARCHAR(80) UNIQUE NOT NULL
        );
        '''
    initiateTable(db_name, 'countries', create_table_str)

def initiateTracksTable(db_name):
    '''
    This will generate a table called tracks with the columns:

    track_id
    country
    country_id
    location
    '''
    create_table_str = '''
        CREATE TABLE tracks (
        track_id SERIAL PRIMARY KEY NOT NULL,
        country_id INT REFERENCES countries(country_id) NOT NULL,
        country VARCHAR(80) NOT NULL,
        location VARCHAR(120) NOT NULL,
        UNIQUE(country, location)
        );
        '''
    initiateTable(db_name, 'tracks', create_table_str)


def initiateEventsTable(db_name):
    '''
    This will generate a table called events with the columns:
    '''
    create_table_str = '''
        CREATE TABLE events (
        event_id SERIAL PRIMARY KEY NOT NULL,
        track_id INT REFERENCES tracks(track_id) NOT NULL,
        country_id INT REFERENCES countries(country_id) NOT NULL,
        location VARCHAR(120) NOT NULL,
        event_name VARCHAR(1000) NOT NULL,
        event_format VARCHAR(50),
        qualifying_date DATE,
        race_date DATE,
        fastf1_api_support BOOL NOT NULL
        );
        '''
    initiateTable(db_name, 'events', create_table_str)


def initiateResultsTable(db_name):
    '''
    This will generate a table for session results.
    '''

    create_table_str = '''
        CREATE TABLE results (
        result_id SERIAL PRIMARY KEY NOT NULL,
        event_id INT REFERENCES events(event_id) NOT NULL,
        driver_id INT REFERENCES drivers(driver_id) NOT NULL,
        driver_code VARCHAR(4),
        q1_time INTERVAL,
        q2_time INTERVAL,
        q3_time INTERVAL,
        grid_position INT,
        race_finish_status VARCHAR(64),
        race_finish_position INT,
        race_points INT,
        race_time INTERVAL,
        fastest_lap_time INTERVAL,
        fastest_lap_number INT,
        UNIQUE(event_id,driver_id)
        );
        '''
    initiateTable(db_name, 'results', create_table_str)


def populateTeamsTable(db_name, years):
    '''
    Will populate the teams table for teams present in sessions during the specified years.
    Will only populate using their most recent race entry. 
    '''
    years = np.sort(years)
    for year in years[::-1]:
        #Loop through years in reverse order.  
        schedule = fastf1.get_event_schedule(year=year, include_testing=False)
        for event_index, event in schedule[::-1].iterrows():
            try:
                for session_type in ['FP1', 'FP2', 'FP3', 'Q', 'Sprint', 'R']:
                    try:
                        session = fastf1.get_session(year, event['Country'], session_type)
                    except Exception as e:
                        continue #Session not in event.
                    driver_info = fastf1.api.driver_info(session.api_path)
                    for key, item in driver_info.items():
                        team_name = item['TeamName']
                        team_color = item['TeamColour']

                        try:
                            connection = pg2.connect(database = db_name, user = sql_user, password = sql_password)
                        except:
                            print("Unable to connect to the database") 

                        cursor = connection.cursor()
                        try:
                            cursor.execute("INSERT INTO teams (team_name, team_color) VALUES (%s,%s) ON CONFLICT DO NOTHING", (team_name, team_color)) #Still will iterate team_ids.  Hopefully not a problem?
                            failed = False
                        except Exception as e:
                            print("\nError inserting into table:")
                            print(e)
                            exc_type, exc_obj, exc_tb = sys.exc_info()
                            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                            print(exc_type, fname, exc_tb.tb_lineno)

                            print("'%s' row NOT inserted.\n"%team_name)
                            failed = True

                        connection.commit() # Makes sure the change is shown in the database
                        connection.close()
                        cursor.close()

            except Exception as e:
                print('Skipping %s %i due to error:'%(event['Country'], year))
                print(e)
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print(exc_type, fname, exc_tb.tb_lineno)

            

def populateDriverTable(db_name, years):
    '''
    Will populate the drivers table for teams present in sessions during the specified years.
    Will only populate using their most recent race entry. 
    '''
    try:
        connection = pg2.connect(database = db_name, user = sql_user, password = sql_password)
    except:
        print("Unable to connect to the database")
        return
    cursor = connection.cursor()

    years = np.sort(years)
    for year in years[::-1]:
        #Loop through years in reverse order.  
        schedule = fastf1.get_event_schedule(year=year, include_testing=False)
        for event_index, event in schedule[::-1].iterrows():
            print('Processing Year %i Event %i/%i'%(year, event_index + 1, len(schedule)))
            try:
                for session_type in ['FP1', 'FP2', 'FP3', 'Q', 'Sprint', 'R']:
                    # Makes many redundent calls for very few table entries, but to be thorough still checking every 
                    # session for new drivers.  Populating these tables is done infrequently.
                    try:
                        session = fastf1.get_session(year, event['Country'], session_type)
                    except Exception as e:
                        continue #Session not in event.
                    driver_info = fastf1.api.driver_info(session.api_path)
                    for key, item in driver_info.items():
                        if 'Tla' in list(item.keys()):
                            driver_code = item['Tla']
                        else:
                            driver_code = 'null'
                        if 'BroadcastName' in list(item.keys()):
                            broadcast_name = item['BroadcastName']
                        else:
                            broadcast_name = 'null'
                        if 'FirstName' in list(item.keys()):
                            first_name = item['FirstName']
                        else:
                            first_name = 'null'
                        if 'LastName' in list(item.keys()):
                            last_name = item['LastName']
                        else:
                            last_name = 'null'
                        if 'RacingNumber' in list(item.keys()):
                            driver_number = item['RacingNumber']
                        else:
                            driver_number = 'null'
                        if 'TeamName' in list(item.keys()):
                            team_name = item['TeamName']
                        else:
                            continue
                            #team_name = 'null'
                        if 'TeamColour' in list(item.keys()):
                            team_color = item['TeamColour']
                        else:
                            team_color = 'null'
                        if 'HeadshotUrl' in list(item.keys()):
                            headshot_url = item['HeadshotUrl']
                        else:
                            headshot_url = 'null'
                        if 'CountryCode' in list(item.keys()):
                            country_code = item['CountryCode']
                        else:
                            country_code = 'null'
                        if 'Reference' in list(item.keys()):
                            driver_reference = item['Reference']
                        else:
                            driver_reference = 'null'

                        try:
                            cursor.execute("INSERT INTO drivers (driver_code, broadcast_name, first_name, last_name, driver_number, most_recent_team_id, team_color, headshot_url, country_code, driver_reference) VALUES (%s,%s,%s,%s,%s,(SELECT t.team_id FROM teams t WHERE t.team_name = %s),%s,%s,%s,%s) ON CONFLICT DO NOTHING", (driver_code, broadcast_name, first_name, last_name, driver_number, team_name, team_color, headshot_url, country_code, driver_reference))
                            failed = False
                        except Exception as e:
                            print("\nError inserting into table:")
                            print(e)
                            exc_type, exc_obj, exc_tb = sys.exc_info()
                            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                            print(exc_type, fname, exc_tb.tb_lineno)
                            print('Input values for failed entry:', driver_code, broadcast_name, first_name, last_name, driver_number, team_name, team_color, headshot_url, country_code, driver_reference)
                            print(item)
                            return
                            failed = True


            except Exception as e:
                print('Skipping %s %i due to error:'%(event['Country'], year))
                print(e)
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print(exc_type, fname, exc_tb.tb_lineno)


    connection.commit() # Makes sure the change is shown in the database
    connection.close()
    cursor.close()

def populateCountryTable(db_name, years):
    '''
    Will populate the country table. 
    '''
    try:
        connection = pg2.connect(database = db_name, user = sql_user, password = sql_password)
    except:
        print("Unable to connect to the database") 
        return
    cursor = connection.cursor()

    years = np.sort(years)
    for year in years[::-1]:
        #Loop through years in reverse order.  
        schedule = fastf1.get_event_schedule(year=year, include_testing=False)
        for event_index, event in schedule[::-1].iterrows():
            try:
                country = event['Country']
                if country in list(known_country_alias.keys()):
                    country = known_country_alias[country]

                try:
                    cursor.execute("INSERT INTO countries (country) VALUES (%s) ON CONFLICT DO NOTHING;", (country,)) #Still will iterate team_ids.  Hopefully not a problem?
                    failed = False
                except Exception as e:
                    print("\nError inserting into table:")
                    print(e)
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print(exc_type, fname, exc_tb.tb_lineno)

                    print("'%s' row NOT inserted.\n"%country)
                    failed = True

            except Exception as e:
                print('Skipping %s %i due to error:'%(event['Country'], year))
                print(e)
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print(exc_type, fname, exc_tb.tb_lineno)


    connection.commit() # Makes sure the change is shown in the database
    connection.close()
    cursor.close()

def populateTracksTable(db_name, years):
    '''
    Will populate the tracks table. 
    '''
    try:
        connection = pg2.connect(database = db_name, user = sql_user, password = sql_password)
    except:
        print("Unable to connect to the database")
        return
    cursor = connection.cursor()

    years = np.sort(years)
    for year in years:
        #Loop through years in reverse order.  
        schedule = fastf1.get_event_schedule(year=year, include_testing=False)
        for event_index, event in schedule.iterrows():
            try:
                country = event['Country']
                if country in list(known_country_alias.keys()):
                    country = known_country_alias[country]
                location = event['Location']

                try:
                    cursor.execute("INSERT INTO tracks (country, country_id, location) VALUES (%s, (SELECT c.country_id FROM countries c WHERE c.country = %s), %s) ON CONFLICT DO NOTHING", (country, country, location)) #Still will iterate team_ids.  Hopefully not a problem?
                    failed = False
                except Exception as e:
                    print("\nError inserting into table:")
                    print(e)
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print(exc_type, fname, exc_tb.tb_lineno)

                    print("'%s' row NOT inserted.\n"%country)
                    failed = True

            except Exception as e:
                print('Skipping %s %i due to error:'%(event['Country'], year))
                print(e)
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print(exc_type, fname, exc_tb.tb_lineno)


    connection.commit() # Makes sure the change is shown in the database
    connection.close()
    cursor.close()

def populateEventsTable(db_name, years):
    '''
    Will populate the events table for events present in the specified years.
    Depends on driver_id, country_id, track_id.   
    '''
    try:
        connection = pg2.connect(database = db_name, user = sql_user, password = sql_password)
    except:
        print("Unable to connect to the database")
        return
    cursor = connection.cursor()

    years = np.sort(years)
    for year in years:
        #Loop through years in reverse order.  
        schedule = fastf1.get_event_schedule(year=year, include_testing=False)
        for event_index, event in schedule.iterrows():
            # event_id # SERIAL PRIMARY KEY NOT NULL,
            
            #track_id Requires SELECT matching country AND location
            country = event['Country']
            if country in list(known_country_alias.keys()):
                country = known_country_alias[country]
            location = event['Location']
            # (SELECT t.track_id FROM tracks t WHERE t.country = %s AND t.location = %s)

            #country_id Requires SELECT matching country
            # (SELECT t.country_id FROM tracks t WHERE t.country = %s LIMIT 1)

            #location already called
            event_name = event['OfficialEventName'] # VARCHAR(1000) NOT NULL,
            event_format = event['EventFormat'] # VARCHAR(50),
            qualifying_date = event.get_session('Q').date.date() # DATE NOT NULL,
            race_date = event.get_session('R').date.date() # DATE NOT NULL,
            fastf1_api_support = event['F1ApiSupport']# BOOL NOT NULL

            try:
                cursor.execute("INSERT INTO events (track_id, country_id, location, event_name, event_format, qualifying_date, race_date, fastf1_api_support) VALUES ( (SELECT t.track_id FROM tracks t WHERE t.country = %s AND t.location = %s), (SELECT t.country_id FROM tracks t WHERE t.country = %s LIMIT 1), %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING", (country, location, country, location, event_name, event_format, qualifying_date, race_date, fastf1_api_support))
                failed = False
            except Exception as e:
                print("\nError inserting into table:")
                print(e)
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print(exc_type, fname, exc_tb.tb_lineno)

                failed = True


            except Exception as e:
                print('Skipping %s %i due to error:'%(event['Country'], year))
                print(e)
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print(exc_type, fname, exc_tb.tb_lineno)


    connection.commit() # Makes sure the change is shown in the database
    connection.close()
    cursor.close()

def populateResultsTable(db_name, years):
    '''
    Will populate the drivers table for teams present in sessions during the specified years.
    Will only populate using their most recent race entry. 
    '''
    try:
        connection = pg2.connect(database = db_name, user = sql_user, password = sql_password)
    except:
        print("Unable to connect to the database")
        return
    cursor = connection.cursor()

    years = np.sort(years)
    for year in years[::-1]:
        #Loop through years in reverse order.  
        schedule = fastf1.get_event_schedule(year=year, include_testing=False)
        for event_index, event in schedule[::-1].iterrows():
            try:
                print('Processing results for SQL entry of %s %i:'%(event['OfficialEventName'], year))
                qualifying_session = fastf1.get_session(year, event['Country'], 'Q')
                qualifying_session.load()
                qualifying_results = qualifying_session.results.convert_dtypes()

                race_session = fastf1.get_session(year, event['Country'], 'R')
                race_session.load()
                race_results = race_session.results.convert_dtypes()

                # Below attempts to account for situations where someone qualified but did not race or vice versa, ordered by race finish.
                driver_codes = np.append(race_session.results['Abbreviation'], qualifying_results['Abbreviation'][~np.isin(qualifying_results['Abbreviation'],race_results['Abbreviation'])])

                for driver_code in driver_codes:
                    driver_qualifying_results = qualifying_results.query('Abbreviation == "{}"'.format(driver_code)).squeeze()
                    driver_race_results = race_results.query('Abbreviation == "{}"'.format(driver_code)).squeeze()
                    fastest_lap = race_session.laps.pick_driver(driver_code).pick_fastest()
                    
                    #result_id =  # SERIAL PRIMARY KEY NOT NULL
                    #event_id = (SELECT e.event_id FROM events e WHERE e.race_date = %s)  # event.get_session('R').date.date()
                    #driver_id = (SELECT d.driver_id FROM drivers d WHERE d.driver_code = %s) # driver_code

                    # Fill relevant time intervals, replace NaT with Null
                    q1_time = driver_qualifying_results['Q1'] # INTERVAL
                    if type(q1_time) is pd._libs.tslibs.nattype.NaTType or pd.isnull(q1_time):
                        q1_time = None

                    q2_time = driver_qualifying_results['Q2'] # INTERVAL
                    if type(q2_time) is pd._libs.tslibs.nattype.NaTType or pd.isnull(q2_time):
                        q2_time = None

                    q3_time = driver_qualifying_results['Q3'] # INTERVAL
                    if type(q3_time) is pd._libs.tslibs.nattype.NaTType or pd.isnull(q3_time):
                        q3_time = None

                    race_time = driver_race_results['Time'] # INTERVAL
                    if type(race_time) is pd._libs.tslibs.nattype.NaTType or pd.isnull(race_time):
                        race_time = None

                    fastest_lap_time = fastest_lap['LapTime'] # INTERVAL
                    if type(fastest_lap_time) is pd._libs.tslibs.nattype.NaTType or pd.isnull(fastest_lap_time):
                        fastest_lap_time = None

                    # Obtain other information
                    race_finish_status = driver_race_results['Status'] # VARCHAR(64)
                    
                    grid_position = driver_race_results['GridPosition']
                    if np.isnan(grid_position):
                        grid_position = None
                    else:
                        grid_position = int(grid_position) # INT
                    race_finish_position = driver_race_results['Position']
                    if np.isnan(race_finish_position):
                        race_finish_position = None
                    else:
                        race_finish_position = int(race_finish_position) # INT
                    race_points = driver_race_results['Points']
                    if np.isnan(race_points):
                        race_points = None
                    else:
                        race_points = int(race_points) # INT
                    
                    fastest_lap_number = fastest_lap['LapNumber']
                    if np.isnan(fastest_lap_number):
                        fastest_lap_number = None
                    else:
                        fastest_lap_number = int(fastest_lap['LapNumber']) #INT

                    try:
                        cursor.execute("INSERT INTO results (event_id, driver_id, driver_code, q1_time, q2_time, q3_time, grid_position, race_finish_status, race_finish_position, race_points, race_time, fastest_lap_time, fastest_lap_number) VALUES ( (SELECT e.event_id FROM events e WHERE e.race_date = %s),(SELECT d.driver_id FROM drivers d WHERE d.driver_code = %s),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING", (event.get_session('R').date.date(), driver_code, driver_code, q1_time, q2_time, q3_time, grid_position, race_finish_status, race_finish_position, race_points, race_time, fastest_lap_time, fastest_lap_number))
                        failed = False
                    except Exception as e:
                        print("\nError inserting into table:")
                        print(e)
                        exc_type, exc_obj, exc_tb = sys.exc_info()
                        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                        print(exc_type, fname, exc_tb.tb_lineno)

                        failed = True
                        print('event: ', event)
                        print('Values on error are: ', event.get_session('R').date.date(), driver_code, driver_code, q1_time, q2_time, q3_time, grid_position, race_finish_status, race_finish_position, race_points, race_time, fastest_lap_time, fastest_lap_number)
                        import pdb; pdb.set_trace()

                    connection.commit() # Makes sure the change is shown in the database

            except Exception as e:
                print('Skipping %s %i due to error:'%(event['OfficialEventName'], year))
                print(e)
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print(exc_type, fname, exc_tb.tb_lineno)


    connection.commit() # Makes sure the change is shown in the database
    connection.close()
    cursor.close()

            
def initiateAllTables(db_name):
    '''
    Initiates the standard set of tables.
    '''
    initiateCountriesTable(db_name) # First, has no dependant table references.
    initiateTracksTable(db_name) # Second
    initiateEventsTable(db_name) # Third
    initiateTeamsTable(db_name) # Fourth
    initiateDriversTable(db_name) # Fifth
    initiateResultsTable(db_name) # Sixth
    
if __name__ == '__main__':
    db_name = 'formula1'
    years = np.arange(2018,2023)
    createDatabase(db_name)
    initiateAllTables(db_name)
    print('*****\n\n\n')

    session = fastf1.get_session(2020, 'UK', 'R')

    # populateTeamsTable(db_name, years)
    # populateDriverTable(db_name, years)
    # populateCountryTable(db_name, years)
    # populateTracksTable(db_name, years)
    # populateEventsTable(db_name, years)
    populateResultsTable(db_name, years)
