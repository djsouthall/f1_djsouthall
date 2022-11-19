'''
These functions are designed to generate SQL databases and tables regarding.


Initial set of sample code for working with SQL databases using python.
Specifically this uses PostgreSQL using Psycopg2 (  https://www.psycopg.org/docs/ ).
'''
import os
import pdb
import numpy as np

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
        name_code VARCHAR(4),
        broadcast_name VARCHAR(80),
        first_name VARCHAR(80),
        last_name VARCHAR(80),
        driver_number INT,
        team_id INT REFERENCES teams(team_id) NOT NULL,
        team_color VARCHAR(6),
        headshot_url VARCHAR(1000),
        country_code VARCHAR(10),
        driver_reference VARCHAR(30) UNIQUE 
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
        country VARCHAR(80) NOT NULL,
        country_id INT REFERENCES countries(country_id) NOT NULL,
        location VARCHAR(120) NOT NULL,
        UNIQUE(country, location)
        );
        '''
    initiateTable(db_name, 'tracks', create_table_str)


def initiateEventsTable(db_name):
    '''
    This will generate a table called events with the columns:
    '''
    # api_cols = ['Country',
    #             'Location',
    #             'OfficialEventName',
    #             'EventDate',
    #             'EventName',
    #             'EventFormat',
    #             'Session1',
    #             'Session1Date',
    #             'Session2',
    #             'Session2Date',
    #             'Session3',
    #             'Session3Date',
    #             'Session4',
    #             'Session4Date',
    #             'Session5',
    #             'Session5Date',
    #             'F1ApiSupport']

    create_table_str = '''
        CREATE TABLE events (
        event_id SERIAL PRIMARY KEY NOT NULL,
        track_id INT REFERENCES tracks(track_id) NOT NULL,
        country_id INT REFERENCES countries(country_id) NOT NULL,
        location VARCHAR(120) NOT NULL,
        event_name VARCHAR(1000) NOT NULL,
        event_format VARCHAR(50),
        start_time_p1 TIMESTAMP,
        start_time_p2 TIMESTAMP,
        start_time_p3 TIMESTAMP,
        start_time_q TIMESTAMP NOT NULL,
        start_time_r TIMESTAMP NOT NULL,
        fastf1_api_support BOOL NOT NULL
        );
        '''
    initiateTable(db_name, 'events', create_table_str)

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
                            print("'%s' row NOT inserted.\n"%team_name)
                            failed = True

                        connection.commit() # Makes sure the change is shown in the database
                        connection.close()
                        cursor.close()

            except Exception as e:
                print('Skipping %s %i due to error:'%(event['Country'], year))
                print(e)
            

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
                            name_code = item['Tla']
                        else:
                            name_code = 'null'
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
                            team_name = 'null'
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
                            cursor.execute("INSERT INTO drivers (name_code, broadcast_name, first_name, last_name, driver_number, team_id, team_color, headshot_url, country_code, driver_reference) VALUES (%s,%s,%s,%s,%s,(SELECT t.team_id FROM teams t WHERE t.team_name = %s),%s,%s,%s,%s) ON CONFLICT DO NOTHING", (name_code, broadcast_name, first_name, last_name, driver_number, team_name, team_color, headshot_url, country_code, driver_reference))
                            failed = False
                        except Exception as e:
                            print("\nError inserting into table:")
                            print(e)
                            print("'%s' row NOT inserted.\n"%drivers)
                            failed = True


            except Exception as e:
                print('Skipping %s %i due to error:'%(event['Country'], year))
                print(e)

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
                    print("'%s' row NOT inserted.\n"%country)
                    failed = True

            except Exception as e:
                print('Skipping %s %i due to error:'%(event['Country'], year))
                print(e)

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
    for year in years[::-1]:
        #Loop through years in reverse order.  
        schedule = fastf1.get_event_schedule(year=year, include_testing=False)
        for event_index, event in schedule[::-1].iterrows():
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
                    print("'%s' row NOT inserted.\n"%country)
                    failed = True

            except Exception as e:
                print('Skipping %s %i due to error:'%(event['Country'], year))
                print(e)

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
    
if __name__ == '__main__':
    db_name = 'formula1'
    years = np.arange(2018,2023)
    createDatabase(db_name)
    initiateAllTables(db_name)
    print('*****\n\n\n')

    populateTeamsTable(db_name, years)
    populateDriverTable(db_name, years)
    populateCountryTable(db_name, years)
    populateTracksTable(db_name, years)
