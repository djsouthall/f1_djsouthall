'''SQL library generator for F1 data using fastf1

This script is designed to generate SQL tables using data extracted from
the fastf1 package.  The tables are generated using pscopg2 assuming
the user has alread setup PostGreSql externally (e.g. in pgadmin4).

Because this tool depends on the fastf1 package, it can only obtain data
available with that tool.  This imposes a strict limitation for 
analyzing race data prior to the 2018 Formula 1 season.

Usage:
You can import the SQLGenerator into your own script and call the
commands or can run it from a CLI such as ipython (used for example).

> python make_tables.py -u postgres -p password -y "[2022]" -cdb True -db formula1_2022 

Arguments
---------
-u, --username : str, required
    The SQL username for accessing the database.  This must be set
    up externally.

-p, --password : str, required
    The SQL password for the corresponding username.

-db, --database : str, default = "formula1"
    The name of the SQL database to be created or accessed.  Will be
    forced to lowercase.

-cdb, --create_database : bool, default = True
    If True then an attempt to create the database will be made.  If the
    database already exists then nothing happens.  If False then this
    attempt is skipped and the database is assumed to exist.

-y, --years : str, default = "[2022]"
    String formatted as comma seperated list wrapped in []. An list of 
    years.  Note that fastf1 has incomplete data for years prior to 
    2018, which may cause errors. 

-s, --cache_storage_location : str, default = {f1_cache env variable}
    The path of the f1_cache.  If given the default value (None)
    then by default the path will attempt to use the 'f1_cache'
    environment variable.  If this does not exist then it will
    attempt to use './f1_cache'.  If this does not exist it will
    generate the folder.  If a path is given then it will
    utilize that path unless the folder does not exist, in which
    case an error is raised.

-dc, --disable_cache : bool, default = False
    If True then caching will be disabled (not recommended).
'''
import os
import sys
import pdb
import getpass
import argparse
import json
import numpy as np
import pandas as pd
import warnings
warnings.filterwarnings("ignore")
warnings.simplefilter(action='ignore', category=FutureWarning)

import psycopg2 as pg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

import fastf1
import fastf1.plotting  
fastf1.plotting.setup_mpl()

known_country_alias = {"USA":"United States"}

class SQLGenerator():
    '''
    Makes and fills SQL tables of F1 data.

    A tool to generate and populate SQL tables of Formula 1 related data
    using information pulled from the fastf1 package.  Generating
    tables should be executed before attempting to populate those 
    tables.

    This uses the fastf1 class, which downloads F1 data in relatively
    large volumes.  A data cache will be used by default.  If the
    environment variable 'f1_cache' exists and points to a folder then
    all cached data will be stored there.  Otherwise a folder will be
    created in the current directory titled 'f1_cache'.  If such a
    folder already exists it will be utilized unless.  You can disable
    cache by calling the disable_cache() method, and re-enable it by
    calling enable_cache(path={path}).

    Note that the database tools provided here are for hobbyist use and
    thus are not programmed with injection protection in mind.  Use
    with caution.

    Parameters
    ----------
    db_name : str
        The name of the SQL database.  createDatabase will be called
        using this name.  If the database already exists then nothing
        will be done. 
    sql_user : str
        The SQL username for accessing the database.  This must be set
        up externally.
    sql_password : str
        The SQL password for the corresponding username.
    path : str, optional, default={f1_cache env variable}
        The path of the f1_cache.  If given the default value (None)
        then by default the path will attempt to use the 'f1_cache'
        environment variable.  If this does not exist then it will
        attempt to use './f1_cache'.  If this does not exist it will
        generate the folder.  If a path is given then it will
        utilize that path unless the folder does not exist, in which
        case an error is raised.
    disable_cache : bool, optional, default=False
        Disables caching if True (not recommended).
    '''
    def __init__(self, db_name, sql_user, sql_password, path=None, disable_cache=False):
        '''
        Parameters
        ----------
        db_name : str
            The name of the SQL database.  createDatabase will be called
            using this name.  If the database already exists then 
            nothing will be done. 
        sql_user : str
            The SQL username for accessing the database.  This must be 
            set up externally.
        sql_password : str
            The SQL password for the corresponding username.
        path : str, optional, default={f1_cache env variable}
            The path of the f1_cache.  If given the default value (None)
            then by default the path will attempt to use the 'f1_cache'
            environment variable.  If this does not exist then it will
            attempt to use './f1_cache'.  If this does not exist it will
            generate the folder.  If a path is given then it will
            utilize that path unless the folder does not exist, in which
            case an error is raised.
        disable_cache : bool, optional, default=False
            Disables caching if True (not recommended).
        '''
        for obj in (db_name, sql_user, sql_password):
            assert isinstance(obj, str), 'Argument of wrong type!  Expected str recieved {}'.format(type(obj))
        self.db_name = db_name
        self.sql_user = sql_user
        self.__sql_password = sql_password
        
        if disable_cache == False:
            self.enableCache(path=path)
        else:
            self.disableCache()
    
    def enableCache(self, path=None):
        '''
        Enables fastf1 data cache.

        Enables data caching for the fastf1 class which is used to
        generate the SQL tables.  This is highly recommended.

        Parameters
        ----------
        path : str, optional, default = None
            The path of the f1_cache.  If given the default value (None)
            then by default the path will attempt to use the 'f1_cache'
            environment variable.  If this does not exist then it will
            attempt to use './f1_cache'.  If this does not exist it will
            generate the folder.  If a path is given then it will
            utilize that path unless the folder does not exist, in which
            case an error is raised.

        Returns
        -------
        cache_dir : str
            The path that has been set for caching f1 data.
        '''
        if path == None:
            return self.enableCache(path=os.environ['f1_cache'])
        elif path == os.environ['f1_cache']:
            if os.path.exists(path):
                self.cache_dir = os.environ['f1_cache']
            else: 
                path = os.path.join(os.getcwd(), 'f1_cache')
                print('Default FastF1 cache location does not exist.  Attempting to use {}'.format(path))
                if os.path.exists(path):
                    self.cache_dir = path
                else:
                    try:
                        os.mkdir(path)
                        self.cache_dir = path
                    except Exception as e:
                        raise Exception('Error occured trying to set cache directory to {}.  {}'.format(path, e))
        elif type(path) == 'str':
            if os.path.exists(path):
                self.cache_dir = path
            else:
                raise Exception('Given path does not exists: {}'.format(path))
        else:
            raise Exception('Given path not in expected format.  Please use string.\nGiven path: {}\nGiven type: {}'.format(path, type(path)))

        print('FastF1 data cache location: {}'.format(self.cache_dir))
        fastf1.Cache.enable_cache(self.cache_dir)

        return self.cache_dir

    def disableCache(self):
        '''
        Disables fastf1 data cache.
        '''
        return fastf1.disable_cache()

    def createDatabase(self, template='template1'):
        '''
        Using self.db_name, this will generate a new database if one of 
        that db_name does not already exist.  Returns 1 if a new 
        database has been successfully generated and 0 if that database 
        already existed or if there was an error.

        Parameters
        ----------
        template : str
            The PostGreSQL template to be sent in database creation.

        Returns
        -------
            0 if no new database generated.
            1 if new database generated successfully.
        '''
        if self.db_name != self.db_name.lower():
            print('Renaming %s to %s'%(self.db_name, self.db_name.lower()))
            self.db_name = self.db_name.lower()
        try:
            connection = pg2.connect(host='localhost', port='5432', user=self.sql_user,password=self.__sql_password) # Create a connection with PostgreSQL
            print("Database connected")
        except Exception as e:
            raise Exception('Failed to connect to database.\n{}'.format(e))

        if connection is not None:
            connection.autocommit = True
            cursor = connection.cursor()
            cursor.execute("SELECT datname FROM pg_database;")
            list_database = cursor.fetchall()
            if (self.db_name,) in list_database:
                print("'{}' Database already exists, not generated.".format(self.db_name))
                print('Available databases: {}'.format(list_database))
                connection.commit() # Makes sure the change is shown in the database
                connection.close()
                cursor.close()
                return 0
            else:
                print("'{}' Database does not exist.".format(self.db_name))
                sqlCreateDatabase = "CREATE DATABASE {} WITH TEMPLATE {} OWNER {};".format(self.db_name, template, self.sql_user)
                cursor.execute(sqlCreateDatabase)
                print("'{}' Database Generated.".format(self.db_name))
                connection.commit() # Makes sure the change is shown in the database
                connection.close()
                cursor.close()
                return 1

    def _initiateTable(self, table_name, creation_command_str):
        '''
        Will use the given creation_command_str to generate the table
        within self.db_name.  If the table already exists than nothing
        will happen.  This is typically used internally by other
        methods. 

        Parameters
        ----------
        table_name : str
            The name of the table.  Will be forced to lowercase if not
            given as such.
        creation_command_str : str
            The SQL command which will be executed for creating the
            table.
        '''
        for obj in (table_name, creation_command_str):
            assert isinstance(obj, str), 'Argument of wrong type!  Expected str recieved {}'.format(type(obj))

        if table_name != table_name.lower():
            print('Renaming %s to %s'%(table_name, table_name.lower()))
            table_name = table_name.lower()
        try:
            connection = pg2.connect(host='localhost', port='5432', database = self.db_name, user = self.sql_user, password = self.__sql_password)
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

    def _insertIntoTable(self, table_name, insert_command_str):
        '''
        Will use the given insert_command_str to insert into the table
        within self.db_name.

        Parameters
        ----------
        table_name : str
            The name of the table.  Will be forced to lowercase if not
            given as such.
        insert_command_str : str
            The SQL command which will be executed for inserting into
            the table.
        '''
        for obj in (table_name, insert_command_str):
            assert isinstance(obj, str), 'Argument of wrong type!  Expected str recieved {}'.format(type(obj))
        try:
            connection = pg2.connect(host='localhost', port='5432', database = self.db_name, user = self.sql_user, password = self.__sql_password)
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

    def _initiateDriversTable(self):
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
        self._initiateTable('drivers', create_table_str)

    def _initiateTeamsTable(self):
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
        self._initiateTable('teams', create_table_str)

    def _initiateCountriesTable(self):
        '''
        This will generate a table called countries.
        '''
        create_table_str = '''
            CREATE TABLE countries (
            country_id SERIAL PRIMARY KEY NOT NULL,
            country VARCHAR(80) UNIQUE NOT NULL
            );
            '''
        self._initiateTable('countries', create_table_str)

    def _initiateTracksTable(self):
        '''
        This will generate a table called tracks.
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
        self._initiateTable('tracks', create_table_str)


    def _initiateEventsTable(self):
        '''
        This will generate a table called events.
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
        self._initiateTable('events', create_table_str)


    def _initiateResultsTable(self):
        '''
        This will generate a table for session results.
        '''

        create_table_str = '''
            CREATE TABLE results (
            result_id SERIAL PRIMARY KEY NOT NULL,
            event_id INT REFERENCES events(event_id) NOT NULL,
            sprint BOOL NOT NULL,
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
            UNIQUE(event_id,driver_id,sprint)
            );
            '''
        self._initiateTable('results', create_table_str)

    def _initiateLapsTable(self):
        '''
        This will generate a table for all lap times.
        '''

        create_table_str = '''
            CREATE TABLE laps (
            lap_id SERIAL PRIMARY KEY NOT NULL,
            event_id INT REFERENCES events(event_id) NOT NULL,
            session_type VARCHAR(64),
            driver_id INT REFERENCES drivers(driver_id) NOT NULL,
            driver_code VARCHAR(4),
            lap_number INT,
            lap_time INTERVAL,
            track_status INT,
            tyre_compound VARCHAR(16),
            tyre_life INT,
            UNIQUE(event_id,driver_id,session_type,lap_number)
            );
            '''
        self._initiateTable('laps', create_table_str)

    def _populateTeamsTable(self, years):
        '''
        Will populate the teams table for teams present in sessions 
        during the specified years.  Will only populate using their most
        recent race entry. 
        '''
        years = np.sort(years)
        for year in years[::-1]:
            #Loop through years in reverse order.  
            schedule = fastf1.get_event_schedule(year=year, include_testing=False)
            for event_index, event in schedule[::-1].iterrows():
                try:
                    for session_type in ['FP1', 'FP2', 'FP3', 'Q', 'Sprint', 'R']:
                        try:
                            session = fastf1.get_session(year, event['OfficialEventName'], session_type)
                        except Exception as e:
                            continue #Session not in event.
                        driver_info = fastf1.api.driver_info(session.api_path)
                        for key, item in driver_info.items():
                            team_name = item['TeamName']
                            team_color = item['TeamColour']

                            try:
                                connection = pg2.connect(host='localhost', port='5432', database = self.db_name, user = self.sql_user, password = self.__sql_password)
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
                    print('Skipping %s %i due to error:'%(event['OfficialEventName'], year))
                    print(e)
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print(exc_type, fname, exc_tb.tb_lineno)

            

    def _populateDriversTable(self, years):
        '''
        Will populate the drivers table for teams present in sessions
        during the specified years.  Will only populate using their most
        recent race entry. 
        '''
        try:
            connection = pg2.connect(host='localhost', port='5432', database = self.db_name, user = self.sql_user, password = self.__sql_password)
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
                            session = fastf1.get_session(year, event['OfficialEventName'], session_type)
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
                    print('Skipping %s %i due to error:'%(event['OfficialEventName'], year))
                    print(e)
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print(exc_type, fname, exc_tb.tb_lineno)


        connection.commit() # Makes sure the change is shown in the database
        connection.close()
        cursor.close()

    def _populateCountriesTable(self, years):
        '''
        Will populate the country table. 
        '''
        try:
            connection = pg2.connect(host='localhost', port='5432', database = self.db_name, user = self.sql_user, password = self.__sql_password)
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
                    print('Skipping %s %i due to error:'%(event['OfficialEventName'], year))
                    print(e)
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print(exc_type, fname, exc_tb.tb_lineno)


        connection.commit() # Makes sure the change is shown in the database
        connection.close()
        cursor.close()

    def _populateTracksTable(self, years):
        '''
        Will populate the tracks table. 
        '''
        try:
            connection = pg2.connect(host='localhost', port='5432', database = self.db_name, user = self.sql_user, password = self.__sql_password)
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
                    print('Skipping %s %i due to error:'%(event['OfficialEventName'], year))
                    print(e)
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print(exc_type, fname, exc_tb.tb_lineno)


        connection.commit() # Makes sure the change is shown in the database
        connection.close()
        cursor.close()

    def _populateEventsTable(self, years):
        '''
        Will populate the events table for events present in the 
        specified years.  Depends on driver_id, country_id, track_id.   
        '''
        try:
            connection = pg2.connect(host='localhost', port='5432', database = self.db_name, user = self.sql_user, password = self.__sql_password)
        except:
            print("Unable to connect to the database")
            return
        cursor = connection.cursor()

        years = np.sort(years)
        for year in years:
            #Loop through years in reverse order.  
            schedule = fastf1.get_event_schedule(year=year, include_testing=False)
            for event_index, event in schedule.iterrows():
                #track_id Requires SELECT matching country AND location
                country = event['Country']
                if country in list(known_country_alias.keys()):
                    country = known_country_alias[country]
                location = event['Location']

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
                    print('Skipping %s %i due to error:'%(event['OfficialEventName'], year))
                    print(e)
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print(exc_type, fname, exc_tb.tb_lineno)


        connection.commit() # Makes sure the change is shown in the database
        connection.close()
        cursor.close()

    def _populateResultsTable(self, years):
        '''
        Will populate the drivers table for teams present in sessions 
        during the specified years.  Will only populate using their most
        recent race entry. 
        '''
        try:
            connection = pg2.connect(host='localhost', port='5432', database = self.db_name, user = self.sql_user, password = self.__sql_password)
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
                    qualifying_session = fastf1.get_session(year, event['OfficialEventName'], 'Q')
                    qualifying_session.load()
                    qualifying_results = qualifying_session.results.convert_dtypes()

                    if event['EventFormat'] == 'sprint':
                        sprint = True
                        sprint_session = fastf1.get_session(year, event['OfficialEventName'], 'S')
                        sprint_session.load()
                        sprint_results = sprint_session.results.convert_dtypes()
                    else:
                        sprint = False

                    race_session = fastf1.get_session(year, event['OfficialEventName'], 'R')
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
                            # NEED TO INCLUDE SPRINT BOOL TO TABLE
                            cursor.execute("INSERT INTO results (event_id, sprint, driver_id, driver_code, q1_time, q2_time, q3_time, grid_position, race_finish_status, race_finish_position, race_points, race_time, fastest_lap_time, fastest_lap_number) VALUES ( (SELECT e.event_id FROM events e WHERE e.race_date = %s),%s,(SELECT d.driver_id FROM drivers d WHERE d.driver_code = %s),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING", (event.get_session('R').date.date(), False, driver_code, driver_code, q1_time, q2_time, q3_time, grid_position, race_finish_status, race_finish_position, race_points, race_time, fastest_lap_time, fastest_lap_number))
                            failed = False
                        except Exception as e:
                            print("\nError inserting into table:")
                            print(e)
                            exc_type, exc_obj, exc_tb = sys.exc_info()
                            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                            print(exc_type, fname, exc_tb.tb_lineno)

                            failed = True
                            print('event: ', event)
                            print('Values on error are: ', event.get_session('R').date.date(), sprint, driver_code, driver_code, q1_time, q2_time, q3_time, grid_position, race_finish_status, race_finish_position, race_points, race_time, fastest_lap_time, fastest_lap_number)
                            import pdb; pdb.set_trace()

                        connection.commit() # Makes sure the change is shown in the database
                        
                        if sprint:
                            driver_race_results = sprint_results.query('Abbreviation == "{}"'.format(driver_code)).squeeze()
                            race_time = driver_race_results['Time'] # INTERVAL
                            fastest_lap = sprint_session.laps.pick_driver(driver_code).pick_fastest()
                            
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
                                # NEED TO INCLUDE SPRINT BOOL TO TABLE
                                cursor.execute("INSERT INTO results (event_id, sprint, driver_id, driver_code, q1_time, q2_time, q3_time, grid_position, race_finish_status, race_finish_position, race_points, race_time, fastest_lap_time, fastest_lap_number) VALUES ( (SELECT e.event_id FROM events e WHERE e.race_date = %s),%s,(SELECT d.driver_id FROM drivers d WHERE d.driver_code = %s),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING", (event.get_session('R').date.date(), True, driver_code, driver_code, q1_time, q2_time, q3_time, grid_position, race_finish_status, race_finish_position, race_points, race_time, fastest_lap_time, fastest_lap_number))
                                failed = False
                            except Exception as e:
                                print("\nError inserting into table:")
                                print(e)
                                exc_type, exc_obj, exc_tb = sys.exc_info()
                                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                                print(exc_type, fname, exc_tb.tb_lineno)

                                failed = True
                                print('event: ', event)
                                print('Values on error are: ', event.get_session('R').date.date(), sprint,driver_code, driver_code, q1_time, q2_time, q3_time, grid_position, race_finish_status, race_finish_position, race_points, race_time, fastest_lap_time, fastest_lap_number)
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

    def _populateLapsTable(self, years):
        '''
        Will populate the laps table for all sessions during the 
        specified years.
        '''
        try:
            connection = pg2.connect(host='localhost', port='5432', database = self.db_name, user = self.sql_user, password = self.__sql_password)
        except:
            print("Unable to connect to the database")
            return
        cursor = connection.cursor()

        years = np.sort(years)
        for year in years[::-1]:
            #Loop through years in reverse order.  
            schedule = fastf1.get_event_schedule(year=year, include_testing=False)
            for event_index, event in schedule[::-1].iterrows():
                for session_type in ['FP1', 'FP2', 'FP3', 'Q', 'Sprint', 'R']:
                    try:
                        session = fastf1.get_session(year, event['OfficialEventName'], session_type)
                    except Exception as e:
                        continue #Session not in event.
                    session.load()
                    lap_info = session.laps[['LapTime', 'LapNumber','Compound','Driver','TyreLife','TrackStatus']]
                    lap_info.replace({np.nan: None}, inplace = True)

                    for lap_index, lap in lap_info.iterlaps():
                        try:
                            cursor.execute("INSERT INTO laps (event_id, session_type, driver_id, driver_code, lap_number, lap_time, track_status, tyre_compound, tyre_life) VALUES ( (SELECT e.event_id FROM events e WHERE e.race_date = %s), %s ,(SELECT d.driver_id FROM drivers d WHERE d.driver_code = %s),%s,%s,%s, %s,%s,%s) ON CONFLICT DO NOTHING", (event.get_session('R').date.date(), session_type, lap['Driver'], lap['Driver'], lap['LapNumber'], lap['LapTime'], lap['TrackStatus'], lap['Compound'], lap['TyreLife']))

                            failed = False
                        except Exception as e:
                            print("\nError inserting into table:")
                            print(e)
                            exc_type, exc_obj, exc_tb = sys.exc_info()
                            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                            print(exc_type, fname, exc_tb.tb_lineno)

                    connection.commit() # Makes sure the change is shown in the database

        connection.commit() # Makes sure the change is shown in the database
        connection.close()
        cursor.close()

            
    def initiateAllTables(self):
        '''
        Initiates the standard set of tables.
        '''
        self._initiateCountriesTable() # First, has no dependant table references.
        self._initiateTracksTable() # Second
        self._initiateEventsTable() # Third
        self._initiateTeamsTable() # Fourth
        self._initiateDriversTable() # Fifth
        self._initiateResultsTable() # Sixth
        self._initiateLapsTable() # Seventh
    
    def populateAllTables(self, years):
        '''
        Populates the standard set of tables for the given years.

        Parameters
        ----------
        years : np.ndarray of int
            An array of years.  Note that fastf1 has incomplete data
            for years prior to 2018, which may cause errors. 
        '''
        self._populateCountriesTable(years) # First
        self._populateTracksTable(years) # Second
        self._populateEventsTable(years) # Third
        self._populateTeamsTable(years) # Fourth
        self._populateDriversTable(years) # Fifth
        self._populateResultsTable(years) # Sixth
        self._populateLapsTable(years) # Seventh

class Password(argparse.Action):
    '''
    For handling parseing of password argument.  Taken from:
    https://stackoverflow.com/a/29948740/10156360

    I struggle to see the virtues of this, as I feel it doesn't really
    hide the password from the namespace?  
    '''
    def __call__(self, parser, namespace, values, option_string=None):
        if values is None:
            values = getpass.getpass()
        setattr(namespace, self.dest, values)
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-u", "--username", dest="sql_user", help="SQL Username", required=True)
    parser.add_argument("-p", "--password", action=Password, nargs='?', dest="sql_password", help="SQL Password", required=True)
    parser.add_argument("-y", "--years", dest="years", type = lambda y: json.loads(y), help="Years to populate database.  String formatted as comma seperated list wrapped in [].", default="[2022]")
    parser.add_argument("-db", "--database", dest="db_name", help="Name of database", default="formula1")
    parser.add_argument("-cdb", "--create_database", dest="create_database", type=bool, help="Attempt to create database", default=True)
    parser.add_argument("-dc", "--disable_cache", dest="disable_cache", type=bool, help="Disable data cache.", default=False)
    parser.add_argument("-s", "--cache_storage_location",dest="cache_path", type=str, help="Location for cached data to be stored.", default='')

    args = parser.parse_args()

    print('Attempting to generate database {} for years: {}'.format(args.db_name, args.years))

    if len(args.cache_path) > 0:
        path = args.cache_path
    else:
        path = None

    f1gen = SQLGenerator(args.db_name, args.sql_user, args.sql_password, path=path, disable_cache=args.disable_cache)
    if args.create_database == True:
        print('Attempting to create database.')
        f1gen.createDatabase()
    else:
        print('Assuming database exists.')

    f1gen.initiateAllTables()
    f1gen.populateAllTables(args.years)