'''
These functions are designed to generate SQL databases and tables regarding.


Initial set of sample code for working with SQL databases using python.
Specifically this uses PostgreSQL using Psycopg2 (  https://www.psycopg.org/docs/ ).
'''
import os
import pdb

import psycopg2 as pg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

import fastf1

sql_user = 'postgres'
sql_password = 'password'

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

def initiateDriverTable():
    '''
    This will generate a table called drivers with the columns:

    driver_id
    first_name
    last_name
    nick_name
    birth_date
    current_team_id
    historic_team_ids
    active
    '''
    pass

def initiateDriverStatsTable():
    '''
    This will generate a table called driver_stats.  I am not sure whether
    I want to actually make this database as I might need to scrape more data
    from the internet than I want to. Potential columns:

    driver_id
    race_wins
    '''
    pass

def initiateTeamTable():
    '''
    This will generate a table called teams with the columns:

    team_id
    team_name
    driver_ids
    last_name
    nick_name
    birth_date
    team_id
    '''
    pass

def initiateTrackTable():
    '''
    This will generate a table called tracks with the columns:

    track_id
    track_name
    country
    '''
    pass

def initiateRaceTable():
    '''
    This will generate a table called races with the columns:

    race_id
    track_id
    grand_prix_name
    entrants
    winning_driver_id
    second_driver_id
    third_driver_id
    pole_position
    fastest_lap
    race_date
    '''
    pass


if __name__ == '__main__':
    dbname = 'formula1'
    createDatabase(dbname)



# # 'password' is whatever password you set

# cur = conn.cursor()

# # Pass in a PostgreSQL query as a string
# cur.execute("SELECT * FROM payment")

# # Return a tuple of the first row as Python objects
# cur.fetchone()

# # Return N number of rows
# cur.fetchmany(10)

# # Return All rows at once
# cur.fetchall()

# # To save and index results, assign it to a variable
# data = cur.fetchmany(10)


# # **Inserting Information**
# query1 = '''
#         CREATE TABLE new_table (
#             userid integer
#             , tmstmp timestamp
#             , type varchar(10)
#         );
#         '''
# cur.execute(query1)

# # commit the changes to the database
# cur.commit()

# # Closing connection
# conn.close()

