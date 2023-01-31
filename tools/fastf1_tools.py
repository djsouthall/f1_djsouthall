'''
A series of functions that will be used when working with fastf1 data.
'''

from matplotlib import pyplot as plt
import numpy as np
import pandas as pd
import os
import sys

import fastf1
import fastf1.plotting
fastf1.Cache.enable_cache(os.environ['f1_cache'])  
fastf1.plotting.setup_mpl()

import warnings
warnings.filterwarnings("ignore")
warnings.simplefilter(action='ignore', category=FutureWarning)

def formattedLapTelem(session, sample_interval_seconds=0.1, drivers=None, n_samples=None, telem_param='Speed'):
    '''
    This will obtain the specified telemetry and resample all laps to a
    consistent length (NaN padded).  This will only include laps which
    do not have a lap in or lap out time (no pit stopping).

    This has only been tested for speed.

    Laps for the given drivers will be returned.

    Parameters
    ----------
    drivers : list of str, default = None
        The list of drivers you want to obtain formatted laps for.
        By default this is None, which will result in ALL drivers within
        the given session.  This should be a list of driver 'Tla'
        values, e.g. ['VER', 'HAM']
    n_samples : int, default = None
        The desired length of the output array.  If None then this will
        be calculate as the maximum number necessary to capture all
        laps adequately.   
    '''
    session.load()

    # Get driver info ready
    driver_info = fastf1.api.driver_info(session.api_path)
    all_drivers = [d['Tla'] for i, d in driver_info.items()]
    if drivers is None:
        drivers = all_drivers
    else:
        drivers = np.asarray(drivers)[np.isin(drivers, all_drivers)]

    # Get relevant laps
    # relevant_laps = session.laps.pick_wo_box().pick_track_status(status='1', how='equals') # Getting laps with no pit in or out time, and where entire lap is under normal conditions.
    # return relevant_laps
    return session.laps


def saveTelemForYear(year, outpath=os.path.join(os.environ['f1_install'], 'dataframes'), n_samples=2048, sample_interval_seconds=0.1, telem_param='Speed'):
    '''
    This will loop over all of the laps within the year that are not in
    or outlaps (in races) and store the telemetry data in a consistently
    sampled dataframe for all of the specified drivers.  
    
    See Also
    --------
        formattedLapTelem
    '''
    schedule = fastf1.get_event_schedule(year=year, include_testing=False)
    for event_index, event in schedule.iterrows():
        print('Processing {} for {} {}'.format(telem_param, event['EventName'],event['EventDate'].year))
        session = event.get_session('R')
        formatted_telem = formattedLapTelem(session, sample_interval_seconds=sample_interval_seconds, drivers=None, n_samples=n_samples, telem_param=telem_param)
        
        filename = os.path.join(outpath, '{} {} {}.pkl'.format(telem_param, event['EventName'],event['EventDate'].year).replace(' ', '_'))
        print('Saving {}'.format(filename))
        try:
            formatted_telem.to_pickle(os.path.join(outpath, filename))
        except Exception as e:
            print(e)
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)

def loadTelemForYear(year, path=os.path.join(os.environ['f1_install'], 'dataframes'), telem_param='Speed', return_index_key=False):
    '''
    This will load the data corresponding to a similar call of
    saveTelemForYear
    
    See Also
    --------
        saveTelemForYear
    '''
    schedule = fastf1.get_event_schedule(year=year, include_testing=False)
    index_key = {}
    for event_index, event in schedule.iterrows():
        index_key[event_index] = {'EventName' : event['EventName'], 'name' : event['EventName'].replace('Grand Prix','gp').replace(' ','_').lower()}
        filename = os.path.join(path, '{} {} {}.pkl'.format(telem_param, event['EventName'],event['EventDate'].year).replace(' ', '_'))
        print('Loading {}'.format(filename))
        try:            
            _df = pd.read_pickle(os.path.join(path, filename))
            _df.columns = [c.replace('_','_{}_'.format(event_index)) for c in _df.columns]
            
            try:
                df = df.join(_df)
            except:
                df = _df
        except Exception as e:
            print(e)
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
    
    # Clean column names:
    df.columns = ['{}_{}_{}'.format(c.split('_')[0], int(float(c.split('_')[1])), int(float(c.split('_')[2]))) for c in df.columns] # Forcing integers where some might have been decimals. 
    

    if return_index_key:
        return df, index_key
    else:
        return df

def loadTelemForYearDict(year, path=os.path.join(os.environ['f1_install'], 'dataframes'), telem_param='Speed'):
    '''
    This will load the data corresponding to a similar call of
    saveTelemForYear
    
    See Also
    --------
        saveTelemForYear
    '''
    schedule = fastf1.get_event_schedule(year=year, include_testing=False)
    out = {}
    for event_index, event in schedule.iterrows():
        filename = os.path.join(path, '{} {} {}.pkl'.format(telem_param, event['EventName'],event['EventDate'].year).replace(' ', '_'))
        print('Loading {}'.format(filename))
        try:            
            out[event_index] = {'EventName' : event['EventName'],
                                telem_param : pd.read_pickle(os.path.join(path, filename))}
        except Exception as e:
            print(e)
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
    return out

def loadTelemForEvent(event, path=os.path.join(os.environ['f1_install'], 'dataframes'), telem_param='Speed'):
    '''
    This will load the data corresponding to a similar call of
    saveTelemForYear
    
    See Also
    --------
        saveTelemForYear
    '''
    filename = os.path.join(path, '{} {} {}.pkl'.format(telem_param, event['EventName'],event['EventDate'].year).replace(' ', '_'))
    print('Loading {}'.format(filename))
    try:            
        return pd.read_pickle(os.path.join(path, filename))
    except Exception as e:
        print(e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)

def returnTrackStatusDict():
    track_status_dict = {
            1 : 'Track clear (beginning of session ot to indicate the end of another status)',
            2 : 'Yellow flag (sectors are unknown)',
            3 : '??? Never seen so far, does not exist?',
            4 : 'Safety Car',
            5 : 'Red Flag',
            6 : 'Virtual Safety Car deployed',
            7 : 'Virtual Safety Car ending (As indicated on the drivers steering wheel, on tv and so on; status ‘1’ will mark the actual end)'
        }
    return track_status_dict


if __name__ == '__main__':
    if True:
        if False:
            saveTelemForYear(2022, outpath=os.path.join(os.environ['f1_install'], 'dataframes'), n_samples=2048, sample_interval_seconds=0.1, telem_param='Speed')
        else:
            loaded_dataframes = loadTelemForYear(2022, path=os.path.join(os.environ['f1_install'], 'dataframes'), telem_param='Speed')
    else:
        schedule = fastf1.get_event_schedule(year=2022, include_testing=False)
        for event_index, event in schedule.iterrows():
            session = event.get_session('R')
            session.load()
            break
        # laps = session.laps
        # session = event.get_session('R')
        # session.load()
        lap_info = session.laps[['LapTime', 'LapNumber','Compound','Driver','TyreLife','TrackStatus']]

