'''
A series of functions that will be used when working with fastf1 data.
'''

from matplotlib import pyplot as plt
import numpy as np
import pandas as pd
import os

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
    relevant_laps = session.laps[np.logical_and(session.laps.PitOutTime.isna(), session.laps.PitInTime.isna())] # Getting laps with no pit in or out time. 
    relevant_laps = relevant_laps[np.isin(relevant_laps['Driver'].values, drivers)] # Getting only laps of relevant drivers

    # Setup time indexing
    sample_interval_seconds_str = '{}S'.format(sample_interval_seconds)
    if n_samples is None:
        max_time_seconds = relevant_laps['LapTime'].max().value/1e9 # Get max lap time in seconds.
        n_samples = int(2**np.ceil(np.log2(max_time_seconds/sample_interval_seconds)))#np.ceil(max_time_seconds/sample_interval_seconds)
    else:
        n_samples = int(n_samples)

    time_index = (np.arange(n_samples))*np.timedelta64(int(sample_interval_seconds*1000),'ms')


    # Setup empty DF
    formatted_telem = pd.DataFrame(index=time_index, columns=['{}_{}'.format(lap['Driver'], lap['LapNumber']) for lap_index, lap in relevant_laps.iterlaps()])

    print('Cleaning Laps')

    # Get lap telemetry and resample.
    for i, (lap_index, lap) in enumerate(relevant_laps.iterlaps()):
        print('On lap {}/{}'.format(i+1,len(relevant_laps)), end="\r")
        telem_data = lap.get_telemetry()
        ts = pd.Series(telem_data[telem_param].values, index=telem_data['Time'])
        rs = ts.resample(sample_interval_seconds_str, kind='timestamp').mean().interpolate(method='linear').ewm(span = 2).mean().reindex(time_index) # Smoothed and interpolated series, was also shifting using .shift(-0.5,freq=sample_interval_seconds_str), however I wanted it to start at 0 and it didn't matter.
        formatted_telem['{}_{}'.format(lap['Driver'], lap['LapNumber'])] = rs
    return formatted_telem

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
            
def loadTelemForYear(year, path=os.path.join(os.environ['f1_install'], 'dataframes'), telem_param='Speed'):
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
                                telem_param : pd.from_pickle(os.path.join(path, filename))}
        except Exception as e:
            print(e)
    return out

if __name__ == '__main__':
    if False:
        saveTelemForYear(2022, outpath=os.path.join(os.environ['f1_install'], 'dataframes'), n_samples=2048, sample_interval_seconds=0.1, telem_param='Speed')
    else:
        loaded_dataframes = loadTelemForYear(2022, path=os.path.join(os.environ['f1_install'], 'dataframes'), telem_param='Speed')