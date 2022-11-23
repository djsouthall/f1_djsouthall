'''
A script for exploring an F1 random walk.

Here I hope to use the track sector information from fastf1 to simulate
a random walk, however each step must correspond to an F1 sector.

This is a silly idea but one I am interested in seeing the results of.
Hopefully it is possible with the tools available!

Might turn into a hill climb generator by using Z values, and then
flipping any sector that has a net negative elevation shift to such that
it has a positive shift. 
'''
import os
import sys
import numpy as np
from matplotlib import pyplot as plt
from matplotlib.collections import LineCollection
from matplotlib import cm

import fastf1
import fastf1.plotting
fastf1.Cache.enable_cache(os.environ['f1-cache'])  
fastf1.plotting.setup_mpl()

import warnings
warnings.filterwarnings("ignore")
warnings.simplefilter(action='ignore', category=FutureWarning)

plt.ion()

def getSectorDistances(session):
    '''
    Determines sector distances for given session.

    This will loop over the 10 fastest laps and use the average result 
    as the output.

    Parameters
    ----------
    session : fastf1.core.Session
        The session you want to obtain the lap sectors for. 
    
    Returns
    -------
    sec12 : float
        The lap distance where sector 1 transitions to sector 2.
    sec23 : float
        The lap distance where sector 2 transitions to sector 3.
    '''
    session.load(laps=True)
    laps = session.laps.dropna(subset=['Sector1SessionTime','Sector2SessionTime']).sort_values('LapTime', ascending=True)[0:10]
    sec12s = np.zeros(10)
    sec23s = np.zeros(10)
    for index, (lap_index, lap) in enumerate(laps.iterlaps()):
        try:

            tel = lap.get_telemetry()

            sec12s[index] = tel['Distance'].iloc[np.argmin(abs(tel['SessionTime'] - lap['Sector1SessionTime']))]
            sec23s[index] = tel['Distance'].iloc[np.argmin(abs(tel['SessionTime'] - lap['Sector2SessionTime']))]
        except Exception as e:
            print(e)
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            import pdb; pdb.set_trace()
    
    print(np.mean(sec12s), ':', sec12s)
    print(np.mean(sec23s), ':', sec23s)

    return np.mean(sec12s), np.mean(sec23s)

def getSectorsForTel(tel, sec12, sec23):
    '''
    Returns an array of sector indicators for each point in telemetry.

    Parameters
    ----------
    tel : fastf1.core.Telemetry
        The telemetry of the lap in question.
    sec12 : float
        The lap distance where sector 1 transitions to sector 2.  Can 
        be obtained from getSectorDistances.
    sec23 : float
        The lap distance where sector 2 transitions to sector 3.  Can 
        be obtained from getSectorDistances.
    
    Returns
    -------
    sectors : np.ndarray of ints
        The sector indicators for each point in the telemetry.
    '''
    sectors = np.zeros(len(tel['Distance']), dtype=int)
    sectors[tel['Distance'] < sec12] = 1
    sectors[np.logical_and(tel['Distance'] >= sec12, tel['Distance'] < sec23)] = 2
    sectors[tel['Distance'] >= sec23] = 3

    return sectors


def getSectorVectors(tel, sectors):
    '''
    Calculates the vectors for each sectors transition.

    Parameters
    ----------
    tel : fastf1.core.Telemetry
        The telemetry of the lap in question.
    sectors : np.ndarray of ints
        The sector indicators for each point in the telemetry.
    
    Returns
    -------
    vec12 : np.ndarray of floats
        Normalized vector for transitioning from sector 1 to 2.
    vec23 : np.ndarray of floats
        Normalized vector for transitioning from sector 2 to 3.
    vec31 : np.ndarray of floats
        Normalized vector for transitioning from sector 3 to 1.
    '''
    x = np.array(tel['X'].values)
    y = np.array(tel['Y'].values)
    points = np.array([x, y]).T

    s1 = np.where(sectors == 1)[0]
    s2 = np.where(sectors == 2)[0]
    s3 = np.where(sectors == 3)[0]

    vec12 = (points[min(s2)] - points[max(s1)])/np.linalg.norm(points[min(s2)] - points[max(s1)])
    vec23 = (points[min(s3)] - points[max(s2)])/np.linalg.norm(points[min(s3)] - points[max(s2)])
    vec31 = (points[max(s3)] - points[max(s3)-1])/np.linalg.norm(points[max(s3)] - points[max(s3)-1]) #They lap can overlap, so just using the final vector of the lap.
    
    return vec12, vec23, vec31


def getSectorDict(year, limit_sectors=1000):
    '''
    Returns a specialized dictionary of vectors.
    '''
    schedule = fastf1.get_event_schedule(year=year, include_testing=False)

    sector_dict = {}
    for event_index, (df_index, event) in enumerate(schedule.iterrows()):
        session = fastf1.get_session(year, event['OfficialEventName'], 'Q')
        session.load()
        lap = session.laps.pick_fastest()
        tel = lap.get_telemetry()

        x = np.array(tel['X'].values)
        y = np.array(tel['Y'].values)

        sec12, sec23 = getSectorDistances(session)
        sectors = getSectorsForTel(tel, sec12, sec23)

        vec12, vec23, vec31 = getSectorVectors(tel, sectors)

        points = np.array([x, y]).T.reshape(-1, 1, 2)
        segments = np.concatenate([points[:-1], points[1:]], axis=1)

        for s in [1,2,3]:
            sector_index = event_index*3 + s
            if sector_index > limit_sectors:
                break
            sector_cut = np.where(sectors == s)[0]

            if s == 1:
                starting_vector = vec31
                ending_vector = vec12
            elif s == 2:
                starting_vector = vec12
                ending_vector = vec23
            elif s == 3:
                starting_vector = vec23
                ending_vector = vec31

            sector_dict[sector_index] = {
                'sector' : s,
                'event_index' : event_index,
                'df_index' : df_index,
                'points' : points[sector_cut],
                'starting_vector' : starting_vector,
                'ending_vector' : ending_vector,
            }
        
    return sector_dict


if __name__ == '__main__':
    plt.close('all')
    year = 2022

    sector_dict = getSectorDict(year, limit_sectors=12)

    sector_order = np.random.choice(np.arange(len(sector_dict.keys())), size=len(sector_dict.keys()), replace=False) + 1
    
    for i, sector_index in enumerate(sector_order):
        #Ignoring rotations right now, but ultimately want to align segments to flow. 
        if i == 0:
            segments = np.concatenate([sector_dict[sector_index]['points'][:-1], sector_dict[sector_index]['points'][1:]], axis=1)
            c = np.ones(len(segments))*(sector_dict[sector_index]['event_index'] + 1)
            end_point = sector_dict[sector_index]['points'][-1]
        else:
            _points = sector_dict[sector_index]['points']

            #Translate first point to origin
            _points = _points - _points[0]

            #Rotate each point about origin to align with vector of previous section
            old_vector =  sector_dict[sector_order[i-1]]['ending_vector']
            new_vector =  sector_dict[sector_index]['starting_vector']
            angle_rad = np.arccos(np.dot(old_vector, new_vector))

            print(old_vector)
            print(new_vector)
            print('ANGLE {}'.format(np.rad2deg(angle_rad)))
            cos, sin = np.cos(angle_rad), np.sin(angle_rad)
            #R = np.array(((cos, -sin), (sin, cos)))
            points = np.zeros_like(_points).astype(float)
            points[:,0,0] = cos * _points[:,0,0] - sin * _points[:,0,1]
            points[:,0,1] = sin * _points[:,0,0] + cos * _points[:,0,1]

            # Move to endpoint of previous sector. 
            points = points + end_point

            segments = np.vstack((segments, np.concatenate([points[:-1], points[1:]], axis=1)))
            c = np.append(c, np.ones(len(segments))*(sector_dict[sector_index]['event_index'] + 1))
            end_point = points[-1]

    fig = plt.figure()
    cmap = cm.get_cmap('tab20c')
    lc_comp = LineCollection(segments, norm=plt.Normalize(1, cmap.N+1), cmap=cmap)
    lc_comp.set_array(c)
    lc_comp.set_linewidth(4)
    plt.gca().add_collection(lc_comp)
    plt.axis('equal')
    plt.tick_params(labelleft=False, left=False, labelbottom=False, bottom=False)

    #Maybe color fill based on % of total track and outline as source track?

    cbar = plt.colorbar(mappable=lc_comp, label="Source Race", boundaries=np.arange(1, max(c) + 2))
    # cbar.set_ticks(np.arange(1.5, max(c) + 1.5))
    # cbar.set_ticklabels(np.arange(1, max(c)+1))