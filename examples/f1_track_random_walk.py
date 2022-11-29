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
from matplotlib.backends.backend_pdf import PdfPages

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

def getSectorDict(year, limit_sectors=1000, selected_events=None, selected_sectors=None):
    '''
    Returns a specialized dictionary of sector points and meta info.

    Parameters
    ----------
    year : int
        The F1 season you wish to pull tracks from.
    limit_sectors : int
        The maximum number of sectors to retrieve information for.
        If selected_events and selected_sectors are not specified then
        this will randomly select N=limit_sectors sectors from the
        given season.  If selected_events and selected_sectors are 
        specified then this will be ignored. 
    selected_events : np.ndarray of int
        The events you wish to be included in the dictionary.  Should
        match the same length and shape as selected_events, with sectors
        and elements corresponding to single event-sector pairings.
        This will not handle repeated cases.  These range from 0 to the
        maximum number of races in the given season.
    selected_sectors : np.ndarray of int
        The sectors you wish to be included in the dictionary.  Should
        match the same length and shape as selected_events, with sectors
        and elements corresponding to single event-sector pairings.
        This will not handle repeated cases.  Array contents must all
        be 1's, 2's, or 3's. 
    
    Returns
    -------
    sector_dict : dict
        A dictionary containing meta information about the retrieved
        sectors.  
            'sector' : the sector within the event (1,2, or 3)
            'event_index' : the index of the event (first race = 0)
            'df_index' : index within fastf1 panadas dataframe
            'points' : xy coordinates of the track layout
    '''
    schedule = fastf1.get_event_schedule(year=year, include_testing=False)
    
    if selected_sectors is not None and selected_events is not None:
        if len(selected_sectors) == len(selected_events):
            if np.all(np.isin(selected_sectors , np.arange(3)+1)):
                if np.all(np.isin(selected_events , np.arange(len(schedule)))):
                    pass
                else:
                    print('Events given not in the expected range.  Must be in:\n', np.arange(len(schedule)))
                    print('Generating new selected_sectors and selected_events using given limit_sectors instead: limit_sectors = ', limit_sectors)
                    selected_sectors = None
                    seleced_events = None
            else:
                print('Sectors given not in the sector range.  Must be in:\n', np.arange(3)+1)
                print('Generating new selected_sectors and selected_events using given limit_sectors instead: limit_sectors = ', limit_sectors)
                selected_sectors = None
                seleced_events = None
        else:
            print('selected_sectors length and selected_events length do not match.')
            print('Generating new selected_sectors and selected_events using given limit_sectors instead: limit_sectors = ', limit_sectors)
            selected_sectors = None
            selected_events = None
            
    if selected_sectors is None and selected_events is None:
        print('Selecting sectors.')
        if limit_sectors > len(schedule)*3:
            limit_sectors = len(schedule)*3
    
        all_sectors = np.tile([1,2,3], len(schedule))
        all_events = np.repeat(np.arange(len(schedule)), 3)

        selected = np.random.choice(np.arange(len(schedule)*3), size=limit_sectors, replace=False)

        selected_sectors = all_sectors[selected]
        selected_events = all_events[selected]
    else:
        limit_sectors = len(selected_sectors)
        
    selected_sectors = np.asarray(selected_sectors).flatten()
    selected_events = np.asarray(selected_events).flatten()
    
    print('Using events/sectors: ', list(zip(selected_events, selected_sectors)))

    sector_dict = {}
    
    for event_index, (df_index, event) in enumerate(schedule.iterrows()):
        if event_index not in selected_events:
            continue
        session = fastf1.get_session(year, event['OfficialEventName'], 'Q')
        session.load()
        lap = session.laps.pick_fastest()
        tel = lap.get_telemetry()

        x = np.array(tel['X'].values)
        y = np.array(tel['Y'].values)

        sec12, sec23 = getSectorDistances(session)
        sectors = getSectorsForTel(tel, sec12, sec23)

        points = np.array([x, y]).T.reshape(-1, 1, 2)
        segments = np.concatenate([points[:-1], points[1:]], axis=1)

        for s in [1,2,3]:
            if not np.any(np.logical_and(selected_events == event_index, selected_sectors == s)):
                continue
            sector_index = event_index*3 + s
            sector_cut = np.where(sectors == s)[0]

            sector_dict[sector_index] = {
                'sector' : s,
                'event_index' : event_index,
                'df_index' : df_index,
                'points' : points[sector_cut]
            }
        
    return sector_dict

def save_multi_image(filename):
    pp = PdfPages(filename)
    fig_nums = plt.get_fignums()
    figs = [plt.figure(n) for n in fig_nums]
    for fig in figs:
        fig.savefig(pp, format='pdf')
    pp.close()

if __name__ == '__main__':
    plt.close('all')

    year = 2022

    selected_sectors = None#[1,2,3]#[1,1,1]
    selected_events = None#[0,0,0]#[0,1,2]
    limit_sectors = 100

    # Get relevant sector information
    sector_dict = getSectorDict(year, limit_sectors=limit_sectors, selected_events=selected_events, selected_sectors=selected_sectors)


    # Determine order to append sectors
    sector_order = np.random.choice(list(sector_dict.keys()), size=len(sector_dict.keys()), replace=False)

    # Split into smaller tracks for fun
    sector_orders = np.reshape(sector_order,(-1,6))

    for sector_order in sector_orders:
        # Loop over sectors, transform and append them to end of track.
        for i, sector_index in enumerate(sector_order):
            #Ignoring rotations right now, but ultimately want to align segments to flow. 
            if i == 0:
                # Initiate coordinates and colors. 
                x_all = sector_dict[sector_order[i]]['points'][:,:,0] - sector_dict[sector_order[i]]['points'][:,:,0][0] # Align end point to origin
                y_all = sector_dict[sector_order[i]]['points'][:,:,1] - sector_dict[sector_order[i]]['points'][:,:,1][0] # Align end point to origin
                c = np.ones(len(sector_dict[sector_order[i]]['points'][:,:,0]))*(sector_dict[sector_index]['event_index'] + 1)
                
            else:
                # Obtain untransformed new sector coordinates.  
                x_new = sector_dict[sector_order[i]]['points'][:,:,0] - sector_dict[sector_order[i]]['points'][:,:,0][0] # Align initial point to origin
                y_new = sector_dict[sector_order[i]]['points'][:,:,1] - sector_dict[sector_order[i]]['points'][:,:,1][0] # Align initial point to origin
                
                # Obtain track direction vectors for connecting points
                old_vector = np.array([x_all[-1] , y_all[-1]]) - np.array([x_all[-2] , y_all[-2]])
                old_vector = old_vector.flatten()/np.linalg.norm(old_vector)
                
                new_vector = np.array([x_new[1] , y_new[1]]) - np.array([x_new[0] , y_new[0]])
                new_vector = new_vector.flatten()/np.linalg.norm(new_vector)

                # Calculate angle between vectors, calculate rotation matrix values, rotate      
                angle_rad = np.arctan2( new_vector[0]*old_vector[1] - new_vector[1]*old_vector[0], new_vector[0]*old_vector[0] + new_vector[1]*old_vector[1] )
                cos, sin = np.cos(angle_rad), np.sin(angle_rad)

                x_rotated = cos * x_new - sin * y_new + x_all[-1] # Rotate and shift to end of track
                y_rotated = sin * x_new + cos * y_new + y_all[-1] # Rotate and shift to end of track

                if False:
                    # Sector by sector plotting for debugging.  Shows appending of sectors, and vectors. 
                    fig = plt.figure()
                    
                    plt.plot(x_all, y_all, c='b',label='Previous Sectors')
                    plt.quiver(x_all[-1], y_all[-1], old_vector[0], old_vector[1], color='b', label='Ending Vector for Previous Sectors')
                    
                    plt.plot(x_new + x_all[-1], y_new + y_all[-1], c='g',label='Sector %i'%(sector_order[i]))
                    plt.quiver(x_new[0] + x_all[-1],  y_new[0] + y_all[-1], new_vector[0], new_vector[1], color='g', label='Beginning Vector for Sector %i'%(sector_order[i]))
                    
                    plt.plot(x_rotated, y_rotated, c='r',label='Rotated Sector %i'%(sector_order[i]))

                    plt.legend()

                    plt.show(fig)
                
                # Append
                x_all = np.append(x_all, x_rotated)
                y_all = np.append(y_all, y_rotated)
                c = np.append(c, np.ones(len(sector_dict[sector_order[i]]['points'][:,:,0]))*(sector_dict[sector_index]['event_index'] + 1))
                
        # Reshape points for segment calculation.
        points = np.array([x_all, y_all]).T.reshape(-1, 1, 2)
        segments = np.concatenate([points[:-1], points[1:]], axis=1)

        # Plot
        fig = plt.figure()
        ax = plt.gca()
        plt.axis('equal')
        plt.title('Random Walk Track\n{} Track Sectors Used from {} Unique Circuits'.format(len(sector_order), len(np.unique(c))))

        cmap = cm.get_cmap('rainbow')
        norm = plt.Normalize(vmin=0, vmax=max(c))
        lc_comp = LineCollection(segments, norm=norm, cmap=cmap)
        lc_comp.set_array(c)
        lc_comp.set_linewidth(4)

        plt.gca().add_collection(lc_comp)
        ax.set_aspect('equal', 'box')
        plt.tick_params(labelleft=False, left=False, labelbottom=False, bottom=False)

        cbar = plt.colorbar(mappable=lc_comp, label="Source Race Index", boundaries=np.arange(-0.5, max(c) + 2))
        cbar.set_ticks(np.arange(0, max(c) + 2))

        
        # Label start and finish locations

        x_range = max(x_all) - min(x_all)
        y_range = max(y_all) - min(y_all)
        plt.xlim(min(x_all)-0.1*x_range, max(x_all)+0.1*x_range)
        plt.ylim(min(y_all)-0.1*y_range, max(y_all)+0.1*y_range)

        if x_all[0] > x_all[1]:
            x_offset = -0.05*x_range
        else:
            x_offset = 0.05*x_range
        
        if y_all[0] > y_all[1]:
            y_offset = 0.05*y_range
        else:
            y_offset = -0.05*y_range
            
        plt.annotate('Start', (x_all[0], y_all[0]), (x_all[0] + x_offset, y_all[0] + y_offset), xycoords='data')


        if x_all[-2] > x_all[-1]:
            x_offset = -0.05*x_range
        else:
            x_offset = 0.05*x_range

        if y_all[-2] > y_all[-1]:
            y_offset = -0.05*y_range
        else:
            y_offset = 0.05*y_range
            
        plt.annotate('Finish', (x_all[-1], y_all[-1]), (x_all[-1] + x_offset, y_all[-1] + y_offset), xycoords='data')


    filename = "combined_tracks.pdf"
    save_multi_image(filename)
