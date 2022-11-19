'''
A script for playing with fastf1 package for the first time.
Specifically this plots the telemetry data of Checo during the Monaco 2022 GP
to illustrate throttle irregularities on his crashed lap.
'''
from matplotlib import pyplot as plt
import numpy as np
import os

import fastf1
import fastf1.plotting
fastf1.Cache.enable_cache(os.environ['f1_cache'])  
fastf1.plotting.setup_mpl() # Sets default matplotlib settings based on tastes of fast f1 devs.

plt.ion()

if __name__ == '__main__':
    plt.close('all')
    track = 'Monaco' #Set GP
    year = 2022 #Set Year
    grandprix = '%s %i'%(track, year)

    # Load session data
    session = fastf1.get_session(year, track, 'Q') #Call qualifying session
    session.load()

    # Get Checo's data
    all_perez = session.laps.pick_driver('PER')
    fastest_perez = all_perez.pick_fastest()

    # Prepare figure
    speed_fig = plt.figure()
    speed_ax = plt.subplot(2,1,1)
    throttle_ax = plt.subplot(2,1,2, sharex=speed_ax)
    

    max_lap_time = max(all_perez.pick_fastest().get_car_data()['Time'])

    for lap_index, lap in all_perez.iterlaps():
        # telem_data = lap.get_telemetry()
        # pos_data = lap.get_pos_data()
        car_data = lap.get_car_data()

        t = car_data['Time']
        t_sec = t.dt.total_seconds()
        vCar = car_data['Speed']
        throttle = car_data['Throttle']

        if max(vCar[np.logical_and(t_sec < 11, t_sec >8)]) < 265:
            #Filtering out laps with insufficient 1st straight speeds, so likely not push laps. 
            continue
        elif lap_index == fastest_perez.name:
            # Highlight and label fastest lap. 
            speed_ax.plot(t, vCar, c='r', label='[Fastest] Quali Lap %i'%(lap_index + 1))
            throttle_ax.plot(t, throttle, c='r', label='[Fastest] Quali Lap %i'%(lap_index + 1))
        elif lap_index == len(all_perez) - 1:
            # Highlight and label incident lap. 
            speed_ax.plot(t, vCar, c='dodgerblue', label='[Final] Quali Lap %i'%(lap_index + 1))
            throttle_ax.plot(t, throttle, c='dodgerblue', label='[Final] Quali Lap %i'%(lap_index + 1))
        else:
            # Other likely push laps
            speed_ax.plot(t, vCar, alpha=0.25, linestyle='--', label='Quali Lap %i'%(lap_index + 1))
            throttle_ax.plot(t, throttle, alpha=0.25, linestyle='--', label='Quali Lap %i'%(lap_index + 1))        
    
    # Finalize figure settings
    speed_ax.set_xlim(0, max_lap_time*1.1)
    throttle_ax.set_xlabel('Time')
    speed_ax.set_ylabel('Speed [Km/h]')
    throttle_ax.set_ylabel('Throttle [%]')
    speed_ax.set_title('Perez Qualifying Telem\n%s'%grandprix)
    throttle_ax.legend()
