'''
A script for playing with fastf1 package for the first time. 
'''
from matplotlib import pyplot as plt
import numpy as np
import os

import fastf1
import fastf1.plotting
fastf1.Cache.enable_cache(os.environ['f1-cache'])  
fastf1.plotting.setup_mpl()

plt.ion()

if __name__ == '__main__':
    plt.close('all')
    track = 'Monaco'
    year = 2022
    grandprix = '%s %i'%(track, year)
    session = fastf1.get_session(year, track, 'Q')

    session.load()
    all_perez = session.laps.pick_driver('PER')
    fastest_perez = all_perez.pick_fastest()

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
            speed_ax.plot(t, vCar, c='r', label='[Fastest] Quali Lap %i'%(lap_index + 1))
            throttle_ax.plot(t, throttle, c='r', label='[Fastest] Quali Lap %i'%(lap_index + 1))
        elif lap_index == len(all_perez) - 1:
            speed_ax.plot(t, vCar, c='dodgerblue', label='[Final] Quali Lap %i'%(lap_index + 1))
            throttle_ax.plot(t, throttle, c='dodgerblue', label='[Final] Quali Lap %i'%(lap_index + 1))
        else:
            speed_ax.plot(t, vCar, alpha=0.25, linestyle='--', label='Quali Lap %i'%(lap_index + 1))
            throttle_ax.plot(t, throttle, alpha=0.25, linestyle='--', label='Quali Lap %i'%(lap_index + 1))        
    
    speed_ax.set_xlim(0, max_lap_time*1.1)
    throttle_ax.set_xlabel('Time')
    speed_ax.set_ylabel('Speed [Km/h]')
    throttle_ax.set_ylabel('Throttle [%]')
    speed_ax.set_title('Perez Qualifying Telem\n%s'%grandprix)
    throttle_ax.legend()
