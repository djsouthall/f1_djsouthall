## Preface
This repo serves as playground for working with F1 related data.  In particular I focus on answering some interesting problems that benefit from the use of SQL and machine learning.

## Machine Learning

Please check out the Jupyter notebooks [guess_track_from_telem](https://github.com/djsouthall/f1_djsouthall/blob/main/ml/guess_track_from_telem.ipynb) and [guess_driver_from_telem](https://github.com/djsouthall/f1_djsouthall/blob/main/ml/guess_driver_from_telem.ipynb).  In these notebooks I utlize *keras* and *tensorflow* to identify Formula 1 circuits and drivers based on speed v.s. time race telemetry.  

### Track Identification Preview

[Jupyter Notebook](https://github.com/djsouthall/f1_djsouthall/blob/main/ml/guess_track_from_telem.ipynb)

At present the identification of tracks based on telemetry is extremely accurate.  Speed traces are given to the model, which selects among 22 possible tracks to predict the correct source of the given lap.  Below I show example laps which were processed by the model for the Austrian Grand Prix, with the second figure (below) showing similar data for the Japanese Grand Prix.  The Japanese Grand Prix is the only track currently containing laps incorrectly identified, which is caused by a small subset of the laps which were likely incorrectly identified as green flag (normal) running, and likely should have been ignored as being behind a safety car.  The regular accelerating and braking without achieving top speeds is indicative of a driver warming their tires and brakes, and should not be present in a normal racing lap.  These incorrectly identified laps are shown in red below.  Another interesting study that could be conducted with these traces would be to identify weather or track conditions based on telemetry. 

#### Austrian GP
<img src="https://github.com/djsouthall/f1_djsouthall/blob/main/examples/track_identification/austrian_grand_prix.svg?raw=true" alt="Austrian GP" width="1000"/>

#### Japanese GP
<img src="https://github.com/djsouthall/f1_djsouthall/blob/main/examples/track_identification/japanese_grand_prix.svg?raw=true" alt="Japanese GP" width="1000"/>

### Driver Identification Preview

[Jupyter Notebook](https://github.com/djsouthall/f1_djsouthall/blob/main/ml/guess_driver_from_telem.ipynb)

Driver identification is much more difficult than track identification, as the subtle differences in driver styles is far smaller than the absolute differences in track layouts.  Because of this the current achieved accuracy for drivers based on traces is only ~90%.  Though this is not perfect, it is remarkable considering the fact that there are 20 drivers accounted for in the model, all driving at the top tier of their sport in relatively similar hardware.  Certainly I would not be able to do this well by eye.  Below is a plot showing some laps that were correctly identified as their driver (each driver is represented by a distinct color), compared to laps that were incorrectly identified.  This plotted sample consists of 20 drivers, and shows how small differences are.

<img src="https://github.com/djsouthall/f1_djsouthall/blob/main/examples/driver_identification/lap_id.svg?raw=true" alt="Sorted Driver Laps" width="1000"/>



## SQL

I have also been playing with the fastf1 tool to scrape other forms of F1 race data and store this data into SQL databases.  Please take a look at [f1_djsouthall/sql/make_tables.py](https://github.com/djsouthall/f1_djsouthall/tree/main/sql/make_tables.py) to see the generation of these SQL databases.  In [f1_djsouthall/sql/example_sql_analysis.py](https://github.com/djsouthall/f1_djsouthall/tree/main/sql/example_sql_analysis.py) I perform a few small analysis examples using those tables.

## Track Appending / Random Walk

I also thought it would be fun to play with generating new tracks by randomly combined sectors from existing tracks.  You can see some of that below:

[Jupyter Notebook](https://github.com/djsouthall/f1_djsouthall/blob/main/examples/track_random_walk.ipynb)

<img src="https://github.com/djsouthall/f1_djsouthall/blob/main/examples/combined_tracks/combined_track_0.svg?raw=true" alt="Example 0" height="400"/> <img src="https://github.com/djsouthall/f1_djsouthall/blob/main/examples/combined_tracks/combined_track_1.svg?raw=true" alt="Example 1" height="400"/> <img src="https://github.com/djsouthall/f1_djsouthall/blob/main/examples/combined_tracks/combined_track_4.svg?raw=true" alt="Example 4" height="400"/>

## Thanks

The foundation of this work is the data I have extracted using the brilliant [FastF1](https://github.com/theOehrly/Fast-F1) API.  It really is a great tool, so please check it out if interested!
