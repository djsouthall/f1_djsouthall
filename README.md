## Preface
This repo serves as playground for working with F1 related data.  In particular I focus on answering some interesting problems that benefit from the use of SQL and machine learning.

## Machine Learning

Please check out the Jupyter notebooks [guess_track_from_telem](https://github.com/djsouthall/f1_djsouthall/blob/main/ml/guess_track_from_telem.ipynb) and [guess_driver_from_telem](https://github.com/djsouthall/f1_djsouthall/blob/main/ml/guess_driver_from_telem.ipynb).  In these notebooks I utlize *keras* and *tensorflow* to identify Formula 1 circuits and drivers based on speed v.s. time race telemetry.

## SQL

I have also been playing with the fastf1 tool to scrape other forms of F1 race data and store this data into SQL databases.  Please take a look at [f1_djsouthall/sql/make_tables.py](https://github.com/djsouthall/f1_djsouthall/tree/main/sql/make_tables.py) to see the generation of these SQL databases.  In [f1_djsouthall/sql/example_sql_analysis.py](https://github.com/djsouthall/f1_djsouthall/tree/main/sql/example_sql_analysis.py) I perform a few small analysis examples using those tables.

## Thanks

The foundation of this work is the data I have extracted using the brilliant [FastF1](https://github.com/theOehrly/Fast-F1) API.  It really is a great tool, so please check it out if interested!
