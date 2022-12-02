'''
This will pkl the relevant lap data for us in ML testing.

These can later be loaded with tools.fastf1_tools.saveTelemForYear
'''
import os
import sys
sys.path.append(os.environ['f1_install'])
from tools.fastf1_tools import saveTelemForYear

if __name__ == '__main__':
    saveTelemForYear(2022, outpath=os.path.join(os.environ['f1_install'], 'dataframes'), n_samples=2048, sample_interval_seconds=0.1, telem_param='Speed')
