#import sys
#sys.settrace()
import pyantiML
from multiprocessing import Pool
import numpy as np
import multiprocessing
from multiprocessing import Manager
from functools import partial
from itertools import repeat
import gc
from pathlib import Path
#import pandas as pd

### Examples

# Creates an antiML Object 
#antiML_obj = pyantiML.antiML.from_csv('name_of_file', sep = '|')

#Pandas Dataframe of antiML Object
#antiML_obj.transactions

#Unique IDs
#antiML_obj.unique_ids

#Finds all suspect bridge transactions pair for a given ID and returns Pandas DF
#antiML_obj.one_bridge_id_pd(, single_id)

#Writes a CSV of all suspect bridge transactions for a list of given IDS
#antiML_obj.sus_bridges_pd(list_of_ids)


