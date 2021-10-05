#!/usr/bin/env python

"""main.py: ETL data processing in accordance with the conditions of the test task."""

import os
from pathlib import Path

from handlers import DataStructIdent, CsvInputHandler

BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_DIR = os.path.join(BASE_DIR, 'data_input')
OUTPUT_DIR = os.path.join(BASE_DIR, 'data_output')

ds = DataStructIdent(fields=['D1', 'D2', 'D3', 'M1', 'M2', 'M3'])

path1 = os.path.join(INPUT_DIR, 'csv_data_1.csv')
src1 = CsvInputHandler(path1, ds.fields, delimiter=',')

for row in src1.get_data_row_gen():
    print(row)
