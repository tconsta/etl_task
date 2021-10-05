#!/usr/bin/env python

"""main.py: ETL data processing in accordance with the conditions of the test task."""

import os
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
INPUT_DIR = os.path.join(BASE_DIR, 'data_input')
OUTPUT_DIR = os.path.join(BASE_DIR, 'data_output')
