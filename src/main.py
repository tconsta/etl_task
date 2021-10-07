#!/usr/bin/env python

"""main.py: ETL data processing in accordance with the test task."""

import os
from pathlib import Path
import itertools
import copy
import logging

from handlers import (HeaderType, CsvInputHandler,
                      XmlInputHandler, JsonInputHandler,
                      CsvWriter, DbWriter, DbQuery)

BASE_DIR = Path(__file__).resolve().parent.parent
# Extract data from
INPUT_DIR = os.path.join(BASE_DIR, 'data_input')
# Load data to
OUTPUT_DIR = os.path.join(BASE_DIR, 'data_output')

# Logging
log_path = os.path.join(OUTPUT_DIR, 'etl_log.log')
log_format = "%(asctime)s - %(levelname)s - %(module)s: %(lineno)d - %(message)s"
logging.basicConfig(level='DEBUG', format=log_format)
# logging.basicConfig(level='DEBUG', format=log_format, filename=log_path)
log = logging.getLogger('ETL_logger')

# Define input/output data specifics
domain_obj = HeaderType('D', 3, 'M', 3)

# Data source 1
path1 = os.path.join(INPUT_DIR, 'csv_data_1.csv')
src1 = CsvInputHandler(path1, domain_obj.fields)
it1_from_csv1 = src1.get_row_gen()

# # Data source 2
path2 = os.path.join(INPUT_DIR, 'csv_data_2.csv')
src2 = CsvInputHandler(path2, domain_obj.fields)
it2_from_csv2 = src2.get_row_gen()

# Data source 3
path3 = os.path.join(INPUT_DIR, 'json_data.json')
src3 = JsonInputHandler(path3, domain_obj.fields)
it3_from_json = src3.get_row_gen()

# # Data source 4
path4 = os.path.join(INPUT_DIR, 'xml_data.xml')
src4 = XmlInputHandler(path4, domain_obj.fields)
it4_from_xml = src4.get_row_gen()

# Combine all sources
all_sources_it = itertools.chain(it1_from_csv1, it2_from_csv2,
                                 it3_from_json, it4_from_xml)

# Intermediate results: database
db_path = os.path.join(OUTPUT_DIR, 'quite_a_few_Gb.sqlite3')
db = DbWriter(db_path, domain_obj.fields)
db.create_table()
db.write(all_sources_it)

query = DbQuery(db_path, domain_obj.fields)
it_basic = query.make_basic_query()
it_advanced = query.make_advanced_query()

# Final results
path_basic = os.path.join(OUTPUT_DIR, 'basic_results.tsv')
path_advanced = os.path.join(OUTPUT_DIR, 'advanced_results.tsv')

recv_basic = CsvWriter(path_basic, domain_obj.fields, delimiter='\t')
recv_advanced = CsvWriter(path_advanced, domain_obj.fields, delimiter='\t')

# Create a header for the advanced query
# based on the structure of an existing object
aliased = copy.deepcopy(domain_obj)
aliased.second_lit = 'MS'
aliased.make_heading()

recv_basic.write(it_basic)
recv_advanced.write(it_advanced, aliases=aliased.plain_fields)
