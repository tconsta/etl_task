#!/usr/bin/env python

"""main.py: ETL data processing in accordance with the conditions of the test task."""

import os
from pathlib import Path
import itertools

from handlers import (HeaderType, CsvInputHandler,
                      XmlInputHandler, JsonInputHandler,
                      CsvWriter, DbFiller, DbQuery)

BASE_DIR = Path(__file__).resolve().parent.parent
# Extract data from
INPUT_DIR = os.path.join(BASE_DIR, 'data_input')
# Load data to
OUTPUT_DIR = os.path.join(BASE_DIR, 'data_output')

# Define input/output data specifics
domain_obj = HeaderType('D', 3, 'M', 3)

# Data source 1
path1 = os.path.join(INPUT_DIR, 'csv_data_1.csv')
src1 = CsvInputHandler(path1, domain_obj.fields)
it1_from_csv1 = src1.get_row_gen()

# Data source 2
path2 = os.path.join(INPUT_DIR, 'csv_data_2.csv')
src2 = CsvInputHandler(path2, domain_obj.fields)
it2_from_csv2 = src2.get_row_gen()

# Data source 3
path3 = os.path.join(INPUT_DIR, 'json_data.json')
src3 = JsonInputHandler(path3, domain_obj.fields)
it3_from_json = src3.get_row_gen()

# Data source 4
path4 = os.path.join(INPUT_DIR, 'xml_data.xml')
src4 = XmlInputHandler(path4, domain_obj.fields)
it4_from_xml = src4.get_row_gen()

# Combine all sources
all_sources_it = itertools.chain(it1_from_csv1, it2_from_csv2,
                                 it3_from_json, it4_from_xml)

# Intermediate results: database
db_path = os.path.join(OUTPUT_DIR, 'quite_a_few_Gb.sqlite3')
db = DbFiller(db_path, domain_obj.fields)
db.create_table()
db.write(all_sources_it)

sql_basic = 'SELECT * FROM "important_data" ORDER BY "D1"'
sql_advanced = """SELECT D1, D2, D3, SUM(M1), SUM(M2), SUM(M3)
                  FROM "important_data"
                  GROUP BY D1, D2, D3
                  ORDER BY D1"""

query_basic = DbQuery(db_path, domain_obj.fields, sql_basic)
query_advanced = DbQuery(db_path, domain_obj.fields, sql_advanced)

it_basic = query_basic.get_row_gen()
it_advanced = query_advanced.get_row_gen()

# Final results
path_basic = os.path.join(OUTPUT_DIR, 'basic_results.tsv')
path_advanced = os.path.join(OUTPUT_DIR, 'advanced_results.tsv')

recv_basic = CsvWriter(path_basic, domain_obj.fields, delimiter='\t')
recv_advanced = CsvWriter(path_advanced, domain_obj.fields, delimiter='\t')

recv_basic.write(it_basic)

adv_aliases = ('D1', 'D2', 'D3', 'MS1', 'MS2', 'MS3')
recv_advanced.write(it_advanced, aliases=adv_aliases)
