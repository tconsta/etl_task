#!/usr/bin/env python

"""main.py: ETL data processing in accordance with the conditions of the test task."""

import os
from pathlib import Path

from handlers import (DataStructIdent, CsvInputHandler,
                      XmlInputHandler, JsonInputHandler,
                      CsvWriter, DbFiller, DbQuery)

BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_DIR = os.path.join(BASE_DIR, 'data_input')
OUTPUT_DIR = os.path.join(BASE_DIR, 'data_output')

ds = DataStructIdent(fields=['D2', 'D1', 'D3', 'M1', 'M2', 'M3'])

path1 = os.path.join(INPUT_DIR, 'csv_data_1.csv')
src1 = CsvInputHandler(path1, ds.fields, delimiter=',')
it1_from_csv1 = src1.get_row_gen()


db_path = os.path.join(OUTPUT_DIR, 'quite_a_few_Gb.sqlite3')
db = DbFiller(db_path, ds.fields)
db.create_table()
db.write(it1_from_csv1)

sql_basic = 'SELECT * FROM "important_data" ORDER BY "D1"'
sql_advanced = """SELECT D1, D2, D3, SUM(M1), SUM(M2), SUM(M3)
                  FROM "important_data"
                  GROUP BY D1, D2, D3
                  ORDER BY D1"""

query_basic = DbQuery(db_path, ds.fields, sql_basic)
query_advanced = DbQuery(db_path, ds.fields, sql_advanced)

it_basic = query_basic.get_row_gen()
it_advanced = query_advanced.get_row_gen()

path_basic = os.path.join(OUTPUT_DIR, 'basic_results.tsv')
path_advanced = os.path.join(OUTPUT_DIR, 'advanced_results.tsv')

recv_basic = CsvWriter(path_basic, ds.fields, delimiter='\t')
recv_advanced = CsvWriter(path_advanced, ds.fields, delimiter='\t')

recv_basic.write(it_basic)
recv_advanced.write(it_advanced)
