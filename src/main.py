#!/usr/bin/env python

"""main.py: ETL data processing in accordance with the conditions of the test task."""

import os
from pathlib import Path

from handlers import DataStructIdent, CsvInputHandler, DbFiller, DbQuery

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

query_b = DbQuery(db_path, ds.fields, sql_basic)
query_it = query_b.get_row_gen()

for row in query_it:
    print(row)
