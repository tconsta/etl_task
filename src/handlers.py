"""handlers.py: A set of classes for working with data of different formats."""

import csv
import sqlite3


class BaseHandler:
    def __init__(self, file_path: str, fields: list) -> None:
        self.file_path = file_path
        self.fields = fields


class DataStructIdent:
    """Stores the received list of fields or generates it based on the file."""
    def __init__(self, file_path: str = None, fields: list = None) -> None:
        self.file_path = file_path
        self.fields = fields if fields else []

    def get_fields_from_csv_head(self, sep=','):
        raise NotImplementedError


class CsvInputHandler(BaseHandler):
    """
    Receives data from a CSV file.

    Places it in the desired order, filtering out unnecessary fields.
    """
    def __init__(self, file_path: str, fields: list, delimiter=',') -> None:
        self.delimiter = delimiter
        super().__init__(file_path, fields)

    def get_row_gen(self):
        with open(self.file_path, newline='') as csv_input:
            reader = csv.DictReader(csv_input, delimiter=self.delimiter)
            # heading already grabbed by reader, start read data
            for row in reader:
                # delete unnecessary data and order as required
                nice_data = {key: row[key] for key in self.fields}
                yield nice_data


class XmlInputHandler(BaseHandler):
    pass


class JsonInputHandler(BaseHandler):
    pass


class CsvWriter(BaseHandler):
    pass


class DbFiller(BaseHandler):

    def __init__(self, file_path: str, fields: list) -> None:
        self.insert_counter_limit = 1000
        super().__init__(file_path, fields)

    def create_table(self):
        con = sqlite3.connect(self.file_path)
        cur = con.cursor()
        cur.execute('''CREATE TABLE IF NOT EXISTS important_data
                       (D2 text,
                        D1 text,
                        D3 text,
                        M1 integer,
                        M2 integer,
                        M3 integer);''')
        con.close()

    def write(self, it):
        con = sqlite3.connect(self.file_path)
        cur = con.cursor()
        op_counter = 0
        for row in it:
            cur.execute("""INSERT INTO important_data
                        VALUES (:D2, :D1, :D3, :M1, :M2, :M3)""", row)
            op_counter += 1
            # reduce RAM usage
            if op_counter == self.insert_counter_limit:
                con.commit()
                op_counter = 0
        con.commit()
        con.close()


class DbQuery(BaseHandler):
    pass
