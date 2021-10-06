"""handlers.py: A set of classes for working with data of different formats."""

import csv
import sqlite3
import xml.etree.ElementTree as et


class BaseHandler:
    """Base class that only provides common attributes."""
    def __init__(self, file_path: str, fields: list) -> None:
        self.file_path = file_path
        self.fields = fields


class DataStructIdent:
    """Stores the received list of fields or generates it based on the file."""
    def __init__(self, file_path: str = None, fields: list = None) -> None:
        self.file_path = file_path
        self.fields = fields if fields else []

    """Generates list of fields based on the given file."""
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

    """Yields rows from the given file as dict incrementally."""
    def get_row_gen(self):
        with open(self.file_path, newline='') as csv_input:
            reader = csv.DictReader(csv_input, delimiter=self.delimiter)
            # heading already grabbed by reader, start read data
            for row in reader:
                # delete unnecessary data and order as required
                nice_data = {key: row[key] for key in self.fields}
                yield nice_data


class XmlInputHandler(BaseHandler):

    """Yields "rows" from the given file as dict incrementally."""
    def get_row_gen(self):
        data = {}
        key = None

        context = et.iterparse(self.file_path, events=("start", "end"))
        for ev, elem in context:
            if ev == 'start' and elem.tag == 'objects':
                data = {}
            if ev == 'start' and elem.tag == 'object':
                key = elem.attrib['name']
            if ev == 'start' and elem.tag == 'value':
                val = elem.text
                data[key] = val
            if ev == 'end' and elem.tag == 'objects':
                nice_data = {key: data[key] for key in self.fields}
                yield nice_data


class JsonInputHandler(BaseHandler):

    """Yields "rows" from the given file as dict incrementally."""
    def get_row_gen(self):
        pass


class CsvWriter(BaseHandler):
    """Gets iterable and inserts its items in the given .csv file."""
    def __init__(self, file_path: str, fields: list, **fmtparams) -> None:
        self.fmtparams = fmtparams
        super().__init__(file_path, fields)

    def write(self, it):
        """Writes data from iterable incrementally."""
        with open(self.file_path, 'a', newline='') as csv_output:
            writer = csv.DictWriter(csv_output, fieldnames=self.fields,
                                    **self.fmtparams)
            writer.writeheader()
            for row in it:
                writer.writerow(row)


class DbFiller(BaseHandler):
    """Gets iterable and inserts its items in the given database."""
    def __init__(self, file_path: str, fields: list) -> None:
        self.insert_counter_limit = 1000
        super().__init__(file_path, fields)

    def create_table(self):
        """Creates table if not exists."""
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
        """Writes data from iterable incrementally."""
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
    """Makes SQL queries and provides results incrementally."""
    def __init__(self, file_path: str, fields: list, sql_query: str) -> None:
        self.query = sql_query
        super().__init__(file_path, fields)

    def get_row_gen(self):
        """Yields results of the SQL query as dict incrementally."""
        con = sqlite3.connect(self.file_path)
        cur = con.cursor()
        for row in cur.execute(self.query):
            yield {key: val for key, val in zip(self.fields, row)}
