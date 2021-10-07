"""handlers.py: A set of classes for working with data of different formats."""

import csv
import sqlite3
import xml.etree.ElementTree as et

import json_stream


class BaseHandler:
    """Base class that only provides common attributes to its subclasses."""
    def __init__(self, file_path: str, fields: tuple):
        self.file_path = file_path
        self.fields = fields


class HeaderType:
    """Configures in/out data dimensionality: n,m (X1..Xn, Y1..Ym) and name literals for X,Y.

    Stores the specified parameters or generates them based on the given file header."""
    def __init__(self, first_lit: str = None, num_first: int = None,
                       second_lit: str = None, num_second: int = None):
        # Feature group 1 (X1..Xn)
        self.first_lit = first_lit
        self.num_first = num_first
        # Feature group 2 (Y1..Ym)
        self.second_lit = second_lit
        self.num_second = num_second
        if (self.first_lit and self.second_lit and self.num_first and self.num_second):
            # self.fields example: (('X1', 'X2', 'X3'),('Y1', 'Y2', 'Y3'))
            # and self.plain_fields: ('X1', 'X2', 'X3','Y1', 'Y2', 'Y3')
            self.make_heading()

    def make_heading(self):
        """Creates parameters for easy iteration."""
        first = []
        second = []
        for i in range(self.num_first):
            first.append(self.first_lit + str(i + 1))
        for i in range(self.num_second):
            second.append(self.second_lit + str(i + 1))
        self.fields = tuple(first), tuple(second)
        self.plain_fields = tuple(first) + tuple(second)

    """Generates fields based on the given file."""
    def get_header_from_csv_head(self, file_path, *args):
        # not a big deal
        raise NotImplementedError


class CsvInputHandler(BaseHandler):
    """
    Receives data from a CSV file.

    Places it in the desired order, filtering out unnecessary fields.
    """
    def __init__(self, file_path: str, fields: tuple, **fmtparams):
        self.fmtparams = fmtparams
        super().__init__(file_path, fields)

    """Yields rows from the given file as dict incrementally."""
    def get_row_gen(self):
        with open(self.file_path, newline='') as csv_input:
            dr = csv.DictReader(csv_input, **self.fmtparams)
            # heading already grabbed by reader, start read data
            for d in dr:
                # delete unnecessary data and order as required
                # X1,X2..Xn
                nice_data = tuple(d[key] for key in self.fields[0] + self.fields[1])
                yield nice_data


class XmlInputHandler(BaseHandler):
    """Yields "rows" from the given xml file as dict incrementally."""
    def get_row_gen(self):
        """Yields "rows" from the given xml file as dict incrementally."""
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
                nice_data = tuple(data[key] for key in self.fields[0] + self.fields[1])
                yield nice_data


class JsonInputHandler(BaseHandler):
    """Yields "rows" from the given json file as tuple incrementally."""
    def get_row_gen(self):
        """Yields "rows" from the given json file as tuple incrementally."""
        with open(self.file_path, newline='') as json_input:
            # array start can be found somewhere in the first QTY charachters
            QTY = 100
            array_start = json_input.read(QTY).find('[')
            json_input.seek(array_start)
            for d in json_stream.stream_array(json_stream.tokenize(json_input)):
                nice_data = tuple(str(d[key]) for key in self.fields[0] + self.fields[1])
                yield nice_data


class CsvWriter(BaseHandler):
    """Gets iterable and inserts its items in the given .csv file."""
    def __init__(self, file_path: str, fields: list, **fmtparams):
        self.fmtparams = fmtparams
        super().__init__(file_path, fields)

    def write(self, it, aliases=None):
        """Writes data from iterable incrementally."""
        aliases = aliases if aliases else self.fields[0] + self.fields[1]
        with open(self.file_path, 'a', newline='') as csv_output:
            writer = csv.writer(csv_output, **self.fmtparams)
            writer.writerow(aliases)
            for row in it:
                writer.writerow(row)


class BaseDb(BaseHandler):
    """Provides methods for a very basic SQL injection prevention."""
    @staticmethod
    def validate_data(row):
        for item in row:
            BaseDb._validate_sql(item)

    def validate_fields(self):
        for item in self.fields[0] + self.fields[1]:
            BaseDb._validate_sql(item)

    def _validate_sql(x):
        for item in ("--", "/**/", ";"):
            if item in x:
                print('evil detected')
                raise Exception


class DbWriter(BaseDb):
    """Gets iterable and inserts its items in the given database."""
    def __init__(self, file_path: str, fields: list):
        # Commit when exceeded to reduce RAM usage
        self.insert_counter_limit = 1000
        super().__init__(file_path, fields)

    def create_table(self):
        """Creates table."""
        con = sqlite3.connect(self.file_path)
        cur = con.cursor()
        self.validate_fields()
        columns = ''
        for col in self.fields[0]:
            columns += ' %s text, ' % col
        for col in self.fields[1]:
            columns += ' %s integer, ' % col
        # remove last comma
        columns = columns[:-2]
        cur.execute('DROP TABLE IF EXISTS important_data')
        cur.execute(f'CREATE TABLE important_data ({columns})')
        con.close()

    def write(self, it):
        """Writes data from iterable incrementally."""
        con = sqlite3.connect(self.file_path)
        cur = con.cursor()
        op_counter = 0
        for row in it:
            try:
                self.validate_data(row)
            except Exception:
                from pprint import pprint
                pprint(list(row))
                continue
            else:
                n = len(self.fields[0]) + len(self.fields[1])
                columns = ' ?, ' * n
                # remove last comma
                columns = columns[:-2]
                cur.execute(f"""INSERT INTO important_data
                        VALUES ({columns})""", row)
                op_counter += 1
            # reduce RAM usage
            if op_counter == self.insert_counter_limit:
                con.commit()
                op_counter = 0
        con.commit()
        con.close()


class DbQuery(BaseDb):
    """Makes SQL queries and provides results incrementally."""
    def make_basic_query(self):
        """Yields results of the SQL query incrementally."""
        con = sqlite3.connect(self.file_path)
        cur = con.cursor()
        sql_basic = 'SELECT * FROM "important_data" ORDER BY %s' % self.fields[0][0]
        for row in cur.execute(sql_basic):
            yield row

    def make_advanced_query(self):
        """Yields results of the SQL query incrementally."""
        con = sqlite3.connect(self.file_path)
        cur = con.cursor()

        first_cols = second_cols = ''
        for col in self.fields[0]:
            first_cols += f'"{col}", '
        # remove last comma
        first_cols = first_cols[:-2]
        for col in self.fields[1]:
            second_cols += f'SUM("{col}"), '
        # remove last comma
        second_cols = second_cols[:-2]

        sql_advanced = f"""SELECT {first_cols}, {second_cols}
                  FROM "important_data"
                  GROUP BY {first_cols}
                  ORDER BY {first_cols}"""
        for row in cur.execute(sql_advanced):
            yield row
