"""handlers.py: A set of classes for working with data of different formats."""

import csv


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
    pass


class DbQuery(BaseHandler):
    pass
