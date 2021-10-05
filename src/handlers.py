"""handlers.py: A set of classes for working with data of different formats."""


class BaseHandler:
    def __init__(self, file_path: str, fields: list) -> None:
        self.file_path = file_path
        self.fields = fields


class DataStructIdent:
    """Stores the received list of fields or generates it based on the file."""

    def __init__(self, file_path: str = None, fields: list = None) -> None:
        self.file_path = file_path
        self.fields = fields if fields else []

    def get_fields_from_csv_heading(self, sep=','):
        raise NotImplementedError


class CsvInputHandler(BaseHandler):
    pass


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
