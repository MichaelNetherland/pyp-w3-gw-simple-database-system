import os
from datetime import date

from simple_database.exceptions import ValidationError
from simple_database.config import BASE_DB_FILE_PATH
import json


class Row(object):
    def __init__(self, row):
        for key, value in row.items():
            setattr(self, key, value)


class Table(object):

    def __init__(self, db, name, columns=None):
        self.db = db
        self.name = name

        self.table_filepath = os.path.join(BASE_DB_FILE_PATH, self.db.name,
                                           '{}.json'.format(self.name))
        
        
        # In case the table JSON file doesn't exist already, you must
        # initialize it as an empty table, with this JSON structure:
        # {'columns': columns, 'rows': []}
        
        if not self.name:
            self.name = {'columns': columns, 'rows': []}
        
        self.columns = columns or self._read_columns()

    def _read_columns(self):
        # Read the columns configuration from the table's JSON file
        # and return it.
        
        with open(self.table_filepath, 'r') as f:
            idea = json.load(f)
            return idea['columns']
           
    def insert(self, *args):
        # Validate that the provided row data is correct according to the
        # columns configuration.
        # If there's any error, raise ValidationError exception.
        # Otherwise, serialize the row as a string, and write to to the
        # table's JSON file.
        new = []
        dict1 = {}
        if len(args) != len(self.columns):
            raise ValidationError('Invalid amount of field')
        
        for arg in args:
            for col in self.columns:
                if not type(arg) == eval(col['type']):
                    raise ValidationError('Invalid type of field "{}": Given "{}", expected "{}"').format(col['name'], type(arg), col['type'])
        
        for col in self.columns:
            new.append(col['name'])
        zipped = zip(new, args)
        for k,v in zipped:
            dict1[k] = v
        with open(self.table_filepath, 'r+') as f:
            data = json.load(f)
            data['rows'].append(dict1)
            json.dumps(data, f)
        # https://docs.python.org/3/library/json.html
        # PYTHONPATH=. py.test tests/testfile::TestCase::test_function_nam

# '''
# {
#   'columns': [
#     {'name': 'id', 'type': 'int'},
#     {'name': 'name', 'type': 'str'},
#     {'name': 'birth_date', 'type': 'date'},
#     {'name': 'nationality', 'type': 'str'},
#     {'name': 'alive', 'type': 'bool'},
#     // more column configurations
#   ],
#   'rows': [
#     {
#       'id': 1,
#       'name': 'Jorge Luis Borges',
#       'birth_date': '1899-08-24'  // it's the `isoformat()` of the Date object
#       'nationality': 'ARG',
#       'alive': false
#     },
#     {
#       'id': 2,
#       'name': 'Edgard Alan Poe',
#       'birth_date': '1809-01-19'
#       'nationality': 'USA',
#       'alive': false
#     },
#     // more rows
#   ]
# }
# appgfsdhdfhdf
# '''
    def query(self, **kwargs):
        # Read from the table's JSON file all the rows in the current table
        # and return only the ones that match with provided arguments.
        # We would recomment to  use the `yield` statement, so the resulting
        # iterable object is a generator.

        # IMPORTANT: Each of the rows returned in each loop of the generator
        # must be an instance of the `Row` class, which contains all columns
        # as attributes of the object.
        newlist = []
        with open(self.table_filepath, 'r+') as f:
            data = json.loads(f['rows'])
            for key in data:
                for k,v in key:
                    if v == kwargs[k]:
                        rows = Row(key):
                        yield rows
                    
                

    def all(self):
        # Similar to the `query` method, but simply returning all rows in
        # the table.
        # Again, each element must be an instance of the `Row` class, with
        # the proper dynamic attributes.
        pass

    def count(self):
        # Read the JSON file and return the counter of rows in the table
        pass

    def describe(self):
        # Read the columns configuration from the JSON file, and return it.
        pass


class DataBase(object):
    def __init__(self, name):
        self.name = name
        self.db_filepath = os.path.join(BASE_DB_FILE_PATH, self.name)
        self.tables = self._read_tables()

    @classmethod
    def create(cls, name):
        db_filepath = os.path.join(BASE_DB_FILE_PATH, name)
        # if the db directory already exists, raise ValidationError
        # otherwise, create the proper db directory

    def _read_tables(self):
        # Gather the list of tables in the db directory looking for all files
        # with .json extension.
        # For each of them, instatiate an object of the class `Table` and
        # dynamically assign it to the current `DataBase` object.
        # Finally return the list of table names.
        # Hint: You can use `os.listdir(self.db_filepath)` to loop through
        #       all files in the db directory
        pass

    def create_table(self, table_name, columns):
        # Check if a table already exists with given name. If so, raise
        # ValidationError exception.
        # Otherwise, crete an instance of the `Table` class and assign
        # it to the current db object.
        # Make sure to also append it to `self.tables`
        pass

    def show_tables(self):
        # Return the curren list of tables.
        pass


def create_database(db_name):
    """
    Creates a new DataBase object and returns the connection object
    to the brand new database.
    """
    DataBase.create(db_name)
    return connect_database(db_name)


def connect_database(db_name):
    """
    Connectes to an existing database, and returns the connection object.
    """
    return DataBase(name=db_name)
