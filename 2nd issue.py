import dict  # external library for hash table

class MiniDB:
    def __init__(self):
        self.tables = {}  # dictionary of table objects
        self.meta_tables = {}  # dictionary of meta_table objects
    
    def create_table(self, name, columns):
        # add support for declaring a column as unique
        for column in columns:
            if column['unique']:
                column['index'] = 'BTree'  # set index type to BTree for unique columns
        # create new table and add it to the dictionary of tables
        self.tables[name] = Table(name, columns)
        # add table metadata to the dictionary of meta_tables
        self.meta_tables[name] = MetaTable(name, self.tables[name].get_primary_key())
    
    def create_index(self, table_name, column_name):
        # get the specified table object and the column object
        table = self.tables[table_name]
        column = table.get_column(column_name)
        # check if the column is unique or a primary key
        if column.unique or column.primary_key:
            if column.index == 'BTree':
                # create a BTree index over the column
                index = BTreeIndex(table, column)
                # add the index to the table object and the meta_table object
                table.add_index(index)
                self.meta_tables[table_name].add_index(index)
            elif column.index == 'Hash':
                # create a Hash index over the column
                index = HashIndex(table, column)
                # add the index to the table object and the meta_table object
                table.add_index(index)
                self.meta_tables[table_name].add_index(index)
            else:
                print(f"Error: Index type not supported for column {column_name}")
        else:
            print(f"Error: Index can only be created over unique or primary key columns")

class Column:
    # Creating class Column
    def __init__(self, name, data_type, unique=False, primary_key=False, index=None):
        self.name = name
        self.data_type = data_type
        self.unique = unique
        self.primary_key = primary_key
        self.index = index

class Index:
    def __init__(self, table, column):
        self.table = table
        self.column = column
        self.rows = {}  # dictionary of rows indexed by the column value
    
    def add_row(self, row):
        # add a row to the index
        value = row[self.column.name]
        if value in self.rows:
            self.rows[value].append(row)
        else:
            self.rows[value] = [row]

class BTreeIndex(Index):
    def __init__(self, table, column):
        super().__init__(table, column)
        self.tree = BTree()  # initialize a new BTree data structure
    
    def add_row(self, row):
        # add a row to the index and the BTree
        super().add_row(row)
        self.tree.insert(row[self.column.name], row)

class HashIndex(Index):
    def __init__(self, table, column):
        super().__init__(table, column)
        self.table_size = 100  # choose a hash table size
        self.table = dict.Hash(self.table_size)  # initialize a new hash table
    
    def add_row(self, row):
        # add a row to the index and the hash table
        super().add_row(row)
        hash_key = row[self.column.name] % self.table_size
        if self.table.exists(hash_key):
            self.table[hash_key].append(row)
        else:
            self.table[hash_key] = [row]

class Table:
def init(self, name, columns):
self.name = name
self.columns = columns
self.primary_key = None # set primary key to None by default
self.indexes = [] # list of index objects

def get_column(self, column_name):
    # get the specified column object by name
    for column in self.columns:
        if column.name == column_name:
            return column
    print(f"Error: Column {column_name} not found in table {self.name}")

def add_index(self, index):
    # add an index object to the list of indexes
    self.indexes.append(index)

def get_primary_key(self):
    # get the primary key column object
    for column in self.columns:
        if column.primary_key:
            self.primary_key = column
            return column
    print(f"Error: Primary key not found in table {self.name}")
class MetaTable:
def init(self, name, primary_key):
self.name = name
self.primary_key = primary_key
self.indexes = [] # list of index objects

def add_index(self, index):
    # add an index object to the list of indexes
    self.indexes.append(index)
class BTree:
def init(self):
self.root = None


def insert(self, key, value):
    # insert a key-value pair into the BTree
    if self.root is None:
        self.root = Node(key, value)
    else:
        self.root.insert(key, value)
class Node:
def init(self, key, value):
self.keys = [key]
self.values = [value]
self.children = []

def is_leaf(self):
    # check if the node is a leaf node
    return len(self.children) == 0

def insert(self, key, value):
    # insert a key-value pair into the node or one of its children
    if self.is_leaf():
        # if the node is a leaf, insert the key-value pair into the node
        self.keys.append(key)
        self.values.append(value)
        self.sort_keys()
    else:
        # if the node is not a leaf, insert the key-value pair into one of its children
        i = self.find_child_index(key)
        child = self.children[i]
        child.insert(key, value)

def find_child_index(self, key):
    # find the index of the child that should contain the key
    for i, k in enumerate(self.keys):
        if key < k:
            return i
    return len(self.children) - 1

def sort_keys(self):
    # sort the keys and values of the node by key value
    keys, values = zip(*sorted(zip(self.keys, self.values)))
    self.keys = list(keys)
    self.values = list(values)
