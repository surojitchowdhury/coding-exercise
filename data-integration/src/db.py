import duckdb
from config import DATABASE_PATH, TABLE_MAP

# Database abstraction class. You can change or remove this class at your convenience
class Database:
    def __init__(self, db_path=DATABASE_PATH):
        self.db_path = db_path
        self.connection = None

    def __enter__(self):
        self.connection = duckdb.connect(self.db_path)
        return self.connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection:
            self.connection.close()
            
    @staticmethod
    def getTable(name):
        if name.lower() in TABLE_MAP:
            return TABLE_MAP.get(name.lower())
        else:
            raise TableNotFoundException(f"Table not found in config class")

class TableNotFoundException(Exception):
    pass
