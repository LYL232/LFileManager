from typing import Dict
from .database import Database, MysqlDataBase

DATABASE_CLASS: Dict[str, type] = {
    'mysql': MysqlDataBase
}
