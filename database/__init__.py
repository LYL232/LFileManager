from typing import Dict
from .brute import BruteDatabase, Database

DATABASE_CLASS: Dict[str, type] = {
    'brute': BruteDatabase
}
