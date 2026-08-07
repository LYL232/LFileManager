from typing import Dict
from .brute import BruteDatabase, Database
from .str_check import name_single_character_check

DATABASE_CLASS: Dict[str, type] = {
    'brute': BruteDatabase
}
