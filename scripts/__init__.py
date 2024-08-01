from .base import BaseScript, DataBaseScript, SingleTransactionScript, FileMD5ComputingScript
from .database import InitializeDataBaseScript, ClearDataBaseScript
from .common import (
    ManageDirectoryScript,
    CancelManagementScript,
    MakeDirectoryScript,
    RemoveDirectoryScript,
    QueryDirectoryScript,
    QueryFileRecordScript,
    DumpDatabaseScript,
    QueryRedundantFileScript,
)

SCRIPTS = {
    'init_db': InitializeDataBaseScript,
    'clear_db': ClearDataBaseScript,
    'manage': ManageDirectoryScript,
    'cm': CancelManagementScript,
    'mkdir': MakeDirectoryScript,
    'rm': RemoveDirectoryScript,
    'ls': QueryDirectoryScript,
    'fr': QueryFileRecordScript,
    'dump_db': DumpDatabaseScript,
    'qrf': QueryRedundantFileScript,
}
