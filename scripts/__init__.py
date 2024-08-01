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
    QuerySizeScript
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
    'size': QuerySizeScript,
    'dump_db': DumpDatabaseScript,
    'qrf': QueryRedundantFileScript,
}
