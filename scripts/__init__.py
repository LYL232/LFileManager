from .base import BaseScript, DataBaseScript, SingleTransactionScript, FileMD5ComputingScript
from .database import InitializeDataBaseScript, ClearDataBaseScript, DumpDatabaseScript
from .common import (
    MakeRepositoryInstanceScript,
    CancelManagementScript,
    MakeDirectoryScript,
    RemoveRepositoryScript,
    QueryRepositoryScript,
    QueryFileRecordScript,
    QueryRedundantFileScript,
    QuerySizeScript,
    FindInFileDirectorPathScript, FindInNameScript, FindInSuffixScript,
    QueryDirectoryFileRecordsExistenceScript
)

SCRIPTS = {
    'init_db': InitializeDataBaseScript,
    'clear_db': ClearDataBaseScript,
    'mkins': MakeRepositoryInstanceScript,
    'cm': CancelManagementScript,
    'mkrepo': MakeDirectoryScript,
    'rm': RemoveRepositoryScript,
    'ls': QueryRepositoryScript,
    'fr': QueryFileRecordScript,
    'size': QuerySizeScript,
    'dump_db': DumpDatabaseScript,
    'qrf': QueryRedundantFileScript,
    'fid': FindInFileDirectorPathScript,
    'fin': FindInNameScript,
    'fis': FindInSuffixScript,
    'qde': QueryDirectoryFileRecordsExistenceScript,
}
