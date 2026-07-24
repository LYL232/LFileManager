from __future__ import annotations

from dataclasses import dataclass, field
from time import time

from fontTools.ttLib.ttVisitor import visit

from .database import Database, Transaction
from error import OperationError, DataError
import os
from os.path import exists, join

from typing import Dict, TYPE_CHECKING, List, Tuple

if TYPE_CHECKING:
    from base import Repository, RepositoryInstance, FileRecord


@dataclass(slots=True)
class BruteDatabaseImage:
    # 所有仓库
    repository: Dict[str, Repository] = field(default_factory=dict)

    # 所有仓库具有的实例的记录
    repository_instance: Dict[Tuple[str, str], List[RepositoryInstance]] = field(default_factory=dict)

    # 每个仓库的根文件记录
    repository_root: Dict[str, FileRecord] = field(default_factory=dict)

    # 所有文件记录:ID到文件记录
    file_record: Dict[int, FileRecord] = field(default_factory=dict)

    # 下一个文件记录ID
    next_file_record_id: int = 0


class BruteDatabase(Database):
    def __init__(self, database_path: str = None, logger: callable = print):
        self.database_path = database_path
        self.logger = logger

        self._current_image = BruteDatabaseImage()

        self.repository_file_path = join(database_path, 'repositories.jsonl')
        self.repository_instance_file_path = join(database_path, 'repository_instance.jsonl')
        self.file_record_path = join(database_path, 'file_record.jsonl')
        if exists(database_path):
            raise NotImplementedError

    def begin_transaction(self) -> BruteTransaction:
        return BruteTransaction(self)

    def initialize_database(self):
        """
        初始化数据库，谨慎操作，如果存在相关的表会抛出异常
        :return:
        """
        os.makedirs(self.database_path, exist_ok=False)

    def is_initialized(self) -> bool:
        return all(exists(each) for each in [
            self.repository_file_path,
            self.repository_instance_file_path,
            self.file_record_path
        ])

    def delete_database(self):
        """
        高危操作：删除所有表
        :return:
        """
        os.rmdir(self.database_path)
        self.logger('执行完毕')

    def close(self):
        """无需操作"""

    def new_repository(self, name: str, desc: str):
        if name in self._current_image.repository.keys():
            raise OperationError(f'{name}仓库已经存在')

        self._current_image.repository[name] = new_repo = Repository(name, desc)
        root_record = FileRecord(
            new_repo,
            '', '', 0, time(),
            file_record_id=self._current_image.next_file_record_id
        )
        self._current_image.repository_root[name] = root_record
        self._current_image.file_record[self._current_image.next_file_record_id] = root_record

    def repository_instances(self, repository: Repository) -> List[RepositoryInstance]:
        return self._current_image.repository_instance[repository.name].copy()

    def repositories(self) -> List[Repository]:
        return list(self._current_image.repository.values()).copy()

    def delete_repository(self, name: str):
        if name not in self._current_image.repository.keys():
            DataError(f'并未找到名字为{name}的仓库')
        if any([each.repository.name == name for each in self._current_image.file_record.values()]):
            DataError(f'仍存在{name}仓库的文件记录，无法删除')
        self._current_image.repository.pop(name)
        return True

    def is_repository_instance_exists(
            self,
            repository: Repository,
            instance_name: str
    ) -> bool:
        return (repository.name, instance_name) in \
            self._current_image.repository_instance.keys()

    def new_repository_instance(
            self,
            repository: Repository,
            instance_name: str,
            path: str
    ) -> bool:
        assert not self.is_repository_instance_exists(repository, instance_name) or \
               DataError(f'{repository.name}中已经存在{instance_name}实例')
        self._current_image.repository_instance[(repository.name, instance_name)] = \
            RepositoryInstance(repository, instance_name, path)
        return True

    def update_repository_instance(self, instance: RepositoryInstance) -> bool:
        found = self._current_image.repository_instance.get(
            (instance.repository.name, instance.instance_name), None
        )
        assert found is not None or DataError(f'未找到{instance.repository.name}仓库的{instance.instance_name}实例')
        found.path = instance.path
        return True

    def remove_repository_instance(self, instance: RepositoryInstance) -> bool:
        found = self._current_image.repository_instance.pop(
            (instance.repository.name, instance.instance_name), None
        )
        assert found is None or DataError(f'未找到{instance.repository.name}仓库的{instance.instance_name}实例')
        return True

    def query_repository_instance(
            self,
            repository: Repository,
            instance_name: str
    ) -> RepositoryInstance:
        found = self._current_image.repository_instance.pop(
            (repository.name, instance_name), None
        )
        assert found is not None or DataError(f'未找到{repository.name}仓库的{instance_name}实例')
        return found

    def _write_new_file_records(self, file_records: List[FileRecord]) -> int:
        image = self._current_image
        for each in file_records:
            assert each.directory_file_record_id.file_record_id in image.file_record.keys() or \
                   DataError(f'找不到{each}的父目录')

            self._new_file_record(each)
        return len(file_records)

    def _new_file_record(self, record: FileRecord):
        image = self._current_image
        record.file_record_id = image.next_file_record_id
        record.directory_file_record_id.children[record.name] = record
        image.file_record[image.next_file_record_id] = image
        image.next_file_record_id += 1

    def update_file_records(self, file_records: List[FileRecord]) -> int:
        image = self._current_image
        for each in file_records:
            assert each.file_record_id in image.file_record.keys() or \
                   DataError(f'找不到{each}的文件记录')
            image.file_record[each.file_record_id] = each
        return len(file_records)

    def repository_file_records(self, repository: Repository) -> List[FileRecord]:
        root = self._current_image.repository_root[repository.name]
        res = self._find_file_record(root)
        assert res[0] == root
        res.pop(0)
        return res

    def _find_file_record(self, file_record: FileRecord) -> List[FileRecord]:
        """找到该文件记录下的所有文件记录"""
        res = [file_record]
        for child in file_record.children:
            res.extend(self._find_file_record(child))
        return res

    def delete_file_records(self, file_records: List[FileRecord]) -> int:
        image = self._current_image
        for each in file_records:
            assert each.file_record_id in image.file_record.keys() or \
                   DataError(f'找不到{each}的文件记录')
        return len(file_records)

    def all_file_records(self) -> List[FileRecord]:
        return list(self._current_image.file_record.values())

    def all_repositories(self) -> List[Repository]:
        return list(self._current_image.repository.values())

    def query_common_size_without_md5_files(self, file_records: List[FileRecord]) \
            -> Dict[int, List[FileRecord]]:
        size_classified = {}
        for record in file_records:
            if record.directory_file_record_id is None:
                # 没有父目录说明是根目录
                continue
            size = record.size
            classified = size_classified.get(size, None)
            if classified is None:
                size_classified[size] = classified = []
            classified.append(record)
        return {md5: classified for md5, classified in size_classified.items() if len(classified) > 1}

    def query_common_md5_files(self, file_records: List[FileRecord]) -> \
            Dict[str, List[FileRecord]]:
        md5_classified = {}
        for record in file_records:
            if record.directory_file_record_id is None or record.md5 == record.EMPTY_MD5:
                # 没有父目录说明是根目录，没有MD5也跳过
                continue

            md5 = record.md5
            classified = md5_classified.get(md5, None)
            if classified is None:
                md5_classified[md5] = classified = []
            classified.append(record)
        return {md5: classified for md5, classified in md5_classified.items() if len(classified) > 1}

    def initialize_repositories(self, repositories: List[Repository]) -> int:
        image = self._current_image
        assert len(image.repository) == 0 or DataError(
            f'初始化仓库数据时，仓库数据不为空，目前数据为：{image.repository}'
        )
        for repository in repositories:
            image.repository[repository.name] = repository.clone
        return len(repositories)

    def initialize_repository_instances(self, instances: List[RepositoryInstance]) -> int:
        image = self._current_image
        assert len(image.repository_instance) == 0 or DataError(
            f'初始化仓库实例数据时，仓库实例数据不为空，目前数据为：{image.repository_instance}'
        )
        for instance in instances:
            image.repository_instance[(instance.repository.name, instance.instance_name)] = instance.clone
        return len(instances)

    def initialize_file_records(self, records: List[FileRecord]) -> int:
        image = self._current_image
        assert len(image.file_record) == 0 or DataError(
            f'初始化仓库实例数据时，仓库实例数据不为空，目前数据为：{image.repository_instance}'
        )
        for record in records:
            image.file_record[record.file_record_id] = record.copy()
        return len(records)

    def initialize_repository_root_fire_record(
            self, mappings: List[Tuple[Repository, FileRecord]]
    ) -> int:
        image = self._current_image


class BruteTransaction(Transaction):
    def __init__(self, database: BruteDatabase):
        self.database = database

    def commit(self):
        pass

    def rollback(self):
        pass
