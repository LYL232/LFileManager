from __future__ import annotations

import json
from dataclasses import dataclass, field
from time import time

from .database import Database, Transaction
from error import OperationError, DataError
import os
from os.path import exists, join, abspath

from typing import Dict, List, Tuple, Callable
from base import Repository, RepositoryInstance, FileRecord
import shutil


@dataclass(slots=True)
class BruteDatabaseImage:
    # 所有仓库
    repository: Dict[str, Repository] = field(default_factory=dict)

    # 所有仓库具有的实例的记录
    repository_instance: Dict[str, Dict[str, RepositoryInstance]] = field(default_factory=dict)

    # 每个仓库的根文件记录
    repository_root: Dict[str, int] = field(default_factory=dict)

    # 所有文件记录:ID到文件记录
    file_record: Dict[int, FileRecord] = field(default_factory=dict)

    # 下一个文件记录ID
    next_file_record_id: int = 0

    @property
    def copy(self) -> BruteDatabaseImage:
        repository_instance = {}
        for repo_name, repo_instances in self.repository_instance:
            repository_instance[repo_name] = {
                instance_name: instance.copy for instance_name, instance in repo_instances.items()
            }
        return BruteDatabaseImage(
            repository={key: value.copy for key, value in self.repository.items()},
            repository_instance=repository_instance,
            repository_root=self.repository_root.copy(),
            file_record={key: value.copy for key, value in self.file_record.items()},
            next_file_record_id=self.next_file_record_id
        )


class BruteDatabase(Database):
    def __init__(self, data_base_root_path: str, logger: Callable = print):
        if data_base_root_path.startswith('%'):
            # 百分号开头的特殊处理，将百分号替换为项目根目录
            data_base_root_path = abspath(join(__file__, '..', data_base_root_path[1:]))
        else:
            data_base_root_path = abspath(data_base_root_path)

        self.data_base_root_path = data_base_root_path
        self.main_database_path = join(data_base_root_path, 'main')
        self.backup_database_path = join(data_base_root_path, 'backup')
        self.logger = logger

        self._current_image = self._load_image_from_file(self.main_database_path) \
            if exists(self.main_database_path) else BruteDatabaseImage()

    @staticmethod
    def _get_data_base_file_paths(directory_path:str) -> Tuple[str, str, str, str]:
        return (
            join(directory_path, 'repositories.csv'),
            join(directory_path, 'repository_root.csv'),
            join(directory_path, 'repository_instance.csv'),
            join(directory_path, 'file_record.jsonl'),
        )

    @classmethod
    def _load_image_from_file(cls, directory_path: str) -> BruteDatabaseImage:
        (
            repository_file_path,
            repository_root_file_path,
            repository_instance_file_path,
            file_record_path
        ) = cls._get_data_base_file_paths(directory_path)
        repository = {}
        repository_root = {}
        repository_instance = {}
        file_record = {}
        with open(repository_file_path, 'r', encoding='utf-8') as file:
            line = file.readline()
            while line:
                name, desc = line.strip().split(',')
                repository[name] = Repository(name, desc)
        with open(repository_root_file_path, 'r', encoding='utf-8') as file:
            line = file.readline()
            while line:
                name, root_file_id = line.strip().split(',')
                repository_root[name] = int(root_file_id)
        with open(repository_instance_file_path, 'r', encoding='utf-8') as file:
            line = file.readline()
            while line:
                items = line.strip().split('/')
                repository_name, instance_name = items[:2]
                path = '/'.join(items[2:])
                repository_instances = repository_instance.get(repository_name, None)
                if repository_instances is None:
                    repository_instance[repository_name] = repository_instances = {}
                repository_instances[instance_name] = RepositoryInstance(
                    repository_name=repository_name,
                    instance_name=instance_name,
                    path=path
                )
        with open(file_record_path, 'r', encoding='utf-8') as file:
            line = file.readline()
            while line:
                file_record_obj = json.loads(line)
                file_record_id = file_record_obj['file_record_id']
                file_record[file_record_id] = FileRecord(
                    repository_name=file_record_obj['repository_name'],
                    name=file_record['name'],
                    suffix=file_record['suffix'],
                    size=file_record['size'],
                    modified_time=file_record['modified_time'],
                    directory_file_record_id=file_record['directory_file_record_id'],
                    md5=file_record['md5'],
                    file_record_id=file_record_id
                )
        return BruteDatabaseImage(
            repository=repository,
            repository_root=repository_root
        )

    def save_current_image_to_database(self):
        if exists(self.backup_database_path):
            shutil.rmtree(self.backup_database_path)
        os.makedirs(self.backup_database_path)

        (
            main_repository_file_path,
            main_repository_root_file_path,
            main_repository_instance_file_path,
            main_file_record_path
        ) = self._get_data_base_file_paths(self.main_database_path)

        (
            backup_repository_file_path,
            backup_repository_root_file_path,
            backup_repository_instance_file_path,
            backup_file_record_path
        ) = self._get_data_base_file_paths(self.backup_database_path)

        shutil.copy2(main_repository_file_path, backup)

        image = self._current_image
        repository = image.repository
        repository_root = image.repository_root
        repository_instance = image.repository_instance
        file_record = image.file_record

        with open(self.repository_file_path, 'r', encoding='utf-8') as file:
            line = file.readline()
            while line:
                name, desc = line.strip().split(',')
                repository[name] = Repository(name, desc)
        with open(self.repository_root_file_path, 'r', encoding='utf-8') as file:
            line = file.readline()
            while line:
                name, root_file_id = line.strip().split(',')
                repository_root[name] = int(root_file_id)
        with open(self.repository_instance_file_path, 'r', encoding='utf-8') as file:
            line = file.readline()
            while line:
                items = line.strip().split('/')
                repository_name, instance_name = items[:2]
                path = '/'.join(items[2:])
                repository_instances = repository_instance.get(repository_name, None)
                if repository_instances is None:
                    repository_instance[repository_name] = repository_instances = {}
                repository_instances[instance_name] = RepositoryInstance(
                    repository_name=repository_name,
                    instance_name=instance_name,
                    path=path
                )
        with open(self.repository_instance_file_path, 'r', encoding='utf-8') as file:
            line = file.readline()
            while line:
                file_record_obj = json.loads(line)
                file_record_id = file_record_obj['file_record_id']
                file_record[file_record_id] = FileRecord(
                    repository_name=file_record_obj['repository_name'],
                    name=file_record['name'],
                    suffix=file_record['suffix'],
                    size=file_record['size'],
                    modified_time=file_record['modified_time'],
                    directory_file_record_id=file_record['directory_file_record_id'],
                    md5=file_record['md5'],
                    file_record_id=file_record_id
                )

    def begin_transaction(self) -> BruteTransaction:
        return BruteTransaction(self)

    def initialize_database(self):
        print(f'初始化数据库于{self.data_base_root_path}')
        repository_file_path = join(self.main_database_path, 'repositories.csv')
        repository_root_file_path = join(self.main_database_path, 'repository_root.csv')
        repository_instance_file_path = join(self.main_database_path, 'repository_instance.csv')
        file_record_path = join(self.main_database_path, 'file_record.jsonl')
        os.makedirs(self.main_database_path, exist_ok=False)
        with open(repository_file_path, 'w', encoding='utf-8') as _:
            pass
        with open(repository_root_file_path, 'w', encoding='utf-8') as _:
            pass
        with open(repository_instance_file_path, 'w', encoding='utf-8') as _:
            pass
        with open(file_record_path, 'w', encoding='utf-8') as _:
            pass

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
        os.rmdir(self.data_base_root_path)
        self.logger('执行完毕')

    def close(self):
        """无需操作"""

    def _new_repository(self, name: str, desc: str) -> bool:
        if name in self._current_image.repository.keys():
            raise OperationError(f'{name}仓库已经存在')

        self._current_image.repository[name] = new_repo = Repository(name, desc)
        record_id = self._current_image.next_file_record_id
        root_record = FileRecord(
            new_repo.name,
            '', '', 0, time(),
            file_record_id=self._current_image.next_file_record_id
        )
        self._current_image.repository_root[name] = record_id
        self._current_image.file_record[record_id] = root_record
        self._current_image.next_file_record_id += 1
        return True

    def is_repository_exists(self, name: str) -> bool:
        return name in self._current_image.repository.keys()

    def repository_instances(self, repository: Repository) -> Dict[str, RepositoryInstance]:
        return self._current_image.repository_instance[repository.name].copy()

    def repositories(self) -> List[Repository]:
        return list(self._current_image.repository.values()).copy()

    def delete_repository(self, name: str):
        if name not in self._current_image.repository.keys():
            DataError(f'并未找到名字为{name}的仓库')
        if any([each.repository_name == name for each in self._current_image.file_record.values()]):
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
            assert each.directory_file_record_id in image.file_record.keys() or \
                   DataError(f'找不到{each}的父目录')

            self._new_file_record(each)
        return len(file_records)

    def _new_file_record(self, record: FileRecord):
        image = self._current_image
        record.file_record_id = image.next_file_record_id
        directory_record = image.file_record[record.directory_file_record_id]
        directory_record.children_id[record.full_name] = record.file_record_id
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
        for child in file_record.children_id:
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
            image.repository[repository.name] = repository.copy
        return len(repositories)

    def initialize_repository_instances(self, instances: List[RepositoryInstance]) -> int:
        image = self._current_image
        assert len(image.repository_instance) == 0 or DataError(
            f'初始化仓库实例数据时，仓库实例数据不为空，目前数据为：{image.repository_instance}'
        )
        for instance in instances:
            image.repository_instance[(instance.repository.name, instance.instance_name)] = instance.copy
        return len(instances)

    def initialize_file_records(self, records: List[FileRecord]) -> int:
        image = self._current_image
        assert len(image.file_record) == 0 or DataError(
            f'初始化仓库实例数据时，仓库实例数据不为空，目前数据为：{image.repository_instance}'
        )
        for record in records:
            image.file_record[record.file_record_id] = record.copy
        return len(records)

    def file_record_path(self, record: FileRecord) -> str:
        res = []
        directory_file_record_id = record.directory_file_record_id
        while directory_file_record_id is not None:
            directory = self._current_image.file_record[directory_file_record_id]
            res.append(directory)
            directory_file_record_id = directory.directory_file_record_id
        res.reverse()
        return f'{"/".join([each.full_name for each in res])}/{record.full_name}'

    def initialize_repository_root_fire_record(self, mappings: List[Tuple[str, int]]) -> int:
        image = self._current_image
        for repository_name, file_record_id in mappings:
            image.repository_root[repository_name] = file_record_id
        return len(mappings)

    def find_in_file_path(
            self, record_property: str, keyword: str, repository_name: Repository = None
    ) -> List[FileRecord]:
        if repository_name is None:
            # 如果没指定就是搜索所有仓库
            res = []
            for each in self._current_image.repository.values():
                res.extend(self._find_file_in_repository(record_property, keyword, each))
            return res
        return self._find_file_in_repository(record_property, keyword, repository_name)

    def _find_file_in_repository(self, record_property: str, keyword: str, repository_name: str) -> List[FileRecord]:
        image = self._current_image
        root_file_id = image.repository_root[repository_name]
        root_file = image.file_record[root_file_id]
        assert hasattr(root_file, record_property) or OperationError(f'文件记录没有{record_property}的属性')
        assert isinstance(getattr(root_file, record_property), str) or \
               OperationError(f'文件记录的{record_property}属性不是字符串')
        return self._find_file_in_directory(record_property, keyword, root_file)

    def _find_file_in_directory(self, record_property: str, keyword: str, record: FileRecord):
        image = self._current_image
        res = []
        for child_file_record_id in record.children_id:
            child_file_record: FileRecord = image.file_record[child_file_record_id]
            property_value = getattr(child_file_record, record_property)
            if keyword in property_value:
                res.append(child_file_record)
            if len(child_file_record.children_id) > 0:
                res.extend(self._find_file_in_directory(record_property, keyword, child_file_record))
        return res

    def query_file_record_ids_by_size_and_md5(
            self, size: int, md5: str, repository_name: str = None
    ) -> List[int]:
        image = self._current_image
        if repository_name is None:
            res = []
            for repository_root_id in image.repository_root.values():
                res.extend(self._query_directory_file_record_ids_by_size_and_md5(
                    image.file_record[repository_root_id], size, md5
                ))
            return res
        return self._query_directory_file_record_ids_by_size_and_md5(
            image.file_record[image.repository_root[repository_name]], size, md5
        )

    def _query_directory_file_record_ids_by_size_and_md5(
            self, file_record: FileRecord, size: int, md5: str
    ) -> List[int]:
        image = self._current_image
        if len(file_record.children_id) == 0:
            if file_record.size == size and file_record.md5 == md5:
                return [file_record.file_record_id]
            return []
        res = []
        for child_id in file_record.children_id:
            child_record: FileRecord = image.file_record[child_id]
            res.extend(self._query_directory_file_record_ids_by_size_and_md5(
                child_record, size, md5
            ))
        return res


class BruteTransaction(Transaction):
    def __init__(self, database: BruteDatabase):
        self.database = database
        self.transaction_begin_image = database._current_image

    def commit(self):
        pass

    def rollback(self):
        pass
