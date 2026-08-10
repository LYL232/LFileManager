from __future__ import annotations

from dataclasses import dataclass, field
from time import time

from .database import Database, Transaction
from error import OperationError, DataError, RunTimeError
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
        self.temp_backup_database_path = join(data_base_root_path, 'temp_backup')
        self.logger = logger

        self._current_image = self._load_image_from_file(self.main_database_path) \
            if exists(self.main_database_path) else BruteDatabaseImage()

    @staticmethod
    def _get_data_base_file_paths(directory_path: str) -> Tuple[str, str, str, str]:
        return (
            join(directory_path, 'repositories.csv'),
            join(directory_path, 'repository_root.csv'),
            join(directory_path, 'repository_instance.csv'),
            join(directory_path, 'file_record.csv'),
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
                line = file.readline()
        with open(repository_root_file_path, 'r', encoding='utf-8') as file:
            line = file.readline()
            while line:
                name, root_file_id = line.strip().split(',')
                repository_root[name] = int(root_file_id)
                line = file.readline()
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
                line = file.readline()
        with open(file_record_path, 'r', encoding='utf-8') as file:
            line = file.readline()
            while line:
                (
                    repository_name,
                    file_record_id,
                    name,
                    suffix,
                    size,
                    modified_time,
                    directory_file_record_id,
                    md5
                ) = line.strip().split('/')
                file_record_id = int(file_record_id)
                size = int(size)
                modified_time = float(modified_time)
                directory_file_record_id = int(directory_file_record_id)
                if directory_file_record_id < 0:
                    directory_file_record_id = None
                file_record[file_record_id] = FileRecord(
                    repository_name=repository_name,
                    file_record_id=file_record_id,
                    name=name,
                    suffix=suffix,
                    size=size,
                    modified_time=modified_time,
                    directory_file_record_id=directory_file_record_id,
                    md5=md5,
                )
                line = file.readline()
        return BruteDatabaseImage(
            repository=repository,
            repository_root=repository_root
        )

    def save_current_image_to_database(self):
        # 先移动已经备份好的到一个临时的位置
        if not exists(self.main_database_path):
            os.makedirs(self.main_database_path, exist_ok=False)

        # 如果要移动的临时位置已经有了，那直接删除
        if exists(self.temp_backup_database_path):
            shutil.rmtree(self.temp_backup_database_path)

        if exists(self.backup_database_path):
            shutil.move(self.backup_database_path, self.temp_backup_database_path)
        os.makedirs(self.backup_database_path, exist_ok=False)

        main_files = (
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

        # 移动主数据库到备份路径
        try:
            shutil.copy2(main_repository_file_path, backup_repository_file_path)
            shutil.copy2(main_repository_root_file_path, backup_repository_root_file_path)
            shutil.copy2(main_repository_instance_file_path, backup_repository_instance_file_path)
            shutil.copy2(main_file_record_path, backup_file_record_path)
        except Exception as e:
            # 回退之前的动作
            try:
                shutil.rmtree(self.backup_database_path)
                if exists(self.temp_backup_database_path):
                    shutil.move(self.backup_database_path, self.temp_backup_database_path)
            except Exception as internal_e:
                RunTimeError(f'无法备份主数据库并且无法回退已经执行的备份操作，异常：{internal_e}，复制主数据库时异常：{e}')
            RunTimeError(f'无法备份主数据库，异常：{e}')

        image = self._current_image

        # 写入主数据
        try:
            with open(main_repository_file_path, 'w', encoding='utf-8') as file:
                for name, repository in image.repository.items():
                    file.write(f'{name},{repository.desc}\n')
            with open(main_repository_root_file_path, 'w', encoding='utf-8') as file:
                for name, root_file_id in image.repository_root.items():
                    file.write(f'{name},{root_file_id}\n')
            with open(main_repository_instance_file_path, 'w', encoding='utf-8') as file:
                for repository_name, repository_instances in image.repository_instance.items():
                    for instance_name, instance in repository_instances.items():
                        file.write(f'{instance_name}/{repository_name}/{instance.path}\n')
            with open(main_file_record_path, 'w', encoding='utf-8') as file:
                for file_record in image.file_record.values():
                    assert file_record.file_record_id is not None
                    items = (
                        str(file_record.file_record_id),
                        file_record.name,
                        file_record.suffix,
                        str(file_record.size),
                        str(file_record.modified_time),
                        '-1' if file_record.directory_file_record_id is None \
                            else str(file_record.directory_file_record_id),
                        file_record.md5
                    )
                    file.write('/'.join(items) + '\n')

        except Exception as e:
            for each in main_files:
                if exists(each):
                    os.remove(each)
            assert exists(self.backup_database_path) or RunTimeError(f'写入主数据库时发生异常：{e}，但找不到备份数据库')
            shutil.move(self.backup_database_path, self.main_database_path)
            if exists(self.temp_backup_database_path):
                shutil.move(self.temp_backup_database_path, self.backup_database_path)
            raise RunTimeError(f'写入主数据库时发生异常：{e}')

    def copy_current_image(self) -> BruteDatabaseImage:
        return self._current_image.copy

    def reset_current_image(self, image: BruteDatabaseImage):
        self._current_image = image

    def begin_transaction(self) -> BruteTransaction:
        return BruteTransaction(self)

    def initialize_database(self):
        print(f'初始化数据库于{self.data_base_root_path}')
        (
            repository_file_path,
            repository_root_file_path,
            repository_instance_file_path,
            file_record_path
        ) = self._get_data_base_file_paths(self.main_database_path)
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
        return all(exists(each) for each in self._get_data_base_file_paths(self.main_database_path))

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

    def find_repository(self, name: str) -> Repository:
        res = self._current_image.repository.get(name, None)
        assert res is not None, OperationError(f'名字为{name}的仓库不存在')
        return res

    def repository_instances(self, repository_name: str) -> Dict[str, RepositoryInstance]:
        return self._current_image.repository_instance.get(repository_name, {}).copy()

    def repositories(self) -> List[Repository]:
        return list(self._current_image.repository.values()).copy()

    def delete_repository(self, name: str):
        if name not in self._current_image.repository.keys():
            DataError(f'并未找到名字为{name}的仓库')
        if name in self._current_image.repository_root.keys():
            DataError(f'仍存在{name}仓库的文件记录，无法删除')
        self._current_image.repository.pop(name)
        return True

    def is_repository_instance_exists(self, repository_name: str, instance_name: str) -> bool:
        repository_instances = self._current_image.repository_instance.get(repository_name, None)
        if repository_instances is None:
            return False
        return instance_name in repository_instances.keys()

    def new_repository_instance(
            self,
            repository_name: str,
            instance_name: str,
            path: str
    ) -> bool:
        repository_instance = self._current_image.repository_instance.get(repository_name, None)
        assert repository_instance is not None, OperationError(f'名字为{repository_name}的仓库不存在')
        assert instance_name not in repository_instance.keys() or \
               OperationError(f'{repository_name}中已经存在{instance_name}实例')
        repository_instance[instance_name] = RepositoryInstance(repository_name, instance_name, path)
        return True

    def _find_repository_instances(self, repository_name: str) -> Dict[str, RepositoryInstance]:
        repository = self.find_repository(repository_name)
        repository_instances = self._current_image.repository_instance.get(repository.name, None)
        if repository_instances is None:
            repository_instances = self._current_image.repository_instance[repository.name] = {}
        return repository_instances

    def update_repository_instance(self, repository_name: str, instance_name: str, path: str) -> bool:
        repository_instances = self._find_repository_instances(repository_name)
        assert instance_name in repository_instances.keys(), (
            OperationError(f'仓库：{repository_name}不存在名为{instance_name}的实例'))
        repository_instances[instance_name] = RepositoryInstance(repository_name, instance_name, path)
        return True

    def remove_repository_instance(self, repository_name: str, instance_name: str) -> bool:
        repository_instances = self._find_repository_instances(repository_name)
        found_instance = repository_instances.get(instance_name, None)
        assert found_instance is not None, \
            DataError(f'未找到{repository_name}仓库的{instance_name}实例')
        repository_instances.pop(instance_name)
        return True

    def find_repository_instance(self, repository_name: str, instance_name: str) -> RepositoryInstance:
        repository_instances = self._find_repository_instances(repository_name)
        found_instance = repository_instances.get(instance_name, None)
        assert found_instance is not None, DataError(f'未找到{repository_name}仓库的{instance_name}实例')
        return found_instance

    def _write_new_file_records(self, file_records: List[FileRecord]) -> int:
        image = self._current_image
        for each in file_records:
            assert each.directory_file_record_id is None or \
                   each.directory_file_record_id in image.file_record.keys() or \
                   DataError(f'找不到{each}的父目录')
            self._new_file_record(each)
        return len(file_records)

    def _new_file_record(self, record: FileRecord):
        image = self._current_image
        record.file_record_id = image.next_file_record_id
        if record.directory_file_record_id is not None:
            directory_record = image.file_record[record.directory_file_record_id]
            directory_record.children_id[record.full_name] = record.file_record_id
        image.file_record[image.next_file_record_id] = record
        image.next_file_record_id += 1

    def update_file_records(self, file_records: List[FileRecord]) -> int:
        image = self._current_image
        for each in file_records:
            assert each.file_record_id is not None and each.file_record_id in image.file_record.keys(), \
                DataError(f'找不到{each}的文件记录')
            image.file_record[each.file_record_id] = each
        return len(file_records)

    def _find_file_record(self, file_record_id: int) -> FileRecord:
        res = self._current_image.file_record.get(file_record_id, None)
        assert res is not None, OperationError(f'找不到id为{file_record_id}的文件记录')
        return res

    def repository_file_records(self, repository_name: str) -> List[FileRecord]:
        root_file_record = self._find_file_record(self._current_image.repository_root[repository_name])
        res = self._find_directory_file_records(root_file_record)
        assert res[0].file_record_id == root_file_record.file_record_id
        res.pop(0)
        return res

    def _find_directory_file_records(self, file_record: FileRecord) -> List[FileRecord]:
        """找到该文件记录下的所有文件记录"""
        res = [file_record]
        for name in sorted(file_record.children_id.keys()):
            child_id = file_record.children_id[name]
            res.extend(self._find_directory_file_records(self._find_file_record(child_id)))
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
        self.transaction_image = database.copy_current_image()

    def commit(self):
        self.database.save_current_image_to_database()

    def rollback(self):
        self.database.reset_current_image(self.transaction_image)
