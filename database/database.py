from abc import ABCMeta, abstractmethod
from typing import List, Dict

from base import Repository, RepositoryInstance, FileRecord


class Transaction(metaclass=ABCMeta):
    @abstractmethod
    def commit(self):
        """
        提交事务
        :return: None
        """

    @abstractmethod
    def rollback(self):
        """
        回滚事务
        :return: None
        """


class Database(metaclass=ABCMeta):
    @abstractmethod
    def initialize_database(self):
        """
        数据库初始化
        :return: None
        """

    @abstractmethod
    def is_initialized(self) -> bool:
        """
        :return: 数据集是否已经初始化
        """

    @abstractmethod
    def close(self):
        """
        关闭与数据库的链接
        :return: None
        """

    @abstractmethod
    def delete_database(self):
        """
        高危操作，清空数据库
        :return: None
        """

    @abstractmethod
    def new_repository(self, name: str, desc: str):
        """
        创建一个新的需要管理的仓库
        :param name: 仓库名，不能重复
        :param desc: 描述
        :return: None
        """

    @abstractmethod
    def repositories(self) -> List[Repository]:
        """
        查询所有仓库
        :return: 仓库名称和描述列表，如果没有返回空
        """

    @abstractmethod
    def repository_instances(self, repository: Repository) -> List[RepositoryInstance]:
        """
        一个仓库所关联的实例信息
        :param repository: 仓库名称
        :return: 没有时返回空列表
        """

    @abstractmethod
    def delete_repository(self, name: str) -> bool:
        """
        删除一个仓库
        :param name: 仓库名称
        :return: 0表示删除不成功，1表示删除成功
        """

    @abstractmethod
    def is_repository_instance_exists(
            self,
            repository: Repository,
            instance_name: str
    ) -> bool:
        """
        :param repository: 仓库
        :param instance_name: 实例名称
        :return: 数据库中是否有相关记录
        """

    @abstractmethod
    def new_repository_instance(
            self,
            repository: Repository,
            instance_name: str,
            path: str
    ) -> bool:
        """
        写入仓库实例记录，要求不能存在相同的仓库名和仓库实例名，仓库必须存在
        :param repository: 仓库
        :param instance_name: 实例名称
        :param path: 指向该仓库实例的路径
        :return: 1表示操作成功，0表示操作失败
        """

    @abstractmethod
    def update_repository_instance(self, instance: RepositoryInstance) -> bool:
        """
        修改仓库实例记录，要求存在相同的仓库名和仓库实例名
        :param instance: 仓库实例
        :return: 1表示操作成功，0表示操作失败
        """

    @abstractmethod
    def remove_repository_instance(self, instance: RepositoryInstance) -> bool:
        """
        删除一个仓库实例
        :param instance: 仓库实例名
        :return: 1表示操作成功，0表示操作失败
        """

    @abstractmethod
    def repository_instance(
            self,
            repository: Repository,
            instance_name: str
    ) -> RepositoryInstance:
        """
        查询仓库实例
        :param repository: 仓库
        :param instance_name: 实例名字
        :return: 仓库实例数据
        """

    @abstractmethod
    def begin_transaction(self) -> Transaction:
        """
        开始事务
        :return:
        """

    @abstractmethod
    def new_file_records(self, file_records: List[FileRecord]) -> int:
        """
        向数据库中插入指定的文件记录
        :param file_records: 需要插入的文件记录列表
        :return: 插入的记录数
        """

    @abstractmethod
    def update_file_records(self, file_records: List[FileRecord]) -> int:
        """
        更新文件记录
        :param file_records: 需要更新的文件记录
        :return: 更新的记录数
        """

    @abstractmethod
    def repository_file_records(self, repository: Repository) -> List[FileRecord]:
        """
        读取指定文件记录下的文件记录
        :param repository: 仓库
        :return: 数据库中的文件记录列表
        """

    @abstractmethod
    def delete_file_records(self, file_records: List[FileRecord]) -> int:
        """
        删除指定文件记录
        :param file_records: 文件记录
        :return: 删除的数量
        """

    @abstractmethod
    def all_file_records(self) -> List[FileRecord]:
        """
        获取所有文件记录
        :return: 数据库中的文件记录列表
        """

    @abstractmethod
    def all_repositories(self) -> List[Repository]:
        """
        获取所有仓库信息
        :return: 仓库列表
        """

    @abstractmethod
    def query_common_size_without_md5_files(self, file_records: List[FileRecord]) \
            -> Dict[int, List[FileRecord]]:
        """
        在所给的文件记录里查询所有拥有相同大小的缺失md5的文件记录
        :param file_records: 文件记录
        :return: [size] -> [file_record]
        """

    @abstractmethod
    def query_common_md5_files(self, file_records: List[FileRecord]) -> \
            Dict[int, Dict[str, List[FileRecord]]]:
        """
        查询所有拥有相同大小和md5的文件记录id
        :param file_records: 文件记录
        :return: [size][md5] -> [file_record]
        """

    @abstractmethod
    def create_directories_with_id(self, records: List[DirectoryRecord]) -> int:
        """
        创建指定id的目录记录，用于从文件中恢复初始化
        :param records: 记录列表
        :return: 创建的记录个数
        """

    @abstractmethod
    def create_managements_with_id(self, records: List[ManagementRecord]) -> int:
        """
        创建指定id管理记录，用于从文件中恢复初始化
        :param records: 记录列表
        :return: 创建记录的个数
        """

    @abstractmethod
    def new_files_with_id(self, records: List[FileRecord]) -> int:
        """
        创建指定id管理记录，用于从文件中恢复初始化
        :param records: 记录列表
        :return: 创建记录的个数
        """

    @abstractmethod
    def query_file_by_id(self, file_ids: List[int]) -> Dict[int, FileRecord]:
        """
        根据id列表查询指定的文件记录
        :param file_ids: 需要查询的文件id
        :return: [file_id] -> FileRecord
        """

    @abstractmethod
    def query_repository_by_id(self, repository_ids: List[int]) -> Dict[int, Repository]:
        """
        根据id列表查询指定的仓库
        :param repository_ids: 需要查询的仓库id
        :return: [repository_id] -> Repository
        """

    @abstractmethod
    def query_repository_size(self, repository: Repository) -> int:
        """
        查询目录的大小
        :param repository: 仓库
        :return: 大小（字节）
        """

    @abstractmethod
    def find_in_file_path(self, item: str, keyword: str) -> List[FileRecord]:
        """
        在文件记录的指定字段中寻找指定关键字
        :param item: 字段
        :param keyword: 关键字
        :return: 查询到的文件记录
        """

    @abstractmethod
    def query_file_ids_by_size_and_md5(self, size: int, md5: str) -> List[int]:
        """
        根据指定的md5值查询所有的文件记录id
        :param size: 指定的大小
        :param md5: 指定的md5值
        :return: 文件记录的id列表
        """
