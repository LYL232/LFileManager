from __future__ import annotations
import os
import hashlib
from os.path import join
import platform
import time
from typing import Dict
from error import DataError


# from base import RepositoryInstance


class FileRecord:
    READ_BUFFER = 128 * 1024 * 1024
    __INSTANCE: Dict[int, FileRecord] = {}

    """
    描述一个文件的类
    """
    EMPTY_MD5 = '*' * 32

    def __init__(
            self,
            name: str,
            suffix: str,
            size: int,
            modified_time: float,
            directory_file_record_id: int | None = None,
            md5: str | None = None,
            file_record_id: int | None = None
    ):
        """
        :param size: 该文件的大小，单位为字节
        :param modified_time: 文件的修改时间，整数时间戳
        :param md5: 文件的md5字符串，为空表示未计算
        :param name: 文件名
        :param suffix: 文件后缀
        """
        assert size >= 0, DataError(f'文件的大小为{size}不能小于0')
        assert isinstance(modified_time, float), DataError(f'时间必须为浮点数')
        assert '\\' not in name and '\\' not in suffix or DataError(f'文件名：{name}{suffix}中存在非法字符\\')
        assert '/' not in name and '/' not in suffix or DataError(f'文件名：{name}{suffix}中存在非法字符/')

        self.file_record_id = file_record_id

        self.repository_name = repository_name
        self.directory_file_record_id = directory_file_record_id
        self.children_id: Dict[str, int] = {}
        self.name, self.suffix = name, suffix

        self.size = size
        self.modified_time = modified_time
        self.md5 = md5 or self.EMPTY_MD5

    @property
    def copy(self) -> FileRecord:
        return FileRecord(
            name=self.name,
            suffix=self.suffix,
            size=self.size,
            modified_time=self.modified_time,
            directory_file_record_id=self.directory_file_record_id,
            md5=self.md5,
            file_record_id=self.file_record_id,
        )

    def __str__(self):
        return str(self.json_obj)

    @property
    def json_obj(self) -> dict:
        return {
            'file_record_id': self.file_record_id,
            'directory_file_record_id': self.directory_file_record_id,
            'name': self.name,
            'size': self.size,
            'suffix': self.suffix,
            'modified_time': self.modified_time,
            'modified_date': self.modified_date,
            'md5': self.md5,
        }

    @property
    def full_name(self) -> str:
        return f'{self.name}{self.suffix}'

    @property
    def modified_date(self) -> str:
        return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self.modified_time))

    def __repr__(self):
        return str(self)

    @staticmethod
    def format_path(path: str):
        """
        格式化文件的路径，
        :param path: 文件的路径
        :return: 文件所在目录路径，以/开头，文件名，文件后缀（如果有，则以.开头）
        """
        if platform.system() == 'Windows':
            path = path.replace('\\\\', os.sep)  # 去除双反斜杠
        path = path.replace(os.sep, '/')
        # 检查所有路径中是否存在相对路径
        split_path = path.split('/')
        if split_path[0] == '':
            split_path.pop(0)
        assert all(each != '.' and each != '..' for each in split_path), f'路径中不允许存在相对路径：{path}'
        filename = split_path.pop(-1)
        file_dir_path = '/' if len(split_path) == 0 else '/' + '/'.join(split_path) + '/'
        filename_split = filename.split('.')
        if len(filename_split) == 1:
            return file_dir_path, filename, ''
        suffix = '.' + filename_split.pop(-1)
        return file_dir_path, '.'.join(filename_split), suffix

    @property
    def identity(self):
        return self.directory_file_record_id, self.name

    def __hash__(self):
        return hash(self.identity)

    def __eq__(self, other: object):
        if not isinstance(other, FileRecord):
            return False
        return self.identity == other.identity

# class FileInstance:
#     # 128MB的读取缓存
#     READ_BUFFER = 128 * 1024 * 1024
#
#     def __init__(
#             self,
#             repository_instance: RepositoryInstance,
#             file_record: FileRecord,
#             md5: str = None
#     ):
#         self.repository_instance = repository_instance
#         self.file_record = file_record
#         self.md5 = md5 or FileRecord.EMPTY_MD5
#
    def compute_md5(self) -> str:
        """
        计算文件的md5
        :return: md5
        """
        m = hashlib.md5()
        with open(join(
                self.repository_instance.path,
                *(self.file_record.path.split('/')[1:])), 'rb'
        ) as file:
            while True:
                data = file.read(self.READ_BUFFER)
                if not data:
                    break
                m.update(data)
        self.md5 = m.hexdigest()
        return self.md5
