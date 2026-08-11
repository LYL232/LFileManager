from __future__ import annotations
from dataclasses import dataclass
from typing import List
from os.path import isdir, join, abspath
import hashlib
from os.path import join
import os
import time
from datetime import datetime

from base import FileRecord
from error import OperationError


@dataclass(init=False, slots=True)
class Repository:
    """
    仓库：对应一个或者多个实际的存储位置
    """
    name: str
    desc: str  # 仓库描述

    def __init__(self, name: str, desc: str):
        self.name = name
        self.desc = desc

    @property
    def copy(self) -> Repository:
        return Repository(self.name, self.desc)


@dataclass(slots=True)
class RepositoryInstance:
    """
    仓库实际存在的位置和位置名称
    """
    repository_name: str
    instance_name: str  # 是一个能区分物理存储位置的字符串，比如"第一台笔记本的机械盘"
    path: str | None = None  # 该仓库实例目前的路径

    @property
    def copy(self) -> RepositoryInstance:
        return RepositoryInstance(self.repository_name, self.instance_name, self.path)

    @classmethod
    def get_file_paths_of_dir(cls, dir_abs_path: str) -> List[str]:
        """
        获取指定绝对路径下的所有文件的绝对路径
        :param dir_abs_path: 目录的绝对路径
        :return: 所有文件的绝对路径
        """
        if not isdir(dir_abs_path):
            return []
        res = []
        for each in os.listdir(dir_abs_path):
            file = join(dir_abs_path, each)
            if isdir(file):
                res.extend(cls.get_file_paths_of_dir(file))
            else:
                res.append(file)
        return res

    def get_directory_file_records(self, dir_abs_path: str) -> List[FileRecord]:
        """
        获取指定绝对路径下的所有文件的文件记录
        :param dir_abs_path: 目录的绝对路径
        :return: 所有文件的文件记录
        """
        if not isdir(dir_abs_path):
            return []
        root_path = abspath(self.path)
        res = []
        for each in os.listdir(dir_abs_path):
            file = join(dir_abs_path, each)
            if isdir(file):
                res.extend(self.get_directory_file_records(file))
            else:
                res.append(file)
        return res

    def instance_file_records(self) -> List[FileRecord]:
        """
        获取根路径目录下的所有文件记录
        :return: 该路径下的所有文件对应的文件实例
        """
        if self.path is None:
            return []
        dir_path = abspath(self.path)
        file_paths = self.get_file_paths_of_dir(dir_path)
        assert all(dir_path in each for each in file_paths)
        res = []
        for each in file_paths:
            if dir_path.startswith(f'{dir_path}/.lyl232fm'):
                continue
            assert dir_path in each, f'文件路径：{each}中不包含指定物理根目录路径：{dir_path}'
            date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(os.path.getmtime(each)))
            date_obj = datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
            timestamp = int(time.mktime(date_obj.timetuple()))
            record = FileRecord(
                name=name,
                suffix=suffix,
                size=os.path.getsize(each),
                modified_time=timestamp,
                md5='',
            )
            if record.dir_path.startswith('/.lyl232fm/'):
                continue
            res.append(record)
        return res

    def compute_file_record_md5(self, file_record_path: str) -> str:
        """
        计算文件记录的md5
        :param file_record_path: 文件记录路径
        :return: md5
        """
        assert self.path is not None, OperationError(
            f'仓库{self.repository_name}的{self.instance_name}实例没有注册路径'
        )
        m = hashlib.md5()
        assert file_record_path.startswith('/')
        file_record_path = file_record_path[1:]
        with open(join(self.path, *(file_record_path.split('/'))), 'rb') as file:
            while True:
                data = file.read(FileRecord.FILE_MD5_COMPUTE_READ_BUFFER)
                if not data:
                    break
                m.update(data)
        return m.hexdigest()
