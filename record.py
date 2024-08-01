import os
import hashlib
from os.path import join, isdir, abspath
import platform
from typing import List
from datetime import datetime
import time

from error import CodingError


class DirectoryRecord:
    """
    描述管理目录的类
    """

    def __init__(self, name: str, desc: str, dir_id: int = None):
        """
        :param name: 目录名字
        :param desc: 目录描述
        :param dir_id: 目录id如果没有则表示不是从数据库读出来的
        """
        self.name = name
        self.desc = desc
        self.dir_id = dir_id


class ManagementRecord:
    """
    描述管理的类
    """

    def __init__(self, tag: str, dir_id: int, path: str = ''):
        self.tag = tag
        self.dir_id = dir_id
        self.path = path


class FileRecord:
    """
    描述一个文件的类
    """
    EMPTY_MD5 = '*' * 32
    # 128MB的读取缓存
    READ_BUFFER = 128 * 1024 * 1024

    def __init__(
            self,
            size: int,
            modified_time: float,
            md5: str = None,
            path: str = None,
            dir_path: str = None,
            name: str = None,
            suffix: str = None,
            directory_id: int = None,
            file_id: int = None,
            dir_physical_path: str = None
    ):
        """
        :param size: 该文件的大小，单位为字节
        :param modified_time: 文件的修改时间，整数时间戳
        :param md5: 文件的md5字符串，为空表示未计算
        :param path: 该文件相对于本程序管理的目录的根目录的路径
        :param dir_path: 该文件所在的目录相对与管理目录的路径
        :param name: 文件名
        :param suffix: 文件后缀
        :param directory_id: 所属的目录id，如果为None则表示不是从数据库读取出来的
        :param file_id: 文件记录的id，如果为None则表示不是从数据库读取出来的
        :param dir_physical_path: 文件所属目录的物理路径
        """
        assert size >= 0, f'文件{path}的大小为{size}不能小于等于0'
        assert isinstance(modified_time, int)
        if dir_path is None or name is None or suffix is None:
            assert path is not None
            dir_path, name, suffix = self.format_path(path)
        self.dir_path, self.name, self.suffix = dir_path, name, suffix
        self.path = f'{dir_path}{name}{suffix}'
        self.size = size
        self.modified_time = modified_time
        self.directory_id = directory_id
        self.file_id = file_id
        self.md5 = md5 or self.EMPTY_MD5
        self.dir_physical_path = dir_physical_path
        self._modified_date = None

    def __str__(self):
        return str({
            'dir_path': self.dir_path,
            'name': self.name,
            'size': self.size,
            'suffix': self.suffix,
            'modified_time': self.modified_time,
            'modified_date': self.modified_date,
            'md5': self.md5,
        })

    @property
    def modified_date(self) -> str:
        if self._modified_date is None:
            self._modified_date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self.modified_time))
        return self._modified_date

    def __repr__(self):
        return str(self)

    def compute_md5(self) -> str:
        """
        计算文件的md5
        :return: md5
        """
        m = hashlib.md5()
        assert self.dir_physical_path is not None, CodingError('计算md5值前dir_physical_path不能为空')
        with open(join(self.dir_physical_path, *(self.path.split('/')[1:])), 'rb') as file:
            while True:
                data = file.read(self.READ_BUFFER)
                if not data:
                    break
                m.update(data)
        self.md5 = m.hexdigest()
        return self.md5

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

    @classmethod
    def get_dir_file_records(cls, dir_path: str) -> List['FileRecord']:
        """
        获取指定路径目录下的所有文件对象
        :param dir_path: 目录路径
        :return: 该路径下的所有文件对应的File对象
        """
        dir_path = abspath(dir_path)
        file_paths = cls.get_file_paths_of_dir(dir_path)
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
                path=each.replace(dir_path, ''),
                size=os.path.getsize(each),
                modified_time=timestamp,
                md5='',
                dir_physical_path=dir_path
            )
            if record.dir_path.startswith('/.lyl232fm/'):
                continue
            res.append(record)
        return res
