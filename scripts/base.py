from abc import abstractmethod, ABCMeta
from typing import Union, List
import json
from json.decoder import JSONDecodeError
import os
from os.path import join, dirname, exists, isdir, abspath
import time
from tqdm import tqdm

from database import DATABASE_CLASS, Database
from error import ArgumentError, CodingError, RunTimeError, OperationError
from record import FileRecord


class BaseScript(metaclass=ABCMeta):
    """
    脚本类：实现__call__方法并返回一个整数
    """

    def __init__(self, *args, **kwargs):
        self.base_args = args
        self.base_kwargs = kwargs

    @abstractmethod
    def __call__(self, *args) -> int:
        pass

    @classmethod
    def check_empty_args(cls, *args) -> bool:
        """
        检查位置参数args是否为空
        :param args: 位置参数
        :return: 是否为空
        """
        assert len(args) == 0 or ArgumentError(f'{cls.__name__}收到了额外的运行参数：{args}')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    @staticmethod
    def load_manage_info(dir_path) -> dict:
        """
        加载
        :param dir_path: 管理的目录的路径
        :return: 信息字典
        """
        info_file = join(dir_path, '.lyl232fm', 'info')
        with open(info_file, 'r', encoding='utf8') as file:
            return json.load(file)

    @staticmethod
    def input_query(prompt: str):
        inputs = input(f'{prompt}\n输入"yes"，"y"，"是"确认执行该动作：')
        if inputs == 'y' or inputs == 'yes' or inputs == '是':
            return True
        return False

    @staticmethod
    def human_readable_size(byte_size: int) -> str:
        if byte_size < 1024.0:
            return f'{byte_size}B'
        byte_size /= 1024.0
        if byte_size < 1024:
            return '%.2fKB' % byte_size
        byte_size /= 1024.0
        if byte_size < 1024:
            return '%.2fMB' % byte_size
        byte_size /= 1024.0
        if byte_size < 1024:
            return '%.2fGB' % byte_size
        byte_size /= 1024.0
        return '%.2fTB' % byte_size

    @staticmethod
    def read_csv(path: str, ignore_header: bool = False) -> List[tuple]:
        """
        :param path: 路径
        :param ignore_header: 是否忽略表头
        :return: 读取出的数据元组列表
        """
        with open(path, 'r', encoding='utf8') as file:
            res = []
            while True:
                line = file.readline()
                if len(line) == 0:
                    break
                if ignore_header:
                    ignore_header = False
                    continue
                line = line.strip()
                if len(line) == 0:
                    continue
                res.append(line.split(','))
        return res

    @staticmethod
    def write_csv(path: str, data: List[tuple], headers: List[str] = None) -> None:
        """
        :param path: 路径
        :param data: 数据
        :param headers: 表头
        :return: None
        """
        headers = headers or []
        with open(path, 'w', encoding='utf8') as file:
            if len(headers) == 0:
                file.write(','.join(headers) + '\n')
            for each in data:
                assert len(each) == len(headers)
                file.write(','.join([str(item) for item in each]) + '\n')

    @staticmethod
    def _write_manage_info(dir_path, name: str, tag: str):
        """
        向管理目录.lyl232fm写入管理信息
        :param dir_path: 管理的目录的路径
        :param name: 目录名称
        :param tag: 管理标签
        :return: None
        """
        fm_dir = join(dir_path, '.lyl232fm')
        try:
            os.makedirs(fm_dir, exist_ok=True)
            with open(join(fm_dir, 'info'), 'w', encoding='utf8') as file:
                json.dump({
                    'name': name,
                    'tag': tag
                }, file, ensure_ascii=False, indent=2)
        except Exception as e:
            RunTimeError(f'创建本程序管理目录：{fm_dir}出错，具体异常为：\n{e}')

    @classmethod
    def remove_single_file(cls, path: str):
        """
        删除单个文件，并检查父目录是否为空，如果为空则询问是否删除
        :param path: 需要删除的文件目录
        :return:
        """
        if not cls.input_query(f'将删除：{path}，是否继续？'):
            return
        os.remove(path)
        dir_path = dirname(path)
        while exists(dir_path) and len(os.listdir(dir_path)) == 0:
            if not cls.input_query(f'将删除空目录{dir_path}，是否继续？'):
                break
            os.rmdir(dir_path)
            dir_path = dirname(dir_path)

    @classmethod
    def check_empty_dir(cls, path: str) -> bool:
        """
        检测路径下是否存在空目录
        :param path: 路径
        :return: 返回检测路径下是否存在空目录
        """
        if not isdir(path):
            return False
        if len(os.listdir(path)) == 0:
            return True
        for each in os.listdir(path):
            file = join(path, each)
            if isdir(file):
                if cls.check_empty_dir(file):
                    return True
        return False

    @classmethod
    def remove_empty_dir(cls, path: str) -> bool:
        """
        检测路径下的空目录并询问是否删除
        :param path: 路径
        :return: 返回这个路径是否是空目录
        """
        if not exists(path):
            return False
        if len(os.listdir(path)) == 0:
            return True
        res = True
        for each in os.listdir(path):
            file = join(path, each)
            if isdir(file):
                if cls.remove_empty_dir(file):
                    if cls.input_query(f'将删除{file}，是否继续？'):
                        assert len(os.listdir(file)) == 0, CodingError('要删除的必须是空目录')
                        os.rmdir(file)
                    else:
                        res = False
                else:
                    res = False
            else:
                res = False
        return res


class DataBaseScript(BaseScript, metaclass=ABCMeta):
    def __init__(self, database_config: Union[str, dict], *args, database: Database = None, **kwargs):
        super().__init__(*args, **kwargs)
        if isinstance(database_config, str):
            with open(database_config, 'r', encoding='utf8') as file:
                database_config = json.load(file)
        assert isinstance(database_config, dict)
        self.database_config = database_config
        self._db = database

    def init_db_if_needed(self):
        """
        在需要的时候初始化数据库
        :return:
        """
        if not self.db.is_initialized():
            self.db.initialize()

    @property
    def db(self) -> Database:
        assert self._db is not None or CodingError(f'请使用 with as 语法使用类{type(self)}或者在构造时传入Database对象')
        return self._db

    def __enter__(self):
        database_config = self.database_config.copy()
        database = database_config.pop('database')
        self._db = DATABASE_CLASS[database](**database_config)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.db.close()

    def transaction(self, func: callable, *args, **kwargs):
        transaction = self.db.begin_transaction()
        try:
            res = func(*args, **kwargs)
            transaction.commit()
            return res
        except Exception as e:
            transaction.rollback()
            raise e

    def get_directory_id_by_name_or_local(self, name: str = None):
        """
        通过输入的name参数或者本地的.lyl232fm文件夹里的信息获取目录id
        :param name: 目录名字
        :return: 目录id如果获取不到会抛出异常
        """
        self.init_db_if_needed()
        if name is None:
            try:
                name = self.load_manage_info(abspath('.'))['name']
            except (JSONDecodeError, FileNotFoundError) as e:
                print(e)
                raise OperationError(f'请指定查询的目录名字')
        dir_id = self.db.directory_id(name)
        assert dir_id is not None, OperationError(f'目录名字{name}不存在于数据库中，无法操作')
        return dir_id


class FileMD5ComputingScript(DataBaseScript, metaclass=ABCMeta):
    # 计算md5时多少秒写入数据库一次
    MD5_COMPUTING_SAVE_FREQUENCY = 3

    def file_md5_computing_transactions(self, records: List[FileRecord], func, *args, **kwargs) -> list:
        """
        分批次地计算文件的MD5值并存入数据库中，防止MD5计算时间太久导致很多计算资源白白浪费
        :param records: 需要进行操作的文件记录列表
        :param func: 数据库更新函数
        :param args: 数据库更新函数需要的位置参数
        :param kwargs: 数据库更新函数需要的键值参数
        :return: 每个批次执行后的结果列表
        """
        res, batch = [], []
        last_commit_time = time.time()
        for record in tqdm(records, desc='计算文件md5值'):
            record.compute_md5()
            batch.append(record)
            if time.time() - last_commit_time > self.MD5_COMPUTING_SAVE_FREQUENCY:
                res.append(self.transaction(func, *args, **kwargs, file_records=batch))
                batch = []
                last_commit_time = time.time()
        if len(batch) > 0:
            res.append(self.transaction(func, *args, **kwargs, file_records=batch))
        return res


class SingleTransactionScript(DataBaseScript, metaclass=ABCMeta):
    """
    改变数据库的脚本，在DataBaseScript的基础上进行了事务包装
    """

    def __call__(self, *args) -> int:
        self.init_db_if_needed()
        self.before_transaction(*args)
        transaction = self.db.begin_transaction()
        try:
            res = self.transaction_action(*args)
            transaction.commit()
            self.after_transaction_commit(*args)
            return res
        except Exception as e:
            transaction.rollback()
            raise e

    def before_transaction(self, *args):
        """
        在事务开始前的动作
        :param args: 脚本运行时参数
        :return: None
        """

    @abstractmethod
    def transaction_action(self, *args) -> int:
        """
        需要进行的事务操作
        :param args: 脚本参数
        :return: 脚本返回值
        """

    def after_transaction_commit(self, *args):
        """
        事务成功提交后的动作
        :param args: 脚本运行时参数
        :return: None
        """
