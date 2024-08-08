from abc import abstractmethod, ABCMeta
from typing import Union, List, Dict, Tuple, Set
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
    def load_manage_info(lyl232fm_dir) -> dict:
        """
        加载管理目录里的信息
        :param lyl232fm_dir: 管理的目录的.lyl232fm路径
        :return: 信息字典
        """
        info_file = join(lyl232fm_dir, 'info')
        with open(info_file, 'r', encoding='utf8') as file:
            return json.load(file)

    @staticmethod
    def input_query(prompt: str):
        inputs = input(f'{prompt}\n输入"yes"，"y"，"是"确认执行该动作，其他输入将被视为不执行：').strip()
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
                # 改用反斜杠，因为不会出现在数据库里而逗号会
                res.append(line.split('\\'))
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
            if len(headers) > 0:
                # 使用反斜杠，因为不会出现在数据库里，而逗号会
                file.write('\\'.join(headers) + '\n')
            for each in data:
                assert len(each) == len(headers)
                # 使用反斜杠，因为不会出现在数据库里，而逗号会
                file.write('\\'.join([str(item) for item in each]) + '\n')

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

    @classmethod
    def cmd_ls(cls, cmd: str, outputs: List[str]):
        """
        ls命令的处理：询问将字符串列表输出到命令行还是指定文件中
        :param cmd 命令
        :param outputs: 待输出的命令
        :return: None
        """
        cmds = cmd.split(' ')
        assert len(cmds) <= 2, OperationError(f'ls命令只能接受一个参数，但是却收到了多个参数：{cmds[1:]}')
        cls.write_or_output_lines_to_file(outputs, cmds[1] if len(cmds) == 2 else None)

    @staticmethod
    def write_or_output_lines_to_file(lines: List[str], path: str = None):
        """
        将字符串列表输出到指定文件或者标准输出中
        :param lines: 字符串列表
        :param path: 写入的文件路径，如果为None则视为输出到控制台
        :return: None
        """
        if path is None:
            for each in lines:
                print(each)
            return
        assert not exists(path), OperationError(f'ls 将要输出的文件：{path}已经存在')
        try:
            with open(path, 'w', encoding='utf8') as file:
                for each in lines:
                    file.write(f'{each}\n')
            print(f'已写入{path}')
        except Exception as e:
            OperationError(f'无法写入文件：{path}，原因是：{e}')

    @classmethod
    def file_record_output_lines(cls, file_records: List[FileRecord]) -> List[str]:
        """
        将文件记录转换成对应的输出字符串列表
        :param file_records:
        :return: 字符串列表
        """
        outputs = []
        for record in file_records:
            path = f'{record.dir_path[1:]}{record.name}{record.suffix}'
            outputs.append(f'{path}\t{cls.human_readable_size(record.size)}\t{record.modified_date}')
        return outputs

    @staticmethod
    def _find_management_dir(path: str) -> Union[str, None]:
        """
        从一个路径开始往上查找.lyl232fm目录
        :param path: 路径
        :return: 如果找到一个父目录有.lyl232fm则返回该路径，否则返回None
        """
        current_path = abspath(path)
        while len(current_path) > 0 and exists(current_path):
            fm_dir = join(current_path, '.lyl232fm')
            if exists(fm_dir):
                return fm_dir
            next_path = dirname(current_path)
            if next_path == current_path:
                return None
            current_path = next_path
        return None


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
                name = self.load_manage_info(self._find_management_dir('.'))['name']
            except (JSONDecodeError, FileNotFoundError) as e:
                print(e)
                raise OperationError(f'请指定查询的目录名字')
        dir_id = self.db.directory_id(name)
        assert dir_id is not None, OperationError(f'目录名字{name}不存在于数据库中，无法操作')
        return dir_id

    @classmethod
    def query_actions(
            cls,
            prompt: str,
            exact_response_actions: Dict[str, Tuple[str, callable]] = None,
            cmd_response_actions: Dict[str, Tuple[str, str, callable]] = None
    ):
        """
        询问用户并根据回应执行动作
        :param prompt: 提示
        :param exact_response_actions:
            根据回应应该做的动作，要求精确匹配，动作是一个不接收任何参数，返回布尔值的函数，返回True表示退出询问，否则继续询问
            [输入] -> (动作说明，动作函数)
        :param cmd_response_actions:
            根据回应应该做的动作，命令式的回应，以字典键作为开头进行匹配，动作是一个接收字符串，返回布尔值的函数，返回True表示退出询问，否则继续询问
            [输入] -> (参数说明，动作说明，动作函数)
        :return:
        """
        exact_response_actions = exact_response_actions or {}
        cmd_response_actions = cmd_response_actions or {}
        assert len(exact_response_actions) + len(cmd_response_actions) > 0, CodingError('请至少给一个回应动作')
        skip_action = ('无视并不再询问', cls._query_action_skip)
        exact_response_actions.update(s=skip_action, skip=skip_action)

        # 生成提示
        action_keys_hint = {}
        for key in sorted(list(exact_response_actions.keys())):
            hint, action = exact_response_actions[key]
            a_id = id(action)
            keys_hint = action_keys_hint.get(a_id, ([], None, hint))
            keys_hint[0].append(key)
            action_keys_hint[a_id] = keys_hint
        for key in sorted(list(cmd_response_actions.keys())):
            args_hint, hint, action = cmd_response_actions[key]
            a_id = id(action)
            keys_hint = action_keys_hint.get(a_id, ([], args_hint, hint))
            keys_hint[0].append(key)
            action_keys_hint[a_id] = keys_hint
        keys_hints = []
        for key_list, args_hint, hint in action_keys_hint.values():
            key_list.sort()
            keys_hints.append((key_list, args_hint, hint))
        keys_hints.sort(key=lambda x: x[0][0])
        for key_list, args_hint, hint in keys_hints:
            prompt += f'\n{" ".join(key_list)} {args_hint or ""}: {hint}'
        prompt += '\n其他输入将被视为无效输入并将继续询问\n'

        while True:
            inputs = input(prompt).strip()
            prompt = 'lyl232fm:'
            _, action = exact_response_actions.get(inputs, (None, None))
            if action is not None:
                if action():
                    break
                else:
                    continue
            for cmd, (_, _, act) in cmd_response_actions.items():
                if inputs.startswith(cmd):
                    action = act
                    break
            if action is not None:
                if action(inputs):
                    break
                else:
                    continue
            prompt = '无法识别该命令，请重新输入：'

    @staticmethod
    def _query_action_skip():
        """
        根据用户回答执行动作：跳过 是个空函数
        :return: True
        """
        return True

    def _query_safely_delete_file_records(self, records: List[FileRecord]):
        """
        安全地删除文件记录，在文件记录删除前查询其是否是唯一的，如果是唯一的，则询问是否删除
        :param records: 文件记录
        :return: None
        """
        to_delete = []
        not_save_to_delete = []
        for record in records:
            assert record.md5 != FileRecord.EMPTY_MD5 and record.file_id is not None, \
                CodingError('检查文件记录是否可以安全删除时需要保证该文件记录的md5值和文件id是有效的')
            same_ids = set(self.db.query_file_ids_by_size_and_md5(size=record.size, md5=record.md5))
            file_id = record.file_id
            assert file_id in same_ids, RunTimeError(f'文件记录与数据库不一致：{record}对应的数据库文件记录不存在')
            for each in to_delete:
                if each.file_id in same_ids:
                    same_ids.remove(each.file_id)
            same_ids.remove(file_id)
            to_delete.append(record)
            if len(same_ids) == 0:
                not_save_to_delete.append(record)

        if len(to_delete) == 0:
            return
        if len(not_save_to_delete) > 0:
            for record in not_save_to_delete:
                print(record.path)
            if self.input_query(f'上述的文件记录内容在数据库中没有记录的备份，请问是否删除这些文件记录？'):
                to_delete.extend(not_save_to_delete)
        deleted = self.transaction(
            self.db.delete_file_record_by_ids,
            file_ids=[each.file_id for each in to_delete]
        )
        print(f'删除了{deleted}条文件记录')


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
        for record in tqdm(records, desc='计算文件md5值', disable=len(records) < 5):
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
