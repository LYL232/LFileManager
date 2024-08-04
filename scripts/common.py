"""
常规的脚本：经常使用的
"""
import os
from os.path import isdir, join, exists, abspath, samefile, dirname
from json.decoder import JSONDecodeError
from typing import Set, Dict, Tuple, List
from tqdm import tqdm
import shutil
from abc import abstractmethod, ABCMeta

from scripts import DataBaseScript, SingleTransactionScript, FileMD5ComputingScript
from error import OperationError, RunTimeError, CodingError
from record import FileRecord


class MakeDirectoryScript(SingleTransactionScript):
    def before_transaction(self, name: str, desc: str, *args):
        self.check_empty_args(*args)
        assert 0 < len(name) <= 255 and 0 < len(desc) <= 255, OperationError('名字和描述不能为空，且长度不能超过255')

    def transaction_action(self, name: str, desc: str) -> int:
        """
        创建一个新目录脚本
        :param name: 目录名字
        :param desc: 目录描述
        :return: 0表示正常
        """
        assert not self.db.directory_id(name) is not None, OperationError(f'目录名字：{name}已经被创建')
        self.db.make_directory(name, desc)
        return 0


class RemoveDirectoryScript(SingleTransactionScript):
    def before_transaction(self, name: str, *args):
        self.check_empty_args(*args)
        assert 0 < len(name) <= 255, OperationError('名字不能为空，且长度不能超过255')

    def transaction_action(self, name: str) -> int:
        """
        删除一个目录脚本
        :param name: 目录名字
        :return: 0表示正常
        """
        assert self.db.remove_directory(name) == 1, OperationError(f'目录名字：{name}不存在或者不为空，无法删除')
        return 0


class QueryDirectoryScript(DataBaseScript):
    def __call__(self, name: str = None, *args) -> int:
        """
        查询所有被管理的目录的脚本
        :param directory: 目录名字，如果为空，则为查询所有目录
        :param args: 其他参数，应为空
        :return: 0表示执行正常
        """
        self.check_empty_args(*args)
        self.init_db_if_needed()
        if name is None:
            for directory in self.db.directories():
                print(f'目录：{directory.name}，描述：{directory.desc}，id：{directory.dir_id}')
        else:
            assert self.db.directory_id(name) is not None, OperationError(f'目录名字：{name}不存在')
            for tag, path in self.db.managements(name):
                if exists(path):
                    print(f'标识：{tag}，路径：{path}')
                else:
                    print(f'标识：{tag}，路径：{path}（不存在）')
        return 0


class ManageDirectoryScript(FileMD5ComputingScript):
    def __call__(self, dir_path: str = '.', name: str = None, tag: str = None, *args) -> int:
        """
        管理一个新的物理目录的脚本
        :param dir_path: 目录的路径
        :param name: 目录名字
        :param tag: 目录标识，不能与其他标识重复
        :return: 0表示正常
        """
        self.check_empty_args(*args)
        self.init_db_if_needed()
        dir_path = abspath(dir_path)
        assert tag is None or 0 < len(tag) <= 255, OperationError('管理标识允许的最大长度为255')
        assert isdir(dir_path), OperationError(f'{dir_path}不是一个目录')

        dir_id, dir_path = self.maintain_management(dir_path, name, tag)

        # 获取当前目录的所有文件信息记录
        db_records = self.db.file_records(dir_id)
        local_records = FileRecord.get_dir_file_records(dir_path)
        if len(db_records) == 0:
            total_size = sum(each.size for each in local_records)
            if self.input_query(
                    f'有{len(local_records)}个文件共{self.human_readable_size(total_size)}，'
                    f'是否计算md5并更新至数据库中？'
            ):
                created_rows = sum(self.file_md5_computing_transactions(
                    local_records, self.db.new_file_records, dir_id=dir_id))
                assert created_rows == len(local_records), RunTimeError(
                    f'在往数据库写入数据后，理应写入{len(local_records)}行记录，但只写入了{created_rows}行'
                )
            else:
                created_rows = self.transaction(self.db.new_file_records, dir_id=dir_id, file_records=local_records)
            print(f'更新了{created_rows}条记录')
            return 0
        self._compare_local_records_to_db_records(dir_path, dir_id, local_records, db_records)
        if self.check_empty_dir(dir_path) and self.input_query('检测到存在空目录，是否删除它们？'):
            self.remove_empty_dir(dir_path)
        return 0

    def maintain_management(self, dir_path: str, name: str, tag: str):
        """
        维护目录管理信息
        :param dir_path: 目录的物理路径
        :param name: 目录名字
        :param tag: 目录标签
        :return: 目录id
        """
        fm_dir = self._find_management_dir(dir_path)
        if fm_dir is None:
            # fm_dir不存在
            assert name is not None, OperationError(f'目录名字缺失，而且目标路径{dir_path}不存在.lyl232fm文件夹，无法操作')
            assert tag is not None, OperationError(f'目录管理标识缺失，而且目标路径{dir_path}不存在.lyl232fm文件夹，无法操作')

            dir_id = self.db.directory_id(name)
            assert dir_id is not None, OperationError(f'管理目录名字{name}并未被注册，无法关联，请使用mkdir脚本创建新的管理目录')
            assert not self.db.tag_exists(tag), OperationError(f'标识：{tag}已经存在，无法关联')
            self._write_manage_info(dir_path, name, tag)
            self.transaction(self._create_or_update_management, dir_id=dir_id, tag=tag, dir_path=dir_path)
        else:
            dir_path = dirname(fm_dir)
            try:
                info = self.load_manage_info(fm_dir)
            except (JSONDecodeError, FileNotFoundError):
                raise OperationError(
                    f'该目录下存在由本程序维护的.lyl232fm文件夹：{fm_dir}，但无法读取出有效信息，请删除该.lyl232fm文件夹')

            if (name is not None and info['name'] != name) or (
                    tag is not None and info['tag'] != tag
            ):
                raise OperationError(
                    f'该目录属于目录{info["name"]}且标识为{info["tag"]}。'
                    f'而不是指定的目录{name}且管理标识为{tag}，无法操作'
                )
            # 更新数据库中管理的path字段
            name, tag = name or info['name'], tag or info['tag']
            dir_id = self.db.directory_id(name)
            self.transaction(self._create_or_update_management, dir_id=dir_id, tag=tag, dir_path=dir_path)
            assert dir_id is not None, OperationError(f'管理目录名字{name}并未被注册，无法关联，请使用mkdir脚本创建新的管理目录')
        return dir_id, dir_path

    def _compare_local_records_to_db_records(
            self,
            dir_path: str,
            dir_id: int,
            local_records: List[FileRecord],
            db_records: List[FileRecord]
    ):
        """
        比较当前文件记录与数据库文件记录，并进行相应动作
        :param dir_path: 当前目录的物理路径
        :param dir_id: 当前目录id
        :param local_records: 当前文件记录
        :param db_records: 数据库文件记录
        :return:
        """
        local_records = {each.path: each for each in local_records}
        db_records = {each.path: each for each in db_records}
        current_paths = set(local_records.keys())
        db_file_path = set(db_records.keys())
        common_path = current_paths.intersection(db_file_path)
        current_unique = current_paths - db_file_path
        db_unique = db_file_path - current_paths

        self._common_path_records_action(dir_path, dir_id, common_path, local_records, db_records)

        self._unique_db_records_action(dir_path, dir_id, db_unique, db_records)
        self._unique_local_records_action(dir_path, dir_id, current_unique, local_records)

    def _unique_local_records_action(
            self,
            dir_path: str,
            dir_id: int,
            unique_paths: Set[str],
            local_records: Dict[str, FileRecord],
    ):
        """
        本地存在，但数据库不存在的文件记录动作
        :param dir_path: 正在操作的目录的物理路径
        :param dir_id: 目录id
        :param unique_paths: 需要处理的路径集合
        :param local_records: 本地文件记录
        :return:
        """
        if len(unique_paths) == 0:
            return

        def action_a():
            file_records = [local_records[each] for each in unique_paths]
            if self.input_query(
                    f'这些文件大小共为{self.human_readable_size(sum([each.size for each in file_records]))}，'
                    f'是否在输入数据库前计算md5值？'
            ):
                created = sum(self.file_md5_computing_transactions(
                    file_records, self.db.new_file_records, dir_id=dir_id,
                ))
            else:
                created = self.transaction(self.db.new_file_records, dir_id=dir_id, file_records=file_records)
            assert created == len(file_records), \
                RunTimeError(f'理应插入{len(file_records)}条数据库文件记录，但只插入了{created}条')
            print(f'插入了{created}条文件记录')
            return True

        def action_b():
            for path in unique_paths:
                self.remove_single_file(join(dir_path, *(path.split('/')[1:])))
            return True

        def action_c():
            count = 1
            for path in unique_paths:
                print(f'({count}/{len(unique_paths)})')
                self._unique_local_records_query_each_action(dir_path, dir_id, path, local_records[path])
                count += 1
            return True

        def action_ls(inputs):
            self.cmd_ls(inputs, sorted(list(unique_paths)))
            return False

        self.query_actions(
            f'数据库相较本地中缺失{len(unique_paths)}条文件记录，对于这些文件记录请问需要作何处理？',
            {
                'a': ('将这些文件记录更新到数据库中', action_a),
                'b': ('【注意！】删除本地的这些文件', action_b),
                'c': ('每条记录单独询问', action_c),
            },
            {'ls': ('列出这些文件的目录路径 [写入指定的文件中]', '[写入文件路径（可选）]', action_ls)}
        )

    def _unique_local_records_query_each_action(self, dir_path: str, dir_id: int, path: str, record: FileRecord):
        """
        对于一个本地独有的文件记录的询问操作
        :param dir_path: 正在操作的目录路径
        :param dir_id: 目录id
        :param path: 文件相对路径
        :param record: 文件记录
        :return: None
        """
        print(path)

        def action_a():
            if self.input_query(
                    f'该文件大小为{self.human_readable_size(record.size)}，'
                    f'是否在输入数据库前计算md5值？'
            ):
                created = self.file_md5_computing_transactions(
                    [record], self.db.new_file_records, dir_id=dir_id,
                )
            else:
                created = self.transaction(self.db.new_file_records, dir_id=dir_id, file_records=[record])
            assert created == 1, RunTimeError(f'理应插入1条数据库文件记录，但插入了{created}条')
            print(f'插入了{created}条文件记录')
            return True

        def action_b():
            self.remove_single_file(join(dir_path, *(path.split('/')[1:])))
            return True

        self.query_actions(
            f'请问对上述本地独有的文件记录需要作何处理？',
            {
                'a': ('将该文件记录更新到数据库中', action_a),
                'b': ('【注意！】删除该文件记录对应的文件', action_b),
            }
        )

    def _unique_db_records_action(
            self,
            dir_path: str,
            dir_id: int,
            unique_paths: Set[str],
            db_records: Dict[str, FileRecord],
    ):
        """
        数据库存在，但本地不存在的文件记录动作
        :param dir_path: 正在操作的目录的物理路径
        :param dir_id: 目录id
        :param unique_paths: 需要处理的路径集合
        :param db_records: 数据库文件记录
        :return:
        """
        if len(unique_paths) == 0:
            return

        def action_a():
            ids = [db_records[path].file_id for path in unique_paths]
            assert all(each is not None for each in ids), CodingError('数据库中的文件记录中的文件id不应该为None')
            for path in sorted(list(unique_paths)):
                print(path)
            if self.input_query('将删除上述文件在数据库中的记录，是否继续？'):
                deleted = self.transaction(
                    self.db.delete_file_record_by_ids,
                    file_ids=ids
                )
                print(f'删除了{deleted}条文件记录')
            return True

        def action_b():
            file_records = [db_records[path] for path in unique_paths]
            self._unique_db_records_copy_from_others(dir_path, dir_id, file_records)
            return True

        def action_c():
            count = 1
            for path in unique_paths:
                print(f'({count}/{len(unique_paths)})')
                self._unique_db_records_query_each_action(dir_path, dir_id, path, db_records[path])
                count += 1
            return True

        def action_ls(inputs):
            self.cmd_ls(inputs, sorted(list(unique_paths)))
            return False

        self.query_actions(
            f'本地相较数据库中缺失{len(unique_paths)}条文件记录，请问对这些文件记录要做何操作？',
            {
                'a': ('【注意！】删除所有数据库中的相关文件记录', action_a),
                'b': ('尝试从其他同目录的受管理物理位置复制这些文件到本地', action_b),
                'c': ('单独询问每条文件记录', action_c),
            },
            {
                'ls': ('列出这些文件的目录路径 [写入指定的文件中]', '[写入文件路径（可选）]', action_ls)
            }
        )

    def _unique_db_records_query_each_action(
            self,
            dir_path: str,
            dir_id: int,
            path: str,
            record: FileRecord
    ):
        print(path)

        def action_a():
            assert record.file_id is not None, CodingError('数据库中的文件记录中的文件id不应该为None')
            if self.input_query('将删除文件记录在数据库中的记录，是否继续？'):
                deleted = self.transaction(
                    self.db.delete_file_record_by_ids,
                    file_ids=[record.file_id]
                )
                print(f'删除了{deleted}条文件记录')
            return True

        def action_b():
            self._unique_db_records_copy_from_others(dir_path, dir_id, [record])
            return True

        self.query_actions(
            f'请问对上述冲突记录需要作何处理？',
            {
                'a': ('【注意！】删除数据库中的该文件记录', action_a),
                'b': ('尝试从其他同目录的受管理物理位置复制该文件到本地', action_b),
            }
        )

    def _get_valid_other_management_paths(self, dir_path: str, dir_id: int) -> List[str]:
        """
        获取指定目录其他有效的物理路径
        :param dir_path: 正在操作的目录路径（返回的结果将不包含该目录路径）
        :param dir_id: 目录id
        :return: 其他有效的物理路径
        """
        other_dir_paths = []
        not_exist_path_tags = []
        # 这里假设其他物理位置下的路径的文件都是与数据库一致的
        for tag, path in self.db.managements(dir_id):
            if samefile(path, dir_path):
                continue
            if not exists(path):
                not_exist_path_tags.append(tag)
                continue
            other_dir_paths.append(path)
        if len(not_exist_path_tags):
            self.transaction(self.db.reset_management_path, tags=not_exist_path_tags)
        return other_dir_paths

    def _find_file_in_other_managements(
            self, dir_path: str, dir_id: int, records: List[FileRecord]
    ) -> Tuple[Dict[str, str], Set[str]]:
        """
        在其他的管理记录中找到有效的指定文件记录的备份
        :param dir_path: 正在操作的目录路径
        :param dir_id: 目录id
        :param records: 需要操作的文件记录列表
        :return: ([本地路径->其他有效备份的路径], {找不到的路径})
        """
        other_dir_paths = self._get_valid_other_management_paths(dir_path, dir_id)

        found_file_paths, not_found_paths = {}, set()
        # 尝试寻找所有的文件路径
        for record in records:
            path = record.path
            found = self._find_single_file_in_other_managements(record, other_dir_paths)
            local_real_path = join(dir_path, *(path.split('/')[1:]))
            if found is None:
                not_found_paths.add(path)
            else:
                found_file_paths[local_real_path] = found
        return found_file_paths, not_found_paths

    @staticmethod
    def _find_single_file_in_other_managements(
            record, other_dir_paths: List[str]
    ) -> Tuple[Dict[str, str], Set[str]]:
        """
        在其他的管理记录中找到有效的指定文件记录的备份
        :param record: 需要操作的文件记录
        :param other_dir_paths: 本目录其他有效的物理路径
        :return: ([本地路径->其他有效备份的路径], {找不到的路径})
        """
        for dp in other_dir_paths:
            real_path = join(dp, *(record.path.split('/')[1:]))
            if exists(real_path):
                return real_path
        return None

    def _unique_db_records_copy_from_others(
            self, dir_path, dir_id: int, records: List[FileRecord]
    ):
        """
        数据库有但本地缺失的文件记录，采取从其他位置复制而来的动作
        :param dir_path: 当前正在操作的目录路径
        :param dir_id: 目录id
        :param records: 需要处理的文件记录
        :return: 没法找到物理位置的文件记录
        """
        other_dir_paths = []
        not_exist_path_tags = []
        # 这里假设其他物理位置下的路径的文件都是与数据库一致的
        for tag, path in self.db.managements(dir_id):
            if samefile(path, dir_path):
                continue
            if not exists(path):
                not_exist_path_tags.append(tag)
                continue
            other_dir_paths.append(path)
        if len(not_exist_path_tags):
            self.transaction(self.db.reset_management_path, tags=not_exist_path_tags)

        found_file_paths, not_found_paths = self._find_file_in_other_managements(
            dir_path, dir_id, records
        )

        if len(found_file_paths) > 0:
            local_paths = sorted(list(found_file_paths.keys()))
            for local_real_path in local_paths:
                real_path = found_file_paths[local_real_path]
                print(f'{real_path} -> {local_real_path}')
            if self.input_query('将执行上述文件的复制，是否继续？'):
                total = len(found_file_paths)
                for local_real_path, real_path in tqdm(
                        found_file_paths.items(), total=total, desc='复制文件', disable=total < 3
                ):
                    assert not exists(local_real_path), \
                        CodingError(f'复制文件的目标路径不应该存在文件：{local_real_path}')
                    os.makedirs(os.path.dirname(local_real_path), exist_ok=True)
                    shutil.copy2(real_path, local_real_path)
        if len(not_found_paths) > 0:
            for each in not_found_paths:
                print(each)
            print(
                '【注意！】：上述文件在本地目录中缺失但数据库中存在相应记录，'
                '而且无法在本机上的其他受管理的目录找到，请手动将这些文件复制到对应的本地目录下，'
                '或者接入有相关文件记录的设备并重新使用本程序管理'
            )

    def _common_path_records_action(
            self,
            dir_path: str,
            dir_id: int,
            common_path: Set[str],
            local_records: Dict[str, FileRecord],
            db_records: Dict[str, FileRecord]
    ):
        """
        对现有文件记录与数据库文件记录中路径相同的部分的动作
        :param dir_path: 当前操作的目录路径
        :param dir_id: 当前操作的目录id
        :param common_path: 两者相同路径的集合
        :param local_records: 当前的文件记录
        :param db_records: 数据库中的文件记录
        :return:
        """
        if len(common_path) == 0:
            return
        conflict, match_with_md5, match_wo_md5 = {}, {}, {}
        for path in common_path:
            record_pair = (local_record, db_record) = (local_records[path], db_records[path])
            local_record.file_id = db_record.file_id
            db_record.dir_physical_path = local_record.dir_physical_path
            if local_record.size == db_record.size:
                if local_record.modified_time == db_record.modified_time:
                    if db_record.md5 != FileRecord.EMPTY_MD5:
                        # 如果数据库中有md5记录
                        match_with_md5[path] = record_pair
                    else:
                        # 如果数据库中没有md5记录则在之后询问是否计算这些文件的md5值
                        match_wo_md5[path] = record_pair
                else:
                    conflict[path] = record_pair
            else:
                conflict[path] = record_pair

        self._common_path_match_without_db_md5_action(match_wo_md5)
        conflict.update(self._common_path_match_with_db_md5_action(match_with_md5))

        self._common_path_conflict_action(dir_path, dir_id, conflict)

    def _common_path_conflict_action(
            self,
            dir_path: str,
            dir_id: int,
            path2records: Dict[str, Tuple[FileRecord, FileRecord]]
    ):
        """
        拥有相同路径的本地文件记录与数据库文件记录相冲突的文件记录对
        :param dir_path: 当前操作的目录路径
        :param dir_id: 当前操作的目录路径
        :param path2records: 路径到记录对的映射
        :return:
        """
        if len(path2records) == 0:
            return

        def action_a():
            for each in sorted(list(path2records.keys())):
                print(each)
            if not self.input_query('上述文件记录将被计算md5值并更新入数据库。是否继续？'):
                return False
            records = [local_record for local_record, _ in path2records.values()]
            updated = sum(self.file_md5_computing_transactions(records, self.db.update_file_records))
            print(f'更新了{updated}条数据库记录')
            return True

        def action_b():
            for each in sorted(list(path2records.keys())):
                print(each)
            if not self.input_query('上述文件记录更新入数据库。是否继续？'):
                return False
            records = [local_record for local_record, _ in path2records.values()]
            updated = self.transaction(self.db.update_file_records, file_records=records)
            print(f'更新了{updated}条数据库记录')
            return True

        def action_c():
            count = 1
            for path, (local, db) in path2records.items():
                print(f'({count}/{len(path2records)})')
                self._common_path_conflict_query_each_action(dir_path, dir_id, path, local, db)
                count += 1
            return True

        def action_ls(inputs):
            outputs = []
            for path in sorted(list(path2records.keys())):
                local, db = path2records[path]
                output = path
                if local.size != db.size:
                    output += f'\t大小(本地/数据库)({local.size}/{db.size})'
                if local.modified_time != db.modified_time:
                    output += f'\t修改时间(本地/数据库)({local.modified_date}/{db.modified_date})'
                outputs.append(output)
            self.cmd_ls(inputs, outputs)
            return False

        self.query_actions(
            f'【注意！】共有{len(path2records)}项本地文件记录的文件与数据库中相应记录冲突：请问需要作何处理？',
            {
                'a': ('以本地文件记录覆盖数据库中的记录并计算md5值', action_a),
                'b': ('以本地文件记录覆盖数据库中的记录但不计算md5值', action_b),
                'c': ('每条记录单独询问', action_c),
            },
            {
                'ls': ('列出这些文件的目录路径 [写入指定的文件中]', '[写入文件路径（可选）]', action_ls)
            }
        )

    def _common_path_conflict_query_each_action(
            self, dir_path: str, dir_id: int, path: str,
            local_record: FileRecord, db_record: FileRecord
    ):
        output = path
        if local_record.size != db_record.size:
            output += f'\n大小(本地/数据库)：({local_record.size}/{db_record.size})'
        if local_record.modified_time != db_record.modified_time:
            output += f'\n修改时间(本地/数据库)：({local_record.modified_date}/{db_record.modified_date})'
        print(output)

        def action_a():
            updated = sum(self.file_md5_computing_transactions([local_record], self.db.update_file_records))
            print(f'更新了{updated}条数据库记录')
            return True

        def action_b():
            updated = self.transaction(self.db.update_file_records, file_records=[local_record])
            print(f'更新了{updated}条数据库记录')
            return True

        def action_c():
            other_dir_paths = self._get_valid_other_management_paths(dir_path, dir_id)
            real_split_path = (path.split('/')[1:])
            file_other_real_path = self._find_single_file_in_other_managements(local_record, other_dir_paths)
            file_real_path = join(dir_path, *real_split_path)
            if file_other_real_path is not None:
                if not self.input_query(
                        f'将删除：{file_real_path}，并复制{file_other_real_path}到被删除文件的位置，是否继续？'
                ):
                    return False
                os.remove(file_real_path)
                shutil.copy2(file_other_real_path, file_real_path)
            else:
                self.remove_single_file(file_real_path)
            return True

        self.query_actions(
            f'请问对上述冲突记录需要作何处理？',
            {
                'a': ('以本地文件记录覆盖数据库中的记录并计算md5值', action_a),
                'b': ('以本地文件记录覆盖数据库中的记录但不计算md5值', action_b),
                'c': ('删除本地的文件，并尝试从其他物理位置复制该文件', action_c),
            }
        )

    def _common_path_match_without_db_md5_action(
            self,
            path2records: Dict[str, Tuple[FileRecord, FileRecord]]
    ):
        """
        对于当前文件与数据库文件中大小，修改日期都匹配的文件记录且数据库对应记录没有md5记录的动作
        :param path2records: 路径到文件记录的映射
        :return:
        """
        if len(path2records) == 0:
            return
        total_size = sum(each[0].size for each in path2records.values())
        if not self.input_query(
                f'有{len(path2records)}个文件共{self.human_readable_size(total_size)}与数据库中记录相匹配，'
                f'且数据库中没有md5记录，是否计算本地文件的md5并更新至数据库记录中？'
        ):
            return
        records = [db_record for local_record, db_record in path2records.values()]
        print(
            f'更新了'
            f'{sum(self.file_md5_computing_transactions(records, self.db.update_file_records))}'
            f'条数据库记录'
        )

    def _common_path_match_with_db_md5_action(
            self,
            path2records: Dict[str, Tuple[FileRecord, FileRecord]]
    ) -> Dict[str, Tuple[FileRecord, FileRecord]]:
        """
        对于当前文件与数据库文件中大小，修改日期都匹配的文件记录且数据库对应记录有md5记录的动作
        :param path2records: 路径到文件记录的映射
        :return: 通过计算md5确认与数据库中的不同的文件记录
        """
        if len(path2records) == 0:
            return {}
        total_size = sum(each[0].size for each in path2records.values())
        if not self.input_query(
                f'有{len(path2records)}个文件共{self.human_readable_size(total_size)}与数据库中记录相匹配，'
                f'且数据库中有md5记录，是否计算本地文件的md5以确认是否相同？'
        ):
            return {}
        res = {}
        # 计算本地文件的md5值并比较
        for path, (local_record, db_record) in tqdm(path2records.items(), desc='计算本地文件md5值'):
            md5 = local_record.compute_md5()
            if md5 != db_record.md5:
                res[path] = (local_record, db_record)
        print(f'通过比较md5值，共发现{len(res)}个文件的md5值与数据库中的相应记录不同')
        return res

    def _create_or_update_management(self, dir_id: int, tag: str, dir_path):
        if self.db.tag_exists(tag):
            # 存在记录则更新
            self.db.update_management(tag=tag, path=dir_path)
            return
        assert self.db.create_management(dir_id=dir_id, tag=tag, path=dir_path) == 1, RunTimeError('创建管理信息失败！')


class CancelManagementScript(SingleTransactionScript):
    def before_transaction(self, tag: str, *args):
        self.check_empty_args(*args)
        assert len(tag), OperationError('标识不能为空')

    def transaction_action(self, tag: str) -> int:
        """
        取消管理一个物理目录的脚本
        :param tag: 管理标识，不能与其他管理标识重复
        :return: 0表示正常
        """
        path = self.db.management_physical_path(tag)
        assert path is not None, OperationError(f'标识：{tag}不存在')
        if self.db.cancel_management(tag) == 0:
            return 1
        fm_path = join(path, '.lyl232fm')
        if exists(fm_path):
            try:
                os.remove(fm_path)
                return 0
            except Exception as e:
                print(f'由于{e}的原因，没能成功删除管理文件夹:{fm_path}')
        print('请记得删除该目录下的.lyl232fm文件夹')
        return 0


class QueryFileRecordScript(DataBaseScript):
    def __call__(self, name: str = None, write_path: str = None, *args) -> int:
        """
        查询所有被管理的目录的脚本
        :param name: 目录名字，如果为空，则从当前目录下的.lyl232fm的信息获取
        :param args: 其他参数，应为空
        :return: 0表示执行正常
        """
        self.check_empty_args(*args)
        self.init_db_if_needed()
        dir_id = self.get_directory_id_by_name_or_local(name)
        outputs = self.file_record_output_lines(self.db.file_records(dir_id))
        self.write_or_output_lines_to_file(outputs, write_path)
        return 0


class DumpDatabaseScript(DataBaseScript):
    def __call__(self, out_dir: str, *args):
        self.check_empty_args(*args)
        assert not exists(out_dir), OperationError(f'输出目录{out_dir}必须为空。')
        os.makedirs(out_dir)
        self.write_csv(
            join(out_dir, 'directory.csv'),
            [(record.dir_id, record.name, record.desc) for record in self.db.directories()],
            headers=['id', 'name', 'des']
        )
        self.write_csv(
            join(out_dir, 'management.csv'),
            [
                (
                    record.tag,
                    record.path.replace('\\', '/'),  # 将反斜杠换成正斜杠
                    record.dir_id
                )
                for record in self.db.all_managements()
            ],
            headers=['tag', 'path', 'dir_id']
        )
        self.write_csv(
            join(out_dir, 'file.csv'),
            [
                (
                    record.file_id, record.dir_path, record.name,
                    record.suffix, record.md5, record.size,
                    record.directory_id, record.modified_time
                )
                for record in self.db.all_files()
            ],
            headers=['id', 'dir_path', 'name', 'suffix', 'md5', 'size', 'dir_id', 'modified_timestamp']
        )
        print(f'数据已写入{out_dir}')


class QueryRedundantFileScript(FileMD5ComputingScript):
    def __call__(self, *args):
        self.check_empty_args(*args)
        self._process_common_size_file_ids(self.db.query_common_size_wo_md5_files())
        self._process_common_size_md5_file_ids(self.db.query_common_md5_files())

    def _process_common_size_file_ids(
            self,
            size2file_ids: Dict[int, List[int]]
    ):
        """
        拥有相同路径的本地文件记录与数据库文件记录相冲突的文件记录对
        :param size2file_ids: 大小到文件id的映射
        :return: size_md5_to_file_record 拥有相同大小和md5值的文件记录
        """
        if len(size2file_ids) == 0:
            return
        all_file_ids = []
        for each in size2file_ids.values():
            all_file_ids.extend(each)
        file_records = self.db.query_file_by_id(all_file_ids)
        all_directory = None

        def action_a():
            updated = sum(self.file_md5_computing_transactions(
                list(file_records.values()), self.db.update_file_records))
            print(f'更新了{updated}条数据库记录')
            return True

        def action_ls(inputs):
            nonlocal all_directory
            if all_directory is None:
                all_directory_ids = set()
                for record in file_records.values():
                    all_directory_ids.add(record.directory_id)
                all_directory = self.db.query_directory_by_id(list(all_directory_ids))
            outputs = []
            for size, md5_ids in size2file_ids.items():
                outputs.append(f'大小: {self.human_readable_size(size)}')
                for file_id in md5_ids:
                    record = file_records[file_id]
                    outputs.append(f'{all_directory[record.directory_id].name}:{file_records[file_id].path}')
            self.cmd_ls(inputs, outputs)
            return False

        self.query_actions(
            f'共有{len(size2file_ids)}项文件记录至少与其他文件拥有相同的大小。请问需要作何处理？',
            {'a': ('以本地文件记录覆盖数据库中的记录并计算md5值', action_a), },
            {'ls': ('列出这些文件的目录路径和大小 [写入指定的文件中]', '[写入文件路径（可选）]', action_ls)}
        )

    def _process_common_size_md5_file_ids(
            self,
            size_md5_to_file_records: Dict[int, Dict[str, List[int]]]
    ):
        if len(size_md5_to_file_records) == 0:
            return
        all_file_ids = []
        for md52ids in size_md5_to_file_records.values():
            for ids in md52ids.values():
                all_file_ids.extend(ids)
        file_records = self.db.query_file_by_id(all_file_ids)
        all_directory_ids = set()
        for record in file_records.values():
            all_directory_ids.add(record.directory_id)
        all_directory = self.db.query_directory_by_id(list(all_directory_ids))

        def action_a():
            for size, md5_dict in size_md5_to_file_records.items():
                for md5, _ids in md5_dict.items():
                    print(f'大小: {self.human_readable_size(size)}，md5：{md5}')
                    for i, file_id in enumerate(_ids):
                        _record = file_records[file_id]
                        print(f'【{i}】{all_directory[_record.directory_id].name}:{file_records[file_id].path}')
                    keep_ids = set()
                    while True:
                        keep = input(
                            '请选择您需要保留的文件记录：输入上述文件记录相应的数字，如果都不保留，输入-1'
                            '多个选择可用空格分隔，输入"skip"或者"s"可以跳过这次询问'
                        ).strip()
                        try:
                            if keep == 'skip' or keep == 's':
                                keep_ids = set(list(range(len(_ids))))
                                break
                            if keep == '-1':
                                break
                            for each in keep.split(' '):
                                each = int(each)
                                assert 0 <= each < len(_ids)
                                keep_ids.add(each)
                            break
                        except ValueError:
                            print(f'无法识别输入：{keep}，请重新输入')
                        except AssertionError:
                            print(f'请输入0至{len(_ids)}的整数')
                    if len(keep_ids) == len(_ids):
                        continue
                    to_delete = []
                    for i, file_id in enumerate(_ids):
                        _record = file_records[file_id]
                        if i not in keep_ids:
                            to_delete.append(_record.file_id)
                        print(
                            f'【{i}】{all_directory[_record.directory_id].name}:'
                            f'{file_records[file_id].path}',
                            '将被保留' if i in keep_ids else '将被删除'
                        )
                    if self.input_query('上述操作将会修改数据库，请确认'):
                        print(
                            f'删除了{self.transaction(self.db.delete_file_record_by_ids, file_ids=to_delete)}'
                            f'条文件记录'
                        )
            return True

        def action_ls(inputs):
            outputs = []
            for size, md5_dict in size_md5_to_file_records.items():
                for md5, _ids in md5_dict.items():
                    outputs.append(f'大小: {self.human_readable_size(size)}，md5：{md5}')
                    for file_id in _ids:
                        _record = file_records[file_id]
                        outputs.append(f'{all_directory[_record.directory_id].name}:{file_records[file_id].path}')
            self.cmd_ls(inputs, outputs)
            return False

        self.query_actions(
            f'共有{len(size_md5_to_file_records)}项文件记录至少与其他文件拥有相同的大小和md5值。请问需要作何处理？',
            {'a': ('列出相冲突的文件并询问保留哪条冲突的记录', action_a), },
            {'ls': ('列出这些文件的目录路径、大小和md5值 [写入指定的文件中]', '[写入文件路径（可选）]', action_ls)}
        )


class QuerySizeScript(DataBaseScript):
    def __call__(self, name: str = None, *args) -> int:
        """
        查询所有被管理的目录的脚本
        :param name: 目录名字，如果为空，则从当前目录下的.lyl232fm的信息获取
        :param args: 其他参数，应为空
        :return: 0表示执行正常
        """
        self.check_empty_args(*args)
        print(self.human_readable_size(self.db.query_director_size(self.get_directory_id_by_name_or_local(name))))
        return 0


class FindInColumnScript(DataBaseScript, metaclass=ABCMeta):
    def __call__(self, keyword: str, write_path: str = None, *args) -> int:
        """
        在文件记录的指定字段中查找keyword
        :param keyword: 查找关键字
        :param write_path: 输出文件路径
        :param args: 其他参数，应为空
        :return: 0表示执行正常
        """
        self.check_empty_args(*args)
        outputs = self.file_record_output_lines(self.db.find_in_file_path(self._col_name(), keyword.strip()))
        self.write_or_output_lines_to_file(outputs, write_path)
        return 0

    @abstractmethod
    def _col_name(self) -> str:
        """
        :return: 需要查询的字段名称
        """


class FindInFileDirectorPathScript(FindInColumnScript):
    """
    在父目录路径里查找
    """

    def _col_name(self) -> str:
        return 'dir_path'


class FindInNameScript(FindInColumnScript):
    """
    在文件名里查找
    """

    def _col_name(self) -> str:
        return 'name'


class FindInSuffixScript(FindInColumnScript):
    """
    在后缀名利查找
    """

    def _col_name(self) -> str:
        return 'suffix'
