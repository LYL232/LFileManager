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

        deleted_records = self._common_path_records_action(dir_path, dir_id, common_path, local_records, db_records)
        # 在上个动作中删除的本地文件记录添加到数据库缺失的文件中
        for each in deleted_records:
            db_unique.add(each.path)
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

        unique_paths = sorted(list(unique_paths))

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
                if self._unique_local_records_query_each_action(dir_path, dir_id, path, local_records[path]):
                    break
                count += 1
            return True

        def action_remove_all():
            removing_path = []
            for path in unique_paths:
                real_path = join(dir_path, *(path.split('/')[1:]))
                if not exists(real_path):
                    continue
                removing_path.append(real_path)
                print(real_path)
            if self.input_query('上述文件将被批量删除，请确认是否删除？') and self.input_query('上述操作无法被恢复，请确认：'):
                for path in removing_path:
                    if not exists(path):
                        continue
                    try:
                        os.remove(path)
                        print(f'已删除：{path}')
                    except Exception as e:
                        print(f'无法删除：{path}，原因是：{e}')
            return True

        def action_ls(inputs):
            self.cmd_ls(inputs, unique_paths)
            return False

        self.query_actions(
            f'数据库相较本地中缺失{len(unique_paths)}条文件记录，对于这些文件记录请问需要作何处理？',
            {
                'a': ('将这些文件记录更新到数据库中', action_a),
                'b': ('【注意！】删除本地的这些文件', action_b),
                'remove_all': ('【注意！】批量删除本地的这些文件', action_remove_all),
                'c': ('每条记录单独询问', action_c),
            },
            {'ls': ('[写入文件路径（可选）]', '列出这些文件的目录路径 [写入指定的文件中]', action_ls)}
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

        abort = False

        def action_a():
            if self.input_query(
                    f'该文件大小为{self.human_readable_size(record.size)}，'
                    f'是否在输入数据库前计算md5值？'
            ):
                created = sum(self.file_md5_computing_transactions(
                    [record], self.db.new_file_records, dir_id=dir_id,
                ))
            else:
                created = self.transaction(self.db.new_file_records, dir_id=dir_id, file_records=[record])
            assert created == 1, RunTimeError(f'理应插入1条数据库文件记录，但插入了{created}条')
            print(f'插入了{created}条文件记录')
            return True

        def action_b():
            self.remove_single_file(join(dir_path, *(path.split('/')[1:])))
            return True

        def action_abort():
            nonlocal abort
            abort = True
            return True

        self.query_actions(
            f'请问对上述本地独有的文件记录需要作何处理？',
            {
                'a': ('将该文件记录更新到数据库中', action_a),
                'b': ('【注意！】删除该文件记录对应的文件', action_b),
                'abort': ('不做改变并结束逐一询问', action_abort)
            }
        )
        return abort

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
            sorted_paths = sorted(list(unique_paths))
            records = [db_records[path] for path in sorted_paths]
            assert all(each.file_id is not None for each in records), CodingError('数据库中的文件记录中的文件id不应该为None')
            no_md5_records, md5_records = [], []
            for each in records:
                if each.md5 == FileRecord.EMPTY_MD5:
                    no_md5_records.append(each)
                else:
                    md5_records.append(each)
            if len(no_md5_records) > 0:
                for each in no_md5_records:
                    print(each.path)
                if self.input_query(
                        '上述文件在数据库中没有MD5值记录，如果删除将无法自动检查是否还有相同内容的文件记录在数据库中，'
                        '是否删除这些文件记录？'
                ):
                    self._delete_file_records(no_md5_records)
            if len(md5_records) > 0:
                for each in md5_records:
                    print(each.path)
                if self.input_query('将删除上述文件在数据库中的记录，是否继续？'):
                    self._delete_file_records(md5_records)
            return True

        def action_b():
            file_records = [db_records[path] for path in unique_paths]
            self._unique_db_records_copy_from_others(dir_path, dir_id, file_records)
            return True

        def action_c():
            count = 1
            for path in unique_paths:
                print(f'({count}/{len(unique_paths)})')
                if self._unique_db_records_query_each_action(dir_path, dir_id, path, db_records[path]):
                    break
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
                'ls': ('[写入文件路径（可选）]', '列出这些文件的目录路径 [写入指定的文件中]', action_ls)
            }
        )

    def _unique_db_records_query_each_action(
            self,
            dir_path: str,
            dir_id: int,
            path: str,
            record: FileRecord
    ) -> bool:
        print(path)

        abort = False

        def action_a():
            assert record.file_id is not None, CodingError('数据库中的文件记录中的文件id不应该为None')
            if record.md5 == FileRecord.EMPTY_MD5:
                if self.input_query('将删除文件记录在数据库中的记录，是否继续？'):
                    self._query_safely_delete_file_records([record])
            else:
                if self.input_query(
                        '该文件在数据库中没有MD5值记录，如果删除将无法自动检查是否还有相同内容的文件记录在数据库中，'
                        '是否删除该文件记录？'
                ):
                    self._delete_file_records([record])
            return True

        def action_b():
            self._unique_db_records_copy_from_others(dir_path, dir_id, [record])
            return True

        def action_abort():
            nonlocal abort
            abort = True
            return True

        self.query_actions(
            f'请问对上述冲突记录需要作何处理？',
            {
                'a': ('【注意！】删除数据库中的该文件记录', action_a),
                'b': ('尝试从其他同目录的受管理物理位置复制该文件到本地', action_b),
                'abort': ('直接跳过逐个询问', action_abort),
            }
        )
        return abort

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
        other_dir_paths = self._get_valid_management_paths(dir_id, except_path=dir_path)

        found_file_paths, not_found_paths = {}, set()
        # 尝试寻找所有的文件路径
        for record in records:
            path = record.path
            found = self._find_single_file_in_managements(record, other_dir_paths)
            local_real_path = join(dir_path, *(path.split('/')[1:]))
            if found is None:
                not_found_paths.add(path)
            else:
                found_file_paths[local_real_path] = found
        return found_file_paths, not_found_paths

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
            if not exists(path):
                not_exist_path_tags.append(tag)
                continue
            if samefile(path, dir_path):
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
                        CodingError(f'复制文件的目标路径不应该存在文件：{local_real_path}，'
                                    f'请检查是否是由于Windows默认路径不区分大小写造成的')
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
    ) -> List[FileRecord]:
        """
        对现有文件记录与数据库文件记录中路径相同的部分的动作
        :param dir_path: 当前操作的目录路径
        :param dir_id: 当前操作的目录id
        :param common_path: 两者相同路径的集合
        :param local_records: 当前的文件记录
        :param db_records: 数据库中的文件记录
        :return: 如果删除了本地文件，则返回这些被删除的文件记录
        """
        if len(common_path) == 0:
            return []

        conflict, match_with_md5, match_wo_md5 = {}, {}, {}
        for path in common_path:
            record_pair = (local_record, db_record) = (local_records[path], db_records[path])
            local_record.file_id = db_record.file_id
            db_record.dir_physical_path = local_record.dir_physical_path
            if local_record.size == db_record.size and local_record.modified_time == db_record.modified_time:
                if db_record.md5 != FileRecord.EMPTY_MD5:
                    # 如果数据库中有md5记录
                    match_with_md5[path] = record_pair
                else:
                    # 如果数据库中没有md5记录则在之后询问是否计算这些文件的md5值
                    match_wo_md5[path] = record_pair
            else:
                conflict[path] = record_pair

        self._common_path_match_without_db_md5_action(match_wo_md5)
        conflict.update(self._common_path_match_with_db_md5_action(match_with_md5))

        return self._common_path_conflict_action(dir_path, dir_id, conflict)

    def _common_path_conflict_action(
            self,
            dir_path: str,
            dir_id: int,
            path2records: Dict[str, Tuple[FileRecord, FileRecord]]
    ) -> List[FileRecord]:
        """
        拥有相同路径的本地文件记录与数据库文件记录相冲突的文件记录对
        :param dir_path: 当前操作的目录路径
        :param dir_id: 当前操作的目录路径
        :param path2records: 路径到记录对的映射
        :return: 如果删除了本地文件，则返回这些被删除的文件记录
        """
        deleted_records = []

        if len(path2records) == 0:
            return deleted_records

        def action_a():
            for each in sorted(list(path2records.keys())):
                print(each)
            if not self.input_query('上述文件记录更新至数据库。是否继续？'):
                return False
            records = [local_record for local_record, _ in path2records.values()]
            if self.input_query('是否计算这些文件的md5值？'):
                updated = sum(self.file_md5_computing_transactions(records, self.db.update_file_records))
            else:
                updated = self.transaction(self.db.update_file_records, file_records=records)
            print(f'更新了{updated}条数据库记录')
            return True

        def action_b():
            for local_record, _ in path2records.values():
                if self.remove_single_file(join(dir_path, *(local_record.path.split('/')[1:]))):
                    deleted_records.append(local_record)
            return True

        def action_c():
            count = 1
            for path, (local, db) in path2records.items():
                print(f'({count}/{len(path2records)})')
                abort, deleted = self._common_path_conflict_query_each_action(dir_path, dir_id, path, local, db)
                if deleted:
                    deleted_records.append(local)
                if abort:
                    break
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

        def action_remove_all():
            removing_records = []
            for local_record, _ in path2records.values():
                real_path = join(dir_path, *(local_record.path.split('/')[1:]))
                if not exists(real_path):
                    continue
                removing_records.append((local_record, real_path))
                print(real_path)
            if self.input_query('上述文件将被批量删除，请确认是否删除？') and self.input_query('上述操作无法被恢复，请确认：'):
                for record, real_path in removing_records:
                    if not exists(real_path):
                        continue
                    try:
                        os.remove(real_path)
                        deleted_records.append(record)
                        print(f'已删除：{real_path}')
                    except Exception as e:
                        print(f'无法删除：{real_path}，原因是：{e}')
            return True

        self.query_actions(
            f'【注意！】共有{len(path2records)}项本地文件记录的文件与数据库中相应记录冲突：请问需要作何处理？',
            {
                'a': ('以本地文件记录覆盖数据库中的记录', action_a),
                'b': ('【注意！】删除本地的这些文件', action_b),
                'remove_all': ('【注意！】批量删除本地的这些文件', action_remove_all),
                'c': ('每条记录单独询问', action_c),
            },
            {
                'ls': ('[写入文件路径（可选）]', '列出这些文件的目录路径 [写入指定的文件中]', action_ls)
            }
        )
        return deleted_records

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

        abort = False
        deleted_local_record = False

        def action_a():
            updated = sum(self.file_md5_computing_transactions([local_record], self.db.update_file_records))
            print(f'更新了{updated}条数据库记录')
            return True

        def action_b():
            nonlocal deleted_local_record
            if self.remove_single_file(join(dir_path, *(local_record.path.split('/')[1:]))):
                deleted_local_record = True
            return True

        def action_c():
            other_dir_paths = self._get_valid_management_paths(dir_id, except_path=dir_path)
            real_split_path = (path.split('/')[1:])
            file_other_real_path = self._find_single_file_in_managements(local_record, other_dir_paths)
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

        def action_abort():
            nonlocal abort
            abort = True
            return True

        self.query_actions(
            f'请问对上述冲突记录需要作何处理？',
            {
                'a': ('以本地文件记录覆盖数据库中的记录并计算md5值', action_a),
                'b': ('以本地文件记录覆盖数据库中的记录但不计算md5值', action_b),
                'c': ('删除本地的文件，并尝试从其他物理位置复制该文件', action_c),
                'abort': ('不做改变并结束逐一询问', action_abort),
            }
        )

        return abort, deleted_local_record

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
            dir_id2records = {}  # 根据目录id分类文件记录
            for record in file_records.values():
                dir_id = record.directory_id
                if dir_id not in dir_id2records.keys():
                    dir_records = dir_id2records[dir_id] = []
                else:
                    dir_records = dir_id2records[dir_id]
                dir_records.append(record)

            found_records, not_found_records = [], []
            for dir_id, records in dir_id2records.items():
                dir_paths = self._get_valid_management_paths(dir_id)
                # 尝试寻找所有的文件路径
                for record in records:
                    found = False
                    for dp in dir_paths:
                        real_path = join(dp, *(record.path.split('/')[1:]))
                        if exists(real_path):
                            record.dir_physical_path = dp
                            found_records.append(record)
                            found = True
                            break
                    if not found:
                        not_found_records.append(record)

            if len(not_found_records) > 0:
                nonlocal all_directory
                if all_directory is None:
                    all_directory = self.db.query_directory_by_id(list(dir_id2records.keys()))
                for record in not_found_records:
                    print(f'{all_directory[record.directory_id].name}:{record.path}')
                input('【注意！】上述文件无法找到对应的物理路径，按下回车以继续')
            if len(found_records) > 0:
                updated = sum(self.file_md5_computing_transactions(found_records, self.db.update_file_records))
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
            f'共有{len(size2file_ids)}项文件记录至少与其他文件拥有相同的大小而且数据库中没有md5值。请问需要作何处理？',
            {'a': ('计算md5值之后更新入数据库', action_a), },
            {'ls': ('[写入文件路径（可选）]', '列出这些文件的目录路径和大小 [写入指定的文件中]', action_ls)}
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
            size_list = sorted(size_md5_to_file_records.keys(), reverse=True)
            for size in size_list:
                md5_dict = size_md5_to_file_records[size]
                for md5, _ids in md5_dict.items():
                    print(f'大小: {self.human_readable_size(size)}，md5：{md5}')
                    options = []
                    for file_id in _ids:
                        _record = file_records[file_id]
                        options.append((
                            file_id, f'{all_directory[_record.directory_id].name}:{file_records[file_id].path}'))
                    options.sort(key=lambda x: x[1])
                    print('=' * 120)
                    for i, (_, hint) in enumerate(options):
                        print(f'【{i}】{hint}')
                    print('=' * 120)
                    keep_ids = set()
                    while True:
                        keep = input(
                            '请选择您需要保留的文件记录：输入上述文件记录相应的数字，如果都不保留，输入-1'
                            '多个选择可用空格分隔，输入"skip"或者"s"可以跳过这次询问，输入abort结束询问：'
                        ).strip()
                        try:
                            if keep == 'abort':
                                return True
                            elif keep == 'skip' or keep == 's':
                                keep_ids = set(list(range(len(_ids))))
                                break
                            elif keep == '-1':
                                break
                            for each in keep.split(' '):
                                each = int(each)
                                assert 0 <= each < len(_ids)
                                keep_ids.add(each)
                            break
                        except ValueError:
                            print(f'无法识别输入：{keep}，请重新输入')
                        except AssertionError:
                            print(f'请输入0至{len(_ids) - 1}的整数')
                    if len(keep_ids) == len(_ids):
                        continue
                    to_delete = []

                    print('=' * 120)
                    for i, (file_id, _) in enumerate(options):
                        _record = file_records[file_id]
                        if i not in keep_ids:
                            to_delete.append(_record)
                        print(
                            f'【{i}】{all_directory[_record.directory_id].name}:'
                            f'{file_records[file_id].path}',
                            '将被保留' if i in keep_ids else '将被删除'
                        )
                    print('=' * 120)

                    if self.input_query('上述操作将会修改数据库，请确认'):
                        self._query_safely_delete_file_records(to_delete)
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
            {'ls': ('[写入文件路径（可选）]', '列出这些文件的目录路径、大小和md5值 [写入指定的文件中]', action_ls)}
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


class QueryDirectoryFileRecordsExistenceScript(FileMD5ComputingScript):
    def __call__(
            self,
            path: str = '.',
            md5cache_path: str = '.lyl232fm/md5cache.log',
            in_db_path: str = '.lyl232fm/fr_in_db.log',
            not_in_db_path: str = '.lyl232fm/fr_not_in_db.log',
            *args
    ) -> int:
        """
        查询path下的所有文件是否已经存在于数据库里
        :param path: 该路径下的所有文件将被检查是否被数据库里的文件记录覆盖
        :param in_db_path: 输出文件：path下存在数据库中的文件记录
        :param not_in_db_path: 输出文件：path下不存在数据库中的文件记录
        :return: 0表示正常
        """
        self.check_empty_args(*args)
        self.init_db_if_needed()

        md5_cache = {}

        os.makedirs(abspath(dirname(md5cache_path)), exist_ok=True)

        if exists(md5cache_path):
            try:
                with open(md5cache_path, 'r', encoding='utf8') as file:
                    for line in file.readlines():
                        p, md5 = line.strip().split('\\')
                        md5_cache[p] = md5
            except Exception as e:
                RunTimeError(f'读取MD5缓存文件：{md5cache_path}失败，原因是{e}，请删除手动删除该文件')
        else:
            os.makedirs(abspath(dirname(in_db_path)), exist_ok=True)

        os.makedirs(abspath(dirname(in_db_path)), exist_ok=True)
        os.makedirs(abspath(dirname(not_in_db_path)), exist_ok=True)

        records = FileRecord.get_dir_file_records(abspath(path))
        if len(records) == 0:
            return
        with open(in_db_path, 'w', encoding='utf8') as in_db_file:
            with open(not_in_db_path, 'w', encoding='utf8') as not_in_db_file:
                with open(md5cache_path, 'a+', encoding='utf8') as md5cache_file:
                    self._batch_check_record_in_db(
                        records, in_db_file, not_in_db_file,
                        md5cache_file, md5_cache
                    )
        print(f'在数据库中的文件记录已经写入：{in_db_path}')
        print(f'不在数据库中的文件记录已经写入：{not_in_db_path}')
        print(f'已经计算的文件的md5值缓存在：{md5cache_path}')

    def _batch_check_record_in_db(
            self, records: List[FileRecord],
            in_db_file, not_in_db_file, md5cache_file,
            md5_cache: Dict[str, str]
    ):
        for record in tqdm(records, desc='检查文件中'):
            path = record.path
            if path in md5_cache.keys():
                md5 = md5_cache[path]
            else:
                md5 = record.compute_md5()
                md5cache_file.write(f'{path}\\{md5}\n')
            res = self.db.query_file_ids_by_size_and_md5(record.size, md5)
            if len(res) > 0:
                in_db_file.write(f'{path}\n')
            else:
                not_in_db_file.write(f'{path}\n')
