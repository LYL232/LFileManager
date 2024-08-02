"""
数据库维护脚本
"""
import os
from os.path import join, exists, abspath, dirname
from scripts import DataBaseScript
from record import DirectoryRecord, ManagementRecord, FileRecord
from error import RunTimeError


class InitializeDataBaseScript(DataBaseScript):
    """
    初始化数据库脚本
    """

    def __call__(self, dumped_data_path: str = None, *args) -> int:
        self.check_empty_args(*args)
        self.db.initialize()
        if dumped_data_path is None or not exists(dumped_data_path):
            return 0
        print(f'正在从{dumped_data_path}读取备份的数据')
        directory_records = [
            DirectoryRecord(
                dir_id=int(dir_id),
                name=name,
                desc=des,
            )
            for dir_id, name, des in self.read_csv(join(dumped_data_path, 'directory.csv'), True)
        ]
        management_records = [
            ManagementRecord(
                dir_id=int(dir_id),
                tag=tag,
                # 在文件中的反斜杠路径为正斜杠，得替换回来
                path=path.replace('/', os.path.sep)
            )
            for tag, path, dir_id in self.read_csv(join(dumped_data_path, 'management.csv'), True)
        ]
        file_records = [
            FileRecord(
                file_id=int(file_id),
                dir_path=dir_path,
                name=name,
                suffix=suffix,
                md5=md5,
                size=int(size),
                directory_id=int(dir_id),
                modified_time=int(modified_timestamp)
            )
            for file_id, dir_path, name, suffix, md5, size, dir_id, modified_timestamp
            in self.read_csv(join(dumped_data_path, 'file.csv'), True)
        ]
        if len(file_records) == 0 and len(directory_records) == 0 and len(management_records) == 0:
            return 0
        transaction = self.db.begin_transaction()
        try:
            directories = self.db.create_directories_with_id(directory_records)
            assert directories == len(directory_records), RunTimeError(
                f'导入目录记录数据时出错，理应导入{len(directory_records)}条目录记录，但是只导入了{directories}条')
            managements = self.db.create_managements_with_id(management_records)
            assert managements == len(management_records), RunTimeError(
                f'导入管理记录数据时出错，理应导入{len(management_records)}条目录记录，但是只导入了{managements}条')
            files = self.db.create_files_with_id(file_records)
            assert files == len(file_records), RunTimeError(
                f'导入文件记录数据时出错，理应导入{len(file_records)}条目录记录，但是只导入了{files}条')
            transaction.commit()
            print(f'导入了{directories}条目录记录、{managements}条管理记录、{files}条文件记录')
        except Exception as e:
            transaction.rollback()
            raise e
        return 0


class ClearDataBaseScript(DataBaseScript):
    """
    清空数据库脚本：高危操作
    """

    def __call__(self, *args) -> int:
        from scripts import DumpDatabaseScript
        self.check_empty_args(*args)
        dump_script = DumpDatabaseScript(database_config=self.database_config, database=self.db)
        if not self.input_query('您将删除本程序需要的所有数据表，是否继续？'):
            print('已取消')
            return 0
        while True:
            inputs = input(
                '在删除前，将会导出所有数据到一个目录，请指定那个目录，并且确认那个目录不存在，默认为当前目录的.lyl232fm/dumped_data\n'
            )
            if inputs == '':
                inputs = join(abspath('.'), '.lyl232fm')
                os.makedirs(inputs, exist_ok=True)
                inputs = join(inputs, 'dumped_data')
            if exists(inputs):
                print(f'路径{inputs}已经存在，请重新输入。')
                continue
            dir_path = abspath(dirname(inputs))
            if not exists(dir_path):
                os.makedirs(dir_path, exist_ok=True)
            dump_script(inputs)
            break
        self.db.clear()
        return 0
