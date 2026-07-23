import pymysql
from pymysql.err import ProgrammingError
from pymysql import Connection

from typing import Union, List, Dict

from .database import Transaction, Database
from record import ManagementRecord, FileRecord, DirectoryRecord

from error import OperationError, CodingError


ALL_TABLES = {
    'directory': {
        'mysql': """
            CREATE TABLE directory (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR (255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
                des VARCHAR (255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
                UNIQUE directory_name_index(`name`)
            );
        """,
    },
    'management': {
        'mysql': """
            CREATE TABLE management (
                tag VARCHAR (255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin PRIMARY KEY,
                path VARCHAR (255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin DEFAULT NULL,
                dir_id INT, 
                INDEX management_dir_id_index(dir_id),
                CONSTRAINT management_fk FOREIGN KEY (dir_id) REFERENCES directory(id)
            );
        """
    },
    'file': {
        'mysql': """
            CREATE TABLE file (
                id BIGINT AUTO_INCREMENT PRIMARY KEY, 
                dir_path VARCHAR (255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
                name VARCHAR (255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
                suffix VARCHAR (255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
                md5 CHAR(32) DEFAULT NULL,
                size BIGINT NOT NULL,
                dir_id INT NOT NULL,
                modified_timestamp BIGINT NOT NULL,
                UNIQUE file_path_index(dir_id, dir_path, `name`, suffix), 
                INDEX file_size_index(`size`),
                INDEX file_md5_index(md5),
                INDEX file_same_index(`size`, md5),
                INDEX file_modified_timestamp_index(modified_timestamp),
                CONSTRAINT file_fk FOREIGN KEY (dir_id) REFERENCES directory(id)
            );
        """
    }

}

ALL_TABLE_NAMES = list(ALL_TABLES.keys())


class MysqlTransaction(Transaction):
    def __init__(self, connection: Connection):
        self.connection = connection
        connection.begin()

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()


class MysqlDataBase(Database):
    __DROP_CONSTRAINT_SQL = [
        'ALTER TABLE management DROP FOREIGN KEY management_fk',
        'ALTER TABLE file DROP FOREIGN KEY file_fk',
    ]
    __WHERE_IN_BATCH = 1000  # 使用where in查询时最多一次execute多少个

    def __init__(
            self,
            host: str,
            user: str,
            password: str,
            port: int
    ):
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.connection = pymysql.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            port=self.port,
            db='lyl232fm',
            charset='utf8mb4'
        )

    def begin_transaction(self) -> MysqlTransaction:
        return MysqlTransaction(self.connection)

    def initialize_database(self):
        """
        初始化数据库，谨慎操作，如果存在相关的表会抛出异常
        :return:
        """
        with self.connection.cursor() as cursor:
            for table_name in ALL_TABLE_NAMES:
                assert cursor.execute(f"SHOW TABLES LIKE '{table_name}';") == 0, \
                    OperationError(f'数据库中存在表：{table_name}，初始化之前请删除下列表：{ALL_TABLE_NAMES}')
        self._create_tables()

    def is_initialized(self) -> bool:
        """
        :return: 数据库是否已经初始化好了
        """
        with self.connection.cursor() as cursor:
            for table_name in ALL_TABLE_NAMES:
                if cursor.execute(f"SHOW TABLES LIKE '{table_name}';") == 0:
                    return False
        return True

    def delete_database(self):
        """
        高危操作：删除所有表
        :return:
        """
        # 删除约束
        with self.connection.cursor() as cursor:
            for sql in self.__DROP_CONSTRAINT_SQL:
                try:
                    cursor.execute(sql)
                except ProgrammingError:
                    pass
            for table_name in ALL_TABLE_NAMES:
                cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        print('执行完毕')

    def close(self):
        self.connection.close()

    def _create_tables(self):
        """
        创建初始的表格
        :return:
        """
        with self.connection.cursor() as cursor:
            for table, build_statement in ALL_TABLES.items():
                cursor.execute(build_statement['mysql'])

    def directory_id(self, name: str) -> int:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT id FROM directory 
                WHERE directory.name = %s;
                """,
                (name,)
            )
            res = cursor.fetchone()
            return None if res is None else int(res[0])

    def new_repository(self, name: str, desc: str):
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO directory (name, des) VALUES (%s, %s);
                """,
                (name, desc)
            )

    def repository_instances(self, dir_id_or_name: Union[str, id]) -> List[str]:
        with self.connection.cursor() as cursor:
            if isinstance(dir_id_or_name, str):
                cursor.execute(
                    """
                    SELECT tag, `path` FROM management
                    LEFT JOIN directory on directory.id = management.dir_id
                    WHERE directory.name = %s;
                    """,
                    (dir_id_or_name,)
                )
            else:
                assert isinstance(dir_id_or_name, int)
                cursor.execute(
                    """
                    SELECT tag, `path` FROM management WHERE dir_id = %s;
                    """,
                    (dir_id_or_name,)
                )
            return [each for each in cursor.fetchall()]

    # def reset_management_path(self, tags: List[str]) -> int:
    #     with self.connection.cursor() as cursor:
    #         return cursor.executemany(
    #             """
    #             UPDATE management SET `path` = %s WHERE tag = %s;
    #             """,
    #             [('', tag,) for tag in tags]
    #         )

    def repositories(self) -> List[DirectoryRecord]:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, `name`, `des` FROM directory;
                """
            )
            records = []
            while True:
                res = cursor.fetchone()
                if res is None:
                    break
                records.append(DirectoryRecord(
                    dir_id=int(res[0]),
                    name=res[1],
                    desc=res[2]
                ))
            return records

    def remove_directory(self, name: str) -> int:
        with self.connection.cursor() as cursor:
            return cursor.execute(
                """
                DELETE FROM directory WHERE name = %s;
                """,
                (name,)
            )

    def tag_exists(self, tag: str) -> bool:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*) FROM management WHERE tag = %s;
                """,
                (tag,)
            )
            return cursor.fetchone()[0] != 0

    def create_management(self, dir_id: int, tag: str, path: str) -> int:
        with self.connection.cursor() as cursor:
            return cursor.execute(
                """
                INSERT INTO management (dir_id, tag, `path`) VALUES (%s, %s, %s);
                """,
                (dir_id, tag, path)
            )

    def update_management(self, tag: str, path: str) -> int:
        with self.connection.cursor() as cursor:
            return cursor.execute(
                """
                UPDATE management SET `path` = %s WHERE tag = %s;
                """,
                (path, tag)
            )

    def cancel_management(self, tag: str) -> int:
        with self.connection.cursor() as cursor:
            return cursor.execute(
                """
                DELETE FROM management where tag = %s;
                """,
                (tag,)
            )

    def management_physical_path(self, tag: str) -> str:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT path FROM management WHERE tag = %s;
                """,
                (tag,)
            )
            res = cursor.fetchone()
            if res is None:
                return None
            return res[0]

    def new_file_records(self, dir_id: int, file_records: List[FileRecord]) -> int:
        with self.connection.cursor() as cursor:
            return cursor.executemany(
                """
                INSERT INTO file 
                (dir_path, `name`, suffix, md5, `size`, dir_id, modified_timestamp) 
                VALUES (%s, %s, %s, %s, %s, %s, %s);
                """,
                [
                    (each.dir_path, each.name, each.suffix, each.md5, each.size, dir_id, each.modified_time)
                    for each in file_records
                ]
            )

    def update_file_records(self, file_records: List[FileRecord]) -> int:
        with self.connection.cursor() as cursor:
            assert all(each.file_id is not None for each in file_records), \
                CodingError('更新数据库文件记录时文件id不能为None')
            return cursor.executemany(
                """
                UPDATE `file` SET md5 = %s, `size` = %s, modified_timestamp = %s WHERE id = %s;
                """,
                [
                    (each.md5, each.size, each.modified_time, each.file_id)
                    for each in file_records
                ]
            )

    def repository_file_records(self, dir_id: int) -> List[FileRecord]:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT 
                dir_path, `name`, suffix, md5, `size`, modified_timestamp, id 
                FROM file WHERE dir_id = %s;
                """,
                (dir_id,)
            )
            file_records = []
            while True:
                res = cursor.fetchone()
                if res is None:
                    break
                file_records.append(FileRecord(
                    dir_path=res[0],
                    name=res[1],
                    suffix=res[2],
                    md5=res[3],
                    size=int(res[4]),
                    modified_time=int(res[5]),
                    file_id=int(res[6]),
                    directory_id=dir_id
                ))
            return file_records

    def delete_file_records(self, file_ids: List[int]) -> int:
        with self.connection.cursor() as cursor:
            return cursor.executemany(
                """
                DELETE FROM file WHERE id = %s;
                """,
                [(each,) for each in file_ids]
            )

    def all_file_records(self) -> List[FileRecord]:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT 
                dir_path, `name`, suffix, md5, `size`, modified_timestamp, id, dir_id
                FROM file;
                """,
            )
            file_records = []
            while True:
                res = cursor.fetchone()
                if res is None:
                    break
                file_records.append(FileRecord(
                    dir_path=res[0],
                    name=res[1],
                    suffix=res[2],
                    md5=res[3],
                    size=int(res[4]),
                    modified_time=int(res[5]),
                    file_id=int(res[6]),
                    directory_id=int(res[7])
                ))
            return file_records

    def all_managements(self) -> List[ManagementRecord]:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT tag, `path`, dir_id FROM management;
                """,
            )
            records = []
            while True:
                res = cursor.fetchone()
                if res is None:
                    break
                records.append(ManagementRecord(
                    tag=res[0],
                    path=res[1],
                    dir_id=int(res[2]),
                ))
            return records

    def query_common_size_without_md5_files(self) -> Dict[int, List[int]]:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                WITH t AS (
                    SELECT id, `size` FROM file WHERE md5='********************************'
                )
                SELECT id, `size` FROM t  WHERE `size` IN (
                    SELECT `size` FROM t GROUP BY `size` HAVING COUNT(*) > 1
                ) ORDER BY `size`;
                """,
            )
            size2records = {}
            current_size = None
            current_ids = []
            while True:
                res = cursor.fetchone()
                if res is None:
                    break
                if current_size != int(res[1]):
                    if len(current_ids) > 0:
                        size2records[current_size] = current_ids
                        current_ids = []
                current_size = int(res[1])
                current_ids.append(int(res[0]))

            if len(current_ids) > 0:
                size2records[current_size] = current_ids
            return size2records

    def query_common_md5_files(self) -> Dict[int, Dict[str, List[int]]]:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                WITH t AS (
                    SELECT id, `size`, md5 FROM file WHERE md5!='********************************'
                )
                SELECT id, `size`, md5 FROM t WHERE (`size`, md5) IN (
                    SELECT `size`, md5 FROM t GROUP BY `size`, md5 HAVING COUNT(*) > 1
                ) ORDER BY `size`, md5;
                """,
            )
            size_md5_to_records = {}
            current_size_md5 = (None, None)
            current_ids = []
            while True:
                res = cursor.fetchone()
                if res is None:
                    break
                size, md5 = current_size_md5
                if size != int(res[1]) or md5 != res[2]:
                    if len(current_ids) > 0:
                        if size not in size_md5_to_records.keys():
                            size_md5_to_records[size] = {}
                        size_md5_to_records[size][md5] = current_ids
                        current_ids = []
                current_size_md5 = (int(res[1]), res[2])
                current_ids.append(int(res[0]))

            if len(current_ids) > 0:
                size, md5 = current_size_md5
                if size not in size_md5_to_records.keys():
                    size_md5_to_records[size] = {}
                size_md5_to_records[size][md5] = current_ids
            return size_md5_to_records

    def create_directories_with_id(self, records: List[DirectoryRecord]) -> int:
        with self.connection.cursor() as cursor:
            return cursor.executemany(
                """
                INSERT INTO `directory` 
                (`id`, `name`, des) 
                VALUES (%s, %s, %s);
                """,
                [
                    (each.dir_id, each.name, each.desc) for each in records
                ]
            )

    def create_managements_with_id(self, records: List[ManagementRecord]) -> int:
        with self.connection.cursor() as cursor:
            return cursor.executemany(
                """
                INSERT INTO `management` 
                (tag, `path`, dir_id) 
                VALUES (%s, %s, %s);
                """,
                [
                    (each.tag, each.path, each.dir_id) for each in records
                ]
            )

    def create_files_with_id(self, records: List[FileRecord]) -> int:
        with self.connection.cursor() as cursor:
            return cursor.executemany(
                """
                INSERT INTO `file` 
                (dir_path, `name`, suffix, md5, `size`, dir_id, modified_timestamp) 
                VALUES (%s, %s, %s, %s, %s, %s, %s);
                """,
                [
                    (each.dir_path, each.name, each.suffix, each.md5, each.size, each.directory_id, each.modified_time)
                    for each in records
                ]
            )

    def query_file_by_id(self, file_ids: List[int]) -> Dict[int, FileRecord]:
        with self.connection.cursor() as cursor:
            begin, n = 0, len(file_ids)
            file_records = {}
            while begin < n:
                end = min(len(file_ids), begin + self.__WHERE_IN_BATCH)
                batch = file_ids[begin: end]
                cursor.execute(
                    """
                    SELECT 
                    dir_path, `name`, suffix, md5, `size`, modified_timestamp, id, dir_id
                    FROM file
                    WHERE id IN (%s);
                    """ % ','.join(['%s'] * len(batch)),
                    batch
                )
                while True:
                    res = cursor.fetchone()
                    if res is None:
                        break
                    record = FileRecord(
                        dir_path=res[0],
                        name=res[1],
                        suffix=res[2],
                        md5=res[3],
                        size=int(res[4]),
                        modified_time=int(res[5]),
                        file_id=int(res[6]),
                        directory_id=int(res[7])
                    )
                    file_records[record.file_id] = record
                begin = end
            return file_records

    def query_directory_by_id(self, dir_ids: List[int]) -> Dict[int, FileRecord]:
        with self.connection.cursor() as cursor:
            begin, n = 0, len(dir_ids)
            records = {}
            while begin < n:
                end = min(len(dir_ids), begin + self.__WHERE_IN_BATCH)
                batch = dir_ids[begin: end]
                cursor.execute(
                    """
                    SELECT id, `name`, `des` FROM directory
                    WHERE id IN (%s);
                    """ % ','.join(['%s'] * len(batch)),
                    batch
                )

                while True:
                    res = cursor.fetchone()
                    if res is None:
                        break
                    record = DirectoryRecord(
                        dir_id=int(res[0]),
                        name=res[1],
                        desc=res[2]
                    )
                    records[record.dir_id] = record
                begin = end
            return records

    def query_director_size(self, dir_id: int) -> int:
        """
        查询目录的大小
        :param dir_id: 目录id
        :return: 大小（字节）
        """
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT SUM(`size`) FROM file WHERE dir_id = %s;
                """,
                (dir_id,)
            )
            return int(cursor.fetchone()[0])

    def find_in_file_path(self, column: str, keyword: str) -> List[FileRecord]:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT 
                dir_path, `name`, suffix, md5, `size`, modified_timestamp, id, dir_id
                FROM file WHERE %s LIKE %s;
                """ % (column, '%s'),
                (f'%{keyword}%',)
            )
            file_records = []
            while True:
                res = cursor.fetchone()
                if res is None:
                    break
                file_records.append(FileRecord(
                    dir_path=res[0],
                    name=res[1],
                    suffix=res[2],
                    md5=res[3],
                    size=int(res[4]),
                    modified_time=int(res[5]),
                    file_id=int(res[6]),
                    directory_id=int(res[7])
                ))
            return file_records

    def query_file_ids_by_size_and_md5(self, size: int, md5: str) -> List[int]:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT id FROM file WHERE `size` = %s and md5 = %s;
                """,
                (size, md5,)
            )
            file_ids = []
            while True:
                res = cursor.fetchone()
                if res is None:
                    break
                file_ids.append(int(res[0]))
            return file_ids
