import os
from typing import Union, Set

DB_DIR = 'database/'
EXEC_TMP_DIR = "tmp/"


def get_db_path(db_name: str, testcase_name: Union[str, None] = None) -> str:
    sqlite_path = os.path.join(DB_DIR, db_name,
                               (db_name if testcase_name is None else testcase_name) + '.sqlite')
    return sqlite_path


def get_all_dbnames() -> Set[str]:
    return set([db_name for db_name in os.listdir(DB_DIR)])


def get_value_path(db_name: str) -> str:
    return os.path.join(DB_DIR, db_name, 'values.pkl')


def get_skipped_dbnames() -> Set[str]:
    return {'baseball_1', 'imdb', 'restaurants'}


def orig2test(orig_db_path: str, testcase_name: str) -> str:
    return orig_db_path.replace('.sqlite', '') + testcase_name + '.sqlite'
