import os
from typing import Any, Tuple
import sqlite3
from sql_util.dbpath import EXEC_TMP_DIR
import time
import subprocess
import pickle as pkl
import random
import threading
import re
threadLock = threading.Lock()


TIMEOUT = 60
# for evaluation's sake, replace current year always with 2020
CUR_YEAR = 2020


def get_cursor_path(sqlite_path: str):
    try:
        if not os.path.exists(sqlite_path):
            print('Openning a new connection %s' % sqlite_path)
        connection = sqlite3.connect(sqlite_path)
    except Exception as e:
        print(sqlite_path)
        raise e
    connection.text_factory = lambda b: b.decode(errors='ignore')
    cursor = connection.cursor()
    return cursor


def can_execute_path(sqlite_path: str, q: str) -> bool:
    flag, result = exec_db_path_(sqlite_path, q)
    return flag == 'result'


def clean_tmp_f(f_prefix: str):
    with threadLock:
        for suffix in ('.in', '.out'):
            f_path = f_prefix + suffix
            if os.path.exists(f_path):
                os.unlink(f_path)


# we need a wrapper, because simple timeout will not stop the database connection
def exec_db_path(sqlite_path: str, query: str, process_id: str = '', timeout: int = TIMEOUT) -> Tuple[str, Any]:
    f_prefix = None
    with threadLock:
        while f_prefix is None or os.path.exists(f_prefix + '.in'):
            process_id += str(time.time())
            process_id += str(random.randint(0, 10000000000))
            f_prefix = os.path.join(EXEC_TMP_DIR, process_id)
        pkl.dump((sqlite_path, query), open(f_prefix + '.in', 'wb'))
    try:
        subprocess.call(['python3', 'sql_util/exec_subprocess.py', f_prefix], timeout=timeout, stderr=open('runerr.log', 'a'))
    except Exception as e:
        clean_tmp_f(f_prefix)
        return 'exception', e
    result_path = f_prefix + '.out'
    returned_val = ('exception', TimeoutError)
    try:
        if os.path.exists(result_path):
            returned_val = pkl.load(open(result_path, 'rb'))
    except:
        pass
    clean_tmp_f(f_prefix)
    return returned_val


def replace_cur_year(query: str) -> str:
    return re.sub('YEAR\s*\(\s*CURDATE\s*\(\s*\)\s*\)\s*', '2020', query, flags=re.IGNORECASE)


def exec_db_path_(sqlite_path: str, query: str) -> Tuple[str, Any]:
    query = replace_cur_year(query)
    cursor = get_cursor_path(sqlite_path)
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        cursor.connection.close()
        return 'result', result
    except Exception as e:
        cursor.close()
        cursor.connection.close()
        return 'exception', e
