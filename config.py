import pymysql
from pymysql.constants import CLIENT
from pathlib import Path


# 配置数据库连接
db_config = {
    'host': 'rm-uf6q5h4a7tkthf82cno.mysql.rds.aliyuncs.com',  # 公网地址
    'port': 3306,                             # 端口
    'user': 'db_user4',                       # 数据库账号
    'password': 'Cangjie!user4',              # 数据库密码
    'database': 'db_test1',                   # 数据库名
    'charset': 'utf8mb4',                     # 字符编码
    'client_flag': CLIENT.MULTI_STATEMENTS,   # 允许执行多条SQL语句
    'cursorclass': pymysql.cursors.DictCursor # 返回字典格式的结果
}


# 回测目录
BACKTESTING_DIR = Path("data/backtesting")
BACKTESTING_NEW_DIR = BACKTESTING_DIR / "new"
BACKTESTING_PROCESSED_DIR = BACKTESTING_DIR / "processed"
BACKTESTING_BACKUP_DIR = BACKTESTING_DIR / "backup"