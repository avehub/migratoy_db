ENV = 'dev'  # dev | prod
class OLD_CONF:
    """ 旧数据库配置 """
    if ENV == 'prod':
        HOST = '192.168.0.8'
        PORT = 33068
        USERNAME = 'lucky'
        PASSWORD = '5YZtRhMVaZtFCV'
        DATABASE = 'hjmj_db'
    else:
        HOST = '127.0.0.1'
        PORT = 3306
        USERNAME = 'root'
        PASSWORD = '1234'
        DATABASE = 'old'

class NEW_CONF:
    """ 新数据库配置 """
    if ENV == 'prod':
        HOST = '192.168.0.8'
        PORT = 33068
        USERNAME = 'lucky'
        PASSWORD = '5YZtRhMVaZtFCV'
        DATABASE = 'lucky_game'
    else:
        HOST = '127.0.0.1'
        PORT = 3306
        USERNAME = 'root'
        PASSWORD = '1234'
        DATABASE = 'game_server'
