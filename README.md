# migrator_db
通过python实现的批量mysql老数据迁移脚本，因为工期比较赶，做了的也比较简单，支持多表、分页导入

# 使用
```
# 拉取项目
git clone git@github.com:avehub/migratoy_db.git
# 新建配置文件
touch conf.py
# 配置文件conf.py内容
ENV = 'dev'  # dev | prod
class OLD_CONF:
    """ 旧数据库配置 """
    if ENV == 'prod':
        HOST = '127.0.0.1'
        PORT = 3306
        USERNAME = 'user'
        PASSWORD = 'passwd'
        DATABASE = 'new_db'
    else:
        HOST = '127.0.0.1'
        PORT = 3306
        USERNAME = 'root'
        PASSWORD = 'passwd'
        DATABASE = 'old_db'

class NEW_CONF:
    """ 新数据库配置 """
    if ENV == 'prod':
        HOST = '127.0.0.1'
        PORT = 3306
        USERNAME = 'user'
        PASSWORD = 'passwd'
        DATABASE = 'new_db'
    else:
        HOST = '127.0.0.1'
        PORT = 3306
        USERNAME = 'root'
        PASSWORD = 'passwd'
        DATABASE = 'old_db'


```


## 数据迁移
目前项目内迁移数据模块：用户模块、茶馆(俱乐部)模块、用户战绩
如果需要迁移不同表请进行编辑修改即可

## 以下内容为迁移的数据及关联表介绍（可不用关注）

### 用户模块
- 基础信息、用户资源（金币、钻石、黄钻）、用户VIP

#### 用户旧表信息
```
用户表： players
用户资源表（黄钻）： player_diamond2

用户注册平台platform字段解析：
    iOS商店 = 1,
    安卓不上商店版本 = 2,
    VIVO = 3,
    小米 = 4,
    OPPO = 5,
    微信H5 = 6,
    微信小游戏 = 7,
    _8_支付宝小游戏 = 8,//支付宝小游戏
    _9_就爱斗地主_微信小游戏 = 9,//就爱斗地主_微信小游戏
    _10_哔哩哔哩小游戏 = 10,//哔哩哔哩小游戏

自己注册了几个账号，手机号platform=0，微信platform=3，H5游客platform=6，App游客platform=1

```

#### 关联新表
```
用户表： user
用户等级： user_vip

```

### 茶馆模块
- 茶馆基础信息、基金、公告、茶馆成员

#### 茶馆旧表信息
```
茶馆表： club
茶馆用户关系表： club_members

茶馆状态club_status 字段解析：
默认为1，取值范围1正常，3关闭

茶馆及用户关系：
rule 字段含义：3-普通成员 2-管理员  1-创建者，4-小黑屋

```

#### 关联新表
```
茶馆表： clubs
茶馆用户关系表： club_users


```

### 战绩模块
- 战绩基础信息、房间号、玩家

#### 战绩旧表信息
```
战绩表： club
战绩用户关系表： club_members

战绩状态club_status 字段解析：
默认为1，取值范围1正常，3关闭

战绩及用户关系：
rule 字段含义：3-普通成员 2-管理员  1-创建者，4-小黑屋

```

#### 关联新表
```
战绩表： clubs
战绩用户关系表： club_users


```