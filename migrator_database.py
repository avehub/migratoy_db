#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库迁移框架
支持复杂的多对多关系数据迁移
"""

import logging
import time
import traceback
import decimal
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple, Callable
import pymysql
from sshtunnel import SSHTunnelForwarder

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('migration.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class DatabaseConfig:
    """数据库配置"""
    host: str
    port: int
    username: str
    password: str
    database: str
    charset: str = 'utf8mb4'
    # SSH隧道配置
    ssh_host: str = None
    ssh_port: int = 22
    ssh_username: str = None
    ssh_password: str = None
    ssh_private_key_path: str = None  # SSH私钥路径
    ssh_private_key_password: str = None  # 私钥密码（如果有）


@dataclass
class MigrationResult:
    """迁移结果"""
    success_count: int = 0
    error_count: int = 0
    errors: List[str] = None

    def __post_init__(self):
        if self.errors is None:
            self.errors = []


class DatabaseManager:
    """数据库连接管理器"""

    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.connection = None
        self.ssh_tunnel = None

    def connect(self):
        """建立数据库连接"""
        ssl_config = None
        # 如果配置了SSH，则建立SSH隧道
        logger.info("Start connect to database")
        if hasattr(self.config, 'ssh_host') and self.config.ssh_host:
            logger.info("Before connect to ssh")
            self.ssh_tunnel = SSHTunnelForwarder(
                (self.config.ssh_host, self.config.ssh_port or 22),
                ssh_username=self.config.ssh_username,
                ssh_pkey=self.config.ssh_private_key_path,
                # ssh_private_key_password=self.config.ssh_private_key_password,
                remote_bind_address=(self.config.host, self.config.port or 3306)
            )
            self.ssh_tunnel.start()
            # 更新连接信息，连接到本地隧道端口
            db_host = self.config.host
            db_port = self.ssh_tunnel.local_bind_port
        else:
            logger.info("Before connect to host")
            db_host = self.config.host
            db_port = self.config.port
        try:

            self.connection = pymysql.connect(
                host=db_host,
                port=db_port,
                user=self.config.username,
                password=self.config.password,
                database=self.config.database,
                charset=self.config.charset,
                cursorclass=pymysql.cursors.DictCursor,
                autocommit=False,
            )
            logger.info(f"Connected to database: {self.config.database}")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise

    def disconnect(self):
        """关闭数据库连接"""
        if self.connection:
            self.connection.close()
            self.connection = None
            logger.info("Database connection closed")

        if self.ssh_tunnel:
            self.ssh_tunnel.stop()
            self.ssh_tunnel = None
            logger.info("SSH tunnel closed")

    def execute_query(self, sql: str, params: tuple = None) -> List[Dict]:
        """执行查询"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql, params)
                return cursor.fetchall()
        except Exception as e:
            logger.error(f"Query execution failed: {sql}, Error: {e}")
            raise

    def execute_insert(self, sql: str, params: tuple = None) -> int:
        """执行插入操作"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql, params)
                return cursor.lastrowid
        except Exception as e:
            logger.error(f"Insert execution failed: {sql}, Error: {e}")
            raise

    def execute_batch_insert(self, sql: str, params_list: List[tuple]) -> int:
        """批量插入"""
        try:
            with self.connection.cursor() as cursor:
                return cursor.executemany(sql, params_list)
        except Exception as e:
            logger.error(f"Batch insert failed: {sql}, Error: {e}")
            raise

    def commit(self):
        """提交事务"""
        if self.connection:
            self.connection.commit()

    def rollback(self):
        """回滚事务"""
        if self.connection:
            self.connection.rollback()


class BaseMigrator(ABC):
    """基础迁移器抽象类"""

    def __init__(self, old_db: DatabaseManager, new_db: DatabaseManager):
        self.old_db = old_db
        self.new_db = new_db
        self.name = self.__class__.__name__
        self.result = MigrationResult()
        self.batch_size = 1000  # 默认批次大小

    @abstractmethod
    def extract_data(self) -> List[Dict]:
        """从旧数据库提取数据"""
        pass

    @abstractmethod
    def extract_batch(self, offset, limit) -> List[Dict]:
        """提取一批数据"""
        pass

    @abstractmethod
    def transform_data(self, old_data: List[Dict]) -> List[Dict]:
        """转换数据格式"""
        pass

    @abstractmethod
    def load_data(self, transformed_data: List[Dict]) -> None:
        """加载数据到新数据库"""
        pass

    def validate_data(self, data: List[Dict]) -> bool:
        """数据验证（可选重写）"""
        return True

    def pre_migrate_hook(self) -> None:
        """迁移前钩子（可选重写）"""
        pass

    def post_migrate_hook(self) -> None:
        """迁移后钩子（可选重写）"""
        pass

    def migrate(self) -> MigrationResult:
        """执行迁移流程"""
        logger.info(f"Starting migration: {self.name}")

        try:
            # 迁移前钩子
            self.pre_migrate_hook()

            # 提取数据
            logger.info(f"Extracting data from old database...")
            old_data = self.extract_data()
            logger.info(f"Extracted {len(old_data)} records")

            if not old_data:
                logger.warning("No data to migrate")
                return self.result

            # 转换数据
            logger.info("Transforming data...")
            transformed_data = self.transform_data(old_data)
            logger.info(f"Transformed {len(transformed_data)} records")

            # 验证数据
            if not self.validate_data(transformed_data):
                raise ValueError("Data validation failed")

            # 加载数据
            logger.info("Loading data to new database...")
            self.load_data(transformed_data)

            # 迁移后钩子
            self.post_migrate_hook()

            logger.info(f"Migration completed: {self.name}")

        except Exception as e:
            self.result.error_count += 1
            error_msg = f"Migration failed for {self.name}: {str(e)}"
            self.result.errors.append(error_msg)
            logger.error(error_msg)
            logger.error(traceback.format_exc())

        return self.result

    # 批量迁移数据
    def migrate_in_batches(self, batch_size=5000):
        """批量迁移数据"""
        self.batch_size = batch_size
        offset = 0

        try:
            while True:
                # 提取一批数据
                batch = self.extract_batch(offset, batch_size)
                if not batch:
                    break

                logger.info(f"Processing batch: offset={offset}, size={len(batch)}")

                # 转换数据
                transformed = self.transform_data(batch)

                # 验证数据
                if not self.validate_data(transformed):
                    raise ValueError("Data validation failed")

                # 加载数据
                self.load_data(transformed)

                # 提交当前批次
                self.new_db.connection.commit()

                # 更新偏移量
                offset += len(batch)

        except Exception as e:
            self.new_db.connection.rollback()
            self.result.error_count += 1
            self.result.errors.append(f"Batch failed at offset {offset}: {str(e)}")
            logger.error(f"Error in batch migration: {e}")
            raise
        return self.result


class UserMigrator(BaseMigrator):
    """用户数据迁移器示例"""

    def extract_data(self) -> List[Dict]:
        """从旧用户表提取数据"""
        sql = """
        SELECT 
            uid, nick_name, avatar, sex,  model, reg_time, diamond, appleid, gold, room_card, openid, unionid, pi, 
            phone, vip_level_point, vip_level,lottery_times, shen_fen_zheng_no, real_name, platform, diamond2, dai_li_zhe_kou
        FROM players LEFT JOIN player_diamond2 ON players.uid = player_diamond2.uid
        WHERE players.uid >= 1000000 
	    AND players.real_name <> ""
        """
        return self.old_db.execute_query(sql)

    def extract_batch(self, offset, limit):
        """从旧用户表提取数据(分页)"""
        sql = """
        SELECT 
            players.uid, nick_name, avatar, sex,  model, reg_time, diamond, appleid, gold, room_card, openid, unionid, pi,  
            phone, vip_level_point, vip_level,lottery_times, shen_fen_zheng_no, real_name, platform, diamond2, dai_li_zhe_kou
        FROM players LEFT JOIN player_diamond2 ON players.uid = player_diamond2.uid
        WHERE players.uid >= 1000000 
	    AND players.real_name <> ""
            LIMIT %s OFFSET %s
        """
        return self.old_db.execute_query(sql, (limit, offset))

    def transform_data(self, old_data: List[Dict]) -> List[Dict]:
        """转换用户数据"""
        transformed = []
        for user in old_data:
            # 基础用户信息
            platform = 0
            wechat = 0
            if user['platform'] == 0:
                platform = 1
            elif user['platform'] == 1:
                platform = 3
            elif user['platform'] == 2:
                platform = 3
            elif user['platform'] == 3:
                platform = 3
            elif user['platform'] == 6:
                platform = 2
                wechat = 1
            elif user['platform'] == 7:
                platform = 4
                wechat = 1
            user_data = {
                'uid': user['uid'],
                'avatar': user['avatar'] if len(user['avatar']) > 2 else f"avatar/avatar_{user['avatar']}.png",
                'name': user['nick_name'],
                'sex': user['sex'],
                'created': user['reg_time'],
                'dev_ident': user['model'],
                'diamond': user['diamond'],
                'yellow_diamond': user['diamond2'] if user['diamond2'] else 0,
                'gold': user['gold'] * 10000,
                'room_card': user['room_card'],
                'openid': user['openid'],
                'unionid': user['unionid'],
                'phone': user['phone'],
                'vip': user['vip_level'],
                'id_card': user['shen_fen_zheng_no'],
                'real_name': user['real_name'],
                'platform': platform,
                'apple_id': user['appleid'],
                'discount': user['dai_li_zhe_kou'] if user['dai_li_zhe_kou'] else 1,
                'pi': user['pi'] if user['pi'] and user['pi'] != "None" else str(user['uid']),
                'wechat': wechat,
            }

            # VIP信息（如果VIP等级 > 0）
            vip_data = None
            if user['vip_level'] > 0:
                vip_data = {
                    'uid': user['uid'],
                    'vip_id': user['vip_level'],
                    'cur_exp': user['vip_level_point'],
                    'recharge_amount': decimal.Decimal(user['vip_level_point'] / 10),
                }

            transformed.append({
                'user': user_data,
                'vip': vip_data
            })

        return transformed

    def load_data(self, transformed_data: List[Dict]) -> None:
        """加载用户数据到新数据库"""
        user_sql = """
        INSERT INTO user (
            uid, name, avatar, sex, created, dev_ident, diamond, gold, room_card, openid, unionid, 
            phone, id_card, real_name, platform, apple_id, yellow_diamond, discount, wechat, pi
        ) VALUES (
            %(uid)s, %(name)s, %(avatar)s, %(sex)s, %(created)s, %(dev_ident)s, %(diamond)s, %(gold)s, 
            %(room_card)s, %(openid)s, %(unionid)s, %(phone)s, %(id_card)s, %(real_name)s, %(platform)s, %(apple_id)s,
            %(yellow_diamond)s, %(discount)s, %(wechat)s, %(pi)s
        )
        """

        vip_sql = """
        INSERT INTO user_vip (
            uid, vip_id, cur_exp, recharge_amount
        ) VALUES (
            %(uid)s, %(vip_id)s, %(cur_exp)s, %(recharge_amount)s
        )
        """

        for data in transformed_data:
            try:
                # 插入用户基础信息
                user_id = self.new_db.execute_insert(user_sql, data['user'])

                # 插入VIP信息（如果存在）
                if data['vip']:
                    vip_data = data['vip'].copy()
                    self.new_db.execute_insert(vip_sql, vip_data)

                self.result.success_count += 1

            except Exception as e:
                self.result.error_count += 1
                error_msg = f"Failed to migrate user {data['user']['uid']}: {str(e)}"
                self.result.errors.append(error_msg)
                logger.error(error_msg)


class GameRecordMigrator(BaseMigrator):
    """游戏战绩迁移器示例"""
    # 只获取最近7天内的数据
    end_time = int(time.time())
    start_time = end_time - 7 * 24 * 60 * 60


    def extract_data(self) -> List[Dict]:
        """提取房间战绩和详情数据"""
        sql = """
        SELECT 
            record_id,
            game_type,
            room_id,
            game_type,
            end_time,
            play_setting,
            player_list,
            rule_type,
            round_index
        FROM game_record 
        WHERE r.end_time >= %s
        """
        return self.old_db.execute_query(sql, (self.start_time,))

    def extract_batch(self, offset, limit):
        """提取一批数据"""
        sql = """
        SELECT 
            record_id,
            game_type,
            room_id,
            game_type,
            end_time,
            play_setting,
            player_list,
            rule_type,
            round_index
        FROM game_record 
        WHERE r.end_time >= %s
            LIMIT %s OFFSET %s
        """
        return self.old_db.execute_query(sql, (limit, offset))

    def transform_data(self, old_data: List[Dict]) -> List[Dict]:
        """转换战绩数据结构"""
        # 按record_id分组
        grouped_records = {}

        for row in old_data:
            record_id = row['record_id']

            if record_id not in grouped_records:
                grouped_records[record_id] = {
                    'room_record': {
                        'old_record_id': record_id,
                        'room_id': row['room_id'],
                        'room_name': row['room_name'],
                        'game_type': row['game_type'],
                        'total_rounds': row['total_rounds'],
                        'created_time': row['created_time'],
                        'finished_time': row['finished_time']
                    },
                    'total_records': {},  # 用户总战绩
                    'round_records': []  # 子局战绩
                }

            # 处理详情数据
            if row['detail_id']:
                user_id = row['user_id']

                # 累计总战绩
                if user_id not in grouped_records[record_id]['total_records']:
                    grouped_records[record_id]['total_records'][user_id] = {
                        'user_id': user_id,
                        'username': row['username'],
                        'total_score': 0,
                        'final_score': row['final_score']
                    }

                grouped_records[record_id]['total_records'][user_id]['total_score'] += row['score']

                # 子局战绩
                grouped_records[record_id]['round_records'].append({
                    'user_id': user_id,
                    'username': row['username'],
                    'round_num': row['round_num'],
                    'score': row['score']
                })

        return list(grouped_records.values())

    def load_data(self, transformed_data: List[Dict]) -> None:
        """加载战绩数据"""
        room_sql = """
        INSERT INTO room_records (
            old_record_id, room_id, room_name, game_type, total_rounds,
            created_time, finished_time
        ) VALUES (
            %(old_record_id)s, %(room_id)s, %(room_name)s, %(game_type)s,
            %(total_rounds)s, %(created_time)s, %(finished_time)s
        )
        """

        total_sql = """
        INSERT INTO total_records (
            record_id, user_id, username, total_score, final_score
        ) VALUES (
            %(record_id)s, %(user_id)s, %(username)s, %(total_score)s, %(final_score)s
        )
        """

        round_sql = """
        INSERT INTO round_records (
            record_id, user_id, username, round_num, score
        ) VALUES (
            %(record_id)s, %(user_id)s, %(username)s, %(round_num)s, %(score)s
        )
        """

        for record_data in transformed_data:
            try:
                # 插入房间战绩
                new_record_id = self.new_db.execute_insert(room_sql, record_data['room_record'])

                # 批量插入总战绩
                total_params = []
                for total_record in record_data['total_records'].values():
                    params = total_record.copy()
                    params['record_id'] = new_record_id
                    total_params.append(params)

                if total_params:
                    self.new_db.execute_batch_insert(total_sql, [tuple(p.values()) for p in total_params])

                # 批量插入子局战绩
                round_params = []
                for round_record in record_data['round_records']:
                    params = round_record.copy()
                    params['record_id'] = new_record_id
                    round_params.append(params)

                if round_params:
                    self.new_db.execute_batch_insert(round_sql, [tuple(p.values()) for p in round_params])

                self.result.success_count += 1

            except Exception as e:
                self.result.error_count += 1
                error_msg = f"Failed to migrate record {record_data['room_record']['old_record_id']}: {str(e)}"
                self.result.errors.append(error_msg)
                logger.error(error_msg)

class ClubMigrator(BaseMigrator):
    """俱乐部迁移器"""

    def extract_data(self) -> List[Dict]:
        """从旧表提取数据"""
        sql = """
            SELECT 
                uid, club_name, gong_gao, time, id, room_card, player_counts
            FROM club
            WHERE club_status = 1 
            """
        return self.old_db.execute_query(sql)

    def extract_batch(self, offset, limit):
        """提取一批数据"""
        sql = """
        SELECT 
            uid, club_name, gong_gao, time, id, room_card, player_counts
        FROM club
        WHERE club_status = 1 
        LIMIT %s OFFSET %s
        """
        return self.old_db.execute_query(sql, (limit, offset))


    def transform_data(self, old_data: List[Dict]) -> List[Dict]:
        """转换数据"""
        transformed = []

        for club in old_data:
            # 基础用户信息
            data = {
                'uid': club['uid'],
                'name': club['club_name'],
                'num': club['player_counts'],
                'created': club['time'],
                'id': club['id'],
                'room_card': club['room_card'],
                'notice': club['gong_gao'],
                'other': '{"pay_type": 0, "host_power_room": 3}'
            }

            club_user = {
                'uid': club['uid'],
                'role': 9,  # 3-普通成员 2-管理员  1-创建者，4-小黑屋
                'club_id': club['id'],
                'created': club['time'],
            }

            transformed.append({
                'club': data,
                'club_user': club_user,
            })

        return transformed

    def load_data(self, transformed_data: List[Dict]) -> None:
        """加载用户数据到新数据库"""
        club_sql = """
            INSERT INTO clubs (
                uid, name, num, created, room_card, id, other
            ) VALUES (
                %(uid)s, %(name)s, %(num)s, %(created)s, %(room_card)s, %(id)s, %(other)s
            )
            """

        club_user_sql = """
            INSERT INTO club_users (
                uid, club_id, role, created
            ) VALUES (
                %(uid)s, %(club_id)s, %(role)s, %(created)s
            )
            """

        for data in transformed_data:
            try:
                # 茶馆基础信息
                user_id = self.new_db.execute_insert(club_sql, data['club'])
                # 插入茶馆成员
                club_users = data['club_user'].copy()
                self.new_db.execute_insert(club_user_sql, club_users)

                self.result.success_count += 1

            except Exception as e:
                self.result.error_count += 1
                error_msg = f"Failed to migrate club_uid {data['club']['uid']} club_id {data['club']['id']}: {str(e)}"
                self.result.errors.append(error_msg)
                logger.error(error_msg)

class ClubUserMigrator(BaseMigrator):
    """俱乐部成员迁移器"""

    def extract_data(self) -> List[Dict]:
        """从旧表提取数据"""
        sql = """
            SELECT 
                uid, rule, time, club_id
            FROM club_members
            """
        return self.old_db.execute_query(sql)

    def extract_batch(self, offset, limit):
        """提取一批数据"""
        sql = """
        SELECT 
            uid, rule, time, club_id
        FROM club_members 
        LIMIT %s OFFSET %s
        """
        return self.old_db.execute_query(sql, (limit, offset))

    def transform_data(self, old_data: List[Dict]) -> List[Dict]:
        """转换数据"""
        transformed = []

        for club in old_data:
            role = 0
            status = 0
            # 源数据 3-普通成员 2-管理员  1-创建者，4-小黑屋
            if club['rule'] == 1:
                continue
            if club['rule'] == 2:
                role = 1
            if club['rule'] == 4:
                status = 1
            club_user = {
                'uid': club['uid'],
                'role': role,
                'club_id': club['club_id'],
                'created': club['time'],
                'status': status,
            }

            transformed.append({
                'club_user': club_user,
            })

        return transformed

    def load_data(self, transformed_data: List[Dict]) -> None:
        """加载用户数据到新数据库"""
        club_user_sql = """
            INSERT INTO club_users (
                uid, club_id, role, created, status
            ) VALUES (
                %(uid)s, %(club_id)s, %(role)s, %(created)s, %(status)s
            )
            """

        for data in transformed_data:
            try:
                # 插入茶馆成员
                club_users = data['club_user'].copy()
                self.new_db.execute_insert(club_user_sql, club_users)

                self.result.success_count += 1

            except Exception as e:
                self.result.error_count += 1
                error_msg = f"Failed to migrate club_id {data['club_id']} uid {data['uid']} : {str(e)}"
                self.result.errors.append(error_msg)
                logger.error(error_msg)

class MigrationOrchestrator:
    """迁移编排器"""

    def __init__(self, old_db_config: DatabaseConfig, new_db_config: DatabaseConfig):
        self.old_db = DatabaseManager(old_db_config)
        self.new_db = DatabaseManager(new_db_config)
        self.migrators: List[BaseMigrator] = []
        self.overall_result = MigrationResult()
        self.start_time = None
        self.end_time = None
        self.batch_size = 3000

    def add_migrator(self, migrator_class) -> None:
        """添加迁移器"""
        migrator = migrator_class(self.old_db, self.new_db)
        self.migrators.append(migrator)

    def run_migration(self) -> MigrationResult:
        """运行所有迁移"""
        logger.info("Starting database migration...")

        try:
            # 连接数据库
            self.old_db.connect()
            self.new_db.connect()

            # 执行迁移
            for migrator in self.migrators:
                logger.info(f"Running migrator: {migrator.name}")

                try:
                    # result = migrator.migrate()
                    result = migrator.migrate_in_batches(self.batch_size)
                    self.overall_result.success_count += result.success_count
                    self.overall_result.error_count += result.error_count
                    self.overall_result.errors.extend(result.errors)

                    # 提交当前迁移器的事务
                    self.new_db.commit()
                    logger.info(f"Migrator {migrator.name} completed successfully")

                except Exception as e:
                    # 回滚当前迁移器的事务
                    self.new_db.rollback()
                    error_msg = f"Migrator {migrator.name} failed: {str(e)}"
                    self.overall_result.errors.append(error_msg)
                    logger.error(error_msg)

            logger.info("Database migration completed")

        except Exception as e:
            logger.error(f"Migration orchestrator failed: {str(e)}")
            self.overall_result.errors.append(str(e))

        finally:
            # 断开数据库连接
            self.old_db.disconnect()
            self.new_db.disconnect()

        return self.overall_result

    def generate_report(self) -> str:
        """生成迁移报告"""
        report = f"""
=== 数据库迁移报告 ===
迁移时间: {datetime.now()}
成功记录数: {self.overall_result.success_count}
失败记录数: {self.overall_result.error_count}
总记录数: {self.overall_result.success_count + self.overall_result.error_count}

"""
        if self.overall_result.errors:
            report += "错误详情:\n"
            for i, error in enumerate(self.overall_result.errors, 1):
                report += f"{i}. {error}\n"

        return report


