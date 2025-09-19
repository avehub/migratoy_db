from datetime import datetime
from time import sleep
from migrator_database import ClubMigrator, ClubUserMigrator, DatabaseConfig, MigrationOrchestrator, UserMigrator
from conf import OLD_CONF, NEW_CONF

def main():
    """主函数示例"""
    # 数据库配置
    old_db_config = DatabaseConfig(
        host=OLD_CONF.HOST,
        port=OLD_CONF.PORT,
        username=OLD_CONF.USERNAME,
        password=OLD_CONF.PASSWORD,
        database=OLD_CONF.DATABASE,
    )

    new_db_config = DatabaseConfig(
        host=NEW_CONF.HOST,
        port=NEW_CONF.PORT,
        username=NEW_CONF.USERNAME,
        password=NEW_CONF.PASSWORD,
        database=NEW_CONF.DATABASE,
    )

    # 创建迁移编排器
    orchestrator = MigrationOrchestrator(old_db_config, new_db_config)

    # 添加迁移器（按依赖顺序分步完成）
    # 1. 先迁移用户数据
    orchestrator.add_migrator(UserMigrator)
    # 2. 再迁移俱乐部数据
    #orchestrator.add_migrator(ClubMigrator)
    # 21. 迁移俱部成员数据
    #orchestrator.add_migrator(ClubUserMigrator)
    # 3. 迁移游戏战绩数据
    # orchestrator.add_migrator(GameRecordMigrator)

    # 执行迁移
    result = orchestrator.run_migration()
    # 生成报告
    report = orchestrator.generate_report()
    # 保存报告到文件
    with open(f'migration_report_{datetime.now().strftime("%Y%m%d")}.log', 'w') as f:
        f.write(report)


if __name__ == "__main__":
    main()