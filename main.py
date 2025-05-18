from pathlib import Path
import pandas as pd
import logging
from datetime import datetime
from config.settings import settings
from storage.localdatasave import save_indices_to_local_file
from storage.minio_connector import MinIOConnector
from storage.clickhouse_connector import ClickHouseConnector
from analysis.price_index_python import PriceIndexCalculator
from data_clean.data_cleaner import DataCleaner


def setup_logging():
    """配置更结构化的日志记录"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)-8s - %(message)s',
        handlers=[
            logging.FileHandler("price_index.log"),
            logging.StreamHandler()
        ]
    )
    # 设置MinIO和ClickHouse的日志级别为WARNING，减少冗余信息
    logging.getLogger("minio").setLevel(logging.WARNING)
    logging.getLogger("clickhouse").setLevel(logging.WARNING)


class ProgressTracker:
    """用于跟踪和显示进度"""

    def __init__(self, total, name="items"):
        self.total = total
        self.name = name
        self.count = 0
        self.logger = logging.getLogger("progress")

    def update(self, increment=1):
        self.count += increment
        if self.count % 10 == 0 or self.count == self.total:
            self.logger.info(f"处理进度: {self.count}/{self.total} {self.name} ({self.count / self.total:.1%})")


def main():
    setup_logging()
    logger = logging.getLogger("main")

    try:
        logger.info("=== 价格指数计算系统启动 ===")
        logger.info(f"数据目录: {settings.DATA_DIR}")

        # 1. 初始化存储连接
        logger.info("初始化存储连接...")
        minio = MinIOConnector(settings)
        minio.ensure_bucket_exists()
        logger.info("MinIO连接成功")

        ch = ClickHouseConnector(settings)
        test_result = ch.execute_query("SELECT version()")
        logger.info(f"ClickHouse连接成功 (版本: {test_result[0][0]})")
        ch.initialize_tables()
        logger.info("数据库表结构初始化完成")

        # 2. 加载并清洗数据
        logger.info("加载并清洗数据...")
        cleaner = DataCleaner(Path(settings.DATA_DIR))
        category_df, product_df, price_df = cleaner.run_cleaning()

        logger.info(f"分类数据: {len(category_df)} 条")
        logger.info(f"产品数据: {len(product_df)} 条")
        logger.info(
            f"价格数据: {len(price_df):,} 条 (日期范围: {price_df['date'].min()} 至 {price_df['date'].max()})")

        # 3. 上传原始数据到MinIO
        logger.info("上传原始数据到MinIO备份...")

        # 上传分类和产品数据
        minio.upload_file(Path(settings.DATA_DIR) / "categories.csv", "raw/categories.csv")
        minio.upload_file(Path(settings.DATA_DIR) / "products.csv", "raw/products.csv")
        logger.info("分类和产品数据上传完成")

        # 上传价格文件（使用进度跟踪）
        price_files = list((Path(settings.DATA_DIR) / "daily_price").glob("*.csv"))
        progress = ProgressTracker(len(price_files), "价格文件")

        for price_file in price_files:
            object_name = f"raw/daily_prices/{price_file.name}"
            minio.upload_file(price_file, object_name)
            progress.update()

        logger.info(f"全部 {len(price_files)} 个价格文件上传完成")

        # 4. 导入数据到ClickHouse
        logger.info("导入数据到ClickHouse...")

        # 导入分类数据
        ch.insert_data('categories',
                       category_df[['category_id', 'name', 'hierarchy', 'weight', 'parent_id']].values.tolist())
        logger.info(f"导入分类数据: {len(category_df)} 条")

        # 导入产品数据
        ch.insert_data('products',
                       product_df[
                           ['product_id', 'category_id', 'name', 'weight', 'price', 'change_count']].values.tolist())
        logger.info(f"导入产品数据: {len(product_df)} 条")

        # 分批导入价格数据
        batch_size = 100000
        total_prices = len(price_df)
        progress = ProgressTracker(total_prices, "价格记录")

        for i in range(0, total_prices, batch_size):
            batch = price_df.iloc[i:i + batch_size]
            ch.insert_data('daily_prices',
                           batch[['date', 'product_id', 'category_id', 'price']].values.tolist())
            progress.update(batch_size)

        logger.info(f"全部 {total_prices:,} 条价格记录导入完成")

        # 5. 计算价格指数
        logger.info("计算价格指数...")
        calculator = PriceIndexCalculator(ch)

        cavallo_indices = calculator.calculate_cavallo_index()
        tmall_indices = calculator.calculate_tmall_index()
        logger.info(f"Cavallo指数计算完成: {len(cavallo_indices)} 条记录")
        logger.info(f"Tmall指数计算完成: {len(tmall_indices)} 条记录")

        # 6. 保存结果
        logger.info("保存计算结果...")

        # 保存到ClickHouse
        calculator.save_indices(cavallo_indices, tmall_indices)
        logger.info("指数结果已保存到数据库")

        # 同时保存到本地CSV文件
        save_indices_to_local_file(cavallo_indices, tmall_indices, filepath="data/price_indices.csv")
        logger.info("指数结果已保存到本地文件 data/price_indices.csv")

        # 保存到MinIO
        cavallo_df = pd.DataFrame(cavallo_indices)
        tmall_df = pd.DataFrame(tmall_indices)
        minio.upload_dataframe(cavallo_df, "results/cavallo_index.csv")
        minio.upload_dataframe(tmall_df, "results/tmall_index.csv")
        logger.info("指数结果已上传到MinIO")

        # 7. 最终统计
        logger.info("\n=== 处理结果统计 ===")
        logger.info(f"分类数据: {len(category_df)} 条")
        logger.info(f"产品数据: {len(product_df)} 条")
        logger.info(f"价格数据: {len(price_df):,} 条")
        logger.info(f"时间范围: {price_df['date'].min()} 至 {price_df['date'].max()}")
        logger.info(f"Cavallo指数: {len(cavallo_indices)} 条")
        logger.info(f"Tmall指数: {len(tmall_indices)} 条")
        logger.info("=== 处理完成 ===")

    except Exception as e:
        logger.error("\n处理失败", exc_info=True)
        raise
    finally:
        logger.info("程序执行结束")


if __name__ == "__main__":
    main()