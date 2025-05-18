from clickhouse_driver import Client
import logging
from datetime import datetime

class ClickHouseConnector:
    def __init__(self, config):
        self.client = Client(
            host=config.CH_HOST,
            port=config.CH_PORT,
            user=config.CH_USER,
            password=config.CH_PASSWORD,
            **config.CH_CONNECT_ARGS
        )
        self.logger = logging.getLogger('clickhouse')

    def initialize_tables(self):
        """初始化所有表结构（重建模式）"""
        # 1. 分类表
        self._execute_sql('''
                          CREATE TABLE IF NOT EXISTS categories
                          (
                              category_id
                              UInt32,
                              name
                              String,
                              hierarchy
                              UInt8,
                              weight
                              Float32,
                              parent_id
                              UInt32
                          )
                              ENGINE = MergeTree
                          (
                          )
                              ORDER BY
                          (
                              hierarchy,
                              category_id
                          )
                          ''')

        # 2. 产品表
        self._execute_sql('''
                          CREATE TABLE IF NOT EXISTS products
                          (
                              product_id
                              String,
                              category_id
                              UInt32,
                              name
                              String,
                              weight
                              Float32,
                              price
                              Float32,
                              change_count
                              UInt32
                          )
                              ENGINE = MergeTree
                          (
                          )
                              ORDER BY
                          (
                              category_id,
                              product_id
                          )
                          ''')

        # 3. 价格表
        self._execute_sql('''
                          CREATE TABLE IF NOT EXISTS daily_prices
                          (
                              date
                              Date,
                              product_id
                              String,
                              category_id
                              UInt32,
                              price
                              Float32
                          )
                              ENGINE = MergeTree
                          (
                          )
                              ORDER BY
                          (
                              date,
                              category_id,
                              product_id
                          )
                              PARTITION BY toYYYYMM (date)
        ''')

        self._execute_sql('''
                          CREATE TABLE IF NOT EXISTS price_indices
(
    date Date,
    cavallo_index Float64,
    tmall_index Float64,
    base_date Date
) ENGINE = MergeTree()
ORDER BY (date)
                          ''')

    def _execute_sql(self, sql: str):
        """执行SQL语句"""
        try:
            self.client.execute(sql)
            self.logger.debug(f"Executed SQL: {sql.split()[0]}...")
        except Exception as e:
            self.logger.error(f"SQL execution failed: {str(e)}")
            raise

    def insert_data(self, table: str, data: list):
        """批量插入数据（增强类型检查）"""
        if not data:
            self.logger.warning(f"No data to insert into {table}")
            return

        # 根据表结构进行类型转换
        if table == 'categories':
            data = self._prepare_category_data(data)
        elif table == 'products':
            data = self._prepare_product_data(data)
        elif table == 'daily_prices':
            data = self._prepare_price_data(data)

        # 构造 ClickHouse 插入语句（不使用占位符，避免类型解析问题）
        query = f"INSERT INTO {table} VALUES"

        try:
            # 分批插入以避免内存问题
            batch_size = 50000
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                self.client.execute(query, batch)
                self.logger.info(f"Inserted {min(i + batch_size, len(data))}/{len(data)} rows to {table}")
        except Exception as e:
            self.logger.error(f"Insert failed. First 3 rows: {data[:3]}")
            raise

    def _prepare_category_data(self, data):
        """预处理分类数据"""
        prepared = []
        for row in data:
            try:
                prepared.append([
                    int(row[0]) if row[0] is not None else 0,  # category_id
                    str(row[1]),  # name
                    int(row[2]) if row[2] is not None else 1,  # hierarchy
                    float(row[3]) if row[3] is not None else 1.0,  # weight
                    int(row[4]) if row[4] is not None else None  # parent_id
                ])
            except Exception as e:
                self.logger.error(f"Invalid category data: {row} - {str(e)}")
                raise
        return prepared

    def _prepare_product_data(self, data):
        """预处理产品数据"""
        prepared = []
        for row in data:
            try:
                prepared.append([
                    str(row[0]),  # product_id
                    int(row[1]) if row[1] is not None else 0,  # category_id
                    str(row[2]),  # name
                    float(row[3]) if row[3] is not None else 0.0,  # weight
                    float(row[4]) if row[4] is not None else 0.0,  # price
                    int(row[5]) if row[5] is not None else 0  # change_count
                ])
            except Exception as e:
                self.logger.error(f"Invalid product data: {row} - {str(e)}")
                raise
        return prepared

    def _prepare_price_data(self, data):
        """预处理价格数据"""
        prepared = []
        for row in data:
            try:
                prepared.append([
                    self._parse_date(row[0]),  # date
                    str(row[1]),  # product_id
                    int(row[2]) if row[2] is not None else 0,  # category_id
                    float(row[3]) if row[3] is not None else 0.0  # price
                ])
            except Exception as e:
                self.logger.error(f"Invalid price data: {row} - {str(e)}")
                raise
        return prepared

    def _parse_date(self, date_value):
        """解析日期字段为 datetime.date 类型"""
        from datetime import datetime, date

        if isinstance(date_value, date):  # 注意：datetime 是 date 的子类
            return date_value
        if isinstance(date_value, str):
            try:
                return datetime.strptime(date_value, "%Y-%m-%d").date()
            except ValueError:
                self.logger.error(f"Invalid date string format: {date_value}")
                raise
        self.logger.error(f"Unsupported date type: {type(date_value)}")
        raise TypeError("Invalid date format")

    def execute_query(self, query: str, params=None):
        """执行查询并返回结果"""
        try:
            return self.client.execute(query, params)
        except Exception as e:
            self.logger.error(f"Query failed: {str(e)}")
            raise