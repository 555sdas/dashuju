from pathlib import Path
import pandas as pd
import logging
from datetime import datetime
import chardet


class DataCleaner:
    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        self.logger = logging.getLogger('data_cleaner')

    def _read_csv_with_encoding_detection(self, file_path: Path):
        """自动检测编码读取CSV文件"""
        encodings = ['gbk', 'utf-8', 'gb18030', 'utf-8-sig', 'iso-8859-1']

        with open(file_path, 'rb') as f:
            raw_data = f.read(10000)
            detected = chardet.detect(raw_data)
            if detected['confidence'] > 0.9:
                encodings.insert(0, detected['encoding'])

        for encoding in encodings:
            try:
                df = pd.read_csv(file_path, encoding=encoding)
                if not df.empty and len(df.columns) > 1:
                    return df
            except (UnicodeDecodeError, pd.errors.ParserError):
                continue

        raise ValueError(f"无法读取文件 {file_path.name}")

    def clean_categories(self):
        """清洗分类数据"""
        df = self._read_csv_with_encoding_detection(self.data_dir / "categories.csv")

        # 统一列名并移除无用列
        df = df.rename(columns={'category': 'name', 'parent': 'parent_id'}) \
            .drop(columns=['price'], errors='ignore')

        # 处理空值和类型转换
        df['parent_id'] = df['parent_id'].fillna(0).astype('int64')
        df = df.astype({
            'category_id': 'int32',
            'hierarchy': 'int8',
            'weight': 'float32'
        })

        self.logger.info(f"清洗分类数据完成，共 {len(df)} 条记录")
        return df

    def clean_products(self, valid_category_ids):
        """清洗产品数据"""
        df = self._read_csv_with_encoding_detection(self.data_dir / "products.csv")

        # 基础清洗
        df = df.dropna(subset=['product_id', 'category_id', 'price'])
        df['product_id'] = df['product_id'].astype(str)
        df['category_id'] = df['category_id'].astype('int32')

        # 验证分类引用
        invalid_mask = ~df['category_id'].isin(valid_category_ids)
        if invalid_mask.any():
            self.logger.warning(f"过滤 {invalid_mask.sum()} 条无效分类引用的产品记录")
            df = df[~invalid_mask]

        # 处理异常价格
        price_stats = df.groupby('category_id')['price'].agg(['mean', 'std'])
        df = df.merge(price_stats, on='category_id')
        df = df[
            (df['price'] >= df['mean'] - 3 * df['std']) &
            (df['price'] <= df['mean'] + 3 * df['std'])
            ].drop(columns=['mean', 'std'])

        self.logger.info(f"清洗产品数据完成，共 {len(df)} 条记录")
        return df

    def clean_prices(self, valid_products):
        """清洗价格数据"""
        all_prices = []

        for file in (self.data_dir / "daily_price").glob("daily_prices_*.csv"):
            try:
                # 从文件名提取日期
                date_str = file.stem.split("_")[-1]
                date = datetime.strptime(date_str, "%Y%m%d").date()

                df = self._read_csv_with_encoding_detection(file)
                df['date'] = date

                # 基础清洗
                df = df.dropna(subset=['product_id', 'price'])
                df['product_id'] = df['product_id'].astype(str)
                df['price'] = pd.to_numeric(df['price'], errors='coerce').dropna()

                # 过滤无效产品引用
                df = df[df['product_id'].isin(valid_products)]
                all_prices.append(df)

            except Exception as e:
                self.logger.error(f"处理文件 {file.name} 失败: {str(e)}")
                continue

        if not all_prices:
            raise ValueError("未找到有效的价格文件")

        prices = pd.concat(all_prices)
        self.logger.info(
            f"清洗价格数据完成，共 {len(prices)} 条记录\n"
            f"日期范围: {prices['date'].min()} 至 {prices['date'].max()}"
        )
        return prices

    def run_cleaning(self):
        """执行完整清洗流程"""
        try:
            # 1. 清洗分类数据
            categories = self.clean_categories()

            # 2. 清洗产品数据
            products = self.clean_products(set(categories['category_id']))

            # 3. 清洗价格数据
            prices = self.clean_prices(set(products['product_id']))

            return categories, products, prices

        except Exception as e:
            self.logger.error("数据清洗流程失败", exc_info=True)
            raise