import logging
import numpy as np
from datetime import datetime
from typing import List, Dict
import pandas as pd
import time


class PriceIndexCalculator:
    def __init__(self, ch_connector):
        self.ch = ch_connector
        self.logger = logging.getLogger('price_index')

    def calculate_cavallo_index(self, base_date=None) -> List[Dict]:
        """计算Cavallo指数 (几何平均法)，使用批量查询和本地分组处理提高效率"""
        try:
            start_time = time.time()

            base_date = self._determine_base_date(base_date)
            self.logger.info(f"Calculating Cavallo index with base date: {base_date}")

            # 1. 获取基期价格
            base_prices = self._get_base_prices(base_date)
            if not base_prices:
                raise ValueError("No base period prices found")

            base_df = pd.DataFrame(list(base_prices.items()), columns=['product_id', 'base_price'])

            # 2. 一次性获取所有相关价格数据（仅包含基期有价格的商品）
            all_pids = list(base_prices.keys())
            all_price_rows = []

            batch_size = 1000
            for i in range(0, len(all_pids), batch_size):
                batch = all_pids[i:i + batch_size]
                rows = self.ch.execute_query(
                    """SELECT date, product_id, price
                       FROM daily_prices
                       WHERE product_id IN %(product_ids)s""",
                    {'product_ids': batch}
                )
                all_price_rows.extend(rows)

            if not all_price_rows:
                raise ValueError("No price data found")

            df = pd.DataFrame(all_price_rows, columns=['date', 'product_id', 'price'])
            df['date'] = pd.to_datetime(df['date'])

            # 3. 合并基期价格
            df = df.merge(base_df, on='product_id', how='inner')
            df = df[(df['price'] > 0) & (df['base_price'] > 0)]
            df['ratio'] = df['price'] / df['base_price']
            df['log_ratio'] = np.log(df['ratio'])

            # 4. 分组计算 Cavallo 指数
            grouped = df.groupby('date')['log_ratio'].mean().reset_index()
            grouped['cavallo_index'] = np.exp(grouped['log_ratio']) * 100

            # 5. 构建返回结果
            grouped['base_date'] = base_date.strftime('%Y-%m-%d')
            grouped['date'] = grouped['date'].dt.strftime('%Y-%m-%d')
            grouped['cavallo_index'] = grouped['cavallo_index'].round(4)

            results = grouped[['date', 'cavallo_index', 'base_date']].to_dict(orient='records')

            total_time = time.time() - start_time
            self.logger.info(f"Cavallo index calculation complete. Total time: {total_time:.2f} seconds")

            return results

        except Exception as e:
            self.logger.error(f"Cavallo index calculation failed: {str(e)}")
            return []

    def calculate_tmall_index(self, base_date=None) -> List[Dict]:
        """计算Tmall指数 (加权平均法)"""
        try:
            # 确定基期
            base_date = self._determine_base_date(base_date)
            self.logger.info(f"Calculating Tmall index with base date: {base_date}")

            # 获取分类权重
            category_weights = self._get_category_weights()
            if not category_weights:
                raise ValueError("No category weights found")

            # 获取基期分类均价
            base_avg = self._get_category_averages(base_date)
            if not base_avg:
                raise ValueError("No base period category averages")

            # 获取所有需要计算的日期
            dates = self._get_all_dates()
            results = []

            for date in dates:
                current_avg = self._get_category_averages(date)
                if not current_avg:
                    continue

                # 计算加权指数
                weighted_sum = 0
                total_weight = 0

                for cat_id, curr_avg in current_avg.items():
                    if cat_id in base_avg and base_avg[cat_id] > 0:
                        weight = category_weights.get(cat_id, 0)
                        ratio = curr_avg / base_avg[cat_id]
                        weighted_sum += ratio * weight
                        total_weight += weight

                index = (weighted_sum / total_weight) * 100 if total_weight > 0 else 0
                results.append({
                    'date': date.strftime('%Y-%m-%d'),
                    'tmall_index': round(index, 4),
                    'base_date': base_date.strftime('%Y-%m-%d')
                })

            return results

        except Exception as e:
            self.logger.error(f"Tmall index calculation failed: {str(e)}")
            return []

    def save_indices(self, cavallo_indices: List[Dict], tmall_indices: List[Dict]):
        """保存指数结果到数据库"""
        try:
            # 合并结果
            combined = {}
            for idx in cavallo_indices:
                date = idx['date']
                if date not in combined:
                    combined[date] = {'date': date, 'base_date': idx['base_date']}
                combined[date]['cavallo_index'] = idx.get('cavallo_index', 0.0)  # 默认值0.0

            for idx in tmall_indices:
                date = idx['date']
                if date not in combined:
                    combined[date] = {'date': date, 'base_date': idx['base_date']}
                combined[date]['tmall_index'] = idx.get('tmall_index', 0.0)  # 默认值0.0

            # 准备插入数据
            data = []
            for date, values in combined.items():
                data.append((
                    datetime.strptime(values['date'], '%Y-%m-%d').date(),
                    float(values.get('cavallo_index')) if values.get('cavallo_index') is not None else None,
                    float(values.get('tmall_index')) if values.get('tmall_index') is not None else None,
                    datetime.strptime(values['base_date'], '%Y-%m-%d').date()
                ))

            # 插入到ClickHouse
            if data:
                self.ch.insert_data('price_indices', data)
                self.logger.info(f"Saved {len(data)} index records to ClickHouse")

        except Exception as e:
            self.logger.error(f"Failed to save indices: {str(e)}")
            raise

    def _determine_base_date(self, specified_date=None):
        """确定基期日期"""
        if specified_date:
            return pd.to_datetime(specified_date).date()

        result = self.ch.execute_query("SELECT min(date) FROM daily_prices")
        return result[0][0] if result else None

    def _get_all_dates(self):
        """获取所有价格数据日期"""
        result = self.ch.execute_query("SELECT DISTINCT date FROM daily_prices ORDER BY date")
        return [row[0] for row in result] if result else []

    def _get_base_prices(self, base_date):
        """获取基期价格 {product_id: price}"""
        result = self.ch.execute_query(
            "SELECT product_id, price FROM daily_prices WHERE date = %(date)s",
            {'date': base_date}
        )
        return dict(result) if result else {}

    def _get_current_prices(self, date, product_ids, batch_size=1000):
        result = {}
        for i in range(0, len(product_ids), batch_size):
            batch_ids = list(product_ids)[i:i + batch_size]
            rows = self.ch.execute_query(
                """SELECT product_id, price
                   FROM daily_prices
                   WHERE date = %(date)s
                     AND product_id IN %(product_ids)s""",
                {'date': date, 'product_ids': batch_ids}
            )
            result.update(dict(rows))
        return result

    def _get_category_weights(self):
        """获取分类权重 {category_id: weight}"""
        result = self.ch.execute_query(
            "SELECT category_id, weight FROM categories"
        )
        return dict(result) if result else {}

    def _get_category_averages(self, date):
        """获取分类平均价格 {category_id: avg_price}"""
        result = self.ch.execute_query(
            """SELECT category_id, avg(price)
               FROM daily_prices
               WHERE date = %(date)s
               GROUP BY category_id""",
            {'date': date}
        )
        return dict(result) if result else {}