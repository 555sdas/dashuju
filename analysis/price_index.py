import logging
from datetime import datetime
from typing import List, Dict
import pandas as pd


class PriceIndexCalculator:
    def __init__(self, ch_connector):
        self.ch = ch_connector
        self.logger = logging.getLogger('price_index')

    def calculate_cavallo_index(self, base_date=None) -> List[Dict]:
        """用ClickHouse SQL计算Cavallo指数（几何平均法）"""
        try:
            base_date = self._determine_base_date(base_date)
            self.logger.info(f"Calculating Cavallo index with base date: {base_date}")

            sql = f"""
            WITH
                toDate('{base_date}') AS base_date,

                (
                    SELECT product_id, price AS base_price
                    FROM daily_prices
                    WHERE date = base_date
                ) AS base_prices

            SELECT
                dp.date,
                exp(avg(log(dp.price / bp.base_price))) * 100 AS cavallo_index
            FROM
                daily_prices dp
                INNER JOIN base_prices bp ON dp.product_id = bp.product_id
            WHERE
                dp.price > 0 AND bp.base_price > 0
            GROUP BY
                dp.date
            ORDER BY
                dp.date
            """

            rows = self.ch.execute_query(sql)
            results = [
                {
                    'date': row[0].strftime('%Y-%m-%d') if isinstance(row[0], (datetime, pd.Timestamp)) else str(row[0]),
                    'cavallo_index': round(row[1], 4),
                    'base_date': base_date.strftime('%Y-%m-%d') if hasattr(base_date, 'strftime') else str(base_date)
                }
                for row in rows
            ]
            return results

        except Exception as e:
            self.logger.error(f"Cavallo index calculation failed: {str(e)}")
            return []

    def calculate_tmall_index(self, base_date=None) -> List[Dict]:
        """用ClickHouse SQL计算Tmall指数（加权平均法）"""
        try:
            base_date = self._determine_base_date(base_date)
            self.logger.info(f"Calculating Tmall index with base date: {base_date}")

            sql = f"""
            WITH
                toDate('{base_date}') AS base_date,

                (
                    SELECT category_id, avg(price) AS base_avg_price
                    FROM daily_prices
                    WHERE date = base_date
                    GROUP BY category_id
                ) AS base_avg_prices,

                (
                    SELECT category_id, weight
                    FROM categories
                ) AS category_weights

            SELECT
                dp_avg.date,
                sum(
                    (dp_avg.avg_price / bap.base_avg_price) * cw.weight
                ) / sum(cw.weight) * 100 AS tmall_index
            FROM
                (
                    SELECT date, category_id, avg(price) AS avg_price
                    FROM daily_prices
                    GROUP BY date, category_id
                ) dp_avg
                INNER JOIN base_avg_prices bap ON dp_avg.category_id = bap.category_id AND bap.base_avg_price > 0
                INNER JOIN category_weights cw ON dp_avg.category_id = cw.category_id
            GROUP BY
                dp_avg.date
            ORDER BY
                dp_avg.date
            """

            rows = self.ch.execute_query(sql)
            results = [
                {
                    'date': row[0].strftime('%Y-%m-%d') if isinstance(row[0], (datetime, pd.Timestamp)) else str(row[0]),
                    'tmall_index': round(row[1], 4),
                    'base_date': base_date.strftime('%Y-%m-%d') if hasattr(base_date, 'strftime') else str(base_date)
                }
                for row in rows
            ]
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

