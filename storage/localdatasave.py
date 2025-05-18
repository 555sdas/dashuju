import os
import pandas as pd

def save_indices_to_local_file(cavallo_indices: list, tmall_indices: list, filepath="data/price_indices.csv"):
    """
    将Cavallo和Tmall指数合并并保存为本地CSV文件。
    """
    combined = {}

    for idx in cavallo_indices:
        date = idx['date']
        if date not in combined:
            combined[date] = {'date': date, 'base_date': idx['base_date']}
        combined[date]['cavallo_index'] = idx.get('cavallo_index', 0.0)

    for idx in tmall_indices:
        date = idx['date']
        if date not in combined:
            combined[date] = {'date': date, 'base_date': idx['base_date']}
        combined[date]['tmall_index'] = idx.get('tmall_index', 0.0)

    # 转换为列表，方便转成DataFrame
    records = []
    for date, vals in combined.items():
        records.append({
            'date': vals['date'],
            'cavallo_index': float(vals.get('cavallo_index', 0.0)),
            'tmall_index': float(vals.get('tmall_index', 0.0)),
            'base_date': vals['base_date']
        })

    # 确保目录存在
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    # 保存CSV
    df = pd.DataFrame(records)
    df.to_csv(filepath, index=False)