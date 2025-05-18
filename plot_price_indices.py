import pandas as pd
import matplotlib.pyplot as plt
from clickhouse_driver import Client
from pathlib import Path
from config.settings import settings  # 保持你的导入

def read_price_indices_from_clickhouse():
    """从 ClickHouse 读取价格指数表"""
    client = Client(
        host=settings.CH_HOST,
        port=settings.CH_PORT,
        user=settings.CH_USER,
        password=settings.CH_PASSWORD,
        settings=settings.CH_CONNECT_ARGS.get("settings", {})
    )

    query = """
        SELECT date, cavallo_index, tmall_index
        FROM price_indices
        ORDER BY date
    """
    result = client.execute(query)
    df = pd.DataFrame(result, columns=['date', 'cavallo_index', 'tmall_index'])
    df['date'] = pd.to_datetime(df['date'])
    return df

def plot_indices(df):
    """绘制价格指数折线图并保存到项目根目录下的 data 文件夹"""
    plt.figure(figsize=(12, 6))

    if 'cavallo_index' in df.columns:
        plt.plot(df['date'], df['cavallo_index'], label='Cavallo Index', marker='o', linewidth=1.5)

    if 'tmall_index' in df.columns:
        plt.plot(df['date'], df['tmall_index'], label='Tmall Index', marker='s', linewidth=1.5)

    plt.xlabel('Date')
    plt.ylabel('Index Value')
    plt.title('Price Indices Over Time')
    plt.legend()
    plt.grid(True)
    plt.tight_layout()

    # 确保 data 文件夹存在
    output_dir = Path("data")
    output_dir.mkdir(exist_ok=True)

    output_path = output_dir / "price_indices.png"
    plt.savefig(output_path)
    print(f"Plot saved to: {output_path.resolve()}")

    plt.show()

if __name__ == '__main__':
    df = read_price_indices_from_clickhouse()
    plot_indices(df)
