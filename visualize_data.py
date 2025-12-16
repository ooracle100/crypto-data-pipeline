import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

def visualize_price_trends(db_path='crypto_data.db'):
    conn = sqlite3.connect(db_path)
    
    query = """
        SELECT coin_id, timestamp, price_usd
        FROM price_history
        WHERE coin_id = 'bitcoin'
        ORDER BY timestamp ASC
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    if df.empty:
        print("No data available")
        return
    
    df['timestamp'] = pd.to_datetime(df['timestamp'], format='mixed')
    
    plt.figure(figsize=(12, 6))
    
    for coin in df['coin_id'].unique():
        coin_data = df[df['coin_id'] == coin]
        plt.plot(coin_data['timestamp'], coin_data['price_usd'], 
                marker='o', label=coin.upper(), linewidth=2)
    
    plt.title('Cryptocurrency Price Trends', fontsize=16, fontweight='bold')
    plt.xlabel('Date', fontsize=12)
    plt.ylabel('Price (USD)', fontsize=12)
    plt.legend(fontsize=10)
    plt.grid(True, alpha=0.3)
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    filename = f'crypto_trends_{datetime.now().strftime("%Y%m%d")}.png'
    plt.savefig(filename, dpi=300)
    print(f"✓ Chart saved: {filename}")
    plt.show()

if __name__ == "__main__":
    visualize_price_trends()
