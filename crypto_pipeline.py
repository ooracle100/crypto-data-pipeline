"""
Crypto Data Pipeline - ETL System
Fetches crypto prices from CoinGecko API and stores in local database
Demonstrates: Extract -> Transform -> Load
"""

import requests
import pandas as pd
from datetime import datetime, timedelta
import time
from typing import List
import sqlite3

class CryptoDataPipeline:
    def __init__(self):
        """Initialize pipeline with SQLite database"""
        self.base_url = "https://api.coingecko.com/api/v3"
        self.connection = None
        self._init_database()
    
    def _init_database(self):
        """Create database connection and tables"""
        self.connection = sqlite3.connect('crypto_data.db')
        print("✓ Connected to SQLite database")
        self._create_tables()
    
    def _create_tables(self):
        """Create necessary database tables"""
        cursor = self.connection.cursor()
        
        # Price history table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS price_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                coin_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                price_usd REAL NOT NULL,
                market_cap REAL,
                volume_24h REAL,
                price_change_24h REAL,
                timestamp DATETIME NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Summary stats table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS price_summary (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                coin_id TEXT NOT NULL,
                avg_price REAL,
                min_price REAL,
                max_price REAL,
                volatility REAL,
                record_count INTEGER,
                period_start DATETIME,
                period_end DATETIME,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        self.connection.commit()
        print("✓ Database tables created/verified")
    
    def extract_current_prices(self, coin_ids: List[str]) -> pd.DataFrame:
        """
        EXTRACT: Fetch current prices from CoinGecko API
        
        Args:
            coin_ids: List of coin IDs (e.g., ['bitcoin', 'ethereum'])
        """
        print(f"\n[EXTRACT] Fetching data for {len(coin_ids)} coins...")
        
        endpoint = f"{self.base_url}/coins/markets"
        params = {
            'vs_currency': 'usd',
            'ids': ','.join(coin_ids),
            'order': 'market_cap_desc',
            'sparkline': False
        }
        
        try:
            response = requests.get(endpoint, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if not data:
                print("✗ No data returned from API")
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = pd.DataFrame(data)
            df['timestamp'] = datetime.now()
            
            print(f"✓ Extracted {len(df)} records")
            return df
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                print("✗ Rate limited by CoinGecko. Wait 60 seconds and try again.")
            elif e.response.status_code == 401:
                print("✗ CoinGecko API requires authentication now. Using demo mode.")
            else:
                print(f"✗ HTTP Error: {e}")
            return pd.DataFrame()
        except Exception as e:
            print(f"✗ Extraction failed: {e}")
            return pd.DataFrame()
    
    def extract_historical_prices(self, coin_id: str, days: int = 30) -> pd.DataFrame:
        """
        EXTRACT: Fetch historical price data
        
        Args:
            coin_id: Coin ID (e.g., 'bitcoin')
            days: Number of days of history (max 90 for free tier)
        """
        print(f"\n[EXTRACT] Fetching {days}-day history for {coin_id}...")
        
        endpoint = f"{self.base_url}/coins/{coin_id}/market_chart"
        params = {
            'vs_currency': 'usd',
            'days': min(days, 90),  # CoinGecko free tier limit
            'interval': 'daily' if days > 1 else 'hourly'
        }
        
        try:
            # Add delay to avoid rate limiting
            time.sleep(2)
            
            response = requests.get(endpoint, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if 'prices' not in data:
                print("✗ No price data in response")
                return pd.DataFrame()
            
            # Parse prices
            prices = data['prices']
            df = pd.DataFrame(prices, columns=['timestamp_ms', 'price_usd'])
            df['timestamp'] = pd.to_datetime(df['timestamp_ms'], unit='ms')
            df['coin_id'] = coin_id
            df['symbol'] = coin_id[:3].upper()
            df = df.drop('timestamp_ms', axis=1)
            
            print(f"✓ Extracted {len(df)} historical records")
            return df
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                print("✗ Rate limited. CoinGecko free tier allows ~10-50 calls/minute")
            elif e.response.status_code == 401:
                print("✗ API authentication required (free tier exhausted)")
            else:
                print(f"✗ HTTP Error: {e}")
            return pd.DataFrame()
        except Exception as e:
            print(f"✗ Historical extraction failed: {e}")
            return pd.DataFrame()
    
    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        TRANSFORM: Clean and enrich data
        
        Args:
            df: Raw DataFrame from API
        """
        print("\n[TRANSFORM] Processing data...")
        
        if df.empty:
            print("✗ No data to transform")
            return df
        
        # Handle current price data format
        # Handle current price data format
        if 'current_price' in df.columns:
            # Drop the absolute price_change_24h, keep only percentage
            if 'price_change_24h' in df.columns:
                df = df.drop('price_change_24h', axis=1)
            
            # Now rename
            df_clean = df.rename(columns={
                'id': 'coin_id',
                'current_price': 'price_usd',
                'total_volume': 'volume_24h',
                'price_change_percentage_24h': 'price_change_24h'
            })
            
            # Select exactly these 7 columns
            needed = ['coin_id', 'symbol', 'price_usd', 'market_cap', 
                     'volume_24h', 'price_change_24h', 'timestamp']
            df_clean = df_clean[needed].copy()
            
        elif 'price_usd' in df.columns and 'coin_id' in df.columns:
            df_clean = df.copy()
            
            # Ensure all required columns exist
            if 'symbol' not in df_clean.columns:
                df_clean['symbol'] = df_clean['coin_id'].str[:3].str.upper()
            if 'market_cap' not in df_clean.columns:
                df_clean['market_cap'] = None
            if 'volume_24h' not in df_clean.columns:
                df_clean['volume_24h'] = None
            if 'price_change_24h' not in df_clean.columns:
                df_clean['price_change_24h'] = None
            
            # Select needed columns
            needed = ['coin_id', 'symbol', 'price_usd', 'market_cap', 
                     'volume_24h', 'price_change_24h', 'timestamp']
            df_clean = df_clean[needed].copy()
            
        else:
            print("✗ Unrecognized data format")
            return pd.DataFrame()
        
        # Data quality checks
        df_clean = df_clean.dropna(subset=['price_usd'])
        df_clean = df_clean[df_clean['price_usd'] > 0]
        
        # Round numeric values
        numeric_cols = ['price_usd', 'market_cap', 'volume_24h', 'price_change_24h']
        for col in numeric_cols:
            if col in df_clean.columns:
                try:
                    df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce').round(2)
                except:
                    pass  # Skip if can't convert to numeric
        
        print(f"✓ Transformed {len(df_clean)} valid records")
        return df_clean
    
    def load_data(self, df: pd.DataFrame):
        """
        LOAD: Insert data into database
        
        Args:
            df: Cleaned DataFrame to load
        """
        print("\n[LOAD] Inserting into database...")
        
        if df.empty:
            print("✗ No data to load")
            return
        
        cursor = self.connection.cursor()
        
        # Ensure column order matches table schema
        required_cols = ['coin_id', 'symbol', 'price_usd', 'market_cap', 
                        'volume_24h', 'price_change_24h', 'timestamp']
        
        # Check if all columns exist
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            print(f"✗ Missing columns: {missing_cols}")
            return
        
        # Select ONLY the required columns (drop any extras)
        df_load = df[required_cols].copy()
        
        # Convert timestamp to string format for SQLite
        df_load['timestamp'] = df_load['timestamp'].astype(str)
        
        # Convert to list of tuples
        records = [tuple(row) for row in df_load.values]
        
        insert_sql = '''
            INSERT INTO price_history 
            (coin_id, symbol, price_usd, market_cap, volume_24h, price_change_24h, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        '''
        
        try:
            cursor.executemany(insert_sql, records)
            self.connection.commit()
            print(f"✓ Loaded {len(records)} records successfully")
        except Exception as e:
            self.connection.rollback()
            print(f"✗ Load failed: {e}")
            print(f"   Columns in DataFrame: {df_load.columns.tolist()}")
            print(f"   First record sample: {records[0] if records else 'No records'}")
    
    def generate_summary_stats(self, coin_id: str):
        """Generate and store summary statistics"""
        print(f"\n[ANALYTICS] Generating summary for {coin_id}...")
        
        cursor = self.connection.cursor()
        
        query = '''
            SELECT price_usd, timestamp 
            FROM price_history 
            WHERE coin_id = ?
            ORDER BY timestamp DESC 
            LIMIT 1000
        '''
        cursor.execute(query, (coin_id,))
        results = cursor.fetchall()
        
        if not results:
            print(f"✗ No data found for {coin_id}")
            return
        
        prices = [float(r[0]) for r in results]
        timestamps = [r[1] for r in results]
        
        # Calculate statistics
        stats = {
            'coin_id': coin_id,
            'avg_price': sum(prices) / len(prices),
            'min_price': min(prices),
            'max_price': max(prices),
            'volatility': pd.Series(prices).std(),
            'record_count': len(prices),
            'period_start': min(timestamps),
            'period_end': max(timestamps)
        }
        
        insert_sql = '''
            INSERT INTO price_summary 
            (coin_id, avg_price, min_price, max_price, volatility, 
             record_count, period_start, period_end)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        '''
        
        cursor.execute(insert_sql, (
            stats['coin_id'], stats['avg_price'], stats['min_price'],
            stats['max_price'], stats['volatility'], stats['record_count'],
            stats['period_start'], stats['period_end']
        ))
        self.connection.commit()
        
        print(f"✓ Summary generated:")
        print(f"   Avg: ${stats['avg_price']:.2f}")
        print(f"   Min: ${stats['min_price']:.2f}")
        print(f"   Max: ${stats['max_price']:.2f}")
        print(f"   Volatility: ${stats['volatility']:.2f}")
        print(f"   Records: {stats['record_count']}")
    
    def query_trends(self, coin_id: str, hours: int = 24) -> pd.DataFrame:
        """Query price trends from database"""
        print(f"\n[QUERY] Fetching {hours}h trends for {coin_id}...")
        
        cutoff = datetime.now() - timedelta(hours=hours)
        
        query = '''
            SELECT timestamp, price_usd, price_change_24h
            FROM price_history
            WHERE coin_id = ? AND timestamp >= ?
            ORDER BY timestamp ASC
        '''
        
        df = pd.read_sql_query(query, self.connection, params=(coin_id, cutoff))
        
        if not df.empty:
            print(f"✓ Retrieved {len(df)} trend records")
            
            if len(df) > 1:
                price_change = ((df['price_usd'].iloc[-1] - df['price_usd'].iloc[0]) 
                               / df['price_usd'].iloc[0] * 100)
                print(f"   Price change: {price_change:+.2f}%")
        else:
            print(f"✗ No trend data found for {coin_id} in last {hours} hours")
        
        return df
    
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            print("\n✓ Database connection closed")

def main():
    """Main ETL pipeline execution"""
    print("="*60)
    print("CRYPTO DATA PIPELINE - ETL System")
    print("="*60)
    
    pipeline = CryptoDataPipeline()
    
    try:
        # Define coins to track
        coins = ['bitcoin']
        
        # ETL Process 1: Current prices
        print("\n--- ETL CYCLE 1: Current Prices ---")
        raw_data = pipeline.extract_current_prices(coins)
        
        if not raw_data.empty:
            clean_data = pipeline.transform_data(raw_data)
            pipeline.load_data(clean_data)
            total_records = len(clean_data)
        else:
            print("⚠️  Skipping load - no current price data")
            total_records = 0
        
        # Wait to avoid rate limiting
        print("\n⏳ Waiting 5 seconds to avoid rate limits...")
        time.sleep(5)
        
        # ETL Process 2: Historical data (just Bitcoin to avoid rate limits)
        print("\n--- ETL CYCLE 2: Historical Data (Bitcoin only) ---")
        historical = pipeline.extract_historical_prices('bitcoin', days=30)
        
        if not historical.empty:
            clean_historical = pipeline.transform_data(historical)
            pipeline.load_data(clean_historical)
            total_records += len(clean_historical)
        else:
            print("⚠️  Skipping load - no historical data (likely rate limited)")
        
        # Analytics (only if we have data)
        print("\n--- ANALYTICS PHASE ---")
        if total_records > 0:
            pipeline.generate_summary_stats('bitcoin')
            
            # Query results
            print("\n--- QUERY PHASE ---")
            trends = pipeline.query_trends('bitcoin', hours=168)
            
            if not trends.empty and len(trends) <= 10:
                print(f"\nAll trend data:")
                print(trends)
            elif not trends.empty:
                print(f"\nFirst 5 trend records:")
                print(trends.head())
        else:
            print("⚠️  Skipping analytics - no data in database")
        
        print("\n" + "="*60)
        print("✓ PIPELINE COMPLETE")
        print(f"  Database: crypto_data.db")
        print(f"  Total records processed: {total_records}")
        print("="*60)
        
        if total_records == 0:
            print("\n⚠️  WARNING: No data was collected!")
            print("   This is likely due to CoinGecko rate limiting.")
            print("   Free tier allows ~10-50 calls per minute.")
            print("   Try again in 1-2 minutes, or run with just current prices.")
        
    except Exception as e:
        print(f"\n✗ Pipeline error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        pipeline.close()

if __name__ == "__main__":
    main()
