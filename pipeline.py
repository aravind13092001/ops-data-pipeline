import os
import requests
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

# --- CONFIGURATION ---
DATABASE_URL = os.getenv("DATABASE_URL")
API_URL = "https://api.coingecko.com/api/v3/coins/markets"
# Fetching top 10 coins in USD
API_PARAMS = {
    "vs_currency": "usd",
    "order": "market_cap_desc",
    "per_page": 10,
    "page": 1,
    "sparkline": "false"
}
# Hardcoded exchange rate for transformation demo (1 USD = ~84 INR)
USD_TO_INR_RATE = 84.0 

def get_db_connection():
    """Establishes connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        raise e

def init_db():
    """Initializes the database by running the schema.sql file."""
    print("Initializing database schema...")
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        with open('schema.sql', 'r') as f:
            schema_sql = f.read()
            cur.execute(schema_sql)
        conn.commit()
        print("Schema initialized successfully.")
    except Exception as e:
        conn.rollback()
        print(f"Schema initialization failed: {e}")
        raise e
    finally:
        cur.close()
        conn.close()

def extract_data():
    """Step 1: Extract data from CoinGecko API."""
    print("Fetching data from API...")
    try:
        response = requests.get(API_URL, params=API_PARAMS, timeout=10)
        response.raise_for_status() # Raises error for 4xx/5xx codes
        data = response.json()
        print(f"Successfully fetched {len(data)} records.")
        return data
    except requests.exceptions.RequestException as e:
        print(f"API Request failed: {e}")
        raise e

def transform_data(raw_data):
    """Step 2: Transform data (Calculate INR price, clean fields)."""
    print("Transforming data...")
    transformed = []
    
    for item in raw_data:
        # Transformation Logic:
        # 1. Calculate price in INR manually
        current_price_usd = item.get('current_price', 0)
        current_price_inr = current_price_usd * USD_TO_INR_RATE
        
        # 2. Extract only needed fields and format timestamps
        record = (
            item['id'],                     # coin_id
            item['symbol'].upper(),         # symbol (normalized to uppercase)
            item['name'],                   # name
            current_price_usd,              # current_price_usd
            round(current_price_inr, 2),    # current_price_inr (Calculated field)
            item.get('market_cap', 0),      # market_cap
            item.get('last_updated')        # api_last_updated
        )
        transformed.append(record)
    
    return transformed

def load_data(conn, data):
    """Step 3: Load data into SQL (Upsert to avoid duplicates)."""
    print("Loading data into database...")
    cur = conn.cursor()
    
    query = """
        INSERT INTO crypto_market_data 
        (coin_id, symbol, name, current_price_usd, current_price_inr, market_cap, api_last_updated)
        VALUES %s
        ON CONFLICT (coin_id) DO UPDATE SET
            current_price_usd = EXCLUDED.current_price_usd,
            current_price_inr = EXCLUDED.current_price_inr,
            market_cap = EXCLUDED.market_cap,
            api_last_updated = EXCLUDED.api_last_updated,
            fetched_at = CURRENT_TIMESTAMP;
    """
    
    try:
        execute_values(cur, query, data)
        conn.commit()
        print(f"Successfully upserted {len(data)} records.")
    except Exception as e:
        conn.rollback()
        print(f"Database load failed: {e}")
        raise e
    finally:
        cur.close()

def log_run_status(status, error_msg=None, records_count=0):
    """Step 4: Monitoring - Log the run status to the DB."""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        query = """
            INSERT INTO pipeline_logs (status, error_message, records_processed)
            VALUES (%s, %s, %s)
        """
        cur.execute(query, (status, error_msg, records_count))
        conn.commit()
        print(f"Run logged as: {status}")
    except Exception as e:
        print(f"Failed to log run status: {e}")
    finally:
        if conn:
            conn.close()

def run_pipeline():
    """Main execution flow."""
    try:
        # 0. Ensure DB tables exist
        init_db()
        
        # 1. Extract
        raw_data = extract_data()
        
        # 2. Transform
        clean_data = transform_data(raw_data)
        
        # 3. Load
        conn = get_db_connection()
        load_data(conn, clean_data)
        conn.close()
        
        # 4. Success Log
        log_run_status("SUCCESS", records_count=len(clean_data))
        
    except Exception as e:
        # 5. Failure Log
        print(f"Pipeline failed: {str(e)}")
        log_run_status("FAILED", error_msg=str(e))

if __name__ == "__main__":
    run_pipeline()