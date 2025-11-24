from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.models import Variable
from datetime import datetime
import yfinance as yf

# Constants
SNOWFLAKE_CONN_ID= "snowflake_hw_5_viraat"
SYMBOLS=['AAPL','NVDA']

# Function to get Snowflake Cursor (for query execution)
def get_snowflake_cursor():
    hook= SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    return hook.get_conn().cursor()

# Task to extract data using yfinance
@task
def extract_data(symbol: str):
    """Extract data for a single symbol"""
    ticker= yf.Ticker(symbol) # gives out a ticker object to later access tons of stock-related data using built-in properties and methods
    df=ticker.history(period='180d').reset_index()
    df['Symbol']= symbol      # this line adds a new column called "Symbol" and fills every row with that value
    return df

# Task to transform data
@task
def transform_data(hist_df):
    """ Transforms data for a single symbol"""
    records=[]
    symbol= hist_df['Symbol'].iloc[0]
    for _,row in hist_df.iterrows():
        records.append((
            symbol,
            row['Date'].date().isoformat(),
            row['Open'],
            row['High'],
            row['Low'],
            row['Close'],
            row['Volume']
        ))
    return records

# Task to load data into Snowflake
@task
def load_data(transformed_data1, transformed_data2):     # for both Stock symbols
    """Load all symbol's transformed data"""
    # Combining both symbol data
    all_records = transformed_data1 + transformed_data2
    cursor = get_snowflake_cursor()
    target_table="USER_DB_MARMOT.RAW.STOCKS_INFO_LAB_1"
    try:
        cursor.execute('BEGIN;')
        cursor.execute(f"""CREATE TABLE IF NOT EXISTS {target_table} (
                       SYMBOL STRING,
                       DATE DATE,
                       OPEN FLOAT,
                       HIGH FLOAT,
                       LOW FLOAT,
                       CLOSE FLOAT,
                       VOLUME INT );""")
        
        # Merge each record (insert or update existing)
        merge_sql=f"""MERGE INTO {target_table} AS target
        USING (SELECT %s AS SYMBOL,
                      %s AS DATE,
                      %s AS OPEN,
                      %s AS HIGH,
                      %s AS LOW,
                      %s AS CLOSE,
                      %s AS VOLUME) AS src
        ON target.SYMBOL= src.SYMBOL AND target.DATE= src.DATE
        WHEN MATCHED THEN UPDATE SET
            target.OPEN=src.OPEN,            
            target.HIGH=src.HIGH,            
            target.LOW=src.LOW,
            target.CLOSE=src.CLOSE,            
            target.VOLUME=src.VOLUME
        WHEN NOT MATCHED THEN
            INSERT(SYMBOL,DATE,OPEN,HIGH,LOW,CLOSE,VOLUME)
            VALUES(src.SYMBOL, src.DATE, src.OPEN, src.HIGH, src.LOW, src.CLOSE, src.VOLUME);
            """
        
        # Execute merge for each record
        for record in all_records:
            cursor.execute(merge_sql,record)
        
        cursor.execute("COMMIT;")
    except Exception as e:
        cursor.execute("ROLLBACK;")
        raise e
    finally:
        cursor.close()

# Define the DAG
with DAG(
    dag_id="ETL_Stock_Price_LAB1",
    start_date=datetime(2025, 10, 6),
    schedule_interval="@daily",
    catchup=False
) as dag:
    # Extract tasks
    extract_aapl=extract_data.override(task_id='extract_aapl')('AAPL')
    extract_nvda=extract_data.override(task_id='extract_nvda')('NVDA')

    # Transform tasks
    transform_aapl = transform_data.override(task_id='transform_aapl')(extract_aapl)
    transform_nvda = transform_data.override(task_id='transform_nvda')(extract_nvda)
    
    # Load task combining both transformed datasets
    load_task = load_data(transform_aapl, transform_nvda)

