import logging
import json
import numpy as np
import pandas as pd
import pendulum
from sqlalchemy_utils.types.enriched_datetime.pendulum_date import pendulum
from airflow import DAG
from airflow.sdk import dag, task
from airflow.timetables.interval import DeltaDataIntervalTimetable
from airflow.providers.http.hooks.http import HttpHook
from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook

def wait_for_secs():
    http_hook = HttpHook(method="GET", http_conn_id="llspacedevs")
    response = http_hook.run("api-throttle")
    json = response.json()
    wait_secs = json.get("next_use_secs")
    
    return wait_secs  # Default to 60

def process_dataframe_for_postgres(df: pd.DataFrame) -> pd.DataFrame:
    """Process DataFrame to handle numpy arrays and types for PostgreSQL compatibility."""
    df_processed = df.copy()
    
    # Convert numpy arrays to JSON strings for PostgreSQL compatibility
    # Check all columns for numpy arrays and convert them
    for col in df_processed.columns:
        # Check if column contains numpy arrays
        if df_processed[col].apply(lambda x: isinstance(x, np.ndarray)).any():
            df_processed[col] = df_processed[col].apply(
                lambda x: json.dumps(x.tolist()) if isinstance(x, np.ndarray) else (json.dumps([]) if pd.isna(x) else x)
            )
        
        # Also handle nested arrays in object columns (like country, logo_variants, etc.)
        elif df_processed[col].dtype == 'object':
            # Check if any value in the column is a numpy array
            df_processed[col] = df_processed[col].apply(
                lambda x: json.dumps(x.tolist()) if isinstance(x, np.ndarray) else x
            )
    
    # Convert numpy types to native Python types
    for col in df_processed.columns:
        if df_processed[col].dtype == np.dtype('bool'):
            df_processed[col] = df_processed[col].astype(bool)
        elif df_processed[col].dtype.kind in ['i', 'u', 'f']:  # integer, unsigned integer, float
            df_processed[col] = df_processed[col].astype(object).where(pd.notnull(df_processed[col]), None)
    
    return df_processed

def fetch_spacedevs_data(category: str, data_interval_start: pendulum.datetime, data_interval_end: pendulum.datetime) -> pd.DataFrame:
    """Fetch data from SpaceDevs API with rate limit handling and automatic retry."""
    http_hook = HttpHook(method="GET", http_conn_id="llspacedevs")
    start = data_interval_start.to_iso8601_string()
    end = data_interval_end.to_iso8601_string()
    
    # Ensure proper URL encoding for the parameters
    start_encoded = urllib.parse.quote(start)
    end_encoded = urllib.parse.quote(end)
    endpoint = f"{category}?ordering=id&updated_at_gte={start_encoded}&updated_at_lt={end_encoded}&limit=1000"
    
    max_retries = 3
    retry_count = 0
    
    while retry_count <= max_retries:
        logging.info(f"Fetching data from endpoint: {endpoint} (attempt {retry_count + 1}/{max_retries + 1})")
        
        try:
            response = http_hook.run(endpoint)
            
            if response.status_code == 429:
                # Rate limit exceeded - wait and retry
                wait_secs = wait_for_secs()
                logging.warning(f"Rate limit exceeded. Waiting for {wait_secs} seconds before retrying.")
                
                if retry_count < max_retries:
                    import time
                    time.sleep(wait_secs)
                    retry_count += 1
                    continue  # Retry the request
                else:
                    raise AirflowException(f"Rate limit exceeded after {max_retries} retries. Please try again later.")
                    
            elif response.status_code != 200:
                raise AirflowException(f"Failed to fetch data: {response.text}\n Status Code: {response.status_code}")
            
            # Success - process the response
            json_data = response.json()["results"]
            news_items = pd.json_normalize(json_data)
            return news_items
            
        except AirflowException as e:
            # Re-raise AirflowExceptions (rate limit errors after max retries)
            if "Rate limit exceeded after" in str(e):
                raise
            
            # For other AirflowExceptions, retry if we haven't exceeded max retries
            if retry_count < max_retries:
                logging.warning(f"Request failed, retrying... Error: {e}")
                retry_count += 1
                continue
            else:
                logging.error(f"Failed after {max_retries} retries: {e}")
                raise
        
        except Exception as e:
            # Handle unexpected errors
            if retry_count < max_retries:
                logging.warning(f"Unexpected error, retrying... Error: {e}")
                retry_count += 1
                continue
            else:
                logging.error(f"Failed after {max_retries} retries: {e}")
                raise AirflowException(f"Unexpected error after {max_retries} retries: {e}")
    
    # This should never be reached, but just in case
    raise AirflowException("Failed to fetch data after maximum retries")

def write_to_dataframe(category: str, data_interval_start: pendulum.datetime, data_interval_end: pendulum.datetime) -> pd.DataFrame:
        try:
            df = fetch_spacedevs_data(category, data_interval_start, data_interval_end)
            logger = logging.getLogger("airflow.task")
            # Print entire DataFrame
            #logger.info(f"DataFrame:\n{df.to_string()}")
            
            # Or print with better formatting
            logger.info("DataFrame columns: %s", df.columns.tolist())
            logger.info("DataFrame shape: %s", df.shape)
            logger.info("DataFrame head:\n %s", df.head(10).to_string())
            logger.info("Full DataFrame:\n %s", df.to_string(index=False))
            return df
        except Exception as e:
            logging.error(f"Error in fetch_articles task: {e}")
            raise

@dag(
    dag_id="spacedevs_launch_library",
    schedule=DeltaDataIntervalTimetable(delta=pendulum.duration(days=1)),
    start_date=pendulum.datetime(2025, month=12, day=7, tz="UTC"),
    catchup=True,
    tags=["space", "news", "launches", "spacecraft"],
    max_active_runs=18,
    max_active_tasks=128,
    default_args={
        "retries": 3,
        "retry_delay": pendulum.duration(minutes=1),
        "retry_exponential_backoff": True,
        "max_retry_delay": pendulum.duration(minutes=5),
    },
)
def spacedevs_data_dag():
    def create_fetch_task(category: str):
        """Create a fetch task for a specific category."""
        @task 
        def fetch_task(data_interval_start: pendulum.datetime, data_interval_end: pendulum.datetime) -> pd.DataFrame:
            try:
                df = write_to_dataframe(category, data_interval_start, data_interval_end)
                return df
            except Exception as e:
                logging.error(f"Error in fetch_{category} task: {e}")
                raise
        return fetch_task

    def create_write_task(table_name: str):
        """Create a write task for a specific table."""
        @task
        def write_task(df: pd.DataFrame):        
            pg_hook = PostgresHook(postgres_conn_id="datadazed")
            engine = pg_hook.get_sqlalchemy_engine()
            
            df_processed = process_dataframe_for_postgres(df)
            
            try:
                df_processed.to_sql(table_name, engine, if_exists="append", index=False)
                logging.info(f"Successfully wrote data to table {table_name}")
            except Exception as e:
                logging.error(f"Error writing data to table {table_name}: {e}")
                raise
        return write_task

    categories_tables = {
        "agencies": "ll_agencies"
    }
    for category, table_name in categories_tables.items():
        fetch = create_fetch_task(category)
        write = create_write_task(table_name)
        
        df = fetch()
        write(df)

spacedevs_data_dag()
