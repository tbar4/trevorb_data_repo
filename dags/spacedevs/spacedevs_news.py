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


def fetch_spacedevs_news(category: str, data_interval_start: pendulum.datetime, data_interval_end: pendulum.datetime) -> pd.DataFrame:
    """Fetch news data from SpaceDevs API for a specific category and time range."""
    http_hook = HttpHook(method="GET", http_conn_id="snapi")
    start = data_interval_start.to_iso8601_string()
    end = data_interval_end.to_iso8601_string()
    
    # Ensure proper URL encoding for the parameters
    import urllib.parse
    start_encoded = urllib.parse.quote(start)
    end_encoded = urllib.parse.quote(end)
    endpoint = f"{category}?updated_at_gte={start_encoded}&updated_at_lt={end_encoded}&limit=1000"
    
    logging.info(f"Fetching data from endpoint: {endpoint}")
    try:
        response = http_hook.run(endpoint)
        if response.status_code != 200:
            raise AirflowException(f"Failed to fetch data: {response.text}\n Status Code: {response.status_code}")
        
        json_data = response.json()["results"]
        news_items = pd.json_normalize(json_data)
        return news_items
        
    except Exception as e:
        logging.error(f"Error fetching data from spacedevs API: {e}")
        raise


def process_dataframe_for_postgres(df: pd.DataFrame) -> pd.DataFrame:
    """Process DataFrame to handle numpy arrays and types for PostgreSQL compatibility."""
    df_processed = df.copy()
    
    # Convert numpy arrays to JSON strings for PostgreSQL compatibility
    array_columns = ['authors', 'launches', 'events']
    for col in array_columns:
        if col in df_processed.columns:
            df_processed[col] = df_processed[col].apply(
                lambda x: json.dumps(x.tolist()) if isinstance(x, np.ndarray) else json.dumps([])
            )
    
    # Convert numpy types to native Python types
    for col in df_processed.columns:
        if df_processed[col].dtype == np.dtype('bool'):
            df_processed[col] = df_processed[col].astype(bool)
        elif df_processed[col].dtype.kind in ['i', 'u', 'f']:  # integer, unsigned integer, float
            df_processed[col] = df_processed[col].astype(object).where(pd.notnull(df_processed[col]), None)
    
    return df_processed


def write_to_dataframe(category: str, data_interval_start: pendulum.datetime, data_interval_end: pendulum.datetime) -> pd.DataFrame:
    """Fetch and log data for a specific category."""
    try:
        df = fetch_spacedevs_news(category, data_interval_start, data_interval_end)
        logger = logging.getLogger("airflow.task")
        
        # Log DataFrame information
        logger.info("DataFrame columns: %s", df.columns.tolist())
        logger.info("DataFrame shape: %s", df.shape)
        logger.info("DataFrame head:\n %s", df.head(10).to_string())
        
        return df
        
    except Exception as e:
        logging.error(f"Error in fetch task for {category}: {e}")
        raise

@dag(
    dag_id="spacedevs_news",
    schedule=DeltaDataIntervalTimetable(delta=pendulum.duration(days=1)),
    start_date=pendulum.datetime(2021, 5, 1, tz="UTC"),
    catchup=True,
    tags=["space", "news"],
    max_active_runs=18,
    max_active_tasks=128,
    default_args={
        "retries": 3,
        "retry_delay": pendulum.duration(minutes=1),
        "retry_exponential_backoff": True,
        "max_retry_delay": pendulum.duration(minutes=5),
    },
)
def spacedevs_news_dag():

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
                logging.info(f"Successfully wrote {len(df_processed)} records to table {table_name}.")
            except Exception as e:
                logging.error(f"Error writing to Postgres table {table_name}: {e}")
                raise
        return write_task

    # Create tasks for each category
    fetch_articles = create_fetch_task("articles")
    fetch_blogs = create_fetch_task("blogs")
    fetch_reports = create_fetch_task("reports")
    
    write_articles = create_write_task("space_devs_articles")
    write_blogs = create_write_task("space_devs_blogs")
    write_reports = create_write_task("space_devs_reports")
    
    # Execute the pipeline
    articles_data = fetch_articles()
    blogs_data = fetch_blogs()
    reports_data = fetch_reports()
    
    write_articles(articles_data)
    write_blogs(blogs_data)
    write_reports(reports_data)


spacedevs_news_dag()