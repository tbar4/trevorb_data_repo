import logging
from sqlalchemy_utils.types.enriched_datetime.pendulum_date import pendulum
import pendulum
from airflow import DAG
from airflow.sdk import dag, task
from airflow.timetables.interval import DeltaDataIntervalTimetable
from airflow.providers.http.hooks.http import HttpHook
import pandas as pd
from airflow.exceptions import AirflowException
import logging

def fetch_spacedevs_news(category: str, data_interval_start: pendulum.datetime, data_interval_end: pendulum.datetime) -> pd.DataFrame:
    http_hook = HttpHook(method="GET", http_conn_id="spacedevs_api")
    start = data_interval_start.to_iso8601_string()
    end = data_interval_end.to_iso8601_string()
    # Ensure proper URL encoding for the parameters
    import urllib.parse
    start_encoded = urllib.parse.quote(start)
    end_encoded = urllib.parse.quote(end)
    endpoint = f"{category}?updated_at_gte={start_encoded}&updated_at_lt={end_encoded}"
    logging.info(f"Fetching data from endpoint: {endpoint}")
    try:
        response = http_hook.run(endpoint)
        if response.status_code != 200:
            raise AirflowException(f"Failed to fetch data: {response.text}\n Status Code: {response.status_code}")
        try:
            json_data = response.json()
        except AirflowException as e:
            raise AirflowException(f"Failed to parse JSON response: {e}")
        news_items = pd.json_normalize(json_data)
        return news_items
    except AirflowException as e:
        logging.error(f"Error fetching data from spacedevs API: {e}")
        raise

@dag(
    dag_id="spacedevs_news",
    schedule=DeltaDataIntervalTimetable(delta=pendulum.duration(hours=1)),
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=True,
    tags=["space", "news"],
    max_active_runs=1,
    max_active_tasks=16,
    default_args={
        "retries": 3,
        "retry_delay": pendulum.duration(minutes=5),
        "retry_exponential_backoff": True,
        "max_retry_delay": pendulum.duration(minutes=30),
    },
)
def spacedevs_news_dag():

    @task 
    def fetch_articles(data_interval_start: pendulum.datetime, data_interval_end: pendulum.datetime) -> pd.DataFrame:
        try:
            all_news = fetch_spacedevs_news("articles", data_interval_start, data_interval_end)
            print(all_news)
            return all_news
        except Exception as e:
            logging.error(f"Error in fetch_articles task: {e}")
            raise

    @task 
    def fetch_blogs(data_interval_start: pendulum.datetime, data_interval_end: pendulum.datetime) -> pd.DataFrame:
        try:
            all_news = fetch_spacedevs_news("blogs", data_interval_start, data_interval_end)
            print(all_news)
            return all_news
        except Exception as e:
            logging.error(f"Error in fetch_blogs task: {e}")
            raise

    @task 
    def fetch_reports(data_interval_start: pendulum.datetime, data_interval_end: pendulum.datetime) -> pd.DataFrame:
        try:
            all_news = fetch_spacedevs_news("reports", data_interval_start, data_interval_end)
            print(all_news)
            return all_news
        except Exception as e:
            logging.error(f"Error in fetch_reports task: {e}")
            raise

    fetch_articles()
    fetch_blogs()
    fetch_reports()

spacedevs_news_dag()