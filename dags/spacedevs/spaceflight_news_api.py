from airflow.timetables.interval import DeltaDataIntervalTimetable
import requests
import json
import os
import pendulum
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from bs4 import BeautifulSoup
from airflow.sdk import TaskGroup

S3_BUCKET_NAME = 'trevorb-data-repo'
API = "https://api.spaceflightnewsapi.net/v4/"

def fetch_and_save_data(**context):
    """Fetch data from Spaceflight News API and save to local file."""
    endpoint = context['endpoint']
    data_interval_start = context['data_interval_start'].isoformat()
    data_interval_end = context['data_interval_end'].isoformat()
    ds_nodash = context['ds_nodash']
    
    dataset_dir = f"/tmp/{endpoint}"
    os.makedirs(dataset_dir, exist_ok=True)
    
    response = requests.get(
        f"{API}{endpoint}/",
        params={
            "ordering": "-updated_at",
            "updated_at_gte": data_interval_start,
            "updated_at_lt": data_interval_end,
            "limit": 1000
        }
    )
    
    if response.status_code != 200:
        raise Exception(f"Failed to fetch data from {API}{endpoint}/: {response.status_code}")
    
    data = response.json()["results"]
    
    for article in data:
        if 'url' in article:
            try:
                content = extract_article_content(article['url'])
                article['content'] = content
            except Exception as e:
                print(f"Failed to extract content from {article['url']}: {e}")
                article['content'] = ""
    
    file_path = f"{dataset_dir}/{endpoint}_{ds_nodash}.json"
    with open(file_path, 'w') as f:
        json.dump(data, f)
    
    print(f"Saved {len(data)} items to {file_path}")
    return file_path

def extract_article_content(article_url: str) -> str:
    """Extract article content from URL using BeautifulSoup."""
    try:
        response = requests.get(article_url, timeout=10)
        if response.status_code != 200:
            return ""
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        content_div = soup.find('div', class_='article-content')
        if not content_div:
            content_div = soup.find('div', class_='content')
        if not content_div:
            content_div = soup.find('article')
        if not content_div:
            content_div = soup.find('main')
            
        if not content_div:
            paragraphs = soup.find_all('p')
        else:
            paragraphs = content_div.find_all('p')
            
        article_text = "\n".join(p.get_text().strip() for p in paragraphs if p.get_text().strip())
        
        return article_text
    except Exception as e:
        print(f"Error extracting content from {article_url}: {e}")
        return ""

def upload_to_s3(**context):
    """Upload local file to S3."""
    endpoint = context['endpoint']
    ds_nodash = context['ds_nodash']
    file_path = f"/tmp/{endpoint}/{endpoint}_{ds_nodash}.json"
    
    if not os.path.exists(file_path):
        print(f"File {file_path} not found, skipping upload")
        return
    
    s3_hook = S3Hook(aws_conn_id='minio_s3')
    s3_key = f"spaceflight_news/{endpoint}/{endpoint}_{ds_nodash}.json"
    
    s3_hook.load_file(
        filename=file_path,
        key=s3_key,
        bucket_name=S3_BUCKET_NAME,
        replace=True
    )
    
    print(f"Uploaded {file_path} to s3://{S3_BUCKET_NAME}/{s3_key}")

def create_dataset_tasks(dataset_name: str, dag: DAG):
    """Create tasks for fetching and uploading a specific dataset."""
    with TaskGroup(group_id=dataset_name) as task_group:
        fetch_task = PythonOperator(
            task_id=f'fetch_{dataset_name}',
            python_callable=fetch_and_save_data,
            op_kwargs={'endpoint': dataset_name},
            dag=dag,
        )
        
        upload_task = PythonOperator(
            task_id=f'upload_{dataset_name}_to_s3',
            python_callable=upload_to_s3,
            op_kwargs={'endpoint': dataset_name},
            dag=dag,
        )
        
        fetch_task >> upload_task
    
    return task_group

with DAG(
    dag_id="spacedevs_news",
    schedule=DeltaDataIntervalTimetable(delta=pendulum.duration(days=1)),
    start_date=pendulum.datetime(2021, 5, 21, tz="UTC"),
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
) as dag:
    
    articles_group = create_dataset_tasks("articles", dag)
    blogs_group = create_dataset_tasks("blogs", dag)
    reports_group = create_dataset_tasks("reports", dag)
