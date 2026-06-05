import pendulum
from pathlib import Path
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import yaml

PROJECT = Path(__file__).resolve().parent
with open(PROJECT.parent / "config.yaml", "r") as f:
    config = yaml.safe_load(f)

ENV_PATH = config["ENV_PATH"]
REQUIREMENTS_PATH = config["REQUIREMENTS_PATH"]
VENV_CACHE_PATH = config["VENV_CACHE_PATH"]

with open(REQUIREMENTS_PATH) as f:
    REQUIREMENTS = f.read().splitlines()

@task.virtualenv(requirements=REQUIREMENTS, venv_cache_path=VENV_CACHE_PATH)
def data_extraction(env_path: str):
    """
    Extract analytics one channel at a time

    Args:
        client: Youtube client from initialize_client()
        channel_id: Youtube Channel ID to extract from extract_channel_id()
    """
    import os
    import iso8601
    import logging
    import pandas as pd
    from dotenv import load_dotenv
    from sqlalchemy import create_engine
    from datetime import datetime, timedelta, timezone
    from googleapiclient.discovery import build

    logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

    load_dotenv(env_path)
    yt_api_key = os.getenv("YT_API_KEY")
    db_url = os.getenv("DB_URL")
    extract_period = datetime.now(timezone.utc) - timedelta(days=7)

    def initialize_client():
        """
        Build YouTube API client
        Requires Youtube Data API Key
        """

        logging.info("Initializing YouTube API client...")
        client = build('youtube', 'v3', developerKey=yt_api_key)
        return client

    def extract_channel_id(client, handle):
        """
        Get channel ID from handle

        Args:
            client: Youtube client from initialize_client()
            handle: Youtube channel handle to extract data
        """
        try:
            channel_res = client.channels().list(
                part="id,contentDetails",
                forHandle=handle
            ).execute()
            if not channel_res['items']:
                raise ValueError(f"Channel not found: {handle}")
            channel_id = channel_res['items'][0]['id']
        except Exception as e:
            logging.error(e)

        return channel_id

    def extract_analytics(client, channel_id, handle):
        """
        Extract analytics one channel at a time

        Args:
            client: Youtube client from initialize_client()
            channel_id: Youtube Channel ID to extract from extract_channel_id()
        """

        data = []
        next_page = None

        # Search for completed/archived live broadcasts
        try:
            while True:
                search_res = client.search().list(
                    part="snippet",
                    channelId=channel_id,
                    eventType="completed",
                    type="video",
                    order="date",
                    publishedAfter=extract_period.isoformat(),
                    maxResults=50,
                    pageToken=next_page
                ).execute()

                if not search_res.get('items'):
                    break

                video_ids = [item['id']['videoId'] for item in search_res['items']]
                videos_res = client.videos().list(
                    part="snippet,liveStreamingDetails,contentDetails,status,statistics",
                    id=','.join(video_ids)
                ).execute()

                for video_data in videos_res['items']:
                    # Check if it has live streaming details
                    live_info = video_data.get('liveStreamingDetails', {})
                    if not live_info:
                        continue

                    # Must have actual start time to be a completed live broadcast
                    if 'actualStartTime' not in live_info:
                        continue

                    vid_id = video_data['id']
                    pub_date_str = video_data['snippet']['publishedAt']
                    pub_date = iso8601.parse_date(pub_date_str)

                    # Data filter
                    if pub_date < extract_period:
                        continue

                    # Calculate duration if available
                    duration_hours = None
                    if 'actualEndTime' in live_info and 'actualStartTime' in live_info:
                        start_time = iso8601.parse_date(live_info['actualStartTime'])
                        end_time = iso8601.parse_date(live_info['actualEndTime'])
                        duration_seconds = (end_time - start_time).total_seconds()
                        duration_hours = round(duration_seconds / 3600, 2)
                
                    # Get broadcast content type
                    live_broadcast_content = video_data['snippet'].get('liveBroadcastContent', 'none')

                    data.append({
                        'handle': handle,
                        'video_id': vid_id,
                        'title': video_data['snippet']['title'],
                        'published_at': pub_date_str,
                        'live_broadcast_content': live_broadcast_content,
                        'actual_start_time': live_info.get('actualStartTime'),
                        'actual_end_time': live_info.get('actualEndTime'),
                        'scheduled_start_time': live_info.get('scheduledStartTime'),
                        'duration_hours': duration_hours,
                        'view_count': video_data.get('statistics', {}).get('viewCount', 0),
                        'like_count': video_data.get('statistics', {}).get('likeCount', 0),
                        'comment_count': video_data.get('statistics', {}).get('commentCount', 0)
                    })

                next_page = search_res.get('nextPageToken')
                if not next_page:
                    break
        except Exception as e:
            logging.warning(e)

        return data

    engine = create_engine(db_url)
    df_handles = pd.read_sql('SELECT "Handle" FROM hololive.talent_handle', engine)
    channel_handles = df_handles['Handle'].dropna().unique().tolist()

    client = initialize_client()
    data_analytics_all = []

    logging.info(f"Detected {len(channel_handles)} to extract analytics.\n")
    for handle in channel_handles:
        try:
            channel_id = extract_channel_id(client, handle)
            logging.info(f"Processing channel: {handle}")
            data_analytics = extract_analytics(client, channel_id, handle)
            logging.info(f"{handle}: {len(data_analytics)} completed livestreams found.\n")
            data_analytics_all.extend(data_analytics)
        except Exception as e:
            logging.error(f"Failed to process {handle}: {e}")

    return data_analytics_all

@task.virtualenv(requirements=REQUIREMENTS, venv_cache_path=VENV_CACHE_PATH)
def data_preprocessing(data: list):
    """
    Preprocesses a list of dictionaries to save data as CSV.
    Args:
        data: List of dictionaries
    """
    import re
    import html as html_lib
    import logging
    import pandas as pd

    logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

    def _clean_value(value):
        """
        Internal utility function to clean and normalize string values.
        Otherwise it may cause issues when writing to CSV.
        Args:
            value: A single string value in a dictionary to clean.
        """

        if not value:
            return "-"
        value = value.replace("\n", " | ").replace("\r", " ")
        value = value.replace("\u201c", '"').replace("\u201d", '"')
        value = value.replace("\u2018", "'").replace("\u2019", "'")
        value = html_lib.unescape(value)
        value = value.replace('"', "'")
        value = value.replace(",", ";")
        value = re.sub(r"\s{2,}", " ", value)
        return value.strip()

    if not data:
        logging.warning("No live broadcast videos found to save.")
        return []

    for video in data:
        for key, val in video.items():
            if isinstance(val, str):
                video[key] = _clean_value(val)

    df = pd.DataFrame(data)
    logging.info("Preprocessing complete...")
    return df.to_dict(orient="records")

@task.virtualenv(requirements=REQUIREMENTS, venv_cache_path=VENV_CACHE_PATH)
def data_loading(data: list, env_path: str):
    """
    Load CSV into database
    """
    import os
    import logging
    import pandas as pd
    from dotenv import load_dotenv
    from sqlalchemy import create_engine

    logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

    load_dotenv(env_path)
    db_url = os.getenv("DB_URL")

    if not db_url:
        raise ValueError(f"DB_URL not set in {env_path}")

    if not data:
        logging.warning("No data to load.")
        return

    engine = create_engine(db_url)
    df = pd.DataFrame(data)
    df.to_sql("talent_analytics", engine, schema="hololive", if_exists="replace", index=False)

@dag(
    dag_id="HLCT-talent-analytics",
    schedule="0 0 * * *",
    start_date=days_ago(0),
    catchup=False,
    tags=["HLCT"],
)
def main():
    data_analytics_all = data_extraction(ENV_PATH)
    data = data_preprocessing(data_analytics_all)
    data_loading(data, ENV_PATH)

main()
