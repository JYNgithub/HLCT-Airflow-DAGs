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
TARGET_URL = config["TARGET_URL"]
DRIVER_PATH = config["DRIVER_PATH"]

with open(REQUIREMENTS_PATH) as f:
    REQUIREMENTS = f.read().splitlines()

@task.virtualenv(requirements=REQUIREMENTS, venv_cache_path=VENV_CACHE_PATH)
def data_extraction(target_url: str, driver_path: str):
    """
    Setup driver, get talent urls, then scrape static info for each talent.
    Returns a list of dictionaries.
    """
    import logging
    import time
    import os
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("googletrans").setLevel(logging.WARNING)

    def setup_driver():
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--single-process")
        options.binary_location = os.path.join(driver_path, "chrome-linux64/chrome")
        
        service = Service(os.path.join(driver_path, "chromedriver-linux64/chromedriver"))
        return webdriver.Chrome(service=service, options=options)

    def get_talent_urls(driver, url):
        """
        Loops through the main talents page and returns a list of talent URLs.
        Args:
            driver: WebDriver instance.
            url: The base URL to talents page.
        Returns:
            A list of unique talent URLs found on the page.
        """

        logging.info(f"Loading base page: {url}")
        driver.get(url)
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'a[href*="/talents/"]'))
            )
            elements = driver.find_elements(By.CSS_SELECTOR, 'a[href*="/talents/"]')
            urls = set()

            for el in elements:
                href = el.get_attribute('href')
                if href and href.startswith(url):
                    urls.add(href)

            logging.info(f"Found {len(urls)} talent urls")
            return list(urls)

        except Exception as e:
            logging.error(f"Error fetching talent urls: {e}")
            return []

    def scrape_talent_info_dynamic(driver, url):
        """
        Scrapes dynamic information from each talent page, including name and schedules.
        Args:
            driver: WebDriver instance.
            url: The URL of each single talent page.
        Returns:
            A dictionary of talent information with schedule data.
        """

        try:
            driver.get(url)

            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".right_box .bg_box h1"))
            )

            talent_name = driver.execute_script("""
                const h1 = document.querySelector('.right_box .bg_box h1');
                return h1?.childNodes[0]?.textContent.trim();
            """)

            main_image = driver.execute_script("""
                const img = document.querySelector('#talent_figure figure img');
                return img?.src || null;
            """)

            schedule_data = driver.execute_script("""
                const slides = document.querySelectorAll('.talent_program .swiper-slide');
                if (!slides || slides.length === 0) return {};

                const result = {};
                Array.from(slides).forEach((slide, index) => {
                    const i = index + 1;
                    result["youtube_link" + i] = slide.querySelector("a")?.href || null;
                    result["datetime" + i] = slide.querySelector(".cat")?.textContent.trim() || null;
                    result["image" + i] = slide.querySelector("figure img")?.src || null;
                    result["description" + i] = slide.querySelector(".txt_box .txt")?.textContent.trim() || null;
                });
                return result;
            """)

            time.sleep(1.5)
            return {
                "name": talent_name,
                "default_image": main_image,
                **schedule_data
            }

        except Exception as e:
            logging.warning(f"Failed to extract info from {url}: {e}")
            return None

    driver = setup_driver()
    try:
        all_urls = get_talent_urls(driver, target_url)
        data_dynamic_all = []
        for url in all_urls:
            data_dynamic = scrape_talent_info_dynamic(driver, url)
            if data_dynamic:
                logging.info(f"Extracted {data_dynamic}\n")
                data_dynamic_all.append(data_dynamic)
        logging.info(f"Successfully extracted {len(data_dynamic_all)} talents.")
        return data_dynamic_all
    finally:
        driver.quit()

@task.virtualenv(requirements=REQUIREMENTS, venv_cache_path=VENV_CACHE_PATH)
def data_preprocessing(data: list, env_path: str):
    """
    Preprocesses a list of dictionaries to save data as CSV with async translation.
    Note: Using googletrans==4.0.2 prevents dependency issues, but will need to handle async.
    Args:`
        data: List of dictionaries
    """
    import os
    import logging
    import re
    import html as html_lib
    import asyncio
    import pandas as pd
    from dotenv import load_dotenv
    from sqlalchemy import create_engine
    from googletrans import Translator

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
        value = value.replace(",", "")
        value = re.sub(r"\s{2,}", " ", value)
        return value.strip()

    def _sort_columns(key):
        """
        Internal utility function to sort column names in a specific order.
        Used in save_to_csv_dynamic at line 'header = sorted(all_keys, key=_sort_columns)'.
        """

        col_order = ["name", "default_image", "datetime", "description", "image", "youtube_link"]
        for i, prefix in enumerate(col_order):
            if key.startswith(prefix):
                m = re.match(rf"{prefix}(\d*)$", key)
                suffix = int(m.group(1)) if m and m.group(1).isdigit() else 0
                return (i, suffix)
        return (len(col_order), key)

    async def translate_text(translator, text):
        if pd.isna(text):
            return text
        try:
            result = await translator.translate(str(text), src='ja', dest='en')
            return result.text
        except:
            return text

    async def _preprocess(data):
        logging.info("Preprocessing data...")
        try:
            if not data:
                logging.error("No data to convert.")
                return []

            all_keys = set()
            for row in data:
                all_keys.update(row.keys())
            header = sorted(all_keys, key=_sort_columns)

            cleaned_data = []
            for row in data:
                cleaned_row = {k: _clean_value(v) for k, v in row.items()}
                cleaned_data.append(cleaned_row)

            df = pd.DataFrame(cleaned_data, columns=header)

            translator = Translator()
            for col in df.columns:
                if col.lower().startswith("description"):
                    for i, val in df[col].items():
                        df.at[i, col] = await translate_text(translator, val)

            load_dotenv(env_path)
            db_url = os.getenv("DB_URL")
            engine = create_engine(db_url)

            # Supplement key column
            df_keys = pd.read_sql('SELECT * FROM hololive.talent_handle', engine)
            df = df.merge(df_keys, on='name', how='left')
            df = df[['Handle'] + [col for col in df.columns if col != 'Handle']]

            # Join data with talent_info
            df_info = pd.read_sql('SELECT * FROM hololive.talent_info', engine)
            df = df.merge(df_info, on=["Handle", "name"], how="inner")

            # Sorting rows for sidebar appearance
            df_with_image = df[df['image1'].notna()]
            df_no_image = df[df['image1'].isna()]
            df = pd.concat([df_with_image, df_no_image])
            mask_bracket = df['name'].str.contains(r'\[.*\]', na=False)
            df = pd.concat([df[~mask_bracket], df[mask_bracket]])
            df = df.reset_index(drop=True)
            df.index += 1

            logging.info("Preprocessing complete...")
            return df.to_dict(orient="records")

        except Exception as e:
            logging.error(f"Preprocessing failed: {e}")
            return []

    return asyncio.run(_preprocess(data))

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
    df.to_sql("talent_schedule", engine, schema="hololive", if_exists="replace", index=False)

@dag(
    dag_id="HLCT-talent-schedule",
    schedule="@hourly",
    start_date=days_ago(0),
    catchup=False,
    tags=["HLCT"],
)
def main():
    data_dynamic_all = data_extraction(TARGET_URL, DRIVER_PATH)
    data = data_preprocessing(data_dynamic_all, ENV_PATH)
    data_loading(data, ENV_PATH)

main()
