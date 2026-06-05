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

    def scrape_talent_info_static(driver, url):
        """
        Scrapes static information from each talent page, with built-in sleep time.
        Args:
            driver: WebDriver instance.
            url: The URL of the each single talent page.
        Returns:
            A single dictionary of talent information.
        """

        try:
            driver.get(url)

            # Wait for name to load, ideally would wait for other elements too
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".right_box .bg_box h1"))
            )

            # Extract name from the h1 element
            talent_name = driver.execute_script("""
                const h1 = document.querySelector('.right_box .bg_box h1');
                return h1?.childNodes[0]?.textContent.trim();
            """)

            # Extract information from the dd elements
            talent_info = driver.execute_script("""
                function scrape(labelText) {
                    const normalize = str => str.trim().toLowerCase().replace(/\s+/g, "");
                    const target = normalize(labelText);
                    const dtList = Array.from(document.querySelectorAll("dt"));
                    const dt = dtList.find(el => normalize(el.textContent) === target);
                    return dt?.nextElementSibling?.textContent.trim() || null;
                }
                return {
                    birthday: scrape("Birthday"),
                    height: scrape("Height"),
                    unit: scrape("Unit"),
                    fan_name: scrape("Fan Name"),
                    hashtags: scrape("Hashtags")
                };
            """)

            time.sleep(1.5)  # Best practice
            return {
                "name": talent_name,
                **talent_info,
                "url": url
            }
        except Exception as e:
            logging.warning(f"Failed to extract info from {url}: {e}")
            return None

    driver = setup_driver()
    try:
        all_urls = get_talent_urls(driver, target_url)
        data_static_all = []
        for url in all_urls:
            data_static = scrape_talent_info_static(driver, url)
            if data_static:
                logging.info(f"Extracted {data_static}\n")
                data_static_all.append(data_static)  # This is a list of dictionaries
        logging.info(f"Successfully extracted {len(data_static_all)} talents.")
        return data_static_all
    finally:
        driver.quit()

@task.virtualenv(requirements=REQUIREMENTS, venv_cache_path=VENV_CACHE_PATH)
def data_preprocessing(data: list, env_path: str):
    import os
    import logging
    import html as html_lib
    import re
    import pandas as pd
    from dotenv import load_dotenv
    from sqlalchemy import create_engine

    logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

    load_dotenv(env_path)
    db_url = os.getenv("DB_URL")
    if not db_url:
        raise ValueError(f"DB_URL not set in {env_path}")

    def _clean_value(value):
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

    try:
        if not data:
            logging.warning("No data found to process.")
            return []

        for item in data:
            for key, val in item.items():
                if isinstance(val, str):
                    item[key] = _clean_value(val)

        engine = create_engine(db_url)
        df_keys = pd.read_sql('SELECT * FROM hololive.talent_handle', engine)

        df = pd.DataFrame(data)
        df = df.merge(df_keys, on='name', how='left')
        df = df[['Handle'] + [col for col in df.columns if col != 'Handle']]

        logging.info("Preprocessing complete...")
        return df.to_dict(orient="records")

    except Exception as e:
        logging.error(f"Preprocessing failed: {e}")
        return []

@task.virtualenv(requirements=REQUIREMENTS, venv_cache_path=VENV_CACHE_PATH)
def data_loading(data: list, env_path: str):
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
    df.to_sql("talent_info", engine, schema="hololive", if_exists="replace", index=False)

@dag(
    dag_id="HLCT-talent-info",
    schedule="@weekly",
    start_date=days_ago(0),
    catchup=False,
    tags=["HLCT"],
)
def main():
    data_static_all = data_extraction(TARGET_URL, DRIVER_PATH)
    data = data_preprocessing(data_static_all, ENV_PATH)
    data_loading(data, ENV_PATH)

main()
