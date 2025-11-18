import random
import time
from fake_useragent import UserAgent
from loguru import logger
import requests

ua = UserAgent()

def get_headers():
    return {
        "User-Agent": ua.random,
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.google.com/"
    }

def random_delay(min_s=1.5, max_s=4.0):
    delay = round(random.uniform(min_s, max_s), 2)
    logger.info(f"[utils] Sleeping for {delay}s")
    time.sleep(delay)

def safe_request(url, proxies=None, retry=2):
    tries = 0
    while tries <= retry:
        try:
            resp = requests.get(url, headers=get_headers(), proxies=proxies, timeout=12)
            if resp.status_code == 200:
                return resp.text
            logger.warning(f"[utils] Bad status {resp.status_code} for {url}")
            tries += 1
        except Exception as e:
            logger.error(f"[utils] Request error {e} for {url}")
            tries += 1
            random_delay(1, 2)
    return None

def build_indeed_url(keyword, start=0, country_domain="in.indeed.com"):
    q = keyword.replace(" ", "+")
    return f"https://{country_domain}/jobs?q={q}&start={start}"

def clean_text(text):
    if not text:
        return None
    return " ".join(text.split()).strip()
