import random
import requests
from loguru import logger

def fetch_proxies():
    url = "https://www.proxy-list.download/api/v1/get?type=http"
    try:
        r = requests.get(url, timeout=8)
        text = r.text
        proxy_list = [p.strip() for p in text.splitlines() if p.strip()]
        logger.info(f"[proxies] fetched {len(proxy_list)} proxies")
        return proxy_list
    except Exception as e:
        logger.warning(f"[proxies] could not fetch proxies: {e}")
        return []

def get_random_proxy(proxies):
    if not proxies:
        return None
    p = random.choice(proxies)
    return {"http": f"http://{p}", "https": f"http://{p}"}

def test_proxy(proxy):
    try:
        r = requests.get("https://httpbin.org/ip", proxies=proxy, timeout=6)
        return r.status_code == 200
    except:
        return False

def get_working_proxy(test_limit=15):
    proxies = fetch_proxies()
    for p in proxies[:test_limit]:
        pr = {"http": f"http://{p}", "https": f"http://{p}"}
        if test_proxy(pr):
            logger.info(f"[proxies] working proxy found: {p}")
            return pr
    logger.warning("[proxies] no working proxy found, using direct connection")
    return None
