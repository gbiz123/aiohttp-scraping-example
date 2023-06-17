import requests
import aiohttp
import asyncio
import json
import csv
from collections.abc import MutableMapping
from contextlib import contextmanager
import random

def flatten(dictionary, parent_key='', separator='_'):
   items = []
   for key, value in dictionary.items():
       new_key = parent_key + separator + key if parent_key else key
       if isinstance(value, MutableMapping):
           items.extend(flatten(value, new_key, separator).items())
       else:
           items.append((new_key, value))
   return dict(items)                              

base_url = "https://api-igamedc.igamemedia.com/api/MssWeb/fixtures/"

with open("./datacenter_proxies.txt") as f:
    proxy_list = [line.strip() for line in f.readlines()]

proxies_in_use = []

@contextmanager
def allocate_proxy():
    """Get a free proxy and hold it as in use"""
    available_proxies = [p for p in proxy_list if p not in proxies_in_use]
    if available_proxies:
        proxy = random.choice(available_proxies)
    else:
        proxy = random.choice(proxy_list)
    try:
        proxies_in_use.append(proxy)
        yield proxy
    finally:
        proxies_in_use.remove(proxy)


async def scrape_page(url, session):
    """Scrape the JSON from a page"""
    with allocate_proxy() as proxy:
        async with session.get(url, proxy=proxy, timeout=5000) as resp:
            print("Success!")
            return flatten(await resp.json())


async def bound_scrape(url, session, semaphore):
    """Scrape the page with binding by semaphore"""
    async with semaphore:
        return await scrape_page(url, session)


async def main():
    sem = asyncio.Semaphore(25000)
    empty_page_count = 0
    page_range = (1000, 10244)
    tasks = []
    async with aiohttp.ClientSession() as session:
        for page in range(*page_range):
           url = base_url + str(page)
           tasks.append(asyncio.create_task(bound_scrape(url, session, sem)))
           await asyncio.sleep(0.2)
           print("Request sent.")

        results = await asyncio.gather(*tasks, return_exceptions=True)

    return results

raw_data = asyncio.run(main())
data = [d for d in raw_data if not isinstance(d, Exception)]

with open("data.json", "w") as f:
    json.dump(data, f)

with open("data.csv", "w", newline="") as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=list(data[0].keys()))
    writer.writeheader()
    for row in data:
        writer.writerow(row)
