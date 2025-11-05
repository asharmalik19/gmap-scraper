import re
import asyncio
import logging
import time
import random

from bs4 import BeautifulSoup
import stamina
import pandas as pd
from camoufox.async_api import AsyncCamoufox
from playwright.async_api import TimeoutError


logging.basicConfig(
    filename="gmap_scraper.log",
    filemode="w",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


BUSINESS_TITLE_SELECTOR = "h1.DUwDvf.lfPIob"
FEED_SELECTOR = "div[role=feed]"


async def get_links(page):
    results = await page.locator("a.hfpxzc").all()
    result_links = [await result.get_attribute("href") for result in results]
    return result_links


async def scroll(page):
    """Scrolls through the page until the end. Google maps scroll can sometimes
    get stuck"""
    sidebar = page.locator(FEED_SELECTOR)
    while True:
        await sidebar.press("PageDown")
        await page.wait_for_timeout(500)
        await sidebar.press("PageDown")
        await page.wait_for_timeout(500)
        await asyncio.sleep(random.uniform(5, 7))
        if "reached the end of the list" in await page.content():
            break
    return


# @stamina.retry(on=TimeoutError, attempts=2)
async def get_business_page_source(page, link):
    logging.info(f"Fetching page source for link: {link}")
    try:
        await page.goto(link, timeout=30000)
    except Exception as e:
        logging.error(f"Error {e} while fetching page source for link: {link}")
        return None
    business_title = page.locator(BUSINESS_TITLE_SELECTOR)
    await business_title.wait_for(state="attached", timeout=10000)
    return await page.content()


def scrape_business_details(page_source):
    soup = BeautifulSoup(page_source, "html.parser")
    business_title = soup.select_one(BUSINESS_TITLE_SELECTOR).text.strip()
    business_title = business_title.replace('"', "").replace("'", "")
    business_type_elem = soup.select_one("button.DkEaL")
    business_type = business_type_elem.text.strip() if business_type_elem else ""
    address_elem = soup.select_one('button[data-item-id="address"] div.rogA2c')
    address = address_elem.text.strip() if address_elem else ""
    website_link_elem = soup.select_one('a[data-tooltip="Open website"]')
    website_link = website_link_elem.get("href") if website_link_elem else ""

    domain_pattern = re.compile(
        r"\b(?:https?://)?(?:www\.)?([a-zA-Z0-9.-]+\.[a-zA-Z]{2,})\b"
    )
    closed_business_elem = soup.select_one("span.fCEvvc")
    business_status = closed_business_elem.text.strip() if closed_business_elem else ""
    contact_details_div = soup.select_one(
        f'div[aria-label="Information for {business_title}"]'
    )
    contact_details_text = contact_details_div.text if contact_details_div else ""
    domain_name = domain_pattern.search(contact_details_text)
    domain_name = domain_name.group() if domain_name else ""
    number = soup.select_one('button[aria-label^="Phone"]')
    number = number.text.strip() if number else ""

    business_timings = get_business_timings(page_source)
    business_details = {
        "Company_Name": business_title,
        "Number": number,
        "Full_Address": address,
        "Gmaps_Domains": domain_name,
        "Website": website_link,
        "Business_Type_Gmaps": business_type,
        "Business_Status": business_status,
    }
    business_details.update(business_timings)
    return business_details


def remove_duplicates(data_df):
    columns_to_check = ["Company_Name", "Number", "Full_Address"]
    data_df_without_duplicates = data_df[
        ~data_df.duplicated(subset=columns_to_check, keep="first")
    ]
    return data_df_without_duplicates


def get_business_timings(page_source):
    VALID_KEYS = [
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    ]
    soup = BeautifulSoup(page_source, "html.parser")
    business_hours = {}
    table = soup.find("table", class_="eK4R0e")
    if table:
        for row in table.find_all("tr"):
            cells = row.find_all("td")[:2]
            # sometimes there are name variations stored as multiple divs in a single cell td
            day = cells[0].find("div").get_text(strip=True)
            timings_ul = cells[1].find("ul")
            timings_listed = timings_ul.find_all("li")
            timings_text = [timing.get_text(strip=True) for timing in timings_listed]
            timings_raw = ",".join(timings_text)
            timings_filtered = re.sub(r"\s+", " ", timings_raw, flags=re.UNICODE)
            business_hours.update({day: timings_filtered})

    for key in business_hours.keys():
        if key not in VALID_KEYS:
            logging.error(f"Data Quality Error: Invalid day found - {key}")
            raise ValueError(f"Data Quality Error: Invalid day found - {key}")
    return business_hours


def create_search_queries(locations, keywords) -> asyncio.Queue:
    search_queries = asyncio.Queue()
    for _, row in locations.iterrows():
        location = {
            "City": row["City"],
            "State": row["State"],
            "Country": row["Country"],
        }
        for keyword in keywords:
            location_str = ", ".join(i for i in location.values())
            search_query = keyword + " in " + location_str
            search_queries.put_nowait(search_query)
    return search_queries


# NOTE: I am assuming that playwright would wait for the feed selector in valid case 
# and throw timeout in invalid cases
async def search(page, search_query):
    await page.goto("https://www.google.com/maps")
    await page.fill("#searchboxinput", search_query)
    await page.keyboard.press("Enter")
    await page.wait_for_load_state("load")
    try:
        await page.locator(FEED_SELECTOR).wait_for(timeout=30000)
        logging.info(f"Search Results found for {search_query}")
        print(f"Search Results found for {search_query}")
    except TimeoutError:
        logging.warning(f"Search '{search_query}' returned no results - skipping query: Invalid case")
        print(f"Search '{search_query}' returned no results - skipping query: Invalid case")
        return None
    await scroll(page)
    business_links = await get_links(page)
    return business_links


async def search_worker(page, search_queries_queue, business_links_queue):
    """This function is responsible for calling the seach function for search queries from the queue
    using the provided page object. Multiple instances of this function runs concurrently with page objects
    """
    while True:
        search_query = await search_queries_queue.get()
        if search_query is None:
            break
        links = await search(page, search_query)
        for link in links:
            await business_links_queue.put(link)
        search_queries_queue.task_done()

    
async def page_source_worker(page, business_links_queue, page_source_queue):
    while True:
        link = await business_links_queue.get()
        if link is None:
            break
        page_source = await get_business_page_source(page, link)
        logging.info(f"Successfully fetched page source for {link}")
        await page_source_queue.put(page_source)
        business_links_queue.task_done()


async def main():
    NUMBER_OF_PAGES = 4
    logging.basicConfig(filename="g_map_scraper.log", filemode="w", level=logging.INFO)
    with open("keywords.txt", "r") as file:
        keyword_list = [line.strip() for line in file if line.strip()]
    locations = pd.read_csv("locations.csv")
    search_queries_queue = create_search_queries(locations, keyword_list)
    business_links_queue = asyncio.Queue()
    page_source_queue = asyncio.Queue()
    logging.info(f"Processing search queries: {search_queries_queue.qsize()}")
    async with AsyncCamoufox(headless=False) as browser:
        pages = []
        for _ in range(NUMBER_OF_PAGES):
            page = await browser.new_page()
            pages.append(page)
            await asyncio.sleep(2)

        search_tasks = []
        for page in pages:
            search_task = asyncio.create_task(
                search_worker(page, search_queries_queue, business_links_queue)
            )
            search_tasks.append(search_task)
        await search_queries_queue.join()

        # poison pill for each page object to exit search tasks properly
        for _ in range(NUMBER_OF_PAGES):
            await search_queries_queue.put(None)
        await asyncio.gather(*search_tasks)    
        logging.info(f"Total number of business links: {business_links_queue.qsize()}")   

        get_page_source_tasks = []
        for page in pages:
            page_source_task = asyncio.create_task(
                page_source_worker(page, business_links_queue, page_source_queue)
            )
            get_page_source_tasks.append(page_source_task)
            
        await business_links_queue.join()
        for _ in range(NUMBER_OF_PAGES):
            await page_source_queue.put(None)
        await asyncio.gather(*get_page_source_tasks)

        # TEMP
        for _ in range(1):
            page_source = await page_source_queue.get()
            print(page_source)
        print(page_source_queue.qsize())

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
