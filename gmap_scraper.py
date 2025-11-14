import re
import asyncio
import logging
from datetime import datetime
import random

from bs4 import BeautifulSoup
import stamina
import pandas as pd
from patchright.async_api import TimeoutError
from patchright.async_api import async_playwright


logging.basicConfig(
    filename="gmap_scraper.log",
    filemode="w",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


BUSINESS_TITLE_SELECTOR = "h1.DUwDvf.lfPIob"
FEED_SELECTOR = "div[role=feed]"
NUMBER_OF_PAGES = 4


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


def create_search_queries() -> asyncio.Queue:
    with open("keywords.txt", "r") as file:
        keyword_list = [line.strip() for line in file if line.strip()]
    locations = pd.read_csv("locations.csv")
    search_queries = asyncio.Queue()
    for _, row in locations.iterrows():
        location = {
            "City": row["City"],
            "State": row["State"],
            "Country": row["Country"],
        }
        for keyword in keyword_list:
            location_str = ", ".join(i for i in location.values())
            search_query = keyword + " in " + location_str
            search_queries.put_nowait(search_query)
    return search_queries


# @stamina.retry(on=Exception, attempts=2)
async def get_business_page_source(page, link) -> str | None:
    try:
        await page.goto(link, timeout=30000, wait_until="domcontentloaded")
    except Exception as e:
        logging.error(f"Error while navigating to the page link: {link}: {e}")
        return None
    await page.locator(BUSINESS_TITLE_SELECTOR).wait_for(timeout=30000)
    return await page.content()


async def search(page, search_query) -> list[str] | None:
    await page.goto("https://www.google.com/maps")
    await page.fill("#searchboxinput", search_query)
    await page.keyboard.press("Enter")
    await page.wait_for_load_state("load")
    try:
        await page.locator(FEED_SELECTOR).wait_for(timeout=30000)
        logging.info(f"Search Results found for {search_query}")
    except TimeoutError:
        logging.warning(
            f"Search '{search_query}' returned no results - skipping query: Invalid case"
        )
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
        if links:
            for link in links:
                await business_links_queue.put(link)
        search_queries_queue.task_done()


async def page_source_worker(page, business_links_queue, page_source_queue):
    while True:
        link = await business_links_queue.get()
        if link is None:
            break
        page_source = await get_business_page_source(page, link)
        if page_source:
            logging.info(f"Successfully fetched page source for {link}")
            await page_source_queue.put(page_source)
        business_links_queue.task_done()


async def map_pages_to_worker(pages, worker, input_queue, output_queue):
    """The job of this function is to start the workers. When the workers
    job is done and the input queue is empty, close the workers."""
    task_list = []
    for page in pages:
        task = asyncio.create_task(worker(page, input_queue, output_queue))
        task_list.append(task)
    await input_queue.join()
    for _ in range(NUMBER_OF_PAGES):
        await input_queue.put(None)
    await asyncio.gather(*task_list)


async def main():
    logging.basicConfig(filename="g_map_scraper.log", filemode="w", level=logging.INFO)
    search_queries_queue = create_search_queries()
    business_links_queue = asyncio.Queue()
    page_source_queue = asyncio.Queue()
    logging.info(f"Processing search queries: {search_queries_queue.qsize()}")
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            channel="chrome",
            headless=False,
        )
        pages = []
        for _ in range(NUMBER_OF_PAGES):
            page = await browser.new_page()
            pages.append(page)
            await asyncio.sleep(2)

        await map_pages_to_worker(
            pages, search_worker, search_queries_queue, business_links_queue
        )
        logging.info(f"Num of business links: {business_links_queue.qsize()}")
        await map_pages_to_worker(
            pages, page_source_worker, business_links_queue, page_source_queue
        )
        logging.info(f"Num of page sources: {page_source_queue.qsize()}")

    parsed_businesses = []
    for _ in range(page_source_queue.qsize()):
        page_source = await page_source_queue.get()
        business_info = scrape_business_details(page_source)
        parsed_businesses.append(business_info)
    df = pd.DataFrame(parsed_businesses)
    df.to_csv("g_map_scraper_output.csv", index=False)
    logging.info(f"Output saved to g_map_scraper_output.csv")


if __name__ == "__main__":
    start_time = datetime.now()
    asyncio.run(main())
    end_time = datetime.now()
    logging.info(f"Total time taken: {end_time - start_time}")
