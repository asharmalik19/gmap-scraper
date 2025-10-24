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


BUSINESS_TITLE_SELECTOR = "h1.DUwDvf.lfPIob"
FEED_SELECTOR = "div[role=feed]"


async def search(page, search_query):
    """Returns True if search results are found, False if redirected to a single business.
    Also checks for Google Captcha and raises an exception if detected.
    """
    await page.goto("https://www.google.com/maps")
    await page.fill("#searchboxinput", search_query)
    await page.keyboard.press("Enter")
    await page.wait_for_load_state("load")

    # Check for captcha after search
    # if await page.locator("div#captcha-form").count() > 0:
    #     logging.error("Google Captcha detected after search - automation blocked")
    #     raise Exception(
    #         "Google Captcha detected - please resolve captcha or try again later"
    #     )

    try:
        if await page.wait_for_selector(BUSINESS_TITLE_SELECTOR, timeout=30000):
            logging.info(
                f"Search '{search_query}' redirected to single business - skipping keyword"
            )
            print(
                f"Search '{search_query}' redirected to single business - skipping keyword"
            )
            await page.close()
            return None
    except TimeoutError:
        print(f"Search Results found for {search_query}")

    if "Google Maps can't find" in await page.content():
        logging.info(f"Search '{search_query}' returned no results - skipping keyword")
        print(f"Search '{search_query}' returned no results - skipping keyword")
        await page.close()
        return None

    await page.wait_for_selector(
        FEED_SELECTOR, timeout=30000
    )
    await scroll(page)
    business_links = await get_links(page)
    await page.close()
    return business_links


async def get_links(page):
    results = await page.locator("a.hfpxzc").all()
    result_links = [await result.get_attribute("href") for result in results]
    return result_links


async def scroll(page):
    sidebar = page.locator(FEED_SELECTOR)
    while True:
        await sidebar.press("PageDown")
        await page.wait_for_timeout(500)
        await sidebar.press("PageDown")
        await page.wait_for_timeout(500)
        await asyncio.sleep(5)
        if "reached the end of the list" in await page.content():
            break
    return


@stamina.retry(on=TimeoutError, attempts=2)
def get_business_page_source(page, link):
    page.goto(link, timeout=30000)
    business_title = page.locator(BUSINESS_TITLE_SELECTOR)
    business_title.wait_for(state="attached", timeout=10000)
    return page.content()


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


def create_search_queries(locations, keywords):
    search_queries = []
    for _, row in locations.iterrows():
        location = {
            "City": row["City"],
            "State": row["State"],
            "Country": row["Country"],
        }
        for keyword in keywords:
            location_str = ", ".join(i for i in location.values())
            search_query = keyword + " in " + location_str
            search_queries.append(search_query)
    return search_queries


async def search_queries_in_parallel(search_queries, browser):
    pages_and_queries = []
    for query in search_queries:
        page = await browser.new_page()
        await asyncio.sleep(random.uniform(2, 4))
        pages_and_queries.append((page, query))
    search_tasks = [search(page, query) for page, query in pages_and_queries]
    results = await asyncio.gather(*search_tasks)
    return results


async def create_workers(num_workers=4):
    async with AsyncCamoufox(headless=True) as browser:
        for _ in range(num_workers):
            await browser.new_page()
            

async def main():
    logging.basicConfig(filename="g_map_scraper.log", filemode="w", level=logging.INFO)
    with open("keywords.txt", "r") as file:
        keyword_list = [line.strip() for line in file if line.strip()]
    locations = pd.read_csv("locations.csv")
    search_queries = create_search_queries(locations, keyword_list)

    async with AsyncCamoufox(headless=True) as browser:
        results = await search_queries_in_parallel(search_queries, browser)

    print(results)


if __name__ == "__main__":
    asyncio.run(main())
