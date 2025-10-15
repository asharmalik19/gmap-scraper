from playwright.sync_api import sync_playwright
import re
import pandas as pd
from datetime import datetime
import logging
from bs4 import BeautifulSoup
from playwright.sync_api import TimeoutError
import stamina
import time
import random

from playwright.async_api import async_playwright


def search(page, search_query):
    """Returns True if search results are found, False if redirected to a single business.
    Also checks for Google Captcha and raises an exception if detected.
    """
    page.goto("https://www.google.com/maps")
    page.fill("#searchboxinput", search_query)
    page.keyboard.press("Enter")
    page.wait_for_load_state("load")

    # Check for captcha after search
    if page.locator("div#captcha-form").count() > 0:
        logging.error("Google Captcha detected after search - automation blocked")
        raise Exception(
            "Google Captcha detected - please resolve captcha or try again later"
        )
     
    try:
        if page.wait_for_selector("h1.DUwDvf.lfPIob", timeout=5000):
            logging.info(
                f"Search '{search_query}' redirected to single business - skipping keyword"
            )
            print(
                f"Search '{search_query}' redirected to single business - skipping keyword"
            )
            return False
    except TimeoutError:
        print(f"Search Results found for {search_query}")

    if "Google Maps can't find" in page.content():
        logging.info(f"Search '{search_query}' returned no results - skipping keyword")
        print(f"Search '{search_query}' returned no results - skipping keyword")
        return False

    page.wait_for_selector(
        f"div[aria-label='Results for {search_query}']", timeout=30000
    )
    return True


def get_links(page):
    results = page.locator("a.hfpxzc").all()
    result_links = [result.get_attribute("href") for result in results]
    return result_links


def scroll(page, search_query):
    sidebar = page.locator(f"div[aria-label='Results for {search_query}']")
    # implement the unstuck mechanism
    previous_businesses = page.locator("a.hfpxzc").count()
    last_check_time = time.time()
    check_interval = 3

    while True:
        sidebar.press("PageDown")
        page.wait_for_timeout(500)
        sidebar.press("PageDown")
        page.wait_for_timeout(500)
        time.sleep(random.uniform(2, 3))  # the script seems to get stuck due to scrolling too fast

        current_time = time.time()
        if current_time - last_check_time >= check_interval:
            current_businesses = page.locator("a.hfpxzc").count()
            if current_businesses == previous_businesses:
                sidebar.locator("a.hfpxzc").last.click()
                logging.info("Unstuck mechanism triggered - clicking last result")
                page.wait_for_timeout(2000)
            previous_businesses = current_businesses
            last_check_time = current_time

        if "reached the end of the list" in page.content():
            break
    return


@stamina.retry(on=TimeoutError, attempts=2)
def get_business_page_source(page, link):
    page.goto(link, timeout=30000)
    business_title = page.locator("h1.DUwDvf.lfPIob")
    business_title.wait_for(state="attached", timeout=10000)
    return page.content()


def scrape_business_details(page_source):
    soup = BeautifulSoup(page_source, "html.parser")
    business_title = soup.select_one("h1.DUwDvf.lfPIob").text.strip()
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


async def search_keywords_in_location(keywords, location, page):
    business_links_for_location = []
    for keyword in keywords:
        location = ", ".join(i for i in location.values())
        search_query = keyword + " in " + location
        search_results = search(page=page, search_query=search_query)
        if not search_results:
            continue
        scroll(page=page, search_query=search_query)
        business_links = get_links(page=page)
        business_links_for_location.extend(business_links)
    return business_links_for_location


async def main():
    logging.basicConfig(filename="g_map_scraper.log", filemode="w", level=logging.INFO)
    keyword_list = pd.read_csv("keywords.txt")
    locations = pd.read_csv("locations.csv")

    with async_playwright() as playwright:
        chromium = playwright.chromium
        browser = await chromium.launch(headless=False)
        browser = playwright.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()


        # for keyword in keyword_list:
        #         search_query = make_search_query(keyword=keyword, location=LOCATION)
        #         # handles the case when the search is redirected to a business page
        #         if not search(page=page, search_query=search_query):
        #             continue
        #         scroll(page=page, search_query=search_query)
        #         result_links = get_links(page=page)



if __name__ == "__main__":
    #     for city in cities:
    #         start_time = datetime.now()
    #         LOCATION = {"City": city, "State": "MD", "Country": "USA"}
    #         data_df = pd.DataFrame(
    #             columns=[
    #                 "Company_Name",
    #                 "Number",
    #                 "Full_Address",
    #                 "Gmaps_Domains",
    #                 "Website",
    #                 "Business_Type_Gmaps",
    #                 "Business_Status",
    #                 "Monday",
    #                 "Tuesday",
    #                 "Wednesday",
    #                 "Thursday",
    #                 "Friday",
    #                 "Saturday",
    #                 "Sunday",
    #                 "Gmaps_Links",
    #                 "State",
    #                 "Searched_Keywords",
    #             ]
    #         )

    #         for keyword in keyword_list:
    #             search_query = make_search_query(keyword=keyword, location=LOCATION)
    #             # handles the case when the search is redirected to a business page
    #             if not search(page=page, search_query=search_query):
    #                 continue
    #             scroll(page=page, search_query=search_query)
    #             result_links = get_links(page=page)

    #             for link in result_links:
    #                 try:
    #                     page_source = get_business_page_source(page=page, link=link)
    #                 except TimeoutError:
    #                     logging.error(
    #                         f"Timeout Error: Unable to load page for link {link}"
    #                     )
    #                     print(f"Timeout Error: Unable to load page for link {link}")
    #                     continue
    #                 business_details = scrape_business_details(page_source=page_source)
    #                 business_details.update(
    #                     {
    #                         "Gmaps_Links": link,
    #                         "State": LOCATION["State"],
    #                         "Searched_Keywords": keyword,
    #                     }
    #                 )
    #                 data_df.loc[len(data_df)] = business_details
    #                 print(f"Scraped: {business_details}")
    #                 print("-" * 50)

    #         data_df_without_duplicates = remove_duplicates(data_df=data_df)
    #         data_df_without_duplicates.to_excel(f"{city}.xlsx", index=False)
    #         logging.warning(
    #             f"Scraping completed for {city}. Total execution time: {datetime.now() - start_time}"
    #         )
    #         print(f"Completed scraping {city}")

    #     browser.close()
    # print(f"Total execution time: {datetime.now() - start_time}")
