# Google Maps Async Scraper

A high-performance, asynchronous web scraper for Google Maps business listings built with Python and Playwright.

## Features

*   **Concurrent Scraping:** Uses `asyncio` to process multiple search queries and pages simultaneously.
*   **Detailed Extraction:** Scrapes business name, address, phone, website, operating hours, and more.
*   **Resilient:** Implements retry logic and robust error handling.
*   **Anti-Detection:** Uses `patchright` to avoid bot detection and blocking.

## Installation

1.  **Install Dependencies with uv:**
    ```bash
    uv sync
    ```

2.  **Install Browsers:**
    ```bash
    uv run patchright install chromium
    ```

## Configuration

Create two files in the project directory to define your search parameters:

1.  **`keywords.txt`**: A list of business types to search for (one per line).
    ```text
    plumbers
    coffee shops
    ```

2.  **`locations.csv`**: A CSV file defining the target areas.
    ```csv
    City,State,Country
    Baltimore,MD,USA
    Columbia,MD,USA
    ```

## Usage

Run the script:
```bash
uv run gmap_scraper.py
```

The results will be saved to **`gmap_scraper_output.csv`**.
