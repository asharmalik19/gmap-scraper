# 1. Use the official Playwright image (It has all the OS-level Linux dependencies pre-installed!)
# This saves you from figuring out which 'apt-get' packages you need.
FROM mcr.microsoft.com/playwright:v1.56.1-noble

# 2. Set up the working directory
WORKDIR /app

# 3. Install 'uv' package manager
RUN apt update -y && apt install curl -y
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:$PATH"

# 4. Copy your files
COPY . .

# 5. Install your Python dependencies
RUN uv sync

# 6. Install patchright's specific browser
RUN uv run patchright install chrome

# 7. The command that runs when you start the container
CMD ["uv", "run", "gmap_scraper.py"]