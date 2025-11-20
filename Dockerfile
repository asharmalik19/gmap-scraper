# 1. Use the official Playwright image (It has all the OS-level Linux dependencies pre-installed!)
# This saves you from figuring out which 'apt-get' packages you need.
FROM mcr.microsoft.com/playwright/python:v1.48.0-jammy

# 2. Set up the working directory
WORKDIR /app

# 3. Install 'uv' (The package manager you use)
RUN pip install uv

# 4. Copy your files
COPY . .

# 5. Install your Python dependencies
RUN uv sync

# 6. Install Patchright's specific browsers
# (We do this here so they are baked into the image)
RUN uv run patchright install chrome

# 7. The command that runs when you start the container
CMD ["uv", "run", "gmap_scraper.py"]