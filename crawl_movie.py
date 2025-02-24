import asyncio
import json
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode
from pydantic import BaseModel, Field
from typing import List, Optional

# Define the schema for the extracted movie data
class MovieData(BaseModel):
    Title: str
    Artists: str
    Length: str
    Language: str
    Is_dubbed: bool

async def crawl_movie(movie, crawler, crawl_config):
    """
    Crawls the movie URL and extracts information.

    Args:
        movie (dict): A dictionary containing movie data, including the URL.
        crawler (AsyncWebCrawler): An instance of the web crawler.
        crawl_config (CrawlerRunConfig): Configuration for the crawler run.

    Returns:
        dict: The extracted movie data or an error message.
    """
    url = "https://en.wikipedia.org" + movie["url"]
    result = await crawler.arun(url=url, config=crawl_config)

    if result.success:
        try:
            data = json.loads(result.extracted_content)
            # Process the extracted data as needed
            if not data:
                with open('run.log', 'a') as log_file:
                    log_file.write(f"No data extracted for URL: {url}\n")
            else:
                 with open('run.log', 'a') as log_file:
                    log_file.write(f"Successfully crawled and extracted data for URL: {url}\n")
                    log_file.write(f"title: {movie['title']}, data: {json.dumps(data)}\n")
                    return {"title": movie["title"], "data": data}
        except json.JSONDecodeError as e:
            with open('run.log', 'a') as log_file:
                log_file.write(f"JSON Decode Error for URL {url}: {e}\n")
            return {"error": "Failed to decode JSON", "url": movie["url"]}
    else:
        with open('run.log', 'a') as log_file:
            log_file.write(f"Crawl Error for URL {url}: {result.error_message}\n")
        return {"error": result.error_message, "url": movie["url"]}