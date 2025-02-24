import asyncio
import json
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
from crawl4ai.extraction_strategy import LLMExtractionStrategy
from crawl_movie import crawl_movie, MovieData
import datetime

async def main():
    # Read and parse data.json
    with open("data.json", "r") as f:
        movies = json.load(f)

    # 1. Define the LLM extraction strategy
    llm_strategy = LLMExtractionStrategy(
        provider="gemini/gemini-1.5-flash",
        schema=MovieData.model_json_schema(),
        extraction_type="schema",
        instruction="Extract all Titles, Artists (If Singers are available instead extract Singers,else extract performers if available) and Length from the tables in the page. Add the Language and if_dubbed columns by analyzing the text above the tables and add to the output. Dont hallucinate. If you are unsure, leave it blank.Ignore non music related tables.Ignore awards and nominations tables.",
        chunk_token_threshold=90000,
        overlap_rate=0.0,
        apply_chunking=True,
        input_format="html",
        extra_args={"temperature": 0.0, "max_tokens": 2000000}
    )

    # 2. Build the crawler config
    crawl_config = CrawlerRunConfig(
        extraction_strategy=llm_strategy,
        cache_mode=CacheMode.BYPASS
    )

    # 3. Create a browser config if needed
    browser_cfg = BrowserConfig(headless=True)

    all_results = []  # Initialize an empty list to store all results

    async with AsyncWebCrawler(config=browser_cfg) as crawler:
        # Crawl each movie sequentially
        successful_crawl_count = 0  # Initialize counter
        for movie in movies:
            result = await crawl_movie(movie, crawler, crawl_config)
            await asyncio.sleep(11)  # Wait for 11 seconds between movie crawls
            all_results.append(result)
            successful_crawl_count += 1 # Increment counter on successful crawl

        # Write the results to a JSON file
        with open("movie_results.json", "w") as f:
            json.dump(all_results, f, indent=2)

        print(f"Movie results written to movie_results.json")
        with open('runlog.log', 'a') as log_file:
            log_file.write(f"Successfully wrote movie results to movie_results.json\n")


        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_message = f"[{timestamp}] Successfully crawled movies: {successful_crawl_count}"
        with open('runlog.log', 'w') as run_log_file: # Write to runlog.txt
            run_log_file.write(log_message + "\n")

if __name__ == "__main__":
    asyncio.run(main())