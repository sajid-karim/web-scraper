import logging
import concurrent.futures
from typing import List, Dict, Any, Callable, Optional
import time

logger = logging.getLogger(__name__)


class ParallelProcessor:
    """
    Handles parallel processing of URLs for web scraping.
    """
    def __init__(self, max_workers: int = 5, timeout: int = 120):
        """
        Initialize the ParallelProcessor.
        
        Args:
            max_workers: Maximum number of worker threads (default: 5)
            timeout: Maximum time in seconds to wait for a task (default: 120)
        """
        self.max_workers = max_workers
        self.timeout = timeout
        
    def read_urls_from_file(self, file_path: str) -> List[str]:
        """
        Read URLs from a file, one URL per line.
        
        Args:
            file_path: Path to the file containing URLs
            
        Returns:
            List of URLs
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                # Read lines and strip whitespace
                urls = [line.strip() for line in f if line.strip()]
                
            logger.info(f"Read {len(urls)} URLs from {file_path}")
            return urls
        except Exception as e:
            logger.error(f"Error reading URLs from file {file_path}: {str(e)}")
            return []
    
    def process_urls(self, urls: List[str], scrape_func: Callable[[str], Dict[str, Any]],
                     delay_between_batches: float = 1.0) -> List[Dict[str, Any]]:
        """
        Process URLs in parallel using ThreadPoolExecutor.
        
        Args:
            urls: List of URLs to process
            scrape_func: Function that takes a URL and returns scraped data
            delay_between_batches: Time to wait between batches (default: 1.0)
            
        Returns:
            List of results from processing the URLs
        """
        results = []
        total_urls = len(urls)
        
        logger.info(f"Starting parallel processing of {total_urls} URLs with {self.max_workers} workers")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_url = {executor.submit(scrape_func, url): url for url in urls}
            
            # Process completed tasks as they complete
            for i, future in enumerate(concurrent.futures.as_completed(future_to_url), 1):
                url = future_to_url[future]
                try:
                    result = future.result(timeout=self.timeout)
                    results.append(result)
                    logger.info(f"Completed {i}/{total_urls}: {url}")
                except Exception as e:
                    logger.error(f"Error processing URL {url}: {str(e)}")
                
                # Add delay between batches to avoid overwhelming servers
                if i % self.max_workers == 0 and i < total_urls:
                    logger.debug(f"Completed batch of {self.max_workers}, sleeping for {delay_between_batches}s")
                    time.sleep(delay_between_batches)
        
        logger.info(f"Parallel processing completed. Processed {len(results)} URLs successfully")
        return results 