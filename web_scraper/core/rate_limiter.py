import time
import random
from typing import Dict, Optional
import logging
import requests

logger = logging.getLogger(__name__)


class RateLimiter:
    """
    Handles rate limiting and exponential backoff for web requests.
    """
    def __init__(self, default_delay: float = 1.0, 
                 max_retries: int = 5, 
                 backoff_factor: float = 2.0,
                 jitter: float = 0.1):
        """
        Initialize the RateLimiter.
        
        Args:
            default_delay: Default delay between requests in seconds (default: 1.0)
            max_retries: Maximum number of retries for failed requests (default: 5)
            backoff_factor: Multiplicative factor for exponential backoff (default: 2.0)
            jitter: Random jitter factor to add to delays (default: 0.1)
        """
        self.default_delay = default_delay
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.jitter = jitter
        self.domain_delays: Dict[str, float] = {}
        self.last_request_time: Dict[str, float] = {}
        
    def set_domain_delay(self, domain: str, delay: float) -> None:
        """
        Set a custom delay for a specific domain.
        
        Args:
            domain: The domain to set the delay for
            delay: The delay in seconds
        """
        self.domain_delays[domain] = delay
        
    def wait(self, domain: str) -> None:
        """
        Wait the appropriate amount of time before making another request to the given domain.
        
        Args:
            domain: The domain to wait for
        """
        delay = self.domain_delays.get(domain, self.default_delay)
        last_time = self.last_request_time.get(domain, 0)
        current_time = time.time()
        
        # Calculate how long we need to wait
        wait_time = max(0, last_time + delay - current_time)
        
        if wait_time > 0:
            # Add a small random jitter to avoid patterns
            jitter_amount = random.uniform(0, self.jitter * delay)
            time.sleep(wait_time + jitter_amount)
            
        # Update the last request time
        self.last_request_time[domain] = time.time()
        
    def exponential_backoff(self, retry_count: int) -> float:
        """
        Calculate the exponential backoff wait time based on the retry count.
        
        Args:
            retry_count: The current retry count (0-based)
            
        Returns:
            The wait time in seconds
        """
        # Base delay with exponential backoff
        delay = self.default_delay * (self.backoff_factor ** retry_count)
        
        # Add jitter to avoid thundering herd problem
        jitter_amount = random.uniform(0, self.jitter * delay)
        return delay + jitter_amount
        
    def should_retry(self, exception: Exception) -> bool:
        """
        Determine if a request should be retried based on the exception.
        
        Args:
            exception: The exception raised by the request
            
        Returns:
            True if the request should be retried, False otherwise
        """
        # Don't retry client errors (4xx) except for 429 (Too Many Requests)
        if isinstance(exception, requests.HTTPError):
            status_code = exception.response.status_code if hasattr(exception, 'response') else 0
            
            # Always retry rate limiting (429) and server errors (5xx)
            if status_code == 429 or (500 <= status_code < 600):
                return True
                
            # Don't retry client errors (404 Not Found, 403 Forbidden, etc.)
            if 400 <= status_code < 500 and status_code != 429:
                return False
                
        # Retry network errors, timeouts, etc.
        if isinstance(exception, (requests.ConnectionError, requests.Timeout)):
            return True
            
        # Default to retry
        return True
        
    def retry_with_backoff(self, func, *args, **kwargs):
        """
        Execute a function with retry and exponential backoff logic.
        
        Args:
            func: The function to execute
            *args, **kwargs: Arguments to pass to the function
            
        Returns:
            The result of the function
            
        Raises:
            Exception: If all retries fail
        """
        last_exception = None
        
        for retry in range(self.max_retries + 1):
            try:
                if retry > 0:
                    backoff_time = self.exponential_backoff(retry - 1)
                    logger.info(f"Retry {retry}/{self.max_retries}: Waiting {backoff_time:.2f} seconds")
                    time.sleep(backoff_time)
                    
                return func(*args, **kwargs)
                
            except Exception as e:
                last_exception = e
                logger.warning(f"Request failed (attempt {retry+1}/{self.max_retries+1}): {str(e)}")
                
                # Check if we should retry
                if retry < self.max_retries and not self.should_retry(e):
                    logger.info(f"Not retrying: {str(e)}")
                    break
                
        # If we get here, all retries failed
        raise last_exception or Exception("All retry attempts failed") 