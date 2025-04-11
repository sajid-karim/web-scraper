import logging
import requests
from urllib.parse import urlparse
from typing import Optional, Dict, Any, List, Callable, Union
import time

from web_scraper.core.robots_parser import RobotsParser
from web_scraper.core.rate_limiter import RateLimiter
from web_scraper.utils.user_agent import UserAgentRotator

logger = logging.getLogger(__name__)


class Scraper:
    """
    Main scraper class that handles fetching web pages.
    """
    def __init__(self, 
                 rate_limit: float = 1.0, 
                 respect_robots_txt: bool = True,
                 use_fake_useragent: bool = True,
                 max_retries: int = 3,
                 verify_ssl: bool = True):
        """
        Initialize the Scraper.
        
        Args:
            rate_limit: Default rate limit in seconds between requests (default: 1.0)
            respect_robots_txt: Whether to respect robots.txt files (default: True)
            use_fake_useragent: Whether to use the fake-useragent library (default: True)
            max_retries: Maximum number of retries for failed requests (default: 3)
            verify_ssl: Whether to verify SSL certificates (default: True)
        """
        self.rate_limiter = RateLimiter(default_delay=rate_limit, max_retries=max_retries)
        self.robots_parser = RobotsParser() if respect_robots_txt else None
        self.user_agent_rotator = UserAgentRotator(use_fake_useragent=use_fake_useragent)
        self.verify_ssl = verify_ssl
        
        # Session for connection pooling and cookie persistence
        self.session = requests.Session()
        
    def _get_domain(self, url: str) -> str:
        """
        Extract the domain from a URL.
        
        Args:
            url: The URL to extract the domain from
            
        Returns:
            The domain part of the URL
        """
        parsed_url = urlparse(url)
        return parsed_url.netloc
        
    def can_fetch(self, url: str) -> bool:
        """
        Check if the URL can be fetched according to robots.txt rules.
        
        Args:
            url: The URL to check
            
        Returns:
            True if the URL can be fetched, False otherwise
        """
        if self.robots_parser is None:
            return True
            
        # Get the current user agent
        user_agent = self.user_agent_rotator.get_random_user_agent()
        self.robots_parser.set_user_agent(user_agent)
        
        return self.robots_parser.can_fetch(url, user_agent)
        
    def _prepare_request(self, url: str, headers: Optional[Dict[str, str]] = None, 
                         cookies: Optional[Dict[str, str]] = None, **kwargs) -> Dict[str, Any]:
        """
        Prepare the request parameters.
        
        Args:
            url: The URL to request
            headers: Optional additional headers (default: None)
            cookies: Optional cookies to send with the request (default: None)
            **kwargs: Additional keyword arguments to pass to requests
            
        Returns:
            A dictionary of request parameters
        """
        # Get headers with a random user agent
        req_headers = self.user_agent_rotator.get_headers(headers)
        
        # Prepare request parameters
        request_params = {
            'url': url,
            'headers': req_headers,
            'timeout': kwargs.get('timeout', 30),
            'verify': kwargs.get('verify', self.verify_ssl),
        }
        
        if cookies:
            request_params['cookies'] = cookies
            
        # Add any other kwargs
        for key, value in kwargs.items():
            if key not in request_params:
                request_params[key] = value
                
        return request_params
        
    def get(self, url: str, headers: Optional[Dict[str, str]] = None, 
            cookies: Optional[Dict[str, str]] = None, **kwargs) -> requests.Response:
        """
        Fetch a URL with GET method.
        
        Args:
            url: The URL to fetch
            headers: Optional additional headers (default: None)
            cookies: Optional cookies to send with the request (default: None)
            **kwargs: Additional keyword arguments to pass to requests
            
        Returns:
            A requests.Response object
            
        Raises:
            requests.RequestException: If the request fails
        """
        domain = self._get_domain(url)
        
        # Check robots.txt if needed
        if self.robots_parser and not self.can_fetch(url):
            logger.warning(f"URL {url} is disallowed by robots.txt")
            raise PermissionError(f"URL {url} is disallowed by robots.txt")
            
        # Respect rate limiting
        self.rate_limiter.wait(domain)
        
        # Set custom delay from robots.txt if available
        if self.robots_parser:
            crawl_delay = self.robots_parser.crawl_delay(url)
            if crawl_delay:
                self.rate_limiter.set_domain_delay(domain, crawl_delay)
                
        # Prepare the request
        request_params = self._prepare_request(url, headers, cookies, **kwargs)
        
        # Make the request with retries and backoff
        def _make_request():
            return self.session.get(**request_params)
            
        response = self.rate_limiter.retry_with_backoff(_make_request)
        
        # Check the response status code
        response.raise_for_status()
        
        return response
        
    def post(self, url: str, data: Optional[Dict[str, Any]] = None, 
             json: Optional[Dict[str, Any]] = None, 
             headers: Optional[Dict[str, str]] = None, 
             cookies: Optional[Dict[str, str]] = None, **kwargs) -> requests.Response:
        """
        Fetch a URL with POST method.
        
        Args:
            url: The URL to fetch
            data: Optional form data to send (default: None)
            json: Optional JSON data to send (default: None)
            headers: Optional additional headers (default: None)
            cookies: Optional cookies to send with the request (default: None)
            **kwargs: Additional keyword arguments to pass to requests
            
        Returns:
            A requests.Response object
            
        Raises:
            requests.RequestException: If the request fails
        """
        domain = self._get_domain(url)
        
        # Check robots.txt if needed
        if self.robots_parser and not self.can_fetch(url):
            logger.warning(f"URL {url} is disallowed by robots.txt")
            raise PermissionError(f"URL {url} is disallowed by robots.txt")
            
        # Respect rate limiting
        self.rate_limiter.wait(domain)
        
        # Prepare the request
        request_params = self._prepare_request(url, headers, cookies, **kwargs)
        
        # Add data or JSON parameters
        if data:
            request_params['data'] = data
        if json:
            request_params['json'] = json
            
        # Make the request with retries and backoff
        def _make_request():
            return self.session.post(**request_params)
            
        response = self.rate_limiter.retry_with_backoff(_make_request)
        
        # Check the response status code
        response.raise_for_status()
        
        return response
        
    def close(self) -> None:
        """
        Close the session and clean up resources.
        """
        self.session.close() 