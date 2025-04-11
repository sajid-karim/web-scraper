import urllib.robotparser
import urllib.parse
import requests
from typing import Dict, Optional, List


class RobotsParser:
    """
    Handles parsing and respecting robots.txt files for websites.
    """
    def __init__(self):
        self.parsers: Dict[str, urllib.robotparser.RobotFileParser] = {}
        self.user_agent = "*"  # Default user agent

    def set_user_agent(self, user_agent: str) -> None:
        """
        Set the user agent to use when checking permissions.
        
        Args:
            user_agent: The user agent string
        """
        self.user_agent = user_agent

    def _get_parser(self, url: str) -> urllib.robotparser.RobotFileParser:
        """
        Get or create a parser for the given URL's domain.
        
        Args:
            url: The URL to get a parser for
            
        Returns:
            A RobotFileParser for the URL's domain
        """
        parsed_url = urllib.parse.urlparse(url)
        base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
        
        if base_url not in self.parsers:
            rp = urllib.robotparser.RobotFileParser()
            robots_url = f"{base_url}/robots.txt"
            
            try:
                response = requests.get(robots_url, timeout=10)
                if response.status_code == 200:
                    rp.parse(response.text.splitlines())
                else:
                    # If robots.txt doesn't exist or can't be retrieved, assume everything is allowed
                    rp.allow_all = True
            except requests.RequestException:
                # If there's a request error, assume everything is allowed
                rp.allow_all = True
                
            self.parsers[base_url] = rp
            
        return self.parsers[base_url]

    def can_fetch(self, url: str, user_agent: Optional[str] = None) -> bool:
        """
        Check if the user agent is allowed to fetch the given URL.
        
        Args:
            url: The URL to check
            user_agent: Override the default user agent
            
        Returns:
            True if the user agent is allowed to fetch the URL, False otherwise
        """
        parser = self._get_parser(url)
        agent = user_agent or self.user_agent
        return parser.can_fetch(agent, url)

    def crawl_delay(self, url: str, user_agent: Optional[str] = None) -> Optional[float]:
        """
        Get the crawl delay for the given URL and user agent.
        
        Args:
            url: The URL to check
            user_agent: Override the default user agent
            
        Returns:
            The crawl delay in seconds, or None if not specified
        """
        parser = self._get_parser(url)
        agent = user_agent or self.user_agent
        return parser.crawl_delay(agent)

    def get_sitemaps(self, url: str) -> List[str]:
        """
        Get the list of sitemaps for the given URL's domain.
        
        Args:
            url: The URL to check
            
        Returns:
            A list of sitemap URLs
        """
        parser = self._get_parser(url)
        return parser.sitemaps or [] 