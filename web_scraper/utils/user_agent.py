import random
from typing import List, Optional
import logging

logger = logging.getLogger(__name__)

# Default list of common user agents if fake-useragent is not available
DEFAULT_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:95.0) Gecko/20100101 Firefox/95.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36 Edg/96.0.1054.62",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
]


class UserAgentRotator:
    """
    Handles rotation of user agent headers to mimic real browsers.
    """
    def __init__(self, use_fake_useragent: bool = True):
        """
        Initialize the UserAgentRotator.
        
        Args:
            use_fake_useragent: Whether to use the fake-useragent library (default: True)
        """
        self.user_agents: List[str] = []
        self.use_fake_useragent = use_fake_useragent
        self._fake_ua = None
        
        if use_fake_useragent:
            try:
                from fake_useragent import UserAgent
                self._fake_ua = UserAgent()
                logger.info("Successfully initialized fake-useragent")
            except (ImportError, Exception) as e:
                logger.warning(f"Failed to initialize fake-useragent: {str(e)}. Using default user agents.")
                self.use_fake_useragent = False
                self.user_agents = DEFAULT_USER_AGENTS.copy()
        else:
            self.user_agents = DEFAULT_USER_AGENTS.copy()
            
    def add_user_agent(self, user_agent: str) -> None:
        """
        Add a custom user agent to the rotation.
        
        Args:
            user_agent: The user agent string to add
        """
        if user_agent not in self.user_agents:
            self.user_agents.append(user_agent)
            
    def get_random_user_agent(self) -> str:
        """
        Get a random user agent from the rotation.
        
        Returns:
            A random user agent string
        """
        if self.use_fake_useragent and self._fake_ua:
            try:
                return self._fake_ua.random
            except Exception as e:
                logger.warning(f"Error getting user agent from fake-useragent: {str(e)}. Falling back to default list.")
                # Fall back to the default list
                self.use_fake_useragent = False
                self.user_agents = DEFAULT_USER_AGENTS.copy()
        
        return random.choice(self.user_agents)
    
    def get_headers(self, additional_headers: Optional[dict] = None) -> dict:
        """
        Get a headers dictionary with a random user agent and any additional headers.
        
        Args:
            additional_headers: Additional headers to include (default: None)
            
        Returns:
            A dictionary of HTTP headers
        """
        headers = {
            'User-Agent': self.get_random_user_agent(),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'DNT': '1',  # Do Not Track
        }
        
        if additional_headers:
            headers.update(additional_headers)
            
        return headers 