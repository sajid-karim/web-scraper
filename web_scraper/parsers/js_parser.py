import logging
import time
from typing import Optional, Dict, Any, List, Union, Tuple
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, WebDriverException, NoSuchElementException
)
from webdriver_manager.chrome import ChromeDriverManager

logger = logging.getLogger(__name__)


class JSParser:
    """
    Parser for handling JavaScript-rendered pages using Selenium.
    """
    def __init__(self, headless: bool = True, 
                 load_timeout: int = 30,
                 driver_type: str = "chrome",
                 user_agent: Optional[str] = None):
        """
        Initialize the JSParser.
        
        Args:
            headless: Whether to run the browser in headless mode (default: True)
            load_timeout: Page load timeout in seconds (default: 30)
            driver_type: Browser driver type (default: "chrome")
            user_agent: Optional user agent string to use (default: None)
        """
        self.headless = headless
        self.load_timeout = load_timeout
        self.driver_type = driver_type
        self.user_agent = user_agent
        self.driver = None
        
    def _setup_driver(self) -> None:
        """
        Set up the Selenium WebDriver.
        """
        if self.driver_type.lower() == "chrome":
            options = Options()
            
            if self.headless:
                options.add_argument("--headless")
                
            # Other useful options for scraping
            options.add_argument("--disable-gpu")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-extensions")
            options.add_argument("--disable-notifications")
            options.add_argument("--disable-infobars")
            
            # Set user agent if provided
            if self.user_agent:
                options.add_argument(f"user-agent={self.user_agent}")
                
            try:
                # Use webdriver_manager to automatically handle driver installation
                service = Service(ChromeDriverManager().install())
                self.driver = webdriver.Chrome(service=service, options=options)
                self.driver.set_page_load_timeout(self.load_timeout)
                logger.info("ChromeDriver initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize ChromeDriver: {str(e)}")
                raise
        else:
            raise ValueError(f"Unsupported driver type: {self.driver_type}")
            
    def get_driver(self) -> webdriver.Chrome:
        """
        Get the WebDriver instance, initializing it if necessary.
        
        Returns:
            The WebDriver instance
        """
        if self.driver is None:
            self._setup_driver()
        return self.driver
        
    def load_page(self, url: str, wait_for_element: Optional[str] = None, 
                  wait_timeout: int = 10) -> str:
        """
        Load a page using Selenium and return the page source.
        
        Args:
            url: The URL to load
            wait_for_element: Optional CSS selector to wait for before returning (default: None)
            wait_timeout: Timeout for waiting for elements in seconds (default: 10)
            
        Returns:
            The page source HTML
            
        Raises:
            WebDriverException: If there's an error loading the page
        """
        driver = self.get_driver()
        
        try:
            logger.info(f"Loading page: {url}")
            driver.get(url)
            
            # Wait for specific element if requested
            if wait_for_element:
                try:
                    WebDriverWait(driver, wait_timeout).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, wait_for_element))
                    )
                    logger.info(f"Found element matching selector: {wait_for_element}")
                except TimeoutException:
                    logger.warning(f"Timed out waiting for element: {wait_for_element}")
            
            # Get the page source after JavaScript execution
            page_source = driver.page_source
            return page_source
            
        except WebDriverException as e:
            logger.error(f"Error loading page {url}: {str(e)}")
            raise
            
    def find_elements(self, css_selector: str, timeout: int = 10) -> List[webdriver.remote.webelement.WebElement]:
        """
        Find elements on the current page using a CSS selector.
        
        Args:
            css_selector: The CSS selector to find elements
            timeout: Timeout for waiting for elements in seconds (default: 10)
            
        Returns:
            A list of WebElement objects
            
        Raises:
            TimeoutException: If the elements are not found within the timeout
        """
        driver = self.get_driver()
        
        try:
            elements = WebDriverWait(driver, timeout).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, css_selector))
            )
            return elements
        except TimeoutException:
            logger.warning(f"Timed out waiting for elements: {css_selector}")
            return []
            
    def get_element_text(self, css_selector: str, timeout: int = 10) -> str:
        """
        Get the text content of an element.
        
        Args:
            css_selector: The CSS selector to find the element
            timeout: Timeout for waiting for elements in seconds (default: 10)
            
        Returns:
            The text content of the element, or an empty string if not found
        """
        try:
            elements = self.find_elements(css_selector, timeout)
            if elements:
                return elements[0].text
            return ""
        except Exception as e:
            logger.error(f"Error getting element text for {css_selector}: {str(e)}")
            return ""
            
    def get_element_attribute(self, css_selector: str, attribute: str, 
                              timeout: int = 10) -> str:
        """
        Get an attribute value of an element.
        
        Args:
            css_selector: The CSS selector to find the element
            attribute: The attribute to get
            timeout: Timeout for waiting for elements in seconds (default: 10)
            
        Returns:
            The attribute value, or an empty string if not found
        """
        try:
            elements = self.find_elements(css_selector, timeout)
            if elements:
                return elements[0].get_attribute(attribute) or ""
            return ""
        except Exception as e:
            logger.error(f"Error getting attribute {attribute} for {css_selector}: {str(e)}")
            return ""
            
    def scroll_to_bottom(self, scroll_pause_time: float = 1.0, 
                         max_scrolls: Optional[int] = None) -> None:
        """
        Scroll to the bottom of the page incrementally to load lazy-loaded content.
        
        Args:
            scroll_pause_time: Time to pause between scrolls in seconds (default: 1.0)
            max_scrolls: Maximum number of scrolls to perform (default: None)
        """
        driver = self.get_driver()
        
        # Get scroll height
        last_height = driver.execute_script("return document.body.scrollHeight")
        
        scrolls = 0
        while True:
            # Scroll down to bottom
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            
            # Wait to load page
            time.sleep(scroll_pause_time)
            
            # Calculate new scroll height and compare with last scroll height
            new_height = driver.execute_script("return document.body.scrollHeight")
            
            # Break if max scrolls reached
            scrolls += 1
            if max_scrolls is not None and scrolls >= max_scrolls:
                logger.info(f"Reached maximum number of scrolls: {max_scrolls}")
                break
                
            # Break if the scroll height no longer increases
            if new_height == last_height:
                break
                
            last_height = new_height
            
    def execute_js(self, script: str, *args) -> Any:
        """
        Execute JavaScript in the browser.
        
        Args:
            script: The JavaScript to execute
            *args: Arguments to pass to the JavaScript
            
        Returns:
            The result of the JavaScript execution
        """
        driver = self.get_driver()
        return driver.execute_script(script, *args)
        
    def take_screenshot(self, filepath: str) -> bool:
        """
        Take a screenshot of the current page.
        
        Args:
            filepath: The file path to save the screenshot to
            
        Returns:
            True if successful, False otherwise
        """
        driver = self.get_driver()
        try:
            driver.save_screenshot(filepath)
            logger.info(f"Screenshot saved to {filepath}")
            return True
        except Exception as e:
            logger.error(f"Error taking screenshot: {str(e)}")
            return False
            
    def close(self) -> None:
        """
        Close the browser and clean up resources.
        """
        if self.driver:
            try:
                self.driver.quit()
                logger.info("WebDriver closed successfully")
            except Exception as e:
                logger.error(f"Error closing WebDriver: {str(e)}")
            finally:
                self.driver = None 