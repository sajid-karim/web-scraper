import unittest
import os
import sys
import json
from unittest.mock import patch, MagicMock
from pathlib import Path

# Add parent directory to path to import web_scraper
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from web_scraper.core.scraper import Scraper
from web_scraper.core.robots_parser import RobotsParser
from web_scraper.parsers.html_parser import HTMLParser


class MockResponse:
    """Mock response class for testing"""
    def __init__(self, text, status_code=200):
        self.text = text
        self.status_code = status_code
        self.ok = status_code < 400
    
    def raise_for_status(self):
        if not self.ok:
            raise Exception(f"HTTP Error: {self.status_code}")


class TestScraper(unittest.TestCase):

    def setUp(self):
        """Set up test fixtures"""
        self.scraper = Scraper(rate_limit=0.01, respect_robots_txt=False)
        self.html_parser = HTMLParser()
        
        # Sample HTML content for testing
        self.test_html = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>Test Page</title>
            <meta name="description" content="A test page for unit testing">
        </head>
        <body>
            <h1>Test Page Heading</h1>
            <p>This is a test paragraph with some <a href="https://example.com">link text</a>.</p>
            <ul>
                <li>Item 1</li>
                <li>Item 2</li>
            </ul>
            <table>
                <tr>
                    <th>Header 1</th>
                    <th>Header 2</th>
                </tr>
                <tr>
                    <td>Data 1</td>
                    <td>Data 2</td>
                </tr>
            </table>
        </body>
        </html>
        """
        
        # Sample robots.txt content
        self.robots_txt = """
        User-agent: *
        Disallow: /private/
        Allow: /public/
        
        User-agent: BadBot
        Disallow: /
        
        Crawl-delay: 2
        """

    @patch('requests.Session.get')
    def test_get_request(self, mock_get):
        """Test the get method of the Scraper class"""
        mock_get.return_value = MockResponse(self.test_html)
        
        response = self.scraper.get("https://example.com")
        
        self.assertEqual(response.text, self.test_html)
        mock_get.assert_called_once()
        
    @patch('urllib.robotparser.RobotFileParser.can_fetch')
    @patch('requests.get')
    def test_robots_parser(self, mock_get, mock_can_fetch):
        """Test the RobotsParser class"""
        mock_get.return_value = MockResponse(self.robots_txt)
        mock_can_fetch.return_value = True
        
        robots_parser = RobotsParser()
        result = robots_parser.can_fetch("https://example.com/public/page.html")
        
        self.assertTrue(result)
        
    def test_html_parser_extract_text(self):
        """Test the extract_text method of HTMLParser"""
        text = self.html_parser.extract_text(self.test_html)
        
        self.assertIn("Test Page Heading", text)
        self.assertIn("This is a test paragraph", text)
        
    def test_html_parser_extract_links(self):
        """Test the extract_links method of HTMLParser"""
        links = self.html_parser.extract_links(self.test_html, "https://example.com")
        
        self.assertEqual(len(links), 1)
        self.assertEqual(links[0]['text'], "link text")
        self.assertEqual(links[0]['href'], "https://example.com")
        
    def test_html_parser_extract_table(self):
        """Test the extract_table method of HTMLParser"""
        tables = self.html_parser.extract_table(self.test_html)
        
        self.assertEqual(len(tables), 1)
        self.assertEqual(tables[0]['Header 1'], "Data 1")
        self.assertEqual(tables[0]['Header 2'], "Data 2")
        
    def test_html_parser_extract_metadata(self):
        """Test the extract_metadata method of HTMLParser"""
        metadata = self.html_parser.extract_metadata(self.test_html)
        
        self.assertEqual(metadata['title'], "Test Page")
        self.assertEqual(metadata['description'], "A test page for unit testing")


if __name__ == "__main__":
    unittest.main() 