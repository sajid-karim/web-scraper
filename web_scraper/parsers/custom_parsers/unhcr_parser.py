import logging
from typing import Dict, Any, List, Optional
from bs4 import BeautifulSoup
import re
import json
import requests
import os
import pandas as pd
from urllib.parse import urljoin, urlparse
import csv
from io import StringIO

logger = logging.getLogger(__name__)


class UNHCRParser:
    """
    Custom parser for UNHCR website data, including the Refugee Data Finder
    and Global Trends reports.
    """
    def __init__(self):
        """
        Initialize the UNHCRParser.
        """
        self.base_url = "https://www.unhcr.org"
        self.data_finder_url = "https://www.unhcr.org/refugee-statistics/"
        self.global_trends_url = "https://www.unhcr.org/globaltrends"
        self.output_dir = "./data/unhcr"
        
        # Create output directories if they don't exist
        os.makedirs(os.path.join(self.output_dir, "refugee_data"), exist_ok=True)
        os.makedirs(os.path.join(self.output_dir, "global_trends"), exist_ok=True)
        
    def parse(self, html_content: str, url: str) -> Dict[str, Any]:
        """
        Parse HTML content from UNHCR website.
        
        Args:
            html_content: The HTML content to parse
            url: The URL the content was retrieved from
            
        Returns:
            A dictionary containing the parsed data
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        result = {
            'url': url,
            'source': 'UNHCR',
        }
        
        # Extract page title
        title_elem = soup.find('title')
        if title_elem:
            result['title'] = title_elem.get_text(strip=True)
        
        # Extract page metadata
        meta_tags = soup.find_all('meta')
        metadata = {}
        for tag in meta_tags:
            if tag.get('name') and tag.get('content'):
                metadata[tag['name']] = tag['content']
            elif tag.get('property') and tag.get('content'):
                metadata[tag['property']] = tag['content']
        result['metadata'] = metadata
        
        # Determine the type of page and parse accordingly
        if 'refugee-statistics' in url or 'data-finder' in url.lower():
            result.update(self._parse_refugee_data_finder(soup, url))
        elif 'globaltrends' in url.lower() or 'global-trends' in url.lower():
            result.update(self._parse_global_trends(soup, url))
        else:
            # Generic content parsing
            result.update(self._parse_generic_page(soup, url))
            
        return result
    
    def _parse_refugee_data_finder(self, soup: BeautifulSoup, url: str) -> Dict[str, Any]:
        """
        Parse the UNHCR Refugee Data Finder page.
        
        Args:
            soup: BeautifulSoup object of the page
            url: The URL the content was retrieved from
            
        Returns:
            A dictionary containing refugee data
        """
        result = {
            'content_type': 'refugee_data_finder',
            'refugee_data': {}
        }
        
        # Look for data visualization elements
        data_elements = soup.find_all('div', class_=re.compile('visualization|data-viz|chart|graph'))
        
        # Look for embedded data or scripts that might contain data
        scripts = soup.find_all('script')
        data_json = None
        
        for script in scripts:
            # Look for inline JSON data
            if script.string and ('refugeeData' in script.string or 'refugee_data' in script.string):
                # Try to extract JSON
                json_match = re.search(r'var\s+(\w+)\s*=\s*({.*?});', script.string, re.DOTALL)
                if json_match:
                    try:
                        data_json = json.loads(json_match.group(2))
                        logger.info("Found refugee data in JSON format")
                        break
                    except json.JSONDecodeError:
                        logger.warning("Failed to parse JSON data")
        
        # Check for data download links
        download_links = []
        for link in soup.find_all('a', href=True):
            href = link['href'].lower()
            link_text = link.get_text(strip=True).lower()
            
            if (('.csv' in href or '.xlsx' in href or '.xls' in href or 'download' in href) and
                ('data' in link_text or 'statistics' in link_text or 'download' in link_text)):
                download_url = href if href.startswith('http') else urljoin(self.base_url, href)
                download_links.append({
                    'url': download_url,
                    'text': link.get_text(strip=True)
                })
                
                # Try to download and parse the data
                try:
                    downloaded_data = self._download_and_parse_data(download_url)
                    if downloaded_data:
                        logger.info(f"Successfully downloaded and parsed data from {download_url}")
                        result['refugee_data'][f'downloaded_data_{len(download_links)}'] = downloaded_data
                except Exception as e:
                    logger.error(f"Error downloading data from {download_url}: {e}")
        
        result['refugee_data']['download_links'] = download_links
        
        # Extract data from the page itself
        data_tables = []
        for table in soup.find_all('table'):
            table_data = self._parse_table_to_dict(table)
            if table_data:
                data_tables.append(table_data)
        
        if data_tables:
            result['refugee_data']['tables'] = data_tables
            
        # If we found JSON data, include it
        if data_json:
            result['refugee_data']['json_data'] = data_json
            
        # Check for filters and parameters
        filter_elements = soup.find_all(['select', 'input', 'button'], class_=re.compile('filter|parameter|control'))
        filters = []
        
        for element in filter_elements:
            filter_info = {
                'type': element.name,
                'id': element.get('id', ''),
                'name': element.get('name', ''),
                'label': element.get('aria-label', element.get('placeholder', ''))
            }
            
            # For select elements, get the options
            if element.name == 'select':
                options = []
                for option in element.find_all('option'):
                    options.append({
                        'value': option.get('value', ''),
                        'text': option.get_text(strip=True)
                    })
                filter_info['options'] = options
                
            filters.append(filter_info)
            
        if filters:
            result['refugee_data']['filters'] = filters
            
        # Save the results to JSON
        filename = "refugee_data_finder.json"
        file_path = os.path.join(self.output_dir, "refugee_data", filename)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
            
        logger.info(f"Saved refugee data to {file_path}")
            
        return result
    
    def _parse_global_trends(self, soup: BeautifulSoup, url: str) -> Dict[str, Any]:
        """
        Parse the UNHCR Global Trends report page.
        
        Args:
            soup: BeautifulSoup object of the page
            url: The URL the content was retrieved from
            
        Returns:
            A dictionary containing global trends data
        """
        result = {
            'content_type': 'global_trends',
            'trends_data': {}
        }
        
        # Get the report title and year
        title_elem = soup.find('h1')
        if title_elem:
            title_text = title_elem.get_text(strip=True)
            result['trends_data']['title'] = title_text
            
            # Extract year from title
            year_match = re.search(r'20\d{2}', title_text)
            if year_match:
                result['trends_data']['year'] = year_match.group(0)
        
        # Look for report download links (PDF)
        pdf_links = []
        for link in soup.find_all('a', href=True):
            href = link['href']
            if href.endswith('.pdf'):
                pdf_links.append({
                    'url': href if href.startswith('http') else urljoin(self.base_url, href),
                    'text': link.get_text(strip=True)
                })
        
        result['trends_data']['pdf_links'] = pdf_links
        
        # Extract key figures and statistics
        key_figures_section = soup.find(['div', 'section'], class_=re.compile('key-figures|highlights|statistics'))
        
        if key_figures_section:
            key_figures = []
            
            # Look for stats in various formats
            stats_elements = key_figures_section.find_all(['div', 'p', 'li'], class_=re.compile('stat|figure|number'))
            
            if stats_elements:
                for elem in stats_elements:
                    key_figures.append(elem.get_text(strip=True))
            else:
                # If no specific stat elements, try to extract from paragraphs and list items
                for elem in key_figures_section.find_all(['p', 'li']):
                    # If it contains a number, it's likely a statistic
                    if re.search(r'\d+', elem.get_text()):
                        key_figures.append(elem.get_text(strip=True))
                        
            result['trends_data']['key_figures'] = key_figures
            
        # Extract data tables
        data_tables = []
        for table in soup.find_all('table'):
            table_data = self._parse_table_to_dict(table)
            if table_data:
                data_tables.append(table_data)
        
        if data_tables:
            result['trends_data']['tables'] = data_tables
            
        # Look for time series data
        time_series_section = soup.find(['div', 'section'], string=re.compile('Time Series|Historical|Trends over time', re.I)) or \
                             soup.find(['div', 'section'], class_=re.compile('time-series|historical|trends'))
        
        if time_series_section:
            time_series = {}
            
            # Look for charts and graphs
            chart_elements = time_series_section.find_all(['div', 'iframe'], class_=re.compile('chart|graph|visualization'))
            
            for i, chart in enumerate(chart_elements):
                chart_data = {
                    'title': '',
                    'description': '',
                    'data_source': ''
                }
                
                # Try to find title and description
                title_elem = chart.find(['h2', 'h3', 'h4', 'div'], class_=re.compile('title|caption'))
                if title_elem:
                    chart_data['title'] = title_elem.get_text(strip=True)
                    
                desc_elem = chart.find(['p', 'div'], class_=re.compile('description|caption|subtitle'))
                if desc_elem:
                    chart_data['description'] = desc_elem.get_text(strip=True)
                    
                # Look for data source
                source_elem = chart.find(string=re.compile('Source:', re.I))
                if source_elem:
                    chart_data['data_source'] = source_elem.parent.get_text(strip=True)
                    
                time_series[f'chart_{i+1}'] = chart_data
                
            result['trends_data']['time_series'] = time_series
            
        # Try to extract data from scripts (interactive visualizations)
        scripts = soup.find_all('script')
        for script in scripts:
            if script.string and ('trends' in script.string or 'timeSeries' in script.string):
                # Try to extract JSON data
                json_match = re.search(r'var\s+(\w+)\s*=\s*({.*?});', script.string, re.DOTALL)
                if json_match:
                    try:
                        trend_data = json.loads(json_match.group(2))
                        result['trends_data']['chart_data'] = trend_data
                        logger.info("Found trend chart data in JSON format")
                        break
                    except json.JSONDecodeError:
                        logger.warning("Failed to parse JSON trend data")
                        
        # Save results
        if 'year' in result['trends_data']:
            year = result['trends_data']['year']
            filename = f"global_trends_{year}.json"
        else:
            filename = "global_trends.json"
            
        file_path = os.path.join(self.output_dir, "global_trends", filename)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
            
        logger.info(f"Saved global trends data to {file_path}")
        
        return result
    
    def _parse_generic_page(self, soup: BeautifulSoup, url: str) -> Dict[str, Any]:
        """
        Parse any generic UNHCR page.
        
        Args:
            soup: BeautifulSoup object of the page
            url: The URL the content was retrieved from
            
        Returns:
            A dictionary containing generic page data
        """
        result = {
            'content_type': 'generic',
            'page_details': {}
        }
        
        # Extract main content
        main_content = soup.find('main') or soup.find('div', id=re.compile('content|main', re.I))
        if main_content:
            # Extract headings
            headings = []
            for h in main_content.find_all(['h1', 'h2', 'h3', 'h4']):
                headings.append({
                    'level': int(h.name[1]),
                    'text': h.get_text(strip=True)
                })
            result['page_details']['headings'] = headings
            
            # Extract paragraphs
            paragraphs = main_content.find_all('p')
            result['page_details']['content'] = '\n'.join(p.get_text(strip=True) for p in paragraphs)
            
            # Extract lists
            lists = []
            for list_elem in main_content.find_all(['ul', 'ol']):
                list_items = []
                for li in list_elem.find_all('li'):
                    list_items.append(li.get_text(strip=True))
                lists.append({
                    'type': list_elem.name,
                    'items': list_items
                })
            result['page_details']['lists'] = lists
            
        # Extract links
        links = []
        for a in soup.find_all('a', href=True):
            href = a['href']
            if href.startswith('/') or href.startswith(self.base_url):
                links.append({
                    'text': a.get_text(strip=True),
                    'url': href if href.startswith('http') else urljoin(self.base_url, href)
                })
        result['page_details']['internal_links'] = links
        
        return result
    
    def _download_and_parse_data(self, url: str) -> Optional[Dict[str, Any]]:
        """
        Download and parse data from a URL.
        
        Args:
            url: URL to download data from
            
        Returns:
            Parsed data or None if download/parsing fails
        """
        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()
            
            # Determine file type from URL and headers
            content_type = response.headers.get('Content-Type', '')
            
            if url.endswith('.csv') or 'csv' in content_type:
                # Parse CSV
                csv_data = StringIO(response.text)
                df = pd.read_csv(csv_data)
                
                # Save to disk
                filename = os.path.basename(urlparse(url).path)
                output_path = os.path.join(self.output_dir, "refugee_data", filename)
                
                df.to_csv(output_path, index=False)
                logger.info(f"Saved CSV data to {output_path}")
                
                # Convert to dictionary for JSON serialization
                return {
                    'columns': df.columns.tolist(),
                    'data': df.to_dict(orient='records'),
                    'summary': {
                        'rows': len(df),
                        'columns': len(df.columns)
                    }
                }
                
            elif url.endswith('.xlsx') or url.endswith('.xls') or 'excel' in content_type:
                # Parse Excel
                df = pd.read_excel(response.content)
                
                # Save to disk
                filename = os.path.basename(urlparse(url).path)
                output_path = os.path.join(self.output_dir, "refugee_data", filename)
                
                with open(output_path, 'wb') as f:
                    f.write(response.content)
                logger.info(f"Saved Excel data to {output_path}")
                
                # Also save as CSV for easier processing
                csv_path = os.path.splitext(output_path)[0] + '.csv'
                df.to_csv(csv_path, index=False)
                
                # Convert to dictionary for JSON serialization
                return {
                    'columns': df.columns.tolist(),
                    'data': df.to_dict(orient='records'),
                    'summary': {
                        'rows': len(df),
                        'columns': len(df.columns)
                    }
                }
            
            else:
                logger.warning(f"Unsupported file format for {url}")
                return None
                
        except Exception as e:
            logger.error(f"Error downloading or parsing data from {url}: {e}")
            return None
    
    def _parse_table_to_dict(self, table_elem: BeautifulSoup) -> Optional[Dict[str, Any]]:
        """
        Parse an HTML table to a dictionary structure.
        
        Args:
            table_elem: BeautifulSoup table element
            
        Returns:
            Dictionary representation of the table or None if parsing fails
        """
        try:
            # Get table caption or nearby header as title
            title = ""
            caption = table_elem.find('caption')
            if caption:
                title = caption.get_text(strip=True)
            else:
                # Look for a preceding header
                prev_elem = table_elem.find_previous(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])
                if prev_elem:
                    title = prev_elem.get_text(strip=True)
            
            # Extract headers
            headers = []
            header_row = table_elem.find('tr')
            if header_row:
                for th in header_row.find_all(['th']):
                    headers.append(th.get_text(strip=True))
                    
                # If no th elements, use first row as header
                if not headers:
                    for td in header_row.find_all(['td']):
                        headers.append(td.get_text(strip=True))
            
            # If still no headers, use generic column names
            if not headers:
                # Count max columns
                max_cols = 0
                for row in table_elem.find_all('tr'):
                    cols = len(row.find_all(['td', 'th']))
                    max_cols = max(max_cols, cols)
                
                headers = [f"Column {i+1}" for i in range(max_cols)]
            
            # Extract rows
            rows = []
            for tr in table_elem.find_all('tr')[1:]:  # Skip header row
                cells = tr.find_all(['td', 'th'])
                if cells:
                    row_data = {}
                    for i, cell in enumerate(cells):
                        if i < len(headers):
                            row_data[headers[i]] = cell.get_text(strip=True)
                        else:
                            row_data[f"Column {i+1}"] = cell.get_text(strip=True)
                    
                    if row_data:
                        rows.append(row_data)
            
            return {
                'title': title,
                'headers': headers,
                'rows': rows,
                'row_count': len(rows)
            }
            
        except Exception as e:
            logger.error(f"Error parsing table: {e}")
            return None


# Function version for compatibility with the CLI
def parse(html_content: str, url: str) -> Dict[str, Any]:
    """
    Parse function for CLI compatibility.
    
    Args:
        html_content: The HTML content to parse
        url: The URL the content was retrieved from
        
    Returns:
        A dictionary containing the parsed data
    """
    parser = UNHCRParser()
    return parser.parse(html_content, url) 