import logging
from typing import Dict, Any, List, Optional
from bs4 import BeautifulSoup
import re
import json
import requests
import os
from urllib.parse import urljoin, urlparse
import pandas as pd
import datetime

logger = logging.getLogger(__name__)


class OECDParser:
    """
    Custom parser for OECD International Migration Database to extract
    migration data and economic integration indicators.
    """
    def __init__(self):
        """
        Initialize the OECDParser.
        """
        self.base_url = "https://www.oecd.org"
        self.migration_db_url = "https://www.oecd.org/migration/mig/oecdmigrationdatabases.htm"
        self.integration_url = "https://www.oecd.org/migration/integration-indicators-2012/"
        self.output_dir = "./data/oecd"
        
        # Create output directories if they don't exist
        os.makedirs(os.path.join(self.output_dir, "migration_flows"), exist_ok=True)
        os.makedirs(os.path.join(self.output_dir, "migration_stocks"), exist_ok=True)
        os.makedirs(os.path.join(self.output_dir, "integration"), exist_ok=True)
        os.makedirs(os.path.join(self.output_dir, "reports"), exist_ok=True)
        
    def parse(self, html_content: str, url: str) -> Dict[str, Any]:
        """
        Parse HTML content from OECD website.
        
        Args:
            html_content: The HTML content to parse
            url: The URL the content was retrieved from
            
        Returns:
            A dictionary containing the parsed data
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        result = {
            'url': url,
            'source': 'OECD',
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
        if 'migration' in url.lower() and 'database' in url.lower():
            result.update(self._parse_migration_database_page(soup, url))
        elif 'migration' in url.lower() and 'integration' in url.lower():
            result.update(self._parse_integration_page(soup, url))
        elif 'stat' in url.lower() or 'data' in url.lower():
            result.update(self._parse_stats_page(soup, url))
        else:
            # Generic page parsing
            result.update(self._parse_generic_page(soup, url))
            
        return result
    
    def _parse_migration_database_page(self, soup: BeautifulSoup, url: str) -> Dict[str, Any]:
        """
        Parse a migration database page.
        
        Args:
            soup: BeautifulSoup object of the page
            url: The URL the content was retrieved from
            
        Returns:
            A dictionary containing migration database information
        """
        result = {
            'content_type': 'migration_database',
            'database_info': {
                'title': '',
                'description': '',
                'datasets': []
            }
        }
        
        # Extract page title
        title_elem = soup.find('h1')
        if title_elem:
            result['database_info']['title'] = title_elem.get_text(strip=True)
            
        # Extract description
        description_elem = soup.find(['p', 'div'], class_=re.compile('lead|summary|description'))
        if description_elem:
            result['database_info']['description'] = description_elem.get_text(strip=True)
        else:
            # Use first paragraph as description
            first_p = soup.find('p')
            if first_p:
                result['database_info']['description'] = first_p.get_text(strip=True)
        
        # Look for dataset links
        datasets = []
        for link in soup.find_all('a', href=True):
            href = link['href'].lower()
            link_text = link.get_text(strip=True)
            
            # Check for data files or database links
            if (any(ext in href for ext in ['.csv', '.xlsx', '.xls', '.zip', '.txt']) or 
                any(term in link_text.lower() for term in ['dataset', 'database', 'data', 'statistics'])):
                
                dataset = {
                    'url': href if href.startswith('http') else urljoin(self.base_url, href),
                    'title': link_text,
                }
                
                # Try to determine the type of dataset
                if 'flow' in href.lower() or 'flow' in link_text.lower():
                    dataset['type'] = 'migration_flows'
                elif 'stock' in href.lower() or 'stock' in link_text.lower():
                    dataset['type'] = 'migration_stocks'
                elif 'integration' in href.lower() or 'integration' in link_text.lower():
                    dataset['type'] = 'integration'
                else:
                    dataset['type'] = 'other'
                    
                # Check if it's a direct download
                if any(href.endswith(ext) for ext in ['.csv', '.xlsx', '.xls', '.zip', '.txt']):
                    dataset['is_direct_download'] = True
                    dataset['format'] = os.path.splitext(href)[1][1:]  # Get file extension without dot
                    
                    # Try to download the file
                    if dataset.get('is_direct_download', False):
                        try:
                            self._download_dataset(dataset)
                        except Exception as e:
                            logger.error(f"Error downloading dataset: {e}")
                
                datasets.append(dataset)
                
        result['database_info']['datasets'] = datasets
        
        # Extract tables that might contain metadata about the database
        tables = soup.find_all('table')
        table_data = []
        
        for i, table in enumerate(tables):
            parsed_table = self._parse_table_to_dict(table)
            if parsed_table:
                table_data.append(parsed_table)
                
        if table_data:
            result['database_info']['tables'] = table_data
            
        # Save to file
        title = result['database_info']['title'].strip().replace(' ', '_').lower()
        filename = f"{title[:50]}.json" if title else f"migration_database_{datetime.datetime.now().strftime('%Y%m%d')}.json"
            
        file_path = os.path.join(self.output_dir, "reports", filename)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
            
        logger.info(f"Saved migration database info to {file_path}")
            
        return result
    
    def _parse_integration_page(self, soup: BeautifulSoup, url: str) -> Dict[str, Any]:
        """
        Parse a migration integration indicators page.
        
        Args:
            soup: BeautifulSoup object of the page
            url: The URL the content was retrieved from
            
        Returns:
            A dictionary containing integration indicators information
        """
        result = {
            'content_type': 'integration_indicators',
            'integration_info': {
                'title': '',
                'description': '',
                'indicators': [],
                'datasets': []
            }
        }
        
        # Extract page title
        title_elem = soup.find('h1')
        if title_elem:
            result['integration_info']['title'] = title_elem.get_text(strip=True)
            
        # Extract description
        description_elem = soup.find(['p', 'div'], class_=re.compile('lead|summary|description'))
        if description_elem:
            result['integration_info']['description'] = description_elem.get_text(strip=True)
        else:
            # Use first paragraph as description
            first_p = soup.find('p')
            if first_p:
                result['integration_info']['description'] = first_p.get_text(strip=True)
        
        # Look for integration indicators
        indicator_patterns = [
            r"(\w+(?:\s+\w+)*)\s+indicator",
            r"Indicator(?:s)?\s+on\s+(\w+(?:\s+\w+)*)",
            r"(\w+(?:\s+\w+)*)\s+integration"
        ]
        
        indicators = []
        for p in soup.find_all('p'):
            text = p.get_text(strip=True)
            for pattern in indicator_patterns:
                matches = re.finditer(pattern, text, re.I)
                for match in matches:
                    indicator = match.group(1).strip()
                    if indicator and indicator not in indicators:
                        indicators.append(indicator)
                        
        # Also check list items for indicators
        for li in soup.find_all('li'):
            text = li.get_text(strip=True)
            if any(term in text.lower() for term in ['indicator', 'measure', 'integration', 'index']):
                for pattern in indicator_patterns:
                    matches = re.finditer(pattern, text, re.I)
                    for match in matches:
                        indicator = match.group(1).strip()
                        if indicator and indicator not in indicators:
                            indicators.append(indicator)
                            
        result['integration_info']['indicators'] = indicators
        
        # Look for dataset links
        datasets = []
        for link in soup.find_all('a', href=True):
            href = link['href'].lower()
            link_text = link.get_text(strip=True)
            
            # Check for data files or database links
            if (any(ext in href for ext in ['.csv', '.xlsx', '.xls', '.zip', '.txt']) or 
                any(term in link_text.lower() for term in ['dataset', 'database', 'data', 'statistics', 'indicators'])):
                
                dataset = {
                    'url': href if href.startswith('http') else urljoin(self.base_url, href),
                    'title': link_text,
                }
                
                # Check if it's a direct download
                if any(href.endswith(ext) for ext in ['.csv', '.xlsx', '.xls', '.zip', '.txt']):
                    dataset['is_direct_download'] = True
                    dataset['format'] = os.path.splitext(href)[1][1:]  # Get file extension without dot
                    
                    # Try to download the file
                    if dataset.get('is_direct_download', False):
                        try:
                            self._download_dataset(dataset, subdirectory="integration")
                        except Exception as e:
                            logger.error(f"Error downloading dataset: {e}")
                
                datasets.append(dataset)
                
        result['integration_info']['datasets'] = datasets
        
        # Extract visualizations and charts
        charts = []
        chart_elements = soup.find_all(['div', 'img'], 
                                      attrs={'class': re.compile('chart|graph|visualization')})
        
        for chart_elem in chart_elements:
            chart = {
                'type': chart_elem.name
            }
            
            if chart_elem.name == 'img' and chart_elem.get('src'):
                chart['url'] = chart_elem['src'] if chart_elem['src'].startswith('http') else urljoin(self.base_url, chart_elem['src'])
                chart['alt'] = chart_elem.get('alt', '')
                
            charts.append(chart)
            
        if charts:
            result['integration_info']['charts'] = charts
            
        # Save to file
        title = result['integration_info']['title'].strip().replace(' ', '_').lower()
        filename = f"{title[:50]}.json" if title else f"integration_indicators_{datetime.datetime.now().strftime('%Y%m%d')}.json"
            
        file_path = os.path.join(self.output_dir, "integration", filename)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
            
        logger.info(f"Saved integration indicators info to {file_path}")
            
        return result
    
    def _parse_stats_page(self, soup: BeautifulSoup, url: str) -> Dict[str, Any]:
        """
        Parse a statistics or data page.
        
        Args:
            soup: BeautifulSoup object of the page
            url: The URL the content was retrieved from
            
        Returns:
            A dictionary containing statistics data
        """
        result = {
            'content_type': 'statistics',
            'stats_info': {
                'title': '',
                'description': '',
                'tables': [],
                'datasets': []
            }
        }
        
        # Extract page title
        title_elem = soup.find('h1')
        if title_elem:
            result['stats_info']['title'] = title_elem.get_text(strip=True)
            
        # Extract description
        description_elem = soup.find(['p', 'div'], class_=re.compile('lead|summary|description'))
        if description_elem:
            result['stats_info']['description'] = description_elem.get_text(strip=True)
        else:
            # Use first paragraph as description
            first_p = soup.find('p')
            if first_p:
                result['stats_info']['description'] = first_p.get_text(strip=True)
        
        # Extract tables
        tables = soup.find_all('table')
        table_data = []
        
        for i, table in enumerate(tables):
            parsed_table = self._parse_table_to_dict(table)
            if parsed_table:
                # Determine table type
                title = parsed_table.get('title', '').lower()
                if 'flow' in title:
                    parsed_table['type'] = 'migration_flows'
                elif 'stock' in title:
                    parsed_table['type'] = 'migration_stocks'
                elif 'integration' in title or 'labour' in title or 'employment' in title:
                    parsed_table['type'] = 'integration'
                else:
                    parsed_table['type'] = 'general'
                    
                table_data.append(parsed_table)
                
        result['stats_info']['tables'] = table_data
        
        # Look for dataset links
        datasets = []
        for link in soup.find_all('a', href=True):
            href = link['href'].lower()
            link_text = link.get_text(strip=True)
            
            # Check for data files or database links
            if (any(ext in href for ext in ['.csv', '.xlsx', '.xls', '.zip', '.txt']) or 
                any(term in link_text.lower() for term in ['dataset', 'database', 'data', 'statistics', 'download'])):
                
                dataset = {
                    'url': href if href.startswith('http') else urljoin(self.base_url, href),
                    'title': link_text,
                }
                
                # Try to determine the type of dataset
                if 'flow' in href.lower() or 'flow' in link_text.lower():
                    dataset['type'] = 'migration_flows'
                    subdir = "migration_flows"
                elif 'stock' in href.lower() or 'stock' in link_text.lower():
                    dataset['type'] = 'migration_stocks'
                    subdir = "migration_stocks"
                elif 'integration' in href.lower() or 'integration' in link_text.lower():
                    dataset['type'] = 'integration'
                    subdir = "integration"
                else:
                    dataset['type'] = 'other'
                    subdir = "reports"
                    
                # Check if it's a direct download
                if any(href.endswith(ext) for ext in ['.csv', '.xlsx', '.xls', '.zip', '.txt']):
                    dataset['is_direct_download'] = True
                    dataset['format'] = os.path.splitext(href)[1][1:]  # Get file extension without dot
                    
                    # Try to download the file
                    if dataset.get('is_direct_download', False):
                        try:
                            self._download_dataset(dataset, subdirectory=subdir)
                        except Exception as e:
                            logger.error(f"Error downloading dataset: {e}")
                
                datasets.append(dataset)
                
        result['stats_info']['datasets'] = datasets
        
        # Save to file
        title = result['stats_info']['title'].strip().replace(' ', '_').lower()
        filename = f"{title[:50]}.json" if title else f"stats_{datetime.datetime.now().strftime('%Y%m%d')}.json"
            
        file_path = os.path.join(self.output_dir, "reports", filename)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
            
        logger.info(f"Saved statistics info to {file_path}")
            
        return result
    
    def _parse_generic_page(self, soup: BeautifulSoup, url: str) -> Dict[str, Any]:
        """
        Parse any generic OECD page.
        
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
            
        # Look for migration-related links
        migration_links = []
        for link in soup.find_all('a', href=True):
            href = link['href']
            link_text = link.get_text(strip=True).lower()
            
            if any(term in link_text for term in ['migration', 'immigrant', 'refugee', 'asylum', 'integration']):
                migration_links.append({
                    'url': href if href.startswith('http') else urljoin(self.base_url, href),
                    'text': link.get_text(strip=True)
                })
                
        if migration_links:
            result['page_details']['migration_links'] = migration_links
            
        return result
    
    def _download_dataset(self, dataset: Dict[str, Any], subdirectory: str = None) -> bool:
        """
        Download a dataset from a URL.
        
        Args:
            dataset: Dictionary containing dataset info with at least a 'url' key
            subdirectory: Subdirectory to save the file in (default: determined from dataset type)
            
        Returns:
            True if download was successful, False otherwise
        """
        try:
            url = dataset['url']
            response = requests.get(url, stream=True)
            response.raise_for_status()
            
            # Determine directory
            if subdirectory is None:
                if 'type' in dataset:
                    if dataset['type'] == 'migration_flows':
                        subdirectory = "migration_flows"
                    elif dataset['type'] == 'migration_stocks':
                        subdirectory = "migration_stocks"
                    elif dataset['type'] == 'integration':
                        subdirectory = "integration"
                    else:
                        subdirectory = "reports"
                else:
                    subdirectory = "reports"
            
            # Get filename from URL or dataset title
            if 'title' in dataset and dataset['title']:
                # Make a safe filename from title
                filename = re.sub(r'[^\w\s.-]', '', dataset['title']).strip().replace(' ', '_')
                
                # Add extension if not present
                if 'format' in dataset and not filename.endswith('.' + dataset['format']):
                    filename += '.' + dataset['format']
            else:
                filename = os.path.basename(urlparse(url).path)
                
            if not filename:
                filename = f"dataset_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}"
                if 'format' in dataset:
                    filename += '.' + dataset['format']
                    
            # Save the file
            output_path = os.path.join(self.output_dir, subdirectory, filename)
            
            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
                    
            logger.info(f"Downloaded dataset to {output_path}")
            
            # If it's an Excel file, also save as CSV for easier processing
            if output_path.endswith(('.xlsx', '.xls')):
                try:
                    df = pd.read_excel(output_path)
                    csv_path = os.path.splitext(output_path)[0] + '.csv'
                    df.to_csv(csv_path, index=False)
                    logger.info(f"Converted Excel to CSV at {csv_path}")
                except Exception as e:
                    logger.error(f"Error converting Excel to CSV: {e}")
                    
            return True
            
        except Exception as e:
            logger.error(f"Error downloading dataset: {e}")
            return False
    
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
    parser = OECDParser()
    return parser.parse(html_content, url) 