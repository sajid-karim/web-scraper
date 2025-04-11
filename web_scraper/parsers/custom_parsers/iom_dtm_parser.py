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


class IOMDTMParser:
    """
    Custom parser for IOM's Displacement Tracking Matrix (DTM) to extract 
    displacement and mobility data.
    """
    def __init__(self):
        """
        Initialize the IOMDTMParser.
        """
        self.base_url = "https://dtm.iom.int"
        self.output_dir = "./data/iom_dtm"
        
        # Create output directories if they don't exist
        os.makedirs(os.path.join(self.output_dir, "displacement"), exist_ok=True)
        os.makedirs(os.path.join(self.output_dir, "mobility"), exist_ok=True)
        os.makedirs(os.path.join(self.output_dir, "reports"), exist_ok=True)
        
    def parse(self, html_content: str, url: str) -> Dict[str, Any]:
        """
        Parse HTML content from IOM DTM website.
        
        Args:
            html_content: The HTML content to parse
            url: The URL the content was retrieved from
            
        Returns:
            A dictionary containing the parsed data
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        result = {
            'url': url,
            'source': 'IOM DTM',
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
        if 'displacement' in url.lower():
            result.update(self._parse_displacement_page(soup, url))
        elif 'mobility' in url.lower() or 'flow-monitoring' in url.lower():
            result.update(self._parse_mobility_page(soup, url))
        elif 'report' in url.lower() or 'document' in url.lower():
            result.update(self._parse_report_page(soup, url))
        else:
            # Generic content parsing
            result.update(self._parse_generic_page(soup, url))
            
        return result
    
    def _parse_displacement_page(self, soup: BeautifulSoup, url: str) -> Dict[str, Any]:
        """
        Parse displacement data from DTM.
        
        Args:
            soup: BeautifulSoup object of the page
            url: The URL the content was retrieved from
            
        Returns:
            A dictionary containing displacement data
        """
        result = {
            'content_type': 'displacement',
            'displacement_data': {
                'country': '',
                'date': '',
                'statistics': {},
                'datasets': []
            }
        }
        
        # Extract country information
        country_elem = soup.find('h1') or soup.find('h2')
        if country_elem:
            country_text = country_elem.get_text(strip=True)
            result['displacement_data']['country'] = country_text
            
        # Extract date information
        date_pattern = re.compile(r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}')
        date_match = soup.find(string=date_pattern)
        
        if date_match:
            result['displacement_data']['date'] = date_match.strip()
        else:
            # Try to find in metadata
            for meta in metadata.items():
                if 'date' in meta[0].lower():
                    result['displacement_data']['date'] = meta[1]
                    break
        
        # Extract key displacement statistics
        stats = {}
        
        # Common patterns in displacement reports
        patterns = [
            (r'(\d+,\d+|\d+)\s+(?:displaced|IDPs|refugees)', 'total_displaced'),
            (r'(\d+,\d+|\d+)\s+(?:households|families) displaced', 'households_displaced'),
            (r'(\d+,\d+|\d+)\s+(?:returnees)', 'returnees'),
            (r'(\d+,\d+|\d+)\s+(?:locations)', 'locations')
        ]
        
        # Look for stats in paragraphs
        for p in soup.find_all('p'):
            text = p.get_text(strip=True)
            for pattern, key in patterns:
                match = re.search(pattern, text, re.I)
                if match:
                    # Remove commas from number and convert to integer if possible
                    value = match.group(1).replace(',', '')
                    try:
                        if '.' in value:
                            stats[key] = float(value)
                        else:
                            stats[key] = int(value)
                    except ValueError:
                        stats[key] = value
        
        result['displacement_data']['statistics'] = stats
        
        # Look for data visualizations or charts
        chart_elements = soup.find_all(['div', 'iframe'], 
                                      attrs={'class': re.compile('chart|graph|data-viz|visualization')})
        
        charts = []
        for chart in chart_elements:
            chart_data = {
                'type': chart.name,
                'title': '',
                'data_source': ''
            }
            
            # Try to extract chart title
            title_elem = chart.find(['h3', 'h4', 'div'], 
                                   attrs={'class': re.compile('title|caption')})
            if title_elem:
                chart_data['title'] = title_elem.get_text(strip=True)
                
            # Look for embedded iframes (common for Tableau/PowerBI)
            iframe = chart.find('iframe')
            if iframe and iframe.get('src'):
                chart_data['iframe_src'] = iframe['src']
                
            charts.append(chart_data)
            
        if charts:
            result['displacement_data']['visualizations'] = charts
        
        # Look for downloadable datasets
        datasets = []
        for link in soup.find_all('a', href=True):
            href = link['href'].lower()
            if href.endswith(('.csv', '.xlsx', '.xls', '.json', '.geojson')):
                dataset = {
                    'url': href if href.startswith('http') else urljoin(self.base_url, href),
                    'title': link.get_text(strip=True) or os.path.basename(href),
                    'format': os.path.splitext(href)[1][1:]  # Get file extension without dot
                }
                datasets.append(dataset)
                
        result['displacement_data']['datasets'] = datasets
        
        # Extract tables with more detailed statistics
        tables = soup.find_all('table')
        table_data = []
        
        for i, table in enumerate(tables):
            parsed_table = self._parse_table_to_dict(table)
            if parsed_table:
                table_data.append(parsed_table)
                
        if table_data:
            result['displacement_data']['tables'] = table_data
            
        # Save to file
        country = result['displacement_data']['country'].strip().replace(' ', '_').lower()
        date_str = result['displacement_data']['date']
        
        if country and date_str:
            try:
                # Convert to YYYYMM format
                date_obj = datetime.datetime.strptime(date_str, '%B %Y')
                filename = f"{country}_displacement_{date_obj.strftime('%Y%m')}.json"
            except ValueError:
                filename = f"{country}_displacement_{datetime.datetime.now().strftime('%Y%m%d')}.json"
        else:
            filename = f"displacement_{datetime.datetime.now().strftime('%Y%m%d')}.json"
            
        file_path = os.path.join(self.output_dir, "displacement", filename)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
            
        logger.info(f"Saved displacement data to {file_path}")
            
        return result
    
    def _parse_mobility_page(self, soup: BeautifulSoup, url: str) -> Dict[str, Any]:
        """
        Parse mobility (flow monitoring) data from DTM.
        
        Args:
            soup: BeautifulSoup object of the page
            url: The URL the content was retrieved from
            
        Returns:
            A dictionary containing mobility data
        """
        result = {
            'content_type': 'mobility',
            'mobility_data': {
                'region': '',
                'date': '',
                'statistics': {},
                'datasets': []
            }
        }
        
        # Extract region information
        region_elem = soup.find('h1') or soup.find('h2')
        if region_elem:
            region_text = region_elem.get_text(strip=True)
            result['mobility_data']['region'] = region_text
            
        # Extract date information
        date_pattern = re.compile(r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}')
        date_match = soup.find(string=date_pattern)
        
        if date_match:
            result['mobility_data']['date'] = date_match.strip()
        else:
            # Try to find in metadata
            for meta in metadata.items():
                if 'date' in meta[0].lower():
                    result['mobility_data']['date'] = meta[1]
                    break
                    
        # Extract key mobility statistics
        stats = {}
        
        # Common patterns in flow monitoring reports
        patterns = [
            (r'(\d+,\d+|\d+)\s+(?:migrants|individuals)', 'total_migrants'),
            (r'(\d+,\d+|\d+)\s+(?:movements|flows)', 'total_movements'),
            (r'(\d+,\d+|\d+)\s+(?:flow monitoring points|FMPs)', 'monitoring_points')
        ]
        
        # Look for stats in paragraphs
        for p in soup.find_all('p'):
            text = p.get_text(strip=True)
            for pattern, key in patterns:
                match = re.search(pattern, text, re.I)
                if match:
                    # Remove commas from number and convert to integer if possible
                    value = match.group(1).replace(',', '')
                    try:
                        if '.' in value:
                            stats[key] = float(value)
                        else:
                            stats[key] = int(value)
                    except ValueError:
                        stats[key] = value
        
        result['mobility_data']['statistics'] = stats
        
        # Look for maps (common in mobility reports)
        maps = []
        map_elements = soup.find_all(['img', 'div'], 
                                    attrs={'class': re.compile('map'),
                                           'src': re.compile('map', re.I)})
        
        for map_elem in map_elements:
            map_data = {
                'type': map_elem.name
            }
            
            if map_elem.name == 'img' and map_elem.get('src'):
                map_data['url'] = map_elem['src'] if map_elem['src'].startswith('http') else urljoin(self.base_url, map_elem['src'])
                map_data['alt'] = map_elem.get('alt', '')
            
            maps.append(map_data)
            
        if maps:
            result['mobility_data']['maps'] = maps
        
        # Look for downloadable datasets
        datasets = []
        for link in soup.find_all('a', href=True):
            href = link['href'].lower()
            if href.endswith(('.csv', '.xlsx', '.xls', '.json', '.geojson')):
                dataset = {
                    'url': href if href.startswith('http') else urljoin(self.base_url, href),
                    'title': link.get_text(strip=True) or os.path.basename(href),
                    'format': os.path.splitext(href)[1][1:]  # Get file extension without dot
                }
                datasets.append(dataset)
                
        result['mobility_data']['datasets'] = datasets
        
        # Extract tables with more detailed statistics
        tables = soup.find_all('table')
        table_data = []
        
        for i, table in enumerate(tables):
            parsed_table = self._parse_table_to_dict(table)
            if parsed_table:
                table_data.append(parsed_table)
                
        if table_data:
            result['mobility_data']['tables'] = table_data
            
        # Save to file
        region = result['mobility_data']['region'].strip().replace(' ', '_').lower()
        date_str = result['mobility_data']['date']
        
        if region and date_str:
            try:
                # Convert to YYYYMM format
                date_obj = datetime.datetime.strptime(date_str, '%B %Y')
                filename = f"{region}_mobility_{date_obj.strftime('%Y%m')}.json"
            except ValueError:
                filename = f"{region}_mobility_{datetime.datetime.now().strftime('%Y%m%d')}.json"
        else:
            filename = f"mobility_{datetime.datetime.now().strftime('%Y%m%d')}.json"
            
        file_path = os.path.join(self.output_dir, "mobility", filename)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
            
        logger.info(f"Saved mobility data to {file_path}")
            
        return result
    
    def _parse_report_page(self, soup: BeautifulSoup, url: str) -> Dict[str, Any]:
        """
        Parse a DTM report page.
        
        Args:
            soup: BeautifulSoup object of the page
            url: The URL the content was retrieved from
            
        Returns:
            A dictionary containing report data
        """
        result = {
            'content_type': 'report',
            'report_data': {
                'title': '',
                'date': '',
                'country': '',
                'summary': '',
                'downloads': []
            }
        }
        
        # Extract report title
        title_elem = soup.find('h1') or soup.find('h2')
        if title_elem:
            result['report_data']['title'] = title_elem.get_text(strip=True)
            
        # Extract date
        date_pattern = re.compile(r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}')
        date_match = soup.find(string=date_pattern)
        
        if date_match:
            result['report_data']['date'] = date_match.strip()
        
        # Try to extract country from title or metadata
        country_pattern = re.compile(r'in\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)')
        if 'title' in result['report_data']:
            country_match = re.search(country_pattern, result['report_data']['title'])
            if country_match:
                result['report_data']['country'] = country_match.group(1)
                
        # Extract summary
        summary_elem = soup.find(['div', 'p'], attrs={'class': re.compile('summary|abstract|excerpt')})
        if summary_elem:
            result['report_data']['summary'] = summary_elem.get_text(strip=True)
        else:
            # Use first paragraph as summary
            first_p = soup.find('p')
            if first_p:
                result['report_data']['summary'] = first_p.get_text(strip=True)
                
        # Look for downloadable files
        downloads = []
        for link in soup.find_all('a', href=True):
            href = link['href'].lower()
            if href.endswith(('.pdf', '.doc', '.docx', '.ppt', '.pptx', '.csv', '.xlsx', '.xls')):
                download = {
                    'url': href if href.startswith('http') else urljoin(self.base_url, href),
                    'title': link.get_text(strip=True) or os.path.basename(href),
                    'format': os.path.splitext(href)[1][1:]  # Get file extension without dot
                }
                downloads.append(download)
                
        result['report_data']['downloads'] = downloads
        
        # Save to file
        title = result['report_data']['title'].strip().replace(' ', '_').lower()
        safe_title = re.sub(r'[^\w_]', '', title)
        
        filename = f"{safe_title[:50]}_{datetime.datetime.now().strftime('%Y%m%d')}.json"
            
        file_path = os.path.join(self.output_dir, "reports", filename)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
            
        logger.info(f"Saved report data to {file_path}")
            
        return result
    
    def _parse_generic_page(self, soup: BeautifulSoup, url: str) -> Dict[str, Any]:
        """
        Parse any generic DTM page.
        
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
            
        # Check for data download links
        data_links = []
        for link in soup.find_all('a', href=True):
            href = link['href'].lower()
            link_text = link.get_text(strip=True).lower()
            
            # Look for data links
            if any(term in href or term in link_text for term in 
                  ['data', 'download', 'csv', 'excel', 'json', 'geojson']):
                data_links.append({
                    'url': href if href.startswith('http') else urljoin(self.base_url, href),
                    'text': link.get_text(strip=True)
                })
                
        if data_links:
            result['page_details']['data_links'] = data_links
            
        return result
        
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
    parser = IOMDTMParser()
    return parser.parse(html_content, url) 