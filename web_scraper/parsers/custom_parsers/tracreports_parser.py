import logging
from typing import Dict, Any, List, Optional
from bs4 import BeautifulSoup
import re
import json
import requests
import os
import pandas as pd
from urllib.parse import urljoin, urlparse
import datetime

logger = logging.getLogger(__name__)


class TRACReportsParser:
    """
    Custom parser for TRAC (Transactional Records Access Clearinghouse) reports
    on immigration enforcement statistics.
    """
    def __init__(self):
        """
        Initialize the TRACReportsParser.
        """
        self.base_url = "https://trac.syr.edu"
        self.immigration_url = "https://trac.syr.edu/immigration/"
        self.output_dir = "./data/tracreports"
        
        # Create output directories if they don't exist
        os.makedirs(os.path.join(self.output_dir, "detention"), exist_ok=True)
        os.makedirs(os.path.join(self.output_dir, "removal"), exist_ok=True)
        os.makedirs(os.path.join(self.output_dir, "atd"), exist_ok=True)
        os.makedirs(os.path.join(self.output_dir, "trends"), exist_ok=True)
        
    def parse(self, html_content: str, url: str) -> Dict[str, Any]:
        """
        Parse HTML content from TRAC website.
        
        Args:
            html_content: The HTML content to parse
            url: The URL the content was retrieved from
            
        Returns:
            A dictionary containing the parsed data
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        result = {
            'url': url,
            'source': 'TRAC Reports',
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
        
        # Determine the type of report and parse accordingly
        if 'detention' in url.lower():
            result.update(self._parse_detention_report(soup, url))
        elif 'removal' in url.lower() or 'deport' in url.lower():
            result.update(self._parse_removal_report(soup, url))
        elif 'atd' in url.lower() or 'alternatives-to-detention' in url.lower():
            result.update(self._parse_atd_report(soup, url))
        else:
            # Generic report parsing
            result.update(self._parse_generic_report(soup, url))
            
        return result
    
    def _parse_detention_report(self, soup: BeautifulSoup, url: str) -> Dict[str, Any]:
        """
        Parse a detention statistics report.
        
        Args:
            soup: BeautifulSoup object of the page
            url: The URL the content was retrieved from
            
        Returns:
            A dictionary containing detention statistics
        """
        result = {
            'content_type': 'detention_report',
            'detention_details': {
                'data_date': '',
                'statistics': {},
                'trends': []
            }
        }
        
        # Extract report date
        date_pattern = re.compile(r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}')
        date_match = soup.find(string=date_pattern)
        
        if date_match:
            result['detention_details']['data_date'] = date_match.strip()
        else:
            # Try to find it in the title
            title = result.get('title', '')
            date_match = re.search(date_pattern, title)
            if date_match:
                result['detention_details']['data_date'] = date_match.group(0)
                
        # Extract key detention statistics
        # Look for summary statistics at the top of the report
        summary = {}
        
        # Common patterns in detention reports
        patterns = [
            (r'(\d+,\d+|\d+)\s+(?:individuals|people|immigrants|detainees)', 'total_detained'),
            (r'Average length of stay:\s*(\d+\.?\d*)', 'avg_length_of_stay_days'),
            (r'(\d+,\d+|\d+)\s+(?:new|recent)\s+admissions', 'new_admissions'),
            (r'(\d+,\d+|\d+)\s+(?:individuals|people|immigrants|detainees) released', 'total_released')
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
                            summary[key] = float(value)
                        else:
                            summary[key] = int(value)
                    except ValueError:
                        summary[key] = value
        
        result['detention_details']['statistics'] = summary
        
        # Extract tables with more detailed statistics
        tables = soup.find_all('table')
        table_data = []
        
        for i, table in enumerate(tables):
            parsed_table = self._parse_table_to_dict(table)
            if parsed_table:
                table_data.append(parsed_table)
                
                # Try to detect what kind of table this is
                title = parsed_table.get('title', '').lower()
                if 'custody' in title or 'detention' in title:
                    result['detention_details']['custody_table'] = parsed_table
                elif 'facility' in title or 'location' in title:
                    result['detention_details']['facility_table'] = parsed_table
                elif 'nationality' in title or 'country' in title:
                    result['detention_details']['nationality_table'] = parsed_table
                
        if table_data:
            result['detention_details']['tables'] = table_data
            
        # Look for time series data
        time_series = self._extract_time_series(soup)
        if time_series:
            result['detention_details']['trends'] = time_series
            
        # Save to file system
        filename = f"detention_{datetime.datetime.now().strftime('%Y%m%d')}.json"
        if result['detention_details']['data_date']:
            # Convert to YYYYMM format
            try:
                date_str = result['detention_details']['data_date']
                date_obj = datetime.datetime.strptime(date_str, '%B %Y')
                filename = f"detention_{date_obj.strftime('%Y%m')}.json"
            except ValueError:
                pass
                
        file_path = os.path.join(self.output_dir, "detention", filename)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
            
        logger.info(f"Saved detention data to {file_path}")
            
        return result
    
    def _parse_removal_report(self, soup: BeautifulSoup, url: str) -> Dict[str, Any]:
        """
        Parse a removal/deportation statistics report.
        
        Args:
            soup: BeautifulSoup object of the page
            url: The URL the content was retrieved from
            
        Returns:
            A dictionary containing removal statistics
        """
        result = {
            'content_type': 'removal_report',
            'removal_details': {
                'data_date': '',
                'statistics': {},
                'trends': []
            }
        }
        
        # Extract report date
        date_pattern = re.compile(r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}')
        date_match = soup.find(string=date_pattern)
        
        if date_match:
            result['removal_details']['data_date'] = date_match.strip()
        else:
            # Try to find it in the title
            title = result.get('title', '')
            date_match = re.search(date_pattern, title)
            if date_match:
                result['removal_details']['data_date'] = date_match.group(0)
                
        # Extract key removal statistics
        summary = {}
        
        # Common patterns in removal reports
        patterns = [
            (r'(\d+,\d+|\d+)\s+(?:removals|deportations)', 'total_removals'),
            (r'(\d+,\d+|\d+)\s+expedited\s+removals', 'expedited_removals'),
            (r'(\d+,\d+|\d+)\s+returned', 'returns'),
            (r'increase of\s+(\d+\.?\d*)%', 'increase_percentage'),
            (r'decrease of\s+(\d+\.?\d*)%', 'decrease_percentage')
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
                            summary[key] = float(value)
                        else:
                            summary[key] = int(value)
                    except ValueError:
                        summary[key] = value
        
        result['removal_details']['statistics'] = summary
        
        # Extract tables with more detailed statistics
        tables = soup.find_all('table')
        table_data = []
        
        for i, table in enumerate(tables):
            parsed_table = self._parse_table_to_dict(table)
            if parsed_table:
                table_data.append(parsed_table)
                
                # Try to detect what kind of table this is
                title = parsed_table.get('title', '').lower()
                if 'removal' in title or 'deport' in title:
                    result['removal_details']['main_table'] = parsed_table
                elif 'nationality' in title or 'country' in title:
                    result['removal_details']['nationality_table'] = parsed_table
                elif 'reason' in title or 'grounds' in title:
                    result['removal_details']['grounds_table'] = parsed_table
                
        if table_data:
            result['removal_details']['tables'] = table_data
            
        # Look for time series data
        time_series = self._extract_time_series(soup)
        if time_series:
            result['removal_details']['trends'] = time_series
            
        # Save to file system
        filename = f"removal_{datetime.datetime.now().strftime('%Y%m%d')}.json"
        if result['removal_details']['data_date']:
            # Convert to YYYYMM format
            try:
                date_str = result['removal_details']['data_date']
                date_obj = datetime.datetime.strptime(date_str, '%B %Y')
                filename = f"removal_{date_obj.strftime('%Y%m')}.json"
            except ValueError:
                pass
                
        file_path = os.path.join(self.output_dir, "removal", filename)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
            
        logger.info(f"Saved removal data to {file_path}")
            
        return result
    
    def _parse_atd_report(self, soup: BeautifulSoup, url: str) -> Dict[str, Any]:
        """
        Parse an Alternatives to Detention (ATD) program report.
        
        Args:
            soup: BeautifulSoup object of the page
            url: The URL the content was retrieved from
            
        Returns:
            A dictionary containing ATD program statistics
        """
        result = {
            'content_type': 'atd_report',
            'atd_details': {
                'data_date': '',
                'statistics': {},
                'trends': []
            }
        }
        
        # Extract report date
        date_pattern = re.compile(r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}')
        date_match = soup.find(string=date_pattern)
        
        if date_match:
            result['atd_details']['data_date'] = date_match.strip()
        else:
            # Try to find it in the title
            title = result.get('title', '')
            date_match = re.search(date_pattern, title)
            if date_match:
                result['atd_details']['data_date'] = date_match.group(0)
                
        # Extract key ATD statistics
        summary = {}
        
        # Common patterns in ATD reports
        patterns = [
            (r'(\d+,\d+|\d+)\s+enrolled', 'total_enrolled'),
            (r'(\d+,\d+|\d+)\s+(?:individuals|people|immigrants) under ATD', 'current_participants'),
            (r'(\d+\.?\d*)%\s+compliance rate', 'compliance_rate'),
            (r'Average(?:\s+length)?\s+of\s+enrollment:\s*(\d+\.?\d*)', 'avg_enrollment_days')
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
                            summary[key] = float(value)
                        else:
                            summary[key] = int(value)
                    except ValueError:
                        summary[key] = value
        
        result['atd_details']['statistics'] = summary
        
        # Extract tables with more detailed statistics
        tables = soup.find_all('table')
        table_data = []
        
        for i, table in enumerate(tables):
            parsed_table = self._parse_table_to_dict(table)
            if parsed_table:
                table_data.append(parsed_table)
                
                # Try to detect what kind of table this is
                title = parsed_table.get('title', '').lower()
                if 'atd' in title or 'alternative' in title:
                    result['atd_details']['main_table'] = parsed_table
                elif 'technology' in title or 'monitoring' in title:
                    result['atd_details']['technology_table'] = parsed_table
                elif 'nationality' in title or 'country' in title:
                    result['atd_details']['nationality_table'] = parsed_table
                
        if table_data:
            result['atd_details']['tables'] = table_data
            
        # Look for time series data
        time_series = self._extract_time_series(soup)
        if time_series:
            result['atd_details']['trends'] = time_series
            
        # Save to file system
        filename = f"atd_{datetime.datetime.now().strftime('%Y%m%d')}.json"
        if result['atd_details']['data_date']:
            # Convert to YYYYMM format
            try:
                date_str = result['atd_details']['data_date']
                date_obj = datetime.datetime.strptime(date_str, '%B %Y')
                filename = f"atd_{date_obj.strftime('%Y%m')}.json"
            except ValueError:
                pass
                
        file_path = os.path.join(self.output_dir, "atd", filename)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
            
        logger.info(f"Saved ATD data to {file_path}")
            
        return result
    
    def _parse_generic_report(self, soup: BeautifulSoup, url: str) -> Dict[str, Any]:
        """
        Parse any generic TRAC report page.
        
        Args:
            soup: BeautifulSoup object of the page
            url: The URL the content was retrieved from
            
        Returns:
            A dictionary containing generic report data
        """
        result = {
            'content_type': 'generic_report',
            'report_details': {}
        }
        
        # Extract report title
        title_elem = soup.find('h1') or soup.find('h2')
        if title_elem:
            result['report_details']['title'] = title_elem.get_text(strip=True)
            
        # Extract report date
        date_pattern = re.compile(r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4}|'
                                r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}')
        date_elem = soup.find(string=date_pattern)
        
        if date_elem:
            result['report_details']['date'] = date_elem.strip()
        else:
            # Try to find in metadata
            for meta in soup.find_all('meta'):
                if meta.get('name') in ['date', 'pubdate', 'publication_date']:
                    result['report_details']['date'] = meta.get('content')
                    break
                    
        # Extract main content
        content_div = soup.find('div', id=re.compile('content|main', re.I)) or \
                     soup.find('div', class_=re.compile('content|main', re.I)) or \
                     soup.find('article')
        
        if content_div:
            # Extract paragraphs
            paragraphs = content_div.find_all('p')
            result['report_details']['content'] = '\n'.join(p.get_text(strip=True) for p in paragraphs)
            
            # Extract figures and statistics from text
            stats = self._extract_statistics_from_text(result['report_details'].get('content', ''))
            if stats:
                result['report_details']['extracted_statistics'] = stats
                
        # Extract tables
        tables = soup.find_all('table')
        table_data = []
        
        for i, table in enumerate(tables):
            parsed_table = self._parse_table_to_dict(table)
            if parsed_table:
                table_data.append(parsed_table)
                
        if table_data:
            result['report_details']['tables'] = table_data
            
        # Extract time series data if available
        time_series = self._extract_time_series(soup)
        if time_series:
            result['report_details']['trends'] = time_series
            
        # Extract charts and graphs
        charts = []
        chart_elements = soup.find_all(['img', 'div'], class_=re.compile('chart|graph|figure'))
        
        for chart_elem in chart_elements:
            chart_info = {}
            
            if chart_elem.name == 'img':
                chart_info['type'] = 'image'
                chart_info['url'] = chart_elem.get('src', '')
                chart_info['alt'] = chart_elem.get('alt', '')
            else:
                chart_info['type'] = 'div'
                chart_info['id'] = chart_elem.get('id', '')
                chart_info['class'] = chart_elem.get('class', '')
                
            # Look for caption
            caption = chart_elem.find_next('figcaption') or chart_elem.find_next('caption')
            if caption:
                chart_info['caption'] = caption.get_text(strip=True)
                
            charts.append(chart_info)
            
        if charts:
            result['report_details']['charts'] = charts
            
        # Extract downloads
        downloads = []
        for link in soup.find_all('a', href=True):
            href = link['href'].lower()
            if href.endswith(('.pdf', '.xlsx', '.xls', '.csv', '.zip')):
                downloads.append({
                    'url': href if href.startswith('http') else urljoin(self.base_url, href),
                    'text': link.get_text(strip=True),
                    'type': os.path.splitext(href)[1][1:]  # Get file extension without dot
                })
                
        if downloads:
            result['report_details']['downloads'] = downloads
            
        # Save to trends directory if it contains time series data
        if 'trends' in result['report_details']:
            # Generate filename from title or URL
            if 'title' in result['report_details']:
                title = result['report_details']['title']
                safe_title = re.sub(r'[^\w\s]', '', title).strip().replace(' ', '_').lower()
                filename = f"{safe_title[:50]}.json"
            else:
                filename = f"report_{datetime.datetime.now().strftime('%Y%m%d')}.json"
                
            file_path = os.path.join(self.output_dir, "trends", filename)
            
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
                
            logger.info(f"Saved trend report to {file_path}")
            
        return result
    
    def _extract_statistics_from_text(self, text: str) -> Dict[str, Any]:
        """
        Extract statistical figures from text.
        
        Args:
            text: Text to analyze
            
        Returns:
            Dictionary of extracted statistics
        """
        stats = {}
        
        # Look for numbers with context
        # Pattern: number + noun phrase
        number_patterns = [
            # Simple number with commas: 1,234
            r'(\d{1,3}(?:,\d{3})+)\s+(\w+(?:\s+\w+){0,5})',
            # Large numbers: 1.2 million
            r'(\d+\.?\d*)\s+(million|thousand|billion|percent|%)\s+(\w+(?:\s+\w+){0,5})',
            # Percentages: 12.3%
            r'(\d+\.?\d*%)\s+(?:of\s+)?(\w+(?:\s+\w+){0,5})',
            # Simple numbers with decimal: 123.45
            r'(\d+\.\d+)\s+(\w+(?:\s+\w+){0,5})'
        ]
        
        for pattern in number_patterns:
            matches = re.finditer(pattern, text)
            for match in matches:
                groups = match.groups()
                
                # Format depends on which pattern matched
                if len(groups) == 2:
                    number, context = groups
                    if number.endswith('%'):
                        value = float(number.rstrip('%'))
                    else:
                        value = number.replace(',', '')
                        try:
                            if '.' in value:
                                value = float(value)
                            else:
                                value = int(value)
                        except ValueError:
                            value = number
                elif len(groups) == 3:
                    number, unit, context = groups
                    try:
                        value = float(number)
                        if unit == 'million':
                            value *= 1000000
                        elif unit == 'billion':
                            value *= 1000000000
                        elif unit == 'thousand':
                            value *= 1000
                        elif unit in ['percent', '%']:
                            # Keep as is, but mark as percentage
                            context = context + ' (percentage)'
                    except ValueError:
                        value = number
                else:
                    continue
                
                # Clean up context to be used as key
                key = context.strip().lower().replace(' ', '_')
                stats[key] = value
        
        return stats
    
    def _extract_time_series(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """
        Extract time series data from the page.
        
        Args:
            soup: BeautifulSoup object of the page
            
        Returns:
            List of time series data points
        """
        time_series = []
        
        # Look for time series in tables
        # Time series tables typically have year/month/date columns
        for table in soup.find_all('table'):
            headers = []
            header_row = table.find('tr')
            
            if header_row:
                for th in header_row.find_all(['th', 'td']):
                    headers.append(th.get_text(strip=True))
                
                # Check if this looks like a time series (has date/year/month headers)
                date_headers = [h for h in headers if re.search(r'(year|month|date|fy|q[1-4]|quarter|period)', h, re.I)]
                
                if date_headers:
                    # This is likely a time series table
                    series = {
                        'title': '',
                        'data_type': 'table',
                        'date_column': date_headers[0],
                        'data_points': []
                    }
                    
                    # Try to find a title
                    caption = table.find('caption')
                    if caption:
                        series['title'] = caption.get_text(strip=True)
                    else:
                        # Look for preceding header
                        prev_elem = table.find_previous(['h1', 'h2', 'h3', 'h4'])
                        if prev_elem:
                            series['title'] = prev_elem.get_text(strip=True)
                    
                    # Extract data rows
                    data_rows = []
                    for tr in table.find_all('tr')[1:]:  # Skip header row
                        cells = tr.find_all(['td', 'th'])
                        if cells:
                            row_data = {}
                            for i, cell in enumerate(cells):
                                if i < len(headers):
                                    row_data[headers[i]] = cell.get_text(strip=True)
                            
                            if row_data:
                                data_rows.append(row_data)
                    
                    # Convert to time series format
                    for row in data_rows:
                        if date_headers[0] in row:
                            data_point = {
                                'period': row[date_headers[0]]
                            }
                            
                            # Add metrics
                            for header in headers:
                                if header != date_headers[0] and header in row:
                                    # Try to convert to number
                                    value = row[header].replace(',', '')
                                    try:
                                        if '.' in value:
                                            data_point[header] = float(value)
                                        else:
                                            data_point[header] = int(value)
                                    except ValueError:
                                        data_point[header] = row[header]
                            
                            series['data_points'].append(data_point)
                    
                    time_series.append(series)
        
        # Look for time series in JavaScript (common for interactive charts)
        scripts = soup.find_all('script')
        for script in scripts:
            if script.string and any(x in script.string for x in ['series', 'timeSeries', 'data', 'chart']):
                # Try to extract arrays or objects containing time series data
                # This is more complex due to variety of JS formats, but we'll try some common patterns
                js_patterns = [
                    # Array of objects with date and value properties
                    r'(?:var\s+)?(\w+)\s*=\s*\[({\s*"date"|\{\s*date).*?\];',
                    # Common chart library formats (Highcharts, Chart.js, etc)
                    r'(?:data|series)\s*:\s*\[(.*?)\]',
                    # JSON data structure
                    r'(?:var\s+)?(\w+)\s*=\s*({.*?"\w+"\s*:\s*\[.*?\].*?});'
                ]
                
                for pattern in js_patterns:
                    matches = re.search(pattern, script.string, re.DOTALL)
                    if matches:
                        # We found potential time series data
                        # This is just a basic extraction, a real parser would be more robust
                        series = {
                            'title': 'JavaScript Chart Data',
                            'data_type': 'chart',
                            'source': 'javascript',
                            'raw_data': matches.group(0)[:1000]  # Limit size for sanity
                        }
                        time_series.append(series)
                        break
        
        return time_series
    
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
    parser = TRACReportsParser()
    return parser.parse(html_content, url) 