import logging
from typing import Optional, Dict, Any, List, Union, Callable
from bs4 import BeautifulSoup
import re
import json

logger = logging.getLogger(__name__)


class HTMLParser:
    """
    Parser for extracting content from HTML pages using BeautifulSoup.
    """
    def __init__(self, parser: str = "html.parser"):
        """
        Initialize the HTMLParser.
        
        Args:
            parser: The parser to use with BeautifulSoup (default: "html.parser")
                   Options include "html.parser", "lxml", "html5lib"
        """
        self.parser = parser
        
    def parse(self, html_content: str) -> BeautifulSoup:
        """
        Parse HTML content into a BeautifulSoup object.
        
        Args:
            html_content: The HTML content to parse
            
        Returns:
            A BeautifulSoup object
        """
        return BeautifulSoup(html_content, self.parser)
        
    def extract_text(self, html_content: str, selector: Optional[str] = None, 
                      strip: bool = True) -> str:
        """
        Extract text from HTML content, optionally using a CSS selector.
        
        Args:
            html_content: The HTML content to parse
            selector: Optional CSS selector to target specific elements (default: None)
            strip: Whether to strip whitespace from the text (default: True)
            
        Returns:
            The extracted text
        """
        soup = self.parse(html_content)
        
        if selector:
            elements = soup.select(selector)
            if not elements:
                logger.warning(f"No elements found matching selector: {selector}")
                return ""
                
            text = " ".join(element.get_text() for element in elements)
        else:
            text = soup.get_text()
            
        if strip:
            # Replace multiple whitespaces with a single space
            text = re.sub(r'\s+', ' ', text)
            text = text.strip()
            
        return text
        
    def extract_links(self, html_content: str, base_url: Optional[str] = None,
                        selector: Optional[str] = None) -> List[Dict[str, str]]:
        """
        Extract links from HTML content, optionally using a CSS selector.
        
        Args:
            html_content: The HTML content to parse
            base_url: Optional base URL to resolve relative links (default: None)
            selector: Optional CSS selector to target specific elements (default: None)
            
        Returns:
            A list of dictionaries containing link information (href, text, title)
        """
        soup = self.parse(html_content)
        
        if selector:
            elements = soup.select(selector)
            # Extract all <a> elements within the selected elements
            links = []
            for element in elements:
                links.extend(element.find_all('a', href=True))
        else:
            links = soup.find_all('a', href=True)
            
        result = []
        for link in links:
            href = link.get('href', '').strip()
            
            # Skip empty links or JavaScript links
            if not href or href.startswith('javascript:'):
                continue
                
            # Resolve relative URLs if a base URL is provided
            if base_url and href and not (href.startswith('http://') or href.startswith('https://')):
                if href.startswith('/'):
                    href = f"{base_url.rstrip('/')}{href}"
                else:
                    href = f"{base_url.rstrip('/')}/{href}"
                    
            result.append({
                'href': href,
                'text': link.get_text(strip=True),
                'title': link.get('title', '')
            })
            
        return result
        
    def extract_table(self, html_content: str, selector: Optional[str] = None) -> List[Dict[str, str]]:
        """
        Extract tabular data from HTML tables.
        
        Args:
            html_content: The HTML content to parse
            selector: Optional CSS selector to target specific table elements (default: None)
            
        Returns:
            A list of dictionaries where each dictionary represents a row with column names as keys
        """
        soup = self.parse(html_content)
        
        if selector:
            tables = soup.select(selector)
        else:
            tables = soup.find_all('table')
            
        if not tables:
            logger.warning("No tables found in the HTML content")
            return []
            
        # Use the first matching table
        table = tables[0]
        
        # Extract headers
        headers = []
        header_row = table.find('thead')
        if header_row:
            th_elements = header_row.find_all('th')
            headers = [th.get_text(strip=True) for th in th_elements]
        
        # If no headers were found in thead, try the first row
        if not headers:
            first_row = table.find('tr')
            if first_row:
                # Try to find th elements first, then td elements
                th_elements = first_row.find_all('th')
                if th_elements:
                    headers = [th.get_text(strip=True) for th in th_elements]
                else:
                    td_elements = first_row.find_all('td')
                    headers = [td.get_text(strip=True) for td in td_elements]
                    
        # If still no headers, generate column numbers
        if not headers:
            # Find the row with the most columns to determine the number of columns
            max_columns = 0
            for row in table.find_all('tr'):
                columns = len(row.find_all(['td', 'th']))
                max_columns = max(max_columns, columns)
            
            headers = [f"Column_{i+1}" for i in range(max_columns)]
        
        # Extract rows
        result = []
        for row in table.find_all('tr'):
            # Skip header rows
            if row.find_parent('thead'):
                continue
                
            cells = row.find_all(['td', 'th'])
            if cells:  # Ensure the row has cells
                row_data = {}
                for i, cell in enumerate(cells):
                    if i < len(headers):
                        row_data[headers[i]] = cell.get_text(strip=True)
                    else:
                        # If more cells than headers, add with generated column names
                        row_data[f"Column_{i+1}"] = cell.get_text(strip=True)
                        
                result.append(row_data)
                
        return result
        
    def extract_metadata(self, html_content: str) -> Dict[str, str]:
        """
        Extract metadata from HTML content (meta tags, title, etc.).
        
        Args:
            html_content: The HTML content to parse
            
        Returns:
            A dictionary of metadata key-value pairs
        """
        soup = self.parse(html_content)
        metadata = {}
        
        # Extract title
        title = soup.find('title')
        if title:
            metadata['title'] = title.get_text(strip=True)
            
        # Extract meta tags
        for meta in soup.find_all('meta'):
            # Handle different meta tag formats
            if meta.get('name') and meta.get('content'):
                metadata[meta['name']] = meta['content']
            elif meta.get('property') and meta.get('content'):
                metadata[meta['property']] = meta['content']
            elif meta.get('http-equiv') and meta.get('content'):
                metadata[f"http-equiv:{meta['http-equiv']}"] = meta['content']
                
        return metadata
        
    def extract_by_pattern(self, html_content: str, 
                           tag_name: Optional[str] = None,
                           attributes: Optional[Dict[str, str]] = None,
                           text_pattern: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Extract elements matching specific patterns.
        
        Args:
            html_content: The HTML content to parse
            tag_name: Optional tag name to filter (default: None)
            attributes: Optional dictionary of attributes to match (default: None)
            text_pattern: Optional regex pattern to match text content (default: None)
            
        Returns:
            A list of dictionaries containing information about the matched elements
        """
        soup = self.parse(html_content)
        
        # Find elements matching the tag name
        if tag_name:
            elements = soup.find_all(tag_name)
        else:
            elements = soup.find_all()
            
        # Apply attribute filters
        if attributes:
            filtered_elements = []
            for element in elements:
                match = True
                for attr_name, attr_value in attributes.items():
                    if element.get(attr_name) != attr_value:
                        match = False
                        break
                if match:
                    filtered_elements.append(element)
            elements = filtered_elements
            
        # Apply text pattern filter
        if text_pattern:
            pattern = re.compile(text_pattern)
            filtered_elements = []
            for element in elements:
                if pattern.search(element.get_text()):
                    filtered_elements.append(element)
            elements = filtered_elements
            
        # Extract information from the matched elements
        result = []
        for element in elements:
            element_info = {
                'tag': element.name,
                'text': element.get_text(strip=True),
                'html': str(element),
                'attrs': element.attrs
            }
            result.append(element_info)
            
        return result
        
    def extract_all_data(self, html_content: str) -> Dict[str, Any]:
        """
        Extract comprehensive data from all potentially valuable tags in the HTML.
        
        Args:
            html_content: The HTML content to parse
            
        Returns:
            A dictionary containing all extracted data categorized by tag types
        """
        soup = self.parse(html_content)
        result = {
            'metadata': self.extract_metadata(html_content),
            'headings': [],
            'paragraphs': [],
            'lists': [],
            'tables': [],
            'links': [],
            'images': [],
            'forms': [],
            'semantic_sections': [],
            'data_elements': [],
            'scripts': [],
            'structured_data': []
        }
        
        # Extract headings (h1-h6)
        for i in range(1, 7):
            headings = soup.find_all(f'h{i}')
            for heading in headings:
                result['headings'].append({
                    'level': i,
                    'text': heading.get_text(strip=True),
                    'id': heading.get('id', ''),
                    'path': self._get_element_path(heading)
                })
        
        # Extract paragraphs
        paragraphs = soup.find_all('p')
        for p in paragraphs:
            # Skip empty paragraphs
            text = p.get_text(strip=True)
            if text:
                result['paragraphs'].append({
                    'text': text,
                    'path': self._get_element_path(p),
                    'class': p.get('class', [])
                })
        
        # Extract lists (ul, ol, dl)
        for list_tag in soup.find_all(['ul', 'ol', 'dl']):
            list_items = []
            
            if list_tag.name in ['ul', 'ol']:
                for li in list_tag.find_all('li', recursive=False):
                    list_items.append({
                        'text': li.get_text(strip=True),
                        'html': str(li)
                    })
            elif list_tag.name == 'dl':
                for dt, dd in zip(list_tag.find_all('dt'), list_tag.find_all('dd')):
                    list_items.append({
                        'term': dt.get_text(strip=True),
                        'definition': dd.get_text(strip=True)
                    })
            
            result['lists'].append({
                'type': list_tag.name,
                'items': list_items,
                'class': list_tag.get('class', []),
                'path': self._get_element_path(list_tag)
            })
        
        # Extract tables (using existing method but enhanced)
        tables = soup.find_all('table')
        for table in tables:
            caption = table.find('caption')
            caption_text = caption.get_text(strip=True) if caption else ''
            
            table_data = self._extract_table_data(table)
            
            result['tables'].append({
                'caption': caption_text,
                'data': table_data,
                'class': table.get('class', []),
                'path': self._get_element_path(table)
            })
        
        # Extract links
        links = soup.find_all('a', href=True)
        for link in links:
            href = link['href']
            if href and not href.startswith('javascript:'):
                result['links'].append({
                    'text': link.get_text(strip=True),
                    'href': href,
                    'title': link.get('title', ''),
                    'rel': link.get('rel', []),
                    'target': link.get('target', ''),
                    'path': self._get_element_path(link)
                })
        
        # Extract images
        images = soup.find_all('img')
        for img in images:
            result['images'].append({
                'src': img.get('src', ''),
                'alt': img.get('alt', ''),
                'title': img.get('title', ''),
                'width': img.get('width', ''),
                'height': img.get('height', ''),
                'class': img.get('class', []),
                'path': self._get_element_path(img)
            })
        
        # Extract forms and inputs
        forms = soup.find_all('form')
        for form in forms:
            inputs = []
            for input_tag in form.find_all(['input', 'select', 'textarea']):
                input_data = {
                    'type': input_tag.name,
                    'name': input_tag.get('name', ''),
                    'id': input_tag.get('id', '')
                }
                
                if input_tag.name == 'input':
                    input_data['input_type'] = input_tag.get('type', 'text')
                    input_data['value'] = input_tag.get('value', '')
                    input_data['placeholder'] = input_tag.get('placeholder', '')
                
                inputs.append(input_data)
            
            result['forms'].append({
                'action': form.get('action', ''),
                'method': form.get('method', 'get'),
                'inputs': inputs,
                'path': self._get_element_path(form)
            })
        
        # Extract semantic sections (article, section, nav, aside, footer, etc.)
        semantic_tags = ['article', 'section', 'nav', 'aside', 'header', 'footer', 'main']
        for tag_name in semantic_tags:
            for element in soup.find_all(tag_name):
                # Get heading if available
                heading = element.find(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])
                heading_text = heading.get_text(strip=True) if heading else ''
                
                result['semantic_sections'].append({
                    'type': tag_name,
                    'heading': heading_text,
                    'summary': self._summarize_text(element.get_text()),
                    'class': element.get('class', []),
                    'id': element.get('id', ''),
                    'path': self._get_element_path(element)
                })
        
        # Extract data elements (time, data, meter, progress)
        data_tags = ['time', 'data', 'meter', 'progress']
        for tag_name in data_tags:
            for element in soup.find_all(tag_name):
                result['data_elements'].append({
                    'type': tag_name,
                    'text': element.get_text(strip=True),
                    'value': element.get('value', ''),
                    'datetime': element.get('datetime', ''),
                    'path': self._get_element_path(element)
                })
        
        # Extract structured data (JSON-LD, microdata)
        for script in soup.find_all('script', {'type': 'application/ld+json'}):
            try:
                json_data = json.loads(script.string)
                result['structured_data'].append({
                    'type': 'json-ld',
                    'data': json_data
                })
            except (json.JSONDecodeError, TypeError):
                pass
        
        # Extract data from microdata
        for element in soup.find_all(itemscope=True):
            microdata = {
                'type': element.get('itemtype', ''),
                'properties': {}
            }
            
            for prop in element.find_all(itemprop=True):
                prop_name = prop.get('itemprop', '')
                prop_value = ''
                
                if prop.name == 'meta':
                    prop_value = prop.get('content', '')
                elif prop.name == 'img':
                    prop_value = prop.get('src', '')
                elif prop.name == 'a':
                    prop_value = prop.get('href', '')
                elif prop.name == 'time':
                    prop_value = prop.get('datetime', '')
                else:
                    prop_value = prop.get_text(strip=True)
                
                microdata['properties'][prop_name] = prop_value
            
            result['structured_data'].append({
                'type': 'microdata',
                'data': microdata
            })
        
        # Find custom data attributes (data-*)
        data_attrs_elements = []
        for tag in soup.find_all():
            data_attrs = {k: v for k, v in tag.attrs.items() if k.startswith('data-')}
            if data_attrs:
                data_attrs_elements.append({
                    'tag': tag.name,
                    'data_attrs': data_attrs,
                    'text': tag.get_text(strip=True),
                    'path': self._get_element_path(tag)
                })
                
        if data_attrs_elements:
            result['custom_data_attributes'] = data_attrs_elements
        
        return result
        
    def _get_element_path(self, element) -> str:
        """
        Generate a CSS selector path to the element for reference.
        
        Args:
            element: BeautifulSoup element
            
        Returns:
            A string representing the path to the element
        """
        path_parts = []
        
        for parent in element.parents:
            if parent.name == '[document]':
                break
                
            # Try to use id as identifier
            if parent.get('id'):
                path_parts.append(f"#{parent['id']}")
                break
            # Otherwise use tag with class if available
            elif parent.get('class'):
                class_str = '.'.join(parent['class'])
                path_parts.append(f"{parent.name}.{class_str}")
            else:
                siblings = [s for s in parent.find_previous_siblings(parent.name)]
                if siblings:
                    path_parts.append(f"{parent.name}:nth-of-type({len(siblings) + 1})")
                else:
                    path_parts.append(parent.name)
        
        # Add the element itself
        if element.get('id'):
            path_parts.append(f"#{element['id']}")
        elif element.get('class'):
            class_str = '.'.join(element['class'])
            path_parts.append(f"{element.name}.{class_str}")
        else:
            siblings = [s for s in element.find_previous_siblings(element.name)]
            if siblings:
                path_parts.append(f"{element.name}:nth-of-type({len(siblings) + 1})")
            else:
                path_parts.append(element.name)
        
        # Reverse to get correct order and join
        return ' > '.join(reversed(path_parts))
        
    def _extract_table_data(self, table) -> Dict[str, Any]:
        """
        Extract data from a table element more comprehensively.
        
        Args:
            table: BeautifulSoup table element
            
        Returns:
            Dictionary with table data including headers and rows
        """
        result = {
            'headers': [],
            'rows': []
        }
        
        # Extract headers
        thead = table.find('thead')
        if thead:
            header_rows = thead.find_all('tr')
            for tr in header_rows:
                header_cells = []
                for th in tr.find_all(['th', 'td']):
                    header_cells.append({
                        'text': th.get_text(strip=True),
                        'colspan': int(th.get('colspan', 1)),
                        'rowspan': int(th.get('rowspan', 1)),
                        'scope': th.get('scope', '')
                    })
                result['headers'].append(header_cells)
        else:
            # Look for header row in table
            first_row = table.find('tr')
            if first_row:
                header_cells = []
                for th in first_row.find_all(['th', 'td']):
                    header_cells.append({
                        'text': th.get_text(strip=True),
                        'colspan': int(th.get('colspan', 1)),
                        'rowspan': int(th.get('rowspan', 1))
                    })
                if any(cell.name == 'th' for cell in first_row.find_all(['th', 'td'])):
                    result['headers'].append(header_cells)
        
        # Extract rows from tbody if present, otherwise from table
        body = table.find('tbody') or table
        
        # Skip header row if already processed
        rows = body.find_all('tr')
        start_index = 1 if result['headers'] and not thead else 0
        
        for tr in rows[start_index:]:
            row_data = []
            for cell in tr.find_all(['td', 'th']):
                row_data.append({
                    'text': cell.get_text(strip=True),
                    'colspan': int(cell.get('colspan', 1)),
                    'rowspan': int(cell.get('rowspan', 1)),
                    'html': str(cell)
                })
            result['rows'].append(row_data)
        
        return result
        
    def _summarize_text(self, text: str, max_length: int = 100) -> str:
        """
        Create a short summary of text content.
        
        Args:
            text: The text to summarize
            max_length: Maximum length of summary
            
        Returns:
            A summarized version of the text
        """
        text = re.sub(r'\s+', ' ', text).strip()
        if len(text) <= max_length:
            return text
        return text[:max_length-3] + '...' 